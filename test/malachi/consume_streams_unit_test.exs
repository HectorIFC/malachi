defmodule Malachi.ConsumeStreamsUnitTest do
  # `Malachi.ConsumeStreams` against a real broker, driven a message at a time, so the orders a connection
  # sees only by racing can be set up one step at a time: an ack inside a page, a read that fails and is
  # tried again, a broker that goes down, data retention removed before a page.
  use ExUnit.Case, async: true

  import Malachi.Test.TeardownHelper

  alias Malachi.Broker.ReadView
  alias Malachi.BrokerServer
  alias Malachi.ConsumeStreams
  alias Malachi.Log.Record
  alias Malachi.Wire
  alias Malachi.Wire.Batch

  @moduletag :tmp_dir

  setup %{tmp_dir: dir} do
    one_record = Record.encoded_size(Record.new("v0", key: "k"))
    {:ok, server} = BrokerServer.start_link(dir, segment_max_bytes: one_record)
    on_exit(fn -> stop_quietly(server) end)
    {:ok, root} = BrokerServer.create_topic(server, "events", 4)
    %{server: server, root: root}
  end

  defp produce(server, values),
    do: {:ok, _} = BrokerServer.produce(server, "events", Enum.map(values, &Record.new(&1, key: "k")))

  # A stream opened on `root` through the real broker, as the connection records it.
  defp open(ctx, opts \\ []) do
    {:ok, token, position, broker_pid} =
      BrokerServer.open_consume(ctx.server, ctx.root, Keyword.get(opts, :start, :earliest))

    opened = %{
      id: 1,
      corr: 20,
      broker: ctx.server,
      broker_pid: broker_pid,
      topic: "events",
      range_id: ctx.root,
      token: token,
      position: position,
      window: Keyword.get(opts, :window, 100),
      max: Keyword.get(opts, :max, 100),
      max_bytes: Keyword.get(opts, :max_bytes, 1_048_576),
      allowed: Keyword.get(opts, :allowed, fn -> :ok end)
    }

    {ConsumeStreams.open(ConsumeStreams.new(), opened), token}
  end

  # Feeds the next message the test process received to the streams, returning the pushes decoded.
  defp step(streams) do
    receive do
      message ->
        case ConsumeStreams.handle_message(streams, message) do
          {streams, frames} -> {streams, Enum.map(frames, &push/1)}
          :no -> step(streams)
        end
    after
      2_000 -> flunk("no message for the streams")
    end
  end

  defp push(frame) do
    {:ok, body, <<>>} = Wire.decode_frame(frame)
    {20, 0, payload} = Wire.decode_response(body)
    Wire.decode_push(payload)
  end

  defp values(%{batch: batch}) do
    {:ok, entries} = Batch.decode(batch, max_inflated_bytes: 1_048_576, layout: :positioned)
    Enum.map(entries, fn {position, record, false} -> {position, record.value} end)
  end

  test "an ack inside a page frees none of it; one at its end frees all of it", ctx do
    produce(ctx.server, ["a", "b", "c", "d"])
    {streams, _token} = open(ctx, window: 2, max: 2)

    {streams, [{:records, page}]} = step(streams)
    assert values(page) == [{{0, 0}, "a"}, {{0, 1}, "b"}]
    assert streams.streams[1].in_flight == 2

    {streams, []} = ConsumeStreams.ack(streams, 1, {0, 1}, 0)
    assert streams.streams[1].in_flight == 2

    {streams, frames} = ConsumeStreams.ack(streams, 1, page.next, 0)
    assert [{:records, next}] = Enum.map(frames, &push/1)
    assert values(next) == [{{0, 2}, "c"}, {{0, 3}, "d"}]
    assert streams.streams[1].in_flight == 2
    assert ConsumeStreams.ack(streams, 9, {0, 0}, 0) == {:unknown, 9}
  end

  test "a read that fails is tried again from a fresh view, not at once", ctx do
    produce(ctx.server, ["a"])
    {streams, token} = open(ctx)

    # a view whose segment is led by a node that does not exist: the read fails
    receive do
      {:consume_wake, ^token, view, reporter} ->
        segments =
          Map.new(view.segments, fn {id, segs} ->
            {id, Enum.map(segs, &%{&1 | replica_set: [{:gone, :nowhere@host}]})}
          end)

        {streams, []} =
          ConsumeStreams.handle_message(streams, {:consume_wake, token, %ReadView{view | segments: segments}, reporter})

        assert_receive {:consume_retry, 1}, 1_000
        {streams, []} = ConsumeStreams.handle_message(streams, {:consume_retry, 1})
        # the retry asked the broker for a fresh view, which reads
        {_streams, [{:records, page}]} = step(streams)
        assert values(page) == [{{0, 0}, "a"}]
    after
      2_000 -> flunk("no first view")
    end
  end

  test "a broker that goes down moves every stream it served, to its own range", ctx do
    {streams, _token} = open(ctx)
    {streams, []} = step(streams)
    Process.unlink(ctx.server)
    Process.exit(ctx.server, :kill)

    {streams, [{:moved, moved}]} = step(streams)
    assert %{stream_id: 1, reason: "restarted", targets: [%{range: 0, segment: nil}]} = moved
    refute ConsumeStreams.open?(streams)
  end

  test "data retention removed before a page is counted in it, exactly", ctx do
    # one record per segment: each produce rolls the segment it filled
    for value <- ["a", "b", "c"], do: produce(ctx.server, [value])
    first = sealed_first_segment(ctx, 50)
    :ok = BrokerServer.delete_segment(ctx.server, first.id)

    {streams, _token} = open(ctx)
    {_streams, [{:records, page}]} = step(streams)
    gone = first.length
    assert gone >= 1
    # the page starts where the deleted segment ended, and says how many records it lost
    assert values(page) == Enum.drop([{{0, 0}, "a"}, {{0, 1}, "b"}, {{0, 2}, "c"}], gone)
    assert page.expired == gone and page.expired_exact
  end

  # The range's first segment once its roll's fence sealed it (the fence answers asynchronously).
  defp sealed_first_segment(ctx, attempts) do
    [first | _rest] =
      ctx.server
      |> BrokerServer.metadata()
      |> Malachi.Metadata.segments_of_range(ctx.root)
      |> Enum.sort_by(& &1.start_offset)

    cond do
      first.state == :sealed -> first
      attempts > 0 -> Process.sleep(20) && sealed_first_segment(ctx, attempts - 1)
      true -> flunk("the first segment never sealed")
    end
  end

  test "one page per message: each page goes out before the next is read", ctx do
    for value <- ["v1", "v2", "v3"], do: produce(ctx.server, [value])
    {streams, _token} = open(ctx, max: 1)

    {streams, [{:records, first}]} = step(streams)
    assert values(first) == [{{0, 0}, "v1"}]
    # the next page waits for its own message
    assert_received {:consume_continue, 1}
    {streams, frames} = ConsumeStreams.handle_message(streams, {:consume_continue, 1})
    assert [{:records, second}] = Enum.map(frames, &push/1)
    assert values(second) == [{{0, 1}, "v2"}]
    assert streams.streams[1].in_flight == 2
  end

  test "an ack at the end of a page's last record frees it, though the page ends further on", ctx do
    # a parent with one record in each child's slice, then a split: the left child's first page holds the
    # parent's left record and ends past the right one, at the start of the child's own records
    %{keyspace_size: size} = Malachi.Metadata.get_range(BrokerServer.metadata(ctx.server), ctx.root)
    key = fn wanted -> "k#{Enum.find(1..10_000, &(:erlang.phash2("k#{&1}", size) in wanted))}" end
    {:ok, _} = BrokerServer.produce(ctx.server, "events", [Record.new("left", key: key.(0..(div(size, 2) - 1)))])
    {:ok, _} = BrokerServer.produce(ctx.server, "events", [Record.new("right", key: key.(div(size, 2)..(size - 1)))])
    {:ok, left, _right} = BrokerServer.split_range(ctx.server, ctx.root)

    {:ok, token, position, broker_pid} = BrokerServer.open_consume(ctx.server, left, :earliest)

    opened = %{
      id: 1,
      corr: 20,
      broker: ctx.server,
      broker_pid: broker_pid,
      topic: "events",
      range_id: left,
      token: token,
      position: position,
      # room for both of the parent's records, so the page reads past the right one to the child's own
      window: 2,
      max: 2,
      max_bytes: 1_048_576,
      allowed: fn -> :ok end
    }

    {streams, [{:records, page}]} = step(ConsumeStreams.open(ConsumeStreams.new(), opened))
    assert values(page) == [{{0, 0}, "left"}]
    assert page.next > {0, 1}
    assert streams.streams[1].in_flight == 1

    {streams, _frames} = ConsumeStreams.ack(streams, 1, {0, 1}, 0)
    assert streams.streams[1].in_flight == 0
  end

  test "a page cut by its byte limit reports only the expired data before its last record", ctx do
    for value <- ["v1", "v2", "v3"], do: produce(ctx.server, [value])
    [_first, middle | _rest] = sealed_segments(ctx, 2, 50)
    :ok = BrokerServer.delete_segment(ctx.server, middle.id)

    {streams, _token} = open(ctx, max_bytes: 1)
    {streams, [{:records, first}]} = step(streams)
    assert values(first) == [{{0, 0}, "v1"}]
    assert first.expired == 0 and first.next == {0, 1}

    assert_received {:consume_continue, 1}
    {_streams, frames} = ConsumeStreams.handle_message(streams, {:consume_continue, 1})
    assert [{:records, second}] = Enum.map(frames, &push/1)
    assert values(second) == [{{0, 2}, "v3"}]
    assert second.expired == 1 and second.expired_exact
  end

  test "a page with no records still goes out when it moved past expired data", ctx do
    produce(ctx.server, ["v1"])
    [only] = sealed_segments(ctx, 1, 50)
    # an empty active segment after it, the floor a reader below is clamped up to
    _ = BrokerServer.open_stream(ctx.server, ctx.root)
    :ok = BrokerServer.delete_segment(ctx.server, only.id)

    {streams, _token} = open(ctx)
    {_streams, [{:records, page}]} = step(streams)
    assert values(page) == []
    assert page.expired == 1 and page.next == {0, 1}
  end

  test "a stream whose access was revoked is answered with the refusal and closed", ctx do
    produce(ctx.server, ["v1"])
    {streams, token} = open(ctx, allowed: fn -> {:error, :permission_denied} end)

    receive do
      {:consume_wake, ^token, _view, _reporter} = wake ->
        {streams, [frame]} = ConsumeStreams.handle_message(streams, wake)
        {:ok, body, <<>>} = Wire.decode_frame(frame)
        assert {20, 1, payload} = Wire.decode_response(body)
        assert Wire.decode_error_reason(payload) == "permission_denied"
        refute ConsumeStreams.open?(streams)
        refute Map.has_key?(:sys.get_state(ctx.server).consume.consumers, token)
    after
      2_000 -> flunk("no wake")
    end
  end

  # The range's first `count` segments once each is sealed (a roll's fence answers asynchronously).
  defp sealed_segments(ctx, count, attempts) do
    sealed =
      ctx.server
      |> BrokerServer.metadata()
      |> Malachi.Metadata.segments_of_range(ctx.root)
      |> Enum.sort_by(& &1.start_offset)
      |> Enum.take(count)

    cond do
      length(sealed) == count and Enum.all?(sealed, &(&1.state == :sealed)) -> sealed
      attempts > 0 -> Process.sleep(20) && sealed_segments(ctx, count, attempts - 1)
      true -> flunk("#{count} segments never sealed")
    end
  end

  test "messages that are not about its streams are not taken", ctx do
    {streams, token} = open(ctx)
    assert ConsumeStreams.handle_message(streams, {:consume_wake, make_ref(), nil, nil}) == :no
    assert ConsumeStreams.handle_message(streams, {:stream_moved, make_ref(), :sealed, []}) == :no
    assert ConsumeStreams.handle_message(streams, :unrelated) == :no
    assert {^streams, []} = ConsumeStreams.handle_message(streams, {:consume_retry, 42})
    assert {:ok, closed} = ConsumeStreams.close(streams, 1)
    assert ConsumeStreams.close(closed, 1) == :unknown
    refute Map.has_key?(:sys.get_state(ctx.server).consume.consumers, token)
  end
end
