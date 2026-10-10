defmodule Malachi.ConsumeStreamsTest do
  # Consume streams and `fetch_range` end to end over the real TCP server (`Malachi.Wire` keys 29 to 31):
  # pushes of records with their positions, credit returned by read acks, the wake on new records, where a
  # stream starts, the unary read and its wait, the refusals, the move a split sends, and a connection that
  # produces and consumes at once.
  #
  # async: false. The `producer_streams` flag is the application's own cache (`ClusterFlagsPause`), and
  # the routing reads the topology the consumer-group router publishes, cleared here for each test.
  use ExUnit.Case, async: false

  import Malachi.Test.QuotaForensics, only: [within_one_window: 2, snapshot: 2, assert_refused: 4]
  import Malachi.Test.StreamTCP

  alias Malachi.BrokerServer
  alias Malachi.Cluster.ClusterFlagsCache
  alias Malachi.DataPlaneRouter
  alias Malachi.Routing
  alias Malachi.Test.ClusterFlagsPause
  alias Malachi.Test.TCPHelper
  alias Malachi.Wire

  @router_topology {Malachi.Consumer.CoordinatorRouter, :topology}
  @quota_window_ms 60_000

  setup do
    :ok = ClusterFlagsPause.pause()
    ClusterFlagsCache.put([Routing.flag()])

    topology = :persistent_term.get(@router_topology, nil)
    :persistent_term.erase(@router_topology)

    on_exit(fn ->
      if topology,
        do: :persistent_term.put(@router_topology, topology),
        else: :persistent_term.erase(@router_topology)
    end)

    topic = "cstream_#{System.unique_integer([:positive])}"
    producer = connect()
    {code, _} = TCPHelper.request(producer, Wire.create_topic_key(), 2, Wire.encode_create_topic_req(topic, 4))
    assert code == Wire.ok_code()

    %{producer: producer, consumer: connect(), topic: topic}
  end

  defp connect(user \\ "app", password \\ "app123") do
    {:ok, socket} = TCPHelper.connect()
    {:ok, _token} = TCPHelper.authenticate_wire(socket, user, password)
    on_exit(fn -> :gen_tcp.close(socket) end)
    socket
  end

  # Appends `values` on a producer stream of its own, compressed, and waits for the ack.
  defp produce(socket, topic, values) do
    {:ok, %{stream_id: id}} = open(socket, topic)
    send_append(socket, id, 0, batch(values, codec: :zstd))
    assert {_ack, []} = acked_through(socket, 0)
  end

  defp records_push(socket, timeout \\ 5_000) do
    {:records, page} = push(socket, consumer_corr(), timeout)
    page
  end

  describe "pushes" do
    test "a compressed batch reads back as the same records, each with its position, and the page's end", ctx do
      produce(ctx.producer, ctx.topic, ["a", "b", "c"])
      {:ok, %{stream_id: id, position: {0, 0}}} = open_consume(ctx.consumer, ctx.topic)

      page = records_push(ctx.consumer)
      assert page.stream_id == id
      assert page_records(page) == [{{0, 0}, "a"}, {{0, 1}, "b"}, {{0, 2}, "c"}]
      assert %{next: {0, 3}, skip: 0, backlog: 0, expired: 0, expired_exact: true} = page
    end

    test "records produced after the stream caught up are pushed as they land", ctx do
      {:ok, %{position: {0, 0}}} = open_consume(ctx.consumer, ctx.topic)
      assert {:error, :timeout} = TCPHelper.recv_frame(ctx.consumer, timeout: 200)

      produce(ctx.producer, ctx.topic, ["late"])
      assert [{{0, 0}, "late"}] = page_records(records_push(ctx.consumer))
    end

    test "credit is in records: a full window holds the pushes until an ack frees it", ctx do
      produce(ctx.producer, ctx.topic, Enum.map(1..5, &"v#{&1}"))
      {:ok, %{stream_id: id}} = open_consume(ctx.consumer, ctx.topic, window: 2, max: 1)

      first = records_push(ctx.consumer)
      second = records_push(ctx.consumer)
      assert [{{0, 0}, "v1"}] = page_records(first)
      assert [{{0, 1}, "v2"}] = page_records(second)
      assert {:error, :timeout} = TCPHelper.recv_frame(ctx.consumer, timeout: 300)

      # an ack at the first page's end frees one record of credit, and widens the window to 3
      consume_ack(ctx.consumer, id, first.next, 3)
      assert [{{0, 2}, "v3"}] = page_records(records_push(ctx.consumer))
      assert [{{0, 3}, "v4"}] = page_records(records_push(ctx.consumer))
      assert {:error, :timeout} = TCPHelper.recv_frame(ctx.consumer, timeout: 300)
    end

    test "the window an ack asks for is capped as at open", ctx do
      # more records than the cap, in one compressed batch
      produce(ctx.producer, ctx.topic, Enum.map(1..10_010, &"r#{&1}"))
      {:ok, %{stream_id: id}} = open_consume(ctx.consumer, ctx.topic, window: 1, max: 1_000)
      first = records_push(ctx.consumer)
      consume_ack(ctx.consumer, id, {0, 0}, 4_294_967_295)

      # the cap is 10_000 records in flight, the first page's one included: they all come, and no more
      assert read_records(ctx.consumer, length(page_records(first))) == 10_000
      assert {:error, :timeout} = TCPHelper.recv_frame(ctx.consumer, timeout: 300)
    end

    test "a page holds about max_bytes, and always one record", ctx do
      produce(ctx.producer, ctx.topic, [String.duplicate("x", 300), "y", "z"])
      {:ok, _resp} = open_consume(ctx.consumer, ctx.topic, max_bytes: 10)

      assert [{{0, 0}, _large}] = page_records(first = records_push(ctx.consumer))
      assert first.next == {0, 1}
      assert first.backlog == 2
    end
  end

  describe "compression and history" do
    test "a compressed batch appended on the stream keys reads back on legacy fetch with the flag off", ctx do
      produce(ctx.producer, ctx.topic, ["z1", "z2"])
      ClusterFlagsCache.put([])
      assert values(ctx.consumer, ctx.topic) == ["z1", "z2"]
    end

    test "a child range reads its share of its parent first, then its own records", ctx do
      broker = DataPlaneRouter.shard_for(ctx.topic)

      key_in = fn range, from ->
        "k#{Enum.find(from..100_000, &(:erlang.phash2("k#{&1}", 16) in range.key_start..(range.key_end - 1)))}"
      end

      {:ok, %{stream_id: parent_stream}} = open(ctx.producer, ctx.topic)
      [whole] = routes(ctx.producer, ctx.topic).ranges
      send_append(ctx.producer, parent_stream, 0, batch(["parent"], key: key_in.(whole, 1)))
      assert {_ack, []} = acked_through(ctx.producer, 0)
      {:ok, _left, _right} = BrokerServer.split_range(broker, {ctx.topic, 0})
      assert {:moved, _moved} = push(ctx.producer)

      # the parent's record lands in exactly one child's slice; that child reads it first
      [_parent, left, right] = routes(ctx.producer, ctx.topic).ranges
      parent_key = key_in.(whole, 1)
      child = if :erlang.phash2(parent_key, 16) < left.key_end, do: left, else: right
      {:ok, %{stream_id: child_stream}} = open(ctx.producer, ctx.topic, range: child.range)
      send_append(ctx.producer, child_stream, 0, batch(["child"], key: key_in.(child, 1)))
      assert {_ack, []} = acked_through(ctx.producer, 0)

      {:ok, _resp} = open_consume(ctx.consumer, ctx.topic, range: child.range)
      records = page_records(records_push(ctx.consumer))
      assert [{{0, 0}, "parent"}, {{1, 0}, "child"}] = records
    end
  end

  describe "where a stream starts" do
    setup ctx do
      produce(ctx.producer, ctx.topic, ["r0", "r1", "r2"])
      :ok
    end

    test "latest starts past the records the range holds", ctx do
      {:ok, %{position: {0, 3}}} = open_consume(ctx.consumer, ctx.topic, start: :latest)
      assert {:error, :timeout} = TCPHelper.recv_frame(ctx.consumer, timeout: 200)
    end

    test "a position starts there", ctx do
      {:ok, %{position: {0, 1}}} = open_consume(ctx.consumer, ctx.topic, start: {:position, {0, 1}})
      assert [{{0, 1}, "r1"}, {{0, 2}, "r2"}] = page_records(records_push(ctx.consumer))
    end

    test "a position past the range's sources is refused", ctx do
      assert open_consume(ctx.consumer, ctx.topic, start: {:position, {5, 0}}) == {:error, "invalid_position"}
    end

    test "a group's checkpoint starts where it committed, and a group with none at the start", ctx do
      broker = DataPlaneRouter.shard_for(ctx.topic)
      :ok = BrokerServer.commit_offset(broker, "g1", ctx.topic, %{{ctx.topic, 0} => {0, 2}})

      {:ok, %{position: {0, 2}}} = open_consume(ctx.consumer, ctx.topic, start: {:committed, "g1"})
      assert [{{0, 2}, "r2"}] = page_records(records_push(ctx.consumer))

      other = connect()
      {:ok, %{position: {0, 0}}} = open_consume(other, ctx.topic, start: {:committed, "never"})
    end
  end

  describe "fetch_range" do
    test "answers one page at once", ctx do
      produce(ctx.producer, ctx.topic, ["f0", "f1", "f2"])

      {:ok, page} = fetch_range(ctx.consumer, ctx.topic, max: 2)
      assert page_records(page) == [{{0, 0}, "f0"}, {{0, 1}, "f1"}]
      assert page.next == {0, 2} and page.backlog == 1
    end

    test "waits for records past its start, and answers as soon as they land", ctx do
      task = Task.async(fn -> fetch_range(ctx.consumer, ctx.topic, wait_ms: 5_000) end)
      Process.sleep(200)
      produce(ctx.producer, ctx.topic, ["waited"])

      assert {:ok, page} = Task.await(task, 6_000)
      assert [{{0, 0}, "waited"}] = page_records(page)
    end

    test "after its wait without records, answers an empty page at its start", ctx do
      {:ok, page} = fetch_range(ctx.consumer, ctx.topic, start: :latest, wait_ms: 200)
      assert page_records(page) == [] and page.next == {0, 0}
    end
  end

  describe "refusals" do
    test "a client that does not accept codec none is refused", ctx do
      assert open_consume(ctx.consumer, ctx.topic, accept: [:zstd]) == {:error, "unsupported_codec"}
      assert fetch_range(ctx.consumer, ctx.topic, accept: [:zstd]) == {:error, "unsupported_codec"}
    end

    test "routes that differ from the vnode's are stale_routes", ctx do
      assert open_consume(ctx.consumer, ctx.topic, routes_version: 12_345) == {:error, "stale_routes"}
    end

    test "a read ack for a stream the connection never opened is unknown_stream", ctx do
      consume_ack(ctx.consumer, 77, {0, 0})
      assert unknown_stream?(ctx.consumer, 21)
    end

    test "on a connection that holds streams, a read ack or close of an id it does not hold is unknown_stream", ctx do
      {:ok, %{stream_id: id}} = open_consume(ctx.consumer, ctx.topic)
      consume_ack(ctx.consumer, id + 50, {0, 0})
      assert unknown_stream?(ctx.consumer, 21)

      :ok =
        :gen_tcp.send(
          ctx.consumer,
          Wire.encode_request(Wire.close_stream_key(), 22, Wire.encode_close_stream_req(id + 50))
        )

      assert unknown_stream?(ctx.consumer, 22)
    end

    test "a stream whose consume permission is revoked is refused at its next page, and closed", ctx do
      user = "cstream_revoked_#{System.unique_integer([:positive])}"
      :ok = Malachi.Auth.add_user(user, "secret-pass-123", [])
      on_exit(fn -> Malachi.Auth.remove_user(user) end)
      :ok = Malachi.Auth.grant_acl(user, :consume, ctx.topic)
      strict = Application.get_env(:malachi, :acl_strict)
      Application.put_env(:malachi, :acl_strict, true)

      on_exit(fn ->
        if strict == nil,
          do: Application.delete_env(:malachi, :acl_strict),
          else: Application.put_env(:malachi, :acl_strict, strict)
      end)

      reader = connect(user, "secret-pass-123")
      {:ok, _resp} = open_consume(reader, ctx.topic)
      :ok = Malachi.Auth.revoke_acl(user, :consume, ctx.topic)

      # straight to the broker: under strict ACLs the test's producer holds no grant either
      {:ok, _} =
        BrokerServer.produce(DataPlaneRouter.shard_for(ctx.topic), ctx.topic, [Malachi.Log.Record.new("secret")])

      {:ok, body} = TCPHelper.recv_frame(reader)
      assert {20, 1, payload} = Wire.decode_response(body)
      assert Wire.decode_error_reason(payload) == "permission_denied"
      assert {:error, :timeout} = TCPHelper.recv_frame(reader, timeout: 200)
    end

    test "a session without the consume permission on the topic is refused", ctx do
      user = "cstream_writer_#{System.unique_integer([:positive])}"
      :ok = Malachi.Auth.add_user(user, "secret-pass-123", [:produce])
      on_exit(fn -> Malachi.Auth.remove_user(user) end)
      writer = connect(user, "secret-pass-123")

      assert open_consume(writer, ctx.topic, routes_version: 0) == {:error, "permission_denied"}
      assert fetch_range(writer, ctx.topic, routes_version: 0) == {:error, "permission_denied"}
    end

    test "opening a stream and fetching each spend one token of the subscribe quota", ctx do
      prior =
        for key <- [:subscribe_rate_limit, :subscribe_rate_window_ms], do: {key, Application.get_env(:malachi, key)}

      on_exit(fn -> for {key, value} <- prior, do: Application.put_env(:malachi, key, value) end)
      Application.put_env(:malachi, :subscribe_rate_limit, 2)
      Application.put_env(:malachi, :subscribe_rate_window_ms, @quota_window_ms)

      # a fresh user per attempt, so a window that turns over mid-test reruns it from an unspent quota
      within_one_window(@quota_window_ms, fn ->
        user = "cstream_quota_#{System.unique_integer([:positive])}"
        :ok = Malachi.Auth.add_user(user, "secret-pass-123", [:produce, :consume])
        on_exit(fn -> Malachi.Auth.remove_user(user) end)
        reader = connect(user, "secret-pass-123")

        spent = snapshot(user, :subscribe)
        assert {:ok, _resp} = open_consume(reader, ctx.topic)
        assert {:ok, _page} = fetch_range(reader, ctx.topic)
        assert_refused(fetch_range(reader, ctx.topic), spent, user, :subscribe)
      end)
    end

    test "with the flag off the consume keys are unsupported", ctx do
      ClusterFlagsCache.put([])
      assert open_consume(ctx.consumer, ctx.topic, routes_version: 0) == {:error, "unsupported"}
      assert fetch_range(ctx.consumer, ctx.topic, routes_version: 0) == {:error, "unsupported"}
    end
  end

  describe "moves and closes" do
    test "a split moves the stream to the range's children, where its position goes on", ctx do
      produce(ctx.producer, ctx.topic, ["before"])
      {:ok, %{stream_id: id}} = open_consume(ctx.consumer, ctx.topic)
      page = records_push(ctx.consumer)

      {:ok, left, right} = BrokerServer.split_range(DataPlaneRouter.shard_for(ctx.topic), {ctx.topic, 0})

      assert {:moved, %{stream_id: ^id, reason: "retired", targets: targets}} = push(ctx.consumer, consumer_corr())
      assert Enum.map(targets, & &1.range) == [elem(left, 1), elem(right, 1)]

      # the parent's position names the same record in a child, so a child read from it repeats nothing and
      # goes on with the child's own records
      {:ok, %{position: {0, 1}}} =
        open_consume(ctx.consumer, ctx.topic, range: elem(left, 1), start: {:position, page.next})

      # a connection of its own: the first producer's stream was moved by the split too
      writer = connect()
      child = Enum.find(routes(writer, ctx.topic).ranges, &(&1.range == elem(left, 1)))
      key = "k#{Enum.find(1..10_000, &(:erlang.phash2("k#{&1}", 16) in child.key_start..(child.key_end - 1)))}"
      {:ok, %{stream_id: child_stream}} = open(writer, ctx.topic, range: child.range)
      send_append(writer, child_stream, 0, batch(["after"], key: key))
      assert {_ack, []} = acked_through(writer, 0)

      assert [{{1, 0}, "after"}] = page_records(records_push(ctx.consumer))
    end

    test "a closed stream is pushed nothing more", ctx do
      {:ok, %{stream_id: id}} = open_consume(ctx.consumer, ctx.topic)

      assert {0, <<>>} =
               TCPHelper.request(ctx.consumer, Wire.close_stream_key(), 22, Wire.encode_close_stream_req(id))

      produce(ctx.producer, ctx.topic, ["after"])
      assert {:error, :timeout} = TCPHelper.recv_frame(ctx.consumer, timeout: 300)
    end

    test "a connection holding a consume stream is not closed as idle", ctx do
      timeout = Application.get_env(:malachi, :tcp_recv_timeout)
      Application.put_env(:malachi, :tcp_recv_timeout, 200)
      on_exit(fn -> Application.put_env(:malachi, :tcp_recv_timeout, timeout) end)

      {:ok, _resp} = open_consume(ctx.consumer, ctx.topic)
      Process.sleep(600)
      produce(ctx.producer, ctx.topic, ["still here"])
      assert [{{0, 0}, "still here"}] = page_records(records_push(ctx.consumer))
    end
  end

  describe "fetch_range on a connection that holds streams" do
    test "a fetch waiting for records does not hold the connection's pushes back", ctx do
      quiet = "cstream_quiet_#{System.unique_integer([:positive])}"
      {0, _} = TCPHelper.request(ctx.producer, Wire.create_topic_key(), 2, Wire.encode_create_topic_req(quiet, 4))
      {:ok, _resp} = open_consume(ctx.consumer, ctx.topic)

      # a fetch that waits two seconds on a range nothing is written to
      req = %{
        topic: quiet,
        range: 0,
        routes_version: routes(ctx.producer, quiet).version,
        start: :latest,
        max: 10,
        max_bytes: 1_048_576,
        wait_ms: 2_000,
        accept: [:none]
      }

      :ok =
        :gen_tcp.send(ctx.consumer, Wire.encode_request(Wire.fetch_range_key(), 30, Wire.encode_fetch_range_req(req)))

      produce(ctx.producer, ctx.topic, ["meanwhile"])

      # the push comes while the fetch still waits, and the fetch is answered after its wait
      assert {20, {:records, page}} = decode_any(ctx.consumer)
      assert [{{0, 0}, "meanwhile"}] = page_records(page)
      {:ok, body} = TCPHelper.recv_frame(ctx.consumer, timeout: 5_000)
      assert {30, 0, payload} = Wire.decode_response(body)
      assert page_records(Wire.decode_page(payload)) == []
    end
  end

  describe "one connection, both kinds" do
    test "a connection appends on a producer stream and reads them back on a consume stream", ctx do
      socket = ctx.consumer
      {:ok, %{stream_id: consumer}} = open_consume(socket, ctx.topic)
      {:ok, %{stream_id: producer}} = open(socket, ctx.topic)
      assert producer != consumer

      send_append(socket, producer, 0, batch(["both"]))

      pushes = for _ <- 1..2, do: decode_any(socket)
      assert Enum.any?(pushes, &match?({10, {:append_ack, %{stream_id: ^producer, acked_sequence: 1}}}, &1))
      assert {20, {:records, page}} = Enum.find(pushes, &match?({20, _}, &1))
      assert page.stream_id == consumer and page_records(page) == [{{0, 0}, "both"}]
    end
  end

  # Reads pages until `count` reaches 10_000 records, however slowly they come.
  defp read_records(_socket, count) when count >= 10_000, do: count
  defp read_records(socket, count), do: read_records(socket, count + length(page_records(records_push(socket, 10_000))))

  defp unknown_stream?(socket, corr) do
    {:ok, body} = TCPHelper.recv_frame(socket)
    {^corr, 1, payload} = Wire.decode_response(body)
    Wire.decode_error_reason(payload) == "unknown_stream"
  end

  defp decode_any(socket) do
    {:ok, body} = TCPHelper.recv_frame(socket)
    {corr, 0, payload} = Wire.decode_response(body)
    {corr, Wire.decode_push(payload)}
  end
end
