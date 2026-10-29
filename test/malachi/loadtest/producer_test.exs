defmodule Malachi.Loadtest.ProducerTest do
  # The routing producer the load generator and the chaos checker stream through, against the real TCP
  # server: every record's outcome comes back as an event, lingering
  # joins records into one append, the window holds a closed loop back, and the routing survives what moves
  # a range (a split), stale routes, a refused open and a dropped connection.
  #
  # async: false. The topic routing reads the topology the consumer-group router publishes, which is cleared
  # for each test and put back.
  use ExUnit.Case, async: false

  import Malachi.Test.StreamTCP, only: [fetch_range: 3, page_records: 1]

  alias Malachi.BrokerServer
  alias Malachi.DataPlaneRouter
  alias Malachi.Loadtest.Conn
  alias Malachi.Loadtest.Producer
  alias Malachi.Loadtest.Router
  alias Malachi.Log.Record
  alias Malachi.TCPAcceptorPool
  alias Malachi.Test.PollingHelper
  alias Malachi.Test.RouterTopology
  alias Malachi.Test.StreamStub
  alias Malachi.Test.TCPHelper
  alias Malachi.Wire
  alias Malachi.Wire.Batch

  setup do
    :ok = RouterTopology.clear()

    topic = "producer_#{System.unique_integer([:positive])}"
    admin = connect!(user: "admin", pass: "admin123")
    {:ok, 0, _, admin} = Conn.request(admin, Wire.create_topic_key(), 2, Wire.encode_create_topic_req(topic, 4))
    {:ok, router, admin} = Router.bootstrap(Router.new({"127.0.0.1", TCPAcceptorPool.port()}), admin)
    Conn.close(admin)

    %{topic: topic, router: router}
  end

  defp conn_opts(opts), do: Keyword.merge([port: TCPAcceptorPool.port(), user: "app", pass: "app123"], opts)

  defp connect!(opts) do
    opts = conn_opts(opts)
    {:ok, conn} = Conn.connect(opts)
    {:ok, conn} = Conn.authenticate(conn, opts)
    conn
  end

  defp producer(ctx, opts \\ []) do
    {conn_opts, opts} = Keyword.split(opts, [:user, :pass])
    p = Producer.new(ctx.router, conn_opts(conn_opts), opts)
    on_exit(fn -> Producer.close(p) end)
    p
  end

  defp entries(values, key \\ & &1), do: Enum.map(values, &{&1, Record.new(&1, key: key.(&1))})

  defp now, do: System.monotonic_time(:millisecond)

  # A key whose position in a keyspace of 256 falls in `[low, high)`.
  defp key_within(low, high),
    do: Enum.find_value(0..10_000, &(:erlang.phash2("k#{&1}", 256) in low..(high - 1)//1 && "k#{&1}"))

  # A `Malachi.Test.StreamStub`, stopped when the test ends, and how a client reaches it.
  defp start_stub(overrides \\ %{}) do
    stub = StreamStub.start(overrides)
    on_exit(fn -> StreamStub.stop(stub) end)
    stub
  end

  defp stub_opts(stub), do: [port: stub.port, user: "u", pass: "p"]

  defp stub_router(stub) do
    {:ok, conn} = Conn.connect(stub_opts(stub))
    {:ok, conn} = Conn.authenticate(conn, stub_opts(stub))
    {:ok, router, conn} = Router.bootstrap(Router.new({"127.0.0.1", stub.port}), conn)
    Conn.close(conn)
    router
  end

  # An empty page pushed on consume stream `stream_id`: a push a producer never asked for.
  defp records_page(stream_id),
    do: %{
      stream_id: stream_id,
      next: {0, 0},
      skip: 0,
      backlog: 0,
      expired: 0,
      expired_exact: true,
      batch: Batch.encode([], :none)
    }

  # The id and correlation id of the stream `key`'s lane streams on.
  defp stream_of(p, key),
    do:
      Enum.find_value(p.streams, fn
        {{_address, id}, %{lane: ^key, corr: corr}} -> {id, corr}
        _ -> nil
      end)

  # The `moved` push a broker sends a stream it stopped, under the stream's correlation id.
  defp moved_frame(corr, stream_id) do
    push = %{stream_id: stream_id, reason: "split", routes_version: 0, targets: []}
    Wire.encode_ok(corr, IO.iodata_to_binary(Wire.encode_push(:moved, push)))
  end

  defp acked(events), do: for({:acked, entries, _latency} <- events, {tag, _record} <- entries, do: tag)
  defp failed(events), do: for({:failed, entries, reason} <- events, {tag, _record} <- entries, do: {tag, reason})

  # Hands `values` over and waits until each has its outcome.
  defp produce_all(p, topic, values, opts \\ []) do
    {p, early} = Producer.produce(p, topic, entries(values, Keyword.get(opts, :key, & &1)), now())
    {p, events, :idle} = Producer.await(p, 5_000)
    {p, early ++ events}
  end

  # Every value of `topic`'s `range` from the start, read on the stream keys.
  defp values(topic, range) do
    socket = TCPHelper.connect() |> elem(1)
    {:ok, _token} = TCPHelper.authenticate_wire(socket, "app", "app123")
    {:ok, page} = fetch_range(socket, topic, range: range, max: 1_000)
    :gen_tcp.close(socket)
    page |> page_records() |> Enum.map(&elem(&1, 1))
  end

  test "every record handed over is acknowledged, under its tag, and lands in its range", ctx do
    {p, events} = produce_all(producer(ctx), ctx.topic, ["a", "b", "c"])

    assert acked(events) == ["a", "b", "c"]
    assert failed(events) == []
    assert values(ctx.topic, 0) == ["a", "b", "c"]
    assert Producer.idle?(p)
    # the routes were not read before the first record: they are, once
    assert Producer.stats(p) == %{moved: 0, redirects: 0, refreshes: 1, dropped: 0, connect_failures: 0}
  end

  test "an acknowledgement carries the round trip it took", ctx do
    {_p, events} = produce_all(producer(ctx), ctx.topic, ["a"])
    assert [{:acked, [{"a", _}], latency_us}] = events
    assert latency_us > 0 and latency_us < 5_000_000
  end

  test "without lingering each hand-over is its own append; lingering joins them into one", ctx do
    p = producer(ctx)
    {p, []} = Producer.produce(p, ctx.topic, entries(["a"]), now())
    {p, []} = Producer.produce(p, ctx.topic, entries(["b"]), now())
    {_p, events, :idle} = Producer.await(p, 5_000)
    assert length(events) == 2

    lingering = producer(ctx, linger_ms: 60_000, linger_bytes: 3 * 24, max_inflight: 4)
    {lingering, []} = Producer.produce(lingering, ctx.topic, entries(["c"]), now())
    {lingering, []} = Producer.produce(lingering, ctx.topic, entries(["d"]), now())
    assert {lingering, [], :timeout} = Producer.await(lingering, 300)

    {lingering, []} = Producer.produce(lingering, ctx.topic, entries(["e"]), now())
    {_lingering, events, :idle} = Producer.await(lingering, 5_000)
    assert [{:acked, joined, _latency}] = events
    assert Enum.map(joined, &elem(&1, 0)) == ["c", "d", "e"]
  end

  test "a lingering record goes out once it has waited linger_ms, with no more records behind it", ctx do
    p = producer(ctx, linger_ms: 50, linger_bytes: 1_000_000)
    # the deadline runs only once the range's stream is open: an open still in flight waits on the broker
    {p, []} = Producer.produce(p, ctx.topic, entries(["opens"]), now())
    assert Producer.next_timeout(p, now()) == :infinity
    {p, [{:acked, _, _}], :idle} = Producer.await(p, 5_000)

    {p, []} = Producer.produce(p, ctx.topic, entries(["a"]), now())
    assert Producer.next_timeout(p, now()) in 1..50

    {_p, events, :idle} = Producer.await(p, 5_000)
    assert acked(events) == ["a"]
  end

  test "a full buffer behind a full window leaves no room, until the window opens", ctx do
    p = producer(ctx, max_inflight: 1, linger_bytes: 1)
    {p, []} = Producer.produce(p, ctx.topic, entries(["a"]), now())
    {p, []} = Producer.produce(p, ctx.topic, entries(["b"]), now())
    {p, []} = Producer.produce(p, ctx.topic, entries(["c"]), now())

    refute Producer.room?(p)
    refute Producer.idle?(p)

    {p, events, :idle} = Producer.await(p, 5_000)
    assert acked(events) == ["a", "b", "c"]
    assert Producer.room?(p)
  end

  test "a split moves the stream: what was in flight is answered, and the rest goes to the children", ctx do
    p = producer(ctx, max_inflight: 4)
    {p, _events} = produce_all(p, ctx.topic, ["before"])

    {:ok, {_, left}, {_, right}} = BrokerServer.split_range(DataPlaneRouter.shard_for(ctx.topic), {ctx.topic, 0})
    values = for i <- 1..20, do: "after-#{i}"
    {p, events} = produce_all(p, ctx.topic, values)

    assert Enum.sort(acked(events)) == Enum.sort(values)
    assert failed(events) == []
    assert %{moved: 1} = Producer.stats(p)
    # a child range reads its parent's history too, which holds the record written before the split
    assert Enum.sort(values(ctx.topic, left) ++ values(ctx.topic, right)) == Enum.sort(["before" | values])
  end

  test "an open against stale routes reads them again and opens where they say", ctx do
    stale =
      Router.put_routes(ctx.router, %{
        topic: ctx.topic,
        version: 1,
        keyspace_bits: 4,
        ranges: [
          %{range: 0, key_start: 0, key_end: 16, state: :active, segment: nil}
        ]
      })

    {p, events} = produce_all(producer(%{ctx | router: stale}), ctx.topic, ["a"])

    assert acked(events) == ["a"]
    assert %{redirects: 1, refreshes: 1} = Producer.stats(p)
  end

  test "records buffered for a range a split retired go where the routes read now send them", ctx do
    # The routes say range 0 is active, but it was split since: the open is refused (stale routes), the
    # routes read again no longer hold range 0, and the buffered records are routed to its children.
    old =
      Router.put_routes(ctx.router, %{
        topic: ctx.topic,
        version: 2,
        keyspace_bits: 4,
        ranges: [
          %{range: 0, key_start: 0, key_end: 16, state: :active, segment: nil}
        ]
      })

    {:ok, {_, left}, {_, right}} = BrokerServer.split_range(DataPlaneRouter.shard_for(ctx.topic), {ctx.topic, 0})
    values = for i <- 1..20, do: "v#{i}"
    {_p, events} = produce_all(producer(%{ctx | router: old}), ctx.topic, values)

    assert Enum.sort(acked(events)) == Enum.sort(values)
    assert Enum.sort(values(ctx.topic, left) ++ values(ctx.topic, right)) == Enum.sort(values)
  end

  test "an open refused for another reason fails the range's records with that reason", ctx do
    {p, events} = produce_all(producer(ctx, user: "consumer", pass: "consumer123"), ctx.topic, ["a", "b"])

    assert failed(events) == [{"a", "permission_denied"}, {"b", "permission_denied"}]
    assert acked(events) == []
    assert %{redirects: 0} = Producer.stats(p)
  end

  test "records for a topic the server does not know fail with its refusal", ctx do
    {_p, events} = produce_all(producer(ctx), "missing_#{System.unique_integer([:positive])}", ["a"])
    assert failed(events) == [{"a", "no_such_topic"}]
  end

  test "a dropped connection fails what was in flight, and the next records reconnect", ctx do
    p = producer(ctx)
    {p, _events} = produce_all(p, ctx.topic, ["a"])
    [{socket, _address}] = Map.to_list(p.sockets)

    # in flight when the connection drops: no outcome but that it may or may not have landed
    {p, []} = Producer.produce(p, ctx.topic, entries(["b"]), now())
    {:ok, p, events} = Producer.handle_message(p, {:tcp_closed, socket}, now())
    assert failed(events) == [{"b", "connection_lost"}]
    assert %{dropped: 1} = Producer.stats(p)

    {p, events} = produce_all(p, ctx.topic, ["c"])
    assert acked(events) == ["c"]
    refute Map.has_key?(p.sockets, socket)
  end

  test "an append carries records: a drained buffer sends nothing more, however much window is left", ctx do
    p = producer(ctx, max_inflight: 8)
    {p, events} = produce_all(p, ctx.topic, ["a"])

    assert [{:acked, [{"a", _}], _latency}] = events
    [stream] = Map.values(p.streams)
    assert stream.next_seq == 1
  end

  test "an append takes whole hand-overs: one never splits, however the bytes fall", ctx do
    # three hand-overs of 2 records (24 bytes each, 48 a hand-over): a limit of 125 fits two of them (96),
    # and five records (120), so taking records one by one would send a, b, c, d, e; the third hand-over
    # goes whole, once it has lingered
    p = producer(ctx, linger_ms: 300, linger_bytes: 125, max_inflight: 4)
    {p, _warm} = produce_all(p, ctx.topic, ["opens"])

    {p, []} = Producer.produce(p, ctx.topic, entries(["a", "b"]), now())
    {p, []} = Producer.produce(p, ctx.topic, entries(["c", "d"]), now())
    {p, []} = Producer.produce(p, ctx.topic, entries(["e", "f"]), now())
    {_p, events, :idle} = Producer.await(p, 5_000)

    assert [["a", "b", "c", "d"], ["e", "f"]] =
             for({:acked, joined, _} <- events, do: Enum.map(joined, &elem(&1, 0)))
  end

  test "two ranges retired by one read both reroute their records, neither lost to the other", ctx do
    {:ok, {_, left}, {_, right}} = BrokerServer.split_range(DataPlaneRouter.shard_for(ctx.topic), {ctx.topic, 0})
    {:ok, p} = Producer.prepare(producer(ctx, linger_ms: 300, linger_bytes: 1_000_000), ctx.topic, 5_000)

    # lingering, so they wait in both ranges' buffers
    values = for i <- 1..40, do: "w#{i}"
    {p, []} = Producer.produce(p, ctx.topic, entries(values), now())

    for range <- [left, right],
        do: {:ok, _, _} = BrokerServer.split_range(DataPlaneRouter.shard_for(ctx.topic), {ctx.topic, range})

    # both streams told they moved in one read, as a connection that took both pushes at once delivers them
    [{socket, _address}] = Map.to_list(p.sockets)
    frames = for {{_address, id}, %{corr: corr}} <- p.streams, into: <<>>, do: moved_frame(corr, id)
    assert map_size(p.streams) == 2
    {:ok, p, _events} = Producer.handle_message(p, {:tcp, socket, frames}, now())

    {_p, events, :idle} = Producer.await(p, 5_000)
    assert Enum.sort(acked(events)) == Enum.sort(values)
  end

  test "records buffered for two ranges retired at once all land", ctx do
    # both children of a split are split again while records are buffered for them: both lanes are
    # retired in one flush, and rerouting one must not lose sight of the other
    {:ok, {_, left}, {_, right}} = BrokerServer.split_range(DataPlaneRouter.shard_for(ctx.topic), {ctx.topic, 0})
    {:ok, p} = Producer.prepare(producer(ctx, max_inflight: 4), ctx.topic, 5_000)

    for range <- [left, right],
        do: {:ok, _, _} = BrokerServer.split_range(DataPlaneRouter.shard_for(ctx.topic), {ctx.topic, range})

    values = for i <- 1..40, do: "v#{i}"
    {p, events} = produce_all(p, ctx.topic, values)

    assert Enum.sort(acked(events)) == Enum.sort(values)
    assert failed(events) == []
    assert Producer.idle?(p)
  end

  test "a metadata connection the server closed is replaced, and the read made again", ctx do
    p = producer(ctx)
    {p, _events} = produce_all(p, ctx.topic, ["a"])
    assert %Conn{} = p.control
    # as the server closes an idle one, after MALACHI_TCP_RECV_TIMEOUT
    Conn.close(p.control)

    other = "producer_other_#{System.unique_integer([:positive])}"
    admin = connect!(user: "admin", pass: "admin123")
    {:ok, 0, _, admin} = Conn.request(admin, Wire.create_topic_key(), 2, Wire.encode_create_topic_req(other, 4))
    Conn.close(admin)

    {p, events} = produce_all(p, other, ["b"])
    assert acked(events) == ["b"]
    assert %{refreshes: 2} = Producer.stats(p)
  end

  test "a topic's backoff runs from when its read gave up, not from the instant it was asked at", ctx do
    p = producer(ctx)
    missing = "missing_#{System.unique_integer([:positive])}"

    # asked at an instant well behind the clock, as after a call that blocked: the backoff runs from when
    # the read gave up, which is no earlier than t0, so it is still on whenever the next ask comes
    t0 = now()
    {p, [{:failed, _, "no_such_topic"}]} = Producer.produce(p, missing, entries(["a"]), t0 - 1_000)
    {until, "no_such_topic"} = p.unroutable[missing]
    assert until >= t0 + 200

    {p, [{:failed, _, "no_such_topic"}]} = Producer.produce(p, missing, entries(["b"]), t0)
    assert %{refreshes: 1} = Producer.stats(p)
  end

  test "a lane its window holds waits for an ack, not for its linger deadline", ctx do
    p = producer(ctx, linger_ms: 20, max_inflight: 1, linger_bytes: 1)
    {p, _warm} = produce_all(p, ctx.topic, ["opens"])

    {p, []} = Producer.produce(p, ctx.topic, entries(["in flight"]), now())
    {p, []} = Producer.produce(p, ctx.topic, entries(["held"]), now())

    # well past the linger deadline (the producer learns of an ack only from its messages, unread here),
    # only an ack can let "held" go
    assert Producer.next_timeout(p, now() + 1_000) == :infinity

    {_p, events, :idle} = Producer.await(p, 5_000)
    assert acked(events) == ["in flight", "held"]
  end

  test "a refusal that names no append fails every append in flight on the stream, and retires it", ctx do
    p = producer(ctx, max_inflight: 2)
    {p, _warm} = produce_all(p, ctx.topic, ["opens"])

    # in flight as far as the producer knows: their acks wait unread in the mailbox
    {p, []} = Producer.produce(p, ctx.topic, entries(["a"]), now())
    {p, []} = Producer.produce(p, ctx.topic, entries(["b"]), now())
    [{_stream_key, %{corr: corr}}] = Map.to_list(p.streams)
    [{socket, _address}] = Map.to_list(p.sockets)

    # `unsupported` or `permission_denied` uses up no sequence: it cannot say which append it answers
    frame = Wire.encode_error(corr, :permission_denied)
    {:ok, p, events} = Producer.handle_message(p, {:tcp, socket, frame}, now())

    assert failed(events) == [{"a", "permission_denied"}, {"b", "permission_denied"}]
    assert p.streams == %{}
    assert Producer.idle?(p)
  end

  test "an append the byte window cannot take waits for room, rather than being refused", ctx do
    prior = Application.get_env(:malachi, :stream_max_window_bytes)
    on_exit(fn -> Application.put_env(:malachi, :stream_max_window_bytes, prior) end)
    # room for about two appends of one 300 byte record in flight, against a pipeline of 8
    Application.put_env(:malachi, :stream_max_window_bytes, 700)

    p = producer(ctx, max_inflight: 8)
    value = :binary.copy("x", 300)

    {p, []} =
      Enum.reduce(1..10, {p, []}, fn i, {p, []} -> Producer.produce(p, ctx.topic, [{i, Record.new(value)}], now()) end)

    {_p, events, :idle} = Producer.await(p, 5_000)

    assert failed(events) == []
    assert length(acked(events)) == 10
  end

  test "records resent after a move keep the order they were handed over in", ctx do
    # the split stops the stream while the producer has not read it yet: the appends it sends on the
    # stopped stream are answered unknown_stream, and go out again, in order, on the child
    p = producer(ctx, max_inflight: 4)
    {p, _warm} = produce_all(p, ctx.topic, ["opens"])
    {:ok, {_, left}, {_, right}} = BrokerServer.split_range(DataPlaneRouter.shard_for(ctx.topic), {ctx.topic, 0})

    values = for i <- 1..12, do: "seq-#{i}"
    # one key, so one child range; one hand-over per value, so one append each
    p =
      Enum.reduce(values, p, fn value, p ->
        p |> Producer.produce(ctx.topic, entries([value], fn _ -> "same" end), now()) |> elem(0)
      end)

    {_p, events, :idle} = Producer.await(p, 5_000)

    assert Enum.sort(acked(events)) == Enum.sort(values)
    read = Enum.filter(values(ctx.topic, left) ++ values(ctx.topic, right), &String.starts_with?(&1, "seq-"))
    assert read == values
  end

  test "an open whose send fails, on a connection closed under the producer, backs off and opens again", ctx do
    p = producer(ctx)
    {p, _events} = produce_all(p, ctx.topic, ["a"])
    {:ok, _, _} = BrokerServer.split_range(DataPlaneRouter.shard_for(ctx.topic), {ctx.topic, 0})

    # the move is read: the stream is retired, and its connection kept for the next open
    {p, _events, :idle} = Producer.await(p, 5_000, &(&1.streams == %{}))
    [{socket, _address}] = Map.to_list(p.sockets)
    # closed with nothing said, as one the server closed is before its close is read
    :ok = :gen_tcp.close(socket)

    {p, []} = Producer.produce(p, ctx.topic, entries(["b"]), now())
    assert {_p, events, :idle} = Producer.await(p, 2_000)
    assert acked(events) == ["b"]
  end

  test "across a split, what the retired range held goes out before what its children got since" do
    stub = start_stub()
    p = Producer.new(stub_router(stub), stub_opts(stub), max_inflight: 4)
    {:ok, p} = Producer.prepare(p, "t", 2_000)

    StreamStub.set_appends(stub, :silent)
    old = for i <- 1..4, do: "old-#{i}"
    p = Enum.reduce(old, p, &(&2 |> Producer.produce("t", entries([&1], fn _ -> "same" end), now()) |> elem(0)))

    # taken, and left unanswered, before anything else changes
    PollingHelper.wait_until!(fn -> length(StreamStub.appends(stub)) == 4 end)

    # the range splits: its stream is told it moved, with its four appends unanswered
    StreamStub.set_routes(stub, 2, [{1, 0, 128}, {2, 128, 256}])
    [{_stream_key, %{corr: corr}}] = Map.to_list(p.streams)
    StreamStub.push(stub, moved_frame(corr, 1))
    {p, [], :idle} = Producer.await(p, 2_000, &match?(%{stream: nil}, &1.lanes[{"t", 0}]))

    # handed over after the move, to a child: it waits for the retired stream's appends to be settled
    StreamStub.set_appends(stub, :ack)
    {p, []} = Producer.produce(p, "t", entries(["new"], fn _ -> "same" end), now())
    # time enough for a child that did not wait to open its stream and send "new"
    {p, early, _} = Producer.await(p, 300, fn p -> Enum.any?(p.streams, fn {_key, stream} -> stream.open? end) end)
    Process.sleep(100)

    # the four never reached the broker
    for _ <- old, do: StreamStub.push(stub, Wire.encode_error(corr, :unknown_stream))
    {_p, events, :idle} = Producer.await(p, 5_000)

    assert Enum.sort(acked(early ++ events)) == Enum.sort(["new" | old])
    assert for({id, _seq, values} <- StreamStub.appends(stub), id > 1, value <- values, do: value) == old ++ ["new"]
  end

  test "after stop_sending, an append answered unknown_stream is dropped rather than sent again" do
    stub = start_stub()
    {:ok, p} = stub |> stub_router() |> Producer.new(stub_opts(stub), max_inflight: 2) |> Producer.prepare("t", 2_000)

    StreamStub.set_appends(stub, :silent)
    p = Enum.reduce(["a", "b"], p, &(&2 |> Producer.produce("t", entries([&1]), now()) |> elem(0)))
    [{_stream_key, %{corr: corr}}] = Map.to_list(p.streams)
    PollingHelper.wait_until!(fn -> length(StreamStub.appends(stub)) == 2 end)

    p = Producer.stop_sending(p)
    StreamStub.push(stub, moved_frame(corr, 1))
    for _ <- 1..2, do: StreamStub.push(stub, Wire.encode_error(corr, :unknown_stream))
    {p, [], :idle} = Producer.await(p, 2_000, &(not Producer.in_flight?(&1)))

    assert p.opening == %{}
    assert Producer.idle?(p)
    assert length(StreamStub.appends(stub)) == 2
  end

  # `not_primary` and `sealed` are the broker's answers to an append it stored nothing of because its range
  # is written elsewhere now (a roll placed the successor on another node): routing, sent again on a new
  # stream, never a failure.
  for reason <- ["not_primary", "sealed"] do
    test "an append refused #{reason} goes out again on the next stream, acknowledged once, with no failure" do
      # the first append is refused, every later one taken
      stub = start_stub(%{27 => {:after, 1, {:refuse_append, unquote(reason)}, :default}})
      {:ok, p} = stub |> stub_router() |> Producer.new(stub_opts(stub), max_inflight: 4) |> Producer.prepare("t", 2_000)

      {p, []} = Producer.produce(p, "t", entries(["a"]), now())
      {p, events, :idle} = Producer.await(p, 5_000)
      {p, []} = Producer.produce(p, "t", entries(["b"]), now())
      {p, more, :idle} = Producer.await(p, 5_000)

      assert acked(events ++ more) == ["a", "b"]
      assert failed(events ++ more) == []
      # sent twice, on the first stream and then on the one opened after it, and the stream counted redirected
      assert [{1, 0, ["a"]}, {2, 0, ["a"]}, {2, 1, ["b"]}] = StreamStub.appends(stub)
      assert StreamStub.opened(stub) == 2
      assert Producer.stats(p).redirects == 1
    end
  end

  test "routing refusals on several appends in flight resend them in the order they were sent, after the move" do
    stub = start_stub(%{27 => {:after, 3, {:refuse_append, "not_primary"}, :default}})
    {:ok, p} = stub |> stub_router() |> Producer.new(stub_opts(stub), max_inflight: 4) |> Producer.prepare("t", 2_000)

    StreamStub.set_appends(stub, :silent)
    p = Enum.reduce(["a", "b", "c"], p, &(&2 |> Producer.produce("t", entries([&1]), now()) |> elem(0)))
    [{_stream_key, %{corr: corr}}] = Map.to_list(p.streams)
    PollingHelper.wait_until!(fn -> length(StreamStub.appends(stub)) == 3 end)

    # the broker's move first, then each append refused, as a broker sends them
    StreamStub.set_appends(stub, :ack)
    StreamStub.push(stub, moved_frame(corr, 1))

    for seq <- 0..2 do
      push = %{
        stream_id: 1,
        acked_sequence: seq + 1,
        window_appends: 64,
        window_bytes: 16_777_216,
        errors: [%{sequence: seq, reason: "not_primary"}]
      }

      StreamStub.push(stub, Wire.encode_ok(corr, IO.iodata_to_binary(Wire.encode_push(:append_ack, push))))
    end

    {_p, events, :idle} = Producer.await(p, 5_000)

    assert acked(events) == ["a", "b", "c"]
    assert failed(events) == []
    assert [{2, 0, ["a"]}, {2, 1, ["b"]}, {2, 2, ["c"]}] = Enum.drop(StreamStub.appends(stub), 3)
  end

  test "an append refused for a reason that is not routing still fails, and is not sent again" do
    stub = start_stub(%{27 => {:after, 1, {:refuse_append, "no_quorum"}, :default}})
    {:ok, p} = stub |> stub_router() |> Producer.new(stub_opts(stub)) |> Producer.prepare("t", 2_000)

    {p, []} = Producer.produce(p, "t", entries(["a"]), now())
    {_p, events, :idle} = Producer.await(p, 5_000)

    assert failed(events) == [{"a", "no_quorum"}]
    assert acked(events) == []
    assert [{1, 0, ["a"]}] = StreamStub.appends(stub)
  end

  test "a topic whose routes cannot be read is not asked again until its backoff ends", ctx do
    p = producer(ctx)
    missing = "missing_#{System.unique_integer([:positive])}"

    # one instant for both, so the round trip the first one makes does not count against the backoff
    t = now()
    {p, [{:failed, _, "no_such_topic"}]} = Producer.produce(p, missing, entries(["a"]), t)
    {p, [{:failed, _, "no_such_topic"}]} = Producer.produce(p, missing, entries(["b"]), t)
    assert %{refreshes: 1} = Producer.stats(p)

    # once the backoff is over, they are asked again
    {p, [{:failed, _, "no_such_topic"}]} = Producer.produce(p, missing, entries(["c"]), t + 1_000)
    assert %{refreshes: 2} = Producer.stats(p)
  end

  test "a lane the byte window holds waits for an ack, not for its linger deadline", ctx do
    prior = Application.get_env(:malachi, :stream_max_window_bytes)
    on_exit(fn -> Application.put_env(:malachi, :stream_max_window_bytes, prior) end)
    Application.put_env(:malachi, :stream_max_window_bytes, 700)

    p = producer(ctx, linger_ms: 20, max_inflight: 8, linger_bytes: 1)
    {p, _warm} = produce_all(p, ctx.topic, ["opens"])
    value = :binary.copy("x", 300)

    {p, []} =
      Enum.reduce(1..3, {p, []}, fn i, {p, []} -> Producer.produce(p, ctx.topic, [{i, Record.new(value)}], now()) end)

    # room in the pipeline, none in the bytes: past the linger deadline, only an ack lets the last one go
    assert Producer.next_timeout(p, now() + 1_000) == :infinity

    {_p, events, :idle} = Producer.await(p, 5_000)
    assert Enum.sort(acked(events)) == [1, 2, 3]
  end

  test "a range redirected time after time backs off from its fifth redirect" do
    stub = start_stub(%{26 => {:error, "moved"}})
    p = Producer.new(stub_router(stub), stub_opts(stub))

    {p, []} = Producer.produce(p, "t", entries(["a"]), now())

    # four redirects in a row: the range opens again at once
    {p, [], :idle} = Producer.await(p, 2_000, &(Producer.stats(&1).redirects == 4))
    assert %{retry_at: nil} = p.lanes[{"t", 0}]
    assert map_size(p.opening) == 1

    {p, [], :idle} = Producer.await(p, 2_000, &(Producer.stats(&1).redirects == 5))

    assert %{retry_at: retry_at, stream: nil} = p.lanes[{"t", 0}]
    assert is_integer(retry_at)
    assert p.opening == %{}
  end

  describe "edges" do
    test "handing over no records sends only what was already due" do
      p = producer(%{router: Router.new({"127.0.0.1", 1})})
      assert {^p, []} = Producer.produce(p, "t", [], now())
    end

    test "a record no active range covers is read again once, then fails as no_range" do
      stub = start_stub()
      StreamStub.set_routes(stub, 1, [{0, 0, 128}])
      gap = Enum.find_value(0..10_000, &(:erlang.phash2("k#{&1}", 256) >= 128 && "k#{&1}"))
      p = Producer.new(stub_router(stub), stub_opts(stub))

      {p, [{:failed, [{"x", _}], "no_range"}]} = Producer.produce(p, "t", entries(["x"], fn _ -> gap end), now())
      # routes read for the topic, then once more for the record without a range
      assert %{refreshes: 2} = Producer.stats(p)

      StreamStub.set_override(stub, 25, {:error, "no_such_topic"})

      assert {_p, [{:failed, [{"y", _}], "no_such_topic"}]} =
               Producer.produce(p, "t", entries(["y"], fn _ -> gap end), now())
    end

    test "records waiting for a range whose routes can no longer be read fail with the refusal" do
      stub = start_stub()
      p = Producer.new(stub_router(stub), stub_opts(stub), linger_ms: 60_000, linger_bytes: 1_000_000)
      {:ok, p} = Producer.prepare(p, "t", 2_000)
      {p, []} = Producer.produce(p, "t", entries(["a"]), now())
      [{_stream_key, %{corr: corr}}] = Map.to_list(p.streams)

      StreamStub.set_override(stub, 25, {:error, "no_such_topic"})
      StreamStub.push(stub, moved_frame(corr, 1))
      {_p, events, _} = Producer.await(p, 2_000, &match?(%{buffer_bytes: 0}, &1.lanes[{"t", 0}]))

      assert failed(events) == [{"a", "no_such_topic"}]
    end

    test "a primary that advertises no address, in a cluster of several, is a redirect: its records wait" do
      router =
        Router.new({"127.0.0.1", 1})
        |> Router.put_cluster_state(%{
          brokers: [
            %{id: "a@h", host: nil, port: 0, status: :alive},
            %{id: "b@h", host: "10.0.0.2", port: 1, status: :alive}
          ],
          streams_enabled: true
        })
        |> Router.put_routes(%{
          topic: "t",
          version: 1,
          keyspace_bits: 8,
          ranges: [%{range: 0, key_start: 0, key_end: 256, state: :active, segment: %{segment: 0, primary: "a@h"}}]
        })

      # the routes are forgotten, to be read again; the record is neither sent nor failed
      assert {p, []} = Producer.produce(Producer.new(router, []), "t", entries(["a"]), now())
      assert %{redirects: 1} = Producer.stats(p)
      assert Router.routes(p.router, "t") == nil
    end

    test "a broker that refuses the connection, or its credentials, is retried after a backoff", ctx do
      # routes already read, so the only connection tried is the broker's
      {:ok, good} = Producer.prepare(producer(ctx), ctx.topic, 2_000)
      p = Producer.new(good.router, conn_opts(pass: "wrong"))
      # handed over at an instant well behind the clock, as after a call that blocked: the backoff still
      # runs from when the connection gave up, which is no earlier than t0
      t0 = now()
      {p, []} = Producer.produce(p, ctx.topic, entries(["a"]), t0 - 1_000)

      assert %{connect_failures: 1} = Producer.stats(p)
      assert %{retry_at: retry_at, stream: nil} = p.lanes[{ctx.topic, 0}]
      assert retry_at >= t0 + 200
      assert Producer.next_timeout(p, t0) >= 200
    end

    test "metadata no node answers fails the records as metadata_unavailable" do
      {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false])
      {:ok, port} = :inet.port(listen)
      :ok = :gen_tcp.close(listen)

      p = Producer.new(Router.new({"127.0.0.1", port}), port: port, user: "u", pass: "p")
      assert {_p, [{:failed, [{"a", _}], "metadata_unavailable"}]} = Producer.produce(p, "t", entries(["a"]), now())
    end

    test "an append whose send fails goes back to the buffer and out again on the next connection", ctx do
      p = producer(ctx)
      {p, _warm} = produce_all(p, ctx.topic, ["a"])
      [{socket, _address}] = Map.to_list(p.sockets)
      :ok = :gen_tcp.close(socket)

      {p, []} = Producer.produce(p, ctx.topic, entries(["b"]), now())
      assert %{dropped: 1} = Producer.stats(p)
      {_p, events, :idle} = Producer.await(p, 2_000)
      assert acked(events) == ["b"]
    end

    test "a connection the broker closes while appends are in flight fails them as connection_lost" do
      stub = start_stub(%{27 => :close})
      {:ok, p} = stub |> stub_router() |> Producer.new(stub_opts(stub)) |> Producer.prepare("t", 2_000)
      {p, []} = Producer.produce(p, "t", entries(["a"]), now())

      {p, events, _} = Producer.await(p, 2_000, &(Producer.stats(&1).dropped == 1))
      assert failed(events) == [{"a", "connection_lost"}]
      assert %{dropped: 1} = Producer.stats(p)
    end

    test "an activation that fails after a read is the connection dropped", ctx do
      p = producer(ctx)
      {p, _warm} = produce_all(p, ctx.topic, ["a"])
      [{socket, _address}] = Map.to_list(p.sockets)
      :ok = :gen_tcp.close(socket)

      {:ok, p, []} = Producer.handle_message(p, {:tcp, socket, <<>>}, now())
      assert %{dropped: 1} = Producer.stats(p)
    end

    test "frames for nothing the producer holds, or of a kind it does not take, change nothing", ctx do
      p = producer(ctx)
      {p, _warm} = produce_all(p, ctx.topic, ["a"])
      [{socket, _address}] = Map.to_list(p.sockets)
      ack = %{stream_id: 999, acked_sequence: 1, window_appends: 1, window_bytes: 1, errors: []}

      frames =
        Wire.encode_ok(7, IO.iodata_to_binary(Wire.encode_push(:append_ack, ack))) <>
          Wire.encode_ok(
            7,
            IO.iodata_to_binary(
              Wire.encode_push(:moved, %{stream_id: 999, reason: "x", routes_version: 0, targets: []})
            )
          ) <>
          Wire.encode_error(7, :unknown_stream) <>
          Wire.encode_ok(7, IO.iodata_to_binary(Wire.encode_push(:records, records_page(999))))

      assert {:ok, ^p, []} = Producer.handle_message(p, {:tcp, socket, frames}, now())
      assert Producer.handle_message(p, {:tcp_passive, socket}, now()) == :no
    end

    test "prepare leaves a range whose open is already on its way to that open", ctx do
      p = producer(ctx)
      {p, []} = Producer.produce(p, ctx.topic, entries(["a"]), now())
      assert map_size(p.opening) == 1
      assert {:ok, _p} = Producer.prepare(p, ctx.topic, 2_000)
    end
  end

  test "a record counts what it takes in a batch, which is what the broker holds against the window" do
    records = [
      Record.new("v"),
      Record.new(:binary.copy("x", 300), key: "some-key"),
      %{Record.new("with headers", key: "k") | headers: [{"h1", "one"}, {"h2", ""}]}
    ]

    {%{inflated_size: inflated}, <<>>} = records |> Enum.map(&{&1, false}) |> Batch.encode(:none) |> Batch.split()
    p = Producer.new(Router.new({"127.0.0.1", 1}), [], linger_ms: 60_000, linger_bytes: 1_000_000)

    p = %{
      p
      | router:
          Router.put_routes(p.router, %{
            topic: "t",
            version: 1,
            keyspace_bits: 8,
            ranges: [%{range: 0, key_start: 0, key_end: 256, state: :active, segment: nil}]
          })
    }

    {p, _} = Producer.produce(p, "t", Enum.map(records, &{nil, &1}), now())
    assert p.lanes[{"t", 0}].buffer_bytes == inflated
    {%{inflated_size: one}, <<>>} = Batch.split(Batch.encode([{Enum.at(records, 1), false}], :none))
    assert Producer.record_bytes(300, 8) == one
  end

  test "a range whose stream is gone while another range keeps appends in flight opens again at once" do
    stub = start_stub()
    StreamStub.set_routes(stub, 1, [{0, 0, 128}, {1, 128, 256}])
    {:ok, p} = stub |> stub_router() |> Producer.new(stub_opts(stub), max_inflight: 4) |> Producer.prepare("t", 2_000)
    [low, high] = Enum.map([{0, 128}, {128, 256}], fn {lo, hi} -> key_within(lo, hi) end)

    # range 1 has an append in flight that is never answered
    StreamStub.set_appends(stub, :silent)
    {p, []} = Producer.produce(p, "t", entries(["held"], fn _ -> high end), now())
    PollingHelper.wait_until!(fn -> length(StreamStub.appends(stub)) == 1 end)
    StreamStub.set_appends(stub, :ack)

    # range 0's stream is told it moved (a roll): nothing of its own in flight, so it opens again
    stream0 =
      Enum.find_value(p.streams, fn
        {{_a, id}, %{lane: {"t", 0}}} -> id
        _ -> nil
      end)

    corr0 =
      p.streams
      |> Enum.find_value(fn
        {_k, %{lane: {"t", 0}, corr: corr}} -> corr
        _ -> nil
      end)

    StreamStub.push(stub, moved_frame(corr0, stream0))
    {p, [], :idle} = Producer.await(p, 2_000, &match?(%{stream: nil}, &1.lanes[{"t", 0}]))

    {p, []} = Producer.produce(p, "t", entries(["low"], fn _ -> low end), now())

    assert {_p, events, :idle} =
             Producer.await(p, 2_000, fn p ->
               not Enum.any?(p.streams, fn {_k, s} -> s.lane == {"t", 0} and s.inflight != %{} end) and
                 p.lanes[{"t", 0}].buffer_bytes == 0 and p.opening == %{}
             end)

    assert acked(events) == ["low"]
  end

  test "a lane the drain holds keeps no stale backoff, so nothing ticks until the drain's answer" do
    stub = start_stub()
    {:ok, p} = stub |> stub_router() |> Producer.new(stub_opts(stub), max_inflight: 2) |> Producer.prepare("t", 2_000)
    StreamStub.set_appends(stub, :silent)
    {p, []} = Producer.produce(p, "t", entries(["a"]), now())
    PollingHelper.wait_until!(fn -> length(StreamStub.appends(stub)) == 1 end)
    [{{_address, id}, %{corr: corr}}] = Map.to_list(p.streams)

    # the stream is retired with "a" unanswered; a record waits behind it, with a backoff already over
    StreamStub.push(stub, moved_frame(corr, id))
    {p, [], :idle} = Producer.await(p, 2_000, &match?(%{stream: nil}, &1.lanes[{"t", 0}]))
    {p, []} = Producer.produce(p, "t", entries(["b"]), now())
    # the backoff is written in: reaching a lane that both drains and backs off through the producer's own
    # paths takes a second connection whose drop leaves the draining stream alone
    p = %{p | lanes: Map.update!(p.lanes, {"t", 0}, &%{&1 | retry_at: now() - 1})}

    {p, []} = Producer.tick(p, now())
    assert Producer.next_timeout(p, now()) == :infinity

    # the drain's answer: "a" landed, the stream is gone, and the range opens again with "b"
    StreamStub.set_appends(stub, :ack)
    ack = %{stream_id: id, acked_sequence: 1, window_appends: 2, window_bytes: 16_777_216, errors: []}
    StreamStub.push(stub, Wire.encode_ok(corr, IO.iodata_to_binary(Wire.encode_push(:append_ack, ack))))
    {_p, events, :idle} = Producer.await(p, 2_000)
    assert acked(events) == ["a", "b"]
  end

  test "an error the broker names at or above acked_sequence ends the stream there: that append fails now" do
    # A sequence the broker refused out of order ends the stream at it: its error comes in an ack that
    # does not pass it, since the appends before it are still answered (`Malachi.Wire`). The producer left
    # it in flight for good, and the range never opened another stream.
    stub = start_stub()
    {:ok, p} = stub |> stub_router() |> Producer.new(stub_opts(stub), max_inflight: 2) |> Producer.prepare("t", 2_000)
    StreamStub.set_appends(stub, :silent)
    p = Enum.reduce(["a", "b"], p, &(&2 |> Producer.produce("t", entries([&1]), now()) |> elem(0)))
    PollingHelper.wait_until!(fn -> length(StreamStub.appends(stub)) == 2 end)
    [{{_address, id}, %{corr: corr}}] = Map.to_list(p.streams)

    # "b" (sequence 1) refused out of order, "a" (sequence 0) still to be answered
    ended = %{
      stream_id: id,
      acked_sequence: 0,
      window_appends: 2,
      window_bytes: 16_777_216,
      errors: [%{sequence: 1, reason: "sequence_gap"}]
    }

    StreamStub.push(stub, Wire.encode_ok(corr, IO.iodata_to_binary(Wire.encode_push(:append_ack, ended))))
    {p, events, _} = Producer.await(p, 2_000, &match?(%{stream: nil}, &1.lanes[{"t", 0}]))
    assert [{:failed, [{"b", _}], "sequence_gap"}] = events

    # "a" answered: the stream is drained, and the range opens a new one for what comes next
    StreamStub.set_appends(stub, :ack)
    answered = %{ended | acked_sequence: 1, errors: []}
    StreamStub.push(stub, Wire.encode_ok(corr, IO.iodata_to_binary(Wire.encode_push(:append_ack, answered))))
    {p, []} = Producer.produce(p, "t", entries(["c"]), now())
    {_p, events, :idle} = Producer.await(p, 2_000)
    assert acked(events) == ["a", "c"]
    assert [{^id, 0, ["a"]}, {^id, 1, ["b"]}, {other, 0, ["c"]}] = StreamStub.appends(stub)
    assert other != id
  end

  test "a repeat the broker names ends the stream, but the append it names is answered by its original" do
    # A repeat is a second copy of a sequence whose original may still land: failing it would have the
    # caller send it once more, a duplicate if the original was stored.
    stub = start_stub()
    {:ok, p} = stub |> stub_router() |> Producer.new(stub_opts(stub), max_inflight: 2) |> Producer.prepare("t", 2_000)
    StreamStub.set_appends(stub, :silent)
    p = Enum.reduce(["a", "b"], p, &(&2 |> Producer.produce("t", entries([&1]), now()) |> elem(0)))
    PollingHelper.wait_until!(fn -> length(StreamStub.appends(stub)) == 2 end)
    [{{_address, id}, %{corr: corr}}] = Map.to_list(p.streams)

    repeat = %{
      stream_id: id,
      acked_sequence: 0,
      window_appends: 2,
      window_bytes: 16_777_216,
      errors: [%{sequence: 1, reason: "sequence_repeat"}]
    }

    StreamStub.push(stub, Wire.encode_ok(corr, IO.iodata_to_binary(Wire.encode_push(:append_ack, repeat))))
    {p, events, _} = Producer.await(p, 2_000, &match?(%{stream: nil}, &1.lanes[{"t", 0}]))
    assert events == []

    # both originals land: both acknowledged, and the range streams on through a new stream
    StreamStub.set_appends(stub, :ack)
    landed = %{repeat | acked_sequence: 2, errors: []}
    StreamStub.push(stub, Wire.encode_ok(corr, IO.iodata_to_binary(Wire.encode_push(:append_ack, landed))))
    {p, []} = Producer.produce(p, "t", entries(["c"]), now())
    {_p, events, :idle} = Producer.await(p, 2_000)
    assert acked(events) == ["a", "b", "c"]
    assert [{^id, 0, ["a"]}, {^id, 1, ["b"]}, {other, 0, ["c"]}] = StreamStub.appends(stub)
    assert other != id
  end

  test "across a split, a parent range in backoff hands its records over before its children send" do
    stub = start_stub()
    p = Producer.new(stub_router(stub), stub_opts(stub), linger_ms: 100, linger_bytes: 1_000_000)
    {:ok, p} = Producer.prepare(p, "t", 2_000)

    old = for i <- 1..3, do: "old-#{i}"
    {p, []} = Producer.produce(p, "t", entries(old, fn _ -> "same" end), now())

    # its connection drops with the records still lingering: the range backs off, then the range splits
    [{socket, _address}] = Map.to_list(p.sockets)
    {:ok, p, []} = Producer.handle_message(p, {:tcp_closed, socket}, now())
    StreamStub.set_routes(stub, 2, [{1, 0, 128}, {2, 128, 256}])

    {p, []} = Producer.produce(p, "t", entries(["new"], fn _ -> "same" end), now())
    {_p, events, :idle} = Producer.await(p, 2_000)

    assert Enum.sort(acked(events)) == Enum.sort(["new" | old])
    assert for({id, _seq, values} <- StreamStub.appends(stub), id > 1, value <- values, do: value) == old ++ ["new"]
  end

  test "records rerouted from a retired range by routes read mid-split are read again once, not failed" do
    stub = start_stub()
    p = Producer.new(stub_router(stub), stub_opts(stub), linger_ms: 300, linger_bytes: 1_000_000)
    {:ok, p} = Producer.prepare(p, "t", 2_000)
    {high, low} = {key_within(128, 256), key_within(0, 128)}
    {p, []} = Producer.produce(p, "t", entries(["old"], fn _ -> high end), now())

    # the range backs off after a drop, and splits; the next read catches the split half done
    [{socket, _address}] = Map.to_list(p.sockets)
    {:ok, p, []} = Producer.handle_message(p, {:tcp_closed, socket}, now())
    StreamStub.set_routes_sequence(stub, [{2, [{1, 0, 128}]}, {3, [{1, 0, 128}, {2, 128, 256}]}])

    {p, []} = Producer.produce(p, "t", entries(["new"], fn _ -> low end), now())
    {_p, events, :idle} = Producer.await(p, 2_000)

    assert failed(events) == []
    assert Enum.sort(acked(events)) == ["new", "old"]
  end

  test "across a merge, a parent not yet told it moved holds its keys' new range until it is" do
    stub = start_stub()
    StreamStub.set_routes(stub, 1, [{0, 0, 128}, {1, 128, 256}])
    {:ok, p} = stub |> stub_router() |> Producer.new(stub_opts(stub), max_inflight: 4) |> Producer.prepare("t", 2_000)
    high = key_within(128, 256)
    [{id0, corr0}, {id1, corr1}] = for range <- [0, 1], do: stream_of(p, {"t", range})

    # range 1 has "old" unanswered when ranges 0 and 1 merge into range 2
    StreamStub.set_appends(stub, :silent)
    {p, []} = Producer.produce(p, "t", entries(["old"], fn _ -> high end), now())
    PollingHelper.wait_until!(fn -> length(StreamStub.appends(stub)) == 1 end)
    StreamStub.set_routes(stub, 2, [{2, 0, 256}])
    StreamStub.set_appends(stub, :ack)

    # range 0 is told first; range 1, on another node say, a reconcile later
    StreamStub.push(stub, moved_frame(corr0, id0))
    {p, [], :idle} = Producer.await(p, 2_000, &match?(%{stream: nil}, &1.lanes[{"t", 0}]))

    # "new" takes range 1's keys into range 2, which must not send ahead of range 1's records
    {p, []} = Producer.produce(p, "t", entries(["new"], fn _ -> high end), now())
    {p, early, _} = Producer.await(p, 300, fn p -> Enum.any?(p.streams, fn {_k, s} -> s.lane == {"t", 2} end) end)
    refute Enum.any?(p.streams, fn {_k, s} -> s.lane == {"t", 2} end)
    assert StreamStub.opened(stub) == 2

    StreamStub.push(stub, moved_frame(corr1, id1))
    StreamStub.push(stub, Wire.encode_error(corr1, :unknown_stream))
    {_p, events, :idle} = Producer.await(p, 5_000)

    assert Enum.sort(acked(early ++ events)) == ["new", "old"]
    assert for({id, _seq, values} <- StreamStub.appends(stub), id > 2, value <- values, do: value) == ["old", "new"]
  end

  test "a move elsewhere in the topic, which forgets its routes, does not release a merge's hold" do
    stub = start_stub()
    StreamStub.set_routes(stub, 1, [{0, 0, 64}, {1, 64, 128}, {3, 128, 256}])
    {:ok, p} = stub |> stub_router() |> Producer.new(stub_opts(stub), max_inflight: 4) |> Producer.prepare("t", 2_000)
    key = key_within(64, 128)
    [{id0, corr0}, {id1, corr1}, {id3, corr3}] = for range <- [0, 1, 3], do: stream_of(p, {"t", range})

    # range 1 has "old" unanswered when ranges 0 and 1 merge into 2; range 3 is untouched
    StreamStub.set_appends(stub, :silent)
    {p, []} = Producer.produce(p, "t", entries(["old"], fn _ -> key end), now())
    PollingHelper.wait_until!(fn -> length(StreamStub.appends(stub)) == 1 end)
    StreamStub.set_routes(stub, 2, [{2, 0, 128}, {3, 128, 256}])
    StreamStub.set_appends(stub, :ack)

    StreamStub.push(stub, moved_frame(corr0, id0))
    {p, [], :idle} = Producer.await(p, 2_000, &match?(%{stream: nil}, &1.lanes[{"t", 0}]))
    {p, []} = Producer.produce(p, "t", entries(["new"], fn _ -> key end), now())

    # range 3 rolls: its move forgets the topic's routes, while range 1 has still not been told
    StreamStub.push(stub, moved_frame(corr3, id3))
    {p, early, _} = Producer.await(p, 300, fn p -> Enum.any?(p.streams, fn {_k, s} -> s.lane == {"t", 2} end) end)
    refute Enum.any?(p.streams, fn {_k, s} -> s.lane == {"t", 2} end)

    StreamStub.push(stub, moved_frame(corr1, id1))
    StreamStub.push(stub, Wire.encode_error(corr1, :unknown_stream))
    {_p, events, :idle} = Producer.await(p, 5_000)

    assert Enum.sort(acked(early ++ events)) == ["new", "old"]
    assert for({id, _seq, values} <- StreamStub.appends(stub), id > 3, value <- values, do: value) == ["old", "new"]
  end

  test "records of a retired range whose routes can no longer be read fail with the refusal" do
    stub = start_stub()
    p = Producer.new(stub_router(stub), stub_opts(stub), linger_ms: 60_000, linger_bytes: 1_000_000)
    {:ok, p} = Producer.prepare(p, "t", 2_000)
    [{id, corr}] = [stream_of(p, {"t", 0})]
    {p, []} = Producer.produce(p, "t", entries(["old"], fn _ -> key_within(0, 128) end), now())

    # the range splits; with the routes forgotten (as a move elsewhere in the topic forgets them), a record
    # for the other half reads the new ones, which retire the range while its stream is still open (its
    # move has not come), so it holds its records
    StreamStub.set_routes(stub, 2, [{1, 0, 128}, {2, 128, 256}])
    p = %{p | router: Router.forget(p.router, "t")}
    {p, []} = Producer.produce(p, "t", entries(["x"], fn _ -> key_within(128, 256) end), now())
    assert %{stream: {:open, _}} = p.lanes[{"t", 0}]
    assert Router.version(p.router, "t") == 2

    # its move comes, forgetting the routes, when they can no longer be read
    StreamStub.set_override(stub, 25, {:error, "no_such_topic"})
    %{refreshes: before} = Producer.stats(p)
    StreamStub.push(stub, moved_frame(corr, id))
    # the range's records are rerouted (its lane goes), and the read that would place them fails
    assert {p, events, :idle} = Producer.await(p, 2_000, &(not Map.has_key?(&1.lanes, {"t", 0})))
    refute Map.has_key?(p.lanes, {"t", 0})
    assert {"old", "no_such_topic"} in failed(events)

    # the reroute's failed read set the topic's backoff: the other range, opening in the same flush, took
    # its refusal without reading again
    assert {"x", "no_such_topic"} in failed(events)
    assert Producer.stats(p).refreshes == before + 1
  end

  test "while a topic's routes are backing off, no path reads them again" do
    stub = start_stub()
    StreamStub.set_routes(stub, 1, [{0, 0, 128}, {1, 128, 256}])
    p = Producer.new(stub_router(stub), stub_opts(stub), linger_ms: 60_000, linger_bytes: 1_000_000)
    {:ok, p} = Producer.prepare(p, "t", 2_000)

    {p, []} =
      Producer.produce(
        p,
        "t",
        entries(["a", "b"], &if(&1 == "a", do: key_within(0, 128), else: key_within(128, 256))),
        now()
      )

    # the connection drops with both ranges holding records, and the routes can no longer be read
    [{socket, _address}] = Map.to_list(p.sockets)
    {:ok, p, []} = Producer.handle_message(p, {:tcp_closed, socket}, now())
    StreamStub.set_override(stub, 25, {:error, "no_such_topic"})
    %{refreshes: before} = Producer.stats(p)

    # once both backoffs are over, a hand-over finds the routes refused; the two ranges that open in the
    # same call take that refusal rather than asking again
    {p, events} = Producer.produce(p, "t", entries(["c"], fn _ -> key_within(0, 128) end), now() + 1_000)

    assert Producer.stats(p).refreshes == before + 1
    assert Enum.sort(failed(events)) == [{"a", "no_such_topic"}, {"b", "no_such_topic"}, {"c", "no_such_topic"}]
  end

  describe "prepare/3" do
    test "opens a stream on every active range before any record", ctx do
      {:ok, _l, _r} = BrokerServer.split_range(DataPlaneRouter.shard_for(ctx.topic), {ctx.topic, 0})

      assert {:ok, p} = Producer.prepare(producer(ctx), ctx.topic, 5_000)
      assert map_size(p.streams) == 2
      assert Enum.all?(p.lanes, fn {_key, lane} -> match?({:open, _}, lane.stream) end)

      # and the records then go straight out on them
      {_p, events} = produce_all(p, ctx.topic, for(i <- 1..10, do: "v#{i}"))
      assert length(acked(events)) == 10
    end

    test "a range whose open is redirected is opened where the routes then say", ctx do
      stale =
        Router.put_routes(ctx.router, %{
          topic: ctx.topic,
          version: 1,
          keyspace_bits: 4,
          ranges: [
            %{range: 0, key_start: 0, key_end: 16, state: :active, segment: nil}
          ]
        })

      assert {:ok, p} = Producer.prepare(producer(%{ctx | router: stale}), ctx.topic, 5_000)
      assert %{redirects: 1} = Producer.stats(p)
      assert map_size(p.streams) == 1
    end

    test "a range whose open is refused names the refusal once the time is up", ctx do
      assert {:error, "permission_denied", _p} =
               Producer.prepare(producer(ctx, user: "consumer", pass: "consumer123"), ctx.topic, 300)
    end

    test "an open nobody answers in time is a timeout", ctx do
      broker = DataPlaneRouter.shard_for(ctx.topic) |> Process.whereis()
      :ok = :sys.suspend(broker)
      on_exit(fn -> :sys.resume(broker) end)

      assert {:error, "open_timeout", _p} = Producer.prepare(producer(ctx), ctx.topic, 300)
    end

    test "a topic the server does not know is its refusal", ctx do
      assert {:error, "no_such_topic", _p} =
               Producer.prepare(producer(ctx), "missing_#{System.unique_integer([:positive])}", 300)
    end
  end

  test "a message for no connection of the producer's is not taken", ctx do
    p = producer(ctx)
    assert Producer.handle_message(p, {:tcp, make_ref(), "x"}, now()) == :no
    assert Producer.handle_message(p, :other, now()) == :no
    assert Producer.next_timeout(p, now()) == :infinity
  end

  test "options are checked", ctx do
    assert_raise ArgumentError, ~r/max_inflight/, fn -> Producer.new(ctx.router, [], max_inflight: 0) end
    assert_raise ArgumentError, ~r/linger_ms/, fn -> Producer.new(ctx.router, [], linger_ms: -1) end
    assert_raise ArgumentError, ~r/linger_bytes/, fn -> Producer.new(ctx.router, [], linger_bytes: 0) end
  end
end
