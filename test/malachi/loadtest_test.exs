defmodule Malachi.LoadtestTest do
  # Integration cases drive the real TCP server (started by the app in test_helper). Not async: they open
  # many sockets and share the one server.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Malachi.BrokerServer
  alias Malachi.DataPlaneRouter
  alias Malachi.Loadtest
  alias Malachi.Loadtest.Conn
  alias Malachi.Loadtest.Payload
  alias Malachi.Log.Record
  alias Malachi.ProducerDirectory
  alias Malachi.RateLimiter
  alias Malachi.TCPAcceptorPool
  alias Malachi.Test.LoadtestProbes
  alias Malachi.Test.PollingHelper
  alias Malachi.Test.ReadBack
  alias Malachi.Test.RouterTopology
  alias Malachi.Test.StreamStub
  alias Malachi.Test.StreamTCP
  alias Malachi.Test.TCPHelper
  alias Malachi.Wire
  alias Malachi.Wire.Batch

  # Runs a load test quietly and returns its report, capturing the printed summary.
  defp run(opts) do
    opts = Keyword.merge([port: TCPAcceptorPool.port(), user: "admin", pass: "admin123", warmup: 0, duration: 1], opts)
    capture_io(fn -> Process.put(:report, Loadtest.run(opts)) end)
    Process.get(:report)
  end

  defp topic(name), do: "lt_#{name}_#{System.unique_integer([:positive])}"

  # The generator speaks only the stream keys; the topic routing reads the consumer-group router's topology,
  # cleared for each case.
  setup {RouterTopology, :clear}

  describe "produce" do
    test "the report records the batch size and record size it ran with, as fields of their own" do
      # The flush regime the throughput describes. Inside meta.command it is free text in a syntax the
      # Node generator does not share, which is why the ceiling sweep and the published pages read these.
      r = run(scenario: :produce, connections: 2, batch: 7, record_size: 100, topic: topic("regime"))
      assert r.batch == 7
      assert r.record_size == 100

      defaults = run(scenario: :produce, connections: 1, topic: topic("regime_defaults"))
      assert defaults.batch == 10
      assert defaults.record_size == 256
      assert defaults.payload == :constant
      assert defaults.payload_seed == nil
      assert defaults.payload_pool_values == nil
    end

    test "json and random runs produce cleanly, pipelined too, and record the payload they sent" do
      for {payload, opts} <- [{:json, [pipeline: 4]}, {:random, [payload_seed: 9]}] do
        t = topic("payload_#{payload}")
        r = run([scenario: :produce, connections: 3, batch: 4, payload: payload, topic: t] ++ opts)

        assert r.errors == 0
        assert r.records == r.ops * 4
        assert r.payload == payload
        assert r.payload_seed == Keyword.get(opts, :payload_seed, 1)
        assert r.payload_pool_values == Payload.pool_size(256, 4, 3, Keyword.get(opts, :pipeline, 1))
      end
    end

    test "a pool past 512MiB is announced before it is generated, and a smaller or constant one is not" do
      big = [payload: :json, connections: 64, pipeline: 32, batch: 4096, record_size: 256]

      assert Loadtest.large_pool_note(big) ==
               "note: the json payload pool holds 2048MiB (64 connections x pipeline 32 x batch 4096); " <>
                 "generating it first"

      assert Loadtest.large_pool_note(Keyword.put(big, :pipeline, 1)) == nil
      assert Loadtest.large_pool_note(Keyword.put(big, :payload, :constant)) == nil
      # A read scenario uses the pool only to prepopulate, one request at a time, so the pipeline is moot.
      assert Loadtest.large_pool_note(Keyword.put(big, :scenario, :fetch)) == nil
    end

    test "a pipelined run sizes the pool for a full pipeline per connection" do
      # 256KB values make 8MiB of pool 8 batches of 4, so the pipeline decides: 2 connections each sending
      # 8 ahead need 16 batches for their first bursts to stay apart.
      opts = [connections: 2, batch: 4, pipeline: 8, record_size: 262_144, payload: :random]
      r = run([scenario: :produce, topic: topic("pool_pipeline")] ++ opts)

      assert r.payload_pool_values == 64
      assert Payload.pool_size(262_144, 4, 2, 1) == 32
    end

    test "each connection walks the pool from its own batch, so two connections send different values" do
      t = topic("pool_walk")
      run(scenario: :produce, connections: 2, batch: 2, payload: :json, payload_seed: 5, topic: t)

      values = ReadBack.values(Malachi.LogBroker, t)
      pool = Payload.values(:json, 5, 256, Payload.pool_size(256, 2, 2, 1))
      start = Payload.start_batch(1, div(length(pool), 2), 2) * 2

      # Connection 0 starts at the pool's first batch and connection 1 half way through it.
      assert Enum.at(pool, 0) in values
      assert Enum.at(pool, start) in values
      assert Enum.all?(values, &(&1 in pool))
      refute Enum.any?(values, &(&1 == :binary.copy("x", 256)))
    end

    test "a prepopulated backlog for a fetch run is drawn from the pool" do
      t = topic("pool_prepop")
      run(scenario: :fetch, connections: 1, batch: 5, prepopulate: 10, payload: :random, topic: t)

      assert ReadBack.values(Malachi.LogBroker, t) == Payload.values(:random, 1, 256, 10)
    end

    test "a run that never produces does not generate a pool, and one that prepopulates does" do
      r = run(scenario: :fetch, connections: 1, prepopulate: 0, payload: :json, topic: topic("no_pool"))
      assert r.payload == :json
      assert r.payload_pool_values == nil

      r = run(scenario: :fetch, connections: 1, prepopulate: 10, batch: 5, payload: :json, topic: topic("prep_pool"))
      assert r.payload_pool_values == Payload.pool_size(256, 5, 1, 1)

      # 1MB values make 8MiB of pool a single batch of 8, so the floor of one batch per connection is what
      # sizes it: two connections need two batches.
      r =
        run(
          scenario: :fetch,
          connections: 2,
          prepopulate: 8,
          batch: 8,
          max: 1,
          record_size: 1_048_576,
          payload: :random,
          topic: topic("pool_floor")
        )

      assert r.payload_pool_values == 16
      assert Payload.pool_size(1_048_576, 8, 1, 1) == 8
    end

    test "the pool lives in persistent_term during a run, no worker holds a copy, and a killed run frees it" do
      pools = fn -> for {{Malachi.Loadtest, :pool, _ref}, batches} <- :persistent_term.get(), do: batches end
      opts = [port: TCPAcceptorPool.port(), user: "admin", pass: "admin123", warmup: 0, duration: 30]

      # 65536 batches of one 16-byte value: each encoded batch is small enough to be a heap binary, so a
      # worker holding its own copy of the pool would hold about 10MB.
      runner =
        spawn(fn ->
          # all_at_once starts no connect gate, so the processes the runner links are its three workers and
          # nothing else (the setup's connections are ports it links, while the pool is already published).
          run_opts = [
            scenario: :produce,
            payload: :random,
            record_size: 16,
            batch: 1,
            connections: 3,
            connect_strategy: :all_at_once
          ]

          capture_io(fn -> Loadtest.run(opts ++ run_opts ++ [topic: topic("pool_held")]) end)
        end)

      workers = fn ->
        case Process.info(runner, :links) do
          {:links, links} -> Enum.filter(links, &is_pid/1)
          nil -> []
        end
      end

      assert :ok = PollingHelper.wait_until(fn -> pools.() != [] and length(workers.()) == 3 end, timeout: 15_000)
      assert [batches] = pools.()
      assert tuple_size(batches) == Payload.pool_size(16, 1, 3, 1)

      for worker <- workers.() do
        {:memory, bytes} = Process.info(worker, :memory)
        assert bytes < 2_000_000, "a worker holds #{bytes} bytes, as if it had its own copy of the pool"
      end

      # Killed, the run never reaches its `after`, and the pool must not outlive it anyway.
      Process.exit(runner, :kill)
      assert :ok = PollingHelper.wait_until(fn -> pools.() == [] end)
    end

    test "the pool is released when the run ends, and when its setup fails" do
      pools = fn -> for {{Malachi.Loadtest, :pool, _ref}, _batches} <- :persistent_term.get(), do: :pool end

      run(scenario: :produce, connections: 2, batch: 2, payload: :random, topic: topic("pool_release"))
      assert pools.() == []

      {:ok, closed} = :gen_tcp.listen(0, [])
      {:ok, port} = :inet.port(closed)
      :gen_tcp.close(closed)

      assert_raise Loadtest.SetupError, fn ->
        Loadtest.run(port: port, scenario: :produce, payload: :json, connections: 1, duration: 1)
      end

      assert pools.() == []
    end

    test "a comma-separated host list round-robins connections and produces cleanly" do
      # Both hosts resolve to the same test server, so this proves the multi-host path (parse, per-worker
      # host pick, reconnect opts) end to end without needing a real second node.
      t = topic("multihost")
      r = run(scenario: :produce, connections: 4, batch: 5, host: "127.0.0.1, 127.0.0.1", topic: t)

      assert r.error_reasons == %{}
      assert r.errors == 0
      assert r.dropped == 0
      assert r.records == r.ops * 5
    end

    test "fanning out over multiple topics produces to all of them without errors" do
      t = topic("fanout")
      r = run(scenario: :produce, connections: 6, batch: 5, topics: 3, topic: t)

      assert r.error_reasons == %{}
      assert r.errors == 0
      assert r.dropped == 0
      assert r.records == r.ops * 5

      # every fanned-out topic was created and got records (each is independently consumable)
      for i <- 0..2 do
        assert ReadBack.values(Malachi.LogBroker, "#{t}_#{i}") != [], "topic #{t}_#{i} should have received records"
      end
    end
  end

  describe "read on the stream keys" do
    # A `stream` worker is a consumer (a consume stream on every range of its topic,
    # `Malachi.Loadtest.Consumer`) and a `fetch` worker a fetcher (`fetch_range` on each range in turn,
    # `Malachi.Loadtest.Fetcher`), both opened inside the connect ramp.

    test "stream reads the backlog on consume streams: pages pushed, records counted, nothing refused" do
      r = run(scenario: :stream, connections: 2, batch: 10, prepopulate: 200, max: 50, topic: topic("read_stream"))

      assert r.errors == 0 and r.error_reasons == %{}
      assert r.dropped == 0
      assert r.ops > 0
      # every worker reads the whole topic
      assert r.records >= 2 * 200
    end

    test "fetch pages every range in turn, timed, and starts over at the end" do
      r = run(scenario: :fetch, connections: 2, batch: 10, prepopulate: 100, max: 50, topic: topic("read_fetch"))

      assert r.errors == 0 and r.error_reasons == %{}, inspect(r.error_reasons)
      assert r.ops > 0
      # pages that bring nothing send a range back to the start, so the backlog is read more than once
      assert r.records > 2 * 100
      assert r.latency_ms.p50 > 0
    end

    test "mixed produces and fetches on the stream keys at once" do
      r = run(scenario: :mixed, connections: 4, batch: 5, prepopulate: 50, topic: topic("read_mixed"))

      assert r.errors == 0 and r.error_reasons == %{}
      assert r.ops > 0 and r.records > 0
      # a produce is an op of 5 records; only the fetching half brings pages of more (the 50 prepopulated
      # ones, then each start over), so more records than 5 an op means the fetchers read
      assert r.records > r.ops * 5
    end

    # The consume streams the broker serving `topic` holds on its range `range`.
    defp consume_streams_on(topic, range) do
      %{consume: consume} = :sys.get_state(DataPlaneRouter.shard_for(topic))
      Enum.count(consume.consumers, fn {_token, consumer} -> consumer.range_id == {topic, range} end)
    end

    test "a split mid-run moves every consume stream, and the reading goes on on the children" do
      t = topic("read_split")
      parent = self()

      # the split once both workers' consume streams are open on the range (the broker holds them), so it
      # moves streams, rather than racing the setup
      spawn_link(fn ->
        PollingHelper.wait_until!(fn -> consume_streams_on(t, 0) == 2 end, timeout: 15_000)
        send(parent, {:split, BrokerServer.split_range(DataPlaneRouter.shard_for(t), {t, 0})})
      end)

      r = run(scenario: :stream, connections: 2, batch: 10, prepopulate: 100, duration: 2, topic: t)

      assert_receive {:split, {:ok, _left, _right}}, 5_000
      assert r.errors == 0, inspect(r.error_reasons)
      assert r.moved >= 2
      assert r.records >= 2 * 100
    end

    test "pages pushed while a stream worker's streams are opening are counted" do
      # two ranges, each open followed at once by a page of three: the first range's page comes while the
      # second is still opening, inside the measured run, and counts as the second's does
      stub = StreamStub.start(%{29 => {:push_on_open, ["a", "b", "c"]}})
      StreamStub.set_routes(stub, 1, [{0, 0, 128}, {1, 128, 256}])

      try do
        r = run(scenario: :stream, connections: 2, prepopulate: 0, port: stub.port, topic: "t")
        assert r.records == 2 * 6
        assert r.ops == 2 * 2
      after
        StreamStub.stop(stub)
      end
    end

    # A stub serving one range, answering `overrides` (StreamStub), stopped when `fun` returns.
    defp with_stub(overrides, fun) do
      stub = StreamStub.start(overrides)

      try do
        fun.(stub)
      after
        StreamStub.stop(stub)
      end
    end

    test "a fetch the broker sheds is counted as shed, and one redirected or dropped is no op and no error" do
      with_stub(%{31 => {:error, "overloaded"}}, fn stub ->
        r = run(scenario: :fetch, connections: 1, prepopulate: 0, port: stub.port, topic: "t")
        assert r.overloaded > 0 and r.errors == 0 and r.ops == 0
      end)

      with_stub(%{31 => {:error, "moved"}}, fn stub ->
        r = run(scenario: :fetch, connections: 1, prepopulate: 0, port: stub.port, topic: "t")
        assert r.redirects > 0 and r.route_refreshes > 0
        assert r.errors == 0 and r.ops == 0
      end)

      with_stub(%{31 => :close}, fn stub ->
        r = run(scenario: :fetch, connections: 1, prepopulate: 0, port: stub.port, topic: "t")
        assert r.dropped > 0
        assert r.errors == 0 and r.ops == 0
      end)
    end

    # Writes the refusal of the first open_consume the stub took to its connection, once one came: what a
    # broker that closes a consume stream mid-run sends.
    defp refuse_first_consume(stub) do
      spawn(fn ->
        PollingHelper.wait_until!(fn -> StreamStub.consumes(stub) != [] end)
        [{corr, _start} | _] = StreamStub.consumes(stub)
        StreamStub.push(stub, Wire.encode_error(corr, :permission_denied))
      end)
    end

    test "a fetch whose broker takes no connection waits and tries again, counting no error" do
      with_stub(%{}, fn stub ->
        # once the fetcher has read, its broker moves to an address nobody listens on and the connection it
        # had closes: the routes it reads again name that address, so every connect fails from then on
        move =
          spawn(fn ->
            PollingHelper.wait_until!(fn -> StreamStub.fetched(stub) > 0 end)
            {:ok, listen} = :gen_tcp.listen(0, [])
            {:ok, closed} = :inet.port(listen)
            :gen_tcp.close(listen)
            StreamStub.set_advertised_port(stub, closed)
            StreamStub.set_override(stub, 31, :close)
          end)

        r = run(scenario: :fetch, connections: 1, prepopulate: 0, port: stub.port, topic: "t")
        Process.exit(move, :kill)

        assert r.ops > 0
        assert r.dropped >= 1
        assert r.errors == 0 and r.error_reasons == %{}
        # each failed connect waits before the next try: some dozens of route reads in the second, not
        # one per loop
        assert r.route_refreshes < 100
      end)
    end

    test "a fetch whose routes cannot be read counts each attempt once, waiting out the backoff between" do
      with_stub(%{}, fn stub ->
        # once the fetcher has read, its connection closes and the routes are refused from then on
        refuse =
          spawn(fn ->
            PollingHelper.wait_until!(fn -> StreamStub.fetched(stub) > 0 end)
            StreamStub.set_override(stub, 25, {:error, "no_such_topic"})
            StreamStub.set_override(stub, 31, :close)
          end)

        r = run(scenario: :fetch, connections: 1, prepopulate: 0, port: stub.port, topic: "t")
        Process.exit(refuse, :kill)

        # one read of the routes about every 200 ms of the second, each one error: not one per loop
        assert Map.keys(r.error_reasons) == ["no_such_topic"]
        assert r.errors in 1..10
      end)
    end

    test "a consume stream the broker closes with a refusal mid-run is an error under it, once, and is not read again" do
      with_stub(%{29 => {:push_on_open, ["a"]}}, fn stub ->
        refuse = refuse_first_consume(stub)
        r = run(scenario: :stream, connections: 1, prepopulate: 0, port: stub.port, topic: "t")
        Process.exit(refuse, :kill)

        assert r.error_reasons == %{"permission_denied" => 1}
        # the page pushed before the refusal counts; the range was not opened again, so nothing more came
        assert r.records == 1
        assert length(StreamStub.consumes(stub)) == 1
      end)
    end

    test "a range refused at its open is an error, once, while the other ranges are read throughout" do
      # two ranges: the first open is answered with a page, the second refused
      with_stub(%{29 => {:after, 1, {:push_on_open, ["a", "b"]}, {:error, "permission_denied"}}}, fn stub ->
        StreamStub.set_routes(stub, 1, [{0, 0, 128}, {1, 128, 256}])

        # once both opens came, one more page on the stream that opened: the last stream the stub opened
        # (the setup opened producer streams before it)
        more =
          spawn(fn ->
            PollingHelper.wait_until!(fn -> length(StreamStub.consumes(stub)) == 2 end)
            [{corr, _start} | _] = StreamStub.consumes(stub)
            batch = Batch.encode([{{0, 2}, Record.new("c"), false}], :none, :positioned)

            page = %{
              stream_id: StreamStub.opened(stub),
              next: {0, 3},
              skip: 0,
              backlog: 0,
              expired: 0,
              expired_exact: true,
              batch: batch
            }

            StreamStub.push(stub, Wire.encode_ok(corr, IO.iodata_to_binary(Wire.encode_push(:records, page))))
          end)

        r = run(scenario: :stream, connections: 1, prepopulate: 0, port: stub.port, topic: "t")
        Process.exit(more, :kill)

        assert r.error_reasons == %{"permission_denied" => 1}
        # the page that came after the refusal counts: the range that opened was read on
        assert r.records == 3 and r.ops == 2
      end)
    end

    test "a consume stream refused by the subscribe quota, or overloaded, is counted as shed, not as an error" do
      with_stub(%{29 => {:error, "rate_limited"}}, fn stub ->
        r = run(scenario: :stream, connections: 1, prepopulate: 0, port: stub.port, topic: "t")
        assert r.rate_limited == 1 and r.errors == 0
      end)

      with_stub(%{29 => {:error, "overloaded"}}, fn stub ->
        r = run(scenario: :stream, connections: 1, prepopulate: 0, port: stub.port, topic: "t")
        assert r.overloaded == 1 and r.errors == 0
      end)
    end

    test "pages and fetches inside the warmup are not counted; a refusal inside it is" do
      # the page comes as the stream opens, at the barrier: inside the warmup
      with_stub(%{29 => {:push_on_open, ["a", "b"]}}, fn stub ->
        r = run(scenario: :stream, connections: 1, prepopulate: 0, warmup: 1, port: stub.port, topic: "t")
        assert r.records == 0 and r.ops == 0
      end)

      with_stub(%{29 => {:error, "permission_denied"}}, fn stub ->
        r = run(scenario: :stream, connections: 1, prepopulate: 0, warmup: 1, port: stub.port, topic: "t")
        assert r.error_reasons == %{"permission_denied" => 1}
      end)

      # three fetches answered at once, inside the warmup; every later one finds its connection closed
      with_stub(%{31 => {:after, 3, :close}}, fn stub ->
        r = run(scenario: :fetch, connections: 1, prepopulate: 0, warmup: 1, port: stub.port, topic: "t")
        assert r.ops == 0 and r.errors == 0
      end)
    end

    test "a fetch refused by the subscribe quota is counted apart from a genuine error, as a produce is" do
      for key <- [:subscribe_rate_limit, :subscribe_rate_window_ms] do
        prior = Application.get_env(:malachi, key)
        on_exit(fn -> Application.put_env(:malachi, key, prior) end)
      end

      # a fetch_range spends a subscribe token: a handful are admitted, the rest of the run refused
      Application.put_env(:malachi, :subscribe_rate_limit, 5)
      Application.put_env(:malachi, :subscribe_rate_window_ms, 60_000)
      RateLimiter.reset_bucket("admin", :subscribe)

      r = run(scenario: :fetch, connections: 1, batch: 10, prepopulate: 10, topic: topic("read_quota"))

      assert r.rate_limited > 0, "the quota never bit, so this case proves nothing"
      assert r.errors == 0 and r.error_reasons == %{}
      assert r.overloaded == 0
    end

    test "a fetch the broker refuses is a genuine error under its reason" do
      # the producer may write the topic but not read it
      r =
        run(
          scenario: :fetch,
          connections: 1,
          batch: 10,
          prepopulate: 10,
          user: "producer",
          pass: "producer123",
          topic: topic("read_denied")
        )

      assert r.errors > 0
      assert Map.keys(r.error_reasons) == ["permission_denied"]
      assert r.ops == 0
    end

    test "a consumer whose streams the broker refuses counts the refusal and reads nothing" do
      r =
        run(
          scenario: :stream,
          connections: 1,
          batch: 10,
          prepopulate: 10,
          user: "producer",
          pass: "producer123",
          topic: topic("read_denied_stream")
        )

      assert r.errors == 1
      assert r.error_reasons == %{"permission_denied" => 1}
      assert r.ops == 0
    end
  end

  describe "produce on the stream keys" do
    # A producing connection is a whole client (`Malachi.Loadtest.Producer`): it reads the routes, opens a
    # stream on each range of its topic inside the connect ramp, and counts every acknowledged append as an
    # op.

    # The producer streams open on this node for `topic`, and the connections that hold them.
    defp producer_streams(topic), do: Enum.filter(ProducerDirectory.list().streams, &(&1.topic == topic))

    defp producer_connections(topic) do
      for %{connection: "#PID" <> pid} <- producer_streams(topic), uniq: true, do: :erlang.list_to_pid(to_charlist(pid))
    end

    # The values every range of `topic` holds, read on the stream keys.
    defp stream_values(topic) do
      {:ok, socket} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate_wire(socket, "admin", "admin123")
      routes = StreamTCP.routes(socket, topic)

      values =
        for %{range: range, state: :active} <- routes.ranges,
            value <- read_range(socket, topic, range, :earliest, []),
            do: value

      :gen_tcp.close(socket)
      values
    end

    defp read_range(socket, topic, range, start, acc) do
      {:ok, page} = StreamTCP.fetch_range(socket, topic, range: range, start: start, max: 1_000)

      case StreamTCP.page_records(page) do
        [] -> acc
        records -> read_range(socket, topic, range, {:position, page.next}, acc ++ Enum.map(records, &elem(&1, 1)))
      end
    end

    test "produces on the stream keys: every append acknowledged, nothing refused, records == ops * batch" do
      t = topic("streams")
      # one connection at a time: were the topic's first segment placed by the first worker's open, every
      # later worker would open with routes that no longer name it, and be redirected
      r = run(scenario: :produce, connections: 4, connect_concurrency: 1, batch: 5, topic: t)

      assert r.errors == 0 and r.error_reasons == %{}
      assert r.dropped == 0 and r.overloaded == 0 and r.rate_limited == 0
      assert r.ops > 0
      assert r.records == r.ops * 5
      # every stream opened where the setup's routes said, so routing had nothing to do
      assert r.moved == 0 and r.redirects == 0 and r.route_refreshes == 0,
             inspect(Map.take(r, [:moved, :redirects, :route_refreshes]))

      assert length(stream_values(t)) >= r.records
      # the fields of the keys the generator no longer speaks are gone from the report
      refute Map.has_key?(r, :protocol)
      refute Map.has_key?(r, :reconnects)
    end

    test "the pipeline is the appends each stream keeps in flight" do
      r = run(scenario: :produce, connections: 2, batch: 5, pipeline: 8, topic: topic("streams_pipe"))

      assert r.errors == 0 and r.error_reasons == %{}
      assert r.records == r.ops * 5
      assert r.pipeline == 8
    end

    test "lingering joins batches into one append, so an op carries several of them" do
      # 3 batches of 5 records of 256 bytes, keyed k0..k999, take at most 3 x 5 x (256 + 4 + 22) bytes: they
      # reach linger_bytes long before the 60 s deadline, and each append takes 3 of them
      r =
        run(
          scenario: :produce,
          connections: 2,
          batch: 5,
          record_size: 256,
          linger_ms: 60_000,
          linger_bytes: 3 * 5 * (256 + 4 + 22),
          pipeline: 4,
          topic: topic("streams_linger")
        )

      assert r.errors == 0
      assert r.ops > 0
      assert r.records == r.ops * 15
      assert {r.linger_ms, r.linger_bytes} == {60_000, 3 * 5 * (256 + 4 + 22)}
    end

    test "one batch is one append by default, whatever the record size" do
      # small records, where the framing a batch holds per record (key and 22 bytes) is a large share of
      # it: each append still carries one whole batch
      r =
        run(
          scenario: :produce,
          connections: 2,
          batch: 10,
          record_size: 100,
          pipeline: 4,
          topic: topic("streams_onebatch")
        )

      assert r.errors == 0 and r.ops > 0
      assert r.records == r.ops * 10
    end

    test "the appends in flight at the deadline are waited for and counted" do
      t = topic("streams_drain")
      r = run(scenario: :produce, connections: 2, batch: 5, pipeline: 16, topic: t)

      assert r.errors == 0
      # with no warmup every acknowledgement is counted: what landed is what was counted
      assert length(stream_values(t)) == r.records
    end

    test "a payload pool is streamed record for record" do
      r =
        run(
          scenario: :produce,
          connections: 2,
          batch: 4,
          payload: :json,
          record_size: 300,
          topic: topic("streams_pool")
        )

      assert r.errors == 0
      assert r.records == r.ops * 4
      assert r.payload_pool_values > 0
    end

    test "a stream prepopulate draws its batches from the payload pool" do
      t = topic("streams_prepop_pool")

      r =
        run(scenario: :produce, connections: 1, batch: 10, payload: :json, record_size: 300, prepopulate: 100, topic: t)

      values = stream_values(t)
      assert length(values) >= 100 + r.records
      # json documents, not the constant bytes
      assert Enum.all?(Enum.take(values, 100), &String.starts_with?(&1, "{"))
    end

    test "the prepopulate goes through the stream keys too" do
      t = topic("streams_prepop")
      r = run(scenario: :produce, connections: 1, batch: 10, prepopulate: 200, duration: 1, topic: t)

      assert length(stream_values(t)) >= 200 + r.records
    end

    test "a split mid-run moves every stream, and the records keep landing on the children" do
      t = topic("streams_split")
      parent = self()

      spawn_link(fn ->
        # once every worker's stream is open, and they have streamed a little on it
        PollingHelper.wait_until!(fn -> length(producer_connections(t)) == 3 end)
        Process.sleep(200)
        send(parent, {:split, BrokerServer.split_range(DataPlaneRouter.shard_for(t), {t, 0})})
      end)

      r = run(scenario: :produce, connections: 3, batch: 5, duration: 2, pipeline: 4, topic: t)

      assert_receive {:split, {:ok, _left, _right}}, 5_000
      assert r.errors == 0, inspect(r.error_reasons)
      assert r.moved >= 3
      assert r.route_refreshes >= 3
      # a batch whose keys the split put in two ranges goes out as two appends, each an op of its own
      assert r.records <= r.ops * 5 and r.records >= r.ops
      assert length(stream_values(t)) >= r.records
    end

    test "an append refused by the publish quota is counted apart from a genuine error" do
      for key <- [:publish_rate_limit, :publish_rate_window_ms] do
        prior = Application.get_env(:malachi, key)
        on_exit(fn -> Application.put_env(:malachi, key, prior) end)
      end

      Application.put_env(:malachi, :publish_rate_limit, 5)
      Application.put_env(:malachi, :publish_rate_window_ms, 60_000)
      RateLimiter.reset_bucket("admin", :publish)

      r = run(scenario: :produce, connections: 2, batch: 1, topic: topic("streams_quota"))

      assert r.rate_limited > 0, "the quota never bit, so this case proves nothing"
      assert r.errors == 0 and r.error_reasons == %{}
      assert r.overloaded == 0
    end

    test "an append the broker sheds as overloaded is counted apart from a genuine error" do
      with_stream_server(%{27 => {:refuse_append, "overloaded"}}, fn port ->
        r = run(scenario: :produce, connections: 1, batch: 2, port: port, topic: "t")

        assert r.overloaded > 0
        assert r.errors == 0 and r.error_reasons == %{}
        assert r.ops == 0 and r.rate_limited == 0
      end)
    end

    # `not_primary` and `sealed` refuse an append the broker stored nothing of because a roll placed its
    # range's next segment on another node: the producer sends it again as routing, so a run over a normal
    # roll records no error, and a prepopulate over one does not fail its setup.
    for reason <- ["not_primary", "sealed"] do
      test "an append refused #{reason} is sent again as routing: no error, every record acknowledged" do
        with_stream_server(%{27 => {:after, 1, {:refuse_append, unquote(reason)}, :default}}, fn port, stub ->
          r = run(scenario: :produce, connections: 1, batch: 2, port: port, topic: "t")
          assert StreamStub.refused(stub) == 1, "the stub refused nothing, so this case proves nothing"
          assert r.errors == 0 and r.error_reasons == %{}
          assert r.redirects > 0
          assert r.ops > 0 and r.records == r.ops * 2
        end)

        with_stream_server(%{27 => {:after, 1, {:refuse_append, unquote(reason)}, :default}}, fn port, stub ->
          r = run(scenario: :fetch, connections: 1, batch: 2, prepopulate: 4, port: port, topic: "t")
          assert StreamStub.refused(stub) == 1
          assert r.errors == 0
        end)
      end
    end

    test "what happens in the warmup is not counted, acknowledgements and refusals alike" do
      # appends refused for the first 300 ms of a 1 s warmup (counted from the stream's open, which comes
      # just before it), acknowledged from then on
      with_stream_server(%{27 => {:refuse_append, "rate_limited"}}, fn port, stub ->
        spawn_link(fn ->
          PollingHelper.wait_until!(fn -> StreamStub.opened(stub) > 0 end, interval: 5)
          Process.sleep(300)
          StreamStub.set_override(stub, 27, :default)
        end)

        r = run(scenario: :produce, connections: 1, batch: 1, warmup: 1, duration: 1, port: port, topic: "t")
        refused = StreamStub.refused(stub)
        acknowledged = length(StreamStub.appends(stub)) - refused

        assert refused > 0, "the stub refused nothing, so this case proves nothing"
        assert r.rate_limited == 0
        # the appends acknowledged in the warmup are not ops
        assert r.ops > 0 and r.ops < acknowledged
        assert r.errors == 0
      end)
    end

    test "any other refusal of an append is a genuine error, under the reason the broker gave" do
      prior = Application.get_env(:malachi, :max_inflated_batch_bytes)
      on_exit(fn -> Application.put_env(:malachi, :max_inflated_batch_bytes, prior) end)
      # a batch of 2 x 256 bytes inflates past this: every append is refused
      Application.put_env(:malachi, :max_inflated_batch_bytes, 100)

      r = run(scenario: :produce, connections: 1, batch: 2, record_size: 256, topic: topic("streams_refused"))

      assert r.errors > 0
      assert [reason] = Map.keys(r.error_reasons)
      assert r.error_reasons[reason] == r.errors
      assert r.ops == 0 and r.rate_limited == 0 and r.overloaded == 0
    end

    test "a connection the broker drops mid-run is a drop, not an error, and the client streams on" do
      t = topic("streams_drop")
      parent = self()

      spawn_link(fn ->
        # the client's one connection streaming to t, once open; then the one it opens after the drop, once
        # an append went through it
        PollingHelper.wait_until!(fn -> producer_connections(t) != [] end)
        Process.sleep(300)
        # a listing gives a connection 500 ms to answer: one busy past that is asked again, and the pid is
        # taken from the listing that found it
        PollingHelper.wait_until!(fn ->
          Process.put(:connections, producer_connections(t))
          match?([_], Process.get(:connections))
        end)

        [pid] = Process.get(:connections)
        Process.exit(pid, :kill)
        send(parent, :dropped)

        PollingHelper.wait_until!(fn ->
          Enum.any?(producer_streams(t), &(&1.connection != inspect(pid) and &1.acked_sequence > 0))
        end)

        send(parent, :streamed_on)
      end)

      r = run(scenario: :produce, connections: 1, batch: 5, duration: 2, topic: t)

      assert_receive :dropped, 5_000
      assert_receive :streamed_on, 5_000
      assert r.dropped >= 1
      assert r.route_refreshes >= 1
      assert r.errors == 0, inspect(r.error_reasons)
      assert r.ops > 0 and r.records == r.ops * 5
    end

    test "a cluster that does not serve the stream keys (an older release, its flag off) fails the setup, naming why" do
      with_stream_server(%{24 => :streams_off}, fn port ->
        assert_raise Loadtest.SetupError, ~r/does not serve the stream keys \(the producer_streams flag is off\)/, fn ->
          run(scenario: :produce, connections: 1, port: port, topic: "t")
        end
      end)
    end

    test "a node that does not answer cluster_state fails the setup, naming why" do
      with_stream_server(%{24 => {:error, "unknown_api_key"}}, fn port ->
        assert_raise Loadtest.SetupError,
                     ~r/does not serve the stream keys \(the server does not answer cluster_state: no_cluster_state\)/,
                     fn ->
                       run(scenario: :produce, connections: 1, port: port, topic: "t")
                     end
      end)
    end

    test "a connection lost while reading the cluster state fails the setup, naming it" do
      with_stream_server(%{24 => :close}, fn port ->
        assert_raise Loadtest.SetupError, ~r/could not read the cluster state: the connection failed/, fn ->
          run(scenario: :produce, connections: 1, port: port, topic: "t")
        end
      end)
    end

    test "ranges the server will not open, or routes it will not give, fail the setup, naming the step" do
      # the setup opens a stream on each range first (placing its segment), then reads the routes
      with_stream_server(%{25 => {:error, "no_such_topic"}}, fn port ->
        assert_raise Loadtest.SetupError,
                     ~r/could not open the ranges of "t": the server answered "no_such_topic"/,
                     fn ->
                       run(scenario: :produce, connections: 1, port: port, topic: "t")
                     end
      end)

      with_stream_server(%{25 => {:after, 1, {:error, "no_such_topic"}}}, fn port ->
        assert_raise Loadtest.SetupError,
                     ~r/could not read the routes of "t": the server answered "no_such_topic"/,
                     fn ->
                       run(scenario: :produce, connections: 1, port: port, topic: "t")
                     end
      end)

      with_stream_server(%{25 => {:after, 1, :close}}, fn port ->
        assert_raise Loadtest.SetupError, ~r/could not read the routes of "t": the connection failed/, fn ->
          run(scenario: :produce, connections: 1, port: port, topic: "t")
        end
      end)
    end

    test "a prepopulate the broker stops answering fails after one wait, not one per batch" do
      # 30 batches against a pipeline of 16: the 17th waits for room that never comes
      with_stream_server(%{27 => :silent}, fn port ->
        {elapsed_us, _} =
          :timer.tc(fn ->
            assert_raise Loadtest.SetupError, ~r/could not prepopulate "t": it timed out/, fn ->
              run(
                scenario: :produce,
                connections: 1,
                port: port,
                prepopulate: 300,
                batch: 10,
                setup_timeout_ms: 200,
                topic: "t"
              )
            end
          end)

        assert elapsed_us < 1_500_000, "waited #{div(elapsed_us, 1000)} ms"
      end)
    end

    test "a prepopulate the broker refuses, or never answers, fails the setup" do
      with_stream_server(%{27 => {:refuse_append, "storage_full"}}, fn port ->
        assert_raise Loadtest.SetupError, ~r/could not prepopulate "t": the server answered "storage_full"/, fn ->
          run(scenario: :produce, connections: 1, port: port, prepopulate: 10, batch: 10, topic: "t")
        end
      end)

      with_stream_server(%{27 => :silent}, fn port ->
        assert_raise Loadtest.SetupError, ~r/could not prepopulate "t": it timed out/, fn ->
          run(
            scenario: :produce,
            connections: 1,
            port: port,
            prepopulate: 10,
            batch: 10,
            setup_timeout_ms: 200,
            topic: "t"
          )
        end
      end)
    end

    test "a stream client whose streams cannot open fails the run as a connection that cannot authenticate" do
      # the setup's own open goes through; every worker's, inside the connect ramp, is refused
      with_stream_server(%{26 => {:after, 1, {:error, "permission_denied"}}}, fn port ->
        assert_raise Loadtest.SetupError, ~r/2 of 2 connections failed.*permission_denied/, fn ->
          run(scenario: :produce, connections: 2, port: port, topic: "refused")
        end
      end)

      # and one refused at the setup's open fails there, naming the topic
      with_stream_server(%{26 => {:error, "permission_denied"}}, fn port ->
        assert_raise Loadtest.SetupError,
                     ~r/could not open the ranges of "refused": the server answered "permission_denied"/,
                     fn ->
                       run(scenario: :produce, connections: 2, port: port, topic: "refused")
                     end
      end)
    end
  end

  # A run against a `Malachi.Test.StreamStub`, with `overrides` replacing its answer to a key; `fun` takes
  # the stub's port, and the stub too when it steers it.
  defp with_stream_server(overrides, fun) do
    stub = StreamStub.start(overrides)

    try do
      if is_function(fun, 2), do: fun.(stub.port, stub), else: fun.(stub.port)
    after
      StreamStub.stop(stub)
    end
  end

  describe "read scenarios" do
    test "a healthy stream run reports no drop, whether it drains a backlog or stays idle" do
      # Every consume stream is still open at the deadline: a run that ends there has lost no connection.
      for {label, prepopulate} <- [backlog: 200, idle: 0] do
        r = run(scenario: :stream, connections: 2, prepopulate: prepopulate, window: 50, max: 50, topic: topic("calm"))

        assert r.dropped == 0, "#{label}: the deadline was counted as a dropped connection"
        assert r.errors == 0, "#{label}"
      end
    end
  end

  describe "control-plane scenarios" do
    test "user create/delete cycle runs to a valid report" do
      r = run(scenario: :user, connections: 2, topic: topic("user"))
      assert is_integer(r.ops) and r.ops >= 0
    end

    test "acl grant/revoke cycle runs to a valid report" do
      r = run(scenario: :acl, connections: 2, topic: topic("acl"))
      assert is_integer(r.ops) and r.ops >= 0
    end
  end

  describe "reproduce metadata" do
    test "the recorded command names the topic and the port that were actually used" do
      # Both were missing and both break a reproduction quietly: a run on a non-default port published
      # a command that dials 4040, and a fetch run against an existing topic published one that would
      # create a new empty one.
      t = topic("addressing")
      r = run(scenario: :produce, connections: 2, batch: 5, topic: t)

      assert r.meta.command =~ "--topic=#{t}"
      assert r.meta.command =~ "--port=#{TCPAcceptorPool.port()}"
      assert r.meta.command =~ "--host=127.0.0.1"
    end

    # The argv a shell would hand the mix task, one element per line. Asking a shell is the only
    # question worth asking about quoting: not what the escaping looks like, but what survives it.
    defp replay_argv(command) do
      {output, 0} = System.cmd("sh", ["-c", ~s(set -- #{command}; printf '%s\\n' "$@")])

      # `mix` and `malachi.loadtest` lead; the rest is what OptionParser would see.
      output |> String.split("\n", trim: true) |> Enum.drop(2)
    end

    defp shell_value(command, flag) do
      prefix = flag <> "="

      command
      |> replay_argv()
      |> Enum.find_value("", fn arg ->
        if String.starts_with?(arg, prefix), do: String.replace_prefix(arg, prefix, "")
      end)
    end

    # Absence asked of the argument list, not of the extracted value. Reading `shell_value/2 == ""`
    # also holds for a flag that IS emitted with an empty value, so `--cert=` would have satisfied
    # every assertion that the certificate paths are omitted: the helper answered the question it
    # could rather than the one being asked. A refute on the substring `--key` is worse still, since
    # `--keys=1000` contains it.
    defp shell_flag_absent?(command, flag) do
      command |> replay_argv() |> Enum.all?(&(not String.starts_with?(&1, flag <> "=")))
    end

    test "the payload is in the command only when it is not constant bytes, seed included" do
      refute Loadtest.reproduce_command([]) =~ "payload"

      command = Loadtest.reproduce_command(payload: :json, payload_seed: 12)
      assert shell_value(command, "--payload") == "json"
      assert shell_value(command, "--payload-seed") == "12"
      assert shell_value(Loadtest.reproduce_command(payload: :random), "--payload-seed") == "1"
    end

    test "free text in the recorded command survives a shell round trip" do
      # A topic with a space recorded as `--topic has a space`, which on replay parses as
      # `--topic has` and silently targets a different topic. The server's allowlist rejects this
      # name, so a real run fails, but the command is recorded either way and a string this module
      # emits should not rely on a downstream validator to come out well formed.
      t = "has a space and a ' quote"
      command = Loadtest.reproduce_command(topic: t)

      assert shell_value(command, "--topic") == t
    end

    test "a value starting with a hyphen replays as a value, not as another switch" do
      # A hyphen is inside the server's topic allowlist, so `-weird` is a name the broker accepts.
      # Emitted as two arguments, OptionParser read the value as a switch and the replay did not run
      # at all: it complained that --topic was missing its argument and that -w, -e, -i, -r and -d
      # were unknown options. The `--name=value` form is what makes it a value again.
      command = Loadtest.reproduce_command(topic: "-weird")

      assert shell_value(command, "--topic") == "-weird"
      # Parsed the way a replay parses it, not matched as text: this is the step that used to raise.
      replayed = command |> replay_argv() |> Enum.filter(&String.starts_with?(&1, "--topic="))
      assert {[topic: "-weird"], []} = OptionParser.parse!(replayed, strict: [topic: :string])
    end

    test "a certificate path starting with a hyphen replays the same way" do
      command = Loadtest.reproduce_command(tls: true, cacert: "-relative/ca.pem")

      assert shell_value(command, "--cacert") == "-relative/ca.pem"
    end

    test "an ordinary command is not quoted, so it stays readable" do
      command = Loadtest.reproduce_command(topic: "plain_topic")

      refute command =~ "'"
      assert command =~ "--topic=plain_topic"
      assert command =~ "--host=127.0.0.1"
    end

    test "a TLS run records a command that reconnects over TLS" do
      # Omitting this published a command that reconnects in PLAINTEXT: it does not reproduce the run,
      # and it does not measure the same thing either, since the handshake and the record layer are
      # part of what was timed.
      command =
        Loadtest.reproduce_command(
          tls: true,
          cacert: "ca.pem",
          cert: "client.pem",
          key: "/etc/certs/client key.pem"
        )

      assert command =~ "--tls"
      assert shell_value(command, "--cacert") == "ca.pem"
      assert shell_value(command, "--cert") == "client.pem"
      # Paths come along because they are paths; a space in one still has to survive.
      assert shell_value(command, "--key") == "/etc/certs/client key.pem"
    end

    test "a plaintext run records no transport flags at all" do
      command = Loadtest.reproduce_command([])

      refute command =~ "--tls"
      assert shell_flag_absent?(command, "--cacert")
    end

    test "server-authenticated TLS records the flag without inventing certificate paths" do
      command = Loadtest.reproduce_command(tls: true)

      assert command =~ "--tls"
      assert shell_flag_absent?(command, "--cacert")
      assert shell_flag_absent?(command, "--cert")
      assert shell_flag_absent?(command, "--key")
      # Verification is the default, so a run that did not ask to skip it must not record that it did.
      assert shell_flag_absent?(command, "--insecure")
    end

    test "a run that skipped server verification records it" do
      # --insecure turns off certificate verification, so a command that omitted it would reproduce a
      # stronger configuration than the run and disagree with its own numbers, the same reason the
      # certificate paths are recorded.
      command = Loadtest.reproduce_command(tls: true, insecure: true)

      assert command =~ "--tls"
      assert command =~ "--insecure"
    end

    test "the recorded command still carries no credential values" do
      command = Loadtest.reproduce_command(tls: true, user: "admin", pass: "hunter2", token: "t0ken")

      refute command =~ "hunter2"
      refute command =~ "t0ken"
      refute command =~ "--pass"
      refute command =~ "--token"
    end

    test "a password run records which user it authenticated as, and still no password" do
      # The identity decides what the run was allowed to do: a user without a produce ACL on the
      # topic measures rejections, and a command that reproduces it as admin disagrees with the
      # numbers printed beside it. The name is not the secret.
      command = Loadtest.reproduce_command(user: "reader", pass: "hunter2")

      assert shell_value(command, "--user") == "reader"
      refute command =~ "hunter2"
    end

    test "a password run with no user named records the one that was actually dialled" do
      assert shell_value(Loadtest.reproduce_command([]), "--user") == "admin"
    end

    test "a token or certificate run names no user, having authenticated without one" do
      assert shell_flag_absent?(Loadtest.reproduce_command(token: "t0ken"), "--user")
      assert shell_flag_absent?(Loadtest.reproduce_command(tls: true, cert: "c.pem", key: "k.pem"), "--user")
    end

    test "a run that produced JSON records the flag that produced it" do
      # Without it the reproduction prints a summary to the terminal and writes no JSON, so the one
      # thing the command cannot do is regenerate the page it is quoted on.
      assert Loadtest.reproduce_command(json: true) =~ "--json"
      refute Loadtest.reproduce_command(json: false) =~ "--json"
    end

    test "the report carries a meta block describing when, from what, and on what" do
      r = run(scenario: :produce, connections: 2, batch: 5, topic: topic("meta"))

      assert %{meta: meta} = r
      assert meta.timestamp =~ ~r/^\d{4}-\d{2}-\d{2}T/
      assert is_binary(meta.hardware.cpu)
      assert meta.hardware.schedulers > 0
      assert meta.malachi_version =~ ~r/^\d+\.\d+\.\d+/

      # The command is rebuilt from the EFFECTIVE config, not from what was typed, so a knob left at its
      # default is still part of the reproduction. --keys was never passed here.
      assert meta.command =~ "mix malachi.loadtest"
      assert meta.command =~ "--scenario=produce"
      assert meta.command =~ "--connections=2"
      assert meta.command =~ "--batch=5"
      assert meta.command =~ "--keys=1000", "a defaulted knob still belongs in a reproduce command"
    end

    test "the recorded command carries no credentials" do
      # This string is committed and published with the result. A password reaching it would be a leak
      # that survives in git history, so the reconstruction excludes the auth options by construction.
      r = run(scenario: :produce, connections: 2, batch: 5, topic: topic("nocreds"))

      refute r.meta.command =~ "admin123"
      refute r.meta.command =~ "--pass"
      refute r.meta.command =~ "--token"
    end

    test "--json emits a document a parser accepts, matching the returned report" do
      # The report now carries free text from the environment (an architecture string, a git ref, a
      # host), and the previous hand-built JSON had no escaping: one quote in any of them produced a
      # document no parser would take. Parsing the output is what pins that.
      opts = [
        port: TCPAcceptorPool.port(),
        user: "admin",
        pass: "admin123",
        warmup: 0,
        duration: 1,
        scenario: :produce,
        connections: 2,
        batch: 5,
        topic: topic("json"),
        json: true
      ]

      output = capture_io(fn -> Process.put(:report, Loadtest.run(opts)) end)
      report = Process.get(:report)

      assert {:ok, decoded} = Jason.decode(output)
      assert decoded["scenario"] == "produce"
      assert decoded["records_per_s"] == report.records_per_s
      assert decoded["latency_ms"]["p50"] == report.latency_ms.p50
      assert decoded["meta"]["command"] == report.meta.command
      assert decoded["meta"]["hardware"]["cpu"] == report.meta.hardware.cpu
    end
  end

  describe "lingering options" do
    test "a linger_ms or a linger_bytes out of range is refused" do
      assert_raise ArgumentError, ~r/linger_ms must be a non-negative integer/, fn ->
        Loadtest.reproduce_command(linger_ms: -1)
      end

      assert_raise ArgumentError, ~r/linger_bytes must be a positive integer/, fn ->
        Loadtest.reproduce_command(linger_bytes: 0)
      end
    end

    test "the command records the lingering it used, one batch by default, and no protocol" do
      command = Loadtest.reproduce_command(batch: 4, record_size: 100)
      assert shell_value(command, "--linger-ms") == "0"
      # 4 records as a batch holds them: 100 bytes, the longest key (k999) and 22 of framing
      assert shell_value(command, "--linger-bytes") == "504"
      assert shell_flag_absent?(command, "--protocol")

      command = Loadtest.reproduce_command(linger_ms: 7, linger_bytes: 9)
      assert {shell_value(command, "--linger-ms"), shell_value(command, "--linger-bytes")} == {"7", "9"}
    end

    test "the mix task has no --protocol flag" do
      assert_raise OptionParser.ParseError, ~r/--protocol/, fn ->
        Mix.Tasks.Malachi.Loadtest.run(["--protocol", "legacy"])
      end
    end
  end

  describe "measured window marker" do
    @describetag :tmp_dir

    test "appears only after every connection authenticated and the warmup ended", %{tmp_dir: dir} do
      # The harness samples CPU from this instant. It used to start WARM seconds after the spawn instead,
      # which at 512 connections fell entirely inside authentication, so the published attribution
      # measured Argon2 rather than produce.
      marker = Path.join(dir, "measure.marker")
      probe = LoadtestProbes.watch_auth()
      on_exit(fn -> LoadtestProbes.stop_auth(probe) end)
      LoadtestProbes.watch_file(marker)

      r = run(scenario: :produce, connections: 3, batch: 2, warmup: 1, measure_marker: marker, topic: topic("marker"))

      assert_receive {:file_appeared, ^marker, appeared_at}, 1_000
      auths = LoadtestProbes.successful_auths()

      # The setup's three connections (its own, and the metadata and broker connections of the stream that
      # places the topic's segment) plus one per worker, whose stream opens on the routes the setup read.
      assert length(auths) == 6
      assert appeared_at >= List.last(auths) + 1_000 - LoadtestProbes.poll_ms()
      assert r.error_reasons == %{}
      assert r.errors == 0
    end

    test "marks the boundary itself, appearing neither before the warmup ends nor after the run", %{tmp_dir: dir} do
      # The marker is the measured window's left edge for the harness that samples CPU over it, so it
      # has to land ON warmup_end: early would hand the sampler part of the warmup, late would hand it
      # part of the measured work under another phase's attribution.
      marker = Path.join(dir, "boundary.marker")
      probe = LoadtestProbes.watch_auth()
      on_exit(fn -> LoadtestProbes.stop_auth(probe) end)
      LoadtestProbes.watch_file(marker)

      run(
        scenario: :produce,
        connections: 2,
        batch: 1,
        warmup: 1,
        duration: 2,
        measure_marker: marker,
        topic: topic("bound")
      )

      assert_receive {:file_appeared, ^marker, appeared_at}, 1_000
      ready = List.last(LoadtestProbes.successful_auths())

      assert appeared_at >= ready + 1_000 - LoadtestProbes.poll_ms(), "the marker appeared before the warmup ended"
      assert appeared_at <= ready + 1_500, "the marker appeared after the measured window had started"
    end

    test "is never recorded in the reproduce command", %{tmp_dir: dir} do
      # It names a directory on the machine that ran the load, so a published command carrying it would
      # refuse to start anywhere else.
      command = Loadtest.reproduce_command(measure_marker: Path.join(dir, "m"))

      refute command =~ "measure"
    end

    test "a directory that does not exist is a named error before any connection opens", %{tmp_dir: dir} do
      probe = LoadtestProbes.watch_auth()
      on_exit(fn -> LoadtestProbes.stop_auth(probe) end)

      assert_raise ArgumentError, ~r/measure_marker directory .* does not exist or is not writable/, fn ->
        run(scenario: :produce, connections: 2, measure_marker: Path.join([dir, "missing", "m"]))
      end

      assert LoadtestProbes.successful_auths() == []
    end

    test "a marker that already exists is overwritten rather than refused", %{tmp_dir: dir} do
      marker = Path.join(dir, "stale.marker")
      File.write!(marker, "from an earlier run")

      r = run(scenario: :produce, connections: 1, batch: 1, measure_marker: marker, topic: topic("marker_exists"))

      assert File.read!(marker) == ""
      assert r.error_reasons == %{}
      assert r.errors == 0
    end

    test "a marker path that is a directory is a named error before any connection opens", %{tmp_dir: dir} do
      # Checking only the parent directory let this through, and it then failed inside File.write!, after
      # every connection had authenticated and the warmup had run.
      marker = Path.join(dir, "marker.d")
      File.mkdir_p!(marker)
      probe = LoadtestProbes.watch_auth()
      on_exit(fn -> LoadtestProbes.stop_auth(probe) end)

      assert_raise ArgumentError, ~r/exists and is not a writable regular file \(type directory/, fn ->
        run(scenario: :produce, connections: 2, measure_marker: marker)
      end

      assert LoadtestProbes.successful_auths() == []
    end

    test "a marker that exists and cannot be written is a named error", %{tmp_dir: dir} do
      marker = Path.join(dir, "read-only.marker")
      File.write!(marker, "")
      File.chmod!(marker, 0o444)
      on_exit(fn -> File.chmod(marker, 0o644) end)

      assert_raise ArgumentError, ~r/exists and is not a writable regular file \(type regular, access read\)/, fn ->
        Loadtest.run(measure_marker: marker)
      end
    end

    test "an empty path is a named error", _context do
      assert_raise ArgumentError, ~r/measure_marker must be a non-empty path/, fn ->
        Loadtest.run(measure_marker: "")
      end
    end

    test "a read-only directory is a named error", %{tmp_dir: dir} do
      read_only = Path.join(dir, "ro")
      File.mkdir_p!(read_only)
      File.chmod!(read_only, 0o555)
      on_exit(fn -> File.chmod(read_only, 0o755) end)

      assert_raise ArgumentError, ~r/does not exist or is not writable/, fn ->
        Loadtest.run(measure_marker: Path.join(read_only, "m"))
      end
    end

    test "the mix task accepts --measure-marker and turns a bad one into a Mix error", %{tmp_dir: dir} do
      assert_raise Mix.Error, ~r/measure_marker directory/, fn ->
        Mix.Tasks.Malachi.Loadtest.run(["--measure-marker", Path.join([dir, "missing", "m"])])
      end
    end
  end

  describe "option validation and edge cases" do
    test "zero or negative counts are rejected up front with a named error" do
      assert_raise ArgumentError, ~r/connections must be a positive integer/, fn ->
        Loadtest.run(connections: 0)
      end

      assert_raise ArgumentError, ~r/duration must be a positive integer/, fn ->
        Loadtest.run(duration: 0)
      end

      assert_raise ArgumentError, ~r/batch must be a positive integer/, fn ->
        Loadtest.run(batch: -1)
      end

      assert_raise ArgumentError, ~r/warmup must be a non-negative integer/, fn ->
        Loadtest.run(warmup: -1)
      end
    end

    test "an unknown connect strategy and cross-strategy pacing knobs are named errors up front" do
      assert_raise ArgumentError, ~r/unknown connect_strategy :warp/, fn ->
        Loadtest.run(connect_strategy: :warp)
      end

      # Each pacing knob only applies to the strategy that reads it; accepting it silently would let a
      # run claim a pacing it never applied.
      assert_raise ArgumentError, ~r/connect_concurrency only applies to the :bounded/, fn ->
        Loadtest.run(connect_strategy: :stagger, connect_concurrency: 8)
      end

      assert_raise ArgumentError, ~r/connect_stagger_ms only applies to the :stagger/, fn ->
        Loadtest.run(connect_stagger_ms: 50)
      end

      assert_raise ArgumentError, ~r/connect_stagger_ms only applies to the :stagger/, fn ->
        Loadtest.run(connect_strategy: :all_at_once, connect_stagger_ms: 50)
      end

      assert_raise ArgumentError, ~r/connect_concurrency must be a positive integer/, fn ->
        Loadtest.run(connect_concurrency: 0)
      end
    end

    test "an unknown scenario or payload, a misplaced or bad seed and a json size too small are named errors" do
      assert_raise ArgumentError, ~r/unknown scenario :nope \(expected one of:/, fn -> Loadtest.run(scenario: :nope) end
      assert_raise ArgumentError, ~r/unknown payload :zip/, fn -> Loadtest.run(payload: :zip) end

      assert_raise ArgumentError, "payload_seed only applies to the :json and :random payloads", fn ->
        Loadtest.run(payload_seed: 3)
      end

      for seed <- [-1, 0x1_0000_0000, "7"] do
        assert_raise ArgumentError, ~r/payload_seed must be an integer from 0 to 4294967295/, fn ->
          Loadtest.run(payload: :random, payload_seed: seed)
        end
      end

      min = Payload.min_json_size()

      assert_raise ArgumentError, "json payload needs --record-size >= #{min}, got #{min - 1}", fn ->
        Loadtest.run(payload: :json, record_size: min - 1)
      end
    end

    test "the mix task names the choices of every enum flag it refuses" do
      for {flag, value, choices} <- [
            {"--scenario", "nope", "produce, fetch, mixed, stream, user, acl"},
            {"--connect-strategy", "warp", "bounded, stagger, all-at-once"},
            {"--payload", "zip", "constant, json, random"}
          ] do
        message = "unknown #{flag} #{inspect(value)} (expected one of: #{choices})"
        assert_raise Mix.Error, message, fn -> Mix.Tasks.Malachi.Loadtest.run([flag, value]) end
      end
    end

    test "the mix task turns every enum flag into its option and runs, and a setup failure into a Mix error" do
      t = topic("mix_enums")

      argv =
        ~w(--scenario produce --connect-strategy all-at-once --payload json --payload-seed 3 --connections 1) ++
          ~w(--duration 1 --warmup 0 --json --user admin --pass admin123 --port #{TCPAcceptorPool.port()} --topic #{t})

      output = capture_io(fn -> Mix.Tasks.Malachi.Loadtest.run(argv) end)
      assert %{"payload" => "json", "payload_seed" => 3, "errors" => 0} = Jason.decode!(output)
      assert Jason.decode!(output)["meta"]["command"] =~ "--connect-strategy=all-at-once"

      {:ok, closed} = :gen_tcp.listen(0, [])
      {:ok, port} = :inet.port(closed)
      :gen_tcp.close(closed)

      assert_raise Mix.Error, ~r/could not connect and authenticate to create the topic/, fn ->
        Mix.Tasks.Malachi.Loadtest.run(~w(--port #{port} --connections 1 --duration 1))
      end
    end

    test "prepopulate smaller than batch seeds nothing instead of sending spurious batches" do
      # 1..0 without an explicit step enumerates DOWN and used to send two batches; the //1 step keeps
      # the range empty, so the fetch finds a genuinely empty backlog.
      t = topic("tinyprep")
      r = run(scenario: :fetch, connections: 2, batch: 10, prepopulate: 5, max: 50, topic: t)
      assert r.error_reasons == %{}
      assert r.errors == 0
      assert r.records == 0, "a 5-record prepopulate with batch 10 must seed nothing, read #{r.records}"
    end

    test "a cert without a key (and the reverse) is a named error before connecting" do
      assert {:error, :cert_requires_key} = Conn.connect(tls: true, cert: "client.pem")
      assert {:error, :cert_requires_key} = Conn.connect(tls: true, key: "client-key.pem")
    end
  end

  test "token auth path: a bad token is rejected (the auth failure surfaces, not a silent hang)" do
    # The harness issues no tokens, so we can only exercise rejection: a bad token fails the setup auth,
    # which surfaces as a raised error rather than hanging. catch_error covers the MatchError/exit either way.
    assert catch_error(run(scenario: :produce, connections: 1, token: "not-a-real-token", topic: topic("tok")))
  end

  describe "resilience" do
    # An admin worker is one connection in a closed loop: a drop is counted, and the worker reconnects and
    # goes on. The stub closes the connection on the first create_user it ever sees and answers the rest.
    test "a worker reconnects after its connection drops and keeps going" do
      with_stream_server(%{Wire.create_user_key() => {:after, 1, :close, :default}}, fn port ->
        r = run(port: port, scenario: :user, connections: 1, prepopulate: 0, topic: "recon")

        assert r.dropped == 1, "the forced drop should be counted"
        assert r.error_reasons == %{}
        assert r.errors == 0
        assert r.ops > 0, "after reconnecting the worker should complete ops"
      end)
    end
  end

  describe "a dropped connection is closed when a replacement takes over" do
    # Every create_user closes the connection, so the worker drops and reconnects about once per backoff
    # for the whole run. A dropped connection that is replaced without being closed keeps its port until
    # the worker exits, so the ports one worker holds climb with every drop; closed, they stay at one or two.
    test "in the closed loop" do
      {r, held} =
        max_ports_per_process(fn ->
          with_stream_server(%{Wire.create_user_key() => :close}, fn port ->
            run(port: port, scenario: :user, connections: 1, prepopulate: 0, topic: "closed_drops")
          end)
        end)

      assert r.dropped >= 5, "the server did not drop the connection often enough for this to show anything"
      assert held <= 3, "one process held #{held} ports at once: dropped connections were left open"
    end
  end

  test "a reconnect whose authentication is refused is closed, not left open" do
    # The setup's three connections (its own, and the metadata and broker connections of the stream that
    # places the topic's segment) and the worker's authenticate; the worker's first create_user closes its
    # connection, and every reconnect after that is authenticated with a refusal on a socket the stub keeps
    # open. A refused connection dropped without being closed stays open until the worker exits, one per
    # attempt, up to the retry cap.
    overrides = %{Wire.auth_key() => {:after, 4, {:error, "unauthorized"}}, Wire.create_user_key() => :close}

    {r, held} =
      max_ports_per_process(fn ->
        with_stream_server(overrides, fn port ->
          run(port: port, scenario: :user, connections: 1, prepopulate: 0, topic: "refused_reconnects")
        end)
      end)

    # an accepted reconnect would reach the closing create_user again and count a second drop
    assert r.dropped == 1, "every reconnect was meant to be refused"
    assert held <= 3, "one process held #{held} ports at once: refused reconnects were left open"
  end

  # Runs `fun` and returns its result with the most TCP ports any one process other than the caller owned at
  # the same time while it ran, sampled every 10 ms. A closed port leaves the table at once, so only ports
  # still open count. The caller is left out: a run's setup holds its own connections in it (three at once,
  # the setup's and the two of the stream that places the topic's segment), and the drops measured are a
  # worker's.
  defp max_ports_per_process(fun) do
    caller = self()
    sampler = spawn_link(fn -> sample_ports(caller, 0) end)
    result = fun.()
    send(sampler, {:stop, self()})
    receive(do: ({:max_ports, held} -> {result, held}))
  end

  defp sample_ports(caller, max) do
    receive do
      {:stop, from} -> send(from, {:max_ports, max})
    after
      10 ->
        held =
          for port <- :erlang.ports(),
              :erlang.port_info(port, :name) == {:name, ~c"tcp_inet"},
              {:connected, owner} <- [:erlang.port_info(port, :connected)],
              owner != caller,
              reduce: %{} do
            acc -> Map.update(acc, owner, 1, &(&1 + 1))
          end

        sample_ports(caller, Enum.max([max | Map.values(held)]))
    end
  end

  describe "error reasons" do
    # Issue #181: a healthy node once answered five produces with an error frame and the report said only
    # `errors: 5`. These cases pin that every genuine error keeps the reason the server gave, that the
    # reasons add up to `errors`, and that the two backpressure refusals stay out of them.

    test "an append refused for a reason the generator does not know is counted under that reason" do
      # The cycle mixes an acknowledgement, the two refusals that have counters of their own, and two genuine
      # reasons, one of them the stringified tuple the frontend sends for a term it does not normalize.
      cycle = [
        :default,
        {:refuse_append, "replication_timeout"},
        {:refuse_append, "overloaded"},
        {:refuse_append, "{:sealed, 12}"},
        {:refuse_append, "rate_limited"}
      ]

      for pipeline <- [1, 4] do
        r =
          with_stream_server(%{27 => {:cycle, cycle}}, fn port ->
            run(port: port, scenario: :produce, connections: 2, batch: 5, pipeline: pipeline, topic: "why")
          end)

        assert r.errors > 0, "pipeline #{pipeline}: the scripted refusals never reached the report"
        assert Map.keys(r.error_reasons) == ["replication_timeout", "{:sealed, 12}"], "pipeline #{pipeline}"
        assert Enum.sum(Map.values(r.error_reasons)) == r.errors, "pipeline #{pipeline}: reasons must add up to errors"
        assert r.overloaded > 0 and r.rate_limited > 0, "pipeline #{pipeline}: refusals keep their own counters"
        assert r.records == r.ops * 5
      end
    end

    test "fetch, user and acl errors keep their reason too" do
      for {scenario, key} <- [{:fetch, 31}, {:user, Wire.create_user_key()}, {:acl, Wire.grant_acl_key()}] do
        r =
          with_stream_server(%{key => {:error, "no_such_range"}}, fn port ->
            run(port: port, scenario: scenario, connections: 1, prepopulate: 0, topic: "why_#{scenario}")
          end)

        assert r.errors > 0, "#{scenario}: the scripted refusal never reached the report"
        assert r.error_reasons == %{"no_such_range" => r.errors}, "#{scenario}"
      end
    end

    test "a consume stream the server sheds is counted apart, under its own refusal counter, warmup or not" do
      # `overloaded` and `rate_limited` keep their own counters instead of entering the reason breakdown,
      # and a refusal at the open, which comes at the barrier, is counted inside the warmup too.
      r =
        with_stream_server(%{29 => {:error, "rate_limited"}}, fn port ->
          run(port: port, scenario: :stream, connections: 1, prepopulate: 0, warmup: 1, topic: "shed_stream")
        end)

      assert r.rate_limited == 1
      assert r.errors == 0
      assert r.error_reasons == %{}
    end

    test "an admin cleanup the server refuses is an error with its reason, not a completed op" do
      # The second half of a user or acl cycle used to be matched with the response code ignored, so a
      # refused delete or revoke counted as a successful op and left the user or the grant behind.
      for {scenario, cleanup_key} <- [{:user, Wire.delete_user_key()}, {:acl, Wire.revoke_acl_key()}] do
        r =
          with_stream_server(%{cleanup_key => {:error, "permission_denied"}}, fn port ->
            run(port: port, scenario: scenario, connections: 1, topic: "cleanup_#{scenario}")
          end)

        assert r.errors > 0, "#{scenario}: the refused cleanup was not counted"
        assert r.error_reasons == %{"permission_denied" => r.errors}, "#{scenario}"
        assert r.ops == 0, "#{scenario}: a cycle whose cleanup was refused is not a completed op"
      end
    end

    test "an admin cleanup that loses the connection is a drop, not a MatchError" do
      for {scenario, cleanup_key} <- [{:user, Wire.delete_user_key()}, {:acl, Wire.revoke_acl_key()}] do
        r =
          with_stream_server(%{cleanup_key => :close}, fn port ->
            run(port: port, scenario: scenario, connections: 1, topic: "lost_#{scenario}")
          end)

        assert r.dropped > 0, "#{scenario}: the lost connection was not counted as a drop"
        assert r.errors == 0, "#{scenario}: a transport failure is a drop, not a server error"
      end
    end

    test "no reason table outlives a run, whether it completes or fails to connect" do
      run(scenario: :produce, connections: 1, topic: topic("no_leak"))
      assert reason_tables() == []

      # the setup's three connections authenticate, and every worker's is refused
      with_stream_server(%{Wire.auth_key() => {:after, 3, {:error, "unauthorized"}}}, fn port ->
        assert_raise Loadtest.SetupError, ~r/3 of 3 connections failed/, fn ->
          run(port: port, scenario: :user, connections: 3, topic: "no_leak")
        end
      end)

      assert reason_tables() == []
    end
  end

  describe "setup refusals" do
    test "a refused topic creation fails setup naming the topic and the reason" do
      # create_topic is gated by :produce, so a consume-only user is refused it.
      {user, pass} = add_user([:consume])
      t = topic("denied_create")

      error =
        assert_raise Loadtest.SetupError, fn ->
          Loadtest.run(
            port: TCPAcceptorPool.port(),
            user: user,
            pass: pass,
            scenario: :produce,
            connections: 1,
            duration: 1,
            topic: t
          )
        end

      assert error.message =~ t
      assert error.message =~ "permission_denied"
    end

    test "a connection lost while creating the topic fails setup with a SetupError, not a MatchError" do
      error =
        assert_raise Loadtest.SetupError, fn ->
          with_stream_server(%{Wire.create_topic_key() => :close}, fn port ->
            run(port: port, connections: 1, topic: "lost")
          end)
        end

      assert error.message =~ "lost"
      assert error.message =~ "connection failed"
    end

    test "a topic that already exists is not a setup failure" do
      t = topic("rerun")
      run(scenario: :produce, connections: 1, topic: t)
      r = run(scenario: :produce, connections: 1, topic: t)

      assert r.error_reasons == %{}
      assert r.ops > 0
    end
  end

  describe "connect strategies and setup failures" do
    test "every connect strategy completes a run against the real server" do
      # bounded with concurrency 1 fully serializes the gate (grant -> connect -> release -> next), so a
      # deadlock or a lost grant would hang this test rather than pass it.
      for opts <- [
            [connect_strategy: :bounded, connect_concurrency: 1],
            [connect_strategy: :stagger, connect_stagger_ms: 1],
            [connect_strategy: :all_at_once]
          ] do
        r = run([scenario: :produce, connections: 4, batch: 2, topic: topic("strat")] ++ opts)
        assert r.error_reasons == %{}, "#{inspect(opts)} should complete cleanly"
        assert r.errors == 0, "#{inspect(opts)} should complete cleanly"
        assert r.ops > 0
      end
    end

    test "a server that is not there fails setup with a SetupError, not a crash" do
      # Grab a port the OS just released, so the connect is refused instantly.
      {:ok, listen} = :gen_tcp.listen(0, [:binary])
      {:ok, dead_port} = :inet.port(listen)
      :gen_tcp.close(listen)

      assert_raise Loadtest.SetupError, ~r/could not connect and authenticate to create the topic/, fn ->
        Loadtest.run(port: dead_port, user: "admin", pass: "admin123", connections: 2, duration: 1)
      end
    end

    test "workers that cannot connect fail the run with a SetupError naming how many, not a MatchError" do
      # A stub that authenticates the first four connections and refuses the rest. The setup's three (its
      # own, and the metadata and broker connections of the stream that places the topic's segment) always
      # come first, so ONE worker gets through while the other two fail: the run must abort cleanly
      # counting 2 of 3, and must not report a measurement taken with fewer connections than requested.
      with_stream_server(%{Wire.auth_key() => {:after, 4, {:error, "unauthorized"}}}, fn port ->
        assert_raise Loadtest.SetupError, ~r/2 of 3 connections failed to connect and authenticate/, fn ->
          run(port: port, scenario: :user, connections: 3, topic: "limited")
        end
      end)
    end
  end

  # --- helpers for the error-reason and setup-refusal tests ---

  # A user with `permissions` on the real server, removed when the test ends.
  defp add_user(permissions) do
    user = "lt_user_#{System.unique_integer([:positive])}"
    pass = "Lt-Pass-1!"
    Malachi.Auth.add_user(user, pass, permissions)
    on_exit(fn -> Malachi.Auth.remove_user(user) end)
    {user, pass}
  end

  # Every reason table a run may have left behind (the generator names its table, even unregistered).
  defp reason_tables, do: Enum.filter(:ets.all(), &(:ets.info(&1, :name) == :loadtest_error_reasons))
end
