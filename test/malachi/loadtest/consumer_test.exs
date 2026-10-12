defmodule Malachi.Loadtest.ConsumerTest do
  # The routing consumer and fetcher the load generator reads through, against the real TCP server with the
  # `producer_streams` flag on: every record of every range read, under a window smaller than the topic;
  # records written after the read started; a split's children read from the position read in the parent,
  # a merge's range from the start of its history; a dropped connection read again from where it stopped;
  # refusals and redirects.
  #
  # async: false. The flag is the application's own cache (`ClusterFlagsPause`), and the topic routing reads
  # the topology the consumer-group router publishes, which is cleared for each test and put back.
  use ExUnit.Case, async: false

  alias Malachi.BrokerServer
  alias Malachi.DataPlaneRouter
  alias Malachi.Loadtest.Conn
  alias Malachi.Loadtest.Consumer
  alias Malachi.Loadtest.Fetcher
  alias Malachi.Loadtest.Links
  alias Malachi.Loadtest.Producer
  alias Malachi.Loadtest.ReadLanes
  alias Malachi.Loadtest.Router
  alias Malachi.Log.Record
  alias Malachi.TCPAcceptorPool
  alias Malachi.Test.StreamsOn
  alias Malachi.Test.StreamStub
  alias Malachi.Wire
  alias Malachi.Wire.Batch

  setup do
    :ok = StreamsOn.setup(%{})

    topic = "consumer_#{System.unique_integer([:positive])}"
    admin = connect!(user: "admin", pass: "admin123")
    {:ok, 0, _, admin} = Conn.request(admin, Wire.create_topic_key(), 2, Wire.encode_create_topic_req(topic, 4))
    {:ok, router, admin} = Router.bootstrap(Router.new({"127.0.0.1", TCPAcceptorPool.port()}), admin)
    Conn.close(admin)

    %{topic: topic, router: router}
  end

  defp conn_opts(opts \\ []), do: Keyword.merge([port: TCPAcceptorPool.port(), user: "app", pass: "app123"], opts)

  defp connect!(opts) do
    opts = conn_opts(opts)
    {:ok, conn} = Conn.connect(opts)
    {:ok, conn} = Conn.authenticate(conn, opts)
    conn
  end

  defp now, do: System.monotonic_time(:millisecond)

  # Writes `values` (keyed by themselves) and waits until each is acknowledged.
  defp produce!(ctx, values) do
    p = Producer.new(ctx.router, conn_opts(), max_inflight: 8)
    {p, []} = Producer.produce(p, ctx.topic, Enum.map(values, &{&1, Record.new(&1, key: &1)}), now())
    {p, events, :idle} = Producer.await(p, 5_000)
    Producer.close(p)
    assert length(for {:acked, entries, _} <- events, entry <- entries, do: entry) == length(values)
  end

  defp consumer(ctx, opts \\ []) do
    {conn_opts, opts} = Keyword.split(opts, [:user, :pass])
    c = Consumer.new(ctx.router, conn_opts(conn_opts), opts)
    on_exit(fn -> Consumer.close(c) end)
    c
  end

  defp split!(ctx, range), do: BrokerServer.split_range(DataPlaneRouter.shard_for(ctx.topic), {ctx.topic, range})

  # Reads until `total` records have come, or `timeout_ms`: the consumer and the records counted.
  defp read_all(c, total, timeout_ms \\ 5_000), do: read_loop(c, total, now() + timeout_ms, 0)

  defp read_loop(c, total, _deadline, read) when read >= total, do: {c, read}

  defp read_loop(c, total, deadline, read) do
    left = deadline - now()

    if left <= 0 do
      {c, read}
    else
      receive do
        message ->
          case Consumer.handle_message(c, message, now()) do
            {:ok, c, events} -> read_loop(c, total, deadline, read + records(events))
            :no -> read_loop(c, total, deadline, read)
          end
      after
        min(left, Consumer.next_timeout(c, now())) ->
          {c, events} = Consumer.tick(c, now())
          read_loop(c, total, deadline, read + records(events))
      end
    end
  end

  defp records(events),
    do:
      events
      |> Enum.flat_map(fn
        {:records, _key, n} -> [n]
        _failed -> []
      end)
      |> Enum.sum()

  describe "Consumer" do
    test "reads every record of the topic, through a window smaller than it", ctx do
      values = for i <- 1..30, do: "v#{i}"
      produce!(ctx, values)

      {:ok, c, early} = Consumer.subscribe(consumer(ctx, window: 4, max: 2), ctx.topic, 5_000)
      {_c, read} = read_all(c, 30 - records(early))
      assert records(early) + read == 30
    end

    test "records written after the read started arrive too", ctx do
      {:ok, c, []} = Consumer.subscribe(consumer(ctx), ctx.topic, 5_000)
      produce!(ctx, ["late-1", "late-2"])
      {_c, read} = read_all(c, 2)
      assert read == 2
    end

    test "a split's children read on from the position read in the parent, not from the start", ctx do
      produce!(ctx, for(i <- 1..10, do: "before-#{i}"))
      {:ok, c, early} = Consumer.subscribe(consumer(ctx), ctx.topic, 5_000)
      {c, rest} = read_all(c, 10 - records(early))
      assert records(early) + rest == 10

      {:ok, _, _} = split!(ctx, 0)
      produce!(ctx, for(i <- 1..10, do: "after-#{i}"))
      {c, read} = read_all(c, 10)

      assert read == 10
      # nothing more: the children did not read the parent's records again
      assert {_c, 0} = read_all(c, 1, 500)
      assert %{moved: 1} = Consumer.stats(c)
    end

    test "a merge's range reads from the start of its history: at least once", ctx do
      {:ok, {_, left}, {_, right}} = split!(ctx, 0)
      produce!(ctx, for(i <- 1..10, do: "v#{i}"))
      {:ok, c, early} = Consumer.subscribe(consumer(ctx), ctx.topic, 5_000)
      {c, rest} = read_all(c, 10 - records(early))
      assert records(early) + rest == 10

      {:ok, _merged} =
        BrokerServer.merge_ranges(DataPlaneRouter.shard_for(ctx.topic), {ctx.topic, left}, {ctx.topic, right})

      {c, read} = read_all(c, 10)
      assert read == 10
      assert %{moved: moved} = Consumer.stats(c)
      assert moved >= 1
    end

    test "a merge back into a block read before its split reads on from where that read stopped", ctx do
      # a range read to its end, split, its children read on, then merged back: the merged range's
      # history starts with the range read first, so it opens past its records and reads the children's again
      produce!(ctx, for(i <- 1..10, do: "before-#{i}"))
      {:ok, c, early} = Consumer.subscribe(consumer(ctx), ctx.topic, 5_000)
      {c, rest} = read_all(c, 10 - records(early))
      assert records(early) + rest == 10

      {:ok, {_, left}, {_, right}} = split!(ctx, 0)
      produce!(ctx, for(i <- 1..4, do: "split-#{i}"))
      {c, 4} = read_all(c, 4)

      {:ok, _merged} =
        BrokerServer.merge_ranges(DataPlaneRouter.shard_for(ctx.topic), {ctx.topic, left}, {ctx.topic, right})

      # the children's four again, at least once, and none of the ten before the split
      {c, read} = read_all(c, 100, 1_500)
      assert read == 4
      assert %{moved: 3} = Consumer.stats(c)
    end

    test "a dropped connection reads on from where it stopped", ctx do
      produce!(ctx, ["a", "b"])
      {:ok, c, early} = Consumer.subscribe(consumer(ctx), ctx.topic, 5_000)
      {c, rest} = read_all(c, 2 - records(early))
      assert records(early) + rest == 2

      [{socket, _address}] = Map.to_list(c.sockets)
      {:ok, c, []} = Consumer.handle_message(c, {:tcp_closed, socket}, now())
      assert %{dropped: 1} = Consumer.stats(c)

      produce!(ctx, ["c"])
      {c, read} = read_all(c, 1)
      assert read == 1
      assert {_c, 0} = read_all(c, 1, 500)
    end

    test "pages pushed while the streams are still opening come back from subscribe, counted" do
      # each open is answered and followed at once by a page: the first range's page comes before the
      # second range's answer, so subscribe has read it before it returns
      stub = StreamStub.start(%{29 => {:push_on_open, ["a", "b", "c"]}})
      on_exit(fn -> StreamStub.stop(stub) end)
      StreamStub.set_routes(stub, 1, [{0, 0, 128}, {1, 128, 256}])
      opts = [port: stub.port, user: "u", pass: "p"]
      {:ok, conn} = Conn.connect(opts)
      {:ok, conn} = Conn.authenticate(conn, opts)
      {:ok, router, conn} = Router.bootstrap(Router.new({"127.0.0.1", stub.port}), conn)
      Conn.close(conn)

      {:ok, c, early} = Consumer.subscribe(Consumer.new(router, opts), "t", 2_000)
      assert records(early) >= 3
      {_c, rest} = read_all(c, 6 - records(early), 1_000)
      assert records(early) + rest == 6
    end

    test "an open the broker refuses is the refusal", ctx do
      assert {:error, "permission_denied", _c} =
               Consumer.subscribe(consumer(ctx, user: "producer", pass: "producer123"), ctx.topic, 300)
    end

    test "an open against stale routes reads them again and opens where they say", ctx do
      stale =
        Router.put_routes(ctx.router, %{
          topic: ctx.topic,
          version: 1,
          keyspace_bits: 4,
          ranges: [%{range: 0, key_start: 0, key_end: 16, state: :active, segment: nil}]
        })

      assert {:ok, c, []} = Consumer.subscribe(consumer(%{ctx | router: stale}), ctx.topic, 5_000)
      assert %{redirects: 1} = Consumer.stats(c)
    end

    test "a message for no connection of the consumer's is not taken", ctx do
      c = consumer(ctx)
      assert Consumer.handle_message(c, {:tcp, make_ref(), "x"}, now()) == :no
      assert Consumer.next_timeout(c, now()) == :infinity
    end
  end

  describe "Fetcher" do
    defp fetcher(ctx, opts \\ []) do
      {conn_opts, opts} = Keyword.split(opts, [:user, :pass])
      f = Fetcher.new(ctx.router, conn_opts(conn_opts), opts)
      on_exit(fn -> Fetcher.close(f) end)
      f
    end

    defp fetch_n(f, topic, n), do: Enum.map_reduce(1..n, f, fn _, f -> f |> Fetcher.fetch(topic, now()) |> swap() end)
    defp swap({f, result}), do: {result, f}

    test "pages every record of every range, then starts each range over at its end", ctx do
      {:ok, _, _} = split!(ctx, 0)
      produce!(ctx, for(i <- 1..20, do: "v#{i}"))
      {:ok, f} = Fetcher.prepare(fetcher(ctx, max: 100), ctx.topic)

      # one page per range brings everything; the next finds nothing new and starts over; the one after
      # reads it all again
      {results, _f} = fetch_n(f, ctx.topic, 6)
      counts = for {:ok, n, _latency} <- results, do: n
      assert length(counts) == 6
      assert Enum.sum(Enum.take(counts, 2)) == 20
      assert Enum.slice(counts, 2, 2) == [0, 0]
      assert Enum.sum(Enum.slice(counts, 4, 2)) == 20
    end

    test "a split's children are fetched on from the position read in the parent", ctx do
      produce!(ctx, for(i <- 1..10, do: "before-#{i}"))
      f = fetcher(ctx)
      {f, {:ok, 10, _}} = Fetcher.fetch(f, ctx.topic, now())

      {:ok, _, _} = split!(ctx, 0)
      produce!(ctx, for(i <- 1..10, do: "after-#{i}"))
      {results, _f} = fetch_n(f, ctx.topic, 4)

      assert :redirected in results
      assert Enum.sum(for {:ok, n, _latency} <- results, do: n) == 10
    end

    test "a refusal is the error, a dropped connection is a drop, and the next fetch connects again", ctx do
      assert {_f, {:error, "permission_denied"}} =
               Fetcher.fetch(fetcher(ctx, user: "producer", pass: "producer123"), ctx.topic, now())

      produce!(ctx, ["a"])
      f = fetcher(ctx)
      {f, {:ok, 1, _}} = Fetcher.fetch(f, ctx.topic, now())
      [{_address, %{conn: conn}}] = Map.to_list(f.conns)
      Conn.close(conn)

      assert {f, :dropped} = Fetcher.fetch(f, ctx.topic, now())
      assert {_f, {:ok, _, _}} = Fetcher.fetch(f, ctx.topic, now())
    end

    test "a fetch against stale routes is a redirect, and the next one reads", ctx do
      produce!(ctx, ["a"])

      stale =
        Router.put_routes(ctx.router, %{
          topic: ctx.topic,
          version: 1,
          keyspace_bits: 4,
          ranges: [%{range: 0, key_start: 0, key_end: 16, state: :active, segment: nil}]
        })

      f = fetcher(%{ctx | router: stale})
      assert {f, :redirected} = Fetcher.fetch(f, ctx.topic, now())
      assert {f, {:ok, 1, _}} = Fetcher.fetch(f, ctx.topic, now())
      assert %{redirects: 1} = Fetcher.stats(f)
    end

    test "max must be positive" do
      assert_raise ArgumentError, ~r/max must be a positive integer/, fn ->
        Fetcher.new(Router.new({"127.0.0.1", 1}), [], max: 0)
      end
    end
  end

  describe "edges" do
    defp closed_port do
      {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false])
      {:ok, port} = :inet.port(listen)
      :ok = :gen_tcp.close(listen)
      port
    end

    # A router whose ranges (one, or two halves) are led by `primary` at `address` (nil host: advertises
    # nothing), in a cluster of two brokers.
    defp router_with(primary_host, primary_port, ranges \\ [{0, 0, 256}]) do
      Router.new({"127.0.0.1", closed_port()})
      |> Router.put_cluster_state(%{
        brokers: [
          %{id: "a@h", host: primary_host, port: primary_port, status: :alive},
          %{id: "b@h", host: "127.0.0.1", port: closed_port(), status: :alive}
        ],
        streams_enabled: true
      })
      |> Router.put_routes(%{
        topic: "t",
        version: 1,
        keyspace_bits: 8,
        ranges:
          for {range, key_start, key_end} <- ranges do
            %{
              range: range,
              key_start: key_start,
              key_end: key_end,
              state: :active,
              segment: %{segment: 0, primary: "a@h"}
            }
          end
      })
    end

    defp stub_router(stub) do
      conn_opts = [port: stub.port, user: "u", pass: "p"]
      {:ok, conn} = Conn.connect(conn_opts)
      {:ok, conn} = Conn.authenticate(conn, conn_opts)
      {:ok, router, conn} = Router.bootstrap(Router.new({"127.0.0.1", stub.port}), conn)
      Conn.close(conn)
      {router, conn_opts}
    end

    defp stub_consumer(stub, opts \\ []) do
      {router, conn_opts} = stub_router(stub)
      c = Consumer.new(router, conn_opts, opts)
      on_exit(fn -> Consumer.close(c) end)
      c
    end

    defp start_stub(overrides \\ %{}) do
      stub = StreamStub.start(overrides)
      on_exit(fn -> StreamStub.stop(stub) end)
      stub
    end

    test "a topic the server does not know is the refusal", ctx do
      assert {:error, "no_such_topic", _c} =
               Consumer.subscribe(consumer(ctx), "missing_#{System.unique_integer([:positive])}", 1_000)

      assert {:error, "no_such_topic", _c} =
               Consumer.prepare(consumer(ctx), "missing_#{System.unique_integer([:positive])}")

      f = fetcher(ctx)
      assert {:error, "no_such_topic", _f} = Fetcher.prepare(f, "missing_#{System.unique_integer([:positive])}")
      assert {_f, {:error, "no_such_topic"}} = Fetcher.fetch(f, "missing_#{System.unique_integer([:positive])}", now())
    end

    test "a primary that advertises nothing is a redirect for both readers, and fails a prepare" do
      router = router_with(nil, 0)
      c = Consumer.new(router, [])
      # no failure: a redirect, after which the routes are read again (nothing answers them here)
      assert {:error, "metadata_unavailable", c} = Consumer.subscribe(c, "t", 300)
      assert %{redirects: 1} = Consumer.stats(c)
      assert {:error, "unadvertised", _c} = Consumer.prepare(Consumer.new(router, []), "t")

      f = Fetcher.new(router, [])
      assert {:error, "unadvertised", _f} = Fetcher.prepare(f, "t")
      at = now()
      assert {f, :redirected} = Fetcher.fetch(f, "t", at)
      assert %{redirects: 1} = Fetcher.stats(f)
      # the routes are read again for the next try, but only once a backoff ends: until then nothing is
      # asked, and the fetch says how long to wait
      assert {f, {:held, ms}} = Fetcher.fetch(f, "t", at + 1)
      # the backoff runs from when the fetch gave up, which may be a few ms past `at`
      assert ms in 1..(200 + now() - at)
      assert {_f, {:held, 1}} = Fetcher.fetch(f, "t", at + 1 + ms - 1)
    end

    test "routes that cannot be read are tried once, then held until their backoff ends" do
      # nothing listens anywhere: the first try reads the routes and fails, the next ones wait
      router = router_with("127.0.0.1", closed_port())
      f = Fetcher.new(Router.forget(router, "t"), [])
      at = now()
      assert {f, {:error, "metadata_unavailable"}} = Fetcher.fetch(f, "t", at)
      assert %{refreshes: 1} = Fetcher.stats(f)
      assert {f, {:held, _ms}} = Fetcher.fetch(f, "t", at)
      assert %{refreshes: 1} = Fetcher.stats(f)

      # once it ends, the routes are read again
      assert {f, {:error, "metadata_unavailable"}} = Fetcher.fetch(f, "t", at + 10_000)
      assert %{refreshes: 2} = Fetcher.stats(f)
    end

    test "a broker that takes no connection is retried, and counted, for both readers" do
      router = router_with("127.0.0.1", closed_port())
      # the failed connect forgets the routes, and they cannot be read again (nothing listens anywhere)
      assert {:error, "metadata_unavailable", c} = Consumer.subscribe(Consumer.new(router, []), "t", 300)
      assert %{connect_failures: n} = Consumer.stats(c)
      assert n >= 1
      assert {:error, "connect_failed", _c} = Consumer.prepare(Consumer.new(router, []), "t")

      f = Fetcher.new(router, [])
      assert {:error, "connect_failed", _f} = Fetcher.prepare(f, "t")
      assert {_f, {:error, "connect_failed"}} = Fetcher.fetch(f, "t", now())
    end

    test "a range left to open after the routes were forgotten for a failed connect waits for them, with no event" do
      # the first range's connect fails and forgets the routes; the second, in the same pass, has none to go by
      router = router_with("127.0.0.1", closed_port(), [{0, 0, 128}, {1, 128, 256}])
      c = %{Consumer.new(router, []) | topics: ["t"]}
      assert {c, []} = Consumer.tick(c, now())
      assert %{connect_failures: 1} = Consumer.stats(c)
      assert Enum.all?(c.lanes, fn {_key, lane} -> lane.stream == nil and is_integer(lane.retry_at) end)
    end

    test "an open written to a connection that closed under it drops the connection and backs the range off" do
      stub = start_stub()
      {:ok, c, []} = Consumer.subscribe(stub_consumer(stub), "t", 2_000)
      [{_key, %{corr: corr}}] = Map.to_list(c.streams)
      [{socket, _address}] = Map.to_list(c.sockets)
      StreamStub.push(stub, Wire.encode_error(corr, :permission_denied))
      {c, _events, :idle} = Consumer.await(c, 2_000, &(&1.streams == %{}))

      # the socket closes on this side before the range opens again, so the open cannot be written
      :ok = :gen_tcp.close(socket)
      {c, []} = Consumer.tick(c, now() + 1_000)
      assert %{dropped: 1} = Consumer.stats(c)
      assert c.sockets == %{}
    end

    test "dropping a connection that is not there changes nothing" do
      client = %{conns: %{}, sockets: %{}}
      assert Links.drop_conn(client, {"127.0.0.1", 1}) == client
    end

    test "a range redirected time after time backs off from the fifth redirect" do
      stub = start_stub(%{29 => {:error, "moved"}})
      assert {:error, "open_timeout", c} = Consumer.subscribe(stub_consumer(stub), "t", 400)
      assert %{redirects: redirects} = Consumer.stats(c)
      # five at once, then 50, 100, 150 ms apart: a handful, not one per round trip
      assert redirects in 5..9
    end

    test "prepare connects to every range's broker and opens no stream" do
      stub = start_stub()
      StreamStub.set_routes(stub, 1, [{0, 0, 128}, {1, 128, 256}])
      assert {:ok, c} = Consumer.prepare(stub_consumer(stub), "t")
      assert map_size(c.conns) == 1
      assert c.streams == %{} and c.opening == %{} and c.lanes == %{}
    end

    test "an open answered by the broker closing the connection drops it and opens again, until the timeout" do
      stub = start_stub(%{29 => :close})
      assert {:error, "open_timeout", c} = Consumer.subscribe(stub_consumer(stub), "t", 500)
      assert %{dropped: dropped} = Consumer.stats(c)
      assert dropped >= 2
    end

    test "an open nobody answers is a timeout" do
      stub = start_stub(%{29 => :silent})
      assert {:error, "open_timeout", _c} = Consumer.subscribe(stub_consumer(stub), "t", 200)
    end

    test "frames about nothing the consumer reads, or of a kind it does not take, change nothing" do
      stub = start_stub()
      {:ok, c, []} = Consumer.subscribe(stub_consumer(stub), "t", 2_000)
      [{socket, _address}] = Map.to_list(c.sockets)

      page = %{
        stream_id: 99,
        next: {0, 1},
        skip: 0,
        backlog: 0,
        expired: 0,
        expired_exact: true,
        batch: Batch.encode([], :none)
      }

      ack = %{stream_id: 99, acked_sequence: 1, window_appends: 1, window_bytes: 1, errors: []}

      frames =
        Wire.encode_ok(7, IO.iodata_to_binary(Wire.encode_push(:records, page))) <>
          Wire.encode_ok(
            7,
            IO.iodata_to_binary(Wire.encode_push(:moved, %{stream_id: 99, reason: "x", routes_version: 0, targets: []}))
          ) <>
          Wire.encode_ok(7, IO.iodata_to_binary(Wire.encode_push(:append_ack, ack))) <>
          Wire.encode_error(7, :unknown_stream)

      assert {:ok, ^c, []} = Consumer.handle_message(c, {:tcp, socket, frames}, now())
      assert Consumer.handle_message(c, {:tcp_passive, socket}, now()) == :no
    end

    test "a stream the broker closes with a refusal fails its range, which opens again from where it read" do
      stub = start_stub(%{29 => {:push_on_open, ["a", "b", "c"]}})
      {:ok, c, early} = Consumer.subscribe(stub_consumer(stub), "t", 2_000)
      {c, rest} = read_all(c, 3 - records(early), 2_000)
      assert records(early) + rest == 3
      [{corr, :earliest}] = StreamStub.consumes(stub)
      StreamStub.push(stub, Wire.encode_error(corr, :permission_denied))

      {c, events, _} = Consumer.await(c, 2_000, &(&1.streams == %{}))
      assert {:failed, {"t", 0}, "permission_denied"} in events
      assert Consumer.next_timeout(c, now()) in 1..200

      # once the backoff is over, the range opens again past the three it read, and reads none of them again
      {c, events, :idle} = Consumer.await(c, 2_000, &(map_size(&1.streams) == 1))
      assert [{^corr, :earliest}, {_reopen, {:position, {0, 3}}}] = StreamStub.consumes(stub)
      {_c, more, _} = Consumer.await(c, 100, fn _ -> false end)
      assert records(events ++ more) == 0
    end

    test "with on_refusal: :stop a refused range is reported once and not read again; the others read on" do
      stub = start_stub(%{29 => {:push_on_open, ["a"]}})
      StreamStub.set_routes(stub, 1, [{0, 0, 128}, {1, 128, 256}])
      {:ok, c, early} = Consumer.subscribe(stub_consumer(stub, on_refusal: :stop), "t", 2_000)
      {c, rest} = read_all(c, 2 - records(early), 2_000)
      assert records(early) + rest == 2
      [{corr, :earliest}, _other] = StreamStub.consumes(stub)
      StreamStub.push(stub, Wire.encode_error(corr, :permission_denied))

      {c, events, _} = Consumer.await(c, 2_000, &(map_size(&1.streams) == 1))
      assert Enum.count(events, &match?({:failed, _, "permission_denied"}, &1)) == 1
      # nothing left to open again: no backoff to wait for, and well past one, no new open
      assert Consumer.next_timeout(c, now()) == :infinity
      {c, more, :timeout} = Consumer.await(c, 400, fn _ -> false end)
      assert more == []
      assert length(StreamStub.consumes(stub)) == 2
      assert map_size(c.streams) == 1
    end

    test "with on_refusal: :stop an open refused at once fails subscribe, without waiting out its time" do
      stub = start_stub(%{29 => {:error, "permission_denied"}})
      t0 = now()
      assert {:error, "permission_denied", _c} = Consumer.subscribe(stub_consumer(stub, on_refusal: :stop), "t", 5_000)
      assert now() - t0 < 2_000
    end

    test "start sends the opens and waits for none of them" do
      stub = start_stub(%{29 => {:delay, 300, :default}})
      {c, []} = Consumer.start(stub_consumer(stub), "t", now())
      assert c.streams == %{}
      assert map_size(c.opening) == 1
      {_c, _events, :idle} = Consumer.await(c, 2_000, &(map_size(&1.streams) == 1))
    end

    test "a move while the routes cannot be read holds the range until their backoff ends, then opens it" do
      stub = start_stub()
      {:ok, c, []} = Consumer.subscribe(stub_consumer(stub), "t", 2_000)
      [{{_address, id}, %{corr: corr}}] = Map.to_list(c.streams)
      StreamStub.set_override(stub, 25, {:error, "no_such_topic"})
      moved = %{stream_id: id, reason: "split", routes_version: 2, targets: []}
      StreamStub.push(stub, Wire.encode_ok(corr, IO.iodata_to_binary(Wire.encode_push(:moved, moved))))

      {c, [], :idle} = Consumer.await(c, 2_000, &(&1.streams == %{}))
      # held, not forgotten (no :infinity) and not spinning (no 0)
      assert Consumer.next_timeout(c, now()) in 1..200
      {c, []} = Consumer.tick(c, now())
      assert Consumer.next_timeout(c, now()) in 1..200

      StreamStub.set_override(stub, 25, :default)
      {_c, _events, :idle} = Consumer.await(c, 2_000, &(map_size(&1.streams) == 1))
    end

    test "a fetch's latency is its fetch_range round trip, not the route read before it" do
      # every route read takes 300 ms; the fetch_range itself is answered at once
      stub = start_stub(%{25 => {:delay, 300, :default}})
      {router, conn_opts} = stub_router(stub)
      f = Fetcher.new(router, conn_opts)
      t0 = now()
      assert {f, {:ok, 0, latency_us}} = Fetcher.fetch(f, "t", now())
      Fetcher.close(f)
      assert now() - t0 >= 300
      assert latency_us < 300_000
    end

    test "a range whose own backoff ended while the routes back off waits for theirs, rather than spin" do
      stub = start_stub()
      {:ok, c, []} = Consumer.subscribe(stub_consumer(stub), "t", 2_000)
      [{socket, _address}] = Map.to_list(c.sockets)
      StreamStub.set_override(stub, 25, {:error, "no_such_topic"})

      # the connection drops: the range backs off and its routes are forgotten
      {:ok, c, []} = Consumer.handle_message(c, {:tcp_closed, socket}, now())
      # well past the range's backoff the routes are read, and fail: the range waits for their backoff
      later = now() + 300
      {c, []} = Consumer.tick(c, later)
      assert Consumer.next_timeout(c, later) in 1..200
    end

    test "a topic whose routes could never be read is tried again once their backoff ends" do
      stub = start_stub(%{25 => {:error, "no_such_topic"}})
      {c, []} = Consumer.start(stub_consumer(stub), "t", now())
      assert c.lanes == %{}
      assert Consumer.next_timeout(c, now()) in 1..200

      StreamStub.set_override(stub, 25, :default)
      {_c, _events, :idle} = Consumer.await(c, 2_000, &(map_size(&1.streams) == 1))
    end

    test "a primary advertised only after the routes were read is opened once they are read again, even with on_refusal: :stop" do
      for on_refusal <- [:retry, :stop] do
        # the stub's broker advertises no address at first, then its own
        stub = start_stub()
        StreamStub.add_broker(stub, "other@127.0.0.1", closed_port())
        StreamStub.set_advertised_port(stub, 0)
        {c, []} = Consumer.start(stub_consumer(stub, on_refusal: on_refusal), "t", now())
        {c, [], :timeout} = Consumer.await(c, 300, fn _ -> false end)
        # read again once a backoff ends, not at once: one redirect, a second 200 ms on
        assert %{redirects: redirects} = Consumer.stats(c)
        assert redirects in 1..2

        StreamStub.set_advertised_port(stub, stub.port)
        {_c, [], :idle} = Consumer.await(c, 3_000, &(map_size(&1.streams) == 1))
      end
    end

    test "a producer whose routes named no address for the primary sends its records once they name one" do
      stub = start_stub()
      StreamStub.add_broker(stub, "other@127.0.0.1", closed_port())
      StreamStub.set_advertised_port(stub, 0)
      {router, conn_opts} = stub_router(stub)
      p = Producer.new(router, conn_opts)
      # a redirect, not a failure: the record waits for routes that name an address
      {p, []} = Producer.produce(p, "t", [{"a", Record.new("a")}], now())
      {p, [], :timeout} = Producer.await(p, 300, fn _ -> false end)
      # read again once a backoff ends, not at once: one redirect, a second 200 ms on
      assert %{redirects: redirects} = Producer.stats(p)
      assert redirects in 1..2

      StreamStub.set_advertised_port(stub, stub.port)
      {p, events, :idle} = Producer.await(p, 3_000)
      Producer.close(p)
      assert [{:acked, [{"a", _}], _latency}] = events
    end

    test "a range a split makes of a stopped one is stopped too, and nothing opens on it" do
      stub = start_stub(%{29 => {:error, "permission_denied"}})
      {c, []} = Consumer.start(stub_consumer(stub, on_refusal: :stop), "t", now())
      {c, events, :timeout} = Consumer.await(c, 300, fn _ -> false end)
      assert [{:failed, {"t", 0}, "permission_denied"}] = events
      assert length(StreamStub.consumes(stub)) == 1

      # the range splits, and the routes are read again
      StreamStub.set_routes(stub, 2, [{1, 0, 128}, {2, 128, 256}])
      c = %{c | router: Router.forget(c.router, "t")}
      StreamStub.set_override(stub, 29, :default)
      {c, []} = Consumer.tick(c, now())
      assert %{stream: :stopped} = c.lanes[{"t", 1}]
      assert %{stream: :stopped} = c.lanes[{"t", 2}]
      assert Consumer.next_timeout(c, now()) == :infinity
      assert length(StreamStub.consumes(stub)) == 1
    end

    test "a new range starts where its nearest retired ancestor stopped: the one the topic made last" do
      lane = fn block, start -> %{block: block, start: start} end

      reconcile = fn retired, ranges ->
        elem(ReadLanes.reconcile(%{}, retired, "t", ranges, fn _ -> true end, lane), 0)
      end

      # a grandparent {0,1024} (range 0) read to {0,7}; its right child {512,1024} (range 2), stopped or
      # read on: the grandchildren take the child's, though the grandparent's key comes first in the map
      for parent_start <- [:stopped, {:position, {1, 3}}] do
        retired = %{
          {"t", 0} => %{block: {0, 1024}, start: {:position, {0, 7}}},
          {"t", 2} => %{block: {512, 1024}, start: parent_start}
        }

        lanes =
          reconcile.(retired, [%{range: 5, key_start: 512, key_end: 768}, %{range: 6, key_start: 768, key_end: 1024}])

        assert lanes[{"t", 5}].start == parent_start
        assert lanes[{"t", 6}].start == parent_start
      end

      # A (0) split into A1 (1, stopped) and A2 (2); they merged into M (3), read to {3,4}, which split: M's
      # children take M's position, not the stopped A1's though A1's block is the smaller
      retired = %{
        {"t", 0} => %{block: {0, 100}, start: {:position, {0, 9}}},
        {"t", 1} => %{block: {0, 50}, start: :stopped},
        {"t", 2} => %{block: {50, 100}, start: {:position, {1, 2}}},
        {"t", 3} => %{block: {0, 100}, start: {:position, {3, 4}}}
      }

      assert reconcile.(retired, [%{range: 4, key_start: 0, key_end: 50}])[{"t", 4}].start == {:position, {3, 4}}

      # a parent retired after its child (range 1, stopped): the child's children are still stopped
      retired = %{{"t", 1} => %{block: {0, 50}, start: :stopped}, {"t", 0} => %{block: {0, 100}, start: :earliest}}
      assert reconcile.(retired, [%{range: 4, key_start: 0, key_end: 25}])[{"t", 4}].start == :stopped

      # a stopped range split and merged back into exactly its block: still stopped
      retired = %{
        {"t", 0} => %{block: {0, 100}, start: :stopped},
        {"t", 1} => %{block: {0, 50}, start: :stopped},
        {"t", 2} => %{block: {50, 100}, start: :stopped}
      }

      assert reconcile.(retired, [%{range: 3, key_start: 0, key_end: 100}])[{"t", 3}].start == :stopped
    end

    test "a position read in one topic is never where another topic's range starts" do
      # topic x's whole block, read to {0, 10}, retired by a split; then topic y's first ranges
      lane = fn block, start -> %{block: block, start: start} end
      x_ranges = [%{range: 1, key_start: 0, key_end: 128}, %{range: 2, key_start: 128, key_end: 256}]
      lanes = %{{"x", 0} => %{block: {0, 256}, start: {:position, {0, 10}}}}
      {lanes, retired} = ReadLanes.reconcile(lanes, %{}, "x", x_ranges, fn _ -> true end, lane)
      assert lanes[{"x", 1}].start == {:position, {0, 10}}

      y_ranges = [%{range: 1, key_start: 0, key_end: 128}]
      {lanes, _retired} = ReadLanes.reconcile(lanes, retired, "y", y_ranges, fn _ -> true end, lane)
      assert lanes[{"y", 1}].start == :earliest
    end

    test "on_refusal must be :retry or :stop" do
      assert_raise ArgumentError, ~r/on_refusal must be :retry or :stop/, fn ->
        Consumer.new(Router.new({"127.0.0.1", 1}), [], on_refusal: :never)
      end
    end

    test "a connection whose activation fails after a read is dropped" do
      stub = start_stub()
      {:ok, c, []} = Consumer.subscribe(stub_consumer(stub), "t", 2_000)
      [{socket, _address}] = Map.to_list(c.sockets)
      :ok = :gen_tcp.close(socket)

      {:ok, c, []} = Consumer.handle_message(c, {:tcp, socket, <<>>}, now())
      assert %{dropped: 1} = Consumer.stats(c)
    end

    test "routes that can no longer be read hold every range, with no event" do
      stub = start_stub()
      {:ok, c, []} = Consumer.subscribe(stub_consumer(stub), "t", 2_000)
      c = %{c | router: Router.forget(c.router, "t")}
      StreamStub.set_override(stub, 25, {:error, "no_such_topic"})
      assert {_c, []} = Consumer.tick(c, now())
    end

    test "window and max must be positive" do
      assert_raise ArgumentError, ~r/window must be a positive integer/, fn ->
        Consumer.new(Router.new({"127.0.0.1", 1}), [], window: 0)
      end
    end
  end
end
