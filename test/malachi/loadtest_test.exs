defmodule Malachi.LoadtestTest do
  # Integration cases drive the real TCP server (started by the app in test_helper). Not async: they open
  # many sockets and share the one server.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Malachi.Loadtest
  alias Malachi.Loadtest.Histogram
  alias Malachi.Wire

  @port Application.compile_env(:malachi, :tcp_port, 4040)

  # Runs a load test quietly and returns its report, capturing the printed summary.
  defp run(opts) do
    opts = Keyword.merge([port: @port, user: "admin", pass: "admin123", warmup: 0, duration: 1], opts)
    capture_io(fn -> Process.put(:report, Loadtest.run(opts)) end)
    Process.get(:report)
  end

  defp topic(name), do: "lt_#{name}_#{System.unique_integer([:positive])}"

  describe "Histogram" do
    test "records samples and reports monotonic percentiles" do
      h = Histogram.new()
      assert Histogram.count(h) == 0
      assert Histogram.percentile(h, 50) == 0.0

      # 1000 samples at ~1000us and 10 at ~100_000us: p50 near 1ms, p99.99 out in the tail
      for _ <- 1..1000, do: Histogram.record(h, 1000)
      for _ <- 1..10, do: Histogram.record(h, 100_000)

      assert Histogram.count(h) == 1010
      p50 = Histogram.percentile(h, 50)
      p99 = Histogram.percentile(h, 99)
      p100 = Histogram.percentile(h, 100)

      assert p50 >= 900 and p50 <= 1100, "p50 #{p50} should be ~1000us"
      assert p99 >= p50, "percentiles must be monotonic"
      assert p100 >= 90_000, "the tail must reflect the 100ms samples"
    end

    test "clamps non-positive and huge latencies without crashing" do
      h = Histogram.new()
      Histogram.record(h, 0)
      Histogram.record(h, -5)
      Histogram.record(h, 1_000_000_000)
      assert Histogram.count(h) == 3
    end
  end

  describe "produce" do
    test "produces durably with zero errors, records == ops * batch" do
      t = topic("produce")
      r = run(scenario: :produce, connections: 4, batch: 5, topic: t)

      assert r.errors == 0
      assert r.dropped == 0
      assert r.overloaded == 0
      assert r.reconnects == 0
      assert r.ops > 0
      assert r.records == r.ops * 5
      assert r.records_per_s > 0
    end

    test "pipelining keeps zero errors and still produces" do
      t = topic("pipe")
      r = run(scenario: :produce, connections: 4, batch: 5, pipeline: 8, topic: t)
      assert r.errors == 0
      assert r.records == r.ops * 5
    end

    test "fanning out over multiple topics produces to all of them without errors" do
      t = topic("fanout")
      r = run(scenario: :produce, connections: 6, batch: 5, topics: 3, topic: t)

      assert r.errors == 0
      assert r.dropped == 0
      assert r.records == r.ops * 5

      # every fanned-out topic was created and got records (each is independently consumable)
      for i <- 0..2 do
        {records, _next} = Malachi.BrokerServer.consume(Malachi.LogBroker, "#{t}_#{i}", %{}, 1000, 0)
        assert records != [], "topic #{t}_#{i} should have received records"
      end
    end
  end

  describe "read scenarios" do
    test "fetch reads back a prepopulated backlog" do
      t = topic("fetch")
      r = run(scenario: :fetch, connections: 4, batch: 10, prepopulate: 200, max: 50, topic: t)
      assert r.errors == 0
      assert r.records > 0, "fetch should read the prepopulated records"
    end

    test "mixed runs produce and fetch together without errors" do
      t = topic("mixed")
      r = run(scenario: :mixed, connections: 4, batch: 10, prepopulate: 200, topic: t)
      assert r.errors == 0
      assert r.ops > 0
    end

    test "stream receives pushes from the backlog without errors" do
      t = topic("stream")
      r = run(scenario: :stream, connections: 4, prepopulate: 200, window: 50, max: 25, topic: t)
      assert r.errors == 0
      assert r.records > 0, "stream should receive pushed records"
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

  test "token auth path: a bad token is rejected (the auth failure surfaces, not a silent hang)" do
    # The harness issues no tokens, so we can only exercise rejection: a bad token fails the setup auth,
    # which surfaces as a raised error rather than hanging. catch_error covers the MatchError/exit either way.
    assert catch_error(run(scenario: :produce, connections: 1, token: "not-a-real-token", topic: topic("tok")))
  end

  describe "resilience" do
    # A minimal wire server (packet: 4 matches the client's length-prefixed framing) that acks auth and
    # create_topic, and drops the connection on the first produce it ever sees to force a reconnect, then
    # serves produces normally. Proves the worker reconnects and keeps going instead of aborting.
    test "a worker reconnects after its connection drops and keeps producing" do
      seen = :ets.new(:fake_produces, [:public, :set])
      :ets.insert(seen, {:produces, 0})

      {:ok, listen} = :gen_tcp.listen(0, [:binary, packet: 4, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen)
      spawn(fn -> fake_accept(listen, seen) end)

      r =
        run(port: port, host: "127.0.0.1", scenario: :produce, connections: 1, batch: 5, prepopulate: 0, topic: "recon")

      :gen_tcp.close(listen)

      assert r.dropped >= 1, "the forced drop should be counted"
      assert r.reconnects >= 1, "the worker should reconnect and continue, not abort"
      assert r.errors == 0
      assert r.ops > 0, "after reconnecting the worker should complete produces"
    end
  end

  # --- fake wire server (resilience test) ---

  defp fake_accept(listen, seen) do
    case :gen_tcp.accept(listen) do
      {:ok, sock} ->
        spawn(fn -> fake_serve(sock, seen) end)
        fake_accept(listen, seen)

      {:error, _closed} ->
        :ok
    end
  end

  defp fake_serve(sock, seen) do
    case :gen_tcp.recv(sock, 0) do
      {:ok, <<api_key::16, corr::32, _payload::binary>>} ->
        cond do
          api_key == Wire.produce_key() and :ets.update_counter(seen, :produces, 1) == 1 ->
            # First produce anywhere: drop the connection to force the worker to reconnect.
            :gen_tcp.close(sock)

          api_key == Wire.produce_key() ->
            :gen_tcp.send(sock, ok_body(corr, <<5::32>>))
            fake_serve(sock, seen)

          true ->
            # auth / create_topic / anything else: ack so setup and re-auth succeed.
            :gen_tcp.send(sock, ok_body(corr, <<>>))
            fake_serve(sock, seen)
        end

      {:error, _closed} ->
        :ok
    end
  end

  # An unframed ok-response body; packet: 4 on the listen socket prepends the length prefix.
  defp ok_body(corr, payload), do: <<corr::32, Wire.ok_code()::16, payload::binary>>
end
