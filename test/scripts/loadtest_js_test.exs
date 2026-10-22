defmodule LoadtestJsTest do
  # scripts/loadtest.js is one of the two generators the published ceiling comes from, and nothing tested
  # it: its only check was a histogram self-test CI never ran. These cases run the real script against
  # the test server the app starts in test_helper, and observe it from the server side (authentication
  # telemetry, the file system) rather than trusting what it reports about itself.
  #
  # Not async: the authentication counts would pick up other tests' connections.
  use ExUnit.Case, async: false

  alias Malachi.BrokerServer
  alias Malachi.DataPlaneRouter
  alias Malachi.Loadtest.Payload
  alias Malachi.LogApi
  alias Malachi.TCPAcceptorPool
  alias Malachi.Test.LoadtestProbes
  alias Malachi.Test.PollingHelper
  alias Malachi.Test.StreamsOn
  alias Malachi.Test.SubscribeResetStub

  @moduletag :tmp_dir

  @script Path.expand("../../scripts/loadtest.js", __DIR__)

  setup_all do
    # A missing runtime fails loudly instead of skipping: a skipped generator test reads as a passing one.
    node = System.find_executable("node") || flunk("node is required to test scripts/loadtest.js; install Node 22.15+")
    %{node: node}
  end

  setup do
    # the generator produces on the stream keys, which the producer_streams flag serves
    :ok = StreamsOn.setup(%{})
    probe = LoadtestProbes.watch_auth()
    on_exit(fn -> LoadtestProbes.stop_auth(probe) end)
    :ok
  end

  # `stderr: true` folds the generator's error output into the returned string, for the cases that are
  # about what it refuses. The default keeps stderr out, so a run's JSON is the whole of stdout.
  # `user:` and `pass:` run it as someone other than admin; `port:` points it at a server other than the
  # test one.
  defp run_js(ctx, args, opts \\ []) do
    env = [
      {"MALACHI_HOST", "127.0.0.1"},
      {"MALACHI_PORT", Integer.to_string(Keyword.get_lazy(opts, :port, &TCPAcceptorPool.port/0))},
      {"MALACHI_USER", Keyword.get(opts, :user, "admin")},
      {"MALACHI_PASS", Keyword.get(opts, :pass, "admin123")}
    ]

    System.cmd(ctx.node, [@script | args], env: env, stderr_to_stdout: Keyword.get(opts, :stderr, false))
  end

  defp topic, do: "ltjs_#{System.unique_integer([:positive])}"

  describe "--measure-marker" do
    test "is created only after every connection authenticated", ctx do
      marker = Path.join(ctx.tmp_dir, "measure.marker")
      LoadtestProbes.watch_file(marker)

      args =
        ~w(--scenario produce --json --connections 3 --batch 2 --duration 1 --warmup 1 --topic) ++
          [topic(), "--measure-marker", marker]

      assert {output, 0} = run_js(ctx, args)
      assert_receive {:file_appeared, ^marker, appeared_at}, 1_000
      auths = LoadtestProbes.successful_auths()

      # After the last authentication AND the whole warmup: the connections are kept through it, so the
      # last authentication happens before the warmup starts.
      assert appeared_at >= List.last(auths) + 1_000 - LoadtestProbes.poll_ms()
      assert {:ok, %{"errors" => 0, "error_reasons" => %{}}} = Jason.decode(output)
    end

    test "is left out of the recorded command, value included", ctx do
      marker = Path.join(ctx.tmp_dir, "measure.marker")
      args = ~w(--scenario produce --json --connections 1 --duration 1 --topic) ++ [topic(), "--measure-marker", marker]

      assert {output, 0} = run_js(ctx, args)
      assert {:ok, %{"meta" => %{"command" => command}}} = Jason.decode(output)

      refute command =~ "measure-marker"
      refute command =~ marker
      assert command =~ "--connections 1"
    end

    test "a directory that does not exist fails before any connection opens", ctx do
      marker = Path.join([ctx.tmp_dir, "missing", "m"])

      assert {output, 1} =
               run_js(ctx, ~w(--scenario produce --connections 2 --duration 1 --measure-marker) ++ [marker],
                 stderr: true
               )

      assert output =~ "does not exist or is not writable"
      assert LoadtestProbes.successful_auths() == []
    end

    test "a marker that already exists is overwritten rather than refused", ctx do
      marker = Path.join(ctx.tmp_dir, "stale.marker")
      File.write!(marker, "from an earlier run")
      args = ~w(--scenario produce --json --connections 1 --duration 1 --topic) ++ [topic(), "--measure-marker", marker]

      assert {_output, 0} = run_js(ctx, args)
      assert File.read!(marker) == ""
    end

    test "a marker path that is a directory fails before any connection opens", ctx do
      # Checking only the parent directory let this through, and it then failed inside writeFileSync,
      # after every connection had authenticated and the warmup had run.
      marker = Path.join(ctx.tmp_dir, "marker.d")
      File.mkdir_p!(marker)

      assert {output, 1} =
               run_js(ctx, ~w(--scenario produce --connections 2 --duration 1 --measure-marker) ++ [marker],
                 stderr: true
               )

      assert output =~ "exists and is not a regular file"
      assert LoadtestProbes.successful_auths() == []
    end

    test "a marker that exists and cannot be written fails before any connection opens", ctx do
      marker = Path.join(ctx.tmp_dir, "read-only.marker")
      File.write!(marker, "")
      File.chmod!(marker, 0o444)
      on_exit(fn -> File.chmod(marker, 0o644) end)

      assert {output, 1} =
               run_js(ctx, ~w(--scenario produce --connections 2 --duration 1 --measure-marker) ++ [marker],
                 stderr: true
               )

      assert output =~ "exists and is not writable"
      assert LoadtestProbes.successful_auths() == []
    end

    test "a trailing flag with no path is refused, not ignored", ctx do
      assert {_output, 1} = run_js(ctx, ~w(--scenario produce --connections 1 --duration 1 --measure-marker))
      assert LoadtestProbes.successful_auths() == []
    end
  end

  describe "producing on producer streams" do
    test "a closed loop counts every record the server stored, and no more, its pipeline drained at the end", ctx do
      t = topic()
      args = ~w(--scenario produce --json --connections 2 --batch 5 --pipeline 4 --duration 1 --topic) ++ [t]
      # the streams the server holds open for the run, seen from its side: each worker's, under its label,
      # asking the window its pipeline sets
      watcher = Task.async(fn -> watch_streams(t, System.monotonic_time(:millisecond) + 4_000, MapSet.new()) end)

      assert {output, 0} = run_js(ctx, args)
      seen = Task.await(watcher, 10_000)
      assert MapSet.new(seen, & &1.label) == MapSet.new(["malachi-loadtest-0", "malachi-loadtest-1"])
      assert Enum.all?(seen, &(&1.window.appends == 4))
      report = Jason.decode!(output)
      assert %{"errors" => 0, "ops" => ops, "records" => records, "pipeline" => 4} = report
      assert ops > 0 and records == ops * 5, "each acknowledged append is one op of one batch"
      assert length(first_values(t, 1_000_000)) == records
    end

    test "an open loop counts each hand-over once all its records are acknowledged, timed from its schedule", ctx do
      t = topic()
      args = ~w(--scenario produce --json --connections 2 --batch 3 --rate 300 --duration 1 --topic) ++ [t]

      assert {output, 0} = run_js(ctx, args)
      report = Jason.decode!(output)
      assert %{"mode" => "open-loop", "errors" => 0, "saturated" => false, "ops" => ops, "records" => records} = report
      # each hand-over timed from its schedule to its last acknowledgement, a round trip at least
      assert %{"latency_ms" => %{"coordinated_omission_corrected" => true, "min" => min, "p50" => p50}} = report
      assert min > 0 and p50 >= min
      assert ops > 100 and records == ops * 3
      assert length(first_values(t, 1_000_000)) == records
    end
  end

  describe "producing, what it counts" do
    test "a refusal is counted apart when the broker shed it, as an error otherwise, and a lost append not at all",
         ctx do
      script = ~s"""
      const { Stats } = require(#{inspect(@script)});
      const s = new Stats();
      for (const r of ['overloaded', 'overloaded', 'rate_limited', 'connection_lost', 'permission_denied']) s.refusal(r);
      console.log(JSON.stringify({ overloaded: s.overloaded, rateLimited: s.rateLimited, errors: s.errors, reasons: s.errorReasons }));
      """

      assert {output, 0} = System.cmd(ctx.node, ["-e", script])

      assert Jason.decode!(output) == %{
               "overloaded" => 2,
               "rateLimited" => 1,
               "errors" => 1,
               "reasons" => %{"permission_denied" => 1}
             }
    end

    test "appends the publish quota refuses are counted in rate_limited, not in errors", ctx do
      prior =
        for key <- [:publish_rate_limit, :publish_rate_window_ms],
            into: %{},
            do: {key, Application.get_env(:malachi, key)}

      on_exit(fn ->
        for {key, value} <- prior,
            do: if(value, do: Application.put_env(:malachi, key, value), else: Application.delete_env(:malachi, key))
      end)

      Application.put_env(:malachi, :publish_rate_limit, 20)
      Application.put_env(:malachi, :publish_rate_window_ms, 60_000)
      user = "ltjs_quota_#{System.unique_integer([:positive])}"
      :ok = Malachi.Auth.add_user(user, "Quota-Pass-1!", [:produce, :consume, :admin])
      on_exit(fn -> Malachi.Auth.remove_user(user) end)

      args = ~w(--scenario produce --json --connections 1 --batch 5 --duration 1 --topic) ++ [topic()]
      assert {output, 0} = run_js(ctx, args, user: user, pass: "Quota-Pass-1!")
      report = Jason.decode!(output)
      assert %{"errors" => 0, "rate_limited" => limited, "records" => records} = report
      assert limited > 0 and records <= 20
    end

    test "what routing did over the run is reported: the routes read, and the move a split made", ctx do
      t = topic()
      parent = self()
      # split once the measured window opens: every producer has read its routes and opened its streams by
      # then, so the move reaches them whatever the runner's speed
      marker = Path.join(ctx.tmp_dir, "window.marker")

      spawn_link(fn ->
        PollingHelper.wait_until!(fn -> File.exists?(marker) end, timeout: 30_000)
        Process.sleep(200)
        send(parent, {:split, BrokerServer.split_range(DataPlaneRouter.shard_for(t), {t, 0})})
      end)

      args = ~w(--scenario produce --json --connections 2 --duration 2 --measure-marker) ++ [marker, "--topic", t]
      assert {output, 0} = run_js(ctx, args)
      assert_receive {:split, {:ok, _left, _right}}, 5_000
      report = Jason.decode!(output)
      assert %{"errors" => 0, "route_refreshes" => refreshes, "moved" => moved, "redirects" => redirects} = report
      assert refreshes >= 2, "each producer read its routes, and again after the move"
      assert moved + redirects >= 1
    end

    test "an open loop of mixed with an odd number of workers gives each its own op", ctx do
      args = ~w(--scenario mixed --json --connections 3 --rate 100 --duration 1 --prepopulate 20 --topic) ++ [topic()]
      assert {output, 0} = run_js(ctx, args, stderr: false)
      assert %{"errors" => 0, "ops" => ops} = Jason.decode!(output)
      assert ops > 50
    end
  end

  describe "against a broker that stalls or refuses" do
    @fake Path.expand("../../scripts/lib/testing/run_against_fake.js", __DIR__)

    defp against_fake(ctx, policy, args) do
      {output, 0} = System.cmd(ctx.node, [@fake, Jason.encode!(policy), "--" | args])
      Jason.decode!(output)
    end

    test "a prepopulate the broker refuses fails the run, naming the refusal", ctx do
      run =
        against_fake(
          ctx,
          %{open: %{refuse: "permission_denied"}},
          ~w(--scenario fetch --prepopulate 10 --duration 1 --json)
        )

      assert run["code"] == 1
      assert run["stderr"] =~ "could not prepopulate"
      assert run["stderr"] =~ "permission_denied"
    end

    @tag timeout: 120_000
    test "a prepopulate the broker never answers fails after one wait, not one wait per chunk", ctx do
      # twenty chunks of 1000 records against a window of 16 appends: the seventeenth waits for room, and that
      # one wait of 30 seconds is the bound; one wait per chunk left would be two minutes
      run = against_fake(ctx, %{append: "silent"}, ~w(--scenario fetch --prepopulate 20000 --duration 1 --json))
      assert run["code"] == 1
      assert run["stderr"] =~ "prepopulate_timeout"
      assert run["elapsedMs"] < 60_000
    end

    @tag timeout: 120_000
    test "an open loop whose appends are never answered ends 15 seconds after its window, and reports", ctx do
      run = against_fake(ctx, %{append: "silent"}, ~w(--scenario produce --rate 50 --duration 1 --json))
      assert run["code"] == 0
      assert %{"ops" => 0, "mode" => "open-loop"} = Jason.decode!(run["stdout"])
      assert run["elapsedMs"] < 25_000
    end
  end

  # Every open producer stream of `topic` the server lists until `deadline`, polled.
  defp watch_streams(topic, deadline, seen) do
    %{streams: streams} = Malachi.ProducerDirectory.list(2_000)

    seen =
      Enum.reduce(streams, seen, fn stream, acc -> if stream.topic == topic, do: MapSet.put(acc, stream), else: acc end)

    if System.monotonic_time(:millisecond) >= deadline do
      seen |> Enum.uniq_by(&{&1.label, &1.stream_id}) |> Enum.map(&Map.take(&1, [:label, :window]))
    else
      Process.sleep(50)
      watch_streams(topic, deadline, seen)
    end
  end

  describe "the flush regime" do
    test "the report records the batch size and record size it ran with, as fields of their own", ctx do
      args = ~w(--scenario produce --json --connections 1 --duration 1 --batch 7 --record-size 100 --topic) ++ [topic()]

      assert {output, 0} = run_js(ctx, args)
      assert {:ok, %{"batch" => 7, "record_size" => 100}} = Jason.decode(output)

      # This generator's own defaults, recorded, not assumed. The batch size still differs from the Elixir
      # generator's (1 against 10); the record size and the payload are the same as its.
      assert {defaults, 0} =
               run_js(ctx, ~w(--scenario produce --json --connections 1 --duration 1 --topic) ++ [topic()])

      assert {:ok,
              %{
                "batch" => 1,
                "record_size" => 256,
                "payload" => "constant",
                "payload_seed" => nil,
                "payload_pool_values" => nil,
                "pipeline" => 1,
                "linger_ms" => 0,
                # one batch's worth, counted as the producer counts a record: 256 value bytes, the longest
                # key ("k999"), and 22 bytes of framing
                "linger_bytes" => 282
              }} =
               Jason.decode(defaults)
    end

    test "both generators record the same regime fields for the same flags", ctx do
      # The two generators mirror their flags explicitly; the ceiling sweep reads these fields from either
      # one through a single path, so a name or a value drifting in one of them would break it quietly.
      flags = %{
        "connections" => 2,
        "batch" => 3,
        "record_size" => 160,
        "payload" => "json",
        "payload_seed" => 7,
        "pipeline" => 2,
        "linger_ms" => 5,
        "linger_bytes" => 4096
      }

      {node_output, 0} =
        run_js(
          ctx,
          ~w(--scenario produce --json --connections 2 --batch 3 --record-size 160 --payload json) ++
            ~w(--payload-seed 7 --pipeline 2 --linger-ms 5 --linger-bytes 4096 --duration 1 --topic) ++ [topic()]
        )

      elixir_report =
        ExUnit.CaptureIO.capture_io(fn ->
          Malachi.Loadtest.run(
            port: TCPAcceptorPool.port(),
            user: "admin",
            pass: "admin123",
            scenario: :produce,
            connections: 2,
            batch: 3,
            record_size: 160,
            payload: :json,
            payload_seed: 7,
            pipeline: 2,
            linger_ms: 5,
            linger_bytes: 4096,
            duration: 1,
            warmup: 0,
            topic: topic(),
            json: true
          )
        end)

      regime = fn json -> json |> Jason.decode!() |> Map.take(Map.keys(flags)) end

      assert regime.(node_output) == flags
      assert regime.(elixir_report) == flags

      # Derived from the flags by the same rule on both sides, so it must agree too.
      pool = Payload.pool_size(160, 3, 2, 2)
      assert %{"payload_pool_values" => ^pool} = Jason.decode!(node_output)
      assert %{"payload_pool_values" => ^pool} = Jason.decode!(elixir_report)

      # The error breakdown too: same name, same shape (reason to count), empty on a clean run.
      assert %{"errors" => 0, "error_reasons" => %{}} = Jason.decode!(node_output)
      assert %{"errors" => 0, "error_reasons" => %{}} = Jason.decode!(elixir_report)
    end
  end

  describe "payloads" do
    # Observed from the broker, not from the report: a generator that reports json and sends constant
    # bytes passes every test that reads its JSON.
    test "both generators put the same seeded json values on the wire, in pool order", ctx do
      pool = Payload.values(:json, 42, 256, 6)

      node_topic = topic()

      assert {_output, 0} =
               run_js(
                 ctx,
                 ~w(--scenario produce --json --connections 1 --batch 3 --duration 1 --payload json) ++
                   ~w(--payload-seed 42 --topic) ++ [node_topic]
               )

      elixir_topic = topic()

      ExUnit.CaptureIO.capture_io(fn ->
        Malachi.Loadtest.run(
          port: TCPAcceptorPool.port(),
          user: "admin",
          pass: "admin123",
          scenario: :produce,
          connections: 1,
          batch: 3,
          payload: :json,
          payload_seed: 42,
          duration: 1,
          warmup: 0,
          topic: elixir_topic,
          json: true
        )
      end)

      assert first_values(node_topic, 6) == pool
      assert first_values(elixir_topic, 6) == pool
    end

    test "random values on the wire are the seeded ones, not one repeated byte", ctx do
      t = topic()
      args = ~w(--scenario produce --json --connections 1 --batch 2 --record-size 64 --duration 1 --payload random)

      assert {_output, 0} = run_js(ctx, args ++ ["--topic", t])
      assert first_values(t, 4) == Payload.values(:random, 1, 64, 4)
    end

    test "a prepopulated backlog is drawn from the pool too", ctx do
      t = topic()
      args = ~w(--scenario fetch --json --connections 1 --duration 1 --prepopulate 5 --payload json --topic)

      assert {_output, 0} = run_js(ctx, args ++ [t])
      assert first_values(t, 5) == Payload.values(:json, 1, 256, 5)
    end

    test "constant bytes are still the one repeated byte this generator always sent", ctx do
      t = topic()

      assert {_output, 0} =
               run_js(ctx, ~w(--scenario produce --json --connections 1 --batch 2 --duration 1 --topic) ++ [t])

      assert first_values(t, 2) == List.duplicate(:binary.copy("a", 256), 2)
    end

    test "an unknown payload, a seed with constant bytes, a bad seed and a json size too small are refused " <>
           "before any connection opens",
         ctx do
      min = Payload.min_json_size()

      for {args, message} <- [
            {~w(--payload zip), ~s(Unknown payload "zip" (expected: constant, json, random\))},
            {~w(--payload-seed 3), "--payload-seed only applies to the json and random payloads"},
            {~w(--payload json --payload-seed -1), "--payload-seed must be an integer from 0 to 4294967295"},
            {~w(--payload random --payload-seed 4294967296), "--payload-seed must be an integer from 0 to 4294967295"},
            {~w(--payload json --record-size #{min - 1}), "json payload needs --record-size >= #{min}"}
          ] do
        assert {output, 1} =
                 run_js(ctx, ~w(--scenario produce --connections 2 --duration 1) ++ args, stderr: true)

        assert output =~ message, inspect(args)
      end

      assert LoadtestProbes.successful_auths() == []
    end
  end

  describe "the pool walk" do
    test "a run that never produces generates no pool, and one that prepopulates does", ctx do
      base = ~w(--scenario fetch --json --connections 1 --duration 1 --batch 5 --payload json --topic)

      assert {output, 0} = run_js(ctx, ~w(--prepopulate 0) ++ base ++ [topic()])
      assert %{"payload_pool_values" => nil} = Jason.decode!(output)

      # This generator prints its prepopulate progress to stdout ahead of the report, even under --json,
      # so the report is read from its first line on.
      assert {output, 0} = run_js(ctx, ~w(--prepopulate 10) ++ base ++ [topic()])
      [report] = Regex.run(~r/^\{.*\z/ms, output)
      pool = Payload.pool_size(256, 5, 1, 1)
      assert %{"payload_pool_values" => ^pool} = Jason.decode!(report)

      # 1MB values make 8MiB of pool a single batch of 8, so the floor of one batch per connection is what
      # sizes it: two connections need two batches.
      args =
        ~w(--scenario fetch --json --connections 2 --duration 1 --batch 8 --max 1 --prepopulate 8) ++
          ~w(--record-size 1048576 --payload random --topic)

      assert {output, 0} = run_js(ctx, args ++ [topic()])
      [report] = Regex.run(~r/^\{.*\z/ms, output)
      assert %{"payload_pool_values" => 16} = Jason.decode!(report)
    end

    test "the warmup and the measured window walk one cursor, as the Elixir generator does", ctx do
      # A cursor rebuilt for the measured window would send the warmup's batches again from the start of
      # the pool. Every record either phase wrote is read back, in order.
      t = topic()
      args = ~w(--scenario produce --json --connections 1 --batch 2 --warmup 1 --duration 1 --payload json --topic)

      assert {_output, 0} = run_js(ctx, args ++ [t])
      values = first_values(t, 1_000_000)
      pool = Payload.values(:json, 1, 256, Payload.pool_size(256, 2, 1, 1))

      assert length(values) < length(pool), "the run wrapped the pool, so a repeat proves nothing"
      assert values == Enum.take(pool, length(values))
    end

    test "two connections start on batches of their own", ctx do
      t = topic()
      args = ~w(--scenario produce --json --connections 2 --batch 2 --duration 1 --payload json --topic)

      assert {_output, 0} = run_js(ctx, args ++ [t])
      values = first_values(t, 1_000_000)
      pool = Payload.values(:json, 1, 256, Payload.pool_size(256, 2, 2, 1))
      second = Payload.start_batch(1, div(length(pool), 2), 2) * 2

      assert Enum.at(pool, 0) in values
      assert Enum.at(pool, second) in values
      assert Enum.all?(values, &(&1 in pool))
    end

    test "an open-loop run keeps each connection walking the pool, request after request", ctx do
      # The open loop asks for an op on every request; an op made afresh each time would restart at its
      # connection's first batch and send the same two batches all run long.
      t = topic()
      args = ~w(--scenario produce --json --connections 2 --batch 2 --rate 200 --duration 1 --payload json --topic)

      assert {_output, 0} = run_js(ctx, args ++ [t])
      values = first_values(t, 1_000_000)
      pool = Payload.values(:json, 1, 256, Payload.pool_size(256, 2, 2, 1))

      assert Enum.all?(values, &(&1 in pool))
      assert length(Enum.uniq(values)) > 4
      assert length(Enum.uniq(values)) == length(values)
    end
  end

  # The values of the first `n` records of `topic`, read back through the broker.
  defp first_values(topic, n) do
    {:ok, records, _cursor} = LogApi.fetch(DataPlaneRouter.shard_for(topic), topic, :start, n)
    Enum.map(records, & &1.value)
  end

  describe "the JSON report" do
    test "is the whole of stdout, a prepopulated backlog included", ctx do
      args = ~w(--scenario fetch --json --connections 1 --duration 1 --prepopulate 10 --topic) ++ [topic()]
      assert {output, 0} = run_js(ctx, args)
      assert {:ok, %{"scenario" => "fetch"}} = Jason.decode(output)
    end
  end

  describe "error reasons" do
    test "a refused fetch is counted under the reason the broker gave", ctx do
      # May produce, so the topic is created, but may not consume.
      {user, pass} = add_user([:produce])
      args = ~w(--scenario fetch --json --connections 2 --duration 1 --prepopulate 0 --topic) ++ [topic()]

      assert {output, 0} = run_js(ctx, args, user: user, pass: pass)
      report = Jason.decode!(output)

      assert report["errors"] > 0
      assert report["error_reasons"] == %{"permission_denied" => report["errors"]}
    end

    test "a refused topic creation fails the run and names the reason", ctx do
      # create_topic is gated by :produce, so a consume-only user is refused it.
      {user, pass} = add_user([:consume])
      args = ~w(--scenario produce --json --connections 1 --duration 1 --topic) ++ [topic()]

      assert {output, 1} = run_js(ctx, args, user: user, pass: pass, stderr: true)
      assert output =~ "permission_denied"
    end

    test "a topic that already exists is not a setup failure", ctx do
      args = ~w(--scenario produce --json --connections 1 --duration 1 --topic) ++ [topic()]

      assert {_first, 0} = run_js(ctx, args)
      assert {output, 0} = run_js(ctx, args)
      assert %{"errors" => 0, "error_reasons" => %{}} = Jason.decode!(output)
    end
  end

  # A user with `permissions` on the test server, removed when the test ends.
  defp add_user(permissions) do
    user = "ltjs_user_#{System.unique_integer([:positive])}"
    pass = "Ltjs-Pass-1!"
    Malachi.Auth.add_user(user, pass, permissions)
    on_exit(fn -> Malachi.Auth.remove_user(user) end)
    {user, pass}
  end

  describe "dropped stream connections" do
    # Issue #218, mirrored from the Elixir generator: a stream connection lost before or after its
    # subscribe went out used to end the worker, counted in `errors` or (when the socket had already
    # closed) not at all, where the Elixir generator counts a drop and resubscribes.

    test "a stream worker whose subscribe send fails counts the drop and resubscribes", ctx do
      report = stream_on_reset_stub(ctx, :reconnect)

      assert report["dropped"] == 1, "the failed subscribe was not counted as a drop"
      assert report["reconnects"] == 1, "the worker should reconnect and resubscribe, not end"
      assert report["errors"] == 0, "a transport failure is a drop, not a server error"
      assert report["error_reasons"] == %{}
    end

    test "a stream worker that cannot reconnect after a failed subscribe gives up without crashing", ctx do
      report = stream_on_reset_stub(ctx, :give_up)

      assert report["dropped"] == 1, "the failed subscribe was not counted as a drop"
      assert report["reconnects"] == 0
      assert report["errors"] == 0
    end

    test "a stream connection lost after its subscribe went out counts the drop and resubscribes", ctx do
      # A FIN reaches the client as 'close' and a reset as 'error', so both of the client's paths for a
      # connection lost on a live subscription are covered.
      for how <- [:close, :reset] do
        report = stream_on_reset_stub(ctx, {:mid_stream, how}, 1)

        assert report["dropped"] == 1, "#{how}: the lost stream connection was not counted as a drop"
        assert report["reconnects"] == 1, "#{how}: the worker should reconnect and resubscribe, not end"
        assert report["errors"] == 0, "#{how}: a transport failure is a drop, not a server error"
      end
    end

    test "a reconnect that hangs at the deadline is abandoned instead of stretching the run", ctx do
      # The rates divide by the time the run took, so waiting out a stalled authentication (30s) would report
      # a fraction of the real rate and count pushes that arrived after the window.
      report = stream_on_reset_stub(ctx, :stall_reconnect)

      assert report["dropped"] == 1
      assert report["reconnects"] == 0
      assert report["duration_s"] < 1.5, "the run waited on the stalled reconnect past its 1s window"
    end

    # Without the fix this never finishes on its own: the refused reconnects keep sockets open, the process
    # outlives its report, and run_js waits on it until the test times out.
    @tag timeout: 30_000
    test "a reconnect whose authentication is refused leaves no socket holding the process open", ctx do
      report = stream_on_reset_stub(ctx, :refuse_reconnect_auth)

      assert report["dropped"] == 1
      assert report["reconnects"] == 0
      assert report["errors"] == 0, "a refused reconnect is not a stream error"
    end

    test "a refused subscription stays an error under its reason, not a drop", ctx do
      {user, pass} = add_user([:produce])
      args = ~w(--scenario stream --json --connections 2 --duration 1 --prepopulate 0 --topic) ++ [topic()]

      assert {output, 0} = run_js(ctx, args, user: user, pass: pass)
      report = Jason.decode!(output)

      assert report["errors"] == 2, "each worker subscribed once and each refusal counts once"
      assert report["error_reasons"] == %{"permission_denied" => 2}
      assert report["dropped"] == 0
      assert report["reconnects"] == 0
    end

    test "a healthy stream run that stays idle reports no drop", ctx do
      args = ~w(--scenario stream --json --connections 2 --duration 1 --prepopulate 0 --topic) ++ [topic()]

      assert {output, 0} = run_js(ctx, args)
      assert %{"dropped" => 0, "reconnects" => 0, "errors" => 0} = Jason.decode!(output)
    end

    test "outside the stream scenario reconnects is null; dropped counts the producers' drops, null for fetch", ctx do
      produce = ~w(--scenario produce --json --connections 1 --duration 1 --topic) ++ [topic()]
      assert {output, 0} = run_js(ctx, produce)
      assert %{"dropped" => 0, "reconnects" => nil} = Jason.decode!(output)

      # the reads of fetch and of mixed count a lost connection in errors, so dropped claims no zero there
      for scenario <- ~w(fetch mixed) do
        args = ~w(--scenario #{scenario} --json --connections 2 --duration 1 --prepopulate 10 --topic) ++ [topic()]
        assert {output, 0} = run_js(ctx, args)
        assert %{"dropped" => nil, "reconnects" => nil} = Jason.decode!(output)
      end
    end
  end

  # A stream run against Malachi.Test.SubscribeResetStub, returning the decoded report. The connections open
  # one at a time and nothing is prepopulated, which is what the stub's ordering relies on: this generator
  # prepopulates on a connection of its own, which would take the stub's second place.
  defp stream_on_reset_stub(ctx, mode, connections \\ 2) do
    args =
      ~w(--scenario stream --json --connections #{connections} --connect-strategy bounded --connect-concurrency 1) ++
        ~w(--duration 1 --warmup 0 --prepopulate 0 --topic subscribe_reset)

    SubscribeResetStub.with_stub(mode, fn port ->
      assert {output, 0} = run_js(ctx, args, port: port)
      Jason.decode!(output)
    end)
  end

  describe "connections across the warmup" do
    test "a produce run authenticates each connection once and keeps it through the warmup", ctx do
      # Every connection used to be closed and reopened after the warmup, which only the stream scenario
      # needs: 1989 authentications against the Elixir generator's 997 on the same CI ladder, a second
      # auth storm inside every point, and a methodology the two generators no longer shared.
      args = ~w(--scenario produce --json --connections 4 --duration 1 --warmup 1 --topic) ++ [topic()]

      assert {_output, 0} = run_js(ctx, args)
      # The topic-creating admin connection, then each worker's producer: its metadata connection and the
      # connection to the broker leading the range, as the Elixir producer opens them.
      assert length(LoadtestProbes.successful_auths()) == 1 + 4 * 2
    end

    test "a mixed run keeps its connections too, since it subscribes to nothing", ctx do
      args =
        ~w(--scenario mixed --json --connections 2 --duration 1 --warmup 1 --prepopulate 50 --topic) ++ [topic()]

      assert {_output, 0} = run_js(ctx, args)
      # Admin, the prepopulating producer (metadata and broker), the producing worker's two, the reader's one.
      assert length(LoadtestProbes.successful_auths()) == 1 + 2 + 2 + 1
    end

    test "a stream run still reconnects after the warmup, since a subscription ends only with its socket", ctx do
      args =
        ~w(--scenario stream --json --connections 2 --duration 1 --warmup 1 --prepopulate 50 --topic) ++ [topic()]

      assert {_output, 0} = run_js(ctx, args)
      # Admin, the prepopulating producer's two, then every worker twice.
      assert length(LoadtestProbes.successful_auths()) == 1 + 2 + 2 * 2
    end

    test "without a warmup nothing reconnects in any scenario", ctx do
      args = ~w(--scenario stream --json --connections 2 --duration 1 --prepopulate 50 --topic) ++ [topic()]

      assert {_output, 0} = run_js(ctx, args)
      # Admin, the prepopulating producer's two, then each worker once.
      assert length(LoadtestProbes.successful_auths()) == 1 + 2 + 2
    end
  end
end
