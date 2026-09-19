defmodule Malachi.LoadtestTest do
  # Integration cases drive the real TCP server (started by the app in test_helper). Not async: they open
  # many sockets and share the one server.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Malachi.Loadtest
  alias Malachi.Loadtest.Conn
  alias Malachi.Test.LoadtestProbes
  alias Malachi.Wire

  @port Application.compile_env(:malachi, :tcp_port, 4040)

  # Runs a load test quietly and returns its report, capturing the printed summary.
  defp run(opts) do
    opts = Keyword.merge([port: @port, user: "admin", pass: "admin123", warmup: 0, duration: 1], opts)
    capture_io(fn -> Process.put(:report, Loadtest.run(opts)) end)
    Process.get(:report)
  end

  defp topic(name), do: "lt_#{name}_#{System.unique_integer([:positive])}"

  describe "produce" do
    test "produces durably with zero errors, records == ops * batch" do
      t = topic("produce")
      r = run(scenario: :produce, connections: 4, batch: 5, topic: t)

      assert r.error_reasons == %{}
      assert r.errors == 0
      assert r.dropped == 0
      assert r.overloaded == 0
      # A healthy run is under any configured quota. This is also what keeps a refused produce from
      # hiding: the generator counts it here rather than folding it into `errors`.
      assert r.rate_limited == 0
      assert r.reconnects == 0
      assert r.ops > 0
      assert r.records == r.ops * 5
      assert r.records_per_s > 0
    end

    test "the report records the batch size and record size it ran with, as fields of their own" do
      # The flush regime the throughput describes. Inside meta.command it is free text in a syntax the
      # Node generator does not share, which is why the ceiling sweep and the published pages read these.
      r = run(scenario: :produce, connections: 2, batch: 7, record_size: 100, topic: topic("regime"))
      assert r.batch == 7
      assert r.record_size == 100

      defaults = run(scenario: :produce, connections: 1, topic: topic("regime_defaults"))
      assert defaults.batch == 10
      assert defaults.record_size == 256
    end

    test "a produce refused by the publish quota is counted apart from a genuine error" do
      # The generator's whole reason for telling `rate_limited` from `errors` is that an operator reading
      # a run must be able to see a quota biting rather than a broker misbehaving. Without a case that
      # actually gets refused, `error_status/1` and the counter it feeds are only ever exercised on the
      # path where nothing is refused, and a regression that folded refusals back into `errors` (or
      # dropped them entirely) would not be noticed.
      for key <- [:publish_rate_limit, :publish_rate_window_ms] do
        prior = Application.get_env(:malachi, key)
        on_exit(fn -> Application.put_env(:malachi, key, prior) end)
      end

      # A handful of produce requests are admitted and the rest of the second is refused. The quota is
      # keyed by user and the generator authenticates as admin, so every connection shares this bucket.
      Application.put_env(:malachi, :publish_rate_limit, 5)
      Application.put_env(:malachi, :publish_rate_window_ms, 60_000)
      Malachi.RateLimiter.reset_bucket("admin", :publish)

      r = run(scenario: :produce, connections: 2, batch: 1, topic: topic("quota"))

      assert r.rate_limited > 0, "the quota never bit, so this case proves nothing"
      assert r.error_reasons == %{}
      assert r.errors == 0, "refusals were counted as errors, which is exactly what the split prevents"
      assert r.overloaded == 0, "a quota refusal must not be reported as broker saturation"
    end

    test "pipelining keeps zero errors and still produces" do
      t = topic("pipe")
      r = run(scenario: :produce, connections: 4, batch: 5, pipeline: 8, topic: t)
      assert r.error_reasons == %{}
      assert r.errors == 0
      assert r.records == r.ops * 5
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
        {records, _next} = Malachi.BrokerServer.consume(Malachi.LogBroker, "#{t}_#{i}", %{}, 1000, 0)
        assert records != [], "topic #{t}_#{i} should have received records"
      end
    end
  end

  describe "read scenarios" do
    test "fetch reads back a prepopulated backlog" do
      t = topic("fetch")
      r = run(scenario: :fetch, connections: 4, batch: 10, prepopulate: 200, max: 50, topic: t)
      assert r.error_reasons == %{}
      assert r.errors == 0
      assert r.records > 0, "fetch should read the prepopulated records"
    end

    test "mixed runs produce and fetch together without errors" do
      t = topic("mixed")
      r = run(scenario: :mixed, connections: 4, batch: 10, prepopulate: 200, topic: t)
      assert r.error_reasons == %{}
      assert r.errors == 0
      assert r.ops > 0
    end

    test "stream receives pushes from the backlog without errors" do
      t = topic("stream")
      r = run(scenario: :stream, connections: 4, prepopulate: 200, window: 50, max: 25, topic: t)
      assert r.error_reasons == %{}
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

  describe "reproduce metadata" do
    test "the recorded command names the topic and the port that were actually used" do
      # Both were missing and both break a reproduction quietly: a run on a non-default port published
      # a command that dials 4040, and a fetch run against an existing topic published one that would
      # create a new empty one.
      t = topic("addressing")
      r = run(scenario: :produce, connections: 2, batch: 5, topic: t)

      assert r.meta.command =~ "--topic=#{t}"
      assert r.meta.command =~ "--port=#{@port}"
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
        port: @port,
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

      # The setup connection plus one per worker.
      assert length(auths) == 4
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
      assert r.error_reasons == %{}
      assert r.errors == 0
      assert r.ops > 0, "after reconnecting the worker should complete produces"
    end
  end

  describe "error reasons" do
    # Issue #181: a healthy node once answered five produces with an error frame and the report said only
    # `errors: 5`. These cases pin that every genuine error keeps the reason the server gave, that the
    # reasons add up to `errors`, and that the two backpressure refusals stay out of them.

    test "a produce refused for a reason the generator does not know is counted under that reason" do
      # The cycle mixes a success, the two refusals that have counters of their own, and two genuine
      # reasons, one of them the stringified tuple the frontend sends for a term it does not normalize.
      cycle = [
        :ok,
        {:error, "replication_timeout"},
        {:error, "overloaded"},
        {:error, "{:sealed, 12}"},
        {:error, "rate_limited"}
      ]

      for pipeline <- [1, 4] do
        r =
          with_scripted_server(produce_cycle(cycle), fn port ->
            run(
              port: port,
              host: "127.0.0.1",
              scenario: :produce,
              connections: 2,
              batch: 5,
              pipeline: pipeline,
              topic: "why"
            )
          end)

        assert r.errors > 0, "pipeline #{pipeline}: the scripted refusals never reached the report"
        assert Map.keys(r.error_reasons) == ["replication_timeout", "{:sealed, 12}"], "pipeline #{pipeline}"
        assert Enum.sum(Map.values(r.error_reasons)) == r.errors, "pipeline #{pipeline}: reasons must add up to errors"
        assert r.overloaded > 0 and r.rate_limited > 0, "pipeline #{pipeline}: refusals keep their own counters"
        assert r.records == r.ops * 5
      end
    end

    test "fetch, user and acl errors keep their reason too" do
      for scenario <- [:fetch, :user, :acl] do
        r =
          with_scripted_server(refuse_after_setup("no_such_range"), fn port ->
            run(
              port: port,
              host: "127.0.0.1",
              scenario: scenario,
              connections: 1,
              prepopulate: 0,
              topic: "why_#{scenario}"
            )
          end)

        assert r.errors > 0, "#{scenario}: the scripted refusal never reached the report"
        assert r.error_reasons == %{"no_such_range" => r.errors}, "#{scenario}"
      end
    end

    test "a refused fetch reports the reason the real broker gave, end to end" do
      # A user that may produce (so setup creates the topic) but not consume.
      {user, pass} = add_user([:produce])
      r = run(scenario: :fetch, connections: 2, user: user, pass: pass, topic: topic("denied_fetch"))

      assert r.errors > 0
      assert r.error_reasons == %{"permission_denied" => r.errors}
    end

    test "no reason table outlives a run, whether it completes or fails to connect" do
      run(scenario: :produce, connections: 1, topic: topic("no_leak"))
      assert reason_tables() == []

      seen = :ets.new(:limited_conns, [:public, :set])
      :ets.insert(seen, {:conns, 0})
      {:ok, listen} = :gen_tcp.listen(0, [:binary, packet: 4, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen)
      spawn(fn -> fake_accept_limited(listen, seen, 2) end)

      assert_raise Loadtest.SetupError, fn ->
        Loadtest.run(port: port, host: "127.0.0.1", user: "admin", pass: "admin123", connections: 3, duration: 1)
      end

      :gen_tcp.close(listen)
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
          Loadtest.run(port: @port, user: user, pass: pass, scenario: :produce, connections: 1, duration: 1, topic: t)
        end

      assert error.message =~ t
      assert error.message =~ "permission_denied"
    end

    test "a connection lost while creating the topic fails setup with a SetupError, not a MatchError" do
      answer = fn
        api_key, _n -> if api_key == Wire.create_topic_key(), do: :close, else: :ok
      end

      error =
        assert_raise Loadtest.SetupError, fn ->
          with_scripted_server(answer, fn port ->
            Loadtest.run(
              port: port,
              host: "127.0.0.1",
              user: "admin",
              pass: "admin123",
              connections: 1,
              duration: 1,
              topic: "lost"
            )
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

    test "a refused prepopulate fails setup instead of leaving a shorter backlog" do
      # Every produce is refused, the prepopulate's included. The fetch that would follow is refused too,
      # so a setup that swallowed the refusal shows up as errors rather than as a crash.
      error =
        assert_raise Loadtest.SetupError, fn ->
          with_scripted_server(refuse_after_setup("rate_limited"), fn port ->
            Loadtest.run(
              port: port,
              host: "127.0.0.1",
              user: "admin",
              pass: "admin123",
              scenario: :fetch,
              connections: 1,
              duration: 1,
              batch: 10,
              prepopulate: 20,
              topic: "seed"
            )
          end)
        end

      assert error.message =~ "seed"
      assert error.message =~ "rate_limited"
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
      # A wire stub that serves the first `allow` connections and slams the door on the rest. The admin
      # setup connection always comes first, so allow = 2 lets setup and ONE worker through while the
      # other two workers fail: the run must abort cleanly counting 2 of 3, and must not report a
      # measurement taken with fewer connections than requested.
      seen = :ets.new(:limited_conns, [:public, :set])
      :ets.insert(seen, {:conns, 0})

      {:ok, listen} = :gen_tcp.listen(0, [:binary, packet: 4, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen)
      spawn(fn -> fake_accept_limited(listen, seen, 2) end)

      assert_raise Loadtest.SetupError, ~r/2 of 3 connections failed to connect and authenticate/, fn ->
        Loadtest.run(
          port: port,
          host: "127.0.0.1",
          user: "admin",
          pass: "admin123",
          connections: 3,
          duration: 1,
          topic: "limited"
        )
      end

      :gen_tcp.close(listen)
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

  # --- limited wire server (setup-failure test) ---

  # Serves the wire protocol for the first `allow` connections and closes every later one right after
  # accept, so worker connections fail while the earlier admin setup succeeds.
  defp fake_accept_limited(listen, seen, allow) do
    case :gen_tcp.accept(listen) do
      {:ok, sock} ->
        if :ets.update_counter(seen, :conns, 1) <= allow do
          spawn(fn -> fake_serve_all(sock) end)
        else
          :gen_tcp.close(sock)
        end

        fake_accept_limited(listen, seen, allow)

      {:error, _closed} ->
        :ok
    end
  end

  # Acks every request (auth, create_topic); the run aborts before any produce reaches it.
  defp fake_serve_all(sock) do
    case :gen_tcp.recv(sock, 0) do
      {:ok, <<_api_key::16, corr::32, _payload::binary>>} ->
        :gen_tcp.send(sock, ok_body(corr, <<>>))
        fake_serve_all(sock)

      {:error, _closed} ->
        :ok
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

  # Answers the produces in `cycle` order, server-wide, and acks everything else.
  defp produce_cycle(cycle) do
    fn
      api_key, n -> if api_key == Wire.produce_key(), do: Enum.at(cycle, rem(n, length(cycle))), else: :ok
    end
  end

  # Acks auth and create_topic, and refuses every later request with `reason`.
  defp refuse_after_setup(reason) do
    fn
      api_key, _n -> if api_key in [Wire.auth_key(), Wire.create_topic_key()], do: :ok, else: {:error, reason}
    end
  end

  # Runs `fun` with the port of a wire server that answers each request as `answer` says. `answer` gets the
  # api key and a server-wide count of the requests before this one, and returns `:ok`,
  # `{:error, reason}`, or `:close` to drop the connection without answering. An ok produce carries a
  # count of 5 records; any other ok carries no payload.
  defp with_scripted_server(answer, fun) do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, packet: 4, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    requests = :counters.new(1, [:atomics])
    spawn(fn -> scripted_accept(listen, answer, requests) end)

    try do
      fun.(port)
    after
      :gen_tcp.close(listen)
    end
  end

  defp scripted_accept(listen, answer, requests) do
    case :gen_tcp.accept(listen) do
      {:ok, sock} ->
        spawn(fn -> scripted_serve(sock, answer, requests) end)
        scripted_accept(listen, answer, requests)

      {:error, _closed} ->
        :ok
    end
  end

  defp scripted_serve(sock, answer, requests) do
    case :gen_tcp.recv(sock, 0) do
      {:ok, <<api_key::16, corr::32, _payload::binary>>} ->
        n = :counters.get(requests, 1)
        :counters.add(requests, 1, 1)

        case answer.(api_key, n) do
          :close ->
            :gen_tcp.close(sock)

          reply ->
            :gen_tcp.send(sock, scripted_body(api_key, corr, reply))
            scripted_serve(sock, answer, requests)
        end

      {:error, _closed} ->
        :ok
    end
  end

  defp scripted_body(api_key, corr, :ok) do
    if api_key == Wire.produce_key(), do: ok_body(corr, <<5::32>>), else: ok_body(corr, <<>>)
  end

  # An unframed error-response body: the reason as the wire's present-string (tag 1, 32-bit length).
  defp scripted_body(_api_key, corr, {:error, reason}),
    do: <<corr::32, Wire.error_code()::16, 1, byte_size(reason)::32, reason::binary>>
end
