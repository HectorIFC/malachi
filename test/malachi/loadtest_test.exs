defmodule Malachi.LoadtestTest do
  # Integration cases drive the real TCP server (started by the app in test_helper). Not async: they open
  # many sockets and share the one server.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Malachi.Loadtest
  alias Malachi.Loadtest.Conn
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

    test "a comma-separated host list round-robins connections and produces cleanly" do
      # Both hosts resolve to the same test server, so this proves the multi-host path (parse, per-worker
      # host pick, reconnect opts) end to end without needing a real second node.
      t = topic("multihost")
      r = run(scenario: :produce, connections: 4, batch: 5, host: "127.0.0.1, 127.0.0.1", topic: t)

      assert r.errors == 0
      assert r.dropped == 0
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

    # An absent flag reads back as an empty string. Asserting that beats a refute on the substring
    # `--key`, which passes for the wrong reason and fails for another one: `--keys=1000` contains it.
    defp shell_flag_absent?(command, flag), do: shell_value(command, flag) == ""

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

    test "prepopulate smaller than batch seeds nothing instead of sending spurious batches" do
      # 1..0 without an explicit step enumerates DOWN and used to send two batches; the //1 step keeps
      # the range empty, so the fetch finds a genuinely empty backlog.
      t = topic("tinyprep")
      r = run(scenario: :fetch, connections: 2, batch: 10, prepopulate: 5, max: 50, topic: t)
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
