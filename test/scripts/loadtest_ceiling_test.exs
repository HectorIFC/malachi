defmodule LoadtestCeilingTest do
  # scripts/loadtest-ceiling.sh decides nothing about what gets published (that is
  # Mix.Tasks.Malachi.Loadtest.Ceiling, tested on its own), but it decides everything about what RUNS:
  # the knobs it refuses, the environment every point sees, the order points run in, which ladder a batch
  # size gets, the A-A repeat, stale files from a previous sweep, and the exit status CI publishes on.
  # Only a CI run of 10 to 15 minutes used to exercise any of that.
  #
  # The script runs for real in a throwaway tree, with stubs first on the PATH: `mix run` becomes a bare
  # TCP listener standing in for the server, `mix malachi.loadtest` and `node` become a generator that
  # writes a canned result and logs how it was called, and `mix malachi.loadtest.ceiling` goes to the
  # real task in this project. TMPDIR points into the test directory, because the script clears
  # "$TMPDIR/malachi_log" before every boot.
  #
  # Not async: every case boots listeners and runs the real mix.
  use ExUnit.Case, async: false

  alias Malachi.Test.MetricsFixtures

  @moduletag :tmp_dir
  @moduletag timeout: 180_000

  @script Path.expand("../../scripts/loadtest-ceiling.sh", __DIR__)
  @project Path.expand("../..", __DIR__)
  @scrape_lib Path.expand("../../scripts/metrics_scrape_lib.sh", __DIR__)

  setup_all do
    # Missing tools fail loudly instead of skipping: a skipped harness test reads as a passing one.
    for tool <- ~w(bash jq python3) do
      System.find_executable(tool) || flunk("#{tool} is required to test scripts/loadtest-ceiling.sh")
    end

    %{mix: System.find_executable("mix") || flunk("mix is required to test scripts/loadtest-ceiling.sh")}
  end

  setup %{tmp_dir: dir} do
    root = Path.join(dir, "tree")
    File.mkdir_p!(Path.join(root, "scripts"))
    File.cp!(@script, Path.join([root, "scripts", "loadtest-ceiling.sh"]))
    File.cp!(@scrape_lib, Path.join([root, "scripts", "metrics_scrape_lib.sh"]))

    stubs = Path.join(dir, "stubs")
    File.mkdir_p!(stubs)
    write_stub!(stubs, "mix", mix_stub())
    write_stub!(stubs, "node", node_stub())
    write_stub!(stubs, "generator", generator_stub())
    write_stub!(stubs, "curl", curl_stub())
    MetricsFixtures.write_flush_scrapes!(stubs)
    File.write!(Path.join(stubs, "listen.py"), listener())

    %{
      root: root,
      stubs: stubs,
      run_dir: Path.join(dir, "runs"),
      out: Path.join([dir, "out", "result.json"]),
      log: Path.join(dir, "generator.log"),
      curl_log: Path.join(dir, "curl.log"),
      rules: Path.join(dir, "rules"),
      tmp: dir
    }
  end

  describe "refuses invalid knobs before booting anything" do
    test "BATCH, which the batch ladder replaced", ctx do
      assert {output, 2} = run_script(ctx, [{"BATCH", "10"}])
      assert output =~ "BATCH was replaced by BATCH_LADDER"
      refute File.exists?(ctx.log)
    end

    test "an unknown generator", ctx do
      assert {output, 2} = run_script(ctx, [{"GENERATOR", "python"}])
      assert output =~ "GENERATOR must be node or elixir"
    end

    test "a headline batch size outside the ladder", ctx do
      assert {output, 2} = run_script(ctx, [{"BATCH_LADDER", "10 100"}, {"HEADLINE_BATCH", "50"}])
      assert output =~ "HEADLINE_BATCH 50 is not in BATCH_LADDER (10 100)"
      refute File.exists?(ctx.log)
    end

    test "a connection ladder set to nothing is refused, not replaced by the default", ctx do
      # Declared inside the shell: an empty value in System.cmd's env removes the variable instead of
      # setting it, which would test the unset case twice.
      assert {output, 2} = run_script(ctx, [{"BATCH_LADDER", "10 100"}], "export CONNS_LADDER_100=;")
      assert output =~ "CONNS_LADDER_100 is empty"
      refute File.exists?(ctx.log)
    end

    test "a batch size that is not a number", ctx do
      assert {output, 2} = run_script(ctx, [{"BATCH_LADDER", "10 1-0"}])
      assert output =~ ~s(BATCH_LADDER has "1-0", which is not an integer)
    end
  end

  describe "a sweep" do
    test "runs every point interleaved in the forced regime, repeats the headline peak and publishes the curve", ctx do
      # Rates default to batch * connections, so batch 10 peaks at 8 connections (80) and batch 100's
      # only rung records errors.
      File.write!(ctx.rules, "100 2 errors 500 3\n")

      assert {output, 0} =
               run_script(ctx, [
                 {"BATCH_LADDER", "10 100"},
                 {"CONNS_LADDER_10", "4 8"},
                 {"CONNS_LADDER_100", "2"},
                 # Inherited values the script must override, not honour.
                 {"MALACHI_GROUP_COMMIT", "true"},
                 {"MALACHI_SEGMENT_PREALLOC_BYTES", "0"}
               ])

      result = read_json!(ctx.out)
      assert result["records_per_s"] == 80
      assert result["connections"] == 8

      assert result["regime_label"] ==
               "batch 10 x 256B (2.5KB of values per request, group commit off, segment preallocation 64MB)"

      assert result["peak_at_ladder_limit"] == true
      assert [%{"batch" => 10, "status" => "peak"}, %{"batch" => 100, "status" => "no_clean_rung"}] = result["curve"]
      assert %{"connections" => 8, "repeat_records_per_s" => 80, "delta_pct" => +0.0} = result["sweep"]["aa_control"]

      # Interleaved, then the A-A repeat of the headline peak.
      assert generator_calls(ctx) == [{"elixir", 10, 4}, {"elixir", 100, 2}, {"elixir", 10, 8}, {"elixir", 10, 8}]

      for line <- File.read!(ctx.log) |> String.split("\n", trim: true) do
        assert line =~ "group_commit=false"
        assert line =~ "prealloc=67108864"
        assert line =~ "rsize=256"
        assert line =~ "marker=yes"
      end

      assert output =~ "== headline: 80 rec/s @ 8 connections, batch 10 x 256B"
      assert output =~ "WARN: batch 10 peaked at the top of its connection ladder; widen CONNS_LADDER_10"
      assert output =~ "WARN: batch 100 has no peak (no_clean_rung)."
    end

    test "without a headline peak it writes the result, runs no A-A repeat and exits 1", ctx do
      # 4096 has no CONNS_LADDER_4096, so it gets the script's default ladder for it.
      assert {output, 1} =
               run_script(ctx, [{"BATCH_LADDER", "4096"}, {"HEADLINE_BATCH", "4096"}, {"STUB_DEFAULT", "fail"}])

      assert %{"conns_ladders" => %{"4096" => [4, 8, 16, 32, 64]}} = read_json!(Path.join(ctx.run_dir, "sweep.json"))
      assert %{"headline_status" => "no_completed_rung"} = read_json!(ctx.out)
      assert length(generator_calls(ctx)) == 5
      assert output =~ "WARN: batch 4096 has no peak (no_completed_rung)."
      assert output =~ "run failed: batch=4096 connections=4"
    end

    test "the Node leg runs the same points, and a stale file from a previous sweep is not read back", ctx do
      File.mkdir_p!(ctx.run_dir)

      File.write!(
        Path.join(ctx.run_dir, "run-b10-c8-r1.json"),
        Jason.encode!(%{
          "batch" => 10,
          "record_size" => 256,
          "connections" => 8,
          "records_per_s" => 999_999,
          "errors" => 0
        })
      )

      File.write!(ctx.rules, "10 8 fail\n")

      assert {_output, 0} =
               run_script(ctx, [{"GENERATOR", "node"}, {"BATCH_LADDER", "10"}, {"CONNS_LADDER_10", "4 8"}])

      result = read_json!(ctx.out)
      assert result["records_per_s"] == 40
      assert [%{"rungs" => [%{"status" => "clean"}, %{"status" => "failed"}]}] = result["curve"]
      assert generator_calls(ctx) == [{"node", 10, 4}, {"node", 10, 8}, {"node", 10, 4}]
    end

    test "a generator that never reaches its measured window leaves the CPU attribution unset", ctx do
      assert {output, 0} =
               run_script(ctx, [
                 {"BATCH_LADDER", "10"},
                 {"CONNS_LADDER_10", "4"},
                 {"STUB_DEFAULT", "hang"},
                 {"MARKER_TIMEOUT", "1"}
               ])

      assert output =~ "NOTE: no measured window within 1s; CPU attribution left unset for this point"
      result = read_json!(ctx.out)
      assert result["generator_cpu_cores"] == nil
      assert result["server_cpu_cores"] == nil
    end
  end

  describe "the server's flush latency" do
    test "each run carries the window between the marker and the generator's exit", ctx do
      assert {_output, 0} = run_script(ctx, [{"BATCH_LADDER", "10"}, {"CONNS_LADDER_10", "4 8"}])

      for {connections, rep} <- [{4, 1}, {8, 1}] do
        run = read_json!(Path.join(ctx.run_dir, "run-b10-c#{connections}-r#{rep}.json"))

        # The before scrape already holds 100 slow warmup flushes; only the 1000 fast ones are windowed.
        assert %{"flushes" => 1000, "bytes" => 64_000, "records" => 10_000, "p99" => p99} =
                 run["flush_latency_seconds"]

        assert p99 < 0.00025
        assert run["flush_latency_error"] == nil
        assert run["server_cpu_cores"] != nil or not File.exists?("/proc")
      end

      result = read_json!(ctx.out)
      assert %{"flushes" => 1000} = result["flush_latency_seconds"]
      assert [%{"rungs" => [%{"flush_latency_seconds" => %{"flushes" => 1000}} | _]}] = result["curve"]

      # A login and two scrapes per point (three points with the A-A repeat), with the bearer token and
      # the exposition asked for, against the dashboard port the script was given.
      calls = curl_calls(ctx)
      assert length(calls) == 9

      assert Enum.chunk_every(calls, 3)
             |> Enum.all?(fn chunk ->
               Enum.map(chunk, &elem(&1, 0)) == ["POST /login", "GET /metrics", "GET /metrics"]
             end)

      for {"GET /metrics", line} <- calls do
        assert line =~ "Authorization: Bearer stub-token"
        assert line =~ "Accept: text/plain"
      end

      assert {"POST /login", login} = hd(calls)
      assert login =~ ~s({"username":"admin","password":"admin123"})
    end

    for {behavior, reason} <- [
          {"login403", "login failed: HTTP 403 (the dashboard refused the credentials)"},
          {"login429", "login failed: HTTP 429 (the dashboard rate limit tripped)"},
          {"notoken", "login failed: the answer carried no token"},
          {"down", "login failed: curl could not reach http://127.0.0.1:"},
          {"scrape500", "scrape failed: HTTP 500"},
          {"session401", "scrape failed: HTTP 401 (the session is gone; the node may have restarted)"},
          {"noseries", "the scrape has no flush latency series"},
          {"restart", "the node restarted between the scrapes"}
        ] do
      test "#{behavior}: the run keeps its throughput and records why there is no flush window", ctx do
        assert {_output, 0} =
                 run_script(ctx, [{"BATCH_LADDER", "10"}, {"CONNS_LADDER_10", "4"}, {"STUB_CURL", unquote(behavior)}])

        run = read_json!(Path.join(ctx.run_dir, "run-b10-c4-r1.json"))
        assert run["records_per_s"] == 40
        assert run["flush_latency_seconds"] == nil
        assert run["flush_latency_error"] =~ unquote(reason)
      end
    end

    test "without curl the run says so", ctx do
      assert {_output, 0} =
               run_script(ctx, [{"BATCH_LADDER", "10"}, {"CONNS_LADDER_10", "4"}, {"CURL", "no-such-curl"}])

      run = read_json!(Path.join(ctx.run_dir, "run-b10-c4-r1.json"))
      assert run["flush_latency_error"] == "login failed: curl not found"
      refute File.exists?(ctx.curl_log)
    end

    test "a generator that never opens its window scrapes nothing after it either", ctx do
      assert {_output, 0} =
               run_script(ctx, [
                 {"BATCH_LADDER", "10"},
                 {"CONNS_LADDER_10", "4"},
                 {"STUB_DEFAULT", "hang"},
                 {"MARKER_TIMEOUT", "1"}
               ])

      run = read_json!(ctx.out)
      assert run["flush_latency_error"] == "no measured window was signaled, so nothing opened the flush window"
      assert Enum.map(curl_calls(ctx), &elem(&1, 0)) |> Enum.uniq() == ["POST /login"]
    end

    test "flush files from a previous sweep are cleared", ctx do
      File.mkdir_p!(ctx.run_dir)
      stale = Path.join(ctx.run_dir, "run-b10-c4-r1.flush.before.prom")
      File.write!(stale, "stale")

      assert {_output, 0} =
               run_script(ctx, [
                 {"BATCH_LADDER", "10"},
                 {"CONNS_LADDER_10", "4"},
                 {"STUB_DEFAULT", "hang"},
                 {"MARKER_TIMEOUT", "1"}
               ])

      # The hung generator opened no window, so a leftover before file must not have been used as one.
      refute File.exists?(stale)
      assert read_json!(ctx.out)["flush_latency_error"] =~ "no measured window"
    end
  end

  # --- running the script ---

  # `shell_prelude` runs in the shell that execs the script, for a variable System.cmd cannot express.
  defp run_script(ctx, env, shell_prelude \\ "") do
    base = [
      {"PATH", ctx.stubs <> ":" <> System.get_env("PATH", "")},
      {"TMPDIR", ctx.tmp},
      {"GENERATOR", "elixir"},
      {"OUT", ctx.out},
      {"RUN_DIR", ctx.run_dir},
      {"DUR", "1"},
      {"WARM", "0"},
      {"SRV_CPUSET", "0"},
      {"LT_CPUSET", "0"},
      {"MALACHI_PORT", Integer.to_string(free_port())},
      {"STUB_DIR", ctx.stubs},
      {"STUB_LOG", ctx.log},
      {"STUB_RULES", ctx.rules},
      {"STUB_DEFAULT", "clean"},
      {"STUB_CURL", "ok"},
      {"STUB_CURL_LOG", ctx.curl_log},
      # The curl stub answers everything, but a real client must never reach a dashboard another run
      # on this machine is serving.
      {"MALACHI_DASHBOARD_PORT", Integer.to_string(free_port())},
      {"CURL", nil},
      {"REAL_MIX", System.find_executable("mix")},
      {"REAL_PROJECT", @project},
      # Nothing from the environment running the tests may leak into the sweep.
      {"BATCH", nil},
      {"BATCH_LADDER", nil},
      {"HEADLINE_BATCH", nil},
      {"CONNS_LADDER", nil},
      {"REPS", nil}
    ]

    env = Enum.reduce(env, base, fn {key, value}, acc -> List.keystore(acc, key, 0, {key, value}) end)
    script = Path.join([ctx.root, "scripts", "loadtest-ceiling.sh"])

    System.cmd("bash", ["-c", ~s(#{shell_prelude} exec bash "$0"), script], env: env, stderr_to_stdout: true)
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end

  defp read_json!(path), do: path |> File.read!() |> Jason.decode!()

  defp generator_calls(ctx) do
    if File.exists?(ctx.log) do
      for line <- String.split(File.read!(ctx.log), "\n", trim: true) do
        [_, kind, batch, connections] = Regex.run(~r/^gen kind=(\w+) batch=(\d+) conns=(\d+)/, line)
        {kind, String.to_integer(batch), String.to_integer(connections)}
      end
    else
      []
    end
  end

  defp curl_calls(ctx) do
    if File.exists?(ctx.curl_log) do
      for line <- String.split(File.read!(ctx.curl_log), "\n", trim: true) do
        [_, call] = Regex.run(~r/^(\w+ \/\w+)/, line)
        {call, line}
      end
    else
      []
    end
  end

  defp write_stub!(dir, name, body) do
    path = Path.join(dir, name)
    File.write!(path, body)
    File.chmod!(path, 0o755)
  end

  # --- stubs ---

  defp mix_stub do
    ~S"""
    #!/usr/bin/env bash
    case "$1" in
      run) exec python3 "$STUB_DIR/listen.py" "$MALACHI_PORT" ;;
      malachi.loadtest) shift; STUB_KIND=elixir exec "$STUB_DIR/generator" "$@" ;;
      malachi.loadtest.ceiling) cd "$REAL_PROJECT" && MIX_ENV=test exec "$REAL_MIX" "$@" ;;
      *) echo "stub mix: unexpected $*" >&2; exit 97 ;;
    esac
    """
  end

  defp node_stub do
    ~S"""
    #!/usr/bin/env bash
    shift
    STUB_KIND=node exec "$STUB_DIR/generator" "$@"
    """
  end

  # Behaviour per point comes from STUB_RULES lines `<batch> <conns> <behaviour> [rate] [errors]`, else
  # STUB_DEFAULT: clean (marker, then a result), errors (marker, then a result with errors), fail (exit 1
  # with no marker), hang (no marker for two seconds, then a clean result). The default rate is
  # batch * connections.
  defp generator_stub do
    ~S"""
    #!/usr/bin/env bash
    batch="" conns="" rsize="" marker=""
    while [ $# -gt 0 ]; do
      case "$1" in
        --batch) batch="$2"; shift ;;
        --connections) conns="$2"; shift ;;
        --record-size) rsize="$2"; shift ;;
        --measure-marker) marker="$2"; shift ;;
      esac
      shift
    done
    echo "gen kind=$STUB_KIND batch=$batch conns=$conns rsize=$rsize group_commit=$MALACHI_GROUP_COMMIT prealloc=$MALACHI_SEGMENT_PREALLOC_BYTES marker=${marker:+yes}" >> "$STUB_LOG"

    rule="$(awk -v b="$batch" -v c="$conns" '$1 == b && $2 == c { print $3, $4, $5; exit }' "$STUB_RULES" 2> /dev/null)"
    read -r behaviour rate errors <<< "${rule:-$STUB_DEFAULT}"
    rate="${rate:-$((batch * conns))}"
    errors="${errors:-0}"

    result() {
      printf '{"scenario":"produce","batch":%s,"record_size":%s,"connections":%s,"records_per_s":%s,"errors":%s,"duration_s":1,"meta":{"command":"stub"}}\n' \
        "$batch" "$rsize" "$conns" "$rate" "$1"
    }

    case "$behaviour" in
      clean) : > "$marker"; sleep 0.2; result 0 ;;
      errors) : > "$marker"; sleep 0.2; result "$errors" ;;
      fail) exit 1 ;;
      hang) sleep 2; result 0 ;;
    esac
    """
  end

  # Answers like curl with `-w '\n%{http_code}'`: the body, then the status on its own line. STUB_CURL
  # picks the dashboard's behavior; /metrics alternates between the before and the after scrape, which is
  # the order a run asks for them in.
  defp curl_stub do
    ~S"""
    #!/usr/bin/env bash
    method=GET url="" data="" headers=""
    while [ $# -gt 0 ]; do
      case "$1" in
        -X) method="$2"; shift ;;
        --data) data="$2"; shift ;;
        -H) headers="$headers $2;"; shift ;;
        -w|--max-time) shift ;;
        -*) ;;
        *) url="$1" ;;
      esac
      shift
    done
    path="/${url#http://*/}"
    echo "$method $path$headers $data" >> "$STUB_CURL_LOG"

    reply() { [ -n "$2" ] && cat "$2"; printf '
    %s' "$1"; }

    if [ "$STUB_CURL" = down ]; then printf '
    000'; exit 7; fi

    case "$method $path" in
      "POST /login")
        case "$STUB_CURL" in
          login403) printf '{"s":"error"}
    403' ;;
          login429) printf 'slow down
    429' ;;
          notoken) printf '{"s":"ok"}
    200' ;;
          *) printf '{"s":"ok","token":"stub-token"}
    200' ;;
        esac ;;
      "GET /metrics")
        calls="$STUB_DIR/metrics-calls"
        n=$(( $(cat "$calls" 2> /dev/null || echo 0) + 1 ))
        echo "$n" > "$calls"
        case "$STUB_CURL" in
          scrape500) reply 500 "" ;;
          session401) if [ $((n % 2)) = 1 ]; then reply 200 "$STUB_DIR/before.prom"; else reply 401 ""; fi ;;
          noseries) reply 200 "$STUB_DIR/noseries.prom" ;;
          restart) if [ $((n % 2)) = 1 ]; then reply 200 "$STUB_DIR/before.prom"; else reply 200 "$STUB_DIR/restarted.prom"; fi ;;
          *) if [ $((n % 2)) = 1 ]; then reply 200 "$STUB_DIR/before.prom"; else reply 200 "$STUB_DIR/after.prom"; fi ;;
        esac ;;
      *) printf 'not found
    404' ;;
    esac
    """
  end

  defp listener do
    ~S"""
    import socket
    import sys

    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", int(sys.argv[1])))
    server.listen(64)
    while True:
        connection, _ = server.accept()
        connection.close()
    """
  end
end
