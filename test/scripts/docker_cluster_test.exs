defmodule DockerClusterTest do
  # benchmark/docker-cluster.sh decides where each case's data lives, whether a real-disk case really ran
  # on a real disk with its preallocation, what a hung or silent load generator becomes, and what lands in
  # OUT. None of that needs a cluster to check, and a real run takes minutes per case.
  #
  # The script runs for real in a throwaway tree, with stubs first on the PATH: `docker` logs every call
  # with the environment the case exports and answers from STUB_* variables, `sleep` returns at once (the
  # health poll and the stats snapshot would otherwise wait for real), and `findmnt`/`lsblk` describe a
  # fixed disk. `timeout` is the real coreutils one, which is why this module is Linux only.
  #
  # Not async: every case runs a shell script, and the one real-label case runs the real mix task.
  use ExUnit.Case, async: false

  @moduletag :linux
  @moduletag :tmp_dir
  @moduletag timeout: 120_000

  @script Path.expand("../../benchmark/docker-cluster.sh", __DIR__)
  @project Path.expand("../..", __DIR__)
  @prealloc 67_108_864

  setup_all do
    # Missing tools fail loudly instead of skipping: a skipped harness test reads as a passing one.
    tools =
      for tool <- ~w(bash jq timeout dirname sleep), into: %{} do
        {tool, System.find_executable(tool) || flunk("#{tool} is required to test benchmark/docker-cluster.sh")}
      end

    %{tools: tools, mix: System.find_executable("mix") || flunk("mix is required")}
  end

  setup %{tmp_dir: dir} do
    root = Path.join(dir, "tree")
    File.mkdir_p!(Path.join(root, "benchmark"))
    File.cp!(@script, Path.join([root, "benchmark", "docker-cluster.sh"]))

    stubs = Path.join(dir, "stubs")
    File.mkdir_p!(stubs)
    write_stub!(stubs, "docker", docker_stub())
    write_stub!(stubs, "sleep", sleep_stub())
    write_stub!(stubs, "findmnt", findmnt_stub())
    write_stub!(stubs, "lsblk", lsblk_stub())

    %{root: root, stubs: stubs, log: Path.join(dir, "docker.log"), out: Path.join([dir, "out", "cases.jsonl"])}
  end

  describe "refuses invalid knobs before touching docker" do
    for value <- ["2", "yes", "true", ""] do
      test "REAL_DISK=#{inspect(value)}", ctx do
        # Declared inside the shell: an empty value in System.cmd's env removes the variable.
        assert {output, 2} = run_script(ctx, [], "export REAL_DISK=#{inspect(unquote(value))};")
        assert output =~ "REAL_DISK must be 0 or 1, got '#{unquote(value)}'"
        assert docker_calls(ctx) == []
      end
    end

    for {knob, value} <- [
          {"TOPICS", "0"},
          {"BATCH", "-1"},
          {"CASE_TIMEOUT", "5s"},
          {"DISK_PREALLOC_BYTES", "0"},
          {"DUR", "007"}
        ] do
      test "#{knob}=#{value}", ctx do
        assert {output, 2} = run_script(ctx, [{unquote(knob), unquote(value)}])
        assert output =~ "#{unquote(knob)} must be a positive integer, got '#{unquote(value)}'"
        assert docker_calls(ctx) == []
      end
    end

    test "a preallocation above the segment size the broker clamps it to", ctx do
      assert {output, 2} = run_script(ctx, [{"REAL_DISK", "1"}, {"DISK_PREALLOC_BYTES", "67108865"}])
      assert output =~ "DISK_PREALLOC_BYTES must be at most 67108864"
      assert docker_calls(ctx) == []
    end

    test "an RF the three-node cluster cannot hold", ctx do
      assert {output, 2} = run_script(ctx, [{"RFS", "1 4"}])
      assert output =~ "RFS entries must be 1, 2 or 3 (the cluster has three nodes), got '4'"
    end

    test "an empty RFS", ctx do
      assert {output, 2} = run_script(ctx, [], "export RFS=' ';")
      assert output =~ "RFS is empty"
    end

    for knob <- ["MALACHI_SEGMENT_MAX_BYTES", "MALACHI_LOG_ROLL_MAX_BYTES"] do
      test "#{knob}, which moves the preallocation clamp", ctx do
        assert {output, 2} = run_script(ctx, [{unquote(knob), "2048"}])
        assert output =~ "#{unquote(knob)} is not supported by this benchmark"
        assert docker_calls(ctx) == []
      end
    end
  end

  describe "host preconditions" do
    test "a non-Linux host is refused", ctx do
      write_stub!(ctx.stubs, "uname", uname_stub("Darwin"))
      assert {output, 2} = run_script(ctx, [])
      assert output =~ "this benchmark runs on Linux only"
      assert docker_calls(ctx) == []
    end

    test "ALLOW_NON_LINUX runs it as a smoke test and does not ask the host for its disks", ctx do
      write_stub!(ctx.stubs, "uname", uname_stub("Darwin"))
      File.rm!(Path.join(ctx.stubs, "findmnt"))
      File.rm!(Path.join(ctx.stubs, "lsblk"))

      assert {output, 0} = run_script(ctx, [{"ALLOW_NON_LINUX", "1"}, {"RFS", "1"}, {"OUT", ctx.out}])
      assert output =~ "SMOKE TEST: not a Linux host, these numbers are not comparable to anything"
      assert output =~ "on unknown (not a Linux host)"
      assert [%{"smoke_test" => true, "docker_root_backing" => "unknown (not a Linux host)"}] = cases(ctx)
    end

    for tool <- ["timeout", "jq", "findmnt", "lsblk"] do
      test "a missing #{tool} is named", ctx do
        # A PATH holding only what the script needs before its tool check, minus the tool under test.
        bin = Path.join(ctx.stubs, "../bin")
        File.mkdir_p!(bin)

        for {name, path} <- ctx.tools, name != unquote(tool), do: File.ln_s!(path, Path.join(bin, name))

        for name <- ~w(docker findmnt lsblk),
            name != unquote(tool),
            do: File.ln_s!(Path.join(ctx.stubs, name), Path.join(bin, name))

        File.ln_s!(System.find_executable("uname"), Path.join(bin, "uname"))

        assert {output, 2} = run_script(ctx, [{"PATH", bin}])
        assert output =~ "#{unquote(tool)} is required to run this benchmark"
        assert docker_calls(ctx) == []
      end
    end
  end

  describe "the data mode" do
    test "REAL_DISK=0 runs every case on tmpfs with preallocation off and never checks the disk", ctx do
      assert {output, 0} = run_script(ctx, [{"STUB_FSTYPE", "tmpfs"}, {"OUT", ctx.out}])
      assert output =~ "data:    tmpfs (MALACHI_DATA_ROOT=/tmp, preallocation 0 bytes)"

      for call <- compose_calls(ctx, ["up", "run", "exec"]) do
        assert call.env == %{"ROOT" => "/tmp", "PREALLOC" => "0", "RF" => call.env["RF"]}
      end

      refute Enum.any?(exec_commands(ctx), &(&1 in ["df", "du"]))
      assert [%{"data_mode" => "tmpfs", "du_bytes" => nil}, %{"data_mode" => "tmpfs"}] = cases(ctx)
    end

    test "REAL_DISK=1 runs every case on /data with the production preallocation", ctx do
      assert {output, 0} = run_script(ctx, [{"REAL_DISK", "1"}, {"OUT", ctx.out}])
      assert output =~ "data:    disk (MALACHI_DATA_ROOT=/data, preallocation #{@prealloc} bytes)"

      for call <- compose_calls(ctx, ["up", "run", "exec"]) do
        assert call.env["ROOT"] == "/data"
        assert call.env["PREALLOC"] == "#{@prealloc}"
      end

      assert [%{"data_mode" => "disk", "prealloc_bytes" => @prealloc}, %{"data_mode" => "disk"}] = cases(ctx)
    end

    test "DISK_PREALLOC_BYTES sets the preallocation and the bytes the disk check expects", ctx do
      assert {output, 0} =
               run_script(ctx, [
                 {"REAL_DISK", "1"},
                 {"RFS", "1"},
                 {"DISK_PREALLOC_BYTES", "8192"},
                 {"STUB_DU_KB", "11"}
               ])

      # 4 topics x RF 1 x 8192 bytes; 3 nodes x 11KB is 33792 bytes.
      assert output =~ "on disk: 33792 bytes across the nodes (at least 32768 expected)"
      assert Enum.all?(compose_calls(ctx, ["up"]), &(&1.env["PREALLOC"] == "8192"))
    end

    test "the caller's own data root does not leak into a case", ctx do
      assert {_output, 0} = run_script(ctx, [{"MALACHI_DATA_ROOT", "/elsewhere"}, {"RFS", "1"}])
      assert Enum.all?(compose_calls(ctx, ["up"]), &(&1.env["ROOT"] == "/tmp"))
    end
  end

  describe "every case" do
    test "starts and ends with the volumes removed, and the RF is passed through", ctx do
      assert {_output, 0} = run_script(ctx, [])

      assert Enum.map(compose_calls(ctx, ["up", "down"]), &{&1.verb, &1.env["RF"], &1.args}) == [
               {"down", "1", "down -v"},
               {"up", "1", "up -d --force-recreate malachi1 malachi2 malachi3"},
               {"down", "1", "down -v"},
               {"down", "3", "down -v"},
               {"up", "3", "up -d --force-recreate malachi1 malachi2 malachi3"},
               {"down", "3", "down -v"}
             ]
    end

    test "creates every segment during setup with one prepopulated batch per topic", ctx do
      assert {_output, 0} = run_script(ctx, [{"RFS", "1"}, {"BATCH", "50"}])
      [run] = loadtest_runs(ctx)
      assert run.args =~ "--batch 50 --topics 4 --prepopulate 50 "
      assert run.args =~ "--name malachi-cluster-loadtest-"
    end

    for {real_disk, prealloc} <- [{"0", "off"}, {"1", "64MB"}] do
      test "names its regime with the ceiling task's words, group commit off above RF 1, " <>
             "preallocation #{prealloc} with REAL_DISK=#{real_disk}",
           ctx do
        assert {output, 0} =
                 run_script(ctx, [{"STUB_REAL_MIX", "1"}, {"REAL_DISK", unquote(real_disk)}, {"OUT", ctx.out}])

        on =
          "batch 100 x 256B (25KB of values per request, group commit on, segment preallocation #{unquote(prealloc)})"

        off =
          "batch 100 x 256B (25KB of values per request, group commit off, segment preallocation #{unquote(prealloc)})"

        assert output =~ "regime: " <> on
        assert output =~ "regime: " <> off
        assert Enum.map(cases(ctx), &{&1["rf"], &1["regime_label"]}) == [{1, on}, {3, off}]
      end
    end

    test "a regime that cannot be named stops the run before any case", ctx do
      assert {output, 1} = run_script(ctx, [{"STUB_LABEL", "fail"}])
      assert output =~ "could not name the regime for RF=1"
      assert output =~ "label exploded"
      assert compose_calls(ctx, ["up"]) == []
    end

    test "prints where the numbers came from", ctx do
      assert {output, 0} = run_script(ctx, [{"RFS", "1"}])
      assert output =~ "host:    Linux"
      assert output =~ "docker:  Stub Linux, kernel 6.8.0, storage driver overlay2"
      assert output =~ "volumes: /var/lib/docker on ext4 rw,relatime on /dev/sda1 (disk sda rota=false model=Stub Disk)"
      assert output =~ "data on: malachi1 ext4, malachi2 ext4, malachi3 ext4"
      assert output =~ "mid-window CPU: malachi-cluster-1 150.00%"
    end

    test "a disk the host cannot describe is recorded as unknown, not fatal", ctx do
      write_stub!(ctx.stubs, "findmnt", "#!/usr/bin/env bash\nexit 1\n")
      assert {output, 0} = run_script(ctx, [{"RFS", "1"}])
      assert output =~ "volumes: /var/lib/docker on unknown (findmnt could not resolve /var/lib/docker)"
    end

    test "a mount without a block device behind it keeps the filesystem and says the disk is unknown", ctx do
      write_stub!(ctx.stubs, "lsblk", ~s(#!/usr/bin/env bash\necho '{"blockdevices": []}'\n))
      assert {output, 0} = run_script(ctx, [{"RFS", "1"}])
      assert output =~ "on ext4 rw,relatime on /dev/sda1 (disk unknown)"
    end
  end

  describe "the CPU snapshot" do
    test "waits for the generator's measure marker, then reads every container", ctx do
      assert {output, 0} = run_script(ctx, [{"RFS", "1"}, {"REAL_DISK", "1"}, {"OUT", ctx.out}])
      [run] = loadtest_runs(ctx)
      assert run.args =~ "--measure-marker /tmp/malachi-measure-window --json"

      verbs = ctx |> docker_calls() |> Enum.map(& &1.args)

      marker =
        Enum.find_index(
          verbs,
          &(&1 =~ ~r/^exec malachi-cluster-loadtest-\d+ sh -c test -e \/tmp\/malachi-measure-window/)
        )

      stats = Enum.find_index(verbs, &String.starts_with?(&1, "stats --no-stream"))
      assert marker && stats && marker < stats

      assert output =~ "mid-window CPU: malachi-cluster-1 150.00%"
      assert [%{"mid_window_cpu" => "malachi-cluster-1 150.00%"}] = cases(ctx)
    end

    test "is left out when the window never opens, and does not hold the run", ctx do
      assert {output, 0} = run_script(ctx, [{"RFS", "1"}, {"STUB_MARKER", "no"}, {"OUT", ctx.out}])
      refute output =~ "mid-window CPU"
      refute Enum.any?(docker_calls(ctx), &(&1.verb == "stats"))
      assert [%{"outcome" => "ok", "mid_window_cpu" => nil}] = cases(ctx)
    end

    test "stops with a generator that hangs before its window", ctx do
      assert {output, 1} =
               run_script(ctx, [
                 {"RFS", "1"},
                 {"STUB_MARKER", "no"},
                 {"STUB_LOADTEST", "hang"},
                 {"CASE_TIMEOUT", "1"},
                 {"OUT", ctx.out}
               ])

      assert output =~ "(timeout after 1s)"
      refute Enum.any?(docker_calls(ctx), &(&1.verb == "stats"))
      assert [%{"outcome" => "timeout after 1s", "mid_window_cpu" => nil}] = cases(ctx)
    end
  end

  describe "a real-disk case that is not what it claims fails" do
    test "when the data root is tmpfs", ctx do
      assert {output, 1} = run_script(ctx, [{"REAL_DISK", "1"}, {"STUB_FSTYPE", "tmpfs"}, {"OUT", ctx.out}])
      assert output =~ "(wrong filesystem)"
      assert output =~ "/data on malachi1 is tmpfs, so this is not a real-disk run"
      assert output =~ "done, WITH FAILED CASES"
      assert loadtest_runs(ctx) == []
      assert Enum.map(cases(ctx), & &1["outcome"]) == ["wrong filesystem", "wrong filesystem"]
      # The failed case still removed its volumes.
      assert length(compose_calls(ctx, ["down"])) == 4
    end

    test "when the mount cannot be read", ctx do
      assert {output, 1} = run_script(ctx, [{"REAL_DISK", "1"}, {"RFS", "1"}, {"STUB_FSTYPE", "none"}])
      assert output =~ "could not read the mount holding /data on malachi1"
      assert loadtest_runs(ctx) == []
    end

    test "when the host disk cannot hold the preallocation, with the arithmetic", ctx do
      assert {output, 1} = run_script(ctx, [{"REAL_DISK", "1"}, {"RFS", "3"}, {"STUB_DF_KB", "1000"}])
      assert output =~ "(not enough disk)"

      assert output =~
               "need #{4 * 3 * @prealloc + 1024 * 1024 * 1024} bytes (4 topics x RF 3 x #{@prealloc} preallocated + 1GB margin), 1024000 free"

      assert loadtest_runs(ctx) == []
    end

    test "when free space cannot be read", ctx do
      assert {output, 1} = run_script(ctx, [{"REAL_DISK", "1"}, {"RFS", "1"}, {"STUB_DF_KB", "none"}])
      assert output =~ "df could not read /data on malachi1"
    end

    test "when the preallocated bytes are not on the volume afterwards", ctx do
      assert {output, 1} = run_script(ctx, [{"REAL_DISK", "1"}, {"STUB_DU_KB", "1"}, {"OUT", ctx.out}])
      assert output =~ "on disk: 3072 bytes across the nodes (at least #{4 * @prealloc} expected)"
      assert output =~ "preallocation did not land on the volume"

      assert Enum.map(cases(ctx), &{&1["outcome"], &1["du_bytes"]}) == [
               {"preallocation missing", 3072},
               {"preallocation missing", 3072}
             ]
    end

    test "a node whose du prints nothing counts as zero bytes", ctx do
      assert {output, 1} = run_script(ctx, [{"REAL_DISK", "1"}, {"RFS", "1"}, {"STUB_DU_KB", "none"}])
      assert output =~ "on disk: 0 bytes across the nodes"
    end

    test "sums the volume use of all three nodes", ctx do
      assert {output, 0} = run_script(ctx, [{"REAL_DISK", "1"}, {"RFS", "3"}])

      assert output =~
               "on disk: #{3 * 1_000_000 * 1024} bytes across the nodes (at least #{4 * 3 * @prealloc} expected)"

      assert Enum.count(exec_commands(ctx), &(&1 == "du")) == 3
    end
  end

  describe "load generator outcomes" do
    test "errors are a lower number, not a failed run", ctx do
      assert {output, 0} = run_script(ctx, [{"RFS", "3"}, {"STUB_LOADTEST", "errors"}, {"OUT", ctx.out}])
      assert output =~ ~r/^3\s+\|\s+1000\s+1\.5\s+9\.5\s+0\(err=7\)/m
      assert [%{"outcome" => "ok with errors", "loadtest" => %{"errors" => 7}}] = cases(ctx)
    end

    test "no result fails the run with the generator's stderr", ctx do
      assert {output, 1} = run_script(ctx, [{"RFS", "1 3"}, {"STUB_LOADTEST", "none"}, {"OUT", ctx.out}])
      assert output =~ "(no json)"
      assert output =~ "generator exploded"
      assert Enum.map(cases(ctx), &{&1["outcome"], &1["loadtest"]}) == [{"no json", nil}, {"no json", nil}]
    end

    test "a result line that is not a JSON object is no result", ctx do
      assert {output, 1} = run_script(ctx, [{"RFS", "1"}, {"STUB_LOADTEST", "garbage"}])
      assert output =~ "(no json)"
    end

    test "a hung generator is killed, reported, and the next case still runs", ctx do
      assert {output, 1} =
               run_script(ctx, [{"RFS", "3 1"}, {"STUB_LOADTEST", "hang"}, {"CASE_TIMEOUT", "1"}, {"OUT", ctx.out}])

      assert output =~ "(timeout after 1s)"
      assert output =~ "raise CASE_TIMEOUT if setup is legitimately slow"
      assert length(loadtest_runs(ctx)) == 2
      assert Enum.count(docker_calls(ctx), &String.starts_with?(&1.args, "rm -f malachi-cluster-loadtest-")) == 2
      assert Enum.map(cases(ctx), &{&1["rf"], &1["outcome"]}) == [{3, "timeout after 1s"}, {1, "timeout after 1s"}]
    end

    test "a cluster that never turns healthy fails its case and the next case still runs", ctx do
      assert {output, 1} = run_script(ctx, [{"STUB_HEALTHY", "2"}, {"OUT", ctx.out}])
      assert output =~ "cluster did not converge to healthy (RF=1)"
      assert output =~ "cluster did not converge to healthy (RF=3)"
      assert loadtest_runs(ctx) == []
      assert Enum.map(cases(ctx), & &1["outcome"]) == ["unhealthy", "unhealthy"]
    end
  end

  describe "OUT" do
    test "appends one complete JSON object per case", ctx do
      File.mkdir_p!(Path.dirname(ctx.out))
      File.write!(ctx.out, ~s({"earlier": true}\n))

      assert {_output, 0} = run_script(ctx, [{"REAL_DISK", "1"}, {"OUT", ctx.out}])
      assert [%{"earlier" => true}, rf1, rf3] = cases(ctx)

      assert %{
               "data_mode" => "disk",
               "rf" => 1,
               "regime_label" => "LABEL batch=100 rsize=256 group_commit=true prealloc=67108864",
               "outcome" => "ok",
               "prealloc_bytes" => @prealloc,
               "du_bytes" => 3_072_000_000,
               "host" => "Linux" <> _,
               "docker" => "Stub Linux, kernel 6.8.0, storage driver overlay2",
               "docker_root_backing" => "ext4 rw,relatime on /dev/sda1 (disk sda rota=false model=Stub Disk)",
               "smoke_test" => false,
               "mid_window_cpu" => "malachi-cluster-1 150.00%",
               "loadtest" => %{"records_per_s" => 1000, "batch" => 100}
             } = rf1

      assert rf1["nodes"] == %{
               "malachi1" => %{"fstype" => "ext4", "mount_options" => "rw,relatime"},
               "malachi2" => %{"fstype" => "ext4", "mount_options" => "rw,relatime"},
               "malachi3" => %{"fstype" => "ext4", "mount_options" => "rw,relatime"}
             }

      assert %{"rf" => 3, "regime_label" => "LABEL batch=100 rsize=256 group_commit=false prealloc=67108864"} = rf3
    end

    test "creates its directory", ctx do
      out = Path.join([ctx.out, "..", "deeper", "cases.jsonl"]) |> Path.expand()
      assert {_output, 0} = run_script(ctx, [{"RFS", "1"}, {"OUT", out}])
      assert [%{"rf" => 1}] = out |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)
    end

    test "without it nothing is written", ctx do
      assert {_output, 0} = run_script(ctx, [{"RFS", "1"}])
      refute File.exists?(ctx.out)
    end
  end

  # --- helpers ---

  defp run_script(ctx, env, shell_prelude \\ "") do
    base = [
      {"PATH", "#{ctx.stubs}:#{System.get_env("PATH")}"},
      {"STUB_LOG", ctx.log},
      {"REAL_PROJECT", @project},
      {"REAL_MIX", System.find_executable("mix")},
      {"REAL_SLEEP", ctx.tools["sleep"]},
      {"DUR", "1"},
      {"WARM", "1"},
      {"CONNS", "2"},
      {"TOPICS", "4"},
      # Nothing from the environment running the tests may leak into a case.
      {"BATCH", nil},
      {"RSIZE", nil},
      {"RFS", nil},
      {"REAL_DISK", nil},
      {"DISK_PREALLOC_BYTES", nil},
      {"CASE_TIMEOUT", nil},
      {"OUT", nil},
      {"ALLOW_NON_LINUX", nil},
      {"MALACHI_DATA_ROOT", nil},
      {"MALACHI_SEGMENT_PREALLOC_BYTES", nil},
      {"MALACHI_SEGMENT_MAX_BYTES", nil},
      {"MALACHI_LOG_ROLL_MAX_BYTES", nil}
    ]

    env = Enum.reduce(env, base, fn {key, value}, acc -> List.keystore(acc, key, 0, {key, value}) end)
    script = Path.join([ctx.root, "benchmark", "docker-cluster.sh"])
    bash = System.find_executable("bash")

    System.cmd(bash, ["-c", ~s(#{shell_prelude} exec "#{bash}" "$0"), script], env: env, stderr_to_stdout: true)
  end

  # One entry per docker call: `%{verb, args, env}`, where args drops `compose -f <file>`.
  defp docker_calls(ctx) do
    if File.exists?(ctx.log) do
      for line <- String.split(File.read!(ctx.log), "\n", trim: true) do
        [args, env] = String.split(line, " || ")

        env =
          for pair <- String.split(env, " ", trim: true), into: %{} do
            [key, value] = String.split(pair, "=", parts: 2)
            {key, value}
          end

        args = String.replace_prefix(args, "compose -f docker-compose.cluster.yml ", "")
        %{verb: args |> String.split(" ") |> hd(), args: args, env: env}
      end
    else
      []
    end
  end

  defp compose_calls(ctx, verbs), do: Enum.filter(docker_calls(ctx), &(&1.verb in verbs and &1.env["RF"] != ""))

  defp loadtest_runs(ctx), do: Enum.filter(compose_calls(ctx, ["run"]), &(not (&1.args =~ "--entrypoint mix")))

  defp exec_commands(ctx) do
    for %{verb: "exec", args: args} <- docker_calls(ctx), do: args |> String.split(" ") |> Enum.at(3)
  end

  defp cases(ctx), do: ctx.out |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)

  defp write_stub!(dir, name, body) do
    path = Path.join(dir, name)
    File.write!(path, body)
    File.chmod!(path, 0o755)
  end

  # --- stubs ---

  defp uname_stub(system) do
    """
    #!/usr/bin/env bash
    case "$1" in
      -s) echo #{system} ;;
      *) echo "#{system} 25.5.0 arm64" ;;
    esac
    """
  end

  # Answers from STUB_FSTYPE (a filesystem, or `none` for an unreadable mount), STUB_DF_KB and STUB_DU_KB
  # (kilobytes, or `none` for no output), STUB_HEALTHY (healthy node count), STUB_LOADTEST (json, errors,
  # garbage, none, hang), STUB_MARKER (yes, or no for a window that never opens), STUB_LABEL (fail) and
  # STUB_REAL_MIX (1 sends the label to the real task).
  defp docker_stub do
    ~S"""
    #!/usr/bin/env bash
    # One line per call: the awk program the script sends to a node spans several.
    args="$*"
    echo "${args//$'\n'/ } || RF=${RF:-} ROOT=${MALACHI_DATA_ROOT:-} PREALLOC=${MALACHI_SEGMENT_PREALLOC_BYTES:-}" >> "$STUB_LOG"
    [ "$1" = compose ] && shift 3

    case "$1" in
      info)
        case "$*" in
          *DockerRootDir*) echo /var/lib/docker ;;
          *) echo "Stub Linux, kernel 6.8.0, storage driver overlay2" ;;
        esac ;;
      ps)
        for i in $(seq 1 "${STUB_HEALTHY:-3}"); do echo "malachi-cluster-$i"; done ;;
      stats) echo "malachi-cluster-1 150.00%" ;;
      build | up | down | logs | rm) : ;;
      exec)
        # `compose exec -T <service> <cmd>` puts the command fourth; the sampler's plain
        # `exec <container> sh -c ...` puts it third.
        command="$4"
        [ "$3" = sh ] && command=sh
        case "$command" in
          awk)
            [ "${STUB_FSTYPE:-ext4}" = none ] && exit 1
            echo "${STUB_FSTYPE:-ext4} rw,relatime" ;;
          df)
            [ "${STUB_DF_KB:-}" = none ] && exit 1
            echo "Filesystem 1024-blocks Used Available Capacity Mounted on"
            echo "/dev/sda1 200000000 1 ${STUB_DF_KB:-100000000} 1% /data" ;;
          du)
            [ "${STUB_DU_KB:-}" = none ] && exit 0
            echo "${STUB_DU_KB:-1000000} /data/malachi_log" ;;
          sh)
            [ "${STUB_MARKER:-yes}" = yes ] && echo yes
            exit 0 ;;
          *) echo "stub docker: unexpected exec $*" >&2; exit 97 ;;
        esac ;;
      run)
        batch="" rsize="" gc="" prealloc="" entrypoint=""
        while [ $# -gt 0 ]; do
          case "$1" in
            --entrypoint) entrypoint="$2"; shift ;;
            --batch) batch="$2"; shift ;;
            --record-size) rsize="$2"; shift ;;
            --group-commit) gc="$2"; shift ;;
            --segment-prealloc-bytes) prealloc="$2"; shift ;;
          esac
          shift
        done

        if [ "$entrypoint" = mix ]; then
          if [ "${STUB_LABEL:-}" = fail ]; then echo "label exploded" >&2; exit 1; fi
          if [ "${STUB_REAL_MIX:-}" = 1 ]; then
            cd "$REAL_PROJECT" && MIX_ENV=test exec "$REAL_MIX" malachi.loadtest.ceiling label \
              --batch "$batch" --record-size "$rsize" --group-commit "$gc" --segment-prealloc-bytes "$prealloc"
          fi
          echo "Compiling nothing"
          echo "LABEL batch=$batch rsize=$rsize group_commit=$gc prealloc=$prealloc"
          exit 0
        fi

        echo "starting the generator"
        case "${STUB_LOADTEST:-json}" in
          json) echo "{\"records_per_s\":1000,\"batch\":$batch,\"latency_ms\":{\"p50\":1.5,\"p99\":9.5},\"errors\":0,\"dropped\":0,\"overloaded\":0,\"reconnects\":0}" ;;
          errors) echo "{\"records_per_s\":1000,\"batch\":$batch,\"latency_ms\":{\"p50\":1.5,\"p99\":9.5},\"errors\":7,\"dropped\":0,\"overloaded\":0,\"reconnects\":0}" ;;
          garbage) echo "{not json" ;;
          none) echo "generator exploded" >&2; exit 1 ;;
          hang) exec "$REAL_SLEEP" 30 ;;
        esac ;;
      *) echo "stub docker: unexpected $*" >&2; exit 97 ;;
    esac
    """
  end

  # The health poll and the snapshot's half-window wait return at once; the marker poll's fractional
  # pause is kept short but real, so the poll does not spin.
  defp sleep_stub do
    ~S"""
    #!/usr/bin/env bash
    case "$1" in
      *.*) exec "$REAL_SLEEP" 0.05 ;;
    esac
    exit 0
    """
  end

  defp findmnt_stub do
    ~S"""
    #!/usr/bin/env bash
    echo "ext4 rw,relatime /dev/sda1 8:1"
    """
  end

  defp lsblk_stub do
    ~S"""
    #!/usr/bin/env bash
    cat <<'JSON'
    {"blockdevices": [
      {"name": "sda", "pkname": null, "maj:min": "8:0", "rota": false, "model": "Stub Disk  "},
      {"name": "sda1", "pkname": "sda", "maj:min": "8:1", "rota": false, "model": null}
    ]}
    JSON
    """
  end
end
