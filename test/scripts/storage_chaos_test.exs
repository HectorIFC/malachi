defmodule StorageChaosTest do
  # scripts/docker-storage-chaos.sh decides whether segment copies agree, what a failed comparison of them
  # leaves behind, and when: phase 2 recreates the volumes, so evidence kept any later is evidence of nothing
  # (issue #152). None of that needs a cluster, and a real run takes minutes.
  #
  # The drill runs for real in a throwaway tree with stubs first on the PATH: `docker` logs every call and
  # answers from STUB_* variables, and `sleep` returns at once. Portable on purpose (bash, awk, sed, jq), since
  # the drill is run by hand on macOS as well as on the Linux hosts it certifies.
  use ExUnit.Case, async: false

  @moduletag :tmp_dir
  @moduletag timeout: 120_000

  @scripts Path.expand("../../scripts", __DIR__)

  setup_all do
    # Missing tools fail loudly instead of skipping: a skipped harness test reads as a passing one.
    for tool <- ~w(bash jq awk sed uptime) do
      System.find_executable(tool) || flunk("#{tool} is required to test scripts/docker-storage-chaos.sh")
    end

    :ok
  end

  setup %{tmp_dir: dir} do
    root = Path.join(dir, "tree")
    File.mkdir_p!(Path.join(root, "scripts"))

    for name <- ~w(docker-storage-chaos.sh chaos_lib.sh) do
      File.cp!(Path.join(@scripts, name), Path.join([root, "scripts", name]))
    end

    # The result's metadata reads the version from mix.exs, as a real checkout has it.
    File.cp!(Path.join(@scripts, "../mix.exs"), Path.join(root, "mix.exs"))

    stubs = Path.join(dir, "stubs")
    File.mkdir_p!(stubs)
    write_stub!(stubs, "docker", docker_stub())
    write_stub!(stubs, "sleep", "#!/usr/bin/env bash\nexit 0\n")

    %{
      root: root,
      stubs: stubs,
      log: Path.join(dir, "docker.log"),
      work_root: Path.join(dir, "chaos"),
      result: Path.join(dir, "result.json")
    }
  end

  describe "invariant 4 on a passing run" do
    test "reports the per-copy summary, keeps no evidence and records none", ctx do
      assert {output, 0} = run_drill(ctx, [])

      assert output =~ "COPIES segments=2 whole_file=ok content=ok"
      assert output =~ "every segment's copies hold the same records on the 3 nodes"
      assert output =~ "STORAGE CHAOS CERTIFICATION PASSED"
      refute File.exists?(Path.join(ctx.work_root, "evidence"))
      assert %{"verdict" => "passed", "evidence_dir" => nil, "meta" => %{"malachi_version" => version}} = result(ctx)
      assert version =~ ~r/^\d+\.\d+\.\d+$/

      # Read-only, and without starting a node: a stopped node's copy is still a copy.
      # Retried for a minute, over all three volumes.
      assert [copies] =
               Enum.filter(docker_calls(ctx), &(&1 =~ "chaos_checker.exs copies" and not (&1 =~ "segment=")))

      assert copies =~
               "run --rm --no-deps -v vol-1:/copies/malachi1:ro -v vol-2:/copies/malachi2:ro -v vol-3:/copies/malachi3:ro"

      assert copies =~
               "copies malachi1,malachi2,malachi3 chaos_acked 12 5000 malachi1=/copies/malachi1/malachi_log " <>
                 "malachi2=/copies/malachi2/malachi_log malachi3=/copies/malachi3/malachi_log"
    end

    test "copies whose files differ but whose records agree pass, and keep nothing", ctx do
      # The shape every run on main showed (issue #152): a fenced primary's trimmed tail beside followers'
      # preallocated ones. It failed the whole-file comparison this invariant used to make.
      assert {output, 0} = run_drill(ctx, [{"STUB_COPIES", "benign"}, {"STUB_MD5_DIFFER", "0"}])
      assert output =~ "COPIES segments=2 whole_file=differs content=ok"
      refute output =~ "FAIL"
      refute File.exists?(Path.join(ctx.work_root, "evidence"))
    end

    test "repairs are judged by records too: events g and h compare the damaged segment on two nodes", ctx do
      assert {output, 0} = run_drill(ctx, [])
      assert output =~ "sealed copy deleted and node restarted"
      assert output =~ "copy repaired: follower holds the primary's records (identical)"

      repairs =
        Enum.filter(
          docker_calls(ctx),
          &(&1 =~
              "chaos_acked 12 5000 segment=chaos_acked-r0-s1 malachi1=/copies/malachi1/malachi_log " <>
                "malachi2=/copies/malachi2/malachi_log")
        )

      assert length(repairs) == 2
      assert Enum.all?(repairs, &(&1 =~ "--no-deps -v vol-1:/copies/malachi1:ro -v vol-2:/copies/malachi2:ro -v"))
    end

    test "the index repair is still judged byte for byte", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_MD5_DIFFER", "1"}])
      assert output =~ "FAIL: rotted sparse index was not rebuilt by the integrity scrub"
    end
  end

  describe "invariant 4 on a failing run" do
    setup ctx do
      {output, status} = run_drill(ctx, [{"STUB_COPIES", "content"}])
      %{output: output, status: status}
    end

    test "fails, and prints only the segments whose copies are not identical", %{output: output, status: status} do
      assert status == 1

      assert output =~
               "FAIL: segment copies did not reconverge to the same records across the nodes (see the per-copy report above)"

      assert output =~ "segments whose copies are not identical, per node:"

      assert output =~
               "COPY segment=chaos_acked-r0-s1 node=malachi2 status=ok marker=no records=6 bytes=240 digest=aaaaaaaaaaaa"

      assert output =~ "COPIES verdict=content segment=chaos_acked-r0-s1 control=sealed:6 nodes=malachi2"
      refute output =~ "COPY segment=chaos_acked-r0-s2"
    end

    test "keeps the report, the substrate and each node's disagreeing copies", ctx do
      assert %{"verdict" => "failed", "evidence_dir" => evidence} = result(ctx)
      assert String.starts_with?(evidence, Path.join(ctx.work_root, "evidence") <> "/")

      assert File.read!(Path.join(evidence, "copies.txt")) =~ "COPIES verdict=content"
      assert File.read!(Path.join(evidence, "substrate.txt")) =~ "load average"

      for n <- 1..3 do
        container = "malachi-cluster-#{n}"
        assert File.dir?(Path.join(evidence, container))

        assert Enum.any?(docker_calls(ctx), fn call ->
                 call =~ "run --rm -v vol-#{n}:/data:ro -v #{evidence}/#{container}:/out" and
                   call =~ "for d in chaos_acked-r0-s1 ; do"
               end)
      end
    end

    test "keeps the evidence before phase 2 recreates the volumes", ctx do
      calls = docker_calls(ctx)
      last_copy_out = calls |> Enum.with_index() |> Enum.filter(&(elem(&1, 0) =~ ":/out")) |> List.last() |> elem(1)
      phase_2_down = calls |> Enum.with_index() |> Enum.filter(&(elem(&1, 0) =~ "down -v")) |> Enum.at(1) |> elem(1)

      assert last_copy_out < phase_2_down
    end
  end

  defp run_drill(ctx, env) do
    base = [
      {"PATH", "#{ctx.stubs}:#{System.get_env("PATH")}"},
      {"STUB_LOG", ctx.log},
      {"CHAOS_WORK_ROOT", ctx.work_root},
      {"CHAOS_RESULT_FILE", ctx.result},
      # Nothing from the environment running the tests may leak into the drill.
      {"CHECKER_WINDOW_S", nil},
      {"FULL_WINDOW_S", nil},
      {"MALACHI_SEGMENT_PREALLOC_BYTES", nil}
    ]

    env = Enum.reduce(env, base, fn {key, value}, acc -> List.keystore(acc, key, 0, {key, value}) end)
    script = Path.join([ctx.root, "scripts", "docker-storage-chaos.sh"])

    System.cmd(System.find_executable("bash"), [script], env: env, stderr_to_stdout: true)
  end

  # One entry per docker call, `compose -f <file>...` dropped, newlines folded.
  defp docker_calls(ctx) do
    if File.exists?(ctx.log), do: String.split(File.read!(ctx.log), "\n", trim: true), else: []
  end

  defp result(ctx), do: ctx.result |> File.read!() |> Jason.decode!()

  defp write_stub!(dir, name, body) do
    path = Path.join(dir, name)
    File.write!(path, body)
    File.chmod!(path, 0o755)
  end

  # Answers from STUB_MD5_DIFFER (1: each node's index files hash differently) and STUB_COPIES (identical, benign or
  # content: the phase-1 comparison).
  defp docker_stub do
    ~S"""
    #!/usr/bin/env bash
    args="$*"
    if [ "$1" = compose ]; then
      shift
      while [ "$1" = -f ]; do shift 2; done
      args="$*"
    fi
    echo "${args//$'\n'/ }" >> "$STUB_LOG"

    copy_line() { # node
      echo "COPY segment=chaos_acked-r0-s1 node=$1 status=ok marker=no records=6 bytes=240 digest=aaaaaaaaaaaa trailing=0 files=0:2048:aaaaaaaaaaaa"
    }

    case "$1" in
      ps)
        case "$*" in
          *health=healthy*) printf 'malachi-cluster-1\nmalachi-cluster-2\nmalachi-cluster-3\n' ;;
          *) : ;;
        esac ;;
      inspect)
        case "$*" in
          *Mounts*) echo "vol-${2##*-}" ;;
          *Config.Image*) echo stub-image ;;
          *NetworkSettings*) echo stub-net ;;
          *RestartCount*) echo 0 ;;
          *Health.Status*) echo healthy ;;
        esac ;;
      info) echo "docker kernel 6.8.0-stub, 4 cpus, Stub OS" ;;
      exec)
        case "$*" in
          *md5sum*)
            if [ "${STUB_MD5_DIFFER:-0}" = 1 ]; then
              echo "md5-$2  /data/malachi_log/chaos_acked-r0-s1/00000000000000000000.log"
            else
              echo "same  /data/malachi_log/chaos_acked-r0-s1/00000000000000000000.log"
            fi ;;
        esac ;;
      logs) echo "segment {{\"chaos_acked\", 0}, 14}'s copy on this node failed in storage (:enospc)" ;;
      run)
        case "$*" in
          *"chaos_checker.exs produce"*)
            prev=""
            for a in "$@"; do
              if [ "$prev" = -v ] && [ "${a%:/chaos}" != "$a" ]; then echo c-1 > "${a%:/chaos}/acked.log"; fi
              prev="$a"
            done ;;
          *"chaos_checker.exs verify"*)
            echo "acked=1 read=1 missing=0"
            echo "VERIFY OK: every acknowledged write survived" ;;
          *"chaos_checker.exs topology"*)
            echo "SEGMENT range=0 seq=1 state=sealed start=0 length=6 bytes=240 primary=malachi@malachi1 replicas=malachi@malachi1,malachi@malachi2,malachi@malachi3"
            echo "SEGMENT range=0 seq=2 state=active start=6 length=1 bytes=40 primary=malachi@malachi1 replicas=malachi@malachi1,malachi@malachi2,malachi@malachi3"
            echo "SEGMENT range=0 seq=14 state=sealed start=7 length=1 bytes=40 primary=malachi@malachi1 replicas=malachi@malachi1,malachi@malachi2,malachi@malachi3" ;;
          *"chaos_checker.exs copies"*" 12 5000 segment="*)
            copy_line malachi1
            copy_line malachi2
            echo "COPIES verdict=identical segment=chaos_acked-r0-s1 control=sealed:6 nodes=-" ;;
          *"chaos_checker.exs copies"*)
            verdict="${STUB_COPIES:-identical}"
            for n in malachi1 malachi2 malachi3; do copy_line "$n"; done
            echo "COPY segment=chaos_acked-r0-s2 node=malachi1 status=ok marker=no records=1 bytes=40 digest=bbbbbbbbbbbb trailing=0 files=6:2048:bbbbbbbbbbbb"
            nodes=malachi2
            [ "$verdict" = identical ] && nodes=-
            echo "COPIES verdict=$verdict segment=chaos_acked-r0-s1 control=sealed:6 nodes=$nodes"
            echo "COPIES verdict=identical segment=chaos_acked-r0-s2 control=active:1 nodes=-"
            content=ok
            [ "$verdict" = content ] && content=differs
            whole=ok
            [ "$verdict" = identical ] || whole=differs
            echo "COPIES segments=2 whole_file=$whole content=$content"
            [ "$content" = ok ] ;;
          *--scenario*) echo '{"errors":0,"dropped":0,"records_per_s":100}' ;;
          *) : ;;
        esac ;;
      *) : ;;
    esac
    """
  end
end
