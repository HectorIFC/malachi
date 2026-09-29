defmodule ChaosLibTest do
  # The helpers scripts/chaos_lib.sh gives every drill that rolls, restarts or reshards a node: availability
  # through a step, the log of a container about to be recreated, the operator tasks run inside a node, and
  # a digest of a stopped node's data. None of that needs a cluster, so each helper is exercised for real in
  # a throwaway tree, with a `docker` stub first on the PATH that logs every call and answers from STUB_*
  # variables.
  use ExUnit.Case, async: true

  @moduletag :tmp_dir
  @moduletag timeout: 60_000

  @scripts Path.expand("../../scripts", __DIR__)

  setup_all do
    # Missing tools fail loudly instead of skipping: a skipped harness test reads as a passing one.
    for tool <- ~w(bash awk sed jq) do
      System.find_executable(tool) || flunk("#{tool} is required to test scripts/chaos_lib.sh")
    end

    :ok
  end

  setup %{tmp_dir: dir} do
    root = Path.join(dir, "tree")
    File.mkdir_p!(Path.join(root, "scripts"))
    File.cp!(Path.join(@scripts, "chaos_lib.sh"), Path.join([root, "scripts", "chaos_lib.sh"]))
    File.cp!(Path.join(@scripts, "../mix.exs"), Path.join(root, "mix.exs"))

    stubs = Path.join(dir, "stubs")
    File.mkdir_p!(stubs)
    write_stub!(stubs, "docker", docker_stub())

    %{root: root, stubs: stubs, log: Path.join(dir, "docker.log"), work_root: Path.join(dir, "chaos")}
  end

  describe "require_progress" do
    test "passes at once when the acked count already grew", ctx do
      assert {output, 0} = drive(ctx, "printf 'a\\nb\\n' > \"$WORK/acked.log\"; require_progress 1 'the step'")
      assert output =~ "acks kept flowing through the step (1 -> 2)"
      refute output =~ "FAIL"
    end

    test "keeps polling while a node that reports healthy is not serving yet", ctx do
      script = """
      n=0
      sleep() { n=$((n + 1)); [ "$n" -ge 3 ] && echo late >> "$WORK/acked.log"; return 0; }
      echo first > "$WORK/acked.log"
      require_progress 1 'the roll'
      echo "slept=$n"
      """

      assert {output, 0} = drive(ctx, script)
      assert output =~ "acks kept flowing through the roll (1 -> 2)"
      assert output =~ "slept=3"
    end

    test "fails after PROGRESS_TIMEOUT_S, naming the step and where the count stuck", ctx do
      script = """
      n=0
      sleep() { n=$((n + 1)); return 0; }
      printf 'a\\nb\\n' > "$WORK/acked.log"
      require_progress 2 'the roll' && echo returned-ok
      echo "slept=$n failed=$FAILED"
      """

      assert {output, 0} = drive(ctx, script, [{"PROGRESS_TIMEOUT_S", "10"}])
      assert output =~ "FAIL: no produce was acknowledged through the roll (stuck at 2)"
      refute output =~ "returned-ok"
      assert output =~ "slept=5 failed=1"
    end

    test "counts a missing acked file as zero acks", ctx do
      assert {output, 0} =
               drive(ctx, "sleep() { :; }; require_progress 0 'the start'; echo \"failed=$FAILED\"", [
                 {"PROGRESS_TIMEOUT_S", "2"}
               ])

      assert output =~ "FAIL: no produce was acknowledged through the start (stuck at 0)"
      assert output =~ "failed=1"
    end
  end

  describe "capture_logs" do
    test "saves a container's log, numbered in order, with an index naming the step", ctx do
      script = """
      capture_logs malachi-cluster-2 'phase 1 roll of malachi2'
      capture_logs malachi-cluster-3 'phase 1 roll of malachi3'
      ls "$WORK/logs"
      cat "$WORK/logs/index.txt"
      cat "$WORK/logs/01-malachi-cluster-2.log"
      """

      assert {output, 0} = drive(ctx, script)
      assert output =~ "01-malachi-cluster-2.log"
      assert output =~ "02-malachi-cluster-3.log"
      assert output =~ "01 malachi-cluster-2 phase 1 roll of malachi2"
      assert output =~ "02 malachi-cluster-3 phase 1 roll of malachi3"
      assert output =~ "log line of malachi-cluster-2"
    end

    test "says so, and does not fail the run, when the log cannot be read", ctx do
      assert {output, 0} =
               drive(ctx, "capture_logs malachi-cluster-1 'x'; echo \"failed=$FAILED\"", [{"STUB_LOGS", "fail"}])

      assert output =~ "could not read the log of malachi-cluster-1"
      assert output =~ "failed=0"
    end
  end

  describe "roll_node" do
    test "stops the node, keeps its log, requires progress while it is down, then recreates it", ctx do
      script = """
      sleep() { echo more >> "$WORK/acked.log"; }
      echo first > "$WORK/acked.log"
      roll_node malachi2 'phase 1 roll of malachi2'
      echo "status=$? failed=$FAILED"
      cat "$WORK/logs/index.txt"
      """

      assert {output, 0} = drive(ctx, script)
      assert output =~ "status=0 failed=0"
      assert output =~ "01 malachi-cluster-2 phase 1 roll of malachi2"
      assert output =~ "acks kept flowing through phase 1 roll of malachi2, while malachi2 was down ("
      assert output =~ "acks kept flowing through phase 1 roll of malachi2, after the node rejoined"
      assert output =~ "malachi2 served a clean produce of its own after phase 1 roll of malachi2"

      # Stopped, then its log read (it belongs to a container the recreate deletes), then recreated.
      calls = docker_calls(ctx)
      stop_at = Enum.find_index(calls, &(&1 == "stop malachi2"))
      logs_at = Enum.find_index(calls, &(&1 == "logs malachi-cluster-2"))
      up_at = Enum.find_index(calls, &(&1 == "up -d malachi2"))
      assert (stop_at && logs_at && up_at && stop_at < logs_at) and logs_at < up_at
    end

    test "fails a step whose acks stopped while the node was down, whatever comes after", ctx do
      # A build that stops serving whenever one node is down with it. Acks come back once the node does, so
      # growth measured across the recreate would pass; only growth while the node stays stopped says the
      # other two served without it.
      script = """
      sleep() { [ -e "$STUB_LOG.down" ] || echo more >> "$WORK/acked.log"; }
      echo first > "$WORK/acked.log"
      roll_node malachi2 'the roll'
      echo "status=$? failed=$FAILED"
      """

      assert {output, 0} = drive(ctx, script, [{"PROGRESS_TIMEOUT_S", "4"}])
      assert output =~ "FAIL: no produce was acknowledged through the roll, while malachi2 was down (stuck at 1)"
      assert output =~ "status=1 failed=1"
      # Brought back all the same, so the next step's node does not go down with this one still stopped; and
      # the step ends there, without checks that would only repeat the failure.
      assert "up -d malachi2" in docker_calls(ctx)
      refute output =~ "after the node rejoined"
    end

    test "fails a step whose acks stopped once the node rejoined", ctx do
      # The other two nodes acknowledge while this one is down, and nothing after: only growth measured once
      # the node is back says the cluster serves again.
      script = """
      sleep() { [ -e "$STUB_LOG.down" ] && echo more >> "$WORK/acked.log"; :; }
      echo first > "$WORK/acked.log"
      roll_node malachi2 'the roll'
      echo "status=$? failed=$FAILED"
      """

      assert {output, 0} = drive(ctx, script, [{"PROGRESS_TIMEOUT_S", "4"}])
      assert output =~ "acks kept flowing through the roll, while malachi2 was down (1 -> 2)"
      assert output =~ "FAIL: no produce was acknowledged through the roll, after the node rejoined (stuck at 2)"
      assert output =~ "status=1 failed=1"
    end

    test "fails the step when compose cannot stop the service, and recreates nothing", ctx do
      assert {output, 0} =
               drive(ctx, "roll_node malachi3 'the roll'; echo \"status=$? failed=$FAILED\"", [{"STUB_STOP", "fail"}])

      assert output =~ "FAIL: compose could not stop malachi3 (the roll)"
      assert output =~ "status=1 failed=1"
      refute Enum.any?(docker_calls(ctx), &(&1 == "up -d malachi3"))
    end

    test "fails a step whose replaced node does not serve, however many acks the others give", ctx do
      # Acks need only a quorum, so the two other nodes keep the count growing with the replaced node dead.
      script = """
      sleep() { echo more >> "$WORK/acked.log"; }
      echo first > "$WORK/acked.log"
      roll_node malachi2 'the roll'
      echo "status=$? failed=$FAILED"
      """

      assert {output, 0} = drive(ctx, script, [{"STUB_PRODUCE_FAILS_ON", "malachi2"}, {"PROGRESS_TIMEOUT_S", "4"}])
      assert output =~ "acks kept flowing through the roll, after the node rejoined"
      assert output =~ "FAIL: malachi2 did not serve a clean produce of its own after the roll"
      assert output =~ "status=1 failed=1"
      assert Enum.any?(docker_calls(ctx), &(&1 =~ "run --rm --no-deps loadtest --host malachi2 --scenario produce"))
    end

    test "gives a replaced node that is not serving yet a few tries before failing the step", ctx do
      # Healthy comes before serving: the node answers its HTTP check a few seconds before its broker does.
      script = """
      sleep() { echo more >> "$WORK/acked.log"; }
      echo first > "$WORK/acked.log"
      roll_node malachi2 'the roll'
      echo "status=$? failed=$FAILED"
      """

      env = [{"STUB_PRODUCE_FAILS_ON", "malachi2"}, {"STUB_PRODUCE_FAILS_TIMES", "2"}]
      assert {output, 0} = drive(ctx, script, env)
      assert output =~ "malachi2 served a clean produce of its own after the roll"
      assert output =~ "status=0 failed=0"
      assert Enum.count(docker_calls(ctx), &(&1 =~ "--host malachi2 --scenario produce")) == 3
    end

    test "gives up on a node that never serves after PROGRESS_TIMEOUT_S of wall clock, tries included", ctx do
      # Each try is a whole load test run of several seconds. Counting only the pauses between them let a node
      # that never serves hold the step for several times the timeout.
      script = """
      sleep() { echo more >> "$WORK/acked.log"; }
      echo first > "$WORK/acked.log"
      roll_node malachi2 'the roll'
      echo "status=$? failed=$FAILED"
      """

      # Counting only the (stubbed, instant) pauses would give four tries of three seconds; the wall clock
      # gives two, or one when this machine is loaded enough to push the first try past the deadline. Both
      # are the rule holding, so the assertion is the range and not the exact count.
      env = [{"STUB_PRODUCE_FAILS_ON", "malachi2"}, {"STUB_PRODUCE_SECONDS", "3"}, {"PROGRESS_TIMEOUT_S", "5"}]
      assert {output, 0} = drive(ctx, script, env)
      assert output =~ "FAIL: malachi2 did not serve a clean produce of its own after the roll"
      assert Enum.count(docker_calls(ctx), &(&1 =~ "--host malachi2 --scenario produce")) in 1..2
    end

    test "fails the step when compose cannot recreate the service", ctx do
      # Acks flow while the node is down, so the step gets as far as the recreate.
      script = """
      sleep() { echo more >> "$WORK/acked.log"; }
      echo first > "$WORK/acked.log"
      roll_node malachi3 'the roll'; echo "status=$? failed=$FAILED"
      """

      assert {output, 0} = drive(ctx, script, [{"STUB_UP", "fail"}])

      assert output =~ "FAIL: compose could not recreate malachi3 (the roll)"
      assert output =~ "status=1 failed=1"
    end

    test "fails the step when the cluster never gets back to 3/3 healthy", ctx do
      script = """
      sleep() { echo more >> "$WORK/acked.log"; }
      echo first > "$WORK/acked.log"
      roll_node malachi1 'the roll'; echo "status=$? failed=$FAILED"
      """

      assert {output, 0} = drive(ctx, script, [{"STUB_HEALTHY", "2"}])
      assert output =~ "FAIL: the cluster did not reconverge after the roll"
      assert output =~ "status=1 failed=1"
    end
  end

  describe "node_task and reshard_on_lease_holder" do
    test "node_task runs the mix task inside the node, short-named, against that node", ctx do
      assert {_output, 0} = drive(ctx, "node_task 2 'malachi.ring --show'")

      assert ("exec malachi-cluster-2 sh -c cd /app && elixir --sname chaoscli --cookie malachi_bench -S mix " <>
                "malachi.ring --show --node malachi@malachi2") in docker_calls(ctx)
    end

    test "walks the nodes until the lease holder accepts the reshard", ctx do
      assert {output, 0} =
               drive(ctx, "reshard_on_lease_holder 5; echo \"status=$?\"", [{"STUB_LEASE_HOLDER", "2"}])

      assert output =~ "reshard ran on node 2 (the lease holder)"
      assert output =~ "status=0"
      reshards = Enum.filter(docker_calls(ctx), &(&1 =~ "malachi.reshard --to 5"))
      assert length(reshards) == 2
    end

    test "stops at a refusal that is not about the lease, and prints it", ctx do
      assert {output, 0} =
               drive(ctx, "reshard_on_lease_holder 5; echo \"status=$?\"", [{"STUB_LEASE_HOLDER", "broken"}])

      assert output =~ "    reshard refused: the ring is busy"
      assert output =~ "status=1"
      assert Enum.count(docker_calls(ctx), &(&1 =~ "malachi.reshard")) == 1
    end

    test "fails when no node reports holding the lease", ctx do
      assert {output, 0} =
               drive(ctx, "reshard_on_lease_holder 5; echo \"status=$?\"", [{"STUB_LEASE_HOLDER", "none"}])

      assert output =~ "no node accepted the reshard; none reported holding the lease"
      assert output =~ "status=1"
    end
  end

  describe "volume_md5" do
    test "hashes every file of the log and control-plane directories read-only, in a stable order", ctx do
      assert {output, 0} = drive(ctx, "volume_md5 vol-3 some-image")
      assert output =~ "abc  ./malachi.format"

      assert ("run --rm -v vol-3:/data:ro --entrypoint sh some-image -c " <>
                "cd /data && test -d malachi_log && find malachi_log $(test -d malachi_ra && echo malachi_ra) " <>
                "-type f -exec md5sum {} + | sort -k 2") in docker_calls(ctx)
    end

    # The command the stub only records, run for real against a directory laid out like a node's volume.
    test "the command covers both directories, leaves out a missing control-plane one, and fails with no log", ctx do
      drive(ctx, "volume_md5 vol-3 img")
      call = Enum.find(docker_calls(ctx), &String.starts_with?(&1, "run --rm -v vol-3"))
      command = call |> String.split(" -c ", parts: 2) |> List.last()

      data = Path.join(ctx.tmp_dir, "data")
      File.mkdir_p!(Path.join(data, "malachi_log"))
      File.write!(Path.join([data, "malachi_log", "malachi.format"]), "1")
      local = String.replace(command, "cd /data", "cd #{data}")

      {log_only, 0} = System.cmd("sh", ["-c", local])
      assert log_only =~ "malachi_log/malachi.format"

      File.mkdir_p!(Path.join(data, "malachi_ra"))
      File.write!(Path.join([data, "malachi_ra", "names.dets"]), "x")
      {both, 0} = System.cmd("sh", ["-c", local])
      assert [_log, _ra] = String.split(both, "\n", trim: true)
      assert both =~ "malachi_ra/names.dets"

      File.rm_rf!(Path.join(data, "malachi_log"))
      assert {_output, status} = System.cmd("sh", ["-c", local], stderr_to_stdout: true)
      assert status != 0
    end

    test "returns non-zero when the volume cannot be read", ctx do
      assert {output, 0} = drive(ctx, "volume_md5 vol-3 img; echo \"status=$?\"", [{"STUB_RUN", "fail"}])
      assert output =~ ~r/^status=1$/m
    end
  end

  describe "finish" do
    test "keeps the saved container logs as evidence on a failed run", ctx do
      script = """
      capture_logs malachi-cluster-1 'before'
      fail 'something broke'
      finish 'LIB TEST'
      """

      assert {output, 1} = drive(ctx, script)
      assert output =~ "LIB TEST FAILED"
      assert [evidence] = Path.wildcard(Path.join([ctx.work_root, "evidence", "*", "*-node-logs"]))
      assert File.read!(Path.join(evidence, "01-malachi-cluster-1.log")) =~ "log line of malachi-cluster-1"
      assert File.read!(Path.join(evidence, "index.txt")) =~ "01 malachi-cluster-1 before"
    end

    test "prints the nodes' logs as the postmortem of a failed run only when the run started the cluster", ctx do
      own = "OWN_CLUSTER=1; fail 'something broke'; finish 'LIB TEST'"
      assert {output, 1} = drive(ctx, own)
      assert output =~ "postmortem: node logs"
      assert "logs --tail 200 malachi-cluster-1" in docker_calls(ctx)

      File.rm!(ctx.log)

      # A run that stopped before starting its cluster: those nodes, if any, are another run's.
      assert {output, 1} = drive(ctx, "fail 'something broke'; finish 'LIB TEST'")
      refute output =~ "postmortem"
      refute Enum.any?(docker_calls(ctx), &String.starts_with?(&1, "logs "))
      assert output =~ "LIB TEST FAILED"
    end

    test "keeps nothing on a passing run", ctx do
      assert {output, 0} = drive(ctx, "capture_logs malachi-cluster-1 'before'; finish 'LIB TEST'")
      assert output =~ "LIB TEST PASSED"
      refute File.exists?(Path.join(ctx.work_root, "evidence"))
    end
  end

  describe "check_convergence and check_control_plane" do
    test "requires 3/3 healthy and the same control-plane state on every member", ctx do
      assert {output, 0} = drive(ctx, "check_convergence; echo \"failed=$FAILED\"")
      assert output =~ "3/3 healthy"
      assert output =~ "CONTROL-PLANE OK groups=9 projection=all"
      assert output =~ "every control-plane group holds the same state on every member"
      assert output =~ "failed=0"

      assert Enum.any?(
               docker_calls(ctx),
               &(&1 =~ "run --rm --no-deps -v " and
                   &1 =~
                     "--entrypoint sh loadtest -c cd /app && elixir --sname chaoschk --cookie malachi_bench -S mix " <>
                       "run --no-start /chaos_scripts/chaos_checker.exs control-plane " <>
                       "malachi@malachi1,malachi@malachi2,malachi@malachi3 --project all")
             )
    end

    test "fails naming a divergence", ctx do
      assert {output, 0} =
               drive(ctx, "check_convergence; echo \"failed=$FAILED\"", [{"STUB_CONTROL_PLANE", "diverged"}])

      assert output =~ "CONTROL-PLANE MISMATCH group=malachi_log_vn_0 index=40 state differs"
      assert output =~ "FAIL: the control plane diverged: members hold different states at the same index"
      assert output =~ "failed=1"
    end

    test "fails when the members never reach one index", ctx do
      assert {output, 0} =
               drive(ctx, "check_control_plane all; echo \"status=$?\"", [{"STUB_CONTROL_PLANE", "unsettled"}])

      assert output =~ "FAIL: the control plane never settled on every member (see the PENDING lines above)"
      assert output =~ "status=1"
    end

    test "fails, with the tail of its output, when the check itself did not run", ctx do
      assert {output, 0} = drive(ctx, "check_control_plane all; echo \"status=$?\"", [{"STUB_CONTROL_PLANE", "crash"}])
      assert output =~ "FAIL: the control-plane check did not run: ** (RuntimeError) boom"
      assert output =~ "status=1"
    end

    test "compares whole states on a stock cluster, whose services carry one tag each", ctx do
      # Compose tags every service's build on its own (<project>-malachi1, -malachi2, ...), so the tags
      # differ even though the three nodes run one build. The drill says what it runs; the tags are not asked.
      assert {output, 0} = drive(ctx, "check_control_plane all; echo \"status=$?\"")
      assert output =~ ~r/^status=0$/m
      assert Enum.any?(docker_calls(ctx), &(&1 =~ "control-plane"))
    end

    test "compares whole states when the drill declares one image on every node", ctx do
      assert {output, 0} =
               drive(ctx, "NODE_IMAGES=(img:new img:new img:new); check_control_plane all; echo \"status=$?\"")

      assert output =~ ~r/^status=0$/m
    end

    test "refuses to compare whole states while the drill declares different images", ctx do
      script = "NODE_IMAGES=(img:old img:new img:new); check_control_plane all; echo \"status=$?\""
      assert {output, 0} = drive(ctx, script)

      assert output =~
               "FAIL: harness error: whole control-plane states are comparable on one image only, " <>
                 "and the nodes run 2 (img:old img:new img:new)"

      assert output =~ "status=1"
      refute Enum.any?(docker_calls(ctx), &(&1 =~ "control-plane"))
    end

    test "compares the topics projection across different images", ctx do
      assert {output, 0} =
               drive(ctx, "NODE_IMAGES=(img:old img:new img:new); check_control_plane topics; echo \"status=$?\"")

      assert output =~ "status=0"
      assert Enum.any?(docker_calls(ctx), &(&1 =~ "--project topics"))
    end

    test "does not read the control plane of a cluster that is not healthy", ctx do
      assert {output, 0} =
               drive(ctx, "sleep() { :; }; check_convergence; echo \"failed=$FAILED\"", [{"STUB_HEALTHY", "2"}])

      assert output =~ "FAIL: cluster is not fully healthy at the end"
      refute Enum.any?(docker_calls(ctx), &(&1 =~ "control-plane"))
    end
  end

  describe "check_clean_produce" do
    test "produces through every host by default, without starting any node", ctx do
      assert {output, 0} = drive(ctx, "check_clean_produce; echo \"failed=$FAILED\"")
      assert output =~ "post-chaos produce clean: 100 rec/s"
      assert output =~ "failed=0"
      assert Enum.any?(docker_calls(ctx), &(&1 =~ "run --rm --no-deps loadtest --host malachi1,malachi2,malachi3"))
    end

    test "gives a cluster that is not serving yet a few tries, and records the clean one", ctx do
      env = [{"STUB_PRODUCE_FAILS_ON", "malachi1,malachi2,malachi3"}, {"STUB_PRODUCE_FAILS_TIMES", "1"}]
      assert {output, 0} = drive(ctx, "sleep() { :; }; check_clean_produce; echo \"failed=$FAILED\"", env)
      assert output =~ "post-chaos produce clean: 100 rec/s"
      assert output =~ "failed=0"
    end

    test "fails a cluster that never serves again, with what the last try said", ctx do
      env = [{"STUB_PRODUCE_FAILS_ON", "malachi1,malachi2,malachi3"}, {"PROGRESS_TIMEOUT_S", "2"}]
      assert {output, 0} = drive(ctx, "sleep() { :; }; check_clean_produce; echo \"failed=$FAILED\"", env)
      assert output =~ ~s(FAIL: post-chaos produce not clean: {"errors":5)
      assert output =~ "failed=1"
    end

    test "produces through the hosts it is given, while another node is down on purpose", ctx do
      assert {_output, 0} = drive(ctx, "check_clean_produce malachi1,malachi2")
      assert Enum.any?(docker_calls(ctx), &(&1 =~ "run --rm --no-deps loadtest --host malachi1,malachi2 "))
    end
  end

  describe "ring_show and ring_vnode_count" do
    test "read the recorded ring's vnode count through node 1", ctx do
      assert {output, 0} = drive(ctx, "echo \"count=$(ring_vnode_count)\"")
      assert output =~ "count=5"
      assert Enum.any?(docker_calls(ctx), &(&1 =~ "mix malachi.ring --show --node malachi@malachi1"))
    end

    test "retry a ring store that cannot answer yet, and give up with what it said", ctx do
      assert {output, 0} =
               drive(ctx, "sleep() { :; }; ring_show; echo \"status=$?\"", [{"STUB_RING", "unreadable"}])

      assert output =~ "ring store not ready"
      assert output =~ ~r/^status=1$/m
      assert Enum.count(docker_calls(ctx), &(&1 =~ "malachi.ring --show")) == 20
    end
  end

  describe "the result and the teardown" do
    test "finish removes the images this run built, after the containers are gone", ctx do
      assert {_output, 0} = drive(ctx, "OWN_CLUSTER=1; OWN_IMAGES=(img:old img:new); finish 'LIB TEST'")
      calls = docker_calls(ctx)
      down_at = Enum.find_index(calls, &(&1 == "down"))
      rm_at = Enum.find_index(calls, &(&1 == "image rm img:old img:new"))
      assert down_at && rm_at && down_at < rm_at
    end

    test "a run that has to abort still writes its result and removes its images, when it named itself", ctx do
      result = Path.join(ctx.work_root, "abort.json")
      File.mkdir_p!(ctx.work_root)

      script =
        "CHAOS_CERTIFICATION='LIB TEST'; OWN_IMAGES=(img:old); abort_run 'the image did not build'; echo not-reached"

      assert {output, 1} = drive(ctx, script, [{"CHAOS_RESULT_FILE", result}])
      assert output =~ "FAIL: the image did not build"
      assert output =~ "LIB TEST FAILED"
      refute output =~ "not-reached"
      assert "image rm img:old" in docker_calls(ctx)

      assert %{"verdict" => "failed", "failures" => ["the image did not build"]} =
               result |> File.read!() |> Jason.decode!()
    end

    test "finish tears down only a cluster this run started", ctx do
      # A run that ends before start_cluster (an image that did not build) must not take down a cluster of
      # the same compose project that another run is using.
      assert {_output, 1} = drive(ctx, "fail 'no cluster yet'; finish 'LIB TEST'")
      refute "down" in docker_calls(ctx)

      assert {_output, 0} = drive(ctx, "OWN_CLUSTER=1; finish 'LIB TEST'")
      assert "down" in docker_calls(ctx)
    end

    test "a run that has to abort without a name just stops, as the drills always have", ctx do
      # A cluster and an image this run owns, so a `finish` reached by mistake would leave a trace: it would
      # take the cluster down, remove the image, print the postmortem and the FAILED line.
      script = "OWN_CLUSTER=1; OWN_IMAGES=(img:x); abort_run 'cluster never converged'; echo not-reached"
      assert {output, 1} = drive(ctx, script)
      assert output =~ "FAIL: cluster never converged"
      refute output =~ "not-reached"
      refute output =~ "postmortem"
      refute output =~ "FAILED (see FAIL lines above)"
      refute Enum.any?(docker_calls(ctx), &(&1 == "down" or String.starts_with?(&1, "image rm")))
    end

    test "finish removes no image when the run built none", ctx do
      assert {_output, 0} = drive(ctx, "finish 'LIB TEST'")
      refute Enum.any?(docker_calls(ctx), &String.starts_with?(&1, "image rm"))
    end

    test "the result carries a drill's own details, and null without them", ctx do
      result = Path.join(ctx.work_root, "r.json")
      File.mkdir_p!(ctx.work_root)

      script = "CHAOS_DETAILS='{\"old_ref\":\"v1\"}'; finish 'LIB TEST'"
      assert {_output, 0} = drive(ctx, script, [{"CHAOS_RESULT_FILE", result}])
      assert %{"details" => %{"old_ref" => "v1"}} = result |> File.read!() |> Jason.decode!()

      assert {_output, 0} = drive(ctx, "finish 'LIB TEST'", [{"CHAOS_RESULT_FILE", result}])
      assert %{"details" => nil, "verdict" => "passed"} = result |> File.read!() |> Jason.decode!()
    end
  end

  # Sources the library in the tree, the way a drill does, then runs `body` in the same shell.
  defp drive(ctx, body, env \\ []) do
    base = [
      {"PATH", "#{ctx.stubs}:#{System.get_env("PATH")}"},
      {"STUB_LOG", ctx.log},
      {"CHAOS_WORK_ROOT", ctx.work_root},
      {"CHAOS_RESULT_FILE", nil},
      {"PROGRESS_TIMEOUT_S", nil},
      {"RF", "3"},
      {"CHAOS_TOPIC", "chaos_acked"},
      {"TREE", ctx.root}
    ]

    env = Enum.reduce(env, base, fn {key, value}, acc -> List.keystore(acc, key, 0, {key, value}) end)
    # The tree is passed by name: a tmp dir is named after its test, and a test name may hold a quote.
    script = "cd \"$TREE\" && . scripts/chaos_lib.sh && {\n#{body}\n}"
    System.cmd(System.find_executable("bash"), ["-c", script], env: env, stderr_to_stdout: true)
  end

  defp docker_calls(ctx) do
    if File.exists?(ctx.log), do: String.split(File.read!(ctx.log), "\n", trim: true), else: []
  end

  defp write_stub!(dir, name, body) do
    path = Path.join(dir, name)
    File.write!(path, body)
    File.chmod!(path, 0o755)
  end

  # Answers from these variables:
  #   STUB_LOGS          fail: `docker logs` fails
  #   STUB_UP            fail: `compose up` fails
  #   STUB_HEALTHY       how many containers `docker ps --filter health=healthy` lists (default 3)
  #   STUB_LEASE_HOLDER  the node that accepts a reshard, `none`, or `broken` (node 1 refuses for another reason)
  #   STUB_RUN           fail: `docker run` fails
  #   STUB_CONTROL_PLANE ok (default), diverged, unsettled or crash: the checker's control-plane mode
  #   STUB_RING          unreadable: the ring store never answers
  #   STUB_PRODUCE_FAILS_ON  a host: a loadtest through that host alone reports errors
  #   STUB_PRODUCE_FAILS_TIMES  how many times it does before one comes back clean (default: always)
  #   STUB_PRODUCE_SECONDS      how long each of those runs takes, in real seconds (default: none)
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

    case "$1" in
      ps)
        case "$*" in
          *health=healthy*)
            for i in $(seq 1 "${STUB_HEALTHY:-3}"); do echo "malachi-cluster-$i"; done ;;
        esac ;;
      logs)
        [ "${STUB_LOGS:-}" = fail ] && { echo "Error: no such container" >&2; exit 1; }
        echo "log line of $2" ;;
      # A marker file says a node is stopped, so a test's `sleep` can give acks only while it is, or only after.
      stop)
        [ "${STUB_STOP:-}" = fail ] && exit 1
        touch "$STUB_LOG.down"
        exit 0 ;;
      up)
        [ "${STUB_UP:-}" = fail ] && exit 1
        rm -f "$STUB_LOG.down"
        exit 0 ;;
      exec)
        case "$*" in
          *"malachi.ring --show"*)
            if [ "${STUB_RING:-}" = unreadable ]; then echo "ring store not ready"; exit 1; fi
            echo "durable ring: version 3, 5 vnodes (ring size 1024)"; exit 0 ;;
          *malachi.reshard*)
            node="${2##*-}"
            case "${STUB_LEASE_HOLDER:-1}" in
              broken) echo "reshard refused: the ring is busy"; exit 1 ;;
              "$node") echo "resharded to 5"; exit 0 ;;
              *) echo "this node does not hold the cluster lease; try another node"; exit 1 ;;
            esac ;;
        esac ;;
      inspect)
        # As compose tags a build: one tag per service, even for one build.
        case "$*" in *Config.Image*) echo "project-malachi${2##*-}" ;; esac ;;
      run)
        case "$*" in
          *"chaos_checker.exs control-plane"*)
            case "${STUB_CONTROL_PLANE:-ok}" in
              ok) echo "CONTROL-PLANE OK groups=9 projection=all" ;;
              diverged)
                echo "CONTROL-PLANE MISMATCH group=malachi_log_vn_0 index=40 state differs: a=1 b=2 c=1"
                echo "CONTROL-PLANE DIVERGED projection=all"
                exit 1 ;;
              unsettled)
                echo "CONTROL-PLANE PENDING group=malachi_log_vn_0 indexes differ: a=1 b=2 c=2"
                echo "CONTROL-PLANE UNSETTLED projection=all"
                exit 1 ;;
              crash) echo "** (RuntimeError) boom"; exit 1 ;;
            esac
            exit 0 ;;
          *"--host ${STUB_PRODUCE_FAILS_ON:-none} "*)
            n=$(( $(cat "$STUB_LOG.produce" 2>/dev/null || echo 0) + 1 ))
            echo "$n" > "$STUB_LOG.produce"
            [ -n "${STUB_PRODUCE_SECONDS:-}" ] && /bin/sleep "$STUB_PRODUCE_SECONDS"
            if [ -z "${STUB_PRODUCE_FAILS_TIMES:-}" ] || [ "$n" -le "$STUB_PRODUCE_FAILS_TIMES" ]; then
              echo '{"errors":5,"dropped":0,"records_per_s":0}'
            else
              echo '{"errors":0,"dropped":0,"records_per_s":100}'
            fi
            exit 0 ;;
          *--scenario*) echo '{"errors":0,"dropped":0,"records_per_s":100}'; exit 0 ;;
        esac
        [ "${STUB_RUN:-}" = fail ] && exit 1
        echo "abc  ./malachi.format" ;;
      *) : ;;
    esac
    """
  end
end
