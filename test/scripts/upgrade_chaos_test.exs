defmodule UpgradeChaosTest do
  # scripts/docker-upgrade-chaos.sh decides which release is OLD, which tree each image is built from, which
  # image and machine version pin each node runs at each step, and what counts as a refused rollback (issue
  # #196). None of that needs a cluster, and a real run takes about half an hour.
  #
  # The drill runs for real in a throwaway git repository with release tags, so the OLD ref is resolved,
  # exported and patched by git itself; `docker` is a stub first on the PATH that logs every call and answers
  # from STUB_* variables, and `sleep` returns at once.
  use ExUnit.Case, async: false

  @moduletag :tmp_dir
  @moduletag timeout: 300_000

  @scripts Path.expand("../../scripts", __DIR__)

  # A test run by a git hook inherits these, and any one of them would point git at the host repository
  # instead of the throwaway one (the same set test/scripts/worktree_env_test.exs clears).
  @no_git_env [{"GIT_DIR", nil}, {"GIT_WORK_TREE", nil}, {"GIT_COMMON_DIR", nil}, {"GIT_INDEX_FILE", nil}]

  setup_all do
    # Missing tools fail loudly instead of skipping: a skipped harness test reads as a passing one.
    for tool <- ~w(bash jq awk sed tar git uptime) do
      System.find_executable(tool) || flunk("#{tool} is required to test scripts/docker-upgrade-chaos.sh")
    end

    :ok
  end

  setup %{tmp_dir: dir} do
    root = Path.join(dir, "repo")
    File.mkdir_p!(Path.join(root, "scripts"))

    for name <- ~w(docker-upgrade-chaos.sh chaos_lib.sh) do
      File.cp!(Path.join(@scripts, name), Path.join([root, "scripts", name]))
    end

    write!(root, "benchmark/Dockerfile.loadtest", "FROM scratch\n")
    write!(root, "lib/malachi/cluster/machine_version.ex", "defmodule M do\n  @code_version 3\nend\n")
    write!(root, "lib/canary.txt", "released\n")
    write!(root, ".gitignore", "/tmp/\n")
    write!(root, "test/support/canary.patch", patch("lib/canary.txt", "released", "canary") <> version_patch(4))

    git!(root, ["init", "-q"])
    git!(root, ["config", "user.email", "drill@example.com"])
    git!(root, ["config", "user.name", "drill"])
    git!(root, ["config", "commit.gpgsign", "false"])
    write!(root, "mix.exs", ~s(defmodule P do\n  @version "0.13.0"\nend\n))
    git!(root, ["add", "."])
    git!(root, ["commit", "-q", "-m", "one"])
    git!(root, ["tag", "v0.13.0"])
    write!(root, "mix.exs", ~s(defmodule P do\n  @version "0.16.2"\nend\n))
    write!(root, "lib/feature.txt", "0.16.2\n")
    git!(root, ["add", "."])
    git!(root, ["commit", "-q", "-m", "release 0.16.2"])
    git!(root, ["tag", "v0.16.2"])
    write!(root, "mix.exs", ~s(defmodule P do\n  @version "0.16.4"\nend\n))
    write!(root, "lib/feature.txt", "0.16.4\n")
    # From 0.16.4 on (in this repository) a node resumes its vnode members after a restart (#136).
    write!(root, "lib/malachi/application.ex", "defmodule A do\n  def resume_local_vnodes(_v), do: []\nend\n")
    git!(root, ["add", "."])
    git!(root, ["commit", "-q", "-am", "two"])
    git!(root, ["tag", "v0.16.4"])
    # The tree is ahead of the newest release: its code differs from v0.16.4's.
    write!(root, "mix.exs", ~s(defmodule P do\n  @version "0.17.0"\nend\n))
    write!(root, "lib/feature.txt", "unreleased\n")
    git!(root, ["commit", "-q", "-am", "three"])

    stubs = Path.join(dir, "stubs")
    File.mkdir_p!(stubs)
    write!(stubs, "docker", docker_stub())
    File.chmod!(Path.join(stubs, "docker"), 0o755)
    # Time passing is acks arriving: a checker keeps producing while the drill waits.
    write!(stubs, "sleep", ~S"""
    #!/usr/bin/env bash
    f="$STUB_LOG.acked"
    [ -f "$f" ] && [ -f "$(cat "$f")" ] && [ ! -f "$STUB_LOG.stalled" ] && echo more >> "$(cat "$f")"
    exit 0
    """)

    File.chmod!(Path.join(stubs, "sleep"), 0o755)

    %{
      root: root,
      stubs: stubs,
      log: Path.join(dir, "docker.log"),
      work_root: Path.join(dir, "chaos"),
      result: Path.join(dir, "result.json")
    }
  end

  describe "a passing run" do
    test "rolls forward, back, forward again and records what it ran", ctx do
      assert {output, 0} = run_drill(ctx)

      assert output =~
               "OLD is v0.16.4 (release 0.16.4, machine version 3: the newest release, whose code this tree changes); " <>
                 "NEW is the working tree with the canary"

      assert output =~ "the canary flag is refused while nodes 1 and 2 run OLD"
      assert output =~ "the canary command is refused at machine version 3"
      assert output =~ "OLD node 1 dropped 7 unknown replication casts and kept serving"
      assert output =~ "the ring has 5 vnodes"
      assert output =~ "the control plane moved to machine version 4"
      assert output =~ "the canary command applies at machine version 4"
      assert output =~ "node 3 on OLD refused the format 2 directory with exit 78"
      assert output =~ "node 3's data directory is byte for byte what it was (2 files)"
      assert output =~ "UPGRADE AND ROLLBACK CERTIFICATION PASSED"

      assert %{
               "verdict" => "passed",
               "invariants" => %{"acked_writes" => acked},
               "details" => %{
                 "old_ref" => "v0.16.4",
                 "old_rule" => "the newest release, whose code this tree changes",
                 "old_version" => "0.16.4",
                 "old_machine_version" => 3,
                 "old_patch" => nil,
                 "new_patch" => nil,
                 "negative_control" => false,
                 "vnodes_after_split" => 5,
                 "acked_writes_by_phase" => %{"phase1" => p1, "phase2" => p2, "phase3" => p3}
               }
             } = result(ctx)

      assert acked == p1 + p2 + p3 and p1 > 0 and p2 > 0 and p3 > 0
    end

    test "builds OLD from the release's own tree and NEW from the working tree with the canary", ctx do
      File.write!(Path.join(ctx.root, "lib/canary.txt"), "released\n")
      assert {_output, 0} = run_drill(ctx)

      builds = Enum.filter(docker_calls(ctx), &String.starts_with?(&1, "build "))
      assert Enum.any?(builds, &(&1 =~ "-t repo-upgrade:old" and &1 =~ "ctx=ctx-old canary=released version=0.16.4"))
      assert Enum.any?(builds, &(&1 =~ "-t repo-upgrade:new" and &1 =~ "ctx=ctx-new canary=canary version=0.17.0"))
      assert "image rm repo-upgrade:old repo-upgrade:new" in docker_calls(ctx)
    end

    test "runs every node on the image and pin each step calls for", ctx do
      assert {_output, 0} = run_drill(ctx)
      ups = Enum.filter(docker_calls(ctx), &String.starts_with?(&1, "up -d"))

      assert ups == [
               "up -d --force-recreate malachi1 malachi2 malachi3 | pin=3 images=old,old,old restart=on-failure",
               # phase 1, under the pin
               "up -d malachi3 | pin=3 images=old,old,new restart=on-failure",
               "up -d malachi2 | pin=3 images=old,new,new restart=on-failure",
               "up -d malachi1 | pin=3 images=new,new,new restart=on-failure",
               # phase 2, back to OLD, still under the pin
               "up -d malachi3 | pin=3 images=new,new,old restart=on-failure",
               "up -d malachi2 | pin=3 images=new,old,old restart=on-failure",
               "up -d malachi1 | pin=3 images=old,old,old restart=on-failure",
               # phase 3, forward under the pin, then finalized node by node
               "up -d malachi3 | pin=3 images=old,old,new restart=on-failure",
               "up -d malachi2 | pin=3 images=old,new,new restart=on-failure",
               "up -d malachi1 | pin=3 images=new,new,new restart=on-failure",
               "up -d malachi3 | pin= images=new,new,new restart=on-failure",
               "up -d malachi2 | pin= images=new,new,new restart=on-failure",
               "up -d malachi1 | pin= images=new,new,new restart=on-failure",
               # the refused rollback, with its restarts bounded, and the way back
               "up -d malachi3 | pin= images=new,new,old restart=on-failure:3",
               "up -d malachi3 | pin= images=new,new,new restart=on-failure"
             ]
    end

    test "keeps the log of every container before replacing it", ctx do
      assert {_output, 0} = run_drill(ctx)

      # Between two recreates, the node about to be recreated had its log read.
      Enum.reduce(docker_calls(ctx), MapSet.new(), fn call, logged ->
        case Regex.run(~r/^up -d malachi(\d) /, call) do
          [_, n] ->
            assert "malachi-cluster-#{n}" in logged, "no log saved before #{call}"
            MapSet.new()

          nil ->
            case Regex.run(~r/^logs (malachi-cluster-\d)$/, call) do
              [_, container] -> MapSet.put(logged, container)
              nil -> logged
            end
        end
      end)
    end

    test "splits the ring on the lease holder while every node runs NEW", ctx do
      assert {_output, 0} = run_drill(ctx)
      calls = docker_calls(ctx)
      split_at = Enum.find_index(calls, &(&1 =~ "malachi.reshard --to 5"))
      last_p1_swap = Enum.find_index(calls, &(&1 == "up -d malachi1 | pin=3 images=new,new,new restart=on-failure"))
      first_p2_swap = Enum.find_index(calls, &(&1 == "up -d malachi3 | pin=3 images=new,new,old restart=on-failure"))
      assert last_p1_swap < split_at and split_at < first_p2_swap
    end
  end

  describe "choosing OLD" do
    test "on a tree whose code is the newest release, OLD is the release before it", ctx do
      # What main looks like: every merge is tagged, so the newest tag holds this very code, and rolling from
      # it would certify a release against itself.
      git!(ctx.root, ["reset", "-q", "--hard", "v0.16.4"])
      assert {output, 0} = run_drill(ctx)

      assert output =~
               "OLD is v0.16.2 (release 0.16.2, machine version 3: the newest release whose code differs from this " <>
                 "tree, which is release v0.16.4's)"

      assert %{"details" => %{"old_ref" => "v0.16.2"}} = result(ctx)
    end

    test "skips a release that only changed the docs, whose code is this tree's too", ctx do
      git!(ctx.root, ["reset", "-q", "--hard", "v0.16.4"])
      File.write!(Path.join(ctx.root, "README.md"), "docs only\n")
      git!(ctx.root, ["add", "README.md"])
      git!(ctx.root, ["commit", "-q", "-m", "docs"])
      git!(ctx.root, ["tag", "v0.16.5"])

      assert {output, 0} = run_drill(ctx)
      assert output =~ "OLD is v0.16.2 (release 0.16.2, machine version 3: the newest release whose code differs"
      assert output =~ "which is release v0.16.5's)"
    end

    test "a new file git does not track yet counts as code, since the NEW image is built with it", ctx do
      git!(ctx.root, ["reset", "-q", "--hard", "v0.16.4"])
      File.write!(Path.join(ctx.root, "lib/brand_new.ex"), "defmodule BrandNew do\nend\n")
      assert {output, 0} = run_drill(ctx)

      assert output =~
               "OLD is v0.16.4 (release 0.16.4, machine version 3: the newest release, whose code this tree changes)"
    end

    test "an uncommitted change counts as a tree ahead of the newest release", ctx do
      git!(ctx.root, ["reset", "-q", "--hard", "v0.16.4"])
      File.write!(Path.join(ctx.root, "lib/feature.txt"), "work in progress\n")
      assert {output, 0} = run_drill(ctx)

      assert output =~
               "OLD is v0.16.4 (release 0.16.4, machine version 3: the newest release, whose code this tree changes)"
    end

    test "refuses a release below the unsharded floor, before building anything, and records why", ctx do
      assert {output, 2} = run_drill(ctx, [{"OLD_REF", "v0.13.0"}])

      assert output =~
               "OLD_REF=v0.13.0 is release 0.13.0; an unsharded run needs 0.14.2 or later, the first release with " <>
                 "every gate the canary exercises and every control-plane group NEW starts"

      refute Enum.any?(docker_calls(ctx), &String.starts_with?(&1, "build"))
      assert %{"verdict" => "failed", "failures" => [failure]} = result(ctx)
      assert failure =~ "usage error: OLD_REF=v0.13.0 is release 0.13.0"
    end

    test "refuses a release that would run sharded below the sharded floor", ctx do
      # An old release that already resumed its vnode members would be taken sharded, and phase 2 then needs
      # it to bring up a vnode a split created (#242), which only 0.16.1 and later do.
      git!(ctx.root, ["checkout", "-q", "v0.13.0"])
      File.write!(Path.join(ctx.root, "mix.exs"), ~s(defmodule P do\n  @version "0.15.0"\nend\n))

      File.write!(
        Path.join(ctx.root, "lib/malachi/application.ex"),
        "defmodule A do\n  def resume_local_vnodes(_v), do: []\nend\n"
      )

      git!(ctx.root, ["add", "."])
      git!(ctx.root, ["commit", "-q", "-m", "0.15.0"])
      git!(ctx.root, ["tag", "v0.15.0"])
      git!(ctx.root, ["checkout", "-q", "-"])

      assert {output, 2} = run_drill(ctx, [{"OLD_REF", "v0.15.0"}])
      assert output =~ "OLD_REF=v0.15.0 is release 0.15.0; a sharded run needs 0.16.1 or later"
    end

    test "refuses 0.14.1, which lacks a control-plane group NEW starts", ctx do
      git!(ctx.root, ["checkout", "-q", "v0.13.0"])
      File.write!(Path.join(ctx.root, "mix.exs"), ~s(defmodule P do\n  @version "0.14.1"\nend\n))
      git!(ctx.root, ["commit", "-q", "-am", "0.14.1"])
      git!(ctx.root, ["tag", "v0.14.1"])
      git!(ctx.root, ["checkout", "-q", "-"])

      assert {output, 2} = run_drill(ctx, [{"OLD_REF", "v0.14.1"}])
      assert output =~ "OLD_REF=v0.14.1 is release 0.14.1; an unsharded run needs 0.14.2 or later"
    end

    test "refuses an OLD whose machine version it cannot read", ctx do
      git!(ctx.root, ["checkout", "-q", "v0.13.0"])
      File.write!(Path.join(ctx.root, "mix.exs"), ~s(defmodule P do\n  @version "0.16.3"\nend\n))
      File.write!(Path.join(ctx.root, "lib/malachi/cluster/machine_version.ex"), "defmodule M do\nend\n")
      git!(ctx.root, ["commit", "-q", "-am", "0.16.3"])
      git!(ctx.root, ["tag", "v0.16.3"])
      git!(ctx.root, ["checkout", "-q", "-"])

      assert {output, 2} = run_drill(ctx, [{"OLD_REF", "v0.16.3"}])
      assert output =~ "cannot read the machine version of v0.16.3"
    end

    test "refuses a canary tree whose machine version it cannot read", ctx do
      # A patch that computes the version rather than stating it: the drill cannot know what the control
      # plane should reach, so it stops rather than certify against an empty number.
      File.write!(
        Path.join(ctx.root, "test/support/canary.patch"),
        patch("lib/canary.txt", "released", "canary") <> version_patch("Version.current()")
      )

      assert {output, 2} = run_drill(ctx)
      assert output =~ "cannot read the machine version of the canary tree"
    end

    test "accepts an unsharded release between the two floors", ctx do
      git!(ctx.root, ["checkout", "-q", "v0.13.0"])
      File.write!(Path.join(ctx.root, "mix.exs"), ~s(defmodule P do\n  @version "0.14.2"\nend\n))
      git!(ctx.root, ["commit", "-q", "-am", "0.14.2"])
      git!(ctx.root, ["tag", "v0.14.2"])
      git!(ctx.root, ["checkout", "-q", "-"])

      assert {output, 0} = run_drill(ctx, [{"OLD_REF", "v0.14.2"}])
      assert output =~ "control plane: 1 vnodes (unsharded, no split: v0.14.2"
    end

    test "an image that does not build ends the run through finish: recorded, and its images removed", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_BUILD_FAILS", "new"}])
      assert output =~ "FAIL: the NEW image did not build"
      assert output =~ "UPGRADE AND ROLLBACK CERTIFICATION FAILED"
      assert "image rm repo-upgrade:old" in docker_calls(ctx)
      refute Enum.any?(docker_calls(ctx), &String.starts_with?(&1, "up "))
      # No cluster was started, so none is torn down: another run of this compose project may be using it.
      refute "down" in docker_calls(ctx)

      # The record still says which release the run was rolling from.
      assert %{
               "verdict" => "failed",
               "failures" => ["the NEW image did not build"],
               "details" => %{"old_ref" => "v0.16.4", "old_machine_version" => 3}
             } = result(ctx)
    end

    test "names every node's image before its first compose command", ctx do
      # Compose reads the whole override, which requires an image for every node, whatever service a command
      # names: building the load generator before the images are placed fails the run on a real host.
      assert {_output, 0} = run_drill(ctx)
      refute Enum.any?(docker_calls(ctx), &(&1 =~ "UPGRADE_IMAGE_1 is missing"))
      assert Enum.any?(docker_calls(ctx), &(&1 =~ ~r/^build loadtest/))
    end

    test "refuses a ref that names nothing", ctx do
      assert {output, 2} = run_drill(ctx, [{"OLD_REF", "v9.9.9"}])
      assert output =~ "OLD_REF=v9.9.9 names no commit here; fetch the tags (fetch-depth: 0)"
    end

    test "says what to do when no release tag is reachable", ctx do
      git!(ctx.root, ["tag", "-d", "v0.13.0", "v0.16.2", "v0.16.4"])
      assert {output, 2} = run_drill(ctx)
      assert output =~ "no release tag is reachable from HEAD; fetch the tags (fetch-depth: 0) or set OLD_REF"
    end

    test "fails loudly when the canary no longer applies", ctx do
      File.write!(Path.join(ctx.root, "lib/canary.txt"), "drifted\n")
      assert {output, 1} = run_drill(ctx)
      assert output =~ "FAIL: the upgrade canary patch (test/support/canary.patch) does not apply to the NEW tree"

      # OLD was resolved before the patch step, and the record says so.
      assert %{"verdict" => "failed", "details" => %{"old_ref" => "v0.16.4", "old_machine_version" => 3}} =
               result(ctx)
    end

    test "records a run with an extra patch as a negative control", ctx do
      old_patch = Path.join(ctx.root, "old.patch")
      File.write!(old_patch, patch("lib/canary.txt", "released", "reverted"))
      assert {_output, 0} = run_drill(ctx, [{"OLD_PATCH", old_patch}])

      assert %{"details" => %{"negative_control" => true, "old_patch" => ^old_patch, "new_patch" => nil}} = result(ctx)
      assert Enum.any?(docker_calls(ctx), &(&1 =~ "ctx=ctx-old canary=reverted"))
    end
  end

  describe "the shape of the control plane" do
    test "runs four vnodes and splits the ring when OLD resumes its vnode members after a restart", ctx do
      assert {output, 0} = run_drill(ctx)
      assert output =~ "control plane: 4 vnodes (sharded: v0.16.4 resumes its vnode members after a restart)"
      assert Enum.any?(docker_calls(ctx), &(&1 =~ "malachi.reshard --to 5"))
      assert %{"details" => %{"vnodes" => 4, "vnodes_after_split" => 5}} = result(ctx)
    end

    test "runs unsharded and splits nothing when OLD cannot bring its vnode members back", ctx do
      assert {output, 0} = run_drill(ctx, [{"OLD_REF", "v0.16.2"}])
      assert output =~ "control plane: 1 vnodes (unsharded, no split: v0.16.2 does not resume its vnode members"
      refute Enum.any?(docker_calls(ctx), &(&1 =~ "malachi.reshard"))
      refute Enum.any?(docker_calls(ctx), &(&1 =~ "malachi.ring --show"))
      assert %{"details" => %{"vnodes" => 1, "vnodes_after_split" => nil}} = result(ctx)
    end
  end

  describe "the mixed cluster" do
    test "fails when the flag is accepted while OLD nodes remain", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_MIXED_FLAG", "accepted"}])
      assert output =~ "FAIL: the canary flag was not refused while nodes 1 and 2 run OLD"
    end

    test "accepts the refusal of an OLD leader, which does not know the command at all", ctx do
      assert {output, 0} = run_drill(ctx, [{"STUB_MIXED_CANARY", "old_leader"}])
      assert output =~ "the canary command is refused at machine version 3"
    end

    test "fails when node 3's own metadata cache took the refused command", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_CACHE_NOTE", "1"}])
      assert output =~ "FAIL: node 3's metadata cache took the command every replica refused (canary_note=1)"
    end

    test "reads node 3's cache in the same VM that sends the command, before a reconcile can re-seed it", ctx do
      assert {_output, 0} = run_drill(ctx)

      assert [call] =
               Enum.filter(docker_calls(ctx), &(&1 =~ "exec malachi-cluster-3" and &1 =~ "{:canary_note, ~s("))

      assert call =~ ":sys, :get_state, [Malachi.LogBroker]"
    end

    test "fails when the canary command is not refused at the pinned version", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_MIXED_CANARY", "applied"}])
      assert output =~ "FAIL: the canary command was not refused at machine version 3"
    end

    test "fails when no OLD node ever counted the unknown cast", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_UNKNOWN_CASTS", "0"}])
      assert output =~ "FAIL: OLD node 1 counted no unknown replication cast"
    end

    test "reads the unknown casts from an OLD node, not the NEW one that sends them", ctx do
      assert {_output, 0} = run_drill(ctx)
      metrics = Enum.filter(docker_calls(ctx), &(&1 =~ ~r/^exec -i \S+ sh -s$/))
      assert metrics != [] and Enum.all?(metrics, &(&1 == "exec -i malachi-cluster-1 sh -s"))
    end

    test "fails when the topic metadata diverged", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_CONTROL_PLANE", "diverged"}])
      assert output =~ "FAIL: the control plane diverged"
    end
  end

  describe "the refused rollback" do
    test "fails when OLD starts on the format 2 directory", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_REFUSAL", "healthy"}])
      assert output =~ "FAIL: node 3 on OLD did not exit 78 on a format 2 directory (last state: running 0 healthy)"
    end

    test "fails when OLD exits 78 for another reason than the format marker", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_REFUSAL", "flag"}])
      assert output =~ "FAIL: node 3 on OLD exited 78 without the format marker's refusal"
    end

    test "fails when the refused start changed a byte of the data directory", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_MD5_CHANGES", "1"}])
      assert output =~ "FAIL: node 3's data directory changed across the refused start"
    end

    test "takes the canary's machine version from NEW's own tree, not a number of its own", ctx do
      # The next release that raises the machine version moves the canary with it: only the patch changes.
      File.write!(
        Path.join(ctx.root, "test/support/canary.patch"),
        patch("lib/canary.txt", "released", "canary") <> version_patch(7)
      )

      assert {output, 0} = run_drill(ctx, [{"STUB_EFFECTIVE", "7"}])
      assert output =~ "the canary command is refused at machine version 3"
      assert output =~ "the control plane moved to machine version 7"
      assert output =~ "the canary command applies at machine version 7"
    end

    test "fails when the finalized control plane stays below version 4", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_EFFECTIVE", "3"}])
      assert output =~ "FAIL: the control plane did not move to machine version 4 after the pin was removed (at '3')"
    end

    test "fails when a data directory never reaches format 2 after the flip", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_FORMAT", "1"}])
      assert output =~ "FAIL: node 1's data directory did not reach format 2 after the flip (at '1')"
    end
  end

  describe "every other step that can fail" do
    test "fails when the split does not complete", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_SPLIT", "refused"}])
      assert output =~ "FAIL: the split did not complete"
    end

    test "fails when no produce is acknowledged through the split", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_STALL_ON_SPLIT", "1"}, {"PROGRESS_TIMEOUT_S", "4"}])
      assert output =~ "FAIL: no produce was acknowledged through the split"
    end

    test "fails when the rollback to OLD loses a vnode the split created", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_RING_AFTER_ROLLBACK", "4"}])
      assert output =~ "FAIL: expected a 5-vnode ring, got '4'"
    end

    test "fails when compose cannot start node 3 on OLD after the flip", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_UP_REFUSED_NODE", "fail"}])
      assert output =~ "FAIL: compose could not start node 3 on OLD"
    end

    test "fails when nodes 1 and 2 do not serve while node 3 is refused", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_PRODUCE_FAILS_ON", "malachi1,malachi2"}, {"PROGRESS_TIMEOUT_S", "2"}])
      assert output =~ "FAIL: nodes 1 and 2 did not serve a clean produce while node 3 was down"
    end

    test "fails when the canary command does not apply once the pin is removed", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_FINAL_CANARY", "refused"}])
      assert output =~ "FAIL: the canary command did not apply at machine version 4"
    end

    test "fails when the flag is refused with every node on NEW", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_FINAL_FLAG", "refused"}])
      assert output =~ "FAIL: the canary flag was refused with every node on NEW"
    end
  end

  describe "the known OLD crash filter" do
    # Run as the drill defines it: its list and function, lifted from the script, over a log written here.
    @known_then_unknown """
    13:40:11.858 [error] GenServer Malachi.LogHealer terminating
    ** (stop) exited in: GenServer.call(Malachi.LogBroker, :metadata, 5000)
        ** (EXIT) time out
        (elixir 1.19.6) lib/gen_server.ex:1142: GenServer.call/3
    13:40:12.000 [error] GenServer Malachi.LogReplication terminating
    ** (FunctionClauseError) no function clause matching in Malachi.Cluster.ReplicationServer.handle_cast/2
    13:40:13.000 [info] done
    """

    defp filter(ctx, override) do
      log = Path.join(ctx.tmp_dir, "old.log")
      File.write!(log, @known_then_unknown)

      script = """
      eval "$(sed -n '/^OLD_KNOWN_CRASHES=(/,/^}/p' "$DRILL")"
      #{override}
      without_known_crashes "$LOG"
      echo "status=$?"
      """

      System.cmd("bash", ["-c", script],
        env: [{"DRILL", Path.join(@scripts, "docker-upgrade-chaos.sh")}, {"LOG", log}],
        stderr_to_stdout: true
      )
    end

    test "drops a known crash and keeps the one after it, which fails the run", ctx do
      {output, 0} = filter(ctx, "")
      refute output =~ "LogHealer terminating"
      assert output =~ "LogReplication terminating"
      assert output =~ "** (FunctionClauseError)"
      assert output =~ "status=0"
    end

    test "with no known crash listed, removes nothing", ctx do
      {output, 0} = filter(ctx, "OLD_KNOWN_CRASHES=()")
      assert output =~ "LogHealer terminating"
      assert output =~ "LogReplication terminating"
    end

    test "an entry that is not a regular expression fails, rather than hide what it did not read", ctx do
      {output, 0} = filter(ctx, "OLD_KNOWN_CRASHES=('GenServer.call(Malachi.LogBroker')")
      refute output =~ "status=0"
    end

    test "one entry that matches an empty line fails the list, even beside a valid one", ctx do
      # Entries are joined with |, so a single match-anything entry would excuse every crash.
      for entry <- ["'()'", "'.*'", "'x*'"] do
        {output, 0} = filter(ctx, "OLD_KNOWN_CRASHES=('GenServer\\.call\\(Malachi\\.LogBroker' #{entry})")
        assert output =~ "status=2", entry
        refute output =~ "terminating", entry
      end
    end
  end

  describe "the closing invariants" do
    test "fails on a crash report in the saved log of a container that was since replaced", ctx do
      # The crash is only in the log node 2's OLD container had when phase 1 replaced it: the live
      # containers' logs at the end are clean, so only the saved copy can show it.
      assert {output, 1} = run_drill(ctx, [{"STUB_CRASH", "1"}])
      assert output =~ "--- 02-malachi-cluster-2.log"
      assert output =~ "** (FunctionClauseError) no function clause matching"
      assert output =~ "FAIL: a process crashed during the upgrade"
    end

    test "an OLD build's crash that is listed as known is shown, not failed: it is that release's own", ctx do
      # What the drill met on Linux: v0.17.0's heal coordinator died on a broker call timing out, the bug
      # this branch fixes. A released build cannot be fixed by certifying the next one.
      assert {output, 0} = run_drill(ctx, [{"STUB_CRASH", "old_timeout"}])
      assert output =~ "(OLD, a crash it is known to have, listed in OLD_KNOWN_CRASHES; not failed)"
      assert output =~ "GenServer Malachi.LogHealer terminating"
      refute output =~ "FAIL: a process crashed"
    end

    # The list is the drill's own, so these edit the copy of the drill this test runs.
    defp known_crashes!(ctx, entry) do
      drill = Path.join([ctx.root, "scripts", "docker-upgrade-chaos.sh"])
      text = File.read!(drill)
      [known] = Regex.run(~r/^  'GenServer.*:metadata'$/m, text)
      File.write!(drill, String.replace(text, known, "  " <> entry))
    end

    test "an entry awk cannot read as a regular expression fails the run instead of hiding the log", ctx do
      known_crashes!(ctx, "'GenServer.call(Malachi.LogBroker'")
      assert {output, 1} = run_drill(ctx, [{"STUB_CRASH", "old_timeout"}])

      assert output =~
               "FAIL: OLD_KNOWN_CRASHES holds an entry that matches an empty line or one awk cannot read as a regular expression"

      assert output =~ "FAIL: a process crashed during the upgrade"
    end

    test "an empty entry fails the run: an empty regular expression would excuse every crash", ctx do
      known_crashes!(ctx, "''")
      assert {output, 1} = run_drill(ctx, [{"STUB_CRASH", "old_timeout"}])
      assert output =~ "FAIL: OLD_KNOWN_CRASHES holds an entry that matches an empty line"
    end

    test "an entry that matches every line fails the run like an empty one", ctx do
      known_crashes!(ctx, "'.*'")
      assert {output, 1} = run_drill(ctx, [{"STUB_CRASH", "old_timeout"}])
      assert output =~ "FAIL: OLD_KNOWN_CRASHES holds an entry that matches an empty line"
      assert output =~ "FAIL: a process crashed during the upgrade"
    end

    test "any other OLD crash fails, including a known message in a shape it cannot read", ctx do
      # Not a FunctionClauseError: the tag matched a clause and the payload did not fit it.
      assert {output, 1} = run_drill(ctx, [{"STUB_CRASH", "old_keyerror"}])
      assert output =~ "repo-upgrade:old)"
      assert output =~ "** (KeyError) key :records not found"
      assert output =~ "FAIL: a process crashed during the upgrade"
    end

    test "any crash in a NEW build's log fails, whatever it was over", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_CRASH", "new_timeout"}])
      assert output =~ "repo-upgrade:new)"
      assert output =~ "GenServer Malachi.LogHealer terminating"
      assert output =~ "FAIL: a process crashed during the upgrade"
    end

    test "fails a phase whose steps outlast its checker window", ctx do
      assert {output, 1} = run_drill(ctx, [{"PHASE2_WINDOW_S", "0"}])
      assert output =~ "FAIL: phase 2 took"
      assert output =~ "raise PHASE2_WINDOW_S"
    end

    test "refuses to start while another cluster runs", ctx do
      assert {output, 1} = run_drill(ctx, [{"STUB_RUNNING", "malachi-cluster-2"}])
      assert output =~ "refusing to start: another cluster is already running (malachi-cluster-2)."
      refute Enum.any?(docker_calls(ctx), &String.starts_with?(&1, "up "))
      # Refused before building: the images are tagged per checkout, so building first would re-tag, and the
      # way out remove, the images the running cluster uses. Still a result, published as failed.
      refute Enum.any?(docker_calls(ctx), &String.starts_with?(&1, "build "))
      refute Enum.any?(docker_calls(ctx), &String.starts_with?(&1, "image rm"))
      refute Enum.any?(docker_calls(ctx), &String.starts_with?(&1, "down"))
      assert %{"verdict" => "failed", "failures" => failures} = result(ctx)
      assert "another cluster is already running (malachi-cluster-2)" in failures
      # Nor are that cluster's logs printed as this run's postmortem.
      refute output =~ "postmortem"
      refute Enum.any?(docker_calls(ctx), &String.starts_with?(&1, "logs "))
    end
  end

  defp run_drill(ctx, env \\ []) do
    base = [
      {"PATH", "#{ctx.stubs}:#{System.get_env("PATH")}"},
      {"STUB_LOG", ctx.log},
      {"CHAOS_WORK_ROOT", ctx.work_root},
      {"CHAOS_RESULT_FILE", ctx.result},
      {"UPGRADE_CANARY_PATCH", "test/support/canary.patch"},
      # Nothing from the environment running the tests may leak into the drill.
      {"OLD_REF", nil},
      {"OLD_PATCH", nil},
      {"NEW_PATCH", nil},
      {"PHASE1_WINDOW_S", nil},
      {"PHASE2_WINDOW_S", nil},
      {"PHASE3_WINDOW_S", nil},
      {"PROGRESS_TIMEOUT_S", nil},
      {"COMPOSE_PROJECT_NAME", nil},
      {"MALACHI_RA_MACHINE_VERSION", nil},
      {"MALACHI_SEGMENT_PREALLOC_BYTES", nil},
      {"UPGRADE_IMAGE_1", nil},
      {"UPGRADE_IMAGE_2", nil},
      {"UPGRADE_IMAGE_3", nil},
      {"UPGRADE_RESTART_3", nil},
      {"CHAOS_CERTIFICATION", nil}
      | @no_git_env
    ]

    env = Enum.reduce(env, base, fn {key, value}, acc -> List.keystore(acc, key, 0, {key, value}) end)
    script = Path.join([ctx.root, "scripts", "docker-upgrade-chaos.sh"])
    System.cmd(System.find_executable("bash"), [script], env: env, stderr_to_stdout: true)
  end

  defp docker_calls(ctx) do
    if File.exists?(ctx.log), do: String.split(File.read!(ctx.log), "\n", trim: true), else: []
  end

  defp result(ctx), do: ctx.result |> File.read!() |> Jason.decode!()

  defp write!(root, path, body) do
    full = Path.join(root, path)
    File.mkdir_p!(Path.dirname(full))
    File.write!(full, body)
  end

  defp git!(root, args) do
    {output, 0} = System.cmd("git", args, cd: root, stderr_to_stdout: true, env: @no_git_env)
    output
  end

  # Raises NEW's machine version the way the real canary patch does, which is where the drill reads it.
  defp version_patch(version) do
    """
    diff --git a/lib/malachi/cluster/machine_version.ex b/lib/malachi/cluster/machine_version.ex
    --- a/lib/malachi/cluster/machine_version.ex
    +++ b/lib/malachi/cluster/machine_version.ex
    @@ -1,3 +1,3 @@
     defmodule M do
    -  @code_version 3
    +  @code_version #{version}
     end
    """
  end

  defp patch(path, from, to) do
    """
    diff --git a/#{path} b/#{path}
    --- a/#{path}
    +++ b/#{path}
    @@ -1 +1 @@
    -#{from}
    +#{to}
    """
  end

  # Answers from these variables:
  #   STUB_RUNNING        names `docker ps` lists as running
  #   STUB_MIXED_FLAG     accepted: the flag is switched on while OLD nodes remain
  #   STUB_MIXED_CANARY   applied: the canary command applies at the pinned version;
  #                       old_leader: an OLD leader, which does not know it, refuses it
  #   STUB_UNKNOWN_CASTS  what an OLD node's /metrics counts (default 7)
  #   STUB_CONTROL_PLANE  diverged: the checker's control-plane mode reports a divergence
  #   STUB_EFFECTIVE      the effective machine version after the pin is removed (default 4); also the
  #                       version the phase 1 refusal names the canary as introduced at, so a test that
  #                       raises NEW's version in its patch sets this to the same number
  #   STUB_FORMAT         the format the markers report after the flip (default 2)
  #   STUB_REFUSAL        healthy: OLD starts on the directory; flag: it exits 78 for the flag, not the marker
  #   STUB_MD5_CHANGES    1: the data directory's digest differs after the refused start
  #   STUB_CRASH          1: node 2's first log (its OLD container, replaced in phase 1) carries a crash report
  #                       over an unknown message; old_timeout / new_timeout: node 2's first log from that
  #                       build carries the heal coordinator's :metadata call timing out; old_keyerror: node 2's
  #                       first OLD log carries a KeyError, a known tag in a shape it cannot read
  #   STUB_CACHE_NOTE     what node 3's broker cache holds as the canary note (default nil)
  #   STUB_BUILD_FAILS    old or new: that image does not build
  #   STUB_SPLIT          refused: the reshard is refused
  #   STUB_STALL_ON_SPLIT 1: no ack arrives once the split has run
  #   STUB_RING_AFTER_ROLLBACK  the vnode count every ring read after the first reports
  #   STUB_UP_REFUSED_NODE      fail: compose cannot start node 3 on OLD after the flip
  #   STUB_PRODUCE_FAILS_ON     a host list: a load test through exactly those hosts reports errors
  #   STUB_FINAL_CANARY   refused: the canary command is refused once the pin is removed
  #   STUB_FINAL_FLAG     refused: the flag is refused with every node on NEW
  #
  # Every call after the checker starts appends an ack to its file, the way a producing checker would.
  defp docker_stub do
    ~S"""
    #!/usr/bin/env bash
    full="$*"
    args="$*"
    if [ "$1" = compose ]; then
      shift
      while [ "$1" = -f ]; do shift 2; done
      args="$*"
    fi
    # Compose reads the whole override, and it requires an image for every node.
    if [[ "$full" == *docker-compose.upgrade.yml* ]] && [ -z "${UPGRADE_IMAGE_1:-}" ]; then
      echo "compose: required variable UPGRADE_IMAGE_1 is missing a value" | tee -a "$STUB_LOG"
      exit 1
    fi
    case "$1" in
      up)
        img() { v="$1"; echo "${v##*:}"; }
        # Remember what each recreated node runs, so `inspect` answers with it as docker does.
        for n in 1 2 3; do
          case " $* " in *" malachi$n "*) v="UPGRADE_IMAGE_$n"; echo "${!v:-}" > "$STUB_LOG.image.$n" ;; esac
        done
        args="$args | pin=${MALACHI_RA_MACHINE_VERSION:-} images=$(img "$UPGRADE_IMAGE_1"),$(img "$UPGRADE_IMAGE_2"),$(img "$UPGRADE_IMAGE_3") restart=${UPGRADE_RESTART_3:-}" ;;
      build)
        ctx="${@: -1}"
        [ -n "${STUB_BUILD_FAILS:-}" ] && [ "$(basename "$ctx")" = "ctx-${STUB_BUILD_FAILS}" ] && {
          echo "build ctx=$(basename "$ctx") failed" >> "$STUB_LOG"
          exit 1
        }
        [ -n "$ctx" ] && [ -d "$ctx" ] &&
          args="$args ctx=$(basename "$ctx") canary=$(cat "$ctx/lib/canary.txt") version=$(sed -n 's/.*"\(.*\)".*/\1/p' "$ctx/mix.exs")" ;;
    esac
    echo "${args//$'\n'/ }" >> "$STUB_LOG"
    if [ "$1" = up ] && [ "${STUB_UP_REFUSED_NODE:-}" = fail ] && [ "${UPGRADE_RESTART_3:-}" = on-failure:3 ]; then
      exit 1
    fi

    # The split is the moment acks stop, when asked: before this very call counts as one.
    [[ "$full" == *malachi.reshard* ]] && [ "${STUB_STALL_ON_SPLIT:-}" = 1 ] && touch "$STUB_LOG.stalled"
    acked_file="$STUB_LOG.acked"
    [ -f "$acked_file" ] && [ -f "$(cat "$acked_file")" ] && [ ! -f "$STUB_LOG.stalled" ] && echo more >> "$(cat "$acked_file")"

    case "$1" in
      ps)
        case "$*" in
          *health=healthy*) printf 'malachi-cluster-1\nmalachi-cluster-2\nmalachi-cluster-3\n' ;;
          *) [ -n "${STUB_RUNNING:-}" ] && printf '%s\n' "$STUB_RUNNING" ;;
        esac ;;
      inspect)
        case "$*" in
          *Mounts*) echo "vol-${2##*-}" ;;
          *Config.Image*) cat "$STUB_LOG.image.${2##*-}" 2>/dev/null || echo "repo-malachi${2##*-}" ;;
          *NetworkSettings*) echo stub-net ;;
          *State.Status*)
            case "${STUB_REFUSAL:-}" in
              healthy) echo "running 0 healthy" ;;
              *) echo "exited 78 " ;;
            esac ;;
        esac ;;
      exec)
        case "$*" in
          *"malachi.flag enable"*)
            if [ "$2" = malachi-cluster-3 ] && [ "${STUB_MIXED_FLAG:-}" != accepted ]; then
              echo "these nodes are not alive or do not support that flag: malachi@malachi1, malachi@malachi2. Upgrade them"
              exit 1
            fi
            [ "${STUB_FINAL_FLAG:-}" = refused ] && { echo "the flag store did not answer in time"; exit 1; }
            echo "cluster flag enabled: upgrade_canary" ;;
          *"malachi.ring --show"*)
            n=$(( $(cat "$STUB_LOG.ring" 2>/dev/null || echo 0) + 1 ))
            echo "$n" > "$STUB_LOG.ring"
            count=5
            [ "$n" -gt 1 ] && [ -n "${STUB_RING_AFTER_ROLLBACK:-}" ] && count=$STUB_RING_AFTER_ROLLBACK
            echo "durable ring: version 3, $count vnodes (ring size 1024)" ;;
          *malachi.reshard*)
            [ "${STUB_SPLIT:-}" = refused ] && { echo "reshard refused: the ring is busy"; exit 1; }
            echo "resharded" ;;
          *canary_note*)
            if [ "$2" = malachi-cluster-3 ]; then
              case "${STUB_MIXED_CANARY:-}" in
                applied) reply=":ok" ;;
                old_leader) reply="{:error, {:unknown_command, {:canary_note, 3}, 3}}" ;;
                *) reply="{:error, {:unsupported_command, {:canary_note, 3}, ${STUB_EFFECTIVE:-4}, 3}}" ;;
              esac
            elif [ "${STUB_FINAL_CANARY:-}" = refused ]; then
              reply="{:error, {:unsupported_command, {:canary_note, 3}, ${STUB_EFFECTIVE:-4}, 3}}"
            else
              reply=":ok"
            fi
            # Read in the same VM as the command: the reply, then what the broker's cache holds right after.
            if [[ "$*" == *":sys, :get_state"* ]]; then
              echo "reply: $reply"
              echo "canary_note: ${STUB_CACHE_NOTE:-nil}"
            else
              echo "$reply"
            fi ;;
          *effective_machine_version*) echo "%{effective_machine_version: ${STUB_EFFECTIVE:-4}}" ;;
          *"sh -s"*)
            cat >/dev/null
            # Only an OLD node counts the canary cast as unknown; NEW node 3 understands it.
            count=0
            [ "$3" = malachi-cluster-1 ] && count=${STUB_UNKNOWN_CASTS:-7}
            echo "malachi_unexpected_messages_total{server=\"replication\",kind=\"cast\"} $count" ;;
          *malachi.format*) printf 'format=%s\nwritten_by=x\nrequires=x\n' "${STUB_FORMAT:-2}" ;;
        esac ;;
      logs)
        echo "log line of $2"
        if [ "$2" = malachi-cluster-3 ]; then
          case "${STUB_REFUSAL:-}" in
            flag) echo "REFUSING TO START (exit 78): the cluster flag upgrade_canary is on" ;;
            *) echo "REFUSING TO START (exit 78): the format marker /data/malachi_log/malachi.format records format 2, written by release x" ;;
          esac
        fi
        if [ "${STUB_CRASH:-}" = 1 ] && [ "$2" = malachi-cluster-2 ] && [ ! -f "$STUB_LOG.crashed" ]; then
          touch "$STUB_LOG.crashed"
          echo "** (FunctionClauseError) no function clause matching in Malachi.Cluster.ReplicationServer.handle_cast/2"
        fi
        # A crash that is not over an unknown message, in a log of the build named by STUB_CRASH (old or new).
        running=$(cat "$STUB_LOG.image.${2##*-}" 2>/dev/null)
        case "${STUB_CRASH:-}" in
          old_keyerror)
            if [ "$2" = malachi-cluster-2 ] && [ "${running##*:}" = old ] && [ ! -f "$STUB_LOG.crashed" ]; then
              touch "$STUB_LOG.crashed"
              echo "12:00:00.000 [error] GenServer Malachi.LogReplication terminating"
              echo "** (KeyError) key :records not found in: %{batch: []}"
            fi ;;
          old_timeout | new_timeout)
            if [ "$2" = malachi-cluster-2 ] && [ "${running##*:}" = "${STUB_CRASH%_timeout}" ] && [ ! -f "$STUB_LOG.crashed" ]; then
              touch "$STUB_LOG.crashed"
              echo "12:00:00.000 [error] GenServer Malachi.LogHealer terminating"
              echo "** (stop) exited in: GenServer.call(Malachi.LogBroker, :metadata, 5000)"
              echo "    ** (EXIT) time out"
            fi ;;
        esac ;;
      run)
        case "$*" in
          *"chaos_checker.exs produce"*)
            prev=""
            for a in "$@"; do
              if [ "$prev" = -v ] && [ "${a%:/chaos}" != "$a" ]; then
                echo c-1 > "${a%:/chaos}/acked.log"
                echo "${a%:/chaos}/acked.log" > "$acked_file"
              fi
              prev="$a"
            done ;;
          *"chaos_checker.exs verify"*)
            echo "acked=1 read=1 missing=0"
            echo "VERIFY OK: every acknowledged write survived" ;;
          *"chaos_checker.exs control-plane"*)
            if [ "${STUB_CONTROL_PLANE:-}" = diverged ]; then
              echo "CONTROL-PLANE MISMATCH group=malachi_log_vn_0 index=4 state differs: a=1 b=2 c=1 outliers=b"
              echo "CONTROL-PLANE DIVERGED projection=topics"
              exit 1
            fi
            echo "CONTROL-PLANE OK groups=12 projection=all" ;;
          *md5sum*)
            n=$(( $(cat "$STUB_LOG.md5" 2>/dev/null || echo 0) + 1 ))
            echo "$n" > "$STUB_LOG.md5"
            echo "aaa  ./malachi.format"
            if [ "${STUB_MD5_CHANGES:-}" = 1 ] && [ "$n" -gt 1 ]; then echo "ccc  ./seg.log"; else echo "bbb  ./seg.log"; fi ;;
          *"--host ${STUB_PRODUCE_FAILS_ON:-none} "*) echo '{"errors":5,"dropped":0,"records_per_s":0}' ;;
          *--scenario*) echo '{"errors":0,"dropped":0,"records_per_s":100}' ;;
          *) : ;;
        esac ;;
      *) : ;;
    esac
    exit 0
    """
  end
end
