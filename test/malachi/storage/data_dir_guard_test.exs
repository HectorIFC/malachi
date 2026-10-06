defmodule Malachi.Storage.DataDirGuardTest do
  # async: false: the `check/4` tests start real `ra` members, and ra is global and stateful.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO
  import ExUnit.CaptureLog

  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.RaResume
  alias Malachi.StartupRefusal
  alias Malachi.Storage.DataDirGuard
  alias Malachi.Storage.Layout

  @moduletag :tmp_dir

  @self :me@host

  # A node that has run before: its ring store and its unsharded member were started here, and it is
  # configured alone. Each test overrides what its case is about.
  defp facts(overrides) do
    Map.merge(
      %{
        self: @self,
        configured_nodes: [@self],
        sharded?: false,
        member_known?: true,
        missing_vnodes: [],
        ring_members: nil,
        segment_dirs: [],
        adopt?: false,
        adopted?: false
      },
      Map.new(overrides)
    )
  end

  # A directory name exactly as the storage layout writes it for `segment_id`.
  defp segment_dir_name(segment_id), do: Path.basename(Layout.segment_directory("/", segment_id))

  defp make_segment_dir(dir, segment_id) do
    name = segment_dir_name(segment_id)
    File.mkdir_p!(Path.join(dir, name))
    name
  end

  defp unique_cluster, do: :"guard_meta_#{System.unique_integer([:positive])}"

  defp halt_to_self do
    parent = self()
    fn status -> send(parent, {:halted, status}) end
  end

  defp refusal_output(fun) do
    with_io(:stderr, fn -> capture_log(fun) end)
  end

  describe "decide/1, segments nobody knows" do
    test "a member never started here, over segment directories, refuses and names them sorted" do
      assert DataDirGuard.decide(facts(member_known?: false, segment_dirs: ["b-r0-s0", "a-r0-s0"])) ==
               {:refuse, {:unknown_segments, ["a-r0-s0", "b-r0-s0"]}}
    end

    # The vnodes of a sharded control plane are not the unsharded member: on a node configured alone,
    # every vnode must have been started here, since no other replica keeps its history.
    test "a sharded node alone whose vnodes were all started here keeps its segments" do
      assert DataDirGuard.decide(facts(sharded?: true, member_known?: false, segment_dirs: ["a-r0-s0"])) == :ok
    end

    test "a sharded node alone with a vnode never started here refuses, or adopts" do
      lost = facts(sharded?: true, member_known?: false, missing_vnodes: [:vn_b], segment_dirs: ["a-r0-s0"])

      assert DataDirGuard.decide(lost) == {:refuse, {:unknown_segments, ["a-r0-s0"]}}
      assert DataDirGuard.decide(%{lost | adopt?: true}) == {:adopt, ["a-r0-s0"]}
    end

    test "an adoption already taken by the ring check is not taken twice" do
      assert DataDirGuard.decide(facts(member_known?: false, segment_dirs: ["a-r0-s0"], adopt?: true, adopted?: true)) ==
               :ok
    end

    test "whatever the number of nodes: a cluster renamed, or a node rejoining with a lost ra directory" do
      peers = [@self, :b@host, :c@host]

      assert DataDirGuard.decide(facts(member_known?: false, configured_nodes: peers, segment_dirs: ["a-r0-s0"])) ==
               {:refuse, {:unknown_segments, ["a-r0-s0"]}}
    end

    test "the operator's adoption turns that refusal into an adoption" do
      assert DataDirGuard.decide(facts(member_known?: false, segment_dirs: ["b-r0-s0", "a-r0-s0"], adopt?: true)) ==
               {:adopt, ["a-r0-s0", "b-r0-s0"]}
    end

    test "nothing on disk is fine, even for a control plane formed now" do
      assert DataDirGuard.decide(facts(member_known?: false)) == :ok

      assert DataDirGuard.decide(facts(sharded?: true, member_known?: false, missing_vnodes: [:vn_a], adopt?: true)) ==
               :ok
    end

    test "a control plane that ran here before keeps its segments" do
      assert DataDirGuard.decide(facts(segment_dirs: ["a-r0-s0"])) == :ok
    end
  end

  describe "decide/1, a single node told it has peers" do
    test "a one-member cluster configured with peers refuses, and adoption does not apply" do
      grown = facts(ring_members: [@self], configured_nodes: [:c@host, @self, :b@host, @self])

      assert DataDirGuard.decide(grown) == {:refuse, {:grow_unsupported, [:b@host, :c@host]}}
      assert DataDirGuard.decide(%{grown | adopt?: true}) == {:refuse, {:grow_unsupported, [:b@host, :c@host]}}

      assert DataDirGuard.decide(%{grown | segment_dirs: ["a-r0-s0"]}) ==
               {:refuse, {:grow_unsupported, [:b@host, :c@host]}}
    end

    test "a member that did not report its membership refuses rather than passes" do
      assert DataDirGuard.decide(facts(ring_members: :unknown, configured_nodes: [@self, :b@host])) ==
               {:refuse, :membership_unknown}
    end

    test "a member that already has its peers starts" do
      peers = [@self, :b@host, :c@host]
      assert DataDirGuard.decide(facts(ring_members: peers, configured_nodes: peers)) == :ok
    end

    test "a node listed twice is still a node alone" do
      assert DataDirGuard.decide(facts(ring_members: [@self], configured_nodes: [@self, @self])) == :ok

      assert DataDirGuard.decide(
               facts(member_known?: false, configured_nodes: [@self, @self], segment_dirs: ["a-r0-s0"])
             ) ==
               {:refuse, {:unknown_segments, ["a-r0-s0"]}}
    end

    # The ring store is a one-member cluster on a sharded single node too, and so are its vnodes.
    test "a sharded single node told it has peers refuses too" do
      sharded = facts(sharded?: true, member_known?: false, ring_members: [@self], configured_nodes: [@self, :b@host])
      assert DataDirGuard.decide(sharded) == {:refuse, {:grow_unsupported, [:b@host]}}
    end
  end

  describe "decide_ring/1, the cluster marker" do
    test "another control plane's name refuses before anything else, adoption or not" do
      for ring_known? <- [true, false], dirs <- [[], ["a-r0-s0"]], adopt? <- [true, false] do
        facts = %{ring_known?: ring_known?, segment_dirs: dirs, adopt?: adopt?, cluster: "new"}

        assert DataDirGuard.decide_ring(Map.put(facts, :recorded_cluster, "old")) ==
                 {:refuse, {:cluster_renamed, "old"}}

        assert DataDirGuard.decide_ring(Map.put(facts, :recorded_cluster, :unreadable)) ==
                 {:refuse, {:cluster_renamed, :unreadable}}
      end
    end

    test "the same name, or none recorded yet, goes on to the ring check" do
      for recorded <- ["new", nil] do
        facts = %{
          ring_known?: true,
          segment_dirs: ["a-r0-s0"],
          adopt?: false,
          cluster: "new",
          recorded_cluster: recorded
        }

        assert DataDirGuard.decide_ring(facts) == :ok
      end
    end
  end

  describe "decide_ring/1" do
    test "segments on a node that never started a ring store refuse, or adopt" do
      assert DataDirGuard.decide_ring(%{ring_known?: false, segment_dirs: ["b-r0-s0", "a-r0-s0"], adopt?: false}) ==
               {:refuse, {:unknown_segments, ["a-r0-s0", "b-r0-s0"]}}

      assert DataDirGuard.decide_ring(%{ring_known?: false, segment_dirs: ["a-r0-s0"], adopt?: true}) ==
               {:adopt, ["a-r0-s0"]}
    end

    test "a ring store that ran here before, or an empty directory, passes" do
      assert DataDirGuard.decide_ring(%{segment_dirs: ["a-r0-s0"], adopt?: false}) == :ok
      assert DataDirGuard.decide_ring(%{ring_known?: false, segment_dirs: [], adopt?: false}) == :ok
    end
  end

  describe "segment_dirs/1" do
    test "lists only directories the layout writes for segments", %{tmp_dir: dir} do
      readable = make_segment_dir(dir, {{"orders", 0}, 3})
      encoded = make_segment_dir(dir, {{"Ünïcode topic", 2}, 0})
      File.write!(Path.join(dir, "malachi.format"), "{}")
      File.write!(Path.join(dir, "malachi.incarnation"), "1")
      File.mkdir_p!(Path.join(dir, "shard_0"))
      File.mkdir_p!(Path.join(dir, "not a segment"))
      # A file whose name reads as a segment is not a segment directory.
      File.write!(Path.join(dir, segment_dir_name({{"orders", 9}, 9})), "")

      assert Enum.sort(DataDirGuard.segment_dirs(dir)) == Enum.sort([readable, encoded])
    end

    test "a directory that does not exist yet holds nothing", %{tmp_dir: dir} do
      assert DataDirGuard.segment_dirs(Path.join(dir, "missing")) == []
    end

    test "a path that cannot be listed raises rather than reading as empty", %{tmp_dir: dir} do
      file = Path.join(dir, "a_file")
      File.write!(file, "")
      assert_raise File.Error, fn -> DataDirGuard.segment_dirs(file) end
    end
  end

  describe "RaResume.registered?/2 and ring_known?/0" do
    test "false for a name never started here, true once started, and still true once stopped" do
      cluster = unique_cluster()
      server_id = {cluster, node()}
      refute RaResume.registered?(:default, server_id)

      {:ok, _} = MetadataServer.start(cluster, [node()])
      on_exit(fn -> MetadataServer.delete(cluster) end)
      assert RaResume.registered?(:default, server_id)

      :ok = :ra.stop_server(:default, server_id)
      assert RaResume.registered?(:default, server_id)
    end

    test "the suite's own node started its ring store at boot" do
      assert DataDirGuard.ring_known?()
    end
  end

  describe "Malachi.Application.ensure_data_dir_identity/4" do
    test "is the boot's gate over the guard", %{tmp_dir: dir} do
      opts = [halt_fun: halt_to_self()]
      assert Malachi.Application.ensure_data_dir_identity(unique_cluster(), [node()], dir, opts) == :ok

      make_segment_dir(dir, {{"orders", 0}, 0})
      refusal_output(fn -> Malachi.Application.ensure_data_dir_identity(unique_cluster(), [node()], dir, opts) end)
      exit_status = StartupRefusal.exit_status()
      assert_received {:halted, ^exit_status}
    end
  end

  describe "check_ring/4 and Malachi.Application.ensure_ring_identity/4" do
    test "segments on a node that never started a ring store refuse, before anything is formed", %{tmp_dir: dir} do
      cluster = unique_cluster()
      assert DataDirGuard.check_ring(cluster, dir, false, halt_fun: halt_to_self()) == :ok

      name = make_segment_dir(dir, {{"orders", 0}, 0})
      opts = [halt_fun: halt_to_self()]

      {log, _stderr} = refusal_output(fn -> Malachi.Application.ensure_ring_identity(cluster, dir, false, opts) end)

      exit_status = StartupRefusal.exit_status()
      assert_received {:halted, ^exit_status}
      assert log =~ name
      assert DataDirGuard.check_ring(cluster, dir, true, opts) == :ok
    end

    # Written only once nothing later can refuse the name: a start refused after the ring check must not
    # leave a name that never ran, or the real one would then be refused as a rename.
    test "the ring check records nothing; a passing seed or identity check records the control plane",
         %{tmp_dir: dir} do
      cluster = unique_cluster()
      assert DataDirGuard.check_ring(cluster, dir, false, halt_fun: halt_to_self()) == :ok
      assert DataDirGuard.recorded_cluster(dir) == nil

      seeded = Path.join(dir, "seeded")
      assert DataDirGuard.check_seed(cluster, seeded, halt_fun: halt_to_self()) == :ok
      assert DataDirGuard.recorded_cluster(seeded) == Atom.to_string(cluster)

      assert DataDirGuard.check(cluster, [node()], dir, halt_fun: halt_to_self()) == :ok
      assert File.read!(DataDirGuard.cluster_marker_path(dir)) == "cluster=#{cluster}\n"
      refute File.exists?(Path.join(dir, "malachi.cluster.tmp"))
    end

    test "a start refused after the ring check records nothing, so the real name still starts", %{tmp_dir: dir} do
      make_segment_dir(dir, {{"orders", 0}, 0})
      renamed = unique_cluster()
      assert DataDirGuard.check_ring(renamed, dir, true, halt_fun: halt_to_self()) == :ok

      refusal_output(fn -> DataDirGuard.check_seed(renamed, dir, halt_fun: halt_to_self()) end)
      refusal_output(fn -> DataDirGuard.check(renamed, [node()], dir, halt_fun: halt_to_self()) end)
      assert DataDirGuard.recorded_cluster(dir) == nil

      assert DataDirGuard.check_ring(unique_cluster(), dir, true, halt_fun: halt_to_self()) == :ok
    end

    test "an adoption records the control plane too", %{tmp_dir: dir} do
      cluster = unique_cluster()
      make_segment_dir(dir, {{"orders", 0}, 0})

      capture_log(fn -> DataDirGuard.check(cluster, [node()], dir, adopt?: true, halt_fun: halt_to_self()) end)
      assert DataDirGuard.recorded_cluster(dir) == Atom.to_string(cluster)
    end

    test "a recorded control plane is never rewritten", %{tmp_dir: dir} do
      cluster = unique_cluster()
      assert DataDirGuard.check(cluster, [node()], dir, halt_fun: halt_to_self()) == :ok
      assert DataDirGuard.check(unique_cluster(), [node()], dir, halt_fun: halt_to_self()) == :ok
      assert DataDirGuard.recorded_cluster(dir) == Atom.to_string(cluster)
    end

    # The ring store and every ra member are keyed by node name, not by cluster name: a renamed control
    # plane over the same directories would read the old one's state as its own.
    test "a log directory recorded for another control plane refuses, whatever the operator adopts", %{tmp_dir: dir} do
      old = unique_cluster()
      assert DataDirGuard.check(old, [node()], dir, halt_fun: halt_to_self()) == :ok

      new = unique_cluster()
      opts = [adopt?: true, halt_fun: halt_to_self()]
      {log, stderr} = refusal_output(fn -> DataDirGuard.check_ring(new, dir, true, opts) end)

      exit_status = StartupRefusal.exit_status()
      assert_received {:halted, ^exit_status}

      for output <- [log, stderr] do
        assert output =~ "MALACHI_LOG_CLUSTER back to #{old}"
        assert output =~ to_string(new)
      end

      assert DataDirGuard.recorded_cluster(dir) == Atom.to_string(old)
    end

    test "a cluster marker that does not parse refuses rather than passes", %{tmp_dir: dir} do
      for content <- ["cluster=a\ncluster=b\n", "cluster=a", "", "name=a\n"] do
        File.write!(DataDirGuard.cluster_marker_path(dir), content)
        assert DataDirGuard.recorded_cluster(dir) == :unreadable

        {log, _stderr} =
          refusal_output(fn -> DataDirGuard.check_ring(unique_cluster(), dir, true, halt_fun: halt_to_self()) end)

        exit_status = StartupRefusal.exit_status()
        assert_received {:halted, ^exit_status}
        assert log =~ "unreadable name"
      end
    end

    test "a cluster marker that cannot be read raises rather than reads as absent", %{tmp_dir: dir} do
      File.mkdir_p!(DataDirGuard.cluster_marker_path(dir))
      assert_raise File.Error, fn -> DataDirGuard.recorded_cluster(dir) end
    end

    # A marker that is not durable must not be trusted on the next start, so the start fails instead.
    test "a cluster marker whose directory cannot be fsynced fails the start", %{tmp_dir: dir} do
      opts = [sync_fun: fn _dir -> {:error, :eio} end, halt_fun: halt_to_self()]

      error = assert_raise File.Error, fn -> DataDirGuard.check(unique_cluster(), [node()], dir, opts) end
      assert error.reason == :eio
    end

    test "a cluster marker that cannot be written fails the start", %{tmp_dir: dir} do
      File.chmod!(dir, 0o500)
      on_exit(fn -> File.chmod!(dir, 0o700) end)

      # Root writes through the mode bits, so there the case cannot be made.
      if File.write(Path.join(dir, "probe"), "") == {:error, :eacces} do
        assert_raise File.Error, fn -> DataDirGuard.check(unique_cluster(), [node()], dir, halt_fun: halt_to_self()) end
      end
    end

    test "the operator's adoption is logged once, here", %{tmp_dir: dir} do
      name = make_segment_dir(dir, {{"orders", 0}, 0})
      opts = [adopt?: true, halt_fun: halt_to_self()]

      log =
        capture_log(fn -> assert DataDirGuard.check_ring(unique_cluster(), dir, false, opts) == {:adopt, [name]} end)

      assert log =~ name
      refute_received {:halted, _}
    end
  end

  describe "check/4" do
    test "a fresh member over an empty directory starts", %{tmp_dir: dir} do
      assert DataDirGuard.check(unique_cluster(), [node()], dir, halt_fun: halt_to_self()) == :ok
      refute_received {:halted, _}
    end

    test "a fresh member over segment directories refuses to start, saying why", %{tmp_dir: dir} do
      name = make_segment_dir(dir, {{"orders", 0}, 0})
      cluster = unique_cluster()
      opts = [halt_fun: halt_to_self(), adopt?: false]

      {log, stderr} = refusal_output(fn -> DataDirGuard.check(cluster, [node()], dir, opts) end)

      exit_status = StartupRefusal.exit_status()
      assert_received {:halted, ^exit_status}

      for output <- [log, stderr] do
        assert output =~ name
        assert output =~ "MALACHI_ADOPT_ORPHANED_LOG_DIR"
        assert output =~ to_string(cluster)
      end
    end

    test "a refusal over many segments names ten and counts the rest", %{tmp_dir: dir} do
      names = for seq <- 0..11, do: make_segment_dir(dir, {{"orders", 0}, seq})
      opts = [halt_fun: halt_to_self()]

      {log, _stderr} = refusal_output(fn -> DataDirGuard.check(unique_cluster(), [node()], dir, opts) end)

      listed = names |> Enum.sort() |> Enum.take(10)
      assert log =~ Enum.join(listed, ", ") <> " (+2)"
    end

    test "a sharded node alone with a vnode never started here refuses; with every vnode started, starts",
         %{tmp_dir: dir} do
      make_segment_dir(dir, {{"orders", 0}, 0})
      started = unique_cluster()
      {:ok, _} = MetadataServer.start(started, [node()])
      on_exit(fn -> MetadataServer.delete(started) end)

      known = [sharded?: true, vnodes: [{started, 0, [node()]}], halt_fun: halt_to_self()]
      assert DataDirGuard.check(unique_cluster(), [node()], dir, known) == :ok

      lost = [
        sharded?: true,
        vnodes: [{started, 0, [node()]}, {unique_cluster(), 1, [node()]}],
        halt_fun: halt_to_self()
      ]

      {log, _stderr} = refusal_output(fn -> DataDirGuard.check(unique_cluster(), [node()], dir, lost) end)

      exit_status = StartupRefusal.exit_status()
      assert_received {:halted, ^exit_status}
      assert log =~ "orders-r0-s0"
    end

    test "an adopted directory starts and logs what it adopted", %{tmp_dir: dir} do
      name = make_segment_dir(dir, {{"orders", 0}, 0})
      opts = [halt_fun: halt_to_self(), adopt?: true]

      log = capture_log(fn -> assert DataDirGuard.check(unique_cluster(), [node()], dir, opts) == {:adopt, [name]} end)

      refute_received {:halted, _}
      assert log =~ name
    end

    test "adoption defaults to the configured switch", %{tmp_dir: dir} do
      name = make_segment_dir(dir, {{"orders", 0}, 0})
      original = Application.fetch_env(:malachi, :adopt_orphaned_log_dir)
      Application.put_env(:malachi, :adopt_orphaned_log_dir, true)

      on_exit(fn ->
        case original do
          :error -> Application.delete_env(:malachi, :adopt_orphaned_log_dir)
          {:ok, value} -> Application.put_env(:malachi, :adopt_orphaned_log_dir, value)
        end
      end)

      opts = [halt_fun: halt_to_self()]
      capture_log(fn -> assert DataDirGuard.check(unique_cluster(), [node()], dir, opts) == {:adopt, [name]} end)
    end

    test "a member started here keeps its segments", %{tmp_dir: dir} do
      make_segment_dir(dir, {{"orders", 0}, 0})
      cluster = unique_cluster()
      {:ok, _} = MetadataServer.start(cluster, [node()])
      on_exit(fn -> MetadataServer.delete(cluster) end)

      assert DataDirGuard.check(cluster, [node()], dir, halt_fun: halt_to_self()) == :ok
      refute_received {:halted, _}
    end

    # The suite's own node runs a one-member ring store, as every single node does.
    test "a one-member ring store told it has a peer refuses to start", %{tmp_dir: dir} do
      opts = [halt_fun: halt_to_self()]

      {log, _stderr} =
        refusal_output(fn -> DataDirGuard.check(unique_cluster(), [node(), :peer@nowhere], dir, opts) end)

      exit_status = StartupRefusal.exit_status()
      assert_received {:halted, ^exit_status}
      assert log =~ "peer@nowhere"
    end

    # A member still replaying its log answers late: the check asks again until the deadline rather than
    # taking the first silence for an answer.
    test "a membership that answers after a failed attempt is taken", %{tmp_dir: dir} do
      {:ok, attempts} = Agent.start_link(fn -> 0 end)

      members_fun = fn _server_id, _timeout ->
        case Agent.get_and_update(attempts, &{&1, &1 + 1}) do
          0 -> {:timeout, :replaying}
          _ -> {:ok, [{Malachi.LogRing, node()}, {Malachi.LogRing, :peer@nowhere}], {Malachi.LogRing, node()}}
        end
      end

      opts = [members_deadline_ms: 10_000, members_fun: members_fun, halt_fun: halt_to_self()]
      assert DataDirGuard.check(unique_cluster(), [node(), :peer@nowhere], dir, opts) == :ok
      assert Agent.get(attempts, & &1) == 2
      refute_received {:halted, _}
    end

    test "a membership that never answers refuses once the deadline passes", %{tmp_dir: dir} do
      opts = [
        members_deadline_ms: 500,
        members_fun: fn _server_id, _timeout -> {:timeout, :replaying} end,
        halt_fun: halt_to_self()
      ]

      {log, _stderr} =
        refusal_output(fn -> DataDirGuard.check(unique_cluster(), [node(), :peer@nowhere], dir, opts) end)

      exit_status = StartupRefusal.exit_status()
      assert_received {:halted, ^exit_status}
      assert log =~ "MALACHI_LOG_RING_BOOT_TIMEOUT_MS"
    end

    test "a member that cannot report its membership in time refuses rather than passes", %{tmp_dir: dir} do
      cluster = unique_cluster()
      {:ok, _} = MetadataServer.start(cluster, [node()])
      on_exit(fn -> MetadataServer.delete(cluster) end)
      opts = [members_deadline_ms: 0, halt_fun: halt_to_self()]

      {log, _stderr} = refusal_output(fn -> DataDirGuard.check(cluster, [node(), :peer@nowhere], dir, opts) end)

      exit_status = StartupRefusal.exit_status()
      assert_received {:halted, ^exit_status}
      assert log =~ "MALACHI_LOG_RING_BOOT_TIMEOUT_MS"
    end

    # On a cluster the other replicas keep a vnode's history (a rebalance moves vnodes off a node), so a
    # vnode never started here is not a control plane formed now.
    test "a sharded node with peers keeps its segments with vnodes never started here", %{tmp_dir: dir} do
      make_segment_dir(dir, {{"orders", 0}, 0})
      nodes = [node(), :peer@nowhere]

      members_fun = fn _server_id, _timeout ->
        {:ok, Enum.map(nodes, &{Malachi.LogRing, &1}), {Malachi.LogRing, node()}}
      end

      opts = [
        sharded?: true,
        vnodes: [{unique_cluster(), 0, nodes}],
        members_fun: members_fun,
        halt_fun: halt_to_self()
      ]

      assert DataDirGuard.check(unique_cluster(), nodes, dir, opts) == :ok
      refute_received {:halted, _}
    end

    test "an adoption the ring check already took neither refuses nor is logged again", %{tmp_dir: dir} do
      name = make_segment_dir(dir, {{"orders", 0}, 0})
      opts = [adopted?: true, adopt?: true, halt_fun: halt_to_self()]

      log = capture_log(fn -> assert DataDirGuard.check(unique_cluster(), [node()], dir, opts) == :ok end)

      refute_received {:halted, _}
      refute log =~ name
    end

    # MALACHI_LOG_NODES may leave this node out (`Malachi.Cluster.RaCluster.member_node/1`): it holds no
    # ring store member to ask, and waiting the boot timeout for one would only refuse a healthy node.
    test "a node outside its own configured nodes is not asked for a membership", %{tmp_dir: dir} do
      opts = [members_fun: fn _server_id, _timeout -> flunk("asked for a membership") end, halt_fun: halt_to_self()]

      assert DataDirGuard.check(unique_cluster(), [:a@nowhere, :b@nowhere], dir, opts) == :ok
      refute_received {:halted, _}
    end
  end

  describe "decide/1, a switch to sharding" do
    # A ring another node seeded is not this node's to undo: whatever is on disk, and whatever the
    # operator would adopt, the unsharded metadata that ran here would never be read again.
    test "a sharded control plane over an unsharded member that ran here refuses, on disk or not" do
      for dirs <- [[], ["a-r0-s0"]], adopt? <- [false, true] do
        assert DataDirGuard.decide(facts(sharded?: true, member_known?: true, segment_dirs: dirs, adopt?: adopt?)) ==
                 {:refuse, :resharded}
      end

      assert DataDirGuard.decide(facts(sharded?: false, member_known?: true, segment_dirs: ["a-r0-s0"])) == :ok
    end

    test "check/4 refuses a sharded boot on a node whose unsharded member ran here", %{tmp_dir: dir} do
      cluster = unique_cluster()
      {:ok, _} = MetadataServer.start(cluster, [node()])
      on_exit(fn -> MetadataServer.delete(cluster) end)
      nodes = [node(), :peer@nowhere]

      members_fun = fn _server_id, _timeout ->
        {:ok, Enum.map(nodes, &{Malachi.LogRing, &1}), {Malachi.LogRing, node()}}
      end

      opts = [sharded?: true, vnodes: [], members_fun: members_fun, halt_fun: halt_to_self()]

      {log, _stderr} = refusal_output(fn -> DataDirGuard.check(cluster, nodes, dir, opts) end)

      exit_status = StartupRefusal.exit_status()
      assert_received {:halted, ^exit_status}
      assert log =~ "already recorded"
      assert log =~ "MALACHI_RA_DATA_DIR"
    end
  end

  describe "decide_seed/1 and check_seed/3" do
    defp seed_facts(overrides),
      do: Map.merge(%{member_known?: false, segment_dirs: [], adopt?: false, adopted?: false}, Map.new(overrides))

    test "a sharded seed over an unsharded member that ran here refuses, whatever the operator adopts" do
      for dirs <- [[], ["a-r0-s0"]], adopt? <- [false, true] do
        assert DataDirGuard.decide_seed(seed_facts(member_known?: true, segment_dirs: dirs, adopt?: adopt?)) ==
                 {:refuse, :reshard_unsupported}
      end
    end

    # A seed forms every vnode now, empty: no segment on disk can be one of theirs.
    test "a seed over segment directories refuses them, or adopts them once" do
      dirs = ["b-r0-s0", "a-r0-s0"]
      assert DataDirGuard.decide_seed(seed_facts(segment_dirs: dirs)) == {:refuse, {:unknown_segments, Enum.sort(dirs)}}
      assert DataDirGuard.decide_seed(seed_facts(segment_dirs: dirs, adopt?: true)) == {:adopt, Enum.sort(dirs)}
      assert DataDirGuard.decide_seed(seed_facts(segment_dirs: dirs, adopt?: true, adopted?: true)) == :ok
    end

    test "a fresh seed over an empty directory passes" do
      assert DataDirGuard.decide_seed(seed_facts([])) == :ok
    end

    test "refuses segments a seed could not know before writing it, and adopts them when told", %{tmp_dir: dir} do
      name = make_segment_dir(dir, {{"orders", 0}, 0})
      opts = [adopt?: false, halt_fun: halt_to_self()]

      {log, _stderr} = refusal_output(fn -> Malachi.Application.ensure_seed_identity(unique_cluster(), dir, opts) end)

      exit_status = StartupRefusal.exit_status()
      assert_received {:halted, ^exit_status}
      assert log =~ name

      adopt = [adopt?: true, halt_fun: halt_to_self()]
      log = capture_log(fn -> assert DataDirGuard.check_seed(unique_cluster(), dir, adopt) == {:adopt, [name]} end)
      assert log =~ name
      refute_received {:halted, _}
    end

    test "refuses before the seed is written, naming the way back", %{tmp_dir: dir} do
      cluster = unique_cluster()
      assert DataDirGuard.check_seed(cluster, dir, halt_fun: halt_to_self()) == :ok
      refute_received {:halted, _}

      {:ok, _} = MetadataServer.start(cluster, [node()])
      on_exit(fn -> MetadataServer.delete(cluster) end)

      {log, stderr} = refusal_output(fn -> DataDirGuard.check_seed(cluster, dir, halt_fun: halt_to_self()) end)

      exit_status = StartupRefusal.exit_status()
      assert_received {:halted, ^exit_status}

      for output <- [log, stderr] do
        assert output =~ to_string(cluster)
        assert output =~ "remove MALACHI_LOG_VNODES"
      end
    end
  end
end
