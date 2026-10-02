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
        ring_known?: true,
        member_known?: true,
        ring_members: nil,
        segment_dirs: [],
        adopt?: false
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

    test "a ring store never started here refuses, sharded or not" do
      for sharded? <- [false, true] do
        assert DataDirGuard.decide(facts(ring_known?: false, sharded?: sharded?, segment_dirs: ["a-r0-s0"])) ==
                 {:refuse, {:unknown_segments, ["a-r0-s0"]}}
      end
    end

    # The vnodes of a sharded control plane are not the unsharded member: it is the ring store that says
    # this node has run one before.
    test "a sharded node whose ring store was started here keeps its segments" do
      assert DataDirGuard.decide(facts(sharded?: true, member_known?: false, segment_dirs: ["a-r0-s0"])) == :ok
    end

    test "whatever the number of nodes: a cluster renamed, or a node rejoining with a lost ra directory" do
      peers = [@self, :b@host, :c@host]

      assert DataDirGuard.decide(facts(member_known?: false, configured_nodes: peers, segment_dirs: ["a-r0-s0"])) ==
               {:refuse, {:unknown_segments, ["a-r0-s0"]}}

      assert DataDirGuard.decide(facts(ring_known?: false, configured_nodes: peers, segment_dirs: ["a-r0-s0"])) ==
               {:refuse, {:unknown_segments, ["a-r0-s0"]}}
    end

    test "the operator's adoption turns that refusal into an adoption" do
      assert DataDirGuard.decide(facts(member_known?: false, segment_dirs: ["b-r0-s0", "a-r0-s0"], adopt?: true)) ==
               {:adopt, ["a-r0-s0", "b-r0-s0"]}
    end

    test "nothing on disk is fine, even for a control plane formed now" do
      assert DataDirGuard.decide(facts(ring_known?: false, member_known?: false)) == :ok
      assert DataDirGuard.decide(facts(ring_known?: false, member_known?: false, adopt?: true)) == :ok
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

    test "a ring store never started here has no membership to ask" do
      assert DataDirGuard.decide(facts(ring_known?: false, ring_members: nil, configured_nodes: [@self, :b@host])) ==
               :ok
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
      opts = [ring_known?: true, halt_fun: halt_to_self()]
      assert Malachi.Application.ensure_data_dir_identity(unique_cluster(), [node()], dir, opts) == :ok

      make_segment_dir(dir, {{"orders", 0}, 0})
      refusal_output(fn -> Malachi.Application.ensure_data_dir_identity(unique_cluster(), [node()], dir, opts) end)
      exit_status = StartupRefusal.exit_status()
      assert_received {:halted, ^exit_status}
    end
  end

  describe "check/4" do
    test "a fresh member over an empty directory starts", %{tmp_dir: dir} do
      assert DataDirGuard.check(unique_cluster(), [node()], dir, ring_known?: true, halt_fun: halt_to_self()) == :ok
      refute_received {:halted, _}
    end

    test "a fresh member over segment directories refuses to start, saying why", %{tmp_dir: dir} do
      name = make_segment_dir(dir, {{"orders", 0}, 0})
      cluster = unique_cluster()
      opts = [ring_known?: true, halt_fun: halt_to_self(), adopt?: false]

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
      opts = [ring_known?: true, halt_fun: halt_to_self()]

      {log, _stderr} = refusal_output(fn -> DataDirGuard.check(unique_cluster(), [node()], dir, opts) end)

      listed = names |> Enum.sort() |> Enum.take(10)
      assert log =~ Enum.join(listed, ", ") <> " (+2)"
    end

    test "a sharded node whose ring store ran before keeps its segments; one formed now refuses", %{tmp_dir: dir} do
      make_segment_dir(dir, {{"orders", 0}, 0})
      known = [ring_known?: true, sharded?: true, halt_fun: halt_to_self()]
      assert DataDirGuard.check(unique_cluster(), [node()], dir, known) == :ok

      formed_now = [ring_known?: false, sharded?: true, halt_fun: halt_to_self()]
      refusal_output(fn -> DataDirGuard.check(unique_cluster(), [node()], dir, formed_now) end)

      exit_status = StartupRefusal.exit_status()
      assert_received {:halted, ^exit_status}
    end

    test "an adopted directory starts and logs what it adopted", %{tmp_dir: dir} do
      name = make_segment_dir(dir, {{"orders", 0}, 0})
      opts = [ring_known?: true, halt_fun: halt_to_self(), adopt?: true]

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

      opts = [ring_known?: true, halt_fun: halt_to_self()]
      capture_log(fn -> assert DataDirGuard.check(unique_cluster(), [node()], dir, opts) == {:adopt, [name]} end)
    end

    test "a member started here keeps its segments", %{tmp_dir: dir} do
      make_segment_dir(dir, {{"orders", 0}, 0})
      cluster = unique_cluster()
      {:ok, _} = MetadataServer.start(cluster, [node()])
      on_exit(fn -> MetadataServer.delete(cluster) end)

      assert DataDirGuard.check(cluster, [node()], dir, ring_known?: true, halt_fun: halt_to_self()) == :ok
      refute_received {:halted, _}
    end

    # The suite's own node runs a one-member ring store, as every single node does.
    test "a one-member ring store told it has a peer refuses to start", %{tmp_dir: dir} do
      opts = [ring_known?: true, halt_fun: halt_to_self()]

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

      opts = [ring_known?: true, members_deadline_ms: 10_000, members_fun: members_fun, halt_fun: halt_to_self()]
      assert DataDirGuard.check(unique_cluster(), [node(), :peer@nowhere], dir, opts) == :ok
      assert Agent.get(attempts, & &1) == 2
      refute_received {:halted, _}
    end

    test "a membership that never answers refuses once the deadline passes", %{tmp_dir: dir} do
      opts = [
        ring_known?: true,
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
      opts = [ring_known?: true, members_deadline_ms: 0, halt_fun: halt_to_self()]

      {log, _stderr} = refusal_output(fn -> DataDirGuard.check(cluster, [node(), :peer@nowhere], dir, opts) end)

      exit_status = StartupRefusal.exit_status()
      assert_received {:halted, ^exit_status}
      assert log =~ "MALACHI_LOG_RING_BOOT_TIMEOUT_MS"
    end
  end
end
