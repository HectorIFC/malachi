defmodule Malachi.Storage.DataDirGuardBootTest do
  @moduledoc """
  The application's own boot runs `Malachi.Storage.DataDirGuard` (#273), end to end: a peer node boots
  the whole application, with this suite's configuration, over a log directory holding a segment its
  control plane, formed now, would not know. The peer must halt (the refusal's `System.halt/1`) before
  anything serves, and the segment must still be there. A control boot over an empty directory starts.

  The guard's decisions are tested on their own in `Malachi.Storage.DataDirGuardTest`; this is what
  holds the boot to calling it, which no in-process test can do: the suite's application booted once,
  over a fresh directory, where the guard has nothing to say.

  async: false and tagged, like every test that starts peer nodes.
  """
  use ExUnit.Case, async: false

  @moduletag :multinode
  # Each test boots a whole application on a peer, which can take tens of seconds on a loaded runner.
  @moduletag timeout: 180_000

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.RaCluster
  alias Malachi.Cluster.RingMachine
  alias Malachi.Cluster.RingServer
  alias Malachi.Cluster.RingTopology
  alias Malachi.Storage.DataDirGuard
  alias Malachi.Storage.Layout
  alias Malachi.Test.Distribution
  alias Malachi.Test.PollingHelper
  alias Malachi.Test.TmpDir

  setup_all do
    :ok = Distribution.ensure_started()
  end

  # A peer configured exactly like this suite's node, with its own data directories, then the whole
  # application started on it. Returns the peer and what starting answered, or :halted when the peer went
  # down.
  defp boot_peer(log_dir, overrides \\ [], before_boot \\ fn _node -> :ok end) do
    name = Distribution.peer_name("guard_boot")
    ra_dir = TmpDir.path("malachi_guard_boot_ra_#{name}")
    on_exit(fn -> File.rm_rf!(ra_dir) end)
    boot_named(name, log_dir, ra_dir, overrides, before_boot)
  end

  # The same node name and the same directories each time it is called, which is what a restart is: ra
  # keeps a node's state under its name.
  defp boot_named(name, log_dir, ra_dir, overrides \\ [], before_boot \\ fn _node -> :ok end) do
    {peer, node} = start_named_peer(name)
    on_exit(fn -> Distribution.stop_peer(peer) end)

    env =
      Application.get_all_env(:malachi)
      |> Keyword.merge(log_data_dir: log_dir, ra_data_dir: ra_dir)
      |> Keyword.merge(overrides)

    :ok = :erpc.call(node, Application, :put_all_env, [[malachi: env]])
    :ok = before_boot.(node)

    try do
      {node, :erpc.call(node, Application, :ensure_all_started, [:malachi], 60_000)}
    catch
      :error, {:erpc, :noconnection} -> {node, :halted}
    end
  end

  # A name a stopped peer held can stay registered in epmd for a moment after the node is gone, and a
  # peer started under it then never comes up (`:peer.start_link/1` exits with a timeout rather than
  # answering an error), so the name is waited free first.
  defp start_named_peer(name) do
    PollingHelper.wait_until!(fn -> :erl_epmd.port_please(Atom.to_charlist(name), ~c"127.0.0.1") == :noport end)
    {:ok, peer, node} = :peer.start_link(%{name: name, host: ~c"127.0.0.1", longnames: true})
    :ok = :erpc.call(node, :code, :add_paths, [:code.get_path()])
    {peer, node}
  end

  defp segment_dir(log_dir) do
    path = Layout.segment_directory(log_dir, {{"orders", 0}, 0})
    File.mkdir_p!(path)
    path
  end

  test "a boot over a segment its control plane does not know halts before it serves, and keeps it" do
    log_dir = TmpDir.path("malachi_guard_boot_log")
    on_exit(fn -> File.rm_rf!(log_dir) end)
    segment = segment_dir(log_dir)

    assert {_node, :halted} = boot_peer(log_dir)
    assert File.dir?(segment)
  end

  # A sharded control plane has no unsharded member to be unknown: what refuses there is the ring store,
  # read before the boot starts it. Read after, it would always be known, and this boot would start.
  test "a sharded boot over a segment its ring store never knew halts too, and keeps it" do
    log_dir = TmpDir.path("malachi_guard_boot_sharded")
    on_exit(fn -> File.rm_rf!(log_dir) end)
    segment = segment_dir(log_dir)

    assert {_node, :halted} = boot_peer(log_dir, log_vnodes: 2)
    assert File.dir?(segment)
  end

  # A sharded ring that survived while the vnodes' history did not (a partly lost ra directory): on a node
  # configured alone nothing else keeps that history, so the segments are refused, not swept. The ring is
  # recorded, so no seed is due and only the vnode check stands between this boot and the sweep.
  test "a sharded single node whose ring store survived but whose vnodes did not halts, and keeps them" do
    log_dir = TmpDir.path("malachi_guard_boot_vnodes")
    on_exit(fn -> File.rm_rf!(log_dir) end)
    segment = segment_dir(log_dir)

    ring_only = fn node ->
      ra_dir = :erpc.call(node, Application, :get_env, [:malachi, :ra_data_dir])
      {:ok, _apps} = :erpc.call(node, Application, :ensure_all_started, [:ra])
      _ = :erpc.call(node, :ra, :start_in, [String.to_charlist(ra_dir)])

      {:ok, server_id} =
        :erpc.call(node, RaCluster, :start, [RingMachine, Malachi.LogRing, [node]])

      {:ok, ring} =
        :erpc.call(node, HashRing, :add_vnode, [HashRing.new(), :vn_lost, 0])

      topology = :erpc.call(node, RingTopology, :new, [ring, %{vn_lost: [node]}])
      :ok = :erpc.call(node, RingServer, :init, [server_id, topology])
    end

    assert {_node, :halted} = boot_peer(log_dir, [log_vnodes: 2], ring_only)
    assert File.dir?(segment)
  end

  # On a node with peers neither the member check nor the vnode check can tell a ring store formed now
  # from one resumed, so the ring check before the boot is the only refusal. Without it this boot would
  # form the ring store and wait for a peer that never comes, failing the start instead of refusing it.
  test "a sharded node with peers over a segment its ring store never knew halts before forming it" do
    log_dir = TmpDir.path("malachi_guard_boot_peers")
    on_exit(fn -> File.rm_rf!(log_dir) end)
    segment = segment_dir(log_dir)
    name = Distribution.peer_name("guard_peers")
    ra_dir = TmpDir.path("malachi_guard_boot_peers_ra_#{name}")
    on_exit(fn -> File.rm_rf!(ra_dir) end)
    absent = :"#{name}_absent@127.0.0.1"

    overrides = [log_vnodes: 2, log_nodes: [:"#{name}@127.0.0.1", absent], log_ring_boot_timeout_ms: 2_000]

    assert {_node, :halted} = boot_named(name, log_dir, ra_dir, overrides)
    assert File.dir?(segment)
  end

  # A refused start must leave nothing a second start would read as a control plane that knows the
  # segments: an automatic restart after a refusal is the normal case under a restart policy.
  test "a refused start is refused again on the next attempt, sharded or not" do
    for {label, overrides} <- [unsharded: [], sharded: [log_vnodes: 2]] do
      log_dir = TmpDir.path("malachi_guard_boot_retry_#{label}")
      on_exit(fn -> File.rm_rf!(log_dir) end)
      segment = segment_dir(log_dir)
      name = Distribution.peer_name("guard_retry")
      ra_dir = TmpDir.path("malachi_guard_boot_retry_ra_#{name}")
      on_exit(fn -> File.rm_rf!(ra_dir) end)

      assert {_node, :halted} = boot_named(name, log_dir, ra_dir, overrides), "#{label}: the first start"
      assert {_node, :halted} = boot_named(name, log_dir, ra_dir, overrides), "#{label}: the retry"
      assert File.dir?(segment)
    end
  end

  # The sharded seed outranks the environment once written, so a node that ran unsharded must refuse
  # before writing it: refused after, it could never start unsharded again, and on a cluster the sweep
  # would take the segments the unsharded metadata describes.
  test "a node that ran unsharded refuses to be seeded sharded, and still starts unsharded after" do
    log_dir = TmpDir.path("malachi_guard_boot_reshard")
    on_exit(fn -> File.rm_rf!(log_dir) end)
    name = Distribution.peer_name("guard_reshard")
    ra_dir = TmpDir.path("malachi_guard_boot_reshard_ra_#{name}")
    on_exit(fn -> File.rm_rf!(ra_dir) end)

    assert {node, {:ok, _apps}} = boot_named(name, log_dir, ra_dir)
    stop_gracefully(node)

    assert {_node, :halted} = boot_named(name, log_dir, ra_dir, log_vnodes: 2)
    assert {node, {:ok, _apps}} = boot_named(name, log_dir, ra_dir)
    assert :erpc.call(node, RingServer, :topology, [{Malachi.LogRing, node}]) == {:ok, :none}
  end

  # The ring store is one per ra directory and node name, whatever the cluster is called: a cluster
  # renamed over the same directories and asked for vnodes would seed the old cluster's ring with vnodes
  # that know none of its segments. The log directory records its control plane, so the rename is refused
  # before the ring store is touched, and the old name still starts. No segment is on disk: nothing but
  # that record tells the two names apart, which is the case of a cluster member that held no data.
  test "a cluster renamed over the same directories is not seeded sharded, and the old name starts after" do
    log_dir = TmpDir.path("malachi_guard_boot_rename")
    on_exit(fn -> File.rm_rf!(log_dir) end)
    name = Distribution.peer_name("guard_rename")
    ra_dir = TmpDir.path("malachi_guard_boot_rename_ra_#{name}")
    on_exit(fn -> File.rm_rf!(ra_dir) end)

    assert {node, {:ok, _apps}} = boot_named(name, log_dir, ra_dir, log_cluster: :guard_old)
    stop_gracefully(node)
    assert {_node, :halted} = boot_named(name, log_dir, ra_dir, log_cluster: :guard_new, log_vnodes: 2)

    assert {node, {:ok, _apps}} = boot_named(name, log_dir, ra_dir, log_cluster: :guard_old)
    assert :erpc.call(node, RingServer, :topology, [{Malachi.LogRing, node}]) == {:ok, :none}
  end

  # A directory from before the cluster marker existed records nothing to compare. A start under another
  # name is then refused by a later check, over the segments, and must not record that name on the way:
  # the operator's way back, the old name, has to still start.
  test "a renamed start refused over an unmarked directory records nothing, and the old name starts" do
    log_dir = TmpDir.path("malachi_guard_boot_unmarked")
    on_exit(fn -> File.rm_rf!(log_dir) end)
    name = Distribution.peer_name("guard_unmarked")
    ra_dir = TmpDir.path("malachi_guard_boot_unmarked_ra_#{name}")
    on_exit(fn -> File.rm_rf!(ra_dir) end)

    assert {node, {:ok, _apps}} = boot_named(name, log_dir, ra_dir, log_cluster: :guard_old)
    stop_gracefully(node)
    File.rm!(DataDirGuard.cluster_marker_path(log_dir))
    segment = segment_dir(log_dir)

    assert {_node, :halted} = boot_named(name, log_dir, ra_dir, log_cluster: :guard_new)
    assert File.dir?(segment)
    refute File.exists?(DataDirGuard.cluster_marker_path(log_dir))

    assert {_node, {:ok, _apps}} = boot_named(name, log_dir, ra_dir, log_cluster: :guard_old)
    assert DataDirGuard.recorded_cluster(log_dir) == "guard_old"
  end

  # A restart, not a crash: the application stops and ra closes its files.
  defp stop_gracefully(node) do
    true = Node.monitor(node, true)
    :ok = :erpc.call(node, :init, :stop, [])
    assert_receive {:nodedown, ^node}, 60_000
  end

  test "a boot over an empty log directory starts" do
    log_dir = TmpDir.path("malachi_guard_boot_empty")
    on_exit(fn -> File.rm_rf!(log_dir) end)

    assert {_node, {:ok, _apps}} = boot_peer(log_dir)
  end

  test "a sharded boot over an empty log directory starts" do
    log_dir = TmpDir.path("malachi_guard_boot_empty_sharded")
    on_exit(fn -> File.rm_rf!(log_dir) end)

    assert {_node, {:ok, _apps}} = boot_peer(log_dir, log_vnodes: 2)
  end

  # A single node asked for broker group commit with the default replication factor (3) does not get it,
  # and the boot is where it is told: the line goes through the application's own boot, not a direct call.
  test "a boot asked for group commit with a factor above 1 warns that it does not apply" do
    log_dir = TmpDir.path("malachi_guard_boot_gc")
    on_exit(fn -> File.rm_rf!(log_dir) end)
    log_file = TmpDir.path("malachi_guard_boot_gc_log")
    on_exit(fn -> File.rm_rf!(log_file) end)

    handler = %{config: %{file: String.to_charlist(log_file)}, level: :warning}

    before_boot = fn node ->
      :ok = :erpc.call(node, :logger, :add_handler, [:guard_boot_file, :logger_std_h, handler])
    end

    assert {node, {:ok, _apps}} = boot_peer(log_dir, [group_commit: true, log_replication_factor: 3], before_boot)
    # A file handler writes on its own schedule; make what the boot logged readable now.
    :ok = :erpc.call(node, :logger_std_h, :filesync, [:guard_boot_file])
    assert File.read!(log_file) =~ "MALACHI_LOG_REPLICATION_FACTOR=3"
  end
end
