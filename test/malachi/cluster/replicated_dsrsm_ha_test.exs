defmodule Malachi.Cluster.ReplicatedDSRSMHaTest do
  # Real multi-node Raft for a sharded control plane: spins up peer BEAM nodes, forms a
  # ReplicatedDSRSM whose vnodes are each replicated across every node (HA per vnode, D-b-2), and
  # verifies a vnode still commits after losing a member. Tagged so it can be excluded where
  # multi-node networking is unavailable.
  use ExUnit.Case, async: false

  @moduletag :multinode

  alias Malachi.Cluster.DSRSM
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.ReplicatedDSRSM
  alias Malachi.Metadata

  setup_all do
    _ = System.cmd("epmd", ["-daemon"])

    case :net_kernel.start([:"malachi_primary@127.0.0.1", :longnames]) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    {:ok, _} = Application.ensure_all_started(:ra)
    # the local node is itself a cluster member here, so ra must run locally too
    _ = :ra.start_in(~c"#{System.tmp_dir!()}/malachi_ra_rdsrsm_local_#{System.unique_integer([:positive])}")
    :ok
  end

  defp start_peer do
    name = :"malachi_peer_#{System.unique_integer([:positive])}"
    {:ok, peer, node} = :peer.start_link(%{name: name, host: ~c"127.0.0.1", longnames: true})
    on_exit(fn -> try_stop(peer) end)

    :ok = :erpc.call(node, :code, :add_paths, [:code.get_path()])
    {:ok, _} = :erpc.call(node, :application, :ensure_all_started, [:ra])
    data_dir = ~c"#{System.tmp_dir!()}/malachi_ra_rdsrsm_#{name}_#{System.unique_integer([:positive])}"
    {:ok, _} = :erpc.call(node, :ra, :start_in, [data_dir])

    {peer, node}
  end

  defp try_stop(peer) do
    :peer.stop(peer)
  catch
    _kind, _reason -> :ok
  end

  # After an abrupt member loss, ra may need a moment to re-elect; retry until the command lands.
  defp commit(replicated, topic, command, remaining_ms \\ 5_000) do
    case ReplicatedDSRSM.command(replicated, topic, command) do
      {:error, {:raft, _reason}} when remaining_ms > 0 ->
        Process.sleep(100) && commit(replicated, topic, command, remaining_ms - 100)

      reply ->
        reply
    end
  end

  test "each vnode is replicated over the node set and survives losing a member" do
    peers = for _ <- 1..2, do: start_peer()
    peer_nodes = Enum.map(peers, &elem(&1, 1))
    nodes = [node() | peer_nodes]
    peer_by_node = Map.new(peers, fn {peer, node} -> {node, peer} end)

    unique = System.unique_integer([:positive])
    ring_size = Integer.pow(2, 32)
    vnodes = [{:"rd_ha_a_#{unique}", 0}, {:"rd_ha_b_#{unique}", div(ring_size, 2)}]
    on_exit(fn -> Enum.each(vnodes, fn {vnode, _token} -> MetadataServer.delete(vnode) end) end)

    replicated =
      Enum.reduce(vnodes, ReplicatedDSRSM.new(), fn {vnode_id, token}, replicated ->
        {:ok, replicated} = ReplicatedDSRSM.add_vnode(replicated, vnode_id, token, nodes)
        replicated
      end)

    # create enough topics that both vnodes own at least one, and confirm they sharded across vnodes
    names = for i <- 0..9, do: "t#{i}"
    for name <- names, do: assert({:ok, _root} = ReplicatedDSRSM.command(replicated, name, {:create_topic, name, 4}))
    owners = Enum.map(names, fn name -> elem(ReplicatedDSRSM.vnode_for(replicated, name), 1) end)
    assert length(Enum.uniq(owners)) == 2, "expected topics to shard across both vnodes"

    # pick a topic and the vnode that owns it; find that vnode's Raft leader
    victim_topic = "t0"
    {:ok, victim_vnode} = ReplicatedDSRSM.vnode_for(replicated, victim_topic)
    {:ok, _members, {^victim_vnode, leader_node}} = :ra.members(ReplicatedDSRSM.server_for(replicated, victim_vnode))

    # kill one member of that vnode: its leader if the leader is a peer (exercising failover), else a
    # peer follower. Never the local node — it is running the test. Quorum (2/3) is kept either way.
    casualty = if leader_node in peer_nodes, do: leader_node, else: hd(peer_nodes)
    :ok = try_stop(Map.fetch!(peer_by_node, casualty))

    # the owning vnode still commits (its cluster kept quorum / re-elected), and its earlier metadata
    # is intact — while a topic on the other vnode is unaffected too
    assert {:ok, _root} = commit(replicated, victim_topic, {:create_topic, "#{victim_topic}_after", 4})
    {:ok, cache} = ReplicatedDSRSM.snapshot(replicated)
    assert DSRSM.get_topic(cache, victim_topic).name == victim_topic
    assert DSRSM.get_topic(cache, "#{victim_topic}_after").name == "#{victim_topic}_after"

    surviving = Enum.find(names, fn name -> elem(ReplicatedDSRSM.vnode_for(replicated, name), 1) != victim_vnode end)
    assert DSRSM.get_topic(cache, surviving).name == surviving
  end

  test "routes commands and queries cross-node to vnodes placed on disjoint node subsets" do
    peers = for _ <- 1..3, do: start_peer()
    [n0, n1, n2] = Enum.map(peers, &elem(&1, 1))
    # the test node hosts no replica of either vnode, so every command/query must go cross-node
    refute node() in [n0, n1, n2]

    unique = System.unique_integer([:positive])
    vnode_a = :"rd_x_a_#{unique}"
    vnode_b = :"rd_x_b_#{unique}"
    placements = %{vnode_a => [n0, n1], vnode_b => [n1, n2]}
    on_exit(fn -> Enum.each([vnode_a, vnode_b], &MetadataServer.delete/1) end)

    {:ok, replicated} = ReplicatedDSRSM.add_vnode(ReplicatedDSRSM.new(), vnode_a, 0, placements[vnode_a])
    {:ok, replicated} = ReplicatedDSRSM.add_vnode(replicated, vnode_b, div(Integer.pow(2, 32), 2), placements[vnode_b])

    # the stored server for each vnode addresses one of its placement nodes, never the local node
    for {vnode, nodes} <- placements do
      {^vnode, member} = ReplicatedDSRSM.server_for(replicated, vnode)
      assert member in nodes
      refute member == node()
    end

    # commit topics through the DS-RSM (routed to the owning vnode's cluster on remote nodes)
    names = for i <- 0..9, do: "x#{i}"
    for name <- names, do: assert({:ok, _root} = commit(replicated, name, {:create_topic, name, 4}))

    # both vnodes actually own some topics (sharded), and each reads back cross-node. The query fun is
    # a named stdlib capture (the remote leader has it loaded) with get_topic applied locally.
    owners = Enum.map(names, fn name -> elem(ReplicatedDSRSM.vnode_for(replicated, name), 1) end)
    assert owners |> Enum.uniq() |> Enum.sort() == Enum.sort([vnode_a, vnode_b])

    for name <- names do
      {:ok, metadata} = ReplicatedDSRSM.query(replicated, name, &Function.identity/1)
      assert Metadata.get_topic(metadata, name).name == name
    end

    # snapshot materializes every vnode's metadata into one local cache, queried cross-node
    {:ok, cache} = ReplicatedDSRSM.snapshot(replicated)
    for name <- names, do: assert(DSRSM.get_topic(cache, name).name == name)
  end

  test "distributed bootstrap: the orchestrator starts a vnode, a non-orchestrator only routes to it" do
    peers = for _ <- 1..2, do: start_peer()
    [n0, n1] = Enum.map(peers, &elem(&1, 1))
    placement = [n0, n1]
    # the test node hosts no replica, so both views must reach the cluster cross-node
    refute node() in placement

    unique = System.unique_integer([:positive])
    vnode = :"rd_boot_#{unique}"
    on_exit(fn -> MetadataServer.delete(vnode) end)

    # orchestrator: starts the vnode's ra cluster across the placement and writes a topic
    {:ok, orchestrator} = ReplicatedDSRSM.add_vnode(ReplicatedDSRSM.new(), vnode, 0, placement)
    assert {:ok, _root} = commit(orchestrator, "events", {:create_topic, "events", 4})

    # non-orchestrator: only routes to a placement member (route_vnode) — it never calls start
    {:ok, router} = ReplicatedDSRSM.route_vnode(ReplicatedDSRSM.new(), vnode, 0, {vnode, hd(placement)})

    # the router reaches the same remote cluster: it reads the orchestrator's write and commits its own
    {:ok, metadata} = ReplicatedDSRSM.query(router, "events", &Function.identity/1)
    assert Metadata.get_topic(metadata, "events").name == "events"
    assert {:ok, _root2} = commit(router, "events", {:create_topic, "events_via_router", 4})

    {:ok, cache} = ReplicatedDSRSM.snapshot(router)
    assert DSRSM.get_topic(cache, "events").name == "events"
    assert DSRSM.get_topic(cache, "events_via_router").name == "events_via_router"
  end

  test "split_vnode grows the ring and migrates a displaced topic (with its offsets) to the new vnode" do
    unique = System.unique_integer([:positive])
    source = :"rd_split_src_#{unique}"
    dest = :"rd_split_dst_#{unique}"
    on_exit(fn -> Enum.each([source, dest], &MetadataServer.delete/1) end)

    {:ok, replicated} = ReplicatedDSRSM.add_vnode(ReplicatedDSRSM.new(), source, 0, [node()])

    {:ok, root} = commit(replicated, "orders", {:create_topic, "orders", 4})
    :ok = commit(replicated, "orders", {:commit_offset, "workers", "orders", %{root => 500}})

    # place the new vnode exactly at "orders"'s ring position, so the split displaces "orders" to it
    token = :erlang.phash2("orders", Integer.pow(2, 32))
    assert token > 0, "test setup expects a non-zero hash position"

    {:ok, split} = ReplicatedDSRSM.split_vnode(replicated, dest, token, [node()])

    # "orders" now routes to the new vnode, and its topic + committed offsets survive the migration,
    # read back from the new vnode's own raft group
    assert ReplicatedDSRSM.vnode_for(split, "orders") == {:ok, dest}
    {:ok, dest_meta} = ReplicatedDSRSM.query(split, "orders", &Function.identity/1)
    assert Metadata.get_topic(dest_meta, "orders").name == "orders"
    assert Metadata.committed_offsets(dest_meta, "workers", "orders") == %{root => 500}

    # the source vnode no longer holds it (copy-first extract removed it after the insert)
    {:ok, source_meta} = MetadataServer.query(ReplicatedDSRSM.server_for(split, source), &Function.identity/1)
    assert Metadata.get_topic(source_meta, "orders") == nil

    # the migrated topic is writable on the new vnode: the migration fence was lifted by the copy-first
    # extract and never applied to the destination (a fenced topic would answer {:error, :migrating})
    assert :ok = commit(split, "orders", {:commit_offset, "readers", "orders", %{root => 700}})
  end

  test "a split that fails to start the new vnode leaves the source untouched (no fence applied)" do
    unique = System.unique_integer([:positive])
    source = :"rd_nostart_src_#{unique}"
    dest = :"rd_nostart_dst_#{unique}"
    on_exit(fn -> Enum.each([source, dest], &MetadataServer.delete/1) end)

    {:ok, replicated} = ReplicatedDSRSM.add_vnode(ReplicatedDSRSM.new(), source, 0, [node()])
    {:ok, root} = commit(replicated, "orders", {:create_topic, "orders", 4})
    :ok = commit(replicated, "orders", {:commit_offset, "workers", "orders", %{root => 500}})

    # the new vnode's ra cluster can't form on an unreachable node, so split_vnode fails *before* it fences
    # or migrates anything — the source is never mutated
    token = :erlang.phash2("orders", Integer.pow(2, 32))
    assert {:error, _reason} = ReplicatedDSRSM.split_vnode(replicated, dest, token, [:"nonexistent@127.0.0.1"])

    assert ReplicatedDSRSM.vnode_for(replicated, "orders") == {:ok, source}
    {:ok, meta} = ReplicatedDSRSM.query(replicated, "orders", &Function.identity/1)
    assert Metadata.committed_offsets(meta, "workers", "orders") == %{root => 500}
    assert meta.migrating == %{}
    assert commit(replicated, "orders", {:commit_offset, "workers", "orders", %{root => 900}}) == :ok
  end

  test "a split that fails mid-migration rolls back: the migrated topic is restored to its source, un-fenced" do
    unique = System.unique_integer([:positive])
    # ids chosen so the healthy source sorts *before* the down one: migrate_displaced walks sources in id
    # order, migrating "orders" to the new vnode from `source`, then fails on `down` -> triggers rollback
    source = :"rd_rb_a_src_#{unique}"
    down = :"rd_rb_z_down_#{unique}"
    dest = :"rd_rb_dst_#{unique}"
    on_exit(fn -> Enum.each([source, dest], &MetadataServer.delete/1) end)

    {:ok, replicated} = ReplicatedDSRSM.add_vnode(ReplicatedDSRSM.new(), source, 0, [node()])
    {:ok, root} = commit(replicated, "orders", {:create_topic, "orders", 4})
    :ok = commit(replicated, "orders", {:commit_offset, "workers", "orders", %{root => 500}})

    # inject a second source vnode whose ra cluster is down (server on an unreachable node). It is not in
    # the ring — it exists only to make the migration fail *after* `source` has already handed "orders" off
    # to the new vnode, so the rollback's move-back path (new -> source) actually runs.
    broken = %{replicated | vnodes: Map.put(replicated.vnodes, down, {down, :"nonexistent@127.0.0.1"})}

    token = :erlang.phash2("orders", Integer.pow(2, 32))
    assert {:error, _reason} = ReplicatedDSRSM.split_vnode(broken, dest, token, [node()])

    # rollback moved "orders" back to its source (old ring unchanged), offsets intact, no stuck fence, and
    # it is writable again (a fenced topic would answer {:error, :migrating}, which commit/3 would surface)
    assert ReplicatedDSRSM.vnode_for(replicated, "orders") == {:ok, source}
    {:ok, meta} = ReplicatedDSRSM.query(replicated, "orders", &Function.identity/1)
    assert Metadata.get_topic(meta, "orders").name == "orders"
    assert Metadata.committed_offsets(meta, "workers", "orders") == %{root => 500}
    assert meta.migrating == %{}
    assert commit(replicated, "orders", {:commit_offset, "workers", "orders", %{root => 900}}) == :ok

    # and the new vnode was emptied by the move-back — nothing orphaned there
    {:ok, dest_meta} = MetadataServer.query({dest, node()}, &Function.identity/1)
    assert Metadata.get_topic(dest_meta, "orders") == nil
  end
end
