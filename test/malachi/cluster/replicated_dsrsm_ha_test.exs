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
end
