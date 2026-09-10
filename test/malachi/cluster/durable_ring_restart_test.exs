defmodule Malachi.Cluster.DurableRingRestartTest do
  @moduledoc """
  Issue #32, end to end over real Raft: a reshard must survive the cluster going away.

  The reshard runs through the real `SplitCoordinator` with the real `TopologyPublisher`, so the ring
  is written to the durable store exactly as it is in production. Then everything held in memory is
  thrown away (the membership server, which is where the gossiped topology lived) and the ring is
  resolved again from the store, with `MALACHI_LOG_VNODES` still describing the **pre-reshard** cluster.
  Before this store existed, that environment is what a restarted node believed, and every topic the
  reshard had moved was orphaned.

  What this does not do is restart an operating-system process: these `:multinode` tests run on the
  local node with a real `ra`, so what is proven here is the durable round trip, which is where the
  correction lives. A genuine full-cluster restart is the Docker chaos drill.
  """
  use ExUnit.Case, async: false

  import Malachi.Test.TeardownHelper

  @moduletag :multinode

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MembershipServer
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.ReplicatedDSRSM
  alias Malachi.Cluster.ReshardCoordinator
  alias Malachi.Cluster.RingBoot
  alias Malachi.Cluster.RingServer
  alias Malachi.Cluster.RingTopology
  alias Malachi.Cluster.SplitCoordinator
  alias Malachi.Cluster.TopologyPublisher
  alias Malachi.Metadata

  @topics ["orders", "payments", "shipments", "invoices", "refunds", "audits"]

  setup_all do
    _ = System.cmd("epmd", ["-daemon"])

    case :net_kernel.start([:"malachi_primary@127.0.0.1", :longnames]) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    {:ok, _} = Application.ensure_all_started(:ra)
    :ok
  end

  # ra may need a moment to elect after cluster start; retry the command until it lands.
  defp commit(replicated, topic, command, remaining_ms \\ 5_000) do
    case ReplicatedDSRSM.command(replicated, topic, command) do
      {:error, {:raft, _reason}} when remaining_ms > 0 ->
        Process.sleep(100) && commit(replicated, topic, command, remaining_ms - 100)

      reply ->
        reply
    end
  end

  defp start_membership(topology) do
    name = :"drr_ms_#{System.unique_integer([:positive])}"
    {:ok, pid} = MembershipServer.start_link(name: name, peers: [], topology: topology, protocol_period: 3_600_000)
    on_exit(fn -> stop_quietly(pid) end)
    {name, pid}
  end

  defp start_ring_store do
    name = :"drr_ring_#{System.unique_integer([:positive])}"
    {:ok, server_id} = RingServer.start(name)
    on_exit(fn -> RingServer.delete(name) end)
    server_id
  end

  # The vnodes MALACHI_LOG_VNODES would describe: evenly spaced over the ring, the geometry a reshard
  # does not preserve. This is deliberately the same shape `Malachi.Application.sharded_vnodes/2`
  # produces, since it is what a restarting node would otherwise fall back to.
  defp env_vnodes(prefix, count) do
    ring_size = Integer.pow(2, 32)
    for index <- 0..(count - 1), do: {:"#{prefix}_vn_#{index}", div(index * ring_size, count)}
  end

  defp seed_cluster(prefix, count) do
    vnodes = env_vnodes(prefix, count)
    on_exit(fn -> Enum.each(vnodes, fn {id, _token} -> MetadataServer.delete(id) end) end)

    replicated =
      Enum.reduce(vnodes, ReplicatedDSRSM.new(), fn {id, token}, acc ->
        {:ok, next} = ReplicatedDSRSM.add_vnode(acc, id, token, [node()])
        next
      end)

    Enum.each(@topics, fn topic -> {:ok, _root} = commit(replicated, topic, {:create_topic, topic, 4}) end)

    placements = Map.new(vnodes, fn {id, _token} -> {id, [node()]} end)
    {replicated, RingTopology.new(replicated.ring, placements)}
  end

  # The reshard stack, publishing durably exactly as production does.
  defp start_coordinators(membership, ring_server_id) do
    {:ok, splits} =
      SplitCoordinator.start_link(
        membership: membership,
        publish: TopologyPublisher.seam(ring_server_id, membership),
        lease: fn -> {:ok, 1} end
      )

    # The reshard names its vnodes from the ring token, so they cannot be known up front; record each
    # one as it is created so a test that fails midway still deletes the ra clusters it made.
    {:ok, created} = Agent.start(fn -> [] end)

    on_exit(fn ->
      created |> Agent.get(& &1) |> Enum.each(&MetadataServer.delete/1)
      Agent.stop(created)
    end)

    {:ok, reshard} =
      ReshardCoordinator.start_link(
        ring: fn -> MembershipServer.topology(membership).ring end,
        split: fn vnode_id, token, nodes ->
          Agent.update(created, &[vnode_id | &1])
          SplitCoordinator.split(splits, vnode_id, token, nodes)
        end,
        placement: fn _vnode_id -> [node()] end,
        leader?: fn -> true end
      )

    on_exit(fn -> Enum.each([splits, reshard], &stop_quietly/1) end)
    reshard
  end

  # Everything a restart destroys: the gossiped topology lives only in the membership server.
  defp stop_the_cluster(membership_pid), do: stop_quietly(membership_pid)

  # What a booting node does: read the durable store, then apply the precedence rule against whatever
  # the environment happens to say. This is `Malachi.Application.boot_topology/2` without the
  # supervision tree around it.
  defp boot(ring_server_id, env_topology) do
    ring_server_id
    |> then(fn id -> RingBoot.read_until(fn -> RingServer.topology(id) end, timeout_ms: 5_000) end)
    |> RingBoot.resolve(env_topology)
  end

  defp assert_every_topic_resolves(topology) do
    routing = %ReplicatedDSRSM{ring: topology.ring, vnodes: RingTopology.servers(topology)}

    Enum.each(@topics, fn topic ->
      assert {:ok, owner} = ReplicatedDSRSM.vnode_for(routing, topic)

      assert {:ok, metadata} = ReplicatedDSRSM.query(routing, topic, &Function.identity/1)

      assert Metadata.get_topic(metadata, topic) != nil,
             "#{topic} routes to #{inspect(owner)}, which does not hold its metadata: it was orphaned"
    end)
  end

  test "a reshard survives losing every node, and the stale environment does not win" do
    prefix = :"drr_#{System.unique_integer([:positive])}"
    {_replicated, seed} = seed_cluster(prefix, 4)
    ring_server_id = start_ring_store()

    # first boot of a fresh cluster: the store affirms it holds nothing, so the environment seeds it
    assert boot(ring_server_id, seed) == {:seed, seed}
    assert {:ok, ^seed} = RingBoot.confirm_seed(seed, &RingServer.init(ring_server_id, &1))

    {membership, membership_pid} = start_membership(seed)
    reshard = start_coordinators(membership, ring_server_id)

    assert :ok = ReshardCoordinator.reshard(reshard, 6)
    grown = MembershipServer.topology(membership)
    assert HashRing.size(grown.ring) == 6
    assert_every_topic_resolves(grown)

    # every node goes away: the gossiped ring is gone with them
    stop_the_cluster(membership_pid)

    # the cluster comes back with MALACHI_LOG_VNODES still describing the pre-reshard 4-vnode cluster
    assert {:durable, restored} = boot(ring_server_id, seed)

    assert HashRing.size(restored.ring) == 6,
           "the durable ring must win: the environment describes a cluster that no longer exists"

    assert HashRing.vnode_ids(restored.ring) |> Enum.sort() == HashRing.vnode_ids(grown.ring) |> Enum.sort()
    assert restored.placements == grown.placements
    assert restored.version == grown.version
    assert restored.pending == nil

    # the point of the whole issue: nothing was orphaned by the restart
    assert_every_topic_resolves(restored)
  end

  test "regression: a ring grown past MALACHI_LOG_VNODES is not reseeded to the environment's geometry" do
    prefix = :"drr_regression_#{System.unique_integer([:positive])}"
    {_replicated, seed} = seed_cluster(prefix, 4)
    ring_server_id = start_ring_store()
    {:ok, ^seed} = RingBoot.confirm_seed(seed, &RingServer.init(ring_server_id, &1))

    {membership, membership_pid} = start_membership(seed)
    reshard = start_coordinators(membership, ring_server_id)
    assert :ok = ReshardCoordinator.reshard(reshard, 6)

    grown = MembershipServer.topology(membership)
    split_born = HashRing.vnode_ids(grown.ring) -- HashRing.vnode_ids(seed.ring)
    assert length(split_born) == 2, "the reshard must have created vnodes the environment cannot name"

    # a topic that a split actually moved, so reseeding from the environment would strand it
    grown_routing = %ReplicatedDSRSM{ring: grown.ring, vnodes: RingTopology.servers(grown)}

    migrated =
      Enum.find(@topics, fn topic ->
        case ReplicatedDSRSM.vnode_for(grown_routing, topic) do
          {:ok, owner} -> owner in split_born
          _unroutable -> false
        end
      end)

    assert migrated, "expected at least one topic to have moved to a split-created vnode"

    stop_the_cluster(membership_pid)
    assert {:durable, restored} = boot(ring_server_id, seed)

    # the split-created vnodes are still on the ring; the environment's even geometry did not replace it
    assert Enum.all?(split_born, &(&1 in HashRing.vnode_ids(restored.ring)))

    routing = %ReplicatedDSRSM{ring: restored.ring, vnodes: RingTopology.servers(restored)}
    assert {:ok, owner} = ReplicatedDSRSM.vnode_for(routing, migrated)
    assert owner in split_born

    {:ok, metadata} = ReplicatedDSRSM.query(routing, migrated, &Function.identity/1)
    assert Metadata.get_topic(metadata, migrated).name == migrated
  end

  test "a split interrupted by the whole cluster stopping is still pending when it comes back" do
    prefix = :"drr_pending_#{System.unique_integer([:positive])}"
    {_replicated, seed} = seed_cluster(prefix, 2)
    ring_server_id = start_ring_store()
    {:ok, ^seed} = RingBoot.confirm_seed(seed, &RingServer.init(ring_server_id, &1))

    # a coordinator that recorded its intent and then died: the intent is written durably first, so it
    # is on disk even though nothing was migrated
    pending = RingTopology.begin_split(seed, :"#{prefix}_new", 500, [node()])
    assert TopologyPublisher.publish(ring_server_id, spawn_dummy_membership(), pending, seed.version, 1) == :ok

    assert {:durable, restored} = boot(ring_server_id, seed)

    assert restored.pending.new_vnode == :"#{prefix}_new",
           "a split interrupted by a full stop must still be there to be carried to completion"
  end

  defp spawn_dummy_membership do
    name = :"drr_dummy_ms_#{System.unique_integer([:positive])}"
    {:ok, pid} = MembershipServer.start_link(name: name, peers: [], protocol_period: 3_600_000)
    on_exit(fn -> stop_quietly(pid) end)
    name
  end
end
