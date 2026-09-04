defmodule Malachi.Cluster.ReshardCoordinatorMultinodeTest do
  # End-to-end grow re-sharding over real Raft: a single-vnode cluster holding a topic (with committed
  # offsets) grows to N vnodes, one real split at a time through the SplitCoordinator, with the topic and its
  # offsets preserved on whichever vnode owns it under the grown ring. Tagged so it can be excluded where
  # multi-node networking is unavailable.
  use ExUnit.Case, async: false

  import Malachi.Test.TeardownHelper

  @moduletag :multinode

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MembershipServer
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.ReplicatedDSRSM
  alias Malachi.Cluster.ReshardCoordinator
  alias Malachi.Cluster.RingTopology
  alias Malachi.Cluster.SplitCoordinator
  alias Malachi.Metadata

  setup_all do
    _ = System.cmd("epmd", ["-daemon"])

    case :net_kernel.start([:"malachi_primary@127.0.0.1", :longnames]) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    {:ok, _} = Application.ensure_all_started(:ra)
    :ok
  end

  defp start_membership(topology) do
    name = :"reshard_ms_#{System.unique_integer([:positive])}"
    {:ok, pid} = MembershipServer.start_link(name: name, peers: [], topology: topology, protocol_period: 3_600_000)
    on_exit(fn -> stop_quietly(pid) end)
    name
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

  defp start_coordinators(membership) do
    {:ok, splits} = SplitCoordinator.start_link(membership: membership, leader?: fn -> true end)

    # The reshard names its vnodes from the ring token, so they cannot be known up front. Record each one as
    # it is created and register the cleanup *before* running, so a test that fails midway still deletes the
    # ra clusters it made. The agent is deliberately unlinked: `on_exit` runs after the test process (and
    # anything linked to it) is gone.
    {:ok, created} = Agent.start(fn -> [] end)

    on_exit(fn ->
      created |> Agent.get(& &1) |> Enum.each(&MetadataServer.delete/1)
      Agent.stop(created)
    end)

    split = fn vnode_id, token, nodes ->
      Agent.update(created, &[vnode_id | &1])
      SplitCoordinator.split(splits, vnode_id, token, nodes)
    end

    {:ok, reshard} =
      ReshardCoordinator.start_link(
        ring: fn -> MembershipServer.topology(membership).ring end,
        split: split,
        placement: fn _vnode_id -> [node()] end,
        leader?: fn -> true end
      )

    on_exit(fn ->
      Enum.each([splits, reshard], &stop_quietly/1)
    end)

    reshard
  end

  test "grows the ring to the target over ra, preserving the topic and its committed offsets" do
    source = :"reshard_src_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(source) end)

    # a single-vnode cluster holding a topic with a committed consumer-group offset
    {:ok, replicated} = ReplicatedDSRSM.add_vnode(ReplicatedDSRSM.new(), source, 0, [node()])
    {:ok, root} = commit(replicated, "orders", {:create_topic, "orders", 4})
    :ok = commit(replicated, "orders", {:commit_offset, "workers", "orders", %{root => 500}})

    membership = start_membership(RingTopology.new(replicated.ring, %{source => [node()]}))
    reshard = start_coordinators(membership)

    assert :ok = ReshardCoordinator.reshard(reshard, 3)

    topology = MembershipServer.topology(membership)
    # the ring grew to the target, and the two new vnodes were placed
    assert HashRing.size(topology.ring) == 3
    assert map_size(topology.placements) == 3
    assert topology.pending == nil

    # the topic survived the migrations: it resolves under the grown ring and its metadata, including the
    # committed offsets, which travel with the topic export: is intact on whichever vnode now owns it
    grown = %ReplicatedDSRSM{ring: topology.ring, vnodes: RingTopology.servers(topology)}
    assert {:ok, _owner} = ReplicatedDSRSM.vnode_for(grown, "orders")
    {:ok, metadata} = ReplicatedDSRSM.query(grown, "orders", &Function.identity/1)

    assert Metadata.get_topic(metadata, "orders").name == "orders"
    assert Metadata.committed_offsets(metadata, "workers", "orders") == %{root => 500}
  end

  test "re-issuing the same target after a partial grow resumes rather than restarting" do
    source = :"reshard_resume_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(source) end)

    {:ok, replicated} = ReplicatedDSRSM.add_vnode(ReplicatedDSRSM.new(), source, 0, [node()])
    {:ok, _root} = commit(replicated, "orders", {:create_topic, "orders", 4})

    membership = start_membership(RingTopology.new(replicated.ring, %{source => [node()]}))
    reshard = start_coordinators(membership)

    # a first reshard grows part of the way
    assert :ok = ReshardCoordinator.reshard(reshard, 2)
    assert HashRing.size(MembershipServer.topology(membership).ring) == 2

    # re-issuing the larger target continues from the current ring (and is a no-op once at the target)
    assert :ok = ReshardCoordinator.reshard(reshard, 4)

    topology = MembershipServer.topology(membership)
    assert HashRing.size(topology.ring) == 4
    assert :ok = ReshardCoordinator.reshard(reshard, 4)
    assert HashRing.size(MembershipServer.topology(membership).ring) == 4
  end
end
