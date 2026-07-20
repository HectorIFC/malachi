defmodule Malachi.Cluster.SplitCoordinatorTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MembershipServer
  alias Malachi.Cluster.RingTopology
  alias Malachi.Cluster.SplitCoordinator
  alias Malachi.Cluster.VnodeSplit

  defp start_coordinator(leader?, membership) do
    {:ok, pid} = SplitCoordinator.start_link(membership: membership, leader?: leader?)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    pid
  end

  # a lone membership server (no gossip ticks); the split reads/publishes its topology
  defp start_membership(topology \\ nil) do
    name = :"sc_ms_#{System.unique_integer([:positive])}"
    {:ok, pid} = MembershipServer.start_link(name: name, peers: [], topology: topology, protocol_period: 3_600_000)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    name
  end

  test "refuses to split unless this node holds the lease" do
    coord = start_coordinator(fn -> false end, start_membership())
    assert SplitCoordinator.split(coord, :v1, 100, [node()]) == {:error, :not_leader}
  end

  test "as the lease holder, delegates to VnodeSplit. No baseline topology yields :no_topology" do
    coord = start_coordinator(fn -> true end, start_membership())
    assert SplitCoordinator.split(coord, :v1, 100, [node()]) == {:error, :no_topology}
  end

  test "reconcile is a no-op (leaves the topology untouched) when no split is pending" do
    {:ok, ring} = HashRing.add_vnode(HashRing.new(), :v0, 0)
    membership = start_membership(RingTopology.new(ring, %{v0: [node()]}))

    assert VnodeSplit.reconcile(membership, fn -> true end) == :ok
    topo = MembershipServer.topology(membership)
    assert topo.version == 0
    assert topo.pending == nil
  end

  test "reconcile refuses when this node does not hold the lease" do
    assert VnodeSplit.reconcile(start_membership(), fn -> false end) == {:error, :not_leader}
  end

  test "the coordinator reconciles on start and on cast, staying healthy when nothing is pending" do
    coord = start_coordinator(fn -> true end, start_membership())

    # init already cast a reconcile; another explicit cast is also a no-op with no pending split
    assert SplitCoordinator.reconcile(coord) == :ok
    # the coordinator survived both reconciles and still serves splits (mailbox-ordered after the cast)
    assert SplitCoordinator.split(coord, :v1, 100, [node()]) == {:error, :no_topology}
  end
end
