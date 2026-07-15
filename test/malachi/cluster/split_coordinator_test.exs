defmodule Malachi.Cluster.SplitCoordinatorTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.MembershipServer
  alias Malachi.Cluster.SplitCoordinator

  defp start_coordinator(leader?, membership) do
    {:ok, pid} = SplitCoordinator.start_link(membership: membership, leader?: leader?)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    pid
  end

  # a lone membership server (no gossip ticks); the split reads/publishes its topology
  defp start_membership do
    name = :"sc_ms_#{System.unique_integer([:positive])}"
    {:ok, pid} = MembershipServer.start_link(name: name, peers: [], protocol_period: 3_600_000)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    name
  end

  test "refuses to split unless this node holds the lease" do
    coord = start_coordinator(fn -> false end, start_membership())
    assert SplitCoordinator.split(coord, :v1, 100, [node()]) == {:error, :not_leader}
  end

  test "as the lease holder, delegates to VnodeSplit — no baseline topology yields :no_topology" do
    coord = start_coordinator(fn -> true end, start_membership())
    assert SplitCoordinator.split(coord, :v1, 100, [node()]) == {:error, :no_topology}
  end
end
