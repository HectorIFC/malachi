defmodule Malachi.Cluster.MachineVersioningMultinodeTest do
  @moduledoc """
  The six production control-plane machines over real `ra` on three peers: each group reaches machine
  version 1 on every member, applies its commands, refuses an unknown one with the versioned reply,
  and comes back from a full-cluster restart with the same state and the same effective version.

  Lease and ring are the two whose pure modules have no catch-all clause, so before versioning the
  `{:machine_version, 0, 1}` that `ra` applies on the first version bump would have raised inside
  `apply/3` on every replica, and again on every replay.
  """
  use ExUnit.Case, async: false

  @moduletag :multinode
  @moduletag timeout: 180_000

  alias Malachi.Auth.AclMachine
  alias Malachi.Auth.LockoutMachine
  alias Malachi.Auth.UserMachine
  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.LeaseMachine
  alias Malachi.Cluster.MetadataMachine
  alias Malachi.Cluster.RaCluster
  alias Malachi.Cluster.RingMachine
  alias Malachi.Cluster.RingTopology
  alias Malachi.Test.RaPeers

  setup_all do
    RaPeers.ensure_distribution()
  end

  # {machine, a command it accepts before the restart, one after}
  defp stores do
    topology = %{RingTopology.new(HashRing.new(), %{}) | version: 1}

    [
      {MetadataMachine, {:create_topic, "before", 1}, {:create_topic, "after", 1}},
      {LeaseMachine, {:acquire_or_renew, :holder, 60_000}, {:acquire_or_renew, :holder, 60_000}},
      {RingMachine, {:init, topology}, {:advance, 1, 1, %{topology | version: 2}}},
      {UserMachine, {:put_user, "before", "hash", [:produce]}, {:put_user, "after", "hash", [:produce]}},
      {LockoutMachine, {:unlock_user, "before"}, {:unlock_user, "after"}},
      {AclMachine, {:grant, "before", :produce, {:literal, "t"}}, {:grant, "after", :produce, {:literal, "t"}}}
    ]
  end

  defp command(cluster, nodes, command) do
    RaPeers.eventually(fn ->
      Enum.find_value(nodes, fn node ->
        case RaCluster.command({cluster, node}, command) do
          {:ok, reply} -> {:ok, reply}
          {:error, _unreachable} -> nil
        end
      end)
    end)
  end

  defp accepted?({:ok, {:error, _reason}}), do: false
  defp accepted?({:ok, _reply}), do: true
  defp accepted?(_unreachable), do: false

  defp await_converged(clusters, nodes, expected_version) do
    assert RaPeers.eventually(fn ->
             Enum.all?(clusters, fn cluster ->
               Enum.all?(nodes, &(RaPeers.effective(&1, cluster) == expected_version)) and
                 nodes |> Enum.map(&RaPeers.local_state(&1, cluster)) |> Enum.uniq() |> length() == 1
             end)
           end)
  end

  test "every control-plane machine reaches version 1, refuses unknown commands and survives a full restart" do
    peers = for _ <- 1..3, do: RaPeers.start(nil)
    # Outlives the test process, so on_exit can still stop whatever handles the test replaced.
    {:ok, handles} = Agent.start(fn -> peers |> Enum.with_index() |> Map.new(fn {peer, index} -> {index, peer} end) end)

    on_exit(fn ->
      handles |> Agent.get(&Map.values/1) |> Enum.each(&RaPeers.stop/1)
      Agent.stop(handles)
    end)

    nodes = Enum.map(peers, & &1.node)

    groups =
      for {machine, before_command, after_command} <- stores() do
        cluster = :"mv_store_#{System.unique_integer([:positive])}"
        {:ok, _member} = RaCluster.start(machine, cluster, nodes)
        {cluster, before_command, after_command}
      end

    clusters = Enum.map(groups, &elem(&1, 0))

    # A group is born at version 0 and moves to 1 once its leader has heard every member advertise 1.
    await_converged(clusters, nodes, 1)

    for {cluster, before_command, _after} <- groups do
      assert accepted?(command(cluster, nodes, before_command)), "#{cluster} refused #{inspect(before_command)}"
      assert {:ok, {:error, {:unknown_command, {:bogus, 2}, 1}}} = command(cluster, nodes, {:bogus, 1})
    end

    await_converged(clusters, nodes, 1)
    before = Map.new(clusters, &{&1, RaPeers.local_state(hd(nodes), &1)})

    # Full-cluster restart: every member down before any comes back, so the logs replay from disk.
    Enum.each(peers, &RaPeers.stop/1)

    restarted =
      for {peer, index} <- Enum.with_index(peers) do
        restarted = RaPeers.restart(peer, nil, clusters)
        Agent.update(handles, &Map.put(&1, index, restarted))
        restarted
      end

    nodes = Enum.map(restarted, & &1.node)
    await_converged(clusters, nodes, 1)

    for cluster <- clusters, do: assert(RaPeers.local_state(hd(nodes), cluster) == before[cluster])

    for {cluster, _before, after_command} <- groups do
      assert accepted?(command(cluster, nodes, after_command)), "#{cluster} refused #{inspect(after_command)}"
    end

    await_converged(clusters, nodes, 1)
  end
end
