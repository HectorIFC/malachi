defmodule Malachi.Cluster.MachineVersioningMixedTest do
  @moduledoc """
  A Raft group whose members run different code, over real `ra` on peer nodes.

  `Malachi.Test.VersionedMetadataMachine` implements one version ABOVE production, which adds
  `{:probe, topic}`; a peer pinned one below it plays a binary that has never heard of it. Before the
  gate, the members that knew `:probe` applied it and the others skipped it, and the group's replicas
  held different states with no error anywhere. With the gate, `ra`'s `all` strategy keeps the effective
  version down while any member lacks the newer one, and every member refuses `:probe` the same way.

  The same group is then taken through what an operator does to it: a full restart (the whole log
  replays), a rollout held by the pin with a rollback in the middle, the finalization that lifts the
  pin, and a rollback below the new floor, which must stop that member instead of letting it diverge.
  """
  use ExUnit.Case, async: false

  @moduletag :multinode
  @moduletag timeout: 180_000

  alias Malachi.Cluster.MachineVersion
  alias Malachi.Cluster.RaCluster
  alias Malachi.Metadata
  alias Malachi.Test.RaPeers
  alias Malachi.Test.VersionedMetadataMachine, as: Machine

  # The double is one version ahead of production, which is the whole point of it: `@older` is what a
  # previous build plays, `@newer` what this one does.
  @older MachineVersion.code_version()
  @newer MachineVersion.code_version() + 1

  setup_all do
    RaPeers.ensure_distribution()
  end

  defp start_group(pins) do
    cluster = :"mv_mixed_#{System.unique_integer([:positive])}"
    peers = Enum.map(pins, &RaPeers.start/1)
    # Outlives the test process, so on_exit can still stop whatever handles the test replaced.
    {:ok, handles} = Agent.start(fn -> peers |> Enum.with_index() |> Map.new(fn {peer, index} -> {index, peer} end) end)

    on_exit(fn ->
      handles |> Agent.get(&Map.values/1) |> Enum.each(&RaPeers.stop/1)
      Agent.stop(handles)
    end)

    {:ok, _member} = RaCluster.start(Machine, cluster, Enum.map(peers, & &1.node))
    %{cluster: cluster, handles: handles}
  end

  defp peers(%{handles: handles}), do: handles |> Agent.get(& &1) |> Enum.sort() |> Enum.map(&elem(&1, 1))
  defp peer(group, index), do: Enum.at(peers(group), index)

  defp restart(group, index, pin) do
    restarted = RaPeers.restart(peer(group, index), pin, [group.cluster])
    Agent.update(group.handles, &Map.put(&1, index, restarted))
    restarted
  end

  # Submits through any member that answers; retries while a leader is being elected.
  defp command(group, command, members \\ nil) do
    nodes = Enum.map(members || peers(group), & &1.node)

    RaPeers.eventually(fn ->
      Enum.find_value(nodes, fn node ->
        case RaCluster.command({group.cluster, node}, command) do
          {:ok, reply} -> {:ok, reply}
          {:error, _unreachable} -> nil
        end
      end)
    end)
  end

  # Commits a marker topic and waits until every listed member has applied it: after this, the members
  # have applied the same prefix of the log, so their states can be compared.
  defp barrier(group, members \\ nil) do
    members = members || peers(group)
    marker = "barrier-#{System.unique_integer([:positive])}"
    assert {:ok, {:ok, _root}} = command(group, {:create_topic, marker, 1}, members)

    assert RaPeers.eventually(fn ->
             Enum.all?(members, &Metadata.get_topic(RaPeers.local_state(&1.node, group.cluster), marker))
           end)

    marker
  end

  defp states(group, members \\ nil),
    do: Enum.map(members || peers(group), &RaPeers.local_state(&1.node, group.cluster))

  defp await_effective(group, expected, members \\ nil) do
    assert RaPeers.eventually(fn ->
             Enum.all?(members || peers(group), &(RaPeers.effective(&1.node, group.cluster) == expected))
           end),
           "effective versions: #{inspect(Enum.map(peers(group), &RaPeers.effective(&1.node, group.cluster)))}"
  end

  defp check(group, member),
    do: :erpc.call(member.node, MachineVersion, :check, [Machine, {group.cluster, member.node}, :ok])

  test "a member one version behind holds the group back, and every member refuses the newer command alike" do
    group = start_group([nil, nil, @older])
    barrier(group)
    await_effective(group, @older)

    # Several leader ticks later, the member without the newer version still holds the group down.
    Process.sleep(2_500)
    await_effective(group, @older)

    reply = command(group, {:probe, "p"})
    barrier(group)

    # Divergence first: whichever member led, the replicas must hold one state.
    applied_probe = Enum.map(states(group), &(Metadata.get_topic(&1, "p") != nil))
    assert applied_probe == [false, false, false], "members that applied :probe: #{inspect(applied_probe)}"
    [first | rest] = states(group)
    for state <- rest, do: assert(state == first)
    assert {:ok, {:error, _refused}} = reply
  end

  test "a full restart replays every member to the same state and effective version" do
    group = start_group([nil, nil, @older])
    barrier(group)
    await_effective(group, @older)
    assert {:ok, {:error, _refused}} = command(group, {:probe, "p"})
    barrier(group)
    before = hd(states(group))

    # Every member goes down before any comes back: nobody keeps the log in memory.
    Enum.each(peers(group), &RaPeers.stop/1)
    for index <- 0..2, do: restart(group, index, if(index == 2, do: @older, else: nil))

    marker = barrier(group)
    await_effective(group, @older)

    [first | rest] = states(group)
    for state <- rest, do: assert(state == first)
    assert Map.delete(first.topics, marker) == before.topics
  end

  test "a pinned rollout can roll back, finalizing lifts the version, and a rollback below it stops that member" do
    group = start_group([@older, @older, @older])
    barrier(group)
    await_effective(group, @older)
    assert {:ok, {:error, _refused}} = command(group, {:probe, "p"})

    # Rollback in the middle of a pinned rollout: the member comes back on the older code and keeps up.
    rolled_back = restart(group, 0, @older)
    marker = barrier(group)
    assert Metadata.get_topic(RaPeers.local_state(rolled_back.node, group.cluster), marker)
    assert {:ok, nil} = check(group, rolled_back)

    # Finalize: lift the pin with a rolling restart. The leader re-asks its peers on every tick, so the
    # group reaches the newer version once the last member advertises it, with no election needed.
    for index <- 0..2 do
      restart(group, index, nil)
      barrier(group)
    end

    await_effective(group, @newer)
    assert {:ok, {:ok, _root}} = command(group, {:probe, "p"})
    barrier(group)
    for state <- states(group), do: assert(Metadata.get_topic(state, "p"))

    # A rollback below the floor: the member meets the newer noop in its log and stops applying.
    stuck = restart(group, 2, @older)
    assert RaPeers.eventually(fn -> match?({{:stuck, @newer, @older}, :stuck}, check(group, stuck)) end)

    healthy = Enum.take(peers(group), 2)
    marker = barrier(group, healthy)
    Process.sleep(1_000)
    refute Metadata.get_topic(RaPeers.local_state(stuck.node, group.cluster), marker)
  end
end
