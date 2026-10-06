defmodule Malachi.Cluster.BindTopicPolicyUpgradeMultinodeTest do
  @moduledoc """
  The rolling upgrade of the first command that binds a topic to a policy, over real `ra` on peer nodes
  running the production metadata machine.

  `{:bind_topic_policy, topic, name}` is introduced at machine version 4 so that a group whose members
  run different code never applies it on some replicas and refuses it on others. A member pinned to 3
  plays the previous release: while it is in the group, or while the operator holds the pin, the
  effective version stays at 3 and every member refuses the binding the same way. Lifting the pin with a
  rolling restart finalizes the upgrade, the binding applies everywhere, and a full restart replays the
  log to the same state on every member.
  """
  use ExUnit.Case, async: false

  @moduletag :multinode
  @moduletag timeout: 180_000

  alias Malachi.Cluster.MachineVersion
  alias Malachi.Cluster.MetadataMachine
  alias Malachi.Cluster.RaCluster
  alias Malachi.Metadata
  alias Malachi.Test.RaPeers

  @introduced 4
  @older @introduced - 1

  setup_all do
    RaPeers.ensure_distribution()
  end

  test "this build implements the version the binding needs" do
    assert MachineVersion.code_version() >= @introduced
  end

  test "a member on the previous release holds the binding back on every member, and finalizing applies it" do
    group = start_group([nil, nil, @older])
    assert {:ok, {:ok, _root}} = command(group, {:create_topic, "events", 4})
    barrier(group)
    await_effective(group, @older)

    refused = command(group, {:bind_topic_policy, "events", "durable"})
    barrier(group)

    assert refused == {:ok, {:error, {:unsupported_command, {:bind_topic_policy, 3}, @introduced, @older}}}
    for state <- states(group), do: assert(Metadata.topic_policy_name(state, "events") == nil)
    assert_identical(group)

    # Finalize: the last member comes back on the new code, the leader re-asks its peers, and the group
    # reaches the version with no election needed.
    # With no pin left the group reaches this build's version, which is the introducing one or a later
    # release's: the command applies either way.
    restart(group, 2, nil)
    barrier(group)
    await_effective(group, MachineVersion.code_version())

    assert {:ok, :ok} = command(group, {:bind_topic_policy, "events", "durable"})
    barrier(group)
    for state <- states(group), do: assert(Metadata.topic_policy_name(state, "events") == "durable")
    assert_identical(group)

    # Every member goes down before any comes back: the whole log, refusal and binding, replays.
    before = hd(states(group))
    Enum.each(peers(group), &RaPeers.stop/1)
    for index <- 0..2, do: restart(group, index, nil)
    marker = barrier(group)

    assert_identical(group)
    assert Map.delete(hd(states(group)).topics, marker) == before.topics
  end

  defp start_group(pins) do
    cluster = :"bind_upgrade_#{System.unique_integer([:positive])}"
    peers = Enum.map(pins, &RaPeers.start/1)
    {:ok, handles} = Agent.start(fn -> peers |> Enum.with_index() |> Map.new(fn {peer, index} -> {index, peer} end) end)

    on_exit(fn ->
      handles |> Agent.get(&Map.values/1) |> Enum.each(&RaPeers.stop/1)
      Agent.stop(handles)
    end)

    {:ok, _member} = RaCluster.start(MetadataMachine, cluster, Enum.map(peers, & &1.node))
    %{cluster: cluster, handles: handles}
  end

  defp peers(%{handles: handles}), do: handles |> Agent.get(& &1) |> Enum.sort() |> Enum.map(&elem(&1, 1))

  defp restart(group, index, pin) do
    peer = Enum.at(peers(group), index)
    restarted = RaPeers.restart(peer, pin, [group.cluster])
    Agent.update(group.handles, &Map.put(&1, index, restarted))
    restarted
  end

  defp command(group, command) do
    nodes = Enum.map(peers(group), & &1.node)

    RaPeers.eventually(fn ->
      Enum.find_value(nodes, fn node ->
        case RaCluster.command({group.cluster, node}, command) do
          {:ok, reply} -> {:ok, reply}
          {:error, _unreachable} -> nil
        end
      end)
    end)
  end

  # Commits a marker topic and waits until every member has applied it, so their states are comparable.
  defp barrier(group) do
    marker = "barrier-#{System.unique_integer([:positive])}"
    assert {:ok, {:ok, _root}} = command(group, {:create_topic, marker, 1})

    assert RaPeers.eventually(fn ->
             Enum.all?(peers(group), &Metadata.get_topic(RaPeers.local_state(&1.node, group.cluster), marker))
           end)

    marker
  end

  defp states(group), do: Enum.map(peers(group), &RaPeers.local_state(&1.node, group.cluster))

  defp assert_identical(group) do
    [first | rest] = states(group)
    for state <- rest, do: assert(state == first)
  end

  defp await_effective(group, expected) do
    assert RaPeers.eventually(fn ->
             Enum.all?(peers(group), &(RaPeers.effective(&1.node, group.cluster) == expected))
           end),
           "effective versions: #{inspect(Enum.map(peers(group), &RaPeers.effective(&1.node, group.cluster)))}"
  end
end
