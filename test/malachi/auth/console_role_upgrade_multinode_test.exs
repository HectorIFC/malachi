defmodule Malachi.Auth.ConsoleRoleUpgradeMultinodeTest do
  @moduledoc """
  The rolling upgrade of the console roles (#228), over real `ra` on peer nodes running the production user
  machine.

  `{:set_role, username, role}`, the five element `put_user` and `{:import_users_with_roles, entries}` are
  introduced at machine version 5 so that a group whose members run different code never applies them on
  some replicas and refuses them on others. A member pinned to 4 plays the previous release: while it is in
  the group, the effective version stays at 4 and every member refuses the three the same way, while the
  older shapes (a user created without a role) keep applying. Lifting the pin finalizes the upgrade, the
  roles apply everywhere, and a full restart replays the log to the same state on every member.
  """
  use ExUnit.Case, async: false

  @moduletag :multinode
  @moduletag timeout: 180_000

  alias Malachi.Auth.UserMachine
  alias Malachi.Auth.UserRegistry
  alias Malachi.Auth.UserStore
  alias Malachi.Cluster.MachineVersion
  alias Malachi.Cluster.RaCluster
  alias Malachi.Test.RaPeers

  @introduced 5
  @older @introduced - 1

  setup_all do
    RaPeers.ensure_distribution()
  end

  test "this build implements the version the roles need" do
    assert MachineVersion.code_version() >= @introduced
  end

  test "a member on the previous release holds the roles back on every member, and finalizing applies them" do
    group = start_group([nil, nil, @older])
    assert {:ok, :ok} = command(group, {:put_user, "alice", "h", [:produce]})
    barrier(group)
    await_effective(group, @older)

    refusals = [
      {{:set_role, "alice", :viewer}, {:set_role, 3}},
      {{:put_user, "bob", "h", [], :editor}, {:put_user, 5}},
      {{:import_users_with_roles, [{"carol", "h", [], :admin}]}, {:import_users_with_roles, 2}}
    ]

    for {command, key} <- refusals do
      assert command(group, command) == {:ok, {:error, {:unsupported_command, key, @introduced, @older}}}
    end

    # The older shape still applies: creating a user without a role does not wait for the upgrade.
    assert {:ok, :ok} = command(group, {:put_user, "dave", "h", [:consume]})

    # An import whose only role is unknown is sent in the older shape, so it applies instead of being
    # refused whole: the bad entry is skipped before the command is chosen (UserStore.import_command/1).
    {import, 1} =
      UserStore.import_command([
        %{"username" => "erin", "password_hash" => "h", "permissions" => ["produce"]},
        %{"username" => "frank", "password_hash" => "h", "permissions" => [], "role" => "root"}
      ])

    assert {:ok, {:ok, %{imported: 1, skipped: 0}}} = command(group, import)
    barrier(group)

    for state <- states(group) do
      assert {:ok, %{role: nil}} = UserRegistry.get_principal(state, "alice")
      assert {:error, :user_not_found} = UserRegistry.get_principal(state, "bob")
      assert {:error, :user_not_found} = UserRegistry.get_principal(state, "carol")
      assert {:ok, %{role: nil}} = UserRegistry.get_principal(state, "dave")
      assert {:ok, %{role: nil, permissions: [:produce]}} = UserRegistry.get_principal(state, "erin")
      assert {:error, :user_not_found} = UserRegistry.get_principal(state, "frank")
    end

    assert_identical(group)

    # With no pin left the group reaches this build's version, which is the introducing one or a later
    # release's: the command applies either way.
    restart(group, 2, nil)
    barrier(group)
    await_effective(group, MachineVersion.code_version())

    assert {:ok, :ok} = command(group, {:set_role, "alice", :viewer})
    assert {:ok, :ok} = command(group, {:put_user, "bob", "h", [], :editor})

    assert {:ok, {:ok, %{imported: 1, skipped: 0}}} =
             command(group, {:import_users_with_roles, [{"carol", "h", [], :admin}]})

    barrier(group)

    for state <- states(group) do
      assert {:ok, %{role: :viewer}} = UserRegistry.get_principal(state, "alice")
      assert {:ok, %{role: :editor}} = UserRegistry.get_principal(state, "bob")
      assert {:ok, %{role: :admin}} = UserRegistry.get_principal(state, "carol")
    end

    assert_identical(group)

    # Every member goes down before any comes back: the whole log, refusals and roles, replays.
    before = hd(states(group))
    Enum.each(peers(group), &RaPeers.stop/1)
    for index <- 0..2, do: restart(group, index, nil)
    marker = barrier(group)

    assert_identical(group)
    assert Map.delete(hd(states(group)).users, marker) == before.users
  end

  defp start_group(pins) do
    cluster = :"role_upgrade_#{System.unique_integer([:positive])}"
    peers = Enum.map(pins, &RaPeers.start/1)
    {:ok, handles} = Agent.start(fn -> peers |> Enum.with_index() |> Map.new(fn {peer, index} -> {index, peer} end) end)

    on_exit(fn ->
      handles |> Agent.get(&Map.values/1) |> Enum.each(&RaPeers.stop/1)
      Agent.stop(handles)
    end)

    {:ok, _member} = RaCluster.start(UserMachine, cluster, Enum.map(peers, & &1.node))
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

  # Commits a marker user and waits until every member has applied it, so their states are comparable.
  defp barrier(group) do
    marker = "barrier-#{System.unique_integer([:positive])}"
    assert {:ok, :ok} = command(group, {:put_user, marker, "h", []})

    assert RaPeers.eventually(fn ->
             Enum.all?(peers(group), fn peer ->
               match?({:ok, _user}, UserRegistry.get_user(RaPeers.local_state(peer.node, group.cluster), marker))
             end)
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
