defmodule Malachi.Cluster.PolicyRegistryTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.MachineVersion
  alias Malachi.Cluster.Policy
  alias Malachi.Cluster.PolicyMachine
  alias Malachi.Cluster.PolicyRegistry

  @version MachineVersion.code_version()

  describe "define_policy" do
    test "stores a definition under its name, last write wins" do
      {state, :ok} =
        PolicyRegistry.apply(PolicyRegistry.new(), {:define_policy, "durable", %{spread_by: "rack"}}, @version)

      assert PolicyRegistry.get(state, "durable") == %{spread_by: "rack"}

      {state, :ok} = PolicyRegistry.apply(state, {:define_policy, "durable", %{spread_by: "dc"}}, @version)
      assert PolicyRegistry.get(state, "durable") == %{spread_by: "dc"}
    end

    test "refuses a name or a definition the cluster could not act on" do
      state = PolicyRegistry.new()

      assert {^state, {:error, :invalid_policy}} = PolicyRegistry.apply(state, {:define_policy, "", %{}}, @version)
      assert {^state, {:error, :invalid_policy}} = PolicyRegistry.apply(state, {:define_policy, :p, %{}}, @version)

      assert {^state, {:error, :invalid_policy}} =
               PolicyRegistry.apply(state, {:define_policy, "p", %{retention: %{max_bytes: "10GB"}}}, @version)

      assert {^state, {:error, :invalid_policy}} =
               PolicyRegistry.apply(state, {:define_policy, "p", %{oops: 1}}, @version)
    end
  end

  describe "delete_policy" do
    test "removes a definition and is idempotent" do
      {state, :ok} = PolicyRegistry.apply(PolicyRegistry.new(), {:define_policy, "p", %{}}, @version)

      {state, :ok} = PolicyRegistry.apply(state, {:delete_policy, "p"}, @version)
      assert PolicyRegistry.get(state, "p") == nil

      assert {^state, :ok} = PolicyRegistry.apply(state, {:delete_policy, "p"}, @version)
    end
  end

  describe "reads" do
    test "an undefined name, and a name that is not a name, answer nil" do
      state = PolicyRegistry.new()
      assert PolicyRegistry.get(state, "ghost") == nil
      assert PolicyRegistry.get(state, nil) == nil
    end

    test "all/1 is what a retention sweep resolves against, in one read" do
      {state, :ok} = PolicyRegistry.apply(PolicyRegistry.new(), {:define_policy, "a", %{spread_by: "rack"}}, @version)
      {state, :ok} = PolicyRegistry.apply(state, {:define_policy, "b", %{retention: %{max_bytes: 10}}}, @version)

      assert PolicyRegistry.all(state) == %{"a" => %{spread_by: "rack"}, "b" => %{retention: %{max_bytes: 10}}}
    end
  end

  test "an unknown command leaves the state alone rather than raising" do
    state = PolicyRegistry.new()
    assert {^state, {:error, :unknown_command}} = PolicyRegistry.apply(state, {:rename_policy, "a", "b"}, @version)
  end

  test "its commands are introduced at the version that ships the store, so older members refuse them" do
    table = PolicyRegistry.command_versions()

    # Version 3 shipped the store. Its commands stay there forever: every entry ever written replays.
    assert table == %{{:define_policy, 3} => 3, {:delete_policy, 2} => 3}
    assert Enum.all?(Map.values(table), &(&1 <= MachineVersion.code_version()))
  end

  describe "the effective machine version decides which fields a definition may set" do
    test "a field introduced above the effective version is refused by name, with both versions" do
      state = PolicyRegistry.new()

      assert {^state, {:error, {:unsupported_policy_field, "spread_by", 3, 2}}} =
               PolicyRegistry.apply(state, {:define_policy, "p", %{spread_by: "rack"}}, 2)

      assert {_state, :ok} = PolicyRegistry.apply(state, {:define_policy, "p", %{spread_by: "rack"}}, 3)
    end

    test "the ra machine gates the store's commands on the effective version of the log entry" do
      state = PolicyRegistry.new()
      command = {:define_policy, "p", %{spread_by: "rack"}}

      assert {_state, :ok} = PolicyMachine.apply(%{machine_version: 3}, command, state)

      assert {^state, {:error, {:unsupported_command, {:define_policy, 3}, 3, 2}}} =
               PolicyMachine.apply(%{machine_version: 2}, command, state)
    end

    # Live from the first field added after the store (#199 onwards): at the version just below the one
    # that introduced it, the machine must refuse it by name. That only holds while the machine hands the
    # registry the effective version of the entry rather than this build's own. With every field at 3
    # today there is no such version yet, and the pure half of the rule is proved in
    # Malachi.Cluster.PolicyTest with an injected table.
    test "every field newer than the store is refused by the machine one version below its own" do
      for %{name: name, path: path, since: since} <- Policy.fields(), since > 3 do
        policy = put_in(%{}, Enum.map(path, &Access.key(&1, %{})), 1)
        refused = {:error, {:unsupported_policy_field, name, since, since - 1}}

        assert {_state, ^refused} =
                 PolicyMachine.apply(%{machine_version: since - 1}, {:define_policy, "p", policy}, PolicyRegistry.new())
      end
    end
  end
end
