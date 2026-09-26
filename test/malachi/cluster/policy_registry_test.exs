defmodule Malachi.Cluster.PolicyRegistryTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.MachineVersion
  alias Malachi.Cluster.PolicyRegistry

  describe "define_policy" do
    test "stores a definition under its name, last write wins" do
      {state, :ok} = PolicyRegistry.apply(PolicyRegistry.new(), {:define_policy, "durable", %{spread_by: "rack"}})
      assert PolicyRegistry.get(state, "durable") == %{spread_by: "rack"}

      {state, :ok} = PolicyRegistry.apply(state, {:define_policy, "durable", %{spread_by: "dc"}})
      assert PolicyRegistry.get(state, "durable") == %{spread_by: "dc"}
    end

    test "refuses a name or a definition the cluster could not act on" do
      state = PolicyRegistry.new()

      assert {^state, {:error, :invalid_policy}} = PolicyRegistry.apply(state, {:define_policy, "", %{}})
      assert {^state, {:error, :invalid_policy}} = PolicyRegistry.apply(state, {:define_policy, :p, %{}})

      assert {^state, {:error, :invalid_policy}} =
               PolicyRegistry.apply(state, {:define_policy, "p", %{retention: %{max_bytes: "10GB"}}})

      assert {^state, {:error, :invalid_policy}} = PolicyRegistry.apply(state, {:define_policy, "p", %{oops: 1}})
    end
  end

  describe "delete_policy" do
    test "removes a definition and is idempotent" do
      {state, :ok} = PolicyRegistry.apply(PolicyRegistry.new(), {:define_policy, "p", %{}})

      {state, :ok} = PolicyRegistry.apply(state, {:delete_policy, "p"})
      assert PolicyRegistry.get(state, "p") == nil

      assert {^state, :ok} = PolicyRegistry.apply(state, {:delete_policy, "p"})
    end
  end

  describe "reads" do
    test "an undefined name, and a name that is not a name, answer nil" do
      state = PolicyRegistry.new()
      assert PolicyRegistry.get(state, "ghost") == nil
      assert PolicyRegistry.get(state, nil) == nil
    end

    test "all/1 is what a retention sweep resolves against, in one read" do
      {state, :ok} = PolicyRegistry.apply(PolicyRegistry.new(), {:define_policy, "a", %{spread_by: "rack"}})
      {state, :ok} = PolicyRegistry.apply(state, {:define_policy, "b", %{retention: %{max_bytes: 10}}})

      assert PolicyRegistry.all(state) == %{"a" => %{spread_by: "rack"}, "b" => %{retention: %{max_bytes: 10}}}
    end
  end

  test "an unknown command leaves the state alone rather than raising" do
    state = PolicyRegistry.new()
    assert {^state, {:error, :unknown_command}} = PolicyRegistry.apply(state, {:rename_policy, "a", "b"})
  end

  test "its commands are introduced at the version that ships the store, so older members refuse them" do
    table = PolicyRegistry.command_versions()

    assert table == %{{:define_policy, 3} => 3, {:delete_policy, 2} => 3}
    assert Enum.all?(Map.values(table), &(&1 == MachineVersion.code_version()))
  end
end
