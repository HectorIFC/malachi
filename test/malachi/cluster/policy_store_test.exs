defmodule Malachi.Cluster.PolicyStoreTest do
  # ra is global and stateful, and this uses the cluster the application itself started.
  use ExUnit.Case, async: false

  alias Malachi.Cluster.PolicyStore

  setup do
    name = "store_test_#{System.unique_integer([:positive])}"
    on_exit(fn -> PolicyStore.delete(name) end)
    %{name: name}
  end

  test "a definition written here is read back by name and in the whole set", %{name: name} do
    assert PolicyStore.define(name, %{retention: %{max_bytes: 500}}) == :ok

    assert PolicyStore.get(name) == %{retention: %{max_bytes: 500}}
    assert {:ok, all} = PolicyStore.fetch_all()
    assert Map.fetch!(all, name) == %{retention: %{max_bytes: 500}}
  end

  test "a name nothing defined reads as nil, and so does no name at all", %{name: name} do
    assert PolicyStore.get(name) == nil
    # What a topic that points at no policy hands in: the caller does not have to special-case it.
    assert PolicyStore.get(nil) == nil
  end

  test "a definition the cluster could not act on is refused", %{name: name} do
    assert PolicyStore.define(name, %{retention: %{max_age_ms: -1}}) == {:error, :invalid_policy}
    assert PolicyStore.get(name) == nil
  end

  test "a deleted definition is gone, and deleting again is still :ok", %{name: name} do
    :ok = PolicyStore.define(name, %{spread_by: "rack"})

    assert PolicyStore.delete(name) == :ok
    assert PolicyStore.get(name) == nil
    assert PolicyStore.delete(name) == :ok
  end

  test "it names the cluster the application starts" do
    assert PolicyStore.cluster_name() == Malachi.LogPolicies
  end

  describe "a store this node cannot read" do
    setup %{name: name} do
      :ok = PolicyStore.define(name, %{spread_by: "rack"})
      server_id = {PolicyStore.cluster_name(), node()}
      :ok = :ra.stop_server(:default, server_id)
      on_exit(fn -> :ra.restart_server(:default, server_id) end)
      :ok
    end

    test "placement falls open to the cluster defaults, because a worse placement is recoverable", %{name: name} do
      assert PolicyStore.get(name) == nil
    end

    test "retention gets the failure back, because its fallback deletes", %{name: name} do
      # An empty map is what a cluster with no policies answers, and retention cannot tell the two
      # apart: under it, a topic kept for 30 days would expire under a global limit of 7, on every
      # replica, with no way back. The sweep skips instead.
      assert {:error, _reason} = PolicyStore.fetch_all()
      assert PolicyStore.get(name) == nil
    end

    test "a write says it failed, because silently losing a definition is not a default", %{name: name} do
      assert {:error, _reason} = PolicyStore.define(name, %{spread_by: "dc"})
      assert {:error, _reason} = PolicyStore.delete(name)
    end
  end
end
