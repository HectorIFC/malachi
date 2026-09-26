defmodule Malachi.Cluster.PolicyServerTest do
  # ra is global and stateful (one data dir, on-disk Raft logs), so these run alone.
  use ExUnit.Case, async: false

  alias Malachi.Cluster.PolicyServer

  setup do
    name = :"policies_#{System.unique_integer([:positive])}"
    {:ok, server_id} = PolicyServer.start(name)
    on_exit(fn -> PolicyServer.delete_cluster(name) end)
    %{server_id: server_id}
  end

  test "a definition written through the log is read back from the local replica", %{server_id: server_id} do
    assert {:ok, :ok} = PolicyServer.define(server_id, "durable", %{retention: %{max_age_ms: 1_000}})

    assert {:ok, %{retention: %{max_age_ms: 1_000}}} = PolicyServer.get(server_id, "durable")
    assert {:ok, %{"durable" => %{retention: %{max_age_ms: 1_000}}}} = PolicyServer.all(server_id)
  end

  test "an undefined name reads as nil rather than an error", %{server_id: server_id} do
    assert {:ok, nil} = PolicyServer.get(server_id, "ghost")
  end

  test "a definition the cluster could not act on is refused by the log, not stored", %{server_id: server_id} do
    assert {:ok, {:error, :invalid_policy}} =
             PolicyServer.define(server_id, "p", %{retention: %{max_bytes: "10GB"}})

    assert {:ok, nil} = PolicyServer.get(server_id, "p")
  end

  test "reconcile is idempotent, so a staggered boot converges", %{server_id: {name, _node} = server_id} do
    {:ok, :ok} = PolicyServer.define(server_id, "p", %{})

    assert PolicyServer.reconcile(name, [node()]) == :ok
    assert PolicyServer.reconcile(name, [node()]) == :ok
    assert {:ok, %{}} = PolicyServer.get(server_id, "p")
  end

  test "a deleted definition is gone, and deleting again is still :ok", %{server_id: server_id} do
    {:ok, :ok} = PolicyServer.define(server_id, "p", %{spread_by: "rack"})

    assert {:ok, :ok} = PolicyServer.delete(server_id, "p")
    assert {:ok, nil} = PolicyServer.get(server_id, "p")
    assert {:ok, :ok} = PolicyServer.delete(server_id, "p")
  end
end
