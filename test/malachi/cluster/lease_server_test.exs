defmodule Malachi.Cluster.LeaseServerTest do
  # async: false: ra is global/stateful (one data dir, on-disk Raft logs).
  use ExUnit.Case, async: false

  alias Malachi.Cluster.Lease
  alias Malachi.Cluster.LeaseServer

  # a long term so the lease never expires mid-test (expiry itself is covered by LeaseTest, which
  # controls `now` directly; here ra stamps a real clock we cannot advance)
  @duration 60_000

  setup_all do
    :ok
  end

  defp start_lease do
    name = :"lease_#{System.unique_integer([:positive])}"
    {:ok, server_id} = LeaseServer.start(name)
    on_exit(fn -> LeaseServer.delete(name) end)
    server_id
  end

  test "acquires a free lease, then renews it keeping the same fence" do
    server = start_lease()

    assert {:ok, {:ok, 1}} = LeaseServer.acquire_or_renew(server, :node_a, @duration)
    assert {:ok, {:ok, 1}} = LeaseServer.acquire_or_renew(server, :node_a, @duration)

    assert {:ok, %Lease{holder: :node_a, fence: 1}} = LeaseServer.get(server)
  end

  test "refuses another candidate while the lease is held and valid" do
    server = start_lease()
    assert {:ok, {:ok, 1}} = LeaseServer.acquire_or_renew(server, :node_a, @duration)

    assert {:ok, {:error, {:held, :node_a}}} = LeaseServer.acquire_or_renew(server, :node_b, @duration)
    assert {:ok, %Lease{holder: :node_a}} = LeaseServer.get(server)
  end

  test "release frees the lease and the next acquire advances the fence" do
    server = start_lease()
    assert {:ok, {:ok, 1}} = LeaseServer.acquire_or_renew(server, :node_a, @duration)

    assert {:ok, :ok} = LeaseServer.release(server, :node_a, 1)
    assert {:ok, %Lease{holder: nil}} = LeaseServer.get(server)

    assert {:ok, {:ok, 2}} = LeaseServer.acquire_or_renew(server, :node_b, @duration)
  end

  test "state survives a server restart (the lease is Raft-durable)" do
    name = :"lease_#{System.unique_integer([:positive])}"
    {:ok, server_id} = LeaseServer.start(name)
    on_exit(fn -> LeaseServer.delete(name) end)

    {:ok, {:ok, 1}} = LeaseServer.acquire_or_renew(server_id, :node_a, @duration)
    :ok = :ra.stop_server(:default, server_id)
    :ok = :ra.restart_server(:default, server_id)

    assert {:ok, %Lease{holder: :node_a, fence: 1}} = LeaseServer.get(server_id)
  end

  test "reconcile/2 bootstraps the cluster when it has not been started yet" do
    name = :"lease_#{System.unique_integer([:positive])}"
    on_exit(fn -> LeaseServer.delete(name) end)

    # no prior start/2: reconcile forms the (single-node) cluster itself
    assert LeaseServer.reconcile(name, [node()]) == :ok
    assert {:ok, {:ok, 1}} = LeaseServer.acquire_or_renew({name, node()}, :node_a, @duration)
  end

  test "reconcile/2 is an idempotent no-op on an already-formed cluster (does not disrupt the lease)" do
    server = start_lease()
    assert {:ok, {:ok, 1}} = LeaseServer.acquire_or_renew(server, :node_a, @duration)

    assert LeaseServer.reconcile(elem(server, 0), [node()]) == :ok

    # the held lease is untouched
    assert {:ok, %Lease{holder: :node_a, fence: 1}} = LeaseServer.get(server)
  end
end
