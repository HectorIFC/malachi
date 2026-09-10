defmodule Malachi.Cluster.RingServerTest do
  # async: false: ra is global and stateful (one data dir, on-disk Raft logs).
  @moduledoc """
  The durable ring over real `ra`: the first-boot seed, the fenced publish, and above all the
  three-valued read, which is the reason the ring lives in `ra` at all. A store that has never been
  seeded must **affirm** that, so boot can tell a genuinely fresh cluster from one whose record it
  simply cannot see.
  """
  use ExUnit.Case, async: false

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.Ring
  alias Malachi.Cluster.RingServer
  alias Malachi.Cluster.RingTopology

  defp start_ring do
    name = :"ring_#{System.unique_integer([:positive])}"
    {:ok, server_id} = RingServer.start(name)
    on_exit(fn -> RingServer.delete(name) end)
    server_id
  end

  defp topology(vnodes, version \\ 0) do
    ring =
      Enum.reduce(vnodes, HashRing.new(), fn {id, token}, ring ->
        {:ok, ring} = HashRing.add_vnode(ring, id, token)
        ring
      end)

    placements = Map.new(vnodes, fn {id, _token} -> {id, [node()]} end)
    %{RingTopology.new(ring, placements) | version: version}
  end

  test "an unseeded cluster affirms it has never held a ring" do
    server_id = start_ring()

    assert RingServer.topology(server_id) == {:ok, :none}
  end

  test "seeds at first boot and reads the topology back" do
    server_id = start_ring()
    seed = topology([{:vn_a, 0}, {:vn_b, 100}])

    assert RingServer.init(server_id, seed) == :ok
    assert {:ok, stored} = RingServer.topology(server_id)
    assert stored.ring |> HashRing.vnode_ids() |> Enum.sort() == [:vn_a, :vn_b]
    assert stored.placements == seed.placements
  end

  test "a second seed is refused and hands back the ring already stored" do
    server_id = start_ring()
    seed = topology([{:vn_a, 0}])
    :ok = RingServer.init(server_id, seed)

    assert {:error, {:exists, stored}} = RingServer.init(server_id, topology([{:vn_b, 7}]))
    assert HashRing.vnode_ids(stored.ring) == [:vn_a]
  end

  test "advances under the lease fence and refuses a writer that read a stale version" do
    server_id = start_ring()
    :ok = RingServer.init(server_id, topology([{:vn_a, 0}]))
    grown = topology([{:vn_a, 0}, {:vn_b, 100}], 1)

    assert RingServer.advance(server_id, 0, 1, grown) == :ok
    assert {:ok, stored} = RingServer.topology(server_id)
    assert stored.version == 1

    # a writer still believing it was extending version 0 is refused by the log, not by a timeout
    assert {:error, {:conflict, %Ring{}}} =
             RingServer.advance(server_id, 0, 1, topology([{:vn_a, 0}, {:vn_c, 200}], 1))

    assert {:ok, unchanged} = RingServer.topology(server_id)
    assert unchanged.ring |> HashRing.vnode_ids() |> Enum.sort() == [:vn_a, :vn_b]
  end

  test "refuses a writer whose lease fence is older than one already seen" do
    server_id = start_ring()
    :ok = RingServer.init(server_id, topology([{:vn_a, 0}]))
    :ok = RingServer.advance(server_id, 0, 5, topology([{:vn_a, 0}, {:vn_b, 100}], 1))

    assert {:error, {:conflict, _}} =
             RingServer.advance(server_id, 1, 4, topology([{:vn_a, 0}, {:vn_b, 100}, {:vn_c, 200}], 2))
  end

  test "get/1 exposes the stored fence for diagnostics" do
    server_id = start_ring()
    :ok = RingServer.init(server_id, topology([{:vn_a, 0}]))
    :ok = RingServer.advance(server_id, 0, 3, topology([{:vn_a, 0}, {:vn_b, 100}], 1))

    assert {:ok, %Ring{fence: 3} = ring} = RingServer.get(server_id)
    assert Ring.version(ring) == 1
  end

  test "an unreachable store answers with an error, never with 'no ring recorded'" do
    absent = {:"ring_absent_#{System.unique_integer([:positive])}", node()}

    # the distinction boot depends on: this must not look like a fresh cluster
    assert {:error, _reason} = RingServer.topology(absent)
    assert {:error, _reason} = RingServer.get(absent)
    assert {:error, _reason} = RingServer.init(absent, topology([{:vn_a, 0}]))
    assert {:error, _reason} = RingServer.advance(absent, 0, 1, topology([{:vn_a, 0}], 1))
  end

  test "reconcile/2 keeps this node joined and is a no-op once the local server is up" do
    name = :"ring_reconcile_#{System.unique_integer([:positive])}"
    on_exit(fn -> RingServer.delete(name) end)

    # a node that has not joined yet forms the cluster
    assert RingServer.reconcile(name, [node()]) == :ok
    assert RingServer.topology({name, node()}) == {:ok, :none}

    :ok = RingServer.init({name, node()}, topology([{:vn_a, 0}]))

    # and reconciling again neither re-forms nor disturbs what is recorded
    assert RingServer.reconcile(name, [node()]) == :ok
    assert {:ok, stored} = RingServer.topology({name, node()})
    assert HashRing.vnode_ids(stored.ring) == [:vn_a]
  end

  test "the stored ring survives the local server being stopped and resumed" do
    name = :"ring_resume_#{System.unique_integer([:positive])}"
    {:ok, server_id} = RingServer.start(name)
    on_exit(fn -> RingServer.delete(name) end)
    :ok = RingServer.init(server_id, topology([{:vn_a, 0}, {:vn_b, 100}]))

    :ok = :ra.stop_server(:default, {name, node()})
    # resume-first: starting again must restart the registered member, never form an empty one over it
    {:ok, ^server_id} = RingServer.start(name)

    assert {:ok, stored} = RingServer.topology(server_id)
    assert stored.ring |> HashRing.vnode_ids() |> Enum.sort() == [:vn_a, :vn_b]
  end
end
