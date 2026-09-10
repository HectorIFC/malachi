defmodule Malachi.Cluster.TopologyPublisherTest do
  # async: false: ra is global and stateful (one data dir, on-disk Raft logs).
  @moduledoc """
  The persist-then-gossip order, and the property that makes the order matter: a ring the durable store
  **refused** must never be disseminated. Gossiping a refused ring would put the cluster back to
  believing a topology nothing recorded, which is the failure the store exists to prevent.
  """
  use ExUnit.Case, async: false

  import Malachi.Test.TeardownHelper

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MembershipServer
  alias Malachi.Cluster.Ring
  alias Malachi.Cluster.RingServer
  alias Malachi.Cluster.RingTopology
  alias Malachi.Cluster.TopologyPublisher

  defp start_ring_store do
    name = :"tp_ring_#{System.unique_integer([:positive])}"
    {:ok, server_id} = RingServer.start(name)
    on_exit(fn -> RingServer.delete(name) end)
    server_id
  end

  defp start_membership(topology) do
    name = :"tp_ms_#{System.unique_integer([:positive])}"
    {:ok, pid} = MembershipServer.start_link(name: name, peers: [], topology: topology, protocol_period: 3_600_000)
    on_exit(fn -> stop_quietly(pid) end)
    name
  end

  defp topology(vnodes, version) do
    ring =
      Enum.reduce(vnodes, HashRing.new(), fn {id, token}, ring ->
        {:ok, ring} = HashRing.add_vnode(ring, id, token)
        ring
      end)

    placements = Map.new(vnodes, fn {id, _token} -> {id, [node()]} end)
    %{RingTopology.new(ring, placements) | version: version}
  end

  test "records the ring durably and only then publishes it to the membership" do
    ring_server_id = start_ring_store()
    seed = topology([{:vn_a, 0}], 0)
    :ok = RingServer.init(ring_server_id, seed)
    membership = start_membership(seed)
    grown = topology([{:vn_a, 0}, {:vn_b, 500}], 1)

    assert TopologyPublisher.publish(ring_server_id, membership, grown, 0, 1) == :ok

    assert {:ok, stored} = RingServer.topology(ring_server_id)
    assert stored.version == 1
    assert MembershipServer.topology(membership).version == 1
  end

  test "a ring the store refuses is NOT gossiped" do
    ring_server_id = start_ring_store()
    seed = topology([{:vn_a, 0}], 0)
    :ok = RingServer.init(ring_server_id, seed)
    membership = start_membership(seed)

    # a writer that read version 0 but is extending a ring already at 1
    :ok = TopologyPublisher.publish(ring_server_id, membership, topology([{:vn_a, 0}, {:vn_b, 500}], 1), 0, 1)

    stale = topology([{:vn_a, 0}, {:vn_c, 900}], 1)
    assert {:error, {:conflict, %Ring{}}} = TopologyPublisher.publish(ring_server_id, membership, stale, 0, 1)

    gossiped = MembershipServer.topology(membership)

    assert gossiped.ring |> HashRing.vnode_ids() |> Enum.sort() == [:vn_a, :vn_b],
           "the membership must not carry a ring the store rejected"
  end

  test "a writer whose lease fence went stale is refused, and nothing is gossiped" do
    ring_server_id = start_ring_store()
    seed = topology([{:vn_a, 0}], 0)
    :ok = RingServer.init(ring_server_id, seed)
    membership = start_membership(seed)

    :ok = TopologyPublisher.publish(ring_server_id, membership, topology([{:vn_a, 0}, {:vn_b, 500}], 1), 0, 5)

    # the old leader, still believing it leads, carries a fence the store has moved past
    stale_leader = topology([{:vn_a, 0}, {:vn_b, 500}, {:vn_c, 900}], 2)
    assert {:error, {:conflict, _}} = TopologyPublisher.publish(ring_server_id, membership, stale_leader, 1, 4)

    assert MembershipServer.topology(membership).version == 1
  end

  test "seam/2 is the same publish, bound to a store and a membership" do
    ring_server_id = start_ring_store()
    seed = topology([{:vn_a, 0}], 0)
    :ok = RingServer.init(ring_server_id, seed)
    membership = start_membership(seed)
    publish = TopologyPublisher.seam(ring_server_id, membership)

    assert publish.(topology([{:vn_a, 0}, {:vn_b, 500}], 1), 0, 1) == :ok
    assert {:ok, stored} = RingServer.topology(ring_server_id)
    assert stored.version == 1
  end

  test "gossip_only/1 skips the store, which is why it is for tests and never wired in production" do
    ring_server_id = start_ring_store()
    seed = topology([{:vn_a, 0}], 0)
    :ok = RingServer.init(ring_server_id, seed)
    membership = start_membership(seed)

    assert TopologyPublisher.gossip_only(membership).(topology([{:vn_a, 0}, {:vn_b, 500}], 1), 0, 1) == :ok

    assert MembershipServer.topology(membership).version == 1
    assert {:ok, stored} = RingServer.topology(ring_server_id)
    assert stored.version == 0, "gossip_only must leave the durable store untouched"
  end

  test "an unreachable store stops the publication rather than gossiping past it" do
    absent = {:"tp_absent_#{System.unique_integer([:positive])}", node()}
    membership = start_membership(topology([{:vn_a, 0}], 0))

    assert {:error, _reason} =
             TopologyPublisher.publish(absent, membership, topology([{:vn_a, 0}, {:vn_b, 500}], 1), 0, 1)

    assert MembershipServer.topology(membership).version == 0
  end
end
