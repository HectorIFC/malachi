defmodule Malachi.Cluster.VnodeSplitTest do
  # End-to-end vnode split over real Raft: forms a vnode's ra cluster with a topic and a membership server
  # seeded with the version-0 topology, then VnodeSplit.split migrates the displaced topic and advances +
  # publishes the topology. Tagged so it can be excluded where multi-node networking is unavailable.
  use ExUnit.Case, async: false

  import Malachi.Test.TeardownHelper

  @moduletag :multinode

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MembershipServer
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.ReplicatedDSRSM
  alias Malachi.Cluster.RingTopology
  alias Malachi.Cluster.VnodeSplit
  alias Malachi.Metadata

  setup_all do
    _ = System.cmd("epmd", ["-daemon"])

    case :net_kernel.start([:"malachi_primary@127.0.0.1", :longnames]) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    {:ok, _} = Application.ensure_all_started(:ra)
    :ok
  end

  defp start_membership(topology) do
    name = :"vsplit_ms_#{System.unique_integer([:positive])}"
    {:ok, pid} = MembershipServer.start_link(name: name, peers: [], topology: topology, protocol_period: 3_600_000)
    on_exit(fn -> stop_quietly(pid) end)
    name
  end

  # ra may need a moment to elect after cluster start; retry the command until it lands.
  defp commit(replicated, topic, command, remaining_ms \\ 5_000) do
    case ReplicatedDSRSM.command(replicated, topic, command) do
      {:error, {:raft, _reason}} when remaining_ms > 0 ->
        Process.sleep(100) && commit(replicated, topic, command, remaining_ms - 100)

      reply ->
        reply
    end
  end

  test "split migrates the displaced topic over ra and advances + publishes the topology; refuses when not leader" do
    unique = System.unique_integer([:positive])
    source = :"vsplit_src_#{unique}"
    dest = :"vsplit_dst_#{unique}"
    on_exit(fn -> Enum.each([source, dest], &MetadataServer.delete/1) end)

    # a single-vnode cluster on the local node holding a topic
    {:ok, replicated} = ReplicatedDSRSM.add_vnode(ReplicatedDSRSM.new(), source, 0, [node()])
    {:ok, _root} = commit(replicated, "orders", {:create_topic, "orders", 4})

    # a membership seeded with the version-0 routing topology (the boot baseline a split advances from)
    membership = start_membership(RingTopology.new(replicated.ring, %{source => [node()]}))
    token = :erlang.phash2("orders", Integer.pow(2, 32))

    # a non-leader refuses (only the lease holder splits)
    assert VnodeSplit.split(membership, dest, token, [node()], fn -> false end) == {:error, :not_leader}

    # the leader splits end to end
    assert :ok = VnodeSplit.split(membership, dest, token, [node()], fn -> true end)

    # the topology bumped twice: begin_split (v1, intent recorded) then advance (v2, split complete) - and
    # the completed topology has the new vnode with the intent cleared, published back to the membership
    topo = MembershipServer.topology(membership)
    assert topo.version == 2
    assert topo.pending == nil
    assert topo.ring |> HashRing.vnode_ids() |> Enum.sort() == Enum.sort([source, dest])
    assert topo.placements[dest] == [node()]

    # and the migration happened over ra: "orders" now lives in the new vnode's cluster
    grown = %ReplicatedDSRSM{ring: topo.ring, vnodes: RingTopology.servers(topo)}
    assert ReplicatedDSRSM.vnode_for(grown, "orders") == {:ok, dest}
    {:ok, dest_meta} = ReplicatedDSRSM.query(grown, "orders", &Function.identity/1)
    assert Metadata.get_topic(dest_meta, "orders").name == "orders"
  end

  test "a logical split failure clears the intent and leaves the ring unchanged" do
    unique = System.unique_integer([:positive])
    source = :"vsplit_fail_src_#{unique}"
    dest = :"vsplit_fail_dst_#{unique}"
    on_exit(fn -> Enum.each([source, dest], &MetadataServer.delete/1) end)

    {:ok, replicated} = ReplicatedDSRSM.add_vnode(ReplicatedDSRSM.new(), source, 0, [node()])
    {:ok, _root} = commit(replicated, "orders", {:create_topic, "orders", 4})

    membership = start_membership(RingTopology.new(replicated.ring, %{source => [node()]}))
    token = :erlang.phash2("orders", Integer.pow(2, 32))

    # the new vnode's ra cluster can't form on an unreachable node -> split_vnode fails; do_split records the
    # intent (v1) then, seeing the failure, clears it (v2). No crash happened, so nothing is left pending.
    assert {:error, _reason} = VnodeSplit.split(membership, dest, token, [:"nonexistent@127.0.0.1"], fn -> true end)

    topo = MembershipServer.topology(membership)
    assert topo.version == 2
    assert topo.pending == nil
    # the ring is untouched: the split never took effect, only the source exists
    assert HashRing.vnode_ids(topo.ring) == [source]
  end

  test "reconcile completes a split whose coordinator crashed mid-way (intent pending): topic on the new vnode" do
    unique = System.unique_integer([:positive])
    source = :"vsplit_rec_src_#{unique}"
    dest = :"vsplit_rec_dst_#{unique}"
    on_exit(fn -> Enum.each([source, dest], &MetadataServer.delete/1) end)

    {:ok, replicated} = ReplicatedDSRSM.add_vnode(ReplicatedDSRSM.new(), source, 0, [node()])
    {:ok, root} = commit(replicated, "orders", {:create_topic, "orders", 4})
    :ok = commit(replicated, "orders", {:commit_offset, "workers", "orders", %{root => 500}})
    token = :erlang.phash2("orders", Integer.pow(2, 32))

    # simulate a crash mid-split: the migration ran (so "orders" physically lives on dest and dest's cluster
    # is up), but the coordinator died before advancing the topology, so the membership still carries the
    # *pending intent* over the *old* ring (begin_split, v1), exactly the interrupted state B2-2 leaves.
    {:ok, _grown} = ReplicatedDSRSM.split_vnode(replicated, dest, token, [node()])
    base = RingTopology.new(replicated.ring, %{source => [node()]})
    membership = start_membership(RingTopology.begin_split(base, dest, token, [node()]))

    # a non-leader does not reconcile; the leader drives the interrupted split *forward* to completion
    assert VnodeSplit.reconcile(membership, fn -> false end) == {:error, :not_leader}
    assert :ok = VnodeSplit.reconcile(membership, fn -> true end)

    # the completed topology was published: version advanced (v2), intent cleared, ring grown with the new vnode
    topo = MembershipServer.topology(membership)
    assert topo.version == 2
    assert topo.pending == nil
    assert topo.ring |> HashRing.vnode_ids() |> Enum.sort() == Enum.sort([source, dest])
    assert topo.placements[dest] == [node()]

    # "orders" now lives on the new vnode with its offsets, writable (the split finished, not rolled back)
    grown = %ReplicatedDSRSM{ring: topo.ring, vnodes: RingTopology.servers(topo)}
    assert ReplicatedDSRSM.vnode_for(grown, "orders") == {:ok, dest}
    {:ok, dest_meta} = ReplicatedDSRSM.query(grown, "orders", &Function.identity/1)
    assert Metadata.get_topic(dest_meta, "orders").name == "orders"
    assert Metadata.committed_offsets(dest_meta, "workers", "orders") == %{root => 500}
    assert commit(grown, "orders", {:commit_offset, "workers", "orders", %{root => 900}}) == :ok

    # the source no longer holds it (it was migrated forward, not restored)
    {:ok, src_meta} = MetadataServer.query({source, node()}, &Function.identity/1)
    assert Metadata.get_topic(src_meta, "orders") == nil
  end

  test "reconcile keeps the intent pending when the new vnode is unreachable (retry later, no data loss)" do
    unique = System.unique_integer([:positive])
    source = :"vsplit_unreach_src_#{unique}"
    on_exit(fn -> MetadataServer.delete(source) end)

    {:ok, replicated} = ReplicatedDSRSM.add_vnode(ReplicatedDSRSM.new(), source, 0, [node()])
    {:ok, _root} = commit(replicated, "orders", {:create_topic, "orders", 4})
    token = :erlang.phash2("orders", Integer.pow(2, 32))

    # a pending intent whose new vnode lives on an unreachable node: complete_split cannot start that vnode's
    # cluster, so it fails before migrating anything and reconcile must NOT clear the intent (retry later)
    base = RingTopology.new(replicated.ring, %{source => [node()]})
    pending = RingTopology.begin_split(base, :"vsplit_unreach_dst_#{unique}", token, [:"nonexistent@127.0.0.1"])
    membership = start_membership(pending)

    assert :ok = VnodeSplit.reconcile(membership, fn -> true end)

    # the intent is still pending (v1, unchanged) for a later reconcile; the source is untouched
    topo = MembershipServer.topology(membership)
    assert topo.version == 1
    assert topo.pending == pending.pending
    {:ok, meta} = ReplicatedDSRSM.query(replicated, "orders", &Function.identity/1)
    assert Metadata.get_topic(meta, "orders").name == "orders"
    assert meta.migrating == %{}
  end
end
