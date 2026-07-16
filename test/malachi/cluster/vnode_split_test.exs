defmodule Malachi.Cluster.VnodeSplitTest do
  # End-to-end vnode split over real Raft: forms a vnode's ra cluster with a topic and a membership server
  # seeded with the version-0 topology, then VnodeSplit.split migrates the displaced topic and advances +
  # publishes the topology. Tagged so it can be excluded where multi-node networking is unavailable.
  use ExUnit.Case, async: false

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
    _ = :ra.start_in(~c"#{System.tmp_dir!()}/malachi_ra_vsplit_#{System.unique_integer([:positive])}")
    :ok
  end

  defp start_membership(topology) do
    name = :"vsplit_ms_#{System.unique_integer([:positive])}"
    {:ok, pid} = MembershipServer.start_link(name: name, peers: [], topology: topology, protocol_period: 3_600_000)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
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

    # the topology bumped twice — begin_split (v1, intent recorded) then advance (v2, split complete) — and
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
    # the ring is untouched — the split never took effect, only the source exists
    assert HashRing.vnode_ids(topo.ring) == [source]
  end
end
