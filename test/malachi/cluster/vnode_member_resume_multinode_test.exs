defmodule Malachi.Cluster.VnodeMemberResumeMultinodeTest do
  @moduledoc """
  A node that comes back rejoins the metadata vnodes it hosts, over real `ra` across real BEAM nodes (#136,
  found by the rolling upgrade drill of #196).

  `ra` does not restart a node's registered servers when the node comes back. Every other store has a
  reconciler resuming the node's own member; the vnodes had none, so a restarted node never rejoined them:
  a rolling restart took each vnode below quorum at its second node. The earlier multinode tests hid it,
  because they restart peers with `Malachi.Test.RaPeers.restart/3`, which calls `:ra.restart_server/2` by
  hand. These restart a peer without it and leave the resume to `Malachi.Application.resume_local_vnodes/3`,
  the function the vnode coordinator manager runs on every tick.

  `async: false` and tagged, like every test that starts peer nodes.
  """
  use ExUnit.Case, async: false

  @moduletag :multinode

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.RingTopology
  alias Malachi.Metadata
  alias Malachi.Test.RaPeers
  alias Malachi.Test.VnodeCoordinatorProbe, as: Probe

  setup_all do
    :ok = RaPeers.ensure_distribution()
    :ok
  end

  setup do
    a = RaPeers.start(nil)
    b = RaPeers.start(nil)
    vnode = :"vn_resume_#{System.unique_integer([:positive])}"
    nodes = [node(), a.node, b.node]
    {:ok, server_id} = MetadataServer.start(vnode, nodes)
    {:ok, {:ok, _root}} = MetadataServer.command(server_id, {:create_topic, "before", 2})

    # Peers first, then the local member alone: deleting the group through its members while a test has
    # stopped some of them makes ra log crash reports that have nothing to do with the test.
    on_exit(fn ->
      for peer <- [a, b], do: RaPeers.stop(peer)
      _ = :ra.force_delete_server(:default, server_id)
    end)

    %{a: a, b: b, vnode: vnode, nodes: nodes, server_id: server_id}
  end

  test "a restarted node's member stays down on its own, and the resume brings it back with its log", ctx do
    b = RaPeers.restart(ctx.b, nil, [])
    assert :erpc.call(b.node, Process, :whereis, [ctx.vnode]) == nil

    # Written while the member was down, so only a member that rejoined the group can hold it.
    {:ok, {:ok, _root}} = MetadataServer.command(ctx.server_id, {:create_topic, "while_down", 2})

    assert :erpc.call(b.node, Malachi.Application, :resume_local_vnodes, [
             [{ctx.vnode, 0, ctx.nodes}],
             b.node,
             [peers: ctx.nodes]
           ]) ==
             [{ctx.vnode, :ok}]

    assert RaPeers.eventually(fn ->
             state = RaPeers.local_state(b.node, ctx.vnode)
             Metadata.get_topic(state, "before") != nil and Metadata.get_topic(state, "while_down") != nil
           end)
  end

  test "the vnode coordinator manager, wired as the application wires it, brings a restarted node back", ctx do
    b = RaPeers.restart(ctx.b, nil, [])
    assert :erpc.call(b.node, Process, :whereis, [ctx.vnode]) == nil

    {:ok, ring} = HashRing.add_vnode(HashRing.new(), ctx.vnode, 0)
    topology = %RingTopology{version: 1, ring: ring, placements: %{ctx.vnode => ctx.nodes}}
    {:ok, _membership} = :erpc.call(b.node, Probe, :start_membership, [topology])
    {:ok, _manager} = :erpc.call(b.node, Probe, :start_manager, [self()])

    assert RaPeers.eventually(fn -> :erpc.call(b.node, Process, :whereis, [ctx.vnode]) != nil end)
  end

  test "a running member is left alone", ctx do
    assert :erpc.call(ctx.b.node, Malachi.Application, :resume_local_vnodes, [
             [{ctx.vnode, 0, ctx.nodes}],
             ctx.b.node,
             [peers: ctx.nodes]
           ]) ==
             []
  end

  test "a member the group removed is not brought back, although its server is still registered", ctx do
    # A member removed and only stopped, as a rebalance did before it deleted the members it removes (and as
    # a failed delete still leaves one): out of the consensus, still registered on its node.
    {:ok, _members, _leader} = :ra.remove_member(ctx.server_id, {ctx.vnode, ctx.b.node})
    :ok = :erpc.call(ctx.b.node, :ra, :stop_server, [:default, {ctx.vnode, ctx.b.node}])

    assert :erpc.call(ctx.b.node, Malachi.Application, :resume_local_vnodes, [
             [{ctx.vnode, 0, ctx.nodes}],
             ctx.b.node,
             [peers: ctx.nodes]
           ]) ==
             []

    assert :erpc.call(ctx.b.node, Process, :whereis, [ctx.vnode]) == nil
  end
end
