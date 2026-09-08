defmodule Malachi.Cluster.RaClusterTest do
  # async: false: ra is global and stateful (one data dir, on-disk Raft logs).
  @moduledoc """
  The shared `ra` lifecycle, exercised through `Malachi.Cluster.RingMachine`. The point worth pinning
  is resume-first: a name this node has already started must be **restarted**, never re-formed over,
  which is what keeps a returning member from coming back amnesiac.
  """
  use ExUnit.Case, async: false

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.RaCluster
  alias Malachi.Cluster.Ring
  alias Malachi.Cluster.RingMachine
  alias Malachi.Cluster.RingTopology

  defp start_cluster do
    name = :"racluster_#{System.unique_integer([:positive])}"
    {:ok, server_id} = RaCluster.start(RingMachine, name, [node()])
    on_exit(fn -> RaCluster.delete(name) end)
    {name, server_id}
  end

  defp seed_topology do
    {:ok, ring} = HashRing.add_vnode(HashRing.new(), :vn_a, 0)
    RingTopology.new(ring, %{vn_a: [node()]})
  end

  test "starts a cluster and addresses it through a real member" do
    {name, server_id} = start_cluster()

    assert server_id == {name, node()}
    assert RaCluster.ready?(server_id)
    assert RaCluster.leader?(server_id)
  end

  test "member_node/1 prefers the local node, else the first placement node" do
    remote = :"other@127.0.0.1"

    assert RaCluster.member_node([remote, node()]) == node()
    assert RaCluster.member_node([remote]) == remote
  end

  test "commands go through the log and the state reads back linearizably" do
    {_name, server_id} = start_cluster()
    topology = seed_topology()

    assert RaCluster.command(server_id, {:init, topology}) == {:ok, :ok}
    assert {:ok, %Ring{} = state} = RaCluster.query(server_id)
    assert Ring.topology(state) == {:ok, topology}
  end

  test "a machine refusal comes back as a reply, not as an unreachable-cluster error" do
    {_name, server_id} = start_cluster()
    {:ok, :ok} = RaCluster.command(server_id, {:init, seed_topology()})

    assert {:ok, {:error, {:exists, _stored}}} = RaCluster.command(server_id, {:init, seed_topology()})
  end

  test "local_query/2 projects the local replica's state" do
    {_name, server_id} = start_cluster()
    {:ok, :ok} = RaCluster.command(server_id, {:init, seed_topology()})

    assert {:ok, {:ok, _topology}} = RaCluster.local_query(server_id, &Ring.topology/1)
  end

  test "start/3 resumes a member it has already started instead of forming an empty one over it" do
    {name, server_id} = start_cluster()
    {:ok, :ok} = RaCluster.command(server_id, {:init, seed_topology()})

    :ok = :ra.stop_server(:default, server_id)
    assert {:ok, ^server_id} = RaCluster.start(RingMachine, name, [node()])

    assert {:ok, %Ring{} = state} = RaCluster.query(server_id)
    assert match?({:ok, _topology}, Ring.topology(state)), "a resumed member must keep its history"
  end

  test "reconcile/3 is a no-op once the local server is up" do
    {name, server_id} = start_cluster()
    {:ok, :ok} = RaCluster.command(server_id, {:init, seed_topology()})

    assert RaCluster.reconcile(RingMachine, name, [node()]) == :ok
    assert {:ok, %Ring{} = state} = RaCluster.query(server_id)
    assert match?({:ok, _topology}, Ring.topology(state))
  end

  test "reconcile/3 forms the cluster when this node has not joined yet" do
    name = :"racluster_join_#{System.unique_integer([:positive])}"
    on_exit(fn -> RaCluster.delete(name) end)

    assert RaCluster.reconcile(RingMachine, name, [node()]) == :ok
    assert RaCluster.ready?({name, node()})
  end

  test "command/2 and query/1 report an unreachable cluster rather than blocking" do
    absent = {:"racluster_gone_#{System.unique_integer([:positive])}", node()}

    assert {:error, _reason} = RaCluster.command(absent, {:init, seed_topology()})
    assert {:error, _reason} = RaCluster.query(absent)
    assert {:error, _reason} = RaCluster.local_query(absent, &Ring.topology/1)
  end

  test "delete/1 accepts a bare cluster name as the local member, and reports a cluster that is not there" do
    name = :"racluster_del_#{System.unique_integer([:positive])}"
    {:ok, server_id} = RaCluster.start(RingMachine, name, [node()])
    {:ok, :ok} = RaCluster.command(server_id, {:init, seed_topology()})

    assert RaCluster.delete(name) == :ok
    refute RaCluster.ready?(server_id)

    assert {:error, _reason} = RaCluster.delete(:"racluster_never_#{System.unique_integer([:positive])}")
  end

  test "ready?/1 and leader?/1 answer false for a cluster that does not exist" do
    absent = {:"racluster_absent_#{System.unique_integer([:positive])}", node()}

    refute RaCluster.ready?(absent)
    refute RaCluster.leader?(absent)
  end
end
