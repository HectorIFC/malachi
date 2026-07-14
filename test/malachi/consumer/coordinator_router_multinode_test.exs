defmodule Malachi.Consumer.CoordinatorRouterMultinodeTest do
  # Real multi-node validation of the A1/A2 coordinator routing: spins up peer BEAM nodes, forms a vnode's
  # ra cluster across them, and checks that `CoordinatorRouter` tracks the *live* Raft leader — owns?/resolve
  # point at the leader, a group member forwarded there gets a disjoint assignment, a follower rejects
  # coordination (the guard), and after the leader dies the routing reconverges on the newly-elected leader.
  # async: false and tagged so it can be excluded where multi-node networking is unavailable.
  use ExUnit.Case, async: false

  @moduletag :multinode

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.Rebalance
  alias Malachi.Consumer.CoordinatorRouter
  alias Malachi.Consumer.CoordinatorRouterMultinodeFixtures, as: Fixtures
  alias Malachi.Consumer.GroupCoordinator

  @coord Malachi.LogGroupCoordinator

  setup_all do
    _ = System.cmd("epmd", ["-daemon"])

    case :net_kernel.start([:"malachi_primary@127.0.0.1", :longnames]) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    {:ok, _} = Application.ensure_all_started(:ra)
    :ok
  end

  defp start_peer do
    name = :"malachi_router_#{System.unique_integer([:positive])}"
    {:ok, peer, node} = :peer.start_link(%{name: name, host: ~c"127.0.0.1", longnames: true})
    on_exit(fn -> try_stop(peer) end)

    :ok = :erpc.call(node, :code, :add_paths, [:code.get_path()])
    # the coordinator's ranges_fun is &Fixtures.ranges/1 — make sure that module is loadable on the peer
    _ = :erpc.call(node, :code, :ensure_loaded, [Malachi.Consumer.CoordinatorRouterMultinodeFixtures])
    {:ok, _} = :erpc.call(node, :application, :ensure_all_started, [:ra])
    data_dir = ~c"#{System.tmp_dir!()}/malachi_ra_router_#{name}_#{System.unique_integer([:positive])}"
    {:ok, _} = :erpc.call(node, :ra, :start_in, [data_dir])

    {peer, node}
  end

  defp try_stop(peer) do
    :peer.stop(peer)
  catch
    _kind, _reason -> :ok
  end

  # ra needs a moment after a membership/leadership change; retry until `fun` returns {:ok, _}.
  defp retry(fun, remaining_ms \\ 5_000) do
    case fun.() do
      {:ok, _value} = ok -> ok
      _other when remaining_ms > 0 -> Process.sleep(100) && retry(fun, remaining_ms - 100)
      other -> other
    end
  end

  # The current leader, queried through a member whose server stays up for the whole test (`probe`).
  defp leader_via(vnode, probe) do
    retry(fn ->
      case :ra.members({vnode, probe}) do
        {:ok, _members, {^vnode, leader}} -> {:ok, leader}
        _not_ready -> :not_ready
      end
    end)
  end

  defp coordinator_opts do
    [
      ranges_fun: &Fixtures.ranges/1,
      owns_fun: &CoordinatorRouter.owns?/1,
      tick_ms: 3_600_000
    ]
  end

  # Start an unlinked coordinator registered under `name` on `node` (GenServer.start, not start_link — the
  # erpc worker that runs it exits, and an unlinked child survives that).
  defp start_coordinator_on(node, name) do
    {:ok, _} = :erpc.call(node, GenServer, :start, [GroupCoordinator, coordinator_opts(), [name: name]])
  end

  test "coordinator routing tracks the live Raft leader across nodes, and reconverges after failover" do
    {_pa, node_a} = start_peer()
    {_pb, node_b} = start_peer()
    {_pc, node_c} = start_peer()
    nodes = [node_a, node_b, node_c]
    vnode = :"vn_#{System.unique_integer([:positive])}"

    # a 3-node vnode ra cluster (quorum 2 → tolerates one failure for the failover check below)
    {:ok, _} = :erpc.call(node_a, MetadataServer, :start, [vnode, [node_a]])
    assert Rebalance.ra_add_member(vnode, node_b, [node_a]) == :ok
    assert Rebalance.ra_add_member(vnode, node_c, [node_a, node_b]) == :ok

    # Determine the leader once (nothing triggers a re-election until we kill it below), then pick a
    # `probe` follower we never kill, so `:ra.members({vnode, probe})` resolves the live leader for the
    # whole test — including after the leader dies. Route the topology's server id through the probe.
    {:ok, leader} = leader_via(vnode, node_a)
    followers = nodes -- [leader]
    probe = hd(followers)

    # the routing topology every broker consults: "t" hashes to this vnode; the server id lets ra resolve
    # the live leader. Publish it on each node (as the sharded control plane does at boot).
    {:ok, ring} = HashRing.add_vnode(HashRing.new(), vnode, 0)
    servers = %{vnode => {vnode, probe}}
    for n <- nodes, do: :ok = :erpc.call(n, CoordinatorRouter, :put_topology, [ring, servers])

    # the topic's vnode owns a per-vnode coordinator on its leader; routing resolves to that name
    coord_name = CoordinatorRouter.coordinator_name(@coord, vnode)

    # owns?/resolve agree on the leader: the leader owns "t" (resolve → the bare per-vnode name), each
    # follower does not (resolve → forward to the leader)
    assert :erpc.call(leader, CoordinatorRouter, :owns?, ["t"]) == true
    assert :erpc.call(leader, CoordinatorRouter, :resolve, [@coord, "t"]) == coord_name

    for f <- followers do
      assert :erpc.call(f, CoordinatorRouter, :owns?, ["t"]) == false
      assert :erpc.call(f, CoordinatorRouter, :resolve, [@coord, "t"]) == {coord_name, leader}
    end

    # a coordinator on the leader; two members forwarded there (from this primary node) partition the
    # ranges disjointly — the cross-node forwarding + single-authority invariant, end to end
    start_coordinator_on(leader, coord_name)

    ref = {coord_name, leader}
    {:ok, _, _} = GroupCoordinator.poll(ref, "g", "t", :m1)
    {:ok, _, _} = GroupCoordinator.poll(ref, "g", "t", :m2)
    {:ok, _, r1} = GroupCoordinator.poll(ref, "g", "t", :m1)
    {:ok, _, r2} = GroupCoordinator.poll(ref, "g", "t", :m2)
    assert Enum.sort(r1 ++ r2) == [:r0, :r1]
    assert r1 -- r2 == r1

    # a follower must not accept coordination for "t" (defense against stale routing): the guard rejects
    guard_follower = hd(followers)
    start_coordinator_on(guard_follower, coord_name)
    assert GroupCoordinator.poll({coord_name, guard_follower}, "g", "t", :mx) == {:error, :not_owner}

    # failover: the leader dies; the remaining two elect a new leader, and routing reconverges on it
    :ok = :erpc.call(leader, :ra, :stop_server, [:default, {vnode, leader}])

    {:ok, new_leader} =
      retry(fn ->
        case :ra.members({vnode, probe}) do
          {:ok, _members, {^vnode, l}} when l != leader -> {:ok, l}
          _other -> :not_ready
        end
      end)

    assert new_leader != leader
    assert :erpc.call(new_leader, CoordinatorRouter, :owns?, ["t"]) == true
    # resolve on the new leader now returns the bare per-vnode name (local) — members reconverge here
    assert :erpc.call(new_leader, CoordinatorRouter, :resolve, [@coord, "t"]) == coord_name
  end
end
