defmodule Malachi.Cluster.VnodeCoordinatorLivePlacementMultinodeTest do
  @moduledoc """
  A vnode gained after boot gets its coordinators, over real `ra` across real BEAM nodes (issue #217).

  The shape is the one a rebalance leaves behind: a vnode is formed on two nodes, a third is added as
  an `ra` member, and **no new ring is published**, because `Malachi.Cluster.Rebalance` moves members
  without touching the topology. The third node's ring therefore never names it as a host of that
  vnode. It must still run the vnode's coordinators once it leads it, without the application
  restarting, which is what the boot-time placement made impossible.

  `async: false` and tagged, like every test that starts peer nodes.
  """
  use ExUnit.Case, async: false

  @moduletag :multinode

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.Rebalance
  alias Malachi.Cluster.RingTopology
  alias Malachi.Cluster.VnodeCoordinatorManager, as: Manager
  alias Malachi.Test.RaPeers
  alias Malachi.Test.VnodeCoordinatorProbe, as: Probe

  setup_all do
    :ok = RaPeers.ensure_distribution()
    :ok
  end

  defp start_peer do
    name = :"malachi_vcm_#{System.unique_integer([:positive])}"
    {:ok, peer, node} = :peer.start_link(%{name: name, host: ~c"127.0.0.1", longnames: true})
    on_exit(fn -> try_stop(peer) end)

    :ok = :erpc.call(node, :code, :add_paths, [:code.get_path()])
    {:ok, _apps} = :erpc.call(node, :application, :ensure_all_started, [:logger])
    {:ok, _apps} = :erpc.call(node, :application, :ensure_all_started, [:telemetry])
    {:ok, _apps} = :erpc.call(node, :application, :ensure_all_started, [:ra])
    data_dir = ~c"#{System.tmp_dir!()}/malachi_ra_vcm_#{name}_#{System.unique_integer([:positive])}"
    {:ok, _pid} = :erpc.call(node, :ra, :start_in, [data_dir])
    on_exit(fn -> File.rm_rf("#{data_dir}") end)

    node
  end

  defp try_stop(peer) do
    :peer.stop(peer)
  catch
    _kind, _reason -> :ok
  end

  # The ring as the cluster published it: one vnode, placed on the two nodes that formed it.
  defp topology(vnode, nodes) do
    {:ok, ring} = HashRing.add_vnode(HashRing.new(), vnode, 0)
    RingTopology.new(ring, %{vnode => nodes})
  end

  test "a node added to a vnode's ra cluster runs its coordinators once it leads, with no restart" do
    node_a = start_peer()
    node_b = start_peer()
    node_c = start_peer()

    vnode = :"vn_#{System.unique_integer([:positive])}"
    {:ok, _server} = :erpc.call(node_a, MetadataServer, :start, [vnode, [node_a, node_b]])

    # Every node gossips the same ring: the vnode lives on a and b. node_c is not in it, and nothing in
    # this test ever publishes a ring that says otherwise.
    published = topology(vnode, [node_a, node_b])
    {:ok, _membership} = :erpc.call(node_c, Probe, :start_membership, [published])

    # node_c runs its coordinator manager from boot, over a ring that does not mention it.
    {:ok, manager} = :erpc.call(node_c, Probe, :start_manager, [self()])
    assert :erpc.call(node_c, Probe, :running, [manager]) == []
    refute_receive {:spawn, ^node_c, ^vnode}, 300

    # A rebalance adds node_c to the vnode's ra cluster. No ring is published: this is the whole point.
    assert Rebalance.ra_add_member(vnode, node_c, [node_a]) == :ok
    assert RaPeers.eventually(fn -> {:ok, _, _} = :ra.members({vnode, node_c}) end, 10_000)

    # Hand leadership to node_c, which is what makes it the vnode's coordinator.
    assert RaPeers.eventually(
             fn -> :ra.transfer_leadership({vnode, node_a}, {vnode, node_c}) in [:ok, :already_leader] end,
             10_000
           )

    # On the boot-time placement this never happens: node_c's ring still says the vnode is elsewhere.
    assert_receive {:spawn, ^node_c, ^vnode}, 10_000
    assert :erpc.call(node_c, Probe, :running, [manager]) == [vnode]

    # and it watches the machine version of the member it gained, which the boot list also missed
    statuses = :erpc.call(node_c, Manager, :version_status, [manager])
    assert Map.fetch(statuses, {vnode, node_c}) == {:ok, :ok}
  end
end
