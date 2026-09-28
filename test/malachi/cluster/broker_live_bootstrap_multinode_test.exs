defmodule Malachi.Cluster.BrokerLiveBootstrapMultinodeTest do
  @moduledoc """
  A vnode added to the ring after boot is bootstrapped by the broker that adopts it through gossip, over
  real `ra` across real BEAM nodes (issue #242).

  The broker used to bootstrap from the vnode list it booted with, and a ring change adopted from
  gossip only replaced its routing view. So the orchestrator routed to a vnode a split had added and
  never formed it, until it restarted. Here the new ring reaches the broker the way it does in
  production: a peer's membership server holds it, SWIM gossip carries it, and the membership hook casts
  it to the broker, as `Malachi.Application` wires it.

  `async: false` and tagged, like every test that starts peer nodes.
  """
  use ExUnit.Case, async: false

  @moduletag :multinode

  alias Malachi.BrokerServer
  alias Malachi.Cluster.DSRSM
  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MembershipServer
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.RingTopology
  alias Malachi.Metadata
  alias Malachi.Test.Distribution
  alias Malachi.Test.RaPeers
  alias Malachi.Test.TmpDir
  alias Malachi.Test.VnodeCoordinatorProbe, as: Probe

  import Malachi.Test.TeardownHelper

  @half div(Integer.pow(2, 32), 2)

  setup_all do
    :ok = RaPeers.ensure_distribution()
    :ok
  end

  defp start_peer do
    {_peer, node, name} = Distribution.start_peer("blb")
    {:ok, _apps} = :erpc.call(node, :application, :ensure_all_started, [:logger])
    {:ok, _apps} = :erpc.call(node, :application, :ensure_all_started, [:telemetry])
    {:ok, _apps} = :erpc.call(node, :application, :ensure_all_started, [:ra])
    data_dir = String.to_charlist(TmpDir.path("malachi_ra_blb_#{name}"))
    {:ok, _pid} = :erpc.call(node, :ra, :start_in, [data_dir])
    on_exit(fn -> File.rm_rf("#{data_dir}") end)

    node
  end

  defp topology(version, vnodes) do
    ring =
      Enum.reduce(vnodes, HashRing.new(), fn {vnode_id, token, _nodes}, ring ->
        {:ok, ring} = HashRing.add_vnode(ring, vnode_id, token)
        ring
      end)

    %RingTopology{
      version: version,
      ring: ring,
      placements: Map.new(vnodes, fn {vnode_id, _token, nodes} -> {vnode_id, nodes} end)
    }
  end

  # A topic name that routes to `vnode` on `ring`.
  defp topic_on(ring, vnode) do
    Enum.find(
      Stream.map(0..200, &"adopted_#{&1}"),
      fn name -> match?({:ok, ^vnode}, DSRSM.vnode_for(%DSRSM{ring: ring, vnodes: %{}}, name)) end
    )
  end

  test "a vnode a split added is bootstrapped by the broker that adopts the ring through gossip" do
    node_b = start_peer()
    node_c = start_peer()

    suffix = System.unique_integer([:positive])
    booted = :"blb_boot_#{suffix}"
    added = :"blb_new_#{suffix}"
    on_exit(fn -> MetadataServer.delete({booted, node()}) end)

    boot_vnodes = [{booted, 0, [node()]}]
    grown = topology(1, boot_vnodes ++ [{added, @half, [node_b, node_c]}])

    directory = TmpDir.path("malachi_blb_repl")
    on_exit(fn -> File.rm_rf!(directory) end)
    replication = start_supervised!({ReplicationServer, directory: directory})

    # This node is the orchestrator and boots knowing only the first vnode, like a node that was up
    # before the split.
    {:ok, broker} =
      BrokerServer.start_link("unused",
        brokers: [replication],
        metadata_vnodes: boot_vnodes,
        bootstrap_orchestrator: fn -> true end,
        brokers_refresh_interval: 60_000
      )

    on_exit(fn -> stop_quietly(broker) end)
    assert RaPeers.eventually(fn -> MetadataServer.ready?({booted, node()}, 500) end)

    # node_b gossips the ring that carries the split. This node joins it, and its membership hook hands
    # every newer ring to the broker, which is what `Malachi.Application.adopt_ring_topology/1` does.
    {:ok, _membership} = :erpc.call(node_b, Probe, :start_membership, [topology(0, boot_vnodes)])
    name = :"blb_membership_#{suffix}"

    {:ok, membership} =
      MembershipServer.start_link(
        name: name,
        self_ref: {name, node()},
        peers: [{Malachi.LogMembership, node_b}],
        topology: topology(0, boot_vnodes),
        protocol_period: 100,
        on_topology: fn topology -> GenServer.cast(broker, {:adopt_topology, topology}) end
      )

    on_exit(fn -> stop_quietly(membership) end)

    refute MetadataServer.ready?({added, node_b}, 200)
    :ok = :erpc.call(node_b, MembershipServer, :set_topology, [Malachi.LogMembership, grown])

    assert RaPeers.eventually(fn -> MembershipServer.topology(membership) == grown end),
           "gossip never carried the grown ring to this node"

    # The broker routes to the new vnode as soon as it adopts the ring. Before #242 it never formed it.
    assert RaPeers.eventually(fn ->
             :ok = BrokerServer.reconcile_now(broker)
             MetadataServer.ready?({added, node_b}, 500)
           end),
           "the orchestrator routes to the vnode the split added but never bootstrapped it"

    assert MetadataServer.ready?({added, node_c}, 2_000)

    # And a write routed to it commits in its Raft group, on the nodes the ring placed it on.
    topic = topic_on(grown.ring, added)
    assert {:ok, _root} = BrokerServer.create_topic(broker, topic, 4)
    assert {:ok, %{name: ^topic}} = MetadataServer.query({added, node_b}, &Metadata.get_topic(&1, topic))
  end
end
