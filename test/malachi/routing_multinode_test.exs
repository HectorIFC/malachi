defmodule Malachi.RoutingMultinodeTest do
  @moduledoc """
  The routing state over real BEAM nodes: three peers gossiping SWIM membership, each advertising the
  address clients reach it at, one of them on a build without `producer_streams`.

  Every node's view yields the same cluster state, version included, so a client can ask any of them;
  and the flag that switches the stream keys on is refused while one node cannot serve them.
  """
  use ExUnit.Case, async: false

  @moduletag :multinode
  @moduletag timeout: 180_000

  alias Malachi.Cluster.Advertised
  alias Malachi.Cluster.Capabilities
  alias Malachi.Cluster.ClusterFlagsMachine
  alias Malachi.Cluster.ClusterFlagsServer
  alias Malachi.Cluster.Membership
  alias Malachi.Cluster.MembershipServer
  alias Malachi.Cluster.RaCluster
  alias Malachi.Routing
  alias Malachi.Test.RaPeers

  @membership Malachi.LogMembership
  @timings [protocol_period: 50, ack_timeout: 50, indirect_timeout: 50, suspicion_timeout: 1_000]

  setup_all do
    RaPeers.ensure_distribution()
  end

  setup do
    peers = Enum.map([nil, nil, nil], &RaPeers.start/1)
    on_exit(fn -> Enum.each(peers, &RaPeers.stop/1) end)
    nodes = Enum.map(peers, & &1.node)
    [new_a, new_b, old] = nodes

    addresses =
      for {node, i} <- Enum.with_index(nodes), into: %{}, do: {node, %{host: "broker-#{i}.svc", port: 4040 + i}}

    capabilities = %{new_a => [Routing.flag()], new_b => [Routing.flag()], old => []}

    for node <- nodes do
      attributes = %{"rack" => "a"} |> Capabilities.attributes(capabilities[node]) |> Advertised.put(addresses[node])

      opts =
        [
          name: @membership,
          self_ref: {@membership, node},
          peers: for(p <- nodes, p != node, do: {@membership, p}),
          attributes: attributes
        ] ++
          @timings

      {:ok, _pid} = :erpc.call(node, MembershipServer, :start, [opts])
    end

    assert RaPeers.eventually(fn -> Enum.all?(nodes, &converged?(&1, nodes, addresses)) end)
    %{nodes: nodes, old: old, addresses: addresses}
  end

  defp view(node), do: :erpc.call(node, MembershipServer, :view, [@membership])

  # Every member alive and its address learned through gossip.
  defp converged?(node, nodes, addresses) do
    view = view(node)

    Enum.all?(nodes, fn peer ->
      Membership.status(view, {@membership, peer}) == :alive and
        Advertised.of(Membership.attributes(view, {@membership, peer})) == addresses[peer]
    end)
  end

  test "every node's view gives the same cluster state, naming each broker at its advertised address", ctx do
    states =
      for node <- ctx.nodes, do: :erpc.call(node, Routing, :cluster_state, [Routing.members_of(view(node)), [0], false])

    assert [state] = Enum.uniq(states)

    assert state.brokers ==
             ctx.nodes
             |> Enum.map(
               &%{id: Atom.to_string(&1), host: ctx.addresses[&1].host, port: ctx.addresses[&1].port, status: :alive}
             )
             |> Enum.sort_by(& &1.id)
  end

  test "each node reads that same cluster state for itself, ring and flag included", ctx do
    # Each peer has a control plane, as a clustered node does (a node without one answers for itself).
    for node <- ctx.nodes, do: :ok = :erpc.call(node, Application, :put_env, [:malachi, :log_cluster, :routing_mn])

    assert [{:ok, state}] = ctx.nodes |> Enum.map(&:erpc.call(&1, Routing, :read_cluster_state, [])) |> Enum.uniq()
    assert length(state.brokers) == 3
    assert state.vnodes == [0]
    refute state.streams_enabled
  end

  test "producer_streams cannot be switched on while a node does not advertise it, and that node is named", ctx do
    cluster = :"routing_mn_#{System.unique_integer([:positive])}"
    {:ok, _member} = RaCluster.start(ClusterFlagsMachine, cluster, ctx.nodes)
    view = view(hd(ctx.nodes))

    reads = fn node ->
      {Membership.status(view, {@membership, node}), Membership.attributes(view, {@membership, node})}
    end

    assert ClusterFlagsServer.enable({cluster, hd(ctx.nodes)}, "producer_streams", ctx.nodes, reads) ==
             {:error, {:unsupported, [ctx.old]}}
  end
end
