defmodule Malachi.Cluster.ClusterFlagsMultinodeTest do
  @moduledoc """
  The scenario the whole change exists for, over real BEAM nodes: three peers gossiping SWIM
  membership and replicating the flag store, one of them advertising a reduced capability list.

  Switching the flag on is refused and **names** that peer. Once it advertises the full list, the same
  call is accepted and every member reads the flag as on. Before this change there was no way to
  express any of it: a node said nothing about what it supported, so nothing could wait for the cluster.
  """
  use ExUnit.Case, async: false

  @moduletag :multinode
  @moduletag timeout: 180_000

  alias Malachi.Cluster.Capabilities
  alias Malachi.Cluster.ClusterFlags
  alias Malachi.Cluster.ClusterFlagsMachine
  alias Malachi.Cluster.ClusterFlagsServer
  alias Malachi.Cluster.Membership
  alias Malachi.Cluster.MembershipServer
  alias Malachi.Cluster.RaCluster
  alias Malachi.Test.RaPeers

  @membership Malachi.LogMembership
  @cap :batch_format
  @known [@cap, :compaction]
  # Tight timings so the detector and the dissemination converge inside a test.
  @timings [protocol_period: 50, ack_timeout: 50, indirect_timeout: 50, suspicion_timeout: 1_000]

  setup_all do
    RaPeers.ensure_distribution()
  end

  # Three peers, each pinned to the machine version in `pins` (nil for none) and advertising the
  # capability list in `capabilities`, gossiping membership and replicating one flag store.
  defp start_cluster(pins, capabilities) do
    peers = Enum.map(pins, &RaPeers.start/1)
    on_exit(fn -> Enum.each(peers, &RaPeers.stop/1) end)

    nodes = Enum.map(peers, & &1.node)

    [nodes, capabilities]
    |> Enum.zip()
    |> Enum.each(fn {node, advertised} -> start_membership(node, nodes, advertised) end)

    cluster = :"flags_mn_#{System.unique_integer([:positive])}"
    {:ok, _member} = RaCluster.start(ClusterFlagsMachine, cluster, nodes)

    assert converged?(nodes), "membership did not converge: #{inspect(views(nodes))}"

    %{peers: peers, nodes: nodes, cluster: cluster, server_id: {cluster, hd(nodes)}}
  end

  defp start_membership(node, nodes, capabilities) do
    opts =
      [
        name: @membership,
        self_ref: {@membership, node},
        peers: for(peer <- nodes, peer != node, do: {@membership, peer}),
        attributes: Capabilities.attributes(%{"rack" => "a"}, capabilities)
      ] ++ @timings

    {:ok, _pid} = :erpc.call(node, MembershipServer, :start, [opts])
  end

  defp view(node), do: :erpc.call(node, MembershipServer, :view, [@membership])
  defp views(nodes), do: Enum.map(nodes, &view/1)

  # Every node knows every other as alive, and has learned its capability list through gossip.
  defp converged?(nodes) do
    RaPeers.eventually(fn ->
      Enum.all?(nodes, fn node ->
        view = view(node)
        Enum.all?(nodes, &(Membership.status(view, {@membership, &1}) == :alive))
      end)
    end)
  end

  # The membership view as `Capabilities.supported_by_all/3` wants it, read from one node. Which node
  # does not matter: gossip converges, and a view that is behind can only withhold a capability, never
  # invent one.
  defp reads(from_node) do
    view = view(from_node)

    fn node ->
      ref = {@membership, node}
      {Membership.status(view, ref), Membership.attributes(view, ref)}
    end
  end

  defp enabled_everywhere?(nodes, cluster, flag) do
    RaPeers.eventually(fn ->
      Enum.all?(nodes, fn node ->
        node |> RaPeers.local_state(cluster) |> ClusterFlags.enabled?(flag)
      end)
    end)
  end

  describe "a cluster on one build, one peer behind on capabilities" do
    setup do
      # The odd one out advertises nothing, which is exactly what a node still on the old build looks
      # like to its peers: the attribute key is simply absent or empty.
      start_cluster([nil, nil, nil], [@known, @known, []])
    end

    test "a peer advertising a reduced capability list is named, and blocks the flag", context do
      %{nodes: nodes, cluster: cluster, server_id: server_id} = context
      laggard = List.last(nodes)

      assert RaPeers.eventually(fn ->
               Capabilities.supported_by_all(nodes, @cap, reads(hd(nodes))) != :ok
             end)

      assert ClusterFlagsServer.enable(server_id, "batch_format", nodes, reads(hd(nodes)), @known) ==
               {:error, {:unsupported, [laggard]}}

      # A refused flip writes nothing: every member still reads the flag as off.
      for node <- nodes do
        refute node |> RaPeers.local_state(cluster) |> ClusterFlags.enabled?(@cap)
      end
    end

    test "once it advertises the full list the same call is accepted, and every member reads it on", context do
      %{nodes: nodes, cluster: cluster, server_id: server_id} = context
      laggard = List.last(nodes)

      assert ClusterFlagsServer.enable(server_id, "batch_format", nodes, reads(hd(nodes)), @known) ==
               {:error, {:unsupported, [laggard]}}

      # What an upgraded node does when it comes back: re-advertise, through the one function that owns
      # the merge, so the operator's own attributes survive alongside the capabilities. Going through
      # set_attributes/2 raises the member's incarnation, which is what makes the new advertisement win
      # the merge on every peer.
      :ok =
        :erpc.call(laggard, MembershipServer, :set_attributes, [
          @membership,
          Capabilities.attributes(%{"rack" => "a"}, @known)
        ])

      assert RaPeers.eventually(fn ->
               Capabilities.supported_by_all(nodes, @cap, reads(hd(nodes))) == :ok
             end),
             "the full capability list never reached the other peers: #{inspect(views(nodes))}"

      assert ClusterFlagsServer.enable(server_id, "batch_format", nodes, reads(hd(nodes)), @known) == :ok
      assert enabled_everywhere?(nodes, cluster, @cap)

      # The rack attribute the operator set is still there: the capability merge did not replace it.
      assert Membership.attributes(view(hd(nodes)), {@membership, laggard})["rack"] == "a"
    end

    test "the capability list crosses real nodes through gossip, inside the existing message shapes", context do
      %{nodes: nodes} = context
      [first, second, third] = nodes

      # Nothing in the SWIM payload grew a field: the list rides inside the attributes map that every
      # update already carried, which is why a member on an older build can pass it along untouched.
      assert RaPeers.eventually(fn ->
               Capabilities.of(Membership.attributes(view(first), {@membership, second})) == @known
             end)

      assert Capabilities.of(Membership.attributes(view(second), {@membership, first})) == @known
      assert Capabilities.of(Membership.attributes(view(first), {@membership, third})) == []
    end

    test "the flag is idempotent across members", context do
      %{nodes: nodes, cluster: cluster, server_id: server_id} = context
      laggard = List.last(nodes)

      :ok =
        :erpc.call(laggard, MembershipServer, :set_attributes, [
          @membership,
          Capabilities.attributes(%{}, @known)
        ])

      assert RaPeers.eventually(fn -> Capabilities.supported_by_all(nodes, @cap, reads(hd(nodes))) == :ok end)

      assert ClusterFlagsServer.enable(server_id, "batch_format", nodes, reads(hd(nodes)), @known) == :ok
      # A second operator, through a different member: the answer is the same and so is the state.
      assert ClusterFlagsServer.enable({cluster, List.last(nodes)}, "batch_format", nodes, reads(hd(nodes)), @known) ==
               :ok

      assert enabled_everywhere?(nodes, cluster, @cap)

      for node <- nodes do
        assert node |> RaPeers.local_state(cluster) |> ClusterFlags.enabled() == [@cap]
      end
    end
  end

  describe "a cluster mid-upgrade, one peer still on the older machine version" do
    setup do
      # Every peer advertises the capability, so the capability check passes. What holds the flag back
      # here is the other barrier: a peer pinned to machine version 1 keeps the group's effective version
      # at 1, and the command that switches a flag on was introduced at 2.
      start_cluster([nil, nil, 1], [@known, @known, @known])
    end

    test "the command is refused by the log even though every node advertises the capability", context do
      %{nodes: nodes, cluster: cluster, server_id: server_id} = context

      assert Capabilities.supported_by_all(nodes, @cap, reads(hd(nodes))) == :ok
      assert RaPeers.eventually(fn -> Enum.all?(nodes, &(RaPeers.effective(&1, cluster) == 1)) end)

      assert ClusterFlagsServer.enable(server_id, "batch_format", nodes, reads(hd(nodes)), @known) ==
               {:error, {:unsupported_command, {:enable_flag, 2}, 2, 1}}

      # Refused the same way on every replica, so no member switched the flag on behind the others.
      for node <- nodes do
        refute node |> RaPeers.local_state(cluster) |> ClusterFlags.enabled?(@cap)
      end
    end
  end

  describe "an unreachable store" do
    test "enabling answers the transport error rather than pretending the flag is on" do
      reads = fn _node -> {:alive, Capabilities.attributes(%{}, @known)} end

      assert {:error, _unreachable} =
               ClusterFlagsServer.enable({:flags_never_formed, node()}, "batch_format", [node()], reads, @known)
    end
  end
end
