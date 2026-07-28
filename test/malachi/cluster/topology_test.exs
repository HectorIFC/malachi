defmodule Malachi.Cluster.TopologyTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.Topology

  describe "build/1: no strategy" do
    test "nil strategy returns an empty topology list (single-node default)" do
      assert Topology.build(%{strategy: nil}) == []
    end

    test "a config map without a :strategy key returns []" do
      assert Topology.build(%{}) == []
    end
  end

  describe "build/1: gossip" do
    test "defaults the port and omits optional secret/multicast_addr" do
      assert [malachi: [strategy: Cluster.Strategy.Gossip, config: config]] =
               Topology.build(%{strategy: :gossip})

      assert config == [port: 45_892]
    end

    test "carries a custom port, secret and multicast address" do
      assert [malachi: [strategy: Cluster.Strategy.Gossip, config: config]] =
               Topology.build(%{
                 strategy: :gossip,
                 gossip_port: 46_000,
                 gossip_secret: "s3cr3t",
                 gossip_multicast_addr: "230.1.1.1"
               })

      assert config[:port] == 46_000
      assert config[:secret] == "s3cr3t"
      assert config[:multicast_addr] == "230.1.1.1"
    end

    test "treats an empty-string secret as absent" do
      assert [malachi: [strategy: Cluster.Strategy.Gossip, config: config]] =
               Topology.build(%{strategy: :gossip, gossip_secret: "", gossip_multicast_addr: ""})

      refute Keyword.has_key?(config, :secret)
      refute Keyword.has_key?(config, :multicast_addr)
    end
  end

  describe "build/1: kubernetes" do
    test "builds selector/basename/mode and defaults the polling interval" do
      assert [malachi: [strategy: Cluster.Strategy.Kubernetes, config: config]] =
               Topology.build(%{
                 strategy: :kubernetes,
                 kubernetes_selector: "app=malachi",
                 kubernetes_node_basename: "malachi",
                 kubernetes_mode: :dns
               })

      assert config[:kubernetes_selector] == "app=malachi"
      assert config[:kubernetes_node_basename] == "malachi"
      assert config[:mode] == :dns
      assert config[:polling_interval] == 10_000
    end

    test "includes namespace when set and defaults mode to :hostname" do
      assert [malachi: [strategy: Cluster.Strategy.Kubernetes, config: config]] =
               Topology.build(%{
                 strategy: :kubernetes,
                 kubernetes_selector: "app=malachi",
                 kubernetes_node_basename: "malachi",
                 kubernetes_namespace: "prod"
               })

      assert config[:mode] == :hostname
      assert config[:kubernetes_namespace] == "prod"
    end

    test "raises when the selector is missing" do
      assert_raise ArgumentError, ~r/requires kubernetes_selector/, fn ->
        Topology.build(%{strategy: :kubernetes, kubernetes_node_basename: "malachi"})
      end
    end

    test "raises when the node basename is missing" do
      assert_raise ArgumentError, ~r/requires kubernetes_node_basename/, fn ->
        Topology.build(%{strategy: :kubernetes, kubernetes_selector: "app=malachi"})
      end
    end
  end

  describe "build/1: epmd" do
    test "uses the static host list" do
      hosts = [:malachi@a, :malachi@b]

      assert [malachi: [strategy: Cluster.Strategy.Epmd, config: config]] =
               Topology.build(%{strategy: :epmd, epmd_hosts: hosts})

      assert config == [hosts: hosts]
    end

    test "raises on an empty host list" do
      assert_raise ArgumentError, ~r/non-empty host list/, fn ->
        Topology.build(%{strategy: :epmd, epmd_hosts: []})
      end
    end
  end

  describe "build/1: invalid" do
    test "raises on an unknown strategy" do
      assert_raise ArgumentError, ~r/unknown cluster strategy/, fn ->
        Topology.build(%{strategy: :consul})
      end
    end
  end
end
