defmodule Malachi.Consumer.CoordinatorRouterTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.HashRing
  alias Malachi.Consumer.CoordinatorRouter, as: Router

  @name Malachi.LogGroupCoordinator

  # A one-vnode ring: every topic routes to :v1 (deterministic, so tests can fix that vnode's leader).
  defp single_vnode_ring do
    {:ok, ring} = HashRing.add_vnode(HashRing.new(), :v1, 0)
    ring
  end

  describe "location/4 (pure routing decision)" do
    test "no topology (single-node/in-memory) is local, and never queries leadership" do
      assert Router.location("t", nil, :n1@h, fn _ -> flunk("leader_fn must not run") end) == :local
    end

    test "an empty ring falls back to local" do
      topo = %{ring: HashRing.new(), servers: %{}}
      assert Router.location("t", topo, :n1@h, fn _ -> :n2@h end) == :local
    end

    test "this node leading the topic's vnode is local" do
      topo = %{ring: single_vnode_ring(), servers: %{v1: {:meta_v1, :owner@h}}}
      assert Router.location("t", topo, :owner@h, fn {:meta_v1, _} -> :owner@h end) == :local
    end

    test "another node leading the topic's vnode is remote" do
      topo = %{ring: single_vnode_ring(), servers: %{v1: {:meta_v1, :owner@h}}}
      assert Router.location("t", topo, :me@h, fn {:meta_v1, _} -> :owner@h end) == {:remote, :owner@h}
    end

    test "the routed vnode absent from the server map falls back to local" do
      topo = %{ring: single_vnode_ring(), servers: %{}}
      assert Router.location("t", topo, :me@h, fn _ -> flunk("leader_fn must not run") end) == :local
    end

    test "an unresolved leader (nil) falls back to local" do
      topo = %{ring: single_vnode_ring(), servers: %{v1: {:meta_v1, :owner@h}}}
      assert Router.location("t", topo, :me@h, fn _ -> nil end) == :local
    end
  end

  describe "ref/2" do
    test "a local decision resolves to the bare coordinator name" do
      assert Router.ref(:local, @name) == @name
    end

    test "a remote decision resolves to {name, owner_node} (a cross-node GenServer ref)" do
      assert Router.ref({:remote, :n2@h}, @name) == {@name, :n2@h}
    end
  end

  describe "topology persistence" do
    test "put_topology/topology round-trips; teardown restores the unset (nil) state" do
      # mirrors the module's private @topology_key
      on_exit(fn -> :persistent_term.erase({Malachi.Consumer.CoordinatorRouter, :topology}) end)
      ring = single_vnode_ring()
      :ok = Router.put_topology(ring, %{v1: {:meta_v1, node()}})
      assert %{ring: ^ring, servers: %{v1: {:meta_v1, _}}} = Router.topology()
    end
  end

  describe "coordinator_name/2" do
    test "derives a stable per-vnode name from the base name and vnode id" do
      assert Router.coordinator_name(@name, :cluster_vn_3) == :"Elixir.Malachi.LogGroupCoordinator.cluster_vn_3"
      # routing and the boot wiring must agree, so the derivation is a pure function of its inputs
      assert Router.coordinator_name(@name, :cluster_vn_3) == Router.coordinator_name(@name, :cluster_vn_3)
    end
  end
end
