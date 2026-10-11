defmodule Malachi.Consumer.CoordinatorRouterTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.HashRing
  alias Malachi.Consumer.CoordinatorRouter, as: Router
  alias Malachi.Test.SilentRaMember

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

    test "a control plane that is one Raft group routes every topic to that group's leader" do
      topo = %{leader_of: {:log_meta, :member@h}}
      assert Router.location("t", topo, :owner@h, fn {:log_meta, _} -> :owner@h end) == :local
      assert Router.location("other", topo, :me@h, fn {:log_meta, _} -> :owner@h end) == {:remote, :owner@h}
      assert Router.location("t", topo, :me@h, fn _ -> nil end) == :local
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

  # The topology is node-wide (`:persistent_term`) and the running application published its own at boot:
  # a test that replaces it puts that one back, so suites run after it still see the boot topology.
  defp restore_topology_on_exit do
    key = {Malachi.Consumer.CoordinatorRouter, :topology}
    previous = :persistent_term.get(key, nil)

    on_exit(fn ->
      if previous, do: :persistent_term.put(key, previous), else: :persistent_term.erase(key)
    end)
  end

  describe "topology persistence" do
    test "put_topology/topology round-trips; teardown restores what was there" do
      restore_topology_on_exit()
      ring = single_vnode_ring()
      :ok = Router.put_topology(ring, %{v1: {:meta_v1, node()}})
      assert %{ring: ^ring, servers: %{v1: {:meta_v1, _}}} = Router.topology()
    end

    test "a leader that has to be asked of another node is asked briefly, and unanswered means local" do
      restore_topology_on_exit()
      # a member that never answers: this node hosts no member of the cluster, so its leader is not in ra's
      # local table and is asked over the wire, where it would cost ra's default 5 s
      silent = :"router_mute_#{System.unique_integer([:positive])}"
      {:ok, _pid} = SilentRaMember.start_link(silent)
      on_exit(fn -> SilentRaMember.stop(silent) end)
      :ok = Router.put_leader({silent, node()})

      {elapsed_us, owns?} = :timer.tc(fn -> Router.owns?("t") end)
      assert owns?
      assert elapsed_us < 1_000_000, "waited #{div(elapsed_us, 1000)} ms"
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
