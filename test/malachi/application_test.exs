defmodule Malachi.ApplicationTest do
  use ExUnit.Case, async: true

  alias Malachi.Application, as: App

  describe "metadata_cluster_opts/2" do
    test "no cluster configured yields no metadata options (single-node in-memory default)" do
      assert App.metadata_cluster_opts(nil, [:"a@127.0.0.1"]) == []
    end

    test "a configured cluster yields the ra-backed control-plane options" do
      opts = App.metadata_cluster_opts(:log_meta, [:"a@127.0.0.1", :"b@127.0.0.1"])

      assert Keyword.fetch!(opts, :metadata_cluster) == :log_meta
      assert Keyword.fetch!(opts, :metadata_nodes) == [:"a@127.0.0.1", :"b@127.0.0.1"]
    end
  end

  describe "sharded_vnodes/2" do
    test "names vnodes from the base and spreads distinct tokens evenly over the 32-bit ring" do
      vnodes = App.sharded_vnodes(:log_meta, 4)
      ring_size = Integer.pow(2, 32)

      assert vnodes == [
               {:log_meta_vn_0, 0},
               {:log_meta_vn_1, div(ring_size, 4)},
               {:log_meta_vn_2, div(ring_size, 2)},
               {:log_meta_vn_3, div(3 * ring_size, 4)}
             ]

      # tokens are distinct and in range (the ring rejects duplicates / out-of-range placement)
      tokens = Enum.map(vnodes, &elem(&1, 1))
      assert length(Enum.uniq(tokens)) == 4
      assert Enum.all?(tokens, &(&1 >= 0 and &1 < ring_size))
    end

    test "a single vnode sits at token 0" do
      assert App.sharded_vnodes(:log_meta, 1) == [{:log_meta_vn_0, 0}]
    end
  end

  describe "place_vnodes/3" do
    test "assigns each vnode replication_factor nodes by HRW, carrying the token" do
      vnodes = App.sharded_vnodes(:log_meta, 4)
      nodes = [:a@h, :b@h, :c@h, :d@h, :e@h]

      placed = App.place_vnodes(vnodes, nodes, 3)

      # every vnode keeps its {id, token} and gains exactly 3 distinct nodes drawn from the set
      assert length(placed) == 4

      for {{vnode_id, token}, {p_id, p_token, chosen}} <- Enum.zip(vnodes, placed) do
        assert {p_id, p_token} == {vnode_id, token}
        assert length(chosen) == 3
        assert length(Enum.uniq(chosen)) == 3
        assert Enum.all?(chosen, &(&1 in nodes))
      end

      # HRW spreads the vnodes' primaries rather than piling them on one node
      primaries = Enum.map(placed, fn {_id, _token, [primary | _]} -> primary end)
      assert length(Enum.uniq(primaries)) > 1, "expected vnode primaries to spread across nodes"
    end

    test "is deterministic: the same inputs yield the same placement" do
      vnodes = App.sharded_vnodes(:log_meta, 3)
      nodes = [:a@h, :b@h, :c@h]
      assert App.place_vnodes(vnodes, nodes, 2) == App.place_vnodes(vnodes, nodes, 2)
    end

    test "clamps the replica count to the number of nodes" do
      vnodes = App.sharded_vnodes(:log_meta, 2)
      nodes = [:a@h, :b@h]

      for {_id, _token, chosen} <- App.place_vnodes(vnodes, nodes, 3) do
        assert Enum.sort(chosen) == [:a@h, :b@h]
      end
    end

    test "with a topology, each vnode's replicas land in distinct racks (A1)" do
      vnodes = App.sharded_vnodes(:log_meta, 6)
      nodes = [:a1@h, :a2@h, :b1@h, :b2@h]

      attrs = %{
        :a1@h => %{"rack" => "a"},
        :a2@h => %{"rack" => "a"},
        :b1@h => %{"rack" => "b"},
        :b2@h => %{"rack" => "b"}
      }

      placed = App.place_vnodes(vnodes, nodes, 2, spread: {"rack", attrs})

      # every vnode's 2 replicas span both racks — a whole rack can fail without losing a majority
      for {_id, _token, chosen} <- placed do
        racks = Enum.map(chosen, fn n -> attrs[n]["rack"] end)
        assert Enum.sort(racks) == ["a", "b"]
      end

      # still deterministic
      assert App.place_vnodes(vnodes, nodes, 2, spread: {"rack", attrs}) == placed
    end
  end

  describe "desired_placement/5" do
    test "places every vnode over the given live nodes, deterministically" do
      nodes = [:a@h, :b@h, :c@h]
      placement = App.desired_placement(:log_meta, 6, nodes, 2)

      assert placement == App.desired_placement(:log_meta, 6, nodes, 2)
      assert length(placement) == 6

      for {_id, _token, chosen} <- placement do
        assert length(chosen) == 2
        assert Enum.all?(chosen, &(&1 in nodes))
      end
    end

    test "adding a node re-places only vnodes that adopt it (minimal movement)" do
      before = App.desired_placement(:log_meta, 12, [:a@h, :b@h, :c@h], 2)
      after_join = App.desired_placement(:log_meta, 12, [:a@h, :b@h, :c@h, :d@h], 2)

      # a vnode's replica set changes only if it took on the new node; everything else stays put
      for {{id, _t, ns_before}, {id, _t2, ns_after}} <- Enum.zip(before, after_join),
          ns_after != ns_before do
        assert :d@h in ns_after
      end

      # and the join is not a no-op: some vnode actually adopts the new node
      assert Enum.any?(after_join, fn {_id, _t, ns} -> :d@h in ns end)
    end

    test "removing a node re-places only vnodes that held it (minimal movement)" do
      before = App.desired_placement(:log_meta, 12, [:a@h, :b@h, :c@h, :d@h], 2)
      after_leave = App.desired_placement(:log_meta, 12, [:a@h, :b@h, :c@h], 2)

      for {{id, _t, ns_before}, {id, _t2, ns_after}} <- Enum.zip(before, after_leave),
          ns_after != ns_before do
        assert :d@h in ns_before
      end

      # the departed node no longer appears anywhere
      refute Enum.any?(after_leave, fn {_id, _t, ns} -> :d@h in ns end)
    end

    test "clamps replicas to the number of live nodes" do
      for {_id, _token, chosen} <- App.desired_placement(:log_meta, 4, [:a@h, :b@h], 3) do
        assert Enum.sort(chosen) == [:a@h, :b@h]
      end
    end
  end

  describe "rebalance_plan/2" do
    test "is empty when the current placement already matches the desired one" do
      placement = [{:vn_0, 0, [:a@h, :b@h]}, {:vn_1, 1, [:b@h, :c@h]}]
      assert App.rebalance_plan(placement, placement) == []
    end

    test "yields add/remove per changed vnode and omits unchanged ones" do
      current = [{:vn_0, 0, [:a@h, :b@h]}, {:vn_1, 1, [:b@h, :c@h]}]
      desired = [{:vn_0, 0, [:a@h, :d@h]}, {:vn_1, 1, [:b@h, :c@h]}]

      assert App.rebalance_plan(current, desired) ==
               [%{vnode_id: :vn_0, add: [:d@h], remove: [:b@h]}]
    end

    test "a join keeps every vnode's replica count constant (add and remove balance, so add-before-remove never drops below quorum)" do
      current = App.desired_placement(:log_meta, 12, [:a@h, :b@h, :c@h], 2)
      desired = App.desired_placement(:log_meta, 12, [:a@h, :b@h, :c@h, :d@h], 2)
      plan = App.rebalance_plan(current, desired)

      refute plan == []
      for %{add: add, remove: remove} <- plan, do: assert(length(add) == length(remove))
    end

    test "a leave plans to add the same node it removes for exactly the vnodes that held the departed node" do
      current = App.desired_placement(:log_meta, 12, [:a@h, :b@h, :c@h, :d@h], 2)
      desired = App.desired_placement(:log_meta, 12, [:a@h, :b@h, :c@h], 2)
      plan = App.rebalance_plan(current, desired)

      # every planned vnode is one that held the departed node
      for %{remove: remove} <- plan, do: assert(:d@h in remove)
      # and no planned change re-adds the departed node
      for %{add: add} <- plan, do: refute(:d@h in add)

      changed_ids = Enum.map(plan, & &1.vnode_id)
      assert changed_ids == Enum.uniq(changed_ids)
    end
  end

  describe "leading_vnodes/3" do
    test "returns the vnodes this node both hosts and leads, preserving order" do
      this = :n1@h

      vnodes = [
        {:vn_a, 0, [:n1@h, :n2@h]},
        {:vn_b, 1, [:n2@h, :n3@h]},
        {:vn_c, 2, [:n1@h, :n3@h]},
        {:vn_d, 3, [:n1@h]}
      ]

      # this node leads its local server for vn_a and vn_d only
      leader? = fn server_id -> server_id in [{:vn_a, this}, {:vn_d, this}] end

      assert App.leading_vnodes(vnodes, this, leader?) == [:vn_a, :vn_d]
    end

    test "excludes a vnode this node hosts but does not lead" do
      this = :n1@h
      assert App.leading_vnodes([{:vn_a, 0, [:n1@h, :n2@h]}], this, fn _ -> false end) == []
    end

    test "never even queries leadership for a vnode this node does not host" do
      this = :n1@h
      leader? = fn _ -> flunk("must not query leadership for a non-hosted vnode") end
      assert App.leading_vnodes([{:vn_b, 0, [:n2@h, :n3@h]}], this, leader?) == []
    end
  end

  describe "static_seed/1" do
    test "self is the orchestrator only when it is the lowest-sorted node" do
      higher = :zzzz_higher@h
      lower = :"0000_lower@h"

      assert App.static_seed([node()]).()
      assert App.static_seed([node(), higher]).()
      refute App.static_seed([lower, node()]).()
      # order-independent: still the same seed regardless of input order
      refute App.static_seed([higher, lower, node()]).()
    end
  end

  describe "membership_leader/1" do
    alias Malachi.Test.AliveMembersStub

    test "not the leader when membership is unavailable" do
      # a server that does not exist / does not answer → conservative false (never risk two orchestrators)
      refute App.membership_leader(:no_such_membership_server).()
    end

    test "leader iff this node is the lowest-sorted live member" do
      # alive_members is sorted, so the head is the lowest node
      {:ok, self_lowest} =
        AliveMembersStub.start_link([{Malachi.LogMembership, node()}, {Malachi.LogMembership, :zzzz@h}])

      assert App.membership_leader(self_lowest).()

      {:ok, other_lower} =
        AliveMembersStub.start_link([{Malachi.LogMembership, :"0000@h"}, {Malachi.LogMembership, node()}])

      refute App.membership_leader(other_lower).()

      {:ok, empty} = AliveMembersStub.start_link([])
      refute App.membership_leader(empty).()
    end
  end

  describe "broker_refs/1" do
    test "one named ReplicationServer reference per node" do
      assert App.broker_refs([:"a@127.0.0.1", :"b@127.0.0.1"]) ==
               [{Malachi.LogReplication, :"a@127.0.0.1"}, {Malachi.LogReplication, :"b@127.0.0.1"}]
    end
  end

  describe "data_plane_opts/2" do
    test "no cluster configured yields no data-plane options (local single replica default)" do
      assert App.data_plane_opts(nil, [:"a@127.0.0.1"]) == []
    end

    test "a configured cluster places replicas across every node's ReplicationServer" do
      opts = App.data_plane_opts(:log_meta, [:"a@127.0.0.1", :"b@127.0.0.1"])

      assert Keyword.fetch!(opts, :brokers) ==
               [{Malachi.LogReplication, :"a@127.0.0.1"}, {Malachi.LogReplication, :"b@127.0.0.1"}]

      assert is_integer(Keyword.fetch!(opts, :replication_factor))
      # live_brokers narrows placement to the alive set as membership converges
      assert is_function(Keyword.fetch!(opts, :live_brokers), 0)
    end
  end

  describe "membership_seeds/1" do
    test "seeds with the other nodes' membership servers, excluding self" do
      others = [:"a@127.0.0.1", :"b@127.0.0.1"]

      assert App.membership_seeds([node() | others]) ==
               [{Malachi.LogMembership, :"a@127.0.0.1"}, {Malachi.LogMembership, :"b@127.0.0.1"}]
    end
  end

  describe "live_replication_refs/1" do
    test "maps membership members to their nodes' ReplicationServer references" do
      members = [{Malachi.LogMembership, :"a@127.0.0.1"}, {Malachi.LogMembership, :"b@127.0.0.1"}]

      assert App.live_replication_refs(members) ==
               [{Malachi.LogReplication, :"a@127.0.0.1"}, {Malachi.LogReplication, :"b@127.0.0.1"}]
    end
  end

  describe "broker_attributes_for/2" do
    test "maps members to their ReplicationServer refs with each member's attributes (by node)" do
      members = [{Malachi.LogMembership, :"a@127.0.0.1"}, {Malachi.LogMembership, :"b@127.0.0.1"}]
      attributes_of = fn {_name, node} -> %{"rack" => to_string(node)} end

      assert App.broker_attributes_for(members, attributes_of) == %{
               {Malachi.LogReplication, :"a@127.0.0.1"} => %{"rack" => "a@127.0.0.1"},
               {Malachi.LogReplication, :"b@127.0.0.1"} => %{"rack" => "b@127.0.0.1"}
             }
    end
  end

  describe "parse_attributes/1" do
    test "absent or empty yields no attributes" do
      assert App.parse_attributes(nil) == %{}
      assert App.parse_attributes("") == %{}
    end

    test "parses key=value pairs, trimming whitespace" do
      assert App.parse_attributes("rack=a,dc=east") == %{"rack" => "a", "dc" => "east"}
      assert App.parse_attributes(" rack = a , dc = east ") == %{"rack" => "a", "dc" => "east"}
    end

    test "ignores entries without an = and keeps a value that contains =" do
      assert App.parse_attributes("rack=a,bogus,url=http://x=y") == %{"rack" => "a", "url" => "http://x=y"}
    end
  end

  describe "parse_topology/1" do
    test "absent or empty yields no topology" do
      assert App.parse_topology(nil) == %{}
      assert App.parse_topology("") == %{}
    end

    test "parses node=value pairs into a node => value map, trimming" do
      assert App.parse_topology("n1@h=a,n2@h=b") == %{:n1@h => "a", :n2@h => "b"}
      assert App.parse_topology(" n1@h = a , n2@h = b ") == %{:n1@h => "a", :n2@h => "b"}
    end

    test "ignores entries without an =" do
      assert App.parse_topology("n1@h=a,bogus,n2@h=b") == %{:n1@h => "a", :n2@h => "b"}
    end
  end
end
