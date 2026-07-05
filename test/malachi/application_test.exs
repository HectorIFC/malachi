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
end
