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
