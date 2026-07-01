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
    end
  end
end
