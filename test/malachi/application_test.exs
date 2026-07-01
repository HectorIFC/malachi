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
end
