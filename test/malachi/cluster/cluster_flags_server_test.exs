defmodule Malachi.Cluster.ClusterFlagsServerTest do
  @moduledoc """
  The admission check in front of the flag store, without a cluster.

  `enable/5` takes the membership view and the registry as arguments precisely so the decision it makes
  can be tested on its own; what it does afterwards, submitting the command, is covered against real
  `ra` in `Malachi.Cluster.ClusterFlagsMultinodeTest`.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Malachi.Cluster.Capabilities
  alias Malachi.Cluster.ClusterFlags
  alias Malachi.Cluster.ClusterFlagsServer

  @cap :batch_format
  @known [@cap, :compaction]

  setup do
    cluster = :"flags_srv_#{System.unique_integer([:positive])}"
    {:ok, server_id} = ClusterFlagsServer.start(cluster, [node()])
    on_exit(fn -> ClusterFlagsServer.delete(cluster) end)
    %{server_id: server_id, cluster: cluster}
  end

  defp reads(nodes) do
    fn node ->
      case Map.fetch(nodes, node) do
        {:ok, {status, capabilities}} -> {status, Capabilities.attributes(%{}, capabilities)}
        :error -> {nil, %{}}
      end
    end
  end

  defp all_good, do: reads(%{node() => {:alive, @known}})

  test "switches the flag on when every configured node advertises it", %{server_id: server_id} do
    assert ClusterFlagsServer.enable(server_id, "batch_format", [node()], all_good(), @known) == :ok
    assert {:ok, flags} = ClusterFlagsServer.read(server_id, :consistent)
    assert ClusterFlags.enabled(flags) == [@cap]
  end

  test "is idempotent, so an operator may run it twice", %{server_id: server_id} do
    assert ClusterFlagsServer.enable(server_id, "batch_format", [node()], all_good(), @known) == :ok
    assert ClusterFlagsServer.enable(server_id, "batch_format", [node()], all_good(), @known) == :ok
    assert {:ok, flags} = ClusterFlagsServer.read(server_id, :consistent)
    assert ClusterFlags.enabled(flags) == [@cap]
  end

  test "refuses and names the nodes that do not advertise it, writing nothing", %{server_id: server_id} do
    reads = reads(%{node() => {:alive, @known}, :old@h => {:alive, []}, :gone@h => {:dead, @known}})

    log =
      capture_log(fn ->
        assert ClusterFlagsServer.enable(server_id, "batch_format", [node(), :old@h, :gone@h], reads, @known) ==
                 {:error, {:unsupported, [:gone@h, :old@h]}}
      end)

    assert log =~ "old@h"
    assert log =~ "gone@h"

    # Nothing reached the log: a refused flip must leave the cluster exactly as it was.
    assert {:ok, flags} = ClusterFlagsServer.read(server_id, :consistent)
    assert ClusterFlags.enabled(flags) == []
  end

  test "refuses a name this build does not know, before anything reaches the store", %{server_id: server_id} do
    assert ClusterFlagsServer.enable(server_id, "no_such_flag", [node()], all_good(), @known) ==
             {:error, :unknown_flag}

    assert {:ok, flags} = ClusterFlagsServer.read(server_id, :consistent)
    assert ClusterFlags.enabled(flags) == []
  end

  test "the registry this release ships knows nothing, so every name is refused", %{server_id: server_id} do
    assert ClusterFlagsServer.enable(server_id, "batch_format", [node()], all_good()) ==
             {:error, :unknown_flag}
  end

  test "reads the same state consistently and locally", %{server_id: server_id} do
    assert ClusterFlagsServer.enable(server_id, "compaction", [node()], all_good(), @known) == :ok

    assert {:ok, consistent} = ClusterFlagsServer.read(server_id, :consistent)
    assert {:ok, local} = ClusterFlagsServer.read(server_id, :local)
    assert ClusterFlags.enabled(consistent) == [:compaction]
    assert ClusterFlags.enabled(local) == [:compaction]
  end

  test "reconcile is idempotent on a store this node already hosts", %{cluster: cluster} do
    # The reconciler tick calls this on every node that is clustered, forever. A no-op once joined is
    # what keeps that cheap.
    assert ClusterFlagsServer.reconcile(cluster, [node()]) == :ok
    assert ClusterFlagsServer.reconcile(cluster, [node()]) == :ok
  end

  test "a read of a store that does not exist is an error, never an empty answer" do
    # The distinction the cache depends on: "no flag is on" and "I could not ask" must not look alike,
    # or a node that cannot reach the store would happily serve past a flag it cannot honour.
    assert {:error, _reason} = ClusterFlagsServer.read({:flags_never_started, node()}, :consistent)
    assert {:error, _reason} = ClusterFlagsServer.read({:flags_never_started, node()}, :local)
  end
end
