defmodule Malachi.Cluster.MetadataHaTest do
  # Real multi-node Raft: spins up peer BEAM nodes, forms a metadata cluster across them, and
  # verifies the metadata survives losing the leader node. async: false and tagged so it can be
  # excluded where multi-node networking is unavailable.
  use ExUnit.Case, async: false

  @moduletag :multinode

  alias Malachi.Cluster.MetadataServer
  alias Malachi.Metadata

  setup_all do
    # Distribution (and thus epmd) is required for a multi-node Raft cluster.
    _ = System.cmd("epmd", ["-daemon"])

    case :net_kernel.start([:"malachi_primary@127.0.0.1", :longnames]) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    {:ok, _} = Application.ensure_all_started(:ra)
    :ok
  end

  defp start_peer do
    name = :"malachi_peer_#{System.unique_integer([:positive])}"
    {:ok, peer, node} = :peer.start_link(%{name: name, host: ~c"127.0.0.1", longnames: true})
    on_exit(fn -> try_stop(peer) end)

    :ok = :erpc.call(node, :code, :add_paths, [:code.get_path()])
    {:ok, _} = :erpc.call(node, :application, :ensure_all_started, [:ra])
    data_dir = ~c"#{System.tmp_dir!()}/malachi_ra_ha_#{name}_#{System.unique_integer([:positive])}"
    {:ok, _} = :erpc.call(node, :ra, :start_in, [data_dir])

    {peer, node}
  end

  defp try_stop(peer) do
    :peer.stop(peer)
  catch
    _kind, _reason -> :ok
  end

  # The query runs on the (possibly remote) leader, so it must be a function the peers have loaded;
  # an anonymous fun from this test module is not on the peers, so read the whole state with a
  # stdlib capture and inspect it locally.
  defp topic(cluster, node, name) do
    {:ok, state} = retry(fn -> MetadataServer.query({cluster, node}, &Function.identity/1) end)
    Metadata.get_topic(state, name)
  end

  # After an abrupt leader loss, ra needs a moment to elect a new leader; retry until it succeeds.
  defp commit(cluster, node, command) do
    retry(fn -> MetadataServer.command({cluster, node}, command) end)
  end

  defp retry(fun, remaining_ms \\ 5_000) do
    case fun.() do
      {:ok, _value} = ok -> ok
      _other when remaining_ms > 0 -> Process.sleep(100) && retry(fun, remaining_ms - 100)
    end
  end

  test "metadata survives losing the leader node" do
    cluster = :"meta_ha_#{System.unique_integer([:positive])}"
    peers = for _ <- 1..3, do: start_peer()
    nodes = Enum.map(peers, &elem(&1, 1))
    peer_by_node = Map.new(peers, fn {peer, node} -> {node, peer} end)

    {:ok, _local} = MetadataServer.start(cluster, nodes)

    # commit a topic and confirm it replicated to every member
    [first_node | _] = nodes
    assert {:ok, {:ok, _root}} = MetadataServer.command({cluster, first_node}, {:create_topic, "events", 4})
    for node <- nodes, do: assert(topic(cluster, node, "events").name == "events")

    # kill the leader node abruptly (a real crash)
    {:ok, _members, {^cluster, leader_node}} = :ra.members({cluster, first_node})
    :ok = try_stop(Map.fetch!(peer_by_node, leader_node))

    # a surviving member still commits — a new leader was elected from the remaining quorum, and the
    # earlier metadata is intact
    [survivor | _] = nodes -- [leader_node]
    assert {:ok, {:ok, _root2}} = commit(cluster, survivor, {:create_topic, "orders", 4})
    assert topic(cluster, survivor, "events").name == "events"
    assert topic(cluster, survivor, "orders").name == "orders"
  end
end
