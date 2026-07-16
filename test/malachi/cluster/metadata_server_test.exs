defmodule Malachi.Cluster.MetadataServerTest do
  # async: false — ra is global/stateful (one data dir, on-disk Raft logs).
  use ExUnit.Case, async: false

  alias Malachi.Cluster.MetadataServer
  alias Malachi.Metadata

  setup_all do
    data_dir = Path.join(System.tmp_dir!(), "malachi_ra_#{System.unique_integer([:positive])}")
    File.rm_rf!(data_dir)
    _ = :ra.start_in(String.to_charlist(data_dir))
    on_exit(fn -> File.rm_rf!(data_dir) end)
    :ok
  end

  defp start_cluster do
    name = :"vnode_#{System.unique_integer([:positive])}"
    {:ok, server_id} = MetadataServer.start(name)
    on_exit(fn -> MetadataServer.delete(name) end)
    server_id
  end

  test "replicates metadata commands through the Raft log and serves consistent queries" do
    server_id = start_cluster()

    assert {:ok, {:ok, root_id}} = MetadataServer.command(server_id, {:create_topic, "events", 4})

    assert {:ok, %{name: "events", keyspace_size: 16, state: :active}} =
             MetadataServer.query(server_id, &Metadata.get_topic(&1, "events"))

    assert {:ok, {:ok, left_id, right_id}} = MetadataServer.command(server_id, {:split_range, root_id})

    assert {:ok, active} = MetadataServer.query(server_id, &Metadata.active_ranges_of_topic(&1, "events"))
    assert Enum.sort(Enum.map(active, & &1.id)) == Enum.sort([left_id, right_id])
  end

  test "a rejected command returns the machine's error reply" do
    server_id = start_cluster()
    assert {:ok, {:ok, _root}} = MetadataServer.command(server_id, {:create_topic, "events", 4})

    assert {:ok, {:error, :already_exists}} =
             MetadataServer.command(server_id, {:create_topic, "events", 4})

    assert {:ok, {:error, :invalid_topic_name}} =
             MetadataServer.command(server_id, {:create_topic, "../evil", 4})
  end

  test "state survives a server restart (Raft log is durable)" do
    name = :"vnode_#{System.unique_integer([:positive])}"
    {:ok, server_id} = MetadataServer.start(name)
    on_exit(fn -> MetadataServer.delete(name) end)

    {:ok, {:ok, _root}} = MetadataServer.command(server_id, {:create_topic, "durable", 4})
    :ok = :ra.stop_server(:default, server_id)
    :ok = :ra.restart_server(:default, server_id)

    assert {:ok, %{name: "durable"}} = MetadataServer.query(server_id, &Metadata.get_topic(&1, "durable"))
  end

  test "leader?/1 is true for the local server of a formed single-node cluster (1C-b)" do
    server_id = start_cluster()
    # a successful command guarantees a formed cluster with an elected leader
    {:ok, {:ok, _root}} = MetadataServer.command(server_id, {:create_topic, "events", 4})

    assert MetadataServer.leader?(server_id)
  end

  test "leader?/1 is false for an unformed / unreachable cluster (never assume leadership)" do
    refute MetadataServer.leader?({:"no_such_vnode_#{System.unique_integer([:positive])}", node()})
  end

  test "ensure_started/2 starts a fresh cluster, then reuses a running one without restarting" do
    name = :"vnode_ensure_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(name) end)

    # first call forms the cluster
    assert {:ok, server_id} = MetadataServer.ensure_started(name)
    assert {:ok, {:ok, _root}} = MetadataServer.command(server_id, {:create_topic, "events", 4})

    # second call finds it already running: same server id, no restart, state preserved (resume-safe)
    assert MetadataServer.ensure_started(name) == {:ok, server_id}
    assert {:ok, %{name: "events"}} = MetadataServer.query(server_id, &Metadata.get_topic(&1, "events"))
    # a restart would have wiped the elected leader / in-flight state; the topic is still there
    assert {:ok, {:error, :already_exists}} = MetadataServer.command(server_id, {:create_topic, "events", 4})
  end
end
