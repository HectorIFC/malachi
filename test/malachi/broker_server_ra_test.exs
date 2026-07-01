defmodule Malachi.BrokerServerRaTest do
  # async: false — ra is global/stateful (one data dir, on-disk Raft logs).
  use ExUnit.Case, async: false

  alias Malachi.BrokerServer
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Metadata

  setup_all do
    data_dir = Path.join(System.tmp_dir!(), "malachi_ra_bs_#{System.unique_integer([:positive])}")
    File.rm_rf!(data_dir)
    _ = :ra.start_in(String.to_charlist(data_dir))
    on_exit(fn -> File.rm_rf!(data_dir) end)
    :ok
  end

  defp start_replication do
    directory = Path.join(System.tmp_dir!(), "malachi_ra_bs_repl_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    start_supervised!({ReplicationServer, directory: directory}, id: {:repl, System.unique_integer([:positive])})
  end

  test "metadata mutations are committed to the Raft cluster, and the cache matches it" do
    cluster = :"bs_meta_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)

    brokers = for _ <- 1..3, do: start_replication()

    {:ok, control} =
      BrokerServer.start_link("unused",
        brokers: brokers,
        replication_factor: 3,
        segment_max_bytes: Record.encoded_size(Record.new("value", key: "key")),
        metadata_cluster: cluster
      )

    {:ok, root} = BrokerServer.create_topic(control, "events", 4)
    {:ok, _placements} = BrokerServer.produce(control, "events", [Record.new("v", key: "k")])

    # the topic and segment live in the replicated Raft state (queried directly), proving the
    # control-plane mutations went through the log rather than only a local map
    {:ok, replicated} = MetadataServer.query({cluster, node()}, & &1)
    assert Metadata.get_topic(replicated, "events").name == "events"
    assert Metadata.segments_of_range(replicated, root) != []

    # and the broker's local cache is exactly the replicated state (read-your-writes)
    assert BrokerServer.metadata(control) == replicated

    :ok = BrokerServer.stop(control)
  end

  test "a rejected control-plane command surfaces the Raft machine error" do
    cluster = :"bs_meta_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)

    {:ok, control} =
      BrokerServer.start_link("unused", brokers: [start_replication()], metadata_cluster: cluster)

    {:ok, _root} = BrokerServer.create_topic(control, "events", 4)
    assert {:error, :already_exists} = BrokerServer.create_topic(control, "events", 4)

    :ok = BrokerServer.stop(control)
  end

  test "produces across a 3-broker replica set and reads the records back (replicated data plane)" do
    cluster = :"bs_meta_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)

    # The data-plane wiring D2 sets up: several ReplicationServers as the broker set + a replication
    # factor, with ra as the control plane. This is the shape Malachi.Application builds when clustered.
    brokers = for _ <- 1..3, do: start_replication()

    {:ok, control} =
      BrokerServer.start_link("unused", brokers: brokers, replication_factor: 3, metadata_cluster: cluster)

    {:ok, _root} = BrokerServer.create_topic(control, "events", 4)

    records = for index <- 0..4, do: Record.new("v#{index}", key: "k#{index}")
    {:ok, _placements} = BrokerServer.produce(control, "events", records)

    # a segment landed on a 3-broker replica set (placement across the whole broker set)
    [range_id] = BrokerServer.active_range_ids(control, "events")
    [segment] = Metadata.segments_of_range(BrokerServer.metadata(control), range_id)
    assert length(segment.replica_set) == 3

    # committed (quorum-durable) records read back through the broker's primary
    {read, _cursor} = BrokerServer.consume(control, "events", %{}, 100, 0)
    assert read |> Enum.map(& &1.value) |> Enum.sort() == Enum.map(records, & &1.value) |> Enum.sort()

    :ok = BrokerServer.stop(control)
  end
end
