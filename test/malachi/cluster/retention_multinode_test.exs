defmodule Malachi.Cluster.RetentionMultinodeTest do
  # Retention across real BEAM nodes (#191): segments live on replication servers on two peer nodes, the
  # sweep on this node expires them with the application's real delete path (control plane, then every
  # remote replica), and a consumer group reading through this node's broker is told, through its skip
  # reporter, that it was moved past what was deleted. The sweep's telemetry and the skip are emitted
  # where the work happened: here, not on the nodes that only held the copies.
  #
  # async: false and tagged, like every test that starts peer nodes.
  use ExUnit.Case, async: false

  @moduletag :multinode

  alias Malachi.BrokerServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.RetentionCoordinator
  alias Malachi.LogApi
  alias Malachi.Metadata
  alias Malachi.Retention.SkipReporter
  alias Malachi.Test.Distribution

  @server :retention_multinode_repl

  setup_all do
    Distribution.ensure_started()
  end

  # A peer node running one replication server, without the Malachi application. Returns its broker ref.
  defp start_peer_replica do
    {_peer, node, name} = Distribution.start_peer("retention")
    {:ok, _apps} = :erpc.call(node, :application, :ensure_all_started, [:logger])
    {:ok, _apps} = :erpc.call(node, :application, :ensure_all_started, [:telemetry])

    directory = Path.join(System.tmp_dir!(), "#{name}_data")
    on_exit(fn -> File.rm_rf!(directory) end)

    # Started unlinked: linked, it would die with the short-lived `:erpc` worker that started it.
    {:ok, _pid} =
      :erpc.call(node, GenServer, :start, [ReplicationServer, [name: @server, directory: directory], [name: @server]])

    {@server, node}
  end

  defp eventually(check, remaining_ms \\ 5_000) do
    cond do
      check.() -> true
      remaining_ms <= 0 -> false
      true -> Process.sleep(25) && eventually(check, remaining_ms - 25)
    end
  end

  defp segments(broker, range_id), do: broker |> BrokerServer.metadata() |> Metadata.segments_of_range(range_id)

  test "a sweep deletes the remote copies, and the group reading through this node is told it skipped them" do
    replicas = [start_peer_replica(), start_peer_replica()]

    name = :"retention_multinode_broker_#{System.unique_integer([:positive])}"
    start_supervised!({SkipReporter, name: SkipReporter.name_for(name)})
    directory = Path.join(System.tmp_dir!(), "#{name}_data")
    on_exit(fn -> File.rm_rf!(directory) end)

    {:ok, broker} =
      BrokerServer.start_link(directory, name: name, brokers: replicas, replication_factor: 2, segment_max_bytes: 1)

    on_exit(fn -> if Process.alive?(broker), do: BrokerServer.stop(broker) end)

    parent = self()

    :telemetry.attach_many(
      "retention-multinode",
      [[:malachi, :retention, :skip], [:malachi, :retention, :sweep]],
      fn event, measurements, metadata, _config -> send(parent, {event, measurements, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach("retention-multinode") end)

    :ok = LogApi.create_topic(name, "events")

    for value <- ["v0", "v1", "v2", "v3"],
        do: {:ok, 1} = LogApi.produce(name, "events", [%{"key" => value, "value" => value}])

    [root_id] = BrokerServer.active_range_ids(name, "events")
    assert eventually(fn -> Enum.count(segments(name, root_id), &(&1.state == :sealed)) >= 3 end)

    [oldest | _] = Enum.sort_by(segments(name, root_id), & &1.start_offset)
    assert Enum.all?(oldest.replica_set, &match?({:ok, [_ | _]}, ReplicationServer.read(&1, oldest.id, 0, 10)))

    # A group that committed the very beginning, before retention ran.
    :ok = BrokerServer.commit_offset(name, "billing", "events", %{root_id => {0, 0}})

    {:ok, coordinator} =
      RetentionCoordinator.start_link(
        metadata_source: fn -> BrokerServer.metadata(name) end,
        expire_segment: &Malachi.Application.expire_segment(&1, name),
        policy: %{max_age_ms: 0},
        clock: fn -> System.system_time(:millisecond) + 60_000 end,
        interval: 3_600_000
      )

    expired = RetentionCoordinator.run_now(coordinator)
    assert oldest.id in expired

    assert_receive {[:malachi, :retention, :sweep], %{expired: swept, failed: 0}, %{}}
    assert swept == length(expired)

    # Gone from the replicas on the other nodes, through the real delete path.
    assert Enum.all?(oldest.replica_set, &(ReplicationServer.read(&1, oldest.id, 0, 10) == :eof))

    # The next produce gives the group something to land on past the gap.
    {:ok, 1} = LogApi.produce(name, "events", [%{"key" => "v4", "value" => "v4"}])

    # One record per segment, and retention removes the oldest first, so the group resumes exactly past
    # the expired prefix and the skip covers exactly those offsets.
    assert {:ok, records, _cursor} = LogApi.fetch_group(name, "events", "billing", 100)
    assert Enum.map(records, & &1.value) == Enum.drop(["v0", "v1", "v2", "v3", "v4"], length(expired))

    assert_receive {[:malachi, :retention, :skip], %{count: 1, offsets: offsets},
                    %{topic: "events", group: "billing", origin: :cursor, span: :exact}}

    assert offsets == length(expired)
  end
end
