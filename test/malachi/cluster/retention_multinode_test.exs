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
  alias Malachi.Telemetry

  @server :retention_multinode_repl

  setup_all do
    _ = System.cmd("epmd", ["-daemon"])

    case :net_kernel.start([:"malachi_primary@127.0.0.1", :longnames]) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    :ok
  end

  # A peer node running one replication server, without the Malachi application. Returns its broker ref.
  defp start_peer_replica do
    name = :"malachi_retention_#{System.unique_integer([:positive])}"
    {:ok, peer, node} = :peer.start_link(%{name: name, host: ~c"127.0.0.1", longnames: true})
    on_exit(fn -> try_stop(peer) end)

    :ok = :erpc.call(node, :code, :add_paths, [:code.get_path()])
    {:ok, _apps} = :erpc.call(node, :application, :ensure_all_started, [:logger])
    {:ok, _apps} = :erpc.call(node, :application, :ensure_all_started, [:telemetry])

    directory = Path.join(System.tmp_dir!(), "#{name}_data")
    on_exit(fn -> File.rm_rf!(directory) end)

    # Started unlinked: linked, it would die with the short-lived `:erpc` worker that started it.
    {:ok, _pid} =
      :erpc.call(node, GenServer, :start, [ReplicationServer, [name: @server, directory: directory], [name: @server]])

    {@server, node}
  end

  defp try_stop(peer) do
    :peer.stop(peer)
  catch
    _kind, _reason -> :ok
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

    # A sweep event carries no topic, and a telemetry handler sees every event on the node, so this one
    # has to say which sweeps are its own. `Malachi.Cluster.RetentionCoordinatorTest` is async and sweeps
    # while this test runs; without the marker one of ITS sweeps satisfies the `assert_receive` below
    # with `expired: 0` and this test fails for another test's work. The handler runs in the emitting
    # process, so the marker is read straight out of the process that swept. Same device as that module's
    # own `:retention_test_topic`, for the same reason.
    marker = :"retention_multinode_#{System.unique_integer([:positive])}"
    handler_id = "retention-multinode-#{marker}"

    :telemetry.attach_many(
      handler_id,
      [[:malachi, :retention, :skip], [:malachi, :retention, :sweep]],
      fn
        [:malachi, :retention, :sweep] = event, measurements, metadata, _config ->
          if Process.get(:retention_multinode_marker) == marker, do: send(parent, {event, measurements, metadata})

        event, measurements, metadata, _config ->
          send(parent, {event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

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
        # Marks the sweeping process as this test's, which is what the handler above filters on. Set here
        # because this is the one function the coordinator calls from inside its own sweep.
        expire_segment: fn segment ->
          Process.put(:retention_multinode_marker, marker)
          Malachi.Application.expire_segment(segment, name)
        end,
        policy: %{max_age_ms: 0},
        clock: fn -> System.system_time(:millisecond) + 60_000 end,
        interval: 3_600_000
      )

    # The flake, made deterministic. A sweep from another process on this node, emitted BEFORE this
    # test's own, which is exactly what the async retention tests do while this one runs. It carries no
    # topic, so only the marker tells it apart, and `assert_receive` takes the OLDEST match: without the
    # filter this event is the one it reads, and the test fails on another test's `expired: 0`.
    Task.await(Task.async(fn -> Telemetry.retention_sweep(1, 0, 0) end))

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
