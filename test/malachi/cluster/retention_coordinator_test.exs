defmodule Malachi.Cluster.RetentionCoordinatorTest do
  use ExUnit.Case, async: true

  alias Malachi.BrokerServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.RetentionCoordinator
  alias Malachi.Log.Record
  alias Malachi.Metadata

  @range {"t", 0}

  defp with_sealed(segments) do
    base = elem(Metadata.apply(Metadata.new(), {:create_topic, "t", 4}), 0)

    Enum.reduce(segments, base, fn {id, start_offset, bytes, sealed_at}, metadata ->
      {metadata, :ok} = Metadata.apply(metadata, {:register_segment, @range, id, [:b1], start_offset})
      {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, id, 1, bytes, sealed_at})
      metadata
    end)
  end

  defp start(opts) do
    test_pid = self()

    defaults = [
      metadata_source: fn -> with_sealed([{"old", 0, 100, 1_000}, {"new", 1, 100, 9_500}]) end,
      expire_segment: fn segment -> send(test_pid, {:expired, segment.id}) end,
      policy: %{max_age_ms: 5_000},
      clock: fn -> 10_000 end,
      interval: 60_000
    ]

    {:ok, server} = RetentionCoordinator.start_link(Keyword.merge(defaults, opts))
    server
  end

  test "run_now expires segments per policy and calls expire_segment on each" do
    server = start([])

    assert RetentionCoordinator.run_now(server) == ["old"]
    assert_receive {:expired, "old"}
    refute_receive {:expired, "new"}
  end

  test "expire_segment receives the segment's full metadata (for its replica set)" do
    test_pid = self()
    server = start(expire_segment: fn segment -> send(test_pid, {:expired, segment}) end)

    RetentionCoordinator.run_now(server)
    assert_receive {:expired, %{id: "old", replica_set: [:b1], state: :sealed}}
  end

  test "a sweep runs on the tick" do
    test_pid = self()
    _server = start(expire_segment: fn segment -> send(test_pid, {:expired, segment.id}) end, interval: 20)

    # no synchronous run_now — the scheduled tick drives it
    assert_receive {:expired, "old"}, 1_000
  end

  @tag :tmp_dir
  test "end to end: a sweep expires a real sealed segment from the control plane and storage", %{tmp_dir: directory} do
    # a real broker over a named ReplicationServer we can inspect (the same shape the app wires up)
    repl_name = :"ret_repl_#{System.unique_integer([:positive])}"
    repl_dir = Path.join(directory, "repl")
    start_supervised!({ReplicationServer, name: repl_name, directory: repl_dir}, id: repl_name)

    one_record = Record.encoded_size(Record.new("value", key: "key"))
    {:ok, broker} = BrokerServer.start_link(directory, brokers: [repl_name], segment_max_bytes: one_record)

    {:ok, _root} = BrokerServer.create_topic(broker, "events", 4)
    {:ok, _} = BrokerServer.produce(broker, "events", [Record.new("value", key: "k0")])
    {:ok, _} = BrokerServer.produce(broker, "events", [Record.new("value", key: "k1")])

    sealed = BrokerServer.metadata(broker) |> Metadata.get_segment({{"events", 0}, 0})
    assert sealed.state == :sealed
    assert {:ok, [_ | _]} = ReplicationServer.read(repl_name, sealed.id, sealed.start_offset, 10)

    # the real expire_segment: drop from the control plane, then delete on each replica
    expire = fn segment ->
      BrokerServer.delete_segment(broker, segment.id)
      Enum.each(segment.replica_set, &ReplicationServer.delete(&1, segment.id))
    end

    {:ok, coordinator} =
      RetentionCoordinator.start_link(
        metadata_source: fn -> BrokerServer.metadata(broker) end,
        expire_segment: expire,
        policy: %{max_age_ms: 5_000},
        clock: fn -> System.system_time(:millisecond) + 60_000 end,
        interval: 60_000
      )

    assert sealed.id in RetentionCoordinator.run_now(coordinator)

    # gone from the control plane and from storage
    assert BrokerServer.metadata(broker) |> Metadata.get_segment(sealed.id) == nil
    assert ReplicationServer.read(repl_name, sealed.id, sealed.start_offset, 10) == :eof

    BrokerServer.stop(broker)
  end
end
