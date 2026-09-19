defmodule Malachi.Cluster.RetentionCoordinatorTest do
  use ExUnit.Case, async: true

  import Malachi.Test.PollingHelper

  alias Malachi.BrokerServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.RetentionCoordinator
  alias Malachi.Log.Record
  alias Malachi.Metadata

  defp with_sealed(segments, topic \\ "t") do
    base = elem(Metadata.apply(Metadata.new(), {:create_topic, topic, 4}), 0)

    Enum.reduce(segments, base, fn {id, start_offset, bytes, sealed_at}, metadata ->
      {metadata, :ok} = Metadata.apply(metadata, {:register_segment, {topic, 0}, id, [:b1], start_offset})
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

    # no synchronous run_now: the scheduled tick drives it
    assert_receive {:expired, "old"}, 1_000
  end

  test "a non-leader ticks but skips the sweep (only the leader acts)" do
    test_pid = self()

    _server =
      start(
        expire_segment: fn segment -> send(test_pid, {:expired, segment.id}) end,
        interval: 20,
        leader?: fn -> false end
      )

    # the tick fires but does not sweep while this node is not the membership leader
    refute_receive {:expired, _}, 200
  end

  test "run_now sweeps regardless of the leader gate (manual trigger)" do
    server = start(leader?: fn -> false end)
    assert RetentionCoordinator.run_now(server) == ["old"]
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

    # A roll's seal lands when its fence answers, which is asynchronous to the produce that tripped it.
    first_segment = fn -> BrokerServer.metadata(broker) |> Metadata.get_segment({{"events", 0}, 0}) end
    wait_until!(fn -> first_segment.().state == :sealed end)
    sealed = first_segment.()
    assert {:ok, [_ | _]} = ReplicationServer.read(repl_name, sealed.id, sealed.start_offset, 10)

    {:ok, coordinator} =
      RetentionCoordinator.start_link(
        metadata_source: fn -> BrokerServer.metadata(broker) end,
        # the real expire function, pointed at this test's broker
        expire_segment: &Malachi.Application.expire_segment(&1, broker),
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

  describe "sweep telemetry" do
    # Each test sweeps its own topic, so events from the other tests of this async module are ignored.
    setup do
      topic = "sweep_#{System.unique_integer([:positive])}"
      parent = self()
      handler_id = "retention-sweep-#{topic}"

      :telemetry.attach_many(
        handler_id,
        [[:malachi, :retention, :expire], [:malachi, :retention, :sweep]],
        fn event, measurements, metadata, config -> forward(event, measurements, metadata, config, parent, topic) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)
      %{topic: topic}
    end

    # A sweep event carries no topic, so it is forwarded when it comes from a coordinator this test
    # started, which is the only process that registers under the test's topic.
    defp forward([:malachi, :retention, :expire], measurements, %{topic: topic} = metadata, _config, parent, topic),
      do: send(parent, {:expire_event, measurements, metadata})

    defp forward([:malachi, :retention, :sweep], measurements, metadata, _config, parent, topic) do
      if Process.get(:retention_test_topic) == topic, do: send(parent, {:sweep_event, measurements, metadata})
    end

    defp forward(_event, _measurements, _metadata, _config, _parent, _topic), do: :ok

    defp sweeper(topic, expire_segment) do
      metadata = with_sealed([{"old", 0, 100, 1_000}, {"older", 1, 250, 900}, {"new", 2, 100, 9_500}], topic)

      start(
        metadata_source: fn -> metadata end,
        expire_segment: fn segment ->
          Process.put(:retention_test_topic, topic)
          expire_segment.(segment)
        end
      )
    end

    test "each expired segment emits its topic, bytes and result, and the sweep its duration", %{topic: topic} do
      server = sweeper(topic, fn _segment -> :ok end)

      assert RetentionCoordinator.run_now(server) |> Enum.sort() == ["old", "older"]

      assert_receive {:expire_event, %{count: 1, bytes: 100}, %{topic: ^topic, segment: "old", result: :ok}}
      assert_receive {:expire_event, %{count: 1, bytes: 250}, %{topic: ^topic, segment: "older", result: :ok}}
      assert_receive {:sweep_event, %{duration_us: duration, expired: 2, failed: 0}, %{}}
      assert is_integer(duration) and duration >= 0
    end

    test "a refused delete is labelled with the reply and not counted as expired", %{topic: topic} do
      replies = %{"old" => {:error, :migrating}, "older" => {:error, :segment_active}}
      server = sweeper(topic, fn segment -> Map.fetch!(replies, segment.id) end)

      RetentionCoordinator.run_now(server)

      assert_receive {:expire_event, %{bytes: 100}, %{segment: "old", result: :migrating}}
      assert_receive {:expire_event, %{bytes: 250}, %{segment: "older", result: :segment_active}}
      assert_receive {:sweep_event, %{expired: 0, failed: 2}, %{}}
    end

    test "an already-deleted segment is neither expired again nor a failure", %{topic: topic} do
      server = sweeper(topic, fn _segment -> {:error, :no_such_segment} end)

      RetentionCoordinator.run_now(server)

      assert_receive {:expire_event, _measurements, %{result: :no_such_segment}}
      assert_receive {:sweep_event, %{expired: 0, failed: 0}, %{}}
    end

    test "an answer the coordinator does not know is labelled :other", %{topic: topic} do
      server = sweeper(topic, fn _segment -> {:error, :timeout} end)

      RetentionCoordinator.run_now(server)

      assert_receive {:expire_event, _measurements, %{result: :other}}
      assert_receive {:sweep_event, %{expired: 0, failed: 2}, %{}}
    end

    test "a sweep that expires nothing still reports that it ran", %{topic: topic} do
      metadata = with_sealed([{"new", 0, 100, 9_500}], topic)

      server =
        start(
          metadata_source: fn ->
            Process.put(:retention_test_topic, topic)
            metadata
          end
        )

      assert RetentionCoordinator.run_now(server) == []
      assert_receive {:sweep_event, %{expired: 0, failed: 0}, %{}}
      refute_receive {:expire_event, _measurements, _metadata}
    end
  end
end
