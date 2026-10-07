defmodule Malachi.Cluster.RetentionCoordinatorTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  import Malachi.Test.PollingHelper

  alias Malachi.BrokerServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.RetentionCoordinator
  alias Malachi.Log.Record
  alias Malachi.Metadata
  alias Malachi.Test.UnknownMessages

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
      policies: fn -> {:ok, %{}} end,
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

  describe "age rolls" do
    # Topic "t" with an active segment opened at `opened_at`, beside the sealed "old" segment.
    defp with_active(opened_at) do
      metadata = with_sealed([{"old", 0, 100, 1_000}])
      {metadata, :ok} = Metadata.apply(metadata, {:register_segment, {"t", 0}, "head", [:b1], 1}, opened_at)
      metadata
    end

    defp roller(opts) do
      test_pid = self()

      start(
        Keyword.merge(
          [
            metadata_source: fn -> with_active(1_000) end,
            roll_segments: fn segments -> send(test_pid, {:roll, Enum.map(segments, & &1.id)}) end,
            policy: %{max_age_ms: 5_000, segment_max_age_ms: 60_000},
            clock: fn -> 61_000 end
          ],
          opts
        )
      )
    end

    test "a sweep asks to roll the active segments older than the limit, after expiring" do
      server = roller([])

      assert RetentionCoordinator.run_now(server) == ["old"]
      assert_receive {:expired, "old"}
      assert_receive {:roll, ["head"]}
    end

    test "a sweep with nothing due still asks, with nothing" do
      server = roller(clock: fn -> 60_999 end)

      RetentionCoordinator.run_now(server)
      assert_receive {:roll, []}
    end

    test "the segments handed over are the control plane's metadata, for the broker to fence" do
      test_pid = self()
      server = roller(roll_segments: fn segments -> send(test_pid, {:roll, segments}) end)

      RetentionCoordinator.run_now(server)
      assert_receive {:roll, [%{id: "head", state: :active, replica_set: [:b1], start_offset: 1, opened_at: 1_000}]}
    end

    test "a non-leader's tick rolls nothing, and run_now rolls regardless of the gate" do
      server = roller(interval: 20, leader?: fn -> false end)
      refute_receive {:roll, _segments}, 200

      RetentionCoordinator.run_now(server)
      assert_receive {:roll, ["head"]}
    end

    test "the leader's tick rolls" do
      _server = roller(interval: 20)
      assert_receive {:roll, ["head"]}, 1_000
    end

    test "a sweep that could not read the policies rolls nothing either" do
      server = roller(policies: fn -> {:error, :unreachable} end)

      capture_log(fn -> RetentionCoordinator.run_now(server) end)
      refute_receive {:roll, _segments}, 100
    end

    test "with no roll seam the sweep still runs" do
      server = start(metadata_source: fn -> with_active(1_000) end, policy: %{segment_max_age_ms: 60_000})
      assert RetentionCoordinator.run_now(server) == []
    end
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

    test "a refused delete is labeled with the reply and not counted as expired", %{topic: topic} do
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

    test "an answer the coordinator does not know is labeled :other", %{topic: topic} do
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

  describe "policies the sweep could not read" do
    test "a read failure skips the sweep entirely rather than expiring under the global limits" do
      # The whole point: falling back to the global limits here deletes exactly the data a more
      # permissive policy exists to keep, on every replica, with no way back. A skipped sweep costs
      # one interval.
      server = start(policies: fn -> {:error, :unreachable} end)

      log = capture_log(fn -> assert RetentionCoordinator.run_now(server) == [] end)

      assert log =~ "the policy store did not answer"
      refute_receive {:expired, _id}, 100
    end

    test "it says so once, not on every pass, while the store stays down" do
      server = start(policies: fn -> {:error, :unreachable} end)

      assert capture_log(fn -> RetentionCoordinator.run_now(server) end) =~ "the policy store did not answer"
      refute capture_log(fn -> RetentionCoordinator.run_now(server) end) =~ "the policy store did not answer"
    end

    test "a store that answers again resumes sweeping, and says so again if it fails later" do
      answer = :counters.new(1, [])
      :counters.put(answer, 1, 0)

      policies = fn ->
        case :counters.get(answer, 1) do
          0 -> {:error, :unreachable}
          _readable -> {:ok, %{}}
        end
      end

      server = start(policies: policies)

      assert capture_log(fn -> assert RetentionCoordinator.run_now(server) == [] end) =~
               "the policy store did not answer"

      :counters.put(answer, 1, 1)
      assert RetentionCoordinator.run_now(server) == ["old"]

      # The second outage is the half of the name the test used to leave unchecked. The warning is
      # silenced after the first line and un-silenced by the sweep that recovered, so without that
      # reset every later outage passes in silence and nothing here would notice.
      :counters.put(answer, 1, 0)

      assert capture_log(fn -> assert RetentionCoordinator.run_now(server) == [] end) =~
               "the policy store did not answer"
    end
  end

  test "an unknown cast, info message or call is counted and survived" do
    server = start([])

    UnknownMessages.assert_survives_unknown(server, :retention, fn ->
      assert RetentionCoordinator.run_now(server) == ["old"]
    end)
  end

  describe "a broker that does not answer in time" do
    import ExUnit.CaptureLog

    test "skips the sweep, says why once, and stays up" do
      source = fn -> exit({:timeout, {GenServer, :call, [Malachi.LogBroker, :metadata, 5000]}}) end
      server = start(metadata_source: source)

      log =
        capture_log(fn ->
          assert RetentionCoordinator.run_now(server) == []
          assert RetentionCoordinator.run_now(server) == []
        end)

      assert log =~ "retention sweep skipped: the metadata could not be read"
      assert length(String.split(log, "retention sweep skipped")) == 2
      assert Process.alive?(server)
    end

    # One latch for both causes would keep the first cause as the only line while the second one lasts, so
    # an operator reads about a store that has already recovered. Each order is checked, since a latch keyed
    # on either cause alone would pass one of them.
    test "a cause that changes is logged again, whichever came first" do
      for {first, second} <- [{:policies, :metadata}, {:metadata, :policies}] do
        cause = :counters.new(1, [])
        :counters.put(cause, 1, 0)
        failing? = fn which -> if :counters.get(cause, 1) == 0, do: which == first, else: which == second end
        down = fn -> exit({:timeout, {GenServer, :call, [Malachi.LogBroker, :metadata, 5000]}}) end

        server =
          start(
            policies: fn -> if failing?.(:policies), do: {:error, :unreachable}, else: {:ok, %{}} end,
            metadata_source: fn -> if failing?.(:metadata), do: down.(), else: with_sealed([{"old", 0, 100, 1_000}]) end
          )

        first_log = capture_log(fn -> assert RetentionCoordinator.run_now(server) == [] end)
        :counters.put(cause, 1, 1)
        second_log = capture_log(fn -> assert RetentionCoordinator.run_now(server) == [] end)

        assert first_log =~ message(first), "#{first} then #{second}: #{first_log}"
        assert second_log =~ message(second), "#{first} then #{second}: #{second_log}"
      end
    end
  end

  defp message(:policies), do: "the policy store did not answer"
  defp message(:metadata), do: "retention sweep skipped: the metadata could not be read"
end
