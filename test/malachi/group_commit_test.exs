defmodule Malachi.GroupCommitTest do
  # async: false because the coalescing and flush-failure tests lean on the flush timer.
  use ExUnit.Case, async: false

  import Malachi.Test.TeardownHelper

  alias Malachi.BrokerServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Test.FaultySegmentStore
  alias Malachi.Test.TmpDir

  defp start_broker(opts \\ []), do: elem(start_broker_at(opts), 0)

  # Boots an independent group-commit broker (its own ReplicationServer, dir, and topic) and registers
  # cleanup. `opts`: :interval (flush ms, default 10), :group_commit (default true), :repl_opts (e.g. a
  # custom :store), :flush_max_records (eager-flush threshold, default 8000), :max_inflight (overload
  # valve, default 200000). Returns the broker pid and the replication server's data directory, which is
  # what a `FaultySegmentStore` rule or count is scoped by.
  defp start_broker_at(opts) do
    tag = System.unique_integer([:positive])
    base = TmpDir.path("gc_test")
    File.rm_rf!(base)
    repl = :"gc_repl_#{tag}"
    repl_dir = Path.join(base, "repl")

    {:ok, repl_pid} =
      ReplicationServer.start_link([name: repl, directory: repl_dir] ++ Keyword.get(opts, :repl_opts, []))

    {:ok, broker} =
      BrokerServer.start_link(Path.join(base, "broker"),
        brokers: [repl],
        group_commit: Keyword.get(opts, :group_commit, true),
        group_commit_interval_ms: Keyword.get(opts, :interval, 10),
        group_commit_flush_max_records: Keyword.get(opts, :flush_max_records, 8_000),
        group_commit_max_inflight: Keyword.get(opts, :max_inflight, 200_000)
      )

    on_exit(fn ->
      stop_quietly(broker)
      stop_quietly(repl_pid)
      FaultySegmentStore.clear(repl_dir)
      File.rm_rf!(base)
    end)

    {broker, repl_dir}
  end

  defp batch(n, value \\ "v"), do: for(i <- 1..n, do: Record.new(value, key: "k#{i}"))

  defp consume_all(broker, topic) do
    consume_loop(broker, topic, %{}, [])
  end

  defp consume_loop(broker, topic, positions, acc) do
    case BrokerServer.consume(broker, topic, positions, 1000, 0) do
      {[], _next, _skips} -> Enum.reverse(acc)
      {records, next, _skips} -> consume_loop(broker, topic, next, Enum.reverse(records, acc))
    end
  end

  describe "the gate is the configured replication factor (#273)" do
    import ExUnit.CaptureLog

    defp group_commit_with(replicas, replication_factor) do
      tag = System.unique_integer([:positive])
      base = TmpDir.path("gc_gate")
      on_exit(fn -> File.rm_rf!(base) end)

      repls =
        for index <- 1..replicas do
          name = :"gc_gate_#{tag}_#{index}"
          {:ok, pid} = ReplicationServer.start_link(name: name, directory: Path.join(base, "repl#{index}"))
          on_exit(fn -> stop_quietly(pid) end)
          name
        end

      {:ok, broker} =
        BrokerServer.start_link(Path.join(base, "broker"),
          brokers: repls,
          replication_factor: replication_factor,
          group_commit: true
        )

      on_exit(fn -> stop_quietly(broker) end)
      :sys.get_state(broker).group_commit
    end

    # Broker group commit writes the primary alone. Only a configured factor of 1 guarantees every
    # segment one replica, whatever the broker set does later; a set of one with a factor of 3 (a single
    # node's one-member cluster at the default) would put a follower on any broker that joins.
    test "only a configured factor of 1 turns it on, whatever the broker set" do
      assert group_commit_with(1, 1)
      assert group_commit_with(3, 1)
      refute group_commit_with(1, 3)
      refute group_commit_with(3, 3)
    end

    test "a broker set that grows keeps every segment on one replica, so group commit stays correct" do
      tag = System.unique_integer([:positive])
      base = TmpDir.path("gc_grow")
      on_exit(fn -> File.rm_rf!(base) end)

      [one, two] =
        for index <- 1..2 do
          name = :"gc_grow_#{tag}_#{index}"
          {:ok, pid} = ReplicationServer.start_link(name: name, directory: Path.join(base, "repl#{index}"))
          on_exit(fn -> stop_quietly(pid) end)
          name
        end

      {:ok, live} = Agent.start_link(fn -> [one] end)

      {:ok, broker} =
        BrokerServer.start_link(Path.join(base, "broker"),
          brokers: [one],
          replication_factor: 1,
          live_brokers: fn -> Agent.get(live, & &1) end,
          group_commit: true,
          brokers_refresh_interval: 3_600_000,
          segment_max_bytes: 1
        )

      on_exit(fn -> stop_quietly(broker) end)
      {:ok, _} = BrokerServer.create_topic(broker, "t", 8)

      Agent.update(live, fn _ -> [one, two] end)
      send(broker, :refresh_brokers)

      # Tiny segments, so these produces roll and place segments on the grown set: enough of them that
      # placement uses the broker that joined (each lands on either with even odds).
      for _ <- 1..20, do: {:ok, _} = BrokerServer.produce(broker, "t", batch(2))

      assert :sys.get_state(broker).group_commit
      replica_sets = BrokerServer.metadata(broker).segments |> Map.values() |> Enum.map(& &1.replica_set)
      assert Enum.any?(replica_sets, &(&1 == [two])), "the broker that joined was never used"
      assert Enum.all?(replica_sets, &(length(&1) == 1))
      assert length(consume_all(broker, "t")) == 40
    end

    test "asked for with a factor above 1, the node says so at boot" do
      log = capture_log(fn -> assert Malachi.Application.warn_group_commit_needs_rf1(true, 3) == :warned end)
      assert log =~ "MALACHI_LOG_REPLICATION_FACTOR=1"
      assert Malachi.Application.warn_group_commit_needs_rf1(true, 1) == :ok
      assert Malachi.Application.warn_group_commit_needs_rf1(false, 3) == :ok
    end
  end

  test "a produce reply is deferred until the group flush, then returns durable" do
    broker = start_broker(interval: 15)
    {:ok, _} = BrokerServer.create_topic(broker, "t", 8)

    # Each call blocks until the flush fires and replies. If the deferred reply never fired, this would
    # hang and the test would fail on the GenServer.call timeout, so completing at all proves it works.
    for _ <- 1..5, do: {:ok, _} = BrokerServer.produce(broker, "t", batch(10))

    assert length(consume_all(broker, "t")) == 50
  end

  test "records are contiguous and ordered under group commit" do
    broker = start_broker()
    {:ok, _} = BrokerServer.create_topic(broker, "t", 8)

    for i <- 1..20, do: {:ok, _} = BrokerServer.produce(broker, "t", [Record.new("v#{i}", key: "k")])

    values = broker |> consume_all("t") |> Enum.map(& &1.value)
    assert values == Enum.map(1..20, &"v#{&1}")
  end

  test "concurrent producers all commit durably, and their fsyncs coalesce" do
    # A long window so the concurrent burst lands in one or two flushes; a counting store to see fsyncs.
    {broker, repl_dir} = start_broker_at(interval: 60, repl_opts: [store: FaultySegmentStore])
    {:ok, _} = BrokerServer.create_topic(broker, "t", 8)

    producers = 50

    results =
      1..producers
      |> Task.async_stream(fn _ -> BrokerServer.produce(broker, "t", batch(10)) end,
        max_concurrency: producers,
        timeout: 10_000
      )
      |> Enum.map(fn {:ok, r} -> r end)

    assert Enum.all?(results, &match?({:ok, _}, &1))
    assert length(consume_all(broker, "t")) == producers * 10

    # The point of group commit: 50 concurrent produces do NOT cost 50 fsyncs. A single-range topic is
    # one segment, so a flush of the whole burst is one fsync; allow a little slack for burst timing.
    fsyncs = FaultySegmentStore.count(repl_dir, :flushing_sync)
    assert fsyncs > 0
    assert fsyncs <= 5, "expected concurrent produces to coalesce, but saw #{fsyncs} fsyncs for #{producers} produces"
  end

  test "a routing error is returned immediately, not parked for a flush" do
    broker = start_broker(interval: 60)
    # No such topic: nothing is buffered, so the error must come back now (well under the flush interval),
    # not wait for a group flush.
    assert {:error, :no_such_topic} = BrokerServer.produce(broker, "missing", batch(1))
  end

  test "a storage failure in a live pipeline's flush fails the parked cycle too, and the broker stays up" do
    # The pipeline is alive and answers the flush, but a segment's sync failed: `flush/1` says so, and the
    # parked producers must get the same error a dead pipeline gives them, never an ack.
    {broker, repl_dir} = start_broker_at(interval: 60_000, repl_opts: [store: FaultySegmentStore])
    {:ok, _} = BrokerServer.create_topic(broker, "t", 8)
    FaultySegmentStore.fail(repl_dir, :sync, {:error, :enospc})

    tasks = for _ <- 1..3, do: Task.async(fn -> BrokerServer.produce(broker, "t", batch(2)) end)
    wait_until(fn -> length(:sys.get_state(broker).pending_produce) == 3 end)

    ExUnit.CaptureLog.capture_log(fn ->
      send(broker, :group_flush)

      for task <- tasks do
        assert {:error, :flush_failed} = Task.await(task, 5_000)
      end
    end)

    assert Process.alive?(broker)
  end

  test "a dead pipeline fails the flush cycle with an error, without killing the broker" do
    # Park producers behind a long flush interval, kill the replication pipeline, then force the
    # flush: every parked producer must get {:error, :flush_failed} (never an ack without a confirmed
    # fsync), and the broker must survive the pipeline's death instead of crashing on the flush call.
    tag = System.unique_integer([:positive])
    base = TmpDir.path("gc_dead")
    File.rm_rf!(base)
    repl = :"gc_dead_repl_#{tag}"
    {:ok, repl_pid} = ReplicationServer.start_link(name: repl, directory: Path.join(base, "repl"))

    {:ok, broker} =
      BrokerServer.start_link(Path.join(base, "broker"),
        brokers: [repl],
        group_commit: true,
        group_commit_interval_ms: 60_000
      )

    on_exit(fn ->
      stop_quietly(broker)
      File.rm_rf!(base)
    end)

    {:ok, _} = BrokerServer.create_topic(broker, "t", 8)

    tasks = for _ <- 1..3, do: Task.async(fn -> BrokerServer.produce(broker, "t", batch(2)) end)
    # Wait until all three are parked, then kill the pipeline and force the flush.
    wait_until(fn -> length(:sys.get_state(broker).pending_produce) == 3 end)
    GenServer.stop(repl_pid)
    send(broker, :group_flush)

    for task <- tasks do
      assert {:error, :flush_failed} = Task.await(task, 5_000)
    end

    assert Process.alive?(broker), "the broker must survive a dead pipeline during flush"
  end

  defp wait_until(fun, tries \\ 200) do
    cond do
      fun.() -> :ok
      tries == 0 -> flunk("condition never became true")
      true -> Process.sleep(5) && wait_until(fun, tries - 1)
    end
  end

  test "with group commit off, produce and consume still work (flag gates cleanly)" do
    broker = start_broker(group_commit: false)
    {:ok, _} = BrokerServer.create_topic(broker, "t", 8)
    {:ok, _} = BrokerServer.produce(broker, "t", batch(7))
    assert length(consume_all(broker, "t")) == 7
  end

  test "past the inflight cap, produces are shed with :overloaded and the broker survives" do
    # A tiny cap (20 records) and a long window so the burst parks without an interval flush clearing it;
    # a huge eager threshold so the cap, not the eager flush, is what trips. The first couple of produces
    # park (filling the cap); the rest are shed. The broker must stay alive and still commit the parked
    # ones once the window elapses.
    broker = start_broker(interval: 500, flush_max_records: 1_000_000, max_inflight: 20)
    {:ok, _} = BrokerServer.create_topic(broker, "t", 8)

    producers = 40

    results =
      1..producers
      |> Task.async_stream(fn _ -> BrokerServer.produce(broker, "t", batch(10)) end,
        max_concurrency: producers,
        timeout: 10_000
      )
      |> Enum.map(fn {:ok, r} -> r end)

    shed = Enum.count(results, &match?({:error, :overloaded}, &1))
    committed = Enum.count(results, &match?({:ok, _}, &1))

    assert shed > 0, "expected some produces to be shed with :overloaded, saw none"
    assert committed > 0, "expected the parked produces to still commit"
    assert shed + committed == producers, "every produce is either shed or committed, never dropped"
    assert Process.alive?(broker), "the broker must survive backpressure, not crash"
    assert length(consume_all(broker, "t")) == committed * 10
  end

  test "normal load is never shed as :overloaded (no false positives)" do
    # 50 concurrent produces of 10 records = 500, far under the default 200k cap: all must commit cleanly.
    broker = start_broker(interval: 20)
    {:ok, _} = BrokerServer.create_topic(broker, "t", 8)

    results =
      1..50
      |> Task.async_stream(fn _ -> BrokerServer.produce(broker, "t", batch(10)) end,
        max_concurrency: 50,
        timeout: 10_000
      )
      |> Enum.map(fn {:ok, r} -> r end)

    assert Enum.all?(results, &match?({:ok, _}, &1)), "no produce should be shed under normal load"
    assert length(consume_all(broker, "t")) == 500
  end

  test "eager flush returns a large produce without waiting for the interval timer" do
    # A very long interval but a low eager threshold: a produce at/over the threshold must flush right away
    # rather than blocking the caller for the full interval. If the eager path were missing, this call
    # would take ~the interval; we assert it returns well under it.
    broker = start_broker(interval: 5_000, flush_max_records: 10)
    {:ok, _} = BrokerServer.create_topic(broker, "t", 8)

    {elapsed_us, {:ok, _}} = :timer.tc(fn -> BrokerServer.produce(broker, "t", batch(20)) end)

    assert elapsed_us < 1_000_000, "eager flush should return in well under the 5s interval, took #{elapsed_us}us"
    assert length(consume_all(broker, "t")) == 20
  end
end
