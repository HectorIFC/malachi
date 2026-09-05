defmodule Malachi.GroupCommitTest do
  # async: false because the coalescing test uses a named ETS counter and leans on the flush timer.
  use ExUnit.Case, async: false

  import Malachi.Test.TeardownHelper

  alias Malachi.BrokerServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record

  # A SegmentStore that delegates everything to ElixirStore but counts the fsyncs that actually happen
  # (a sync of a store with no buffered records is a no-op and does not count). Lets a test prove that
  # many concurrent produces coalesce into few fsyncs.
  defmodule CountingStore do
    @behaviour Malachi.Storage.SegmentStore
    alias Malachi.Storage.ElixirStore

    @impl true
    def sync(handle) do
      if ElixirStore.pending?(handle), do: :ets.update_counter(:gc_sync_count, :n, 1)
      ElixirStore.sync(handle)
    end

    @impl true
    def open(dir, id, opts), do: ElixirStore.open(dir, id, opts)
    @impl true
    def recover(dir, id, opts), do: ElixirStore.recover(dir, id, opts)
    @impl true
    def open_read(dir, id, opts), do: ElixirStore.open_read(dir, id, opts)
    @impl true
    def append(handle, records), do: ElixirStore.append(handle, records)
    @impl true
    def read(handle, offset, max), do: ElixirStore.read(handle, offset, max)
    @impl true
    def seal(handle), do: ElixirStore.seal(handle)
    @impl true
    def next_offset(handle), do: ElixirStore.next_offset(handle)
    @impl true
    def size_bytes(handle), do: ElixirStore.size_bytes(handle)
    @impl true
    def sealed?(handle), do: ElixirStore.sealed?(handle)
    @impl true
    def pending?(handle), do: ElixirStore.pending?(handle)
    @impl true
    def should_seal?(handle, now_ms), do: ElixirStore.should_seal?(handle, now_ms)
    @impl true
    def close(handle), do: ElixirStore.close(handle)
    @impl true
    def verify(dir, id, opts), do: ElixirStore.verify(dir, id, opts)
    @impl true
    def integrity(handle), do: ElixirStore.integrity(handle)
    @impl true
    def rebuild_index(dir, id, opts), do: ElixirStore.rebuild_index(dir, id, opts)
  end

  # Boots an independent group-commit broker (its own ReplicationServer, dir, and topic) and registers
  # cleanup. `opts`: :interval (flush ms, default 10), :group_commit (default true), :repl_opts (e.g. a
  # custom :store), :flush_max_records (eager-flush threshold, default 8000), :max_inflight (overload
  # valve, default 200000). Returns the broker pid.
  defp start_broker(opts \\ []) do
    tag = System.unique_integer([:positive])
    base = Path.join(System.tmp_dir!(), "gc_test_#{tag}")
    File.rm_rf!(base)
    repl = :"gc_repl_#{tag}"

    {:ok, repl_pid} =
      ReplicationServer.start_link(
        [name: repl, directory: Path.join(base, "repl")] ++ Keyword.get(opts, :repl_opts, [])
      )

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
      File.rm_rf!(base)
    end)

    broker
  end

  defp batch(n, value \\ "v"), do: for(i <- 1..n, do: Record.new(value, key: "k#{i}"))

  defp consume_all(broker, topic) do
    consume_loop(broker, topic, %{}, [])
  end

  defp consume_loop(broker, topic, positions, acc) do
    case BrokerServer.consume(broker, topic, positions, 1000, 0) do
      {[], _next} -> Enum.reverse(acc)
      {records, next} -> consume_loop(broker, topic, next, Enum.reverse(records, acc))
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
    # Owned by (and auto-deleted with) this test process, so no explicit cleanup; public + named so the
    # CountingStore, which runs in the ReplicationServer process, can bump it.
    :ets.new(:gc_sync_count, [:named_table, :public, :set])
    :ets.insert(:gc_sync_count, {:n, 0})

    # A long window so the concurrent burst lands in one or two flushes; a custom store to count fsyncs.
    broker = start_broker(interval: 60, repl_opts: [store: CountingStore])
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
    [{:n, fsyncs}] = :ets.lookup(:gc_sync_count, :n)
    assert fsyncs > 0
    assert fsyncs <= 5, "expected concurrent produces to coalesce, but saw #{fsyncs} fsyncs for #{producers} produces"
  end

  test "a routing error is returned immediately, not parked for a flush" do
    broker = start_broker(interval: 60)
    # No such topic: nothing is buffered, so the error must come back now (well under the flush interval),
    # not wait for a group flush.
    assert {:error, :no_such_topic} = BrokerServer.produce(broker, "missing", batch(1))
  end

  test "a dead pipeline fails the flush cycle with an error, without killing the broker" do
    # Park producers behind a long flush interval, kill the replication pipeline, then force the
    # flush: every parked producer must get {:error, :flush_failed} (never an ack without a confirmed
    # fsync), and the broker must survive the pipeline's death instead of crashing on the flush call.
    tag = System.unique_integer([:positive])
    base = Path.join(System.tmp_dir!(), "gc_dead_#{tag}")
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
