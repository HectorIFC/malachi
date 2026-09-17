defmodule Malachi.MetricsTest do
  use ExUnit.Case, async: false

  # After B3b the per-queue/channel metrics are gone; what remains is the system snapshot (memory,
  # processes, connections, auth/TLS/validation counters) and the periodic history of those snapshots.

  describe "get_system_metrics/0" do
    test "returns system metrics" do
      metrics = Malachi.Metrics.get_system_metrics()

      assert is_integer(metrics.timestamp)
      assert is_integer(metrics.schedulers_online)
      assert metrics.schedulers_online > 0
      assert is_integer(metrics.process_count)
      assert metrics.process_count > 0
      assert is_integer(metrics.process_limit)
      assert is_integer(metrics.run_queue)
      assert is_map(metrics.memory)
      assert is_float(metrics.memory.total_mb)
      assert metrics.memory.total_mb > 0
      assert is_integer(metrics.ets_tables)
      assert is_map(metrics.io)
      assert is_integer(metrics.uptime_seconds)
    end

    test "memory breakdown is present" do
      metrics = Malachi.Metrics.get_system_metrics()

      assert is_float(metrics.memory.processes_mb)
      assert is_float(metrics.memory.ets_mb)
      assert is_float(metrics.memory.atom_mb)
      assert is_float(metrics.memory.binary_mb)
    end
  end

  describe "storage flush state" do
    @flush_key {Malachi.Metrics, :storage_flush}

    test "the bucket list is left out of the per-second snapshot" do
      summary = Malachi.Metrics.get_system_metrics().storage_flush

      assert Map.keys(summary) |> Enum.sort() == [:bytes, :count, :p50_us, :p999_us, :p99_us, :records, :sum_us]
    end

    test "storage_flush_histogram/0 lists every exported edge" do
      histogram = Malachi.Metrics.storage_flush_histogram()

      assert Enum.map(histogram.buckets, &elem(&1, 0)) == Malachi.Histogram.edges()
      assert Enum.all?([histogram.count, histogram.sum_us, histogram.bytes, histogram.records], &is_integer/1)
    end

    test "without the state, recording is a no-op and every reading is zero" do
      state = :persistent_term.get(@flush_key)
      :persistent_term.erase(@flush_key)

      try do
        assert Malachi.Metrics.record_flush(1000, 10, 1) == :ok

        assert %{count: 0, sum_us: 0, bytes: 0, records: 0, buckets: buckets} =
                 Malachi.Metrics.storage_flush_histogram()

        assert Enum.all?(buckets, fn {_edge, count} -> count == 0 end)
        assert length(buckets) == length(Malachi.Histogram.edges())

        assert Malachi.Metrics.get_system_metrics().storage_flush ==
                 %{count: 0, sum_us: 0, bytes: 0, records: 0, p50_us: 0.0, p99_us: 0.0, p999_us: 0.0}
      after
        :persistent_term.put(@flush_key, state)
      end
    end

    test "a Metrics restart keeps the recorded flushes" do
      :ok = Malachi.Metrics.record_flush(1000, 10, 1)
      before = Malachi.Metrics.storage_flush_histogram()
      state = :persistent_term.get(@flush_key)

      :ok = Supervisor.terminate_child(Malachi.Supervisor, Malachi.Metrics)
      {:ok, _pid} = Supervisor.restart_child(Malachi.Supervisor, Malachi.Metrics)

      assert :persistent_term.get(@flush_key) == state
      assert Malachi.Metrics.storage_flush_histogram().count >= before.count

      # And the re-attached reporter still folds flush events into it.
      Malachi.Telemetry.storage_flush(1000, 10, 1, "segment-0", "/tmp/seg")
      assert Malachi.Metrics.storage_flush_histogram().count >= before.count + 1
    end
  end

  describe "metrics history" do
    test "takes snapshots periodically" do
      original = Application.get_env(:malachi, :metrics_snapshot_interval_ms)
      Application.put_env(:malachi, :metrics_snapshot_interval_ms, 100)

      # Restart Metrics to pick up new snapshot interval
      Supervisor.terminate_child(Malachi.Supervisor, Malachi.Metrics)
      Supervisor.restart_child(Malachi.Supervisor, Malachi.Metrics)

      on_exit(fn ->
        if original do
          Application.put_env(:malachi, :metrics_snapshot_interval_ms, original)
        else
          Application.delete_env(:malachi, :metrics_snapshot_interval_ms)
        end

        # Restart again to restore original interval
        Supervisor.terminate_child(Malachi.Supervisor, Malachi.Metrics)
        Supervisor.restart_child(Malachi.Supervisor, Malachi.Metrics)
      end)

      :timer.sleep(250)

      history = Malachi.Metrics.get_history(60)
      assert is_list(history)
      assert history != []
    end

    test "get_history returns system snapshots within the time window" do
      history = Malachi.Metrics.get_history(10)

      assert is_list(history)

      Enum.each(history, fn snapshot ->
        assert is_map(snapshot)
        assert Map.has_key?(snapshot, :timestamp)
        assert Map.has_key?(snapshot, :system)
      end)
    end

    test "cleans up old snapshots" do
      original_history = Application.get_env(:malachi, :metrics_history_seconds)
      original_cleanup = Application.get_env(:malachi, :metrics_cleanup_interval_ms)

      Application.put_env(:malachi, :metrics_history_seconds, 1)
      Application.put_env(:malachi, :metrics_cleanup_interval_ms, 500)

      on_exit(fn ->
        if original_history do
          Application.put_env(:malachi, :metrics_history_seconds, original_history)
        else
          Application.delete_env(:malachi, :metrics_history_seconds)
        end

        if original_cleanup do
          Application.put_env(:malachi, :metrics_cleanup_interval_ms, original_cleanup)
        else
          Application.delete_env(:malachi, :metrics_cleanup_interval_ms)
        end
      end)

      :timer.sleep(1500)
    end
  end
end
