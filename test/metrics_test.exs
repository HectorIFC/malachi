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
