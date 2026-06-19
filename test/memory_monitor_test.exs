defmodule Malachi.MemoryMonitorTest do
  use ExUnit.Case, async: true

  @moduledoc """
  Tests for the MemoryMonitor GenServer.
  Verifies memory statistics, GC triggering, and top process identification.
  """

  describe "get_memory_stats/0" do
    test "returns all required memory fields" do
      stats = Malachi.MemoryMonitor.get_memory_stats()

      assert is_map(stats)
      assert is_float(stats.total_mb)
      assert is_float(stats.processes_mb)
      assert is_float(stats.ets_mb)
      assert is_float(stats.atom_mb)
      assert is_float(stats.binary_mb)
      assert is_float(stats.code_mb)
      assert is_float(stats.system_mb)
    end

    test "memory values are positive" do
      stats = Malachi.MemoryMonitor.get_memory_stats()

      assert stats.total_mb > 0
      assert stats.processes_mb > 0
      assert stats.ets_mb > 0
      assert stats.atom_mb > 0
    end

    test "total memory is sum of processes and system" do
      stats = Malachi.MemoryMonitor.get_memory_stats()

      # Total should approximately equal processes + system
      # (with rounding tolerance)
      sum = stats.processes_mb + stats.system_mb
      assert_in_delta stats.total_mb, sum, 1.0
    end
  end

  describe "get_top_memory_processes/1" do
    test "returns list of process info maps" do
      top = Malachi.MemoryMonitor.get_top_memory_processes(5)

      assert is_list(top)
      assert length(top) <= 5

      for proc <- top do
        assert is_map(proc)
        assert Map.has_key?(proc, :pid)
        assert Map.has_key?(proc, :memory_mb)
        assert Map.has_key?(proc, :name)
        assert is_float(proc.memory_mb)
      end
    end

    test "returns processes sorted by memory descending" do
      top = Malachi.MemoryMonitor.get_top_memory_processes(10)

      memory_values = Enum.map(top, & &1.memory_mb)

      assert memory_values == Enum.sort(memory_values, :desc),
             "Top processes should be sorted by memory descending"
    end

    test "default returns 10 processes" do
      top = Malachi.MemoryMonitor.get_top_memory_processes()

      assert length(top) <= 10
      assert top != []
    end
  end

  describe "get_gc_stats/0" do
    test "returns GC statistics map" do
      stats = Malachi.MemoryMonitor.get_gc_stats()

      assert is_map(stats)
      assert Map.has_key?(stats, :total_gc_runs)
      assert Map.has_key?(stats, :total_reclaimed_mb)
      assert Map.has_key?(stats, :gc_threshold_mb)
      assert Map.has_key?(stats, :auto_gc_enabled)

      assert is_integer(stats.total_gc_runs)
      assert stats.total_gc_runs >= 0
      assert is_float(stats.total_reclaimed_mb)
      assert is_boolean(stats.auto_gc_enabled)
    end
  end

  describe "trigger_gc/0" do
    test "manual GC trigger completes without error" do
      # Should not raise
      assert :ok = Malachi.MemoryMonitor.trigger_gc()

      # Give it a moment to process the cast
      Process.sleep(100)

      stats = Malachi.MemoryMonitor.get_gc_stats()
      assert stats.total_gc_runs >= 1
    end
  end

  describe "memory stats integration with metrics" do
    test "system metrics include memory details" do
      system_metrics = Malachi.Metrics.get_system_metrics()

      assert Map.has_key?(system_metrics, :memory_details)

      if map_size(system_metrics.memory_details) > 0 do
        details = system_metrics.memory_details
        assert is_float(details.total_mb)
        assert is_float(details.ets_mb)
      end
    end

    test "system metrics include atom table stats" do
      system_metrics = Malachi.Metrics.get_system_metrics()

      assert Map.has_key?(system_metrics, :atom_table)
      atom_stats = system_metrics.atom_table

      assert Map.has_key?(atom_stats, :atom_count)
      assert Map.has_key?(atom_stats, :atom_limit)
      assert Map.has_key?(atom_stats, :usage_percent)
      assert Map.has_key?(atom_stats, :status)
    end
  end
end
