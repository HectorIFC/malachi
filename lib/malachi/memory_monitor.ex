defmodule Malachi.MemoryMonitor do
  @moduledoc """
  Monitors system and process memory usage.
  Triggers automatic garbage collection when thresholds are exceeded.

  Provides real-time memory statistics for the dashboard and alerting.

  ## Configuration

    * `:memory_check_interval_ms` - Check interval (default: 30_000)
    * `:gc_threshold_mb` - Auto-GC when total exceeds this (default: 500)
    * `:auto_gc_enabled` - Enable automatic GC (default: true)
  """

  use GenServer
  require Logger
  alias Malachi.I18n

  defstruct [
    :check_interval_ms,
    :gc_threshold_mb,
    :auto_gc_enabled,
    :last_gc_at,
    :total_gc_runs,
    :total_reclaimed_bytes
  ]

  @doc "Starts the memory monitor, which periodically samples VM memory and can trigger GC under pressure."
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Returns a map with current memory usage in MB.

  ## Returns

      %{
        total_mb: 128.5,
        processes_mb: 64.2,
        ets_mb: 12.3,
        atom_mb: 1.1,
        binary_mb: 8.7,
        code_mb: 32.0,
        system_mb: 64.3
      }
  """
  @spec get_memory_stats() :: map()
  def get_memory_stats do
    memory = :erlang.memory()

    %{
      total_mb: bytes_to_mb(memory[:total]),
      processes_mb: bytes_to_mb(memory[:processes]),
      ets_mb: bytes_to_mb(memory[:ets]),
      atom_mb: bytes_to_mb(memory[:atom]),
      binary_mb: bytes_to_mb(memory[:binary]),
      code_mb: bytes_to_mb(memory[:code]),
      system_mb: bytes_to_mb(memory[:system])
    }
  end

  @doc """
  Returns the top N processes by memory consumption.

  ## Returns

      [%{pid: "#PID<0.100.0>", memory_mb: 5.2, name: SomeName, initial_call: {M, F, A}}, ...]
  """
  @spec get_top_memory_processes(pos_integer()) :: [map()]
  def get_top_memory_processes(n \\ 10) do
    Process.list()
    |> Enum.map(fn pid ->
      case Process.info(pid, [:memory, :registered_name, :initial_call]) do
        info when is_list(info) ->
          {pid, Keyword.get(info, :memory, 0), Keyword.get(info, :registered_name), Keyword.get(info, :initial_call)}

        nil ->
          nil
      end
    end)
    |> Enum.reject(&is_nil/1)
    |> Enum.sort_by(fn {_pid, memory, _name, _call} -> memory end, :desc)
    |> Enum.take(n)
    |> Enum.map(fn {pid, memory, name, initial_call} ->
      %{
        pid: inspect(pid),
        memory_mb: bytes_to_mb(memory),
        name: name,
        initial_call: format_initial_call(initial_call)
      }
    end)
  end

  @doc """
  Returns GC statistics from this monitor.
  """
  @spec get_gc_stats() :: map()
  def get_gc_stats do
    GenServer.call(__MODULE__, :get_gc_stats)
  end

  @doc """
  Manually triggers a system-wide garbage collection.
  """
  @spec trigger_gc() :: :ok
  def trigger_gc do
    GenServer.cast(__MODULE__, :trigger_gc)
  end

  # --- GenServer callbacks ---

  @impl true
  def init(_opts) do
    check_interval = Application.get_env(:malachi, :memory_check_interval_ms, 30_000)
    gc_threshold_mb = Application.get_env(:malachi, :gc_threshold_mb, 500)
    auto_gc = Application.get_env(:malachi, :auto_gc_enabled, true)

    schedule_check(check_interval)

    Logger.info(
      I18n.t(:memory_monitor_started,
        interval_ms: check_interval,
        gc_threshold_mb: gc_threshold_mb,
        auto_gc: auto_gc
      )
    )

    {:ok,
     %__MODULE__{
       check_interval_ms: check_interval,
       gc_threshold_mb: gc_threshold_mb,
       auto_gc_enabled: auto_gc,
       last_gc_at: nil,
       total_gc_runs: 0,
       total_reclaimed_bytes: 0
     }}
  end

  @impl true
  def handle_info(:check_memory, state) do
    stats = get_memory_stats()
    new_state = maybe_trigger_gc(state, stats)
    check_high_memory(stats)

    schedule_check(state.check_interval_ms)
    {:noreply, new_state}
  end

  @impl true
  def handle_cast(:trigger_gc, state) do
    new_state = do_system_gc(state)
    {:noreply, new_state}
  end

  @impl true
  def handle_call(:get_gc_stats, _from, state) do
    stats = %{
      total_gc_runs: state.total_gc_runs,
      total_reclaimed_mb: bytes_to_mb(state.total_reclaimed_bytes),
      last_gc_at: state.last_gc_at,
      gc_threshold_mb: state.gc_threshold_mb,
      auto_gc_enabled: state.auto_gc_enabled
    }

    {:reply, stats, state}
  end

  # --- Private helpers ---

  defp maybe_trigger_gc(state, stats) do
    if state.auto_gc_enabled and stats.total_mb > state.gc_threshold_mb do
      do_system_gc(state)
    else
      state
    end
  end

  defp do_system_gc(state) do
    before = :erlang.memory(:total)

    # GC all processes. Use :erlang.garbage_collect/1 which is safe for dead pids
    Process.list()
    |> Enum.each(fn pid ->
      try do
        :erlang.garbage_collect(pid)
      catch
        _, _ -> :ok
      end
    end)

    after_gc = :erlang.memory(:total)
    reclaimed = max(before - after_gc, 0)
    reclaimed_mb = bytes_to_mb(reclaimed)

    Logger.info(
      "System GC complete: reclaimed #{reclaimed_mb} MB " <>
        "(#{bytes_to_mb(before)} MB → #{bytes_to_mb(after_gc)} MB)"
    )

    %{
      state
      | last_gc_at: System.system_time(:second),
        total_gc_runs: state.total_gc_runs + 1,
        total_reclaimed_bytes: state.total_reclaimed_bytes + reclaimed
    }
  end

  defp check_high_memory(stats) do
    if stats.total_mb > 1000 do
      Logger.warning(
        "High memory usage: #{stats.total_mb} MB total " <>
          "(processes: #{stats.processes_mb} MB, ETS: #{stats.ets_mb} MB, " <>
          "binary: #{stats.binary_mb} MB)"
      )

      top_processes = get_top_memory_processes(5)

      Logger.warning(
        "Top memory consumers: " <>
          Enum.map_join(top_processes, ", ", fn p ->
            name = p.name || p.initial_call || p.pid
            "#{name}=#{p.memory_mb}MB"
          end)
      )
    end
  end

  defp bytes_to_mb(bytes) when is_integer(bytes) and bytes >= 0 do
    Float.round(bytes / 1_048_576, 2)
  end

  defp bytes_to_mb(_), do: 0.0

  defp format_initial_call({m, f, a}), do: "#{inspect(m)}.#{f}/#{a}"
  defp format_initial_call(other), do: inspect(other)

  defp schedule_check(interval) do
    Process.send_after(self(), :check_memory, interval)
  end
end
