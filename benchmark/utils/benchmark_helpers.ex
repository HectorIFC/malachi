defmodule BenchmarkHelpers do
  @moduledoc """
  Shared utilities for performance benchmarks.
  Provides timing, memory measurement, and common benchmark patterns.
  """

  @doc """
  Measures execution time in microseconds using monotonic time.
  Returns {duration_us, result}.
  """
  def measure_time(fun) do
    start = System.monotonic_time(:microsecond)
    result = fun.()
    duration = System.monotonic_time(:microsecond) - start
    {duration, result}
  end

  @doc """
  Measures memory usage in MB before and after executing a function.
  Forces garbage collection before measurement for accuracy.
  Returns {memory_used_mb, result}.
  """
  def measure_memory(fun) do
    :erlang.garbage_collect()
    Process.sleep(100)  # Allow GC to complete

    before_bytes = :erlang.memory(:total)
    result = fun.()

    :erlang.garbage_collect()
    Process.sleep(100)

    after_bytes = :erlang.memory(:total)
    memory_used_mb = (after_bytes - before_bytes) / 1_048_576

    {memory_used_mb, result}
  end

  @doc """
  Gets current memory usage by category in MB.
  """
  def get_memory_usage do
    :erlang.garbage_collect()
    Process.sleep(50)

    %{
      total_mb: :erlang.memory(:total) / 1_048_576,
      processes_mb: :erlang.memory(:processes) / 1_048_576,
      ets_mb: :erlang.memory(:ets) / 1_048_576,
      binary_mb: :erlang.memory(:binary) / 1_048_576,
      atom_mb: :erlang.memory(:atom) / 1_048_576
    }
  end

  @doc """
  Executes a warm-up phase to allow JIT compilation and memory allocation.
  Runs the function for the specified duration in seconds.
  """
  def warmup(fun, duration_sec \\ 10) do
    if ci_mode?() do
      IO.puts("Warming up (500 iterations)...")
      Enum.each(1..500, fn _ -> fun.() end)
      :erlang.garbage_collect()
      Process.sleep(500)
    else
      IO.puts("Warming up for #{duration_sec} seconds...")
      end_time = System.monotonic_time(:second) + duration_sec
      warmup_loop(fun, end_time, 0)
    end

    IO.puts("Warm-up complete")
  end

  defp warmup_loop(fun, end_time, count) do
    if System.monotonic_time(:second) < end_time do
      fun.()
      warmup_loop(fun, end_time, count + 1)
    end
  end

  @doc """
  Runs a benchmark for the specified duration and returns {iteration_count, actual_duration_sec}.
  In CI mode, runs a fixed number of iterations instead of timing.
  """
  def benchmark_duration(fun, duration_sec) do
    if ci_mode?() do
      count = 5_000
      start_time = System.monotonic_time(:microsecond)
      Enum.each(1..count, fn _ -> fun.() end)
      actual_duration_sec = (System.monotonic_time(:microsecond) - start_time) / 1_000_000
      {count, actual_duration_sec}
    else
      start_time = System.monotonic_time(:microsecond)
      end_time = start_time + (duration_sec * 1_000_000)
      count = do_benchmark_duration(fun, end_time, 0)
      actual_duration_sec = (System.monotonic_time(:microsecond) - start_time) / 1_000_000
      {count, actual_duration_sec}
    end
  end

  defp do_benchmark_duration(fun, end_time, count) do
    if System.monotonic_time(:microsecond) < end_time do
      fun.()
      do_benchmark_duration(fun, end_time, count + 1)
    else
      count
    end
  end

  @doc """
  Gets MalachiMQ version from mix.exs.
  """
  def get_version do
    case File.read("mix.exs") do
      {:ok, content} ->
        case Regex.run(~r/@version\s+"([^"]+)"/, content) do
          [_, version] -> version
          _ -> "unknown"
        end

      _ ->
        "unknown"
    end
  end

  @doc """
  Creates a unique queue name for benchmarks.
  """
  def unique_queue_name(prefix \\ "bench") do
    "#{prefix}_#{:rand.uniform(1_000_000)}_#{System.system_time(:millisecond)}"
  end

  @doc """
  Ensures benchmark results directory exists.
  """
  def ensure_results_dir do
    dir = "benchmark/results"
    File.mkdir_p!(dir)
    dir
  end

  @doc """
  Generates timestamped filename for results.
  """
  def timestamped_filename(prefix) do
    {{year, month, day}, {hour, minute, second}} = :calendar.local_time()

    filename =
      "#{prefix}_#{year}#{pad(month)}#{pad(day)}_#{pad(hour)}#{pad(minute)}#{pad(second)}.json"

    Path.join(ensure_results_dir(), filename)
  end

  defp pad(num) when num < 10, do: "0#{num}"
  defp pad(num), do: "#{num}"

  @doc """
  Gets system information for benchmark context.
  """
  def get_system_info do
    %{
      "schedulers_online" => :erlang.system_info(:schedulers_online),
      "process_count" => :erlang.system_info(:process_count),
      "process_limit" => :erlang.system_info(:process_limit),
      "ets_limit" => :erlang.system_info(:ets_limit),
      "os" => "#{:erlang.system_info(:os_type) |> elem(0)} #{:erlang.system_info(:os_type) |> elem(1)}",
      "otp_release" => :erlang.system_info(:otp_release) |> to_string(),
      "beam_version" => :erlang.system_info(:version) |> to_string()
    }
  end

  @doc """
  Starts a consumer for benchmarks without linking to the caller.
  Uses start_link (required by Consumer) then immediately unlinks,
  so Process.exit(:kill) from kill_all_consumers won't cascade.
  """
  def start_benchmark_consumer(queue_name, callback, opts \\ []) do
    {:ok, pid} = MalachiMQ.Consumer.start_link({queue_name, callback, opts})
    Process.unlink(pid)
    {:ok, pid}
  end

  @doc """
  Cleans up benchmark resources (queue, consumers, metrics).
  Traps exits during cleanup to prevent kill_all_consumers from
  crashing the caller via linked process EXIT signals.
  """
  def cleanup_queue(queue_name) do
    old_trap = Process.flag(:trap_exit, true)

    try do
      MalachiMQ.Queue.kill_all_consumers(queue_name)
    rescue
      _ -> :ok
    catch
      :exit, _ -> :ok
    end

    # Drain any EXIT messages from killed consumers
    drain_exit_messages()

    try do
      MalachiMQ.Metrics.reset_metrics(queue_name)
    rescue
      _ -> :ok
    catch
      :exit, _ -> :ok
    end

    Process.flag(:trap_exit, old_trap)
    Process.sleep(100)
  end

  defp drain_exit_messages do
    receive do
      {:EXIT, _, _} -> drain_exit_messages()
    after
      0 -> :ok
    end
  end

  @doc """
  Formats a number with thousands separators.
  """
  def format_number(num) when is_integer(num) do
    num
    |> Integer.to_string()
    |> String.reverse()
    |> String.split("", trim: true)
    |> Enum.chunk_every(3)
    |> Enum.join(",")
    |> String.reverse()
  end

  def format_number(num) when is_float(num) do
    Float.round(num, 2)
  end

  @doc """
  Formats bytes to MB with 2 decimal places.
  """
  def bytes_to_mb(bytes) do
    Float.round(bytes / 1_048_576, 2)
  end

  @doc """
  Returns true when running in CI (GitHub Actions sets CI=true).
  """
  def ci_mode?, do: System.get_env("CI") != nil

  @doc """
  Returns the default warm-up duration in seconds (shorter in CI).
  """
  def default_warmup_sec, do: if(ci_mode?(), do: 2, else: 10)

  @doc """
  Returns the default benchmark duration in seconds (shorter in CI).
  """
  def default_duration_sec, do: if(ci_mode?(), do: 5, else: 60)
end
