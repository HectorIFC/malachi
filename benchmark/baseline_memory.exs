#!/usr/bin/env elixir

# Memory Baseline Benchmark
# Measures memory usage under various loads

Code.require_file("utils/benchmark_helpers.ex", "benchmark")
Code.require_file("utils/percentile.ex", "benchmark")
Code.require_file("utils/reporter.ex", "benchmark")

defmodule MemoryBenchmark do

  def run do
    IO.puts("\n#{IO.ANSI.cyan()}Starting Memory Baseline Benchmark#{IO.ANSI.reset()}\n")

    # Start the application
    Application.ensure_all_started(:malachimq)
    Process.sleep(1000)

    results = %{
      "benchmark" => "baseline_memory",
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "version" => BenchmarkHelpers.get_version(),
      "system_info" => BenchmarkHelpers.get_system_info(),
      "results" => %{
        "memory_per_connection" => run_memory_per_connection(),
        "memory_per_buffered_message" => run_memory_per_message(),
        "memory_multiple_queues" => run_memory_multiple_queues()
      }
    }

    # Save results
    filename = BenchmarkHelpers.timestamped_filename("memory")
    BenchmarkReporter.save_results(results, filename)

    # Display results
    BenchmarkReporter.display_results(results, "console")

    IO.puts("\n#{IO.ANSI.green()}✓ Memory benchmark complete#{IO.ANSI.reset()}\n")
  end

  defp run_memory_per_connection do
    IO.puts("\n#{IO.ANSI.yellow()}Test: Memory per Connection#{IO.ANSI.reset()}")

    connection_counts = if(BenchmarkHelpers.ci_mode?(), do: [10, 100, 200], else: [10, 100, 1000])

    results =
      Enum.map(connection_counts, fn count ->
        IO.puts("  Testing #{count} connections...")
        memory_usage = measure_consumer_memory(count)

        {count, memory_usage}
      end)

    # Calculate average memory per connection
    memory_data =
      Enum.map(results, fn {count, usage} ->
        %{
          "connection_count" => count,
          "total_memory_mb" => usage.total_mb,
          "memory_per_connection_mb" => usage.per_connection_mb
        }
      end)

    # Return structured data
    avg_per_connection =
      Enum.map(results, fn {_count, usage} -> usage.per_connection_mb end)
      |> Enum.sum()
      |> Kernel./(length(results))

    Map.merge(
      %{"average_memory_per_connection_mb" => Float.round(avg_per_connection, 3)},
      Enum.with_index(memory_data, 1)
      |> Enum.map(fn {data, idx} -> {"test_#{idx}", data} end)
      |> Enum.into(%{})
    )
  end

  defp measure_consumer_memory(count) do
    queue_name = BenchmarkHelpers.unique_queue_name("memory_conn")

    # Measure baseline memory
    :erlang.garbage_collect()
    Process.sleep(200)
    before_memory = BenchmarkHelpers.get_memory_usage()

    # Start consumers
    _consumers =
      Enum.map(1..count, fn _ ->
        {:ok, pid} = BenchmarkHelpers.start_benchmark_consumer(queue_name, fn _ -> :ok end)
        pid
      end)

    Process.sleep(500)

    # Measure after
    :erlang.garbage_collect()
    Process.sleep(200)
    after_memory = BenchmarkHelpers.get_memory_usage()

    # Calculate difference
    total_mb = after_memory.total_mb - before_memory.total_mb
    per_connection_mb = total_mb / count

    # Cleanup
    BenchmarkHelpers.cleanup_queue(queue_name)

    %{
      total_mb: Float.round(total_mb, 2),
      per_connection_mb: Float.round(per_connection_mb, 3)
    }
  end

  defp run_memory_per_message do
    IO.puts("\n#{IO.ANSI.yellow()}Test: Memory per Buffered Message#{IO.ANSI.reset()}")

    message_counts = if(BenchmarkHelpers.ci_mode?(), do: [100, 1_000, 10_000], else: [1000, 10_000, 100_000])
    message_size = 1024  # 1KB messages

    results =
      Enum.map(message_counts, fn count ->
        IO.puts("  Testing #{count} buffered messages...")
        memory_usage = measure_message_buffer_memory(count, message_size)

        {count, memory_usage}
      end)

    # Calculate average memory per message
    memory_data =
      Enum.map(results, fn {count, usage} ->
        %{
          "message_count" => count,
          "message_size_bytes" => message_size,
          "total_memory_mb" => usage.total_mb,
          "memory_per_1000_messages_mb" => usage.per_1000_messages_mb
        }
      end)

    # Return structured data
    avg_per_1000_messages =
      Enum.map(results, fn {_count, usage} -> usage.per_1000_messages_mb end)
      |> Enum.sum()
      |> Kernel./(length(results))

    Map.merge(
      %{"average_memory_per_1000_messages_mb" => Float.round(avg_per_1000_messages, 3)},
      Enum.with_index(memory_data, 1)
      |> Enum.map(fn {data, idx} -> {"test_#{idx}", data} end)
      |> Enum.into(%{})
    )
  end

  defp measure_message_buffer_memory(count, size) do
    queue_name = BenchmarkHelpers.unique_queue_name("memory_msg")
    payload = :crypto.strong_rand_bytes(size)

    # Don't start consumer - messages will buffer

    # Measure baseline memory
    :erlang.garbage_collect()
    Process.sleep(200)
    before_memory = BenchmarkHelpers.get_memory_usage()

    # Enqueue messages
    Enum.each(1..count, fn _ ->
      MalachiMQ.Queue.enqueue(queue_name, payload)
    end)

    Process.sleep(500)

    # Measure after
    :erlang.garbage_collect()
    Process.sleep(200)
    after_memory = BenchmarkHelpers.get_memory_usage()

    # Calculate difference
    total_mb = after_memory.total_mb - before_memory.total_mb
    per_1000_messages_mb = (total_mb / count) * 1000

    # Cleanup
    BenchmarkHelpers.cleanup_queue(queue_name)

    %{
      total_mb: Float.round(total_mb, 2),
      per_1000_messages_mb: Float.round(per_1000_messages_mb, 3)
    }
  end

  defp run_memory_multiple_queues do
    queue_count = if(BenchmarkHelpers.ci_mode?(), do: 100, else: 1000)
    IO.puts("\n#{IO.ANSI.yellow()}Test: Memory with #{queue_count} Queues#{IO.ANSI.reset()}")
    payload = :crypto.strong_rand_bytes(1024)

    # Measure baseline memory
    :erlang.garbage_collect()
    Process.sleep(200)
    before_memory = BenchmarkHelpers.get_memory_usage()

    # Create queues with consumers
    IO.puts("  Creating #{queue_count} queues with consumers...")

    queues =
      Enum.map(1..queue_count, fn i ->
        queue_name = "memory_multi_#{i}"

        {:ok, _pid} = BenchmarkHelpers.start_benchmark_consumer(queue_name, fn _ -> :ok end)

        # Send one message to each queue
        MalachiMQ.Queue.enqueue(queue_name, payload)

        queue_name
      end)

    Process.sleep(1000)

    # Measure after
    :erlang.garbage_collect()
    Process.sleep(200)
    after_memory = BenchmarkHelpers.get_memory_usage()

    # Calculate difference
    total_mb = after_memory.total_mb - before_memory.total_mb
    per_queue_mb = total_mb / queue_count

    IO.puts("  Total memory: #{Float.round(total_mb, 2)} MB")
    IO.puts("  Per queue: #{Float.round(per_queue_mb, 3)} MB")

    # Cleanup
    Enum.each(queues, &BenchmarkHelpers.cleanup_queue/1)

    %{
      "queue_count" => queue_count,
      "total_memory_mb" => Float.round(total_mb, 2),
      "memory_per_queue_mb" => Float.round(per_queue_mb, 3),
      "ets_memory_mb" => Float.round(after_memory.ets_mb - before_memory.ets_mb, 2),
      "processes_memory_mb" => Float.round(after_memory.processes_mb - before_memory.processes_mb, 2)
    }
  end
end

# Run the benchmark
MemoryBenchmark.run()
