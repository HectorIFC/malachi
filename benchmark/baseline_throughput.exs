#!/usr/bin/env elixir

# Throughput Baseline Benchmark
# Measures message processing throughput under various scenarios

Code.require_file("utils/benchmark_helpers.ex", "benchmark")
Code.require_file("utils/percentile.ex", "benchmark")
Code.require_file("utils/reporter.ex", "benchmark")

defmodule ThroughputBenchmark do
  @warmup_sec BenchmarkHelpers.default_warmup_sec()
  @duration_sec BenchmarkHelpers.default_duration_sec()
  @message_sizes if(BenchmarkHelpers.ci_mode?(), do: [100, 1024, 10_240], else: [100, 1024, 10_240, 102_400])

  def run do
    IO.puts("\n#{IO.ANSI.cyan()}Starting Throughput Baseline Benchmark#{IO.ANSI.reset()}\n")

    # Start the application
    Application.ensure_all_started(:malachimq)
    Process.sleep(1000)

    results = %{
      "benchmark" => "baseline_throughput",
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "version" => BenchmarkHelpers.get_version(),
      "system_info" => BenchmarkHelpers.get_system_info(),
      "results" => %{
        "single_producer_consumer" => run_single_producer_consumer(),
        "multi_producer_consumer" => run_multi_producer_consumer(),
        "concurrent_connections" => run_concurrent_connections(),
        "burst_traffic" => run_burst_traffic()
      }
    }

    # Save results
    filename = BenchmarkHelpers.timestamped_filename("throughput")
    BenchmarkReporter.save_results(results, filename)

    # Display results
    BenchmarkReporter.display_results(results, "console")

    IO.puts("\n#{IO.ANSI.green()}✓ Throughput benchmark complete#{IO.ANSI.reset()}\n")
  end

  defp run_single_producer_consumer do
    IO.puts("\n#{IO.ANSI.yellow()}Test: Single Producer/Consumer (varying message sizes)#{IO.ANSI.reset()}")

    @message_sizes
    |> Enum.map(fn size ->
      result = benchmark_message_size(size)
      {size, result}
    end)
    |> Enum.into(%{})
  end

  defp benchmark_message_size(size) do
    queue_name = BenchmarkHelpers.unique_queue_name("throughput_single")
    payload = :crypto.strong_rand_bytes(size)

    IO.puts("  Benchmarking #{size} byte messages...")

    # Start consumer (unlinked to prevent kill cascade during cleanup)
    received_count = :atomics.new(1, [])

    callback = fn _msg ->
      :atomics.add(received_count, 1, 1)
      :ok
    end

    {:ok, _consumer} = BenchmarkHelpers.start_benchmark_consumer(queue_name, callback)
    Process.sleep(100)

    # Warm-up
    BenchmarkHelpers.warmup(fn ->
      MalachiMQ.Queue.enqueue(queue_name, payload)
    end, @warmup_sec)

    # Clear metrics
    MalachiMQ.Metrics.reset_metrics(queue_name)
    :atomics.put(received_count, 1, 0)

    # Benchmark
    start_time = System.monotonic_time(:millisecond)

    {message_count, _actual_sec} =
      BenchmarkHelpers.benchmark_duration(
        fn -> MalachiMQ.Queue.enqueue(queue_name, payload) end,
        @duration_sec
      )

    duration_ms = System.monotonic_time(:millisecond) - start_time

    # Wait for all messages to be processed
    Process.sleep(1000)

    # Get metrics
    metrics = MalachiMQ.Metrics.get_metrics(queue_name)
    processed = :atomics.get(received_count, 1)

    # Calculate throughput
    duration_sec = duration_ms / 1000
    msgs_per_sec = processed / duration_sec
    bytes_per_sec = processed * size / duration_sec
    mb_per_sec = bytes_per_sec / 1_048_576

    # Cleanup
    BenchmarkHelpers.cleanup_queue(queue_name)

    %{
      "message_size_bytes" => size,
      "duration_sec" => Float.round(duration_sec, 2),
      "messages_sent" => message_count,
      "messages_processed" => processed,
      "throughput_msgs_per_sec" => Float.round(msgs_per_sec, 2),
      "throughput_mb_per_sec" => Float.round(mb_per_sec, 2),
      "latency_avg_us" => Float.round(Map.get(metrics.latency_us || %{}, :avg, 0) * 1.0, 2)
    }
  end

  defp run_multi_producer_consumer do
    IO.puts("\n#{IO.ANSI.yellow()}Test: Multiple Producers/Consumers (10 each, 1KB messages)#{IO.ANSI.reset()}")

    queue_name = BenchmarkHelpers.unique_queue_name("throughput_multi")
    payload = :crypto.strong_rand_bytes(1024)
    producer_count = if(BenchmarkHelpers.ci_mode?(), do: 5, else: 10)
    consumer_count = if(BenchmarkHelpers.ci_mode?(), do: 5, else: 10)

    # Start consumers
    received_count = :atomics.new(1, [])

    callback = fn _msg ->
      :atomics.add(received_count, 1, 1)
      :ok
    end

    _consumers =
      Enum.map(1..consumer_count, fn _ ->
        {:ok, pid} = BenchmarkHelpers.start_benchmark_consumer(queue_name, callback)
        pid
      end)

    Process.sleep(100)

    # Warm-up
    IO.puts("  Warming up...")

    BenchmarkHelpers.warmup(fn ->
      MalachiMQ.Queue.enqueue(queue_name, payload)
    end, @warmup_sec)

    # Clear metrics
    MalachiMQ.Metrics.reset_metrics(queue_name)
    :atomics.put(received_count, 1, 0)

    # Benchmark - each producer sends messages for duration
    IO.puts("  Running benchmark...")
    start_time = System.monotonic_time(:millisecond)

    tasks =
      Enum.map(1..producer_count, fn _ ->
        Task.async(fn ->
          {count, _actual_sec} =
            BenchmarkHelpers.benchmark_duration(
              fn -> MalachiMQ.Queue.enqueue(queue_name, payload) end,
              @duration_sec
            )

          count
        end)
      end)

    message_counts = Task.await_many(tasks, :infinity)
    total_sent = Enum.sum(message_counts)

    duration_ms = System.monotonic_time(:millisecond) - start_time

    # Wait for processing
    Process.sleep(2000)

    # Get metrics
    metrics = MalachiMQ.Metrics.get_metrics(queue_name)
    processed = :atomics.get(received_count, 1)

    # Calculate throughput
    duration_sec = duration_ms / 1000
    msgs_per_sec = processed / duration_sec
    mb_per_sec = (processed * 1024) / duration_sec / 1_048_576

    # Cleanup
    BenchmarkHelpers.cleanup_queue(queue_name)

    %{
      "producer_count" => producer_count,
      "consumer_count" => consumer_count,
      "message_size_bytes" => 1024,
      "duration_sec" => Float.round(duration_sec, 2),
      "messages_sent" => total_sent,
      "messages_processed" => processed,
      "throughput_msgs_per_sec" => Float.round(msgs_per_sec, 2),
      "throughput_mb_per_sec" => Float.round(mb_per_sec, 2),
      "latency_avg_us" => Float.round(Map.get(metrics.latency_us || %{}, :avg, 0) * 1.0, 2)
    }
  end

  defp run_concurrent_connections do
    IO.puts("\n#{IO.ANSI.yellow()}Test: Concurrent Connections (100 producers/consumers, 1KB messages)#{IO.ANSI.reset()}")

    queue_name = BenchmarkHelpers.unique_queue_name("throughput_concurrent")
    payload = :crypto.strong_rand_bytes(1024)
    connection_count = if(BenchmarkHelpers.ci_mode?(), do: 20, else: 100)
    messages_per_connection = if(BenchmarkHelpers.ci_mode?(), do: 100, else: 1000)

    # Start consumers
    received_count = :atomics.new(1, [])

    callback = fn _msg ->
      :atomics.add(received_count, 1, 1)
      :ok
    end

    IO.puts("  Starting #{connection_count} consumers...")

    _consumers =
      Enum.map(1..connection_count, fn _ ->
        {:ok, pid} = BenchmarkHelpers.start_benchmark_consumer(queue_name, callback)
        pid
      end)

    Process.sleep(500)

    # Warm-up
    IO.puts("  Warming up...")

    BenchmarkHelpers.warmup(fn ->
      MalachiMQ.Queue.enqueue(queue_name, payload)
    end, @warmup_sec)

    # Clear metrics
    MalachiMQ.Metrics.reset_metrics(queue_name)
    :atomics.put(received_count, 1, 0)

    # Benchmark - all connections send messages simultaneously
    IO.puts("  Running benchmark...")
    start_time = System.monotonic_time(:millisecond)

    tasks =
      Enum.map(1..connection_count, fn _ ->
        Task.async(fn ->
          Enum.each(1..messages_per_connection, fn _ ->
            MalachiMQ.Queue.enqueue(queue_name, payload)
          end)

          messages_per_connection
        end)
      end)

    Task.await_many(tasks, :infinity)
    total_sent = connection_count * messages_per_connection

    duration_ms = System.monotonic_time(:millisecond) - start_time

    # Wait for processing
    Process.sleep(2000)

    # Get metrics
    metrics = MalachiMQ.Metrics.get_metrics(queue_name)
    processed = :atomics.get(received_count, 1)

    # Calculate throughput
    duration_sec = duration_ms / 1000
    msgs_per_sec = processed / duration_sec
    mb_per_sec = (processed * 1024) / duration_sec / 1_048_576

    # Cleanup
    BenchmarkHelpers.cleanup_queue(queue_name)

    %{
      "concurrent_connections" => connection_count,
      "messages_per_connection" => messages_per_connection,
      "message_size_bytes" => 1024,
      "duration_sec" => Float.round(duration_sec, 2),
      "messages_sent" => total_sent,
      "messages_processed" => processed,
      "throughput_msgs_per_sec" => Float.round(msgs_per_sec, 2),
      "throughput_mb_per_sec" => Float.round(mb_per_sec, 2),
      "latency_avg_us" => Float.round(Map.get(metrics.latency_us || %{}, :avg, 0) * 1.0, 2)
    }
  end

  defp run_burst_traffic do
    IO.puts("\n#{IO.ANSI.yellow()}Test: Burst Traffic (1000 messages in 1 second, then idle)#{IO.ANSI.reset()}")

    queue_name = BenchmarkHelpers.unique_queue_name("throughput_burst")
    payload = :crypto.strong_rand_bytes(1024)
    burst_size = 1000

    # Start consumer
    received_count = :atomics.new(1, [])

    callback = fn _msg ->
      :atomics.add(received_count, 1, 1)
      :ok
    end

    {:ok, _consumer} = BenchmarkHelpers.start_benchmark_consumer(queue_name, callback)
    Process.sleep(100)

    # Warm-up
    BenchmarkHelpers.warmup(fn ->
      MalachiMQ.Queue.enqueue(queue_name, payload)
    end, @warmup_sec)

    # Clear metrics
    MalachiMQ.Metrics.reset_metrics(queue_name)
    :atomics.put(received_count, 1, 0)

    # Send burst
    IO.puts("  Sending burst of #{burst_size} messages...")
    start_time = System.monotonic_time(:millisecond)

    Enum.each(1..burst_size, fn _ ->
      MalachiMQ.Queue.enqueue(queue_name, payload)
    end)

    burst_duration_ms = System.monotonic_time(:millisecond) - start_time

    # Wait for processing
    IO.puts("  Waiting for processing...")
    Process.sleep(5000)

    # Get metrics
    metrics = MalachiMQ.Metrics.get_metrics(queue_name)
    processed = :atomics.get(received_count, 1)
    total_duration_ms = System.monotonic_time(:millisecond) - start_time

    # Calculate throughput
    burst_duration_sec = burst_duration_ms / 1000
    total_duration_sec = total_duration_ms / 1000
    burst_msgs_per_sec = burst_size / burst_duration_sec
    overall_msgs_per_sec = processed / total_duration_sec

    # Cleanup
    BenchmarkHelpers.cleanup_queue(queue_name)

    %{
      "burst_size" => burst_size,
      "message_size_bytes" => 1024,
      "burst_duration_sec" => Float.round(burst_duration_sec, 2),
      "total_duration_sec" => Float.round(total_duration_sec, 2),
      "messages_processed" => processed,
      "burst_throughput_msgs_per_sec" => Float.round(burst_msgs_per_sec, 2),
      "overall_throughput_msgs_per_sec" => Float.round(overall_msgs_per_sec, 2),
      "latency_avg_us" => Float.round(Map.get(metrics.latency_us || %{}, :avg, 0) * 1.0, 2)
    }
  end
end

# Run the benchmark
ThroughputBenchmark.run()
