#!/usr/bin/env elixir

# Edge Cases Baseline Benchmark
# Tests extreme scenarios: max message size, empty messages, connection limits, CPU saturation

Code.require_file("utils/benchmark_helpers.ex", "benchmark")
Code.require_file("utils/percentile.ex", "benchmark")
Code.require_file("utils/reporter.ex", "benchmark")

defmodule EdgeCasesBenchmark do
  @warmup_sec BenchmarkHelpers.default_warmup_sec()
  @duration_sec BenchmarkHelpers.default_duration_sec()
  @tcp_port 4040

  def run do
    IO.puts("\n#{IO.ANSI.cyan()}Starting Edge Cases Baseline Benchmark#{IO.ANSI.reset()}\n")

    # Start the application
    Application.ensure_all_started(:malachi)
    Process.sleep(1000)

    results = %{
      "benchmark" => "baseline_edge_cases",
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "version" => BenchmarkHelpers.get_version(),
      "system_info" => BenchmarkHelpers.get_system_info(),
      "results" => %{
        "max_message_size" => run_max_message_size(),
        "empty_messages" => run_empty_messages(),
        "max_connections" => run_max_connections(),
        "cpu_saturation" => run_cpu_saturation()
      }
    }

    # Save results
    filename = BenchmarkHelpers.timestamped_filename("edge_cases")
    BenchmarkReporter.save_results(results, filename)

    # Display results
    BenchmarkReporter.display_results(results, "console")

    IO.puts("\n#{IO.ANSI.green()}✓ Edge cases benchmark complete#{IO.ANSI.reset()}\n")
  end

  defp run_max_message_size do
    message_size = if(BenchmarkHelpers.ci_mode?(), do: 1 * 1024 * 1024, else: 10 * 1024 * 1024)
    message_size_mb = div(message_size, 1_048_576)
    IO.puts("\n#{IO.ANSI.yellow()}Test: Maximum Message Size (#{message_size_mb}MB)#{IO.ANSI.reset()}")

    queue_name = BenchmarkHelpers.unique_queue_name("edge_max_size")
    payload = :crypto.strong_rand_bytes(message_size)

    IO.puts("  Creating #{message_size} byte (#{message_size_mb} MB) message...")

    # Start consumer
    received_count = :atomics.new(1, [])

    callback = fn _msg ->
      :atomics.add(received_count, 1, 1)
      :ok
    end

    {:ok, _consumer} = BenchmarkHelpers.start_benchmark_consumer(queue_name, callback)
    Process.sleep(100)

    # Warm-up with smaller messages
    warm_size = if(BenchmarkHelpers.ci_mode?(), do: 102_400, else: 1_048_576)
    IO.puts("  Warming up with #{div(warm_size, 1024)}KB messages...")

    warm_payload = :crypto.strong_rand_bytes(warm_size)

    BenchmarkHelpers.warmup(fn ->
      Malachi.Queue.enqueue(queue_name, warm_payload)
    end, @warmup_sec)

    # Clear metrics
    Malachi.Metrics.reset_metrics(queue_name)
    :atomics.put(received_count, 1, 0)

    # Benchmark - send large messages
    message_count = if(BenchmarkHelpers.ci_mode?(), do: 5, else: 10)
    IO.puts("  Sending #{message_count} messages of #{message_size_mb} MB each...")

    {duration_us, _} =
      BenchmarkHelpers.measure_time(fn ->
        Enum.each(1..message_count, fn _ ->
          Malachi.Queue.enqueue(queue_name, payload)
        end)
      end)

    # Wait for processing
    Process.sleep(2000)

    # Get metrics
    metrics = Malachi.Metrics.get_metrics(queue_name)
    processed = :atomics.get(received_count, 1)

    # Calculate throughput
    duration_sec = duration_us / 1_000_000
    msgs_per_sec = processed / duration_sec
    mb_per_sec = (processed * message_size) / duration_sec / 1_048_576

    IO.puts("  Processed: #{processed}/#{message_count}")
    IO.puts("  Throughput: #{Float.round(msgs_per_sec, 2)} msgs/sec, #{Float.round(mb_per_sec, 2)} MB/sec")

    # Cleanup
    BenchmarkHelpers.cleanup_queue(queue_name)

    %{
      "message_size_bytes" => message_size,
      "message_size_mb" => div(message_size, 1_048_576),
      "messages_sent" => message_count,
      "messages_processed" => processed,
      "duration_sec" => Float.round(duration_sec, 2),
      "throughput_msgs_per_sec" => Float.round(msgs_per_sec, 2),
      "throughput_mb_per_sec" => Float.round(mb_per_sec, 2),
      "latency_avg_us" => Float.round(Map.get(metrics.latency_us || %{}, :avg, 0) * 1.0, 2)
    }
  end

  defp run_empty_messages do
    IO.puts("\n#{IO.ANSI.yellow()}Test: Empty Messages (0 bytes payload)#{IO.ANSI.reset()}")

    queue_name = BenchmarkHelpers.unique_queue_name("edge_empty")
    payload = ""  # Empty payload

    # Start consumer
    received_count = :atomics.new(1, [])

    callback = fn _msg ->
      :atomics.add(received_count, 1, 1)
      :ok
    end

    {:ok, _consumer} = BenchmarkHelpers.start_benchmark_consumer(queue_name, callback)
    Process.sleep(100)

    # Warm-up
    IO.puts("  Warming up...")

    BenchmarkHelpers.warmup(fn ->
      Malachi.Queue.enqueue(queue_name, payload)
    end, @warmup_sec)

    # Clear metrics
    Malachi.Metrics.reset_metrics(queue_name)
    :atomics.put(received_count, 1, 0)

    # Benchmark - send for configured duration
    duration_sec = @duration_sec
    IO.puts("  Sending empty messages for #{duration_sec} seconds...")

    {message_count, actual_duration_sec} =
      BenchmarkHelpers.benchmark_duration(
        fn -> Malachi.Queue.enqueue(queue_name, payload) end,
        duration_sec
      )

    # Wait for processing
    Process.sleep(1000)

    # Get metrics
    metrics = Malachi.Metrics.get_metrics(queue_name)
    processed = :atomics.get(received_count, 1)

    # Calculate throughput using actual duration
    msgs_per_sec = processed / actual_duration_sec

    IO.puts("  Sent: #{BenchmarkHelpers.format_number(message_count)}")
    IO.puts("  Processed: #{BenchmarkHelpers.format_number(processed)}")
    IO.puts("  Throughput: #{BenchmarkHelpers.format_number(trunc(msgs_per_sec))} msgs/sec")

    # Cleanup
    BenchmarkHelpers.cleanup_queue(queue_name)

    %{
      "message_size_bytes" => 0,
      "duration_sec" => Float.round(actual_duration_sec, 2),
      "messages_sent" => message_count,
      "messages_processed" => processed,
      "throughput_msgs_per_sec" => Float.round(msgs_per_sec, 2),
      "latency_avg_us" => Float.round(Map.get(metrics.latency_us || %{}, :avg, 0) * 1.0, 2)
    }
  end

  defp run_max_connections do
    IO.puts("\n#{IO.ANSI.yellow()}Test: Maximum Connection Count (until failure)#{IO.ANSI.reset()}")

    # Try to establish connections until we start seeing failures
    max_attempts = if(BenchmarkHelpers.ci_mode?(), do: 1_000, else: 10_000)
    batch_size = 100
    failure_threshold = 0.1  # 10% failure rate

    IO.puts("  Attempting to establish up to #{max_attempts} connections...")
    IO.puts("  Will stop when failure rate exceeds #{trunc(failure_threshold * 100)}%")

    result = find_max_connections(batch_size, max_attempts, failure_threshold, [])

    IO.puts("  Max stable connections: #{result.max_stable_connections}")
    IO.puts("  Total successful: #{result.total_successful}")
    IO.puts("  Failure rate at max: #{Float.round(result.failure_rate * 100, 2)}%")

    result
  end

  defp find_max_connections(batch_size, max_attempts, failure_threshold, sockets, total_successful \\ 0) do
    current_count = length(sockets)

    if current_count >= max_attempts do
      # Close all sockets
      Enum.each(sockets, &safe_close/1)

      %{
        max_stable_connections: current_count,
        total_successful: total_successful,
        failure_rate: 0.0,
        max_attempts_reached: true
      }
    else
      # Try to establish a batch of connections
      IO.write("\r  Current connections: #{current_count}")

      new_sockets =
        1..batch_size
        |> Task.async_stream(
          fn _ -> connect_simple() end,
          max_concurrency: batch_size,
          timeout: 10_000
        )
        |> Enum.filter(fn {:ok, {:ok, socket}} -> socket != nil; _ -> false end)
        |> Enum.map(fn {:ok, {:ok, socket}} -> socket end)

      successful = length(new_sockets)
      failure_rate = (batch_size - successful) / batch_size

      if failure_rate > failure_threshold do
        # Close all sockets
        Enum.each(sockets ++ new_sockets, &safe_close/1)

        IO.puts("")

        %{
          max_stable_connections: current_count,
          total_successful: total_successful + successful,
          failure_rate: Float.round(failure_rate, 3),
          max_attempts_reached: false
        }
      else
        # Continue
        find_max_connections(
          batch_size,
          max_attempts,
          failure_threshold,
          sockets ++ new_sockets,
          total_successful + successful
        )
      end
    end
  end

  defp connect_simple do
    case :gen_tcp.connect(~c"localhost", @tcp_port, [:binary, active: false], 5000) do
      {:ok, socket} -> {:ok, socket}
      {:error, _reason} -> {:error, :connection_failed}
    end
  end

  defp safe_close(socket) do
    try do
      :gen_tcp.close(socket)
    rescue
      _ -> :ok
    end
  end

  defp run_cpu_saturation do
    IO.puts("\n#{IO.ANSI.yellow()}Test: CPU Saturation Point#{IO.ANSI.reset()}")

    queue_name = BenchmarkHelpers.unique_queue_name("edge_cpu")
    payload = :crypto.strong_rand_bytes(1024)

    # Get system info
    schedulers = :erlang.system_info(:schedulers_online)
    IO.puts("  Schedulers online: #{schedulers}")

    # Start with consumers equal to schedulers
    initial_consumers = schedulers
    test_durations = @duration_sec

    consumer_multipliers = if(BenchmarkHelpers.ci_mode?(), do: [1, 10, 50], else: [1, 10, 100, 1000])

    results =
      Enum.map(consumer_multipliers, fn multiplier ->
        consumer_count = initial_consumers * multiplier
        IO.puts("\n  Testing with #{consumer_count} consumers...")

        # Start consumers
        _consumers =
          Enum.map(1..consumer_count, fn _ ->
            {:ok, pid} = BenchmarkHelpers.start_benchmark_consumer(queue_name, fn _ -> :ok end)
            pid
          end)

        Process.sleep(500)

        # Measure baseline CPU
        before_stats = get_cpu_stats()

        # Send messages
        start_time = System.monotonic_time(:millisecond)

        {message_count, _actual_sec} =
          BenchmarkHelpers.benchmark_duration(
            fn -> Malachi.Queue.enqueue(queue_name, payload) end,
            test_durations
          )

        duration_ms = System.monotonic_time(:millisecond) - start_time

        # Wait for processing
        Process.sleep(1000)

        # Measure CPU after
        after_stats = get_cpu_stats()

        # Calculate throughput
        duration_sec = duration_ms / 1000
        msgs_per_sec = message_count / duration_sec

        # Estimate CPU usage
        cpu_percent = estimate_cpu_usage(before_stats, after_stats)

        IO.puts("    Throughput: #{BenchmarkHelpers.format_number(trunc(msgs_per_sec))} msgs/sec")
        IO.puts("    Run queue: #{after_stats.run_queue}")
        IO.puts("    Estimated CPU: ~#{Float.round(cpu_percent, 1)}%")

        # Cleanup consumers
        BenchmarkHelpers.cleanup_queue(queue_name)
        Process.sleep(500)

        {consumer_count,
         %{
           "throughput_msgs_per_sec" => Float.round(msgs_per_sec, 2),
           "run_queue_length" => after_stats.run_queue,
           "estimated_cpu_percent" => Float.round(cpu_percent, 1),
           "process_count" => after_stats.process_count
         }}
      end)
      |> Enum.into(%{})

    results
  end

  defp get_cpu_stats do
    %{
      run_queue: :erlang.statistics(:run_queue),
      process_count: :erlang.system_info(:process_count)
    }
  end

  defp estimate_cpu_usage(_before, after_stats) do
    # Rough estimation based on run queue
    # If run queue > schedulers, CPU is likely saturated
    schedulers = :erlang.system_info(:schedulers_online)
    run_queue = after_stats.run_queue

    if run_queue > schedulers do
      100.0
    else
      (run_queue / schedulers) * 100
    end
  end
end

# Run the benchmark
EdgeCasesBenchmark.run()
