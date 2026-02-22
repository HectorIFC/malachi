#!/usr/bin/env elixir

# Latency Baseline Benchmark
# Measures end-to-end message delivery latency (p50, p95, p99)

Code.require_file("utils/benchmark_helpers.ex", "benchmark")
Code.require_file("utils/percentile.ex", "benchmark")
Code.require_file("utils/reporter.ex", "benchmark")

defmodule LatencyBenchmark do
  @warmup_sec BenchmarkHelpers.default_warmup_sec()
  @duration_sec BenchmarkHelpers.default_duration_sec()

  def run do
    IO.puts("\n#{IO.ANSI.cyan()}Starting Latency Baseline Benchmark#{IO.ANSI.reset()}\n")

    # Start the application
    Application.ensure_all_started(:malachimq)
    Process.sleep(1000)

    results = %{
      "benchmark" => "baseline_latency",
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "version" => BenchmarkHelpers.get_version(),
      "system_info" => BenchmarkHelpers.get_system_info(),
      "results" => %{
        "end_to_end_latency" => run_end_to_end_latency(),
        "latency_under_light_load" => run_latency_under_load(0.1),
        "latency_under_medium_load" => run_latency_under_load(0.5),
        "latency_under_heavy_load" => run_latency_under_load(0.9)
      }
    }

    # Save results
    filename = BenchmarkHelpers.timestamped_filename("latency")
    BenchmarkReporter.save_results(results, filename)

    # Display results
    BenchmarkReporter.display_results(results, "console")

    IO.puts("\n#{IO.ANSI.green()}✓ Latency benchmark complete#{IO.ANSI.reset()}\n")
  end

  defp run_end_to_end_latency do
    IO.puts("\n#{IO.ANSI.yellow()}Test: End-to-End Latency (publish → acknowledge)#{IO.ANSI.reset()}")

    queue_name = BenchmarkHelpers.unique_queue_name("latency_e2e")
    payload = :crypto.strong_rand_bytes(1024)

    # Create ETS table for collecting latency samples
    sample_table = Percentile.create_sample_table()

    # Start consumer that records latencies
    callback = fn msg ->
      latency_us = System.monotonic_time(:microsecond) - msg.timestamp
      :ets.insert(sample_table, {:latency, latency_us})
      :ok
    end

    {:ok, _consumer} = BenchmarkHelpers.start_benchmark_consumer(queue_name, callback)
    Process.sleep(100)

    # Warm-up
    IO.puts("  Warming up...")

    BenchmarkHelpers.warmup(fn ->
      MalachiMQ.Queue.enqueue(queue_name, payload)
    end, @warmup_sec)

    # Clear samples
    Percentile.clear_samples(sample_table)

    # Benchmark
    IO.puts("  Collecting latency samples for #{@duration_sec} seconds...")

    _message_count =
      BenchmarkHelpers.benchmark_duration(
        fn -> MalachiMQ.Queue.enqueue(queue_name, payload) end,
        @duration_sec
      )
      |> elem(0)

    # Wait for all messages to be processed
    Process.sleep(2000)

    # Calculate percentiles
    latency_stats = Percentile.from_ets_bag(sample_table, :latency)

    IO.puts(Percentile.format_results(latency_stats))

    # Cleanup
    :ets.delete(sample_table)
    BenchmarkHelpers.cleanup_queue(queue_name)

    %{
      "messages_sampled" => latency_stats.count,
      "latency_min_us" => Float.round(latency_stats.min * 1.0, 2),
      "latency_p50_us" => Float.round(latency_stats.p50 * 1.0, 2),
      "latency_p95_us" => Float.round(latency_stats.p95 * 1.0, 2),
      "latency_p99_us" => Float.round(latency_stats.p99 * 1.0, 2),
      "latency_max_us" => Float.round(latency_stats.max * 1.0, 2),
      "latency_avg_us" => Float.round(latency_stats.avg * 1.0, 2)
    }
  end

  defp run_latency_under_load(load_percentage) do
    load_name =
      case load_percentage do
        0.1 -> "10% load"
        0.5 -> "50% load"
        0.9 -> "90% load"
        _ -> "#{trunc(load_percentage * 100)}% load"
      end

    IO.puts("\n#{IO.ANSI.yellow()}Test: Latency under #{load_name}#{IO.ANSI.reset()}")

    queue_name = BenchmarkHelpers.unique_queue_name("latency_load")
    payload = :crypto.strong_rand_bytes(1024)

    # Create ETS table for collecting latency samples
    sample_table = Percentile.create_sample_table()

    # Start consumer
    callback = fn msg ->
      latency_us = System.monotonic_time(:microsecond) - msg.timestamp
      :ets.insert(sample_table, {:latency, latency_us})
      :ok
    end

    {:ok, _consumer} = BenchmarkHelpers.start_benchmark_consumer(queue_name, callback)
    Process.sleep(100)

    # Determine max throughput first (quick test)
    IO.puts("  Determining max throughput...")

    throughput_test_sec = if(BenchmarkHelpers.ci_mode?(), do: 2, else: 5)

    {throughput_count, throughput_actual_sec} =
      BenchmarkHelpers.benchmark_duration(
        fn -> MalachiMQ.Queue.enqueue(queue_name, payload) end,
        throughput_test_sec
      )

    max_throughput = throughput_count / throughput_actual_sec

    IO.puts("  Max throughput: ~#{trunc(max_throughput)} msgs/sec")

    target_rate = trunc(max_throughput * load_percentage)
    sleep_between_msgs = if target_rate > 0, do: trunc(1_000_000 / target_rate), else: 0

    IO.puts("  Target rate: #{target_rate} msgs/sec (sleep: #{sleep_between_msgs} μs)")

    # Clear samples
    Percentile.clear_samples(sample_table)

    # Benchmark at target rate
    IO.puts("  Collecting latency samples for #{@duration_sec} seconds at #{load_name}...")

    end_time = System.monotonic_time(:second) + @duration_sec
    _message_count = benchmark_with_rate_limit(queue_name, payload, end_time, sleep_between_msgs)

    # Wait for processing
    Process.sleep(2000)

    # Calculate percentiles
    latency_stats = Percentile.from_ets_bag(sample_table, :latency)

    IO.puts(Percentile.format_results(latency_stats))

    # Cleanup
    :ets.delete(sample_table)
    BenchmarkHelpers.cleanup_queue(queue_name)

    %{
      "load_percentage" => trunc(load_percentage * 100),
      "target_msgs_per_sec" => target_rate,
      "messages_sampled" => latency_stats.count,
      "latency_min_us" => Float.round(latency_stats.min * 1.0, 2),
      "latency_p50_us" => Float.round(latency_stats.p50 * 1.0, 2),
      "latency_p95_us" => Float.round(latency_stats.p95 * 1.0, 2),
      "latency_p99_us" => Float.round(latency_stats.p99 * 1.0, 2),
      "latency_max_us" => Float.round(latency_stats.max * 1.0, 2),
      "latency_avg_us" => Float.round(latency_stats.avg * 1.0, 2)
    }
  end

  defp benchmark_with_rate_limit(queue_name, payload, end_time, sleep_us, count \\ 0) do
    if System.monotonic_time(:second) < end_time do
      MalachiMQ.Queue.enqueue(queue_name, payload)

      if sleep_us > 0 do
        Process.sleep(div(sleep_us, 1000))
      end

      benchmark_with_rate_limit(queue_name, payload, end_time, sleep_us, count + 1)
    else
      count
    end
  end
end

# Run the benchmark
LatencyBenchmark.run()
