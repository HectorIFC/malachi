#!/usr/bin/env elixir

# Rate Limiting Performance Benchmark
#
# Validates that rate limiting adds minimal overhead:
# - Target: < 10µs latency per check
# - Target: < 5% throughput degradation  
# - Target: < 1KB memory per bucket
#
# Usage:
#   mix run benchmark/rate_limiting_benchmark.exs

defmodule RateLimitingBenchmark do
  @moduledoc """
  Performance benchmarks for MalachiMQ rate limiting system.
  """

  def run do
    IO.puts("\n" <> IO.ANSI.cyan() <> "=== MalachiMQ Rate Limiting Benchmark ===" <> IO.ANSI.reset())
    IO.puts("Measuring rate limiter performance characteristics\n")

    # Ensure app is started
    {:ok, _} = Application.ensure_all_started(:malachimq)

    # Ensure rate limiting is enabled
    Application.put_env(:malachimq, :rate_limit_enabled, true)

    run_latency_benchmark()
    run_throughput_benchmark()
    run_memory_benchmark()
    run_concurrent_benchmark()
    run_cleanup_benchmark()

    IO.puts("\n" <> IO.ANSI.green() <> "✓ Benchmark complete!" <> IO.ANSI.reset())
  end

  defp run_latency_benchmark do
    IO.puts(IO.ANSI.yellow() <> "1. Latency Benchmark" <> IO.ANSI.reset())
    IO.puts("   Measuring per-check latency (target: <10µs)\n")

    identifier = "latency_user_#{:rand.uniform(1_000_000)}"
    config = %{limit: 1_000_000, window_ms: 60_000}

    # Warmup
    for _ <- 1..100 do
      MalachiMQ.RateLimiter.check_limit(identifier, :auth, config)
    end

    # Measure
    iterations = 10_000
    latencies = for _ <- 1..iterations do
      {time_us, _result} = :timer.tc(fn ->
        MalachiMQ.RateLimiter.check_limit(identifier, :auth, config)
      end)
      time_us
    end

    avg_latency = Enum.sum(latencies) / length(latencies)
    
    sorted_latencies = Enum.sort(latencies)
    median_latency = Enum.at(sorted_latencies, div(length(sorted_latencies), 2)) * 1.0
    p95_latency = Enum.at(sorted_latencies, trunc(length(sorted_latencies) * 0.95)) * 1.0
    p99_latency = Enum.at(sorted_latencies, trunc(length(sorted_latencies) * 0.99)) * 1.0
    max_latency = Enum.max(latencies) * 1.0

    IO.puts("   Iterations: #{iterations}")
    IO.puts("   Average:    #{Float.round(avg_latency, 2)}µs")
    IO.puts("   Median:     #{Float.round(median_latency, 2)}µs")
    IO.puts("   P95:        #{Float.round(p95_latency, 2)}µs")
    IO.puts("   P99:        #{Float.round(p99_latency, 2)}µs")
    IO.puts("   Max:        #{Float.round(max_latency, 2)}µs")

    if avg_latency < 10 do
      IO.puts(IO.ANSI.green() <> "   ✓ PASS: Average latency under 10µs target" <> IO.ANSI.reset())
    else
      IO.puts(IO.ANSI.red() <> "   ✗ FAIL: Average latency exceeds 10µs target" <> IO.ANSI.reset())
    end

    IO.puts("")
  end

  defp run_throughput_benchmark do
    IO.puts(IO.ANSI.yellow() <> "2. Throughput Benchmark" <> IO.ANSI.reset())
    IO.puts("   Comparing throughput with/without rate limiting (target: <5% degradation)\n")

    queue_name = "throughput_test_#{:rand.uniform(1_000_000)}"
    message_count = 50_000

    # Benchmark WITHOUT rate limiting
    Application.put_env(:malachimq, :rate_limit_enabled, false)
    
    {time_without_us, _result} = :timer.tc(fn ->
      for i <- 1..message_count do
        MalachiMQ.Queue.enqueue(queue_name, "message_#{i}", %{})
      end
    end)

    throughput_without = (message_count / time_without_us) * 1_000_000

    # Benchmark WITH rate limiting (high limit to not actually block)
    Application.put_env(:malachimq, :rate_limit_enabled, true)
    username = "throughput_user"
    config = %{limit: 100_000, window_ms: 1_000}

    # Pre-populate to simulate checking
    MalachiMQ.RateLimiter.check_limit(username, :publish, config)

    {time_with_us, _result} = :timer.tc(fn ->
      for i <- 1..message_count do
        MalachiMQ.RateLimiter.check_limit(username, :publish, config)
        MalachiMQ.Queue.enqueue(queue_name, "message_#{i}", %{})
      end
    end)

    throughput_with = (message_count / time_with_us) * 1_000_000

    degradation_pct = ((throughput_without - throughput_with) / throughput_without) * 100

    IO.puts("   Messages:           #{message_count}")
    IO.puts("   Without limiting:   #{Float.round(throughput_without, 0)} msg/s")
    IO.puts("   With limiting:      #{Float.round(throughput_with, 0)} msg/s")
    IO.puts("   Degradation:        #{Float.round(degradation_pct, 2)}%")

    if degradation_pct < 5 do
      IO.puts(IO.ANSI.green() <> "   ✓ PASS: Degradation under 5% target" <> IO.ANSI.reset())
    else
      IO.puts(IO.ANSI.red() <> "   ✗ FAIL: Degradation exceeds 5% target" <> IO.ANSI.reset())
    end

    IO.puts("")
  end

  defp run_memory_benchmark do
    IO.puts(IO.ANSI.yellow() <> "3. Memory Benchmark" <> IO.ANSI.reset())
    IO.puts("   Measuring memory per bucket (target: <1KB per bucket)\n")

    Application.put_env(:malachimq, :rate_limit_enabled, true)

    # Get initial ETS memory
    initial_memory = :ets.info(:malachimq_rate_limits, :memory) * :erlang.system_info(:wordsize)

    # Create 1000 buckets
    bucket_count = 1_000
    config = %{limit: 100, window_ms: 60_000}

    for i <- 1..bucket_count do
      identifier = "user_#{i}"
      MalachiMQ.RateLimiter.check_limit(identifier, :auth, config)
    end

    # Get final memory
    final_memory = :ets.info(:malachimq_rate_limits, :memory) * :erlang.system_info(:wordsize)

    memory_increase = final_memory - initial_memory
    memory_per_bucket = memory_increase / bucket_count

    IO.puts("   Buckets created:     #{bucket_count}")
    IO.puts("   Initial memory:      #{Float.round(initial_memory / 1024, 2)} KB")
    IO.puts("   Final memory:        #{Float.round(final_memory / 1024, 2)} KB")
    IO.puts("   Memory increase:     #{Float.round(memory_increase / 1024, 2)} KB")
    IO.puts("   Per bucket:          #{Float.round(memory_per_bucket, 0)} bytes")

    if memory_per_bucket < 1024 do
      IO.puts(IO.ANSI.green() <> "   ✓ PASS: Memory per bucket under 1KB target" <> IO.ANSI.reset())
    else
      IO.puts(IO.ANSI.red() <> "   ✗ FAIL: Memory per bucket exceeds 1KB target" <> IO.ANSI.reset())
    end

    IO.puts("")
  end

  defp run_concurrent_benchmark do
    IO.puts(IO.ANSI.yellow() <> "4. Concurrent Access Benchmark" <> IO.ANSI.reset())
    IO.puts("   Testing concurrent check performance\n")

    Application.put_env(:malachimq, :rate_limit_enabled, true)
    config = %{limit: 1_000_000, window_ms: 60_000}

    # Test with increasing concurrency
    concurrency_levels = [1, 10, 50, 100, 500]

    results = for concurrency <- concurrency_levels do
      requests_per_task = 1_000
      total_requests = concurrency * requests_per_task

      {time_us, _} = :timer.tc(fn ->
        tasks = for i <- 1..concurrency do
          Task.async(fn ->
            identifier = "concurrent_user_#{i}"
            for _ <- 1..requests_per_task do
              MalachiMQ.RateLimiter.check_limit(identifier, :auth, config)
            end
          end)
        end

        Task.await_many(tasks, 30_000)
      end)

      throughput = (total_requests / time_us) * 1_000_000
      {concurrency, Float.round(throughput, 0)}
    end

    IO.puts("   Concurrency | Throughput (checks/s)")
    IO.puts("   ----------- | --------------------")
    for {concurrency, throughput} <- results do
      IO.puts("   #{String.pad_leading(to_string(concurrency), 11)} | #{String.pad_leading(to_string(trunc(throughput)), 20)}")
    end

    IO.puts(IO.ANSI.green() <> "   ✓ Concurrent access test complete" <> IO.ANSI.reset())
    IO.puts("")
  end

  defp run_cleanup_benchmark do
    IO.puts(IO.ANSI.yellow() <> "5. Cleanup Performance" <> IO.ANSI.reset())
    IO.puts("   Testing periodic cleanup impact\n")

    Application.put_env(:malachimq, :rate_limit_enabled, true)

    # Create many expired buckets
    config = %{limit: 10, window_ms: 100}
    
    for i <- 1..5_000 do
      identifier = "cleanup_user_#{i}"
      MalachiMQ.RateLimiter.check_limit(identifier, :auth, config)
    end

    # Wait for buckets to expire
    Process.sleep(150)

    initial_buckets = MalachiMQ.RateLimiter.get_stats().total_buckets

    # Trigger cleanup manually (would normally happen every 5 minutes)
    send(Process.whereis(MalachiMQ.RateLimiter), :cleanup)
    Process.sleep(100)

    final_buckets = MalachiMQ.RateLimiter.get_stats().total_buckets

    IO.puts("   Buckets before:  #{initial_buckets}")
    IO.puts("   Buckets after:   #{final_buckets}")
    IO.puts("   Cleaned:         #{initial_buckets - final_buckets}")

    IO.puts(IO.ANSI.green() <> "   ✓ Cleanup test complete" <> IO.ANSI.reset())
    IO.puts("")
  end
end

# Run benchmark
RateLimitingBenchmark.run()
