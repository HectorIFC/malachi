#!/usr/bin/env elixir

Mix.install([
  {:jason, "~> 1.4"},
  {:malachi, path: Path.expand("..", __DIR__)}
])

Code.require_file("utils/benchmark_helpers.ex", __DIR__)

defmodule ValidationBenchmark do
  @moduledoc """
  Benchmark for input validation performance.
  
  Measures:
  - Cache hit vs miss performance
  - Validation overhead on publish throughput
  - ReDoS resistance
  """

  @warmup_sec 5
  @duration_sec 30

  def run do
    IO.puts("\n🔬 Malachi Validation Benchmark")
    IO.puts("=" <> String.duplicate("=", 79))
    
    Application.ensure_all_started(:malachi)
    Process.sleep(1000)
    
    results = %{
      "benchmark" => "validation_performance",
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "version" => BenchmarkHelpers.get_version(),
      "system_info" => BenchmarkHelpers.get_system_info(),
      "results" => %{}
    }
    
    results = run_all_benchmarks(results)
    
    # Display results
    IO.puts("\n📊 Results Summary")
    IO.puts("=" <> String.duplicate("=", 79))
    display_results(results["results"])
    
    results
  end

  defp run_all_benchmarks(results) do
    results
    |> run_cache_performance()
    |> run_validation_overhead()
    |> run_redos_resistance()
  end

  # ============================================================
  # Cache Performance
  # ============================================================

  defp run_cache_performance(results) do
    IO.puts("\n🏎️  1. Validation Cache Performance")
    IO.puts("-" <> String.duplicate("-", 79))
    
    # Test 1: Cold cache (unique names)
    IO.puts("Testing cold cache (10,000 unique queue names)...")
    
    cold_cache_result = measure_validation_latency(fn i ->
      Malachi.Validator.validate_queue_name("queue_#{i}")
    end, 10_000)
    
    # Test 2: Warm cache (same name)
    IO.puts("Testing warm cache (same queue name repeated)...")
    
    warm_cache_result = measure_validation_latency(fn _i ->
      Malachi.Validator.validate_queue_name("cached_queue")
    end, 10_000)
    
    cache_results = %{
      "cold_cache_avg_us" => cold_cache_result.avg_us,
      "cold_cache_p50_us" => cold_cache_result.p50_us,
      "cold_cache_p99_us" => cold_cache_result.p99_us,
      "warm_cache_avg_us" => warm_cache_result.avg_us,
      "warm_cache_p50_us" => warm_cache_result.p50_us,
      "warm_cache_p99_us" => warm_cache_result.p99_us,
      "speedup_factor" => Float.round(cold_cache_result.avg_us / warm_cache_result.avg_us * 1.0, 2)
    }
    
    IO.puts("  Cold cache: avg=#{format_us(cold_cache_result.avg_us)}, p99=#{format_us(cold_cache_result.p99_us)}")
    IO.puts("  Warm cache: avg=#{format_us(warm_cache_result.avg_us)}, p99=#{format_us(warm_cache_result.p99_us)}")
    IO.puts("  Speedup: #{cache_results["speedup_factor"]}x")
    
    put_in(results, ["results", "cache_performance"], cache_results)
  end

  # ============================================================
  # Validation Overhead on Publish
  # ============================================================

  defp run_validation_overhead(results) do
    IO.puts("\n📈 2. Publish Throughput with Validation")
    IO.puts("-" <> String.duplicate("-", 79))
    
    queue_name = BenchmarkHelpers.unique_queue_name("validation_bench")
    
    # Setup consumer
    received = :atomics.new(1, [])
    :atomics.put(received, 1, 0)
    
    callback = fn _msg ->
      :atomics.add(received, 1, 1)
      :ok
    end
    
    {:ok, _consumer} = BenchmarkHelpers.start_benchmark_consumer(queue_name, callback)

    # Warmup
    IO.puts("Warming up...")
    BenchmarkHelpers.warmup(fn ->
      Malachi.Queue.enqueue(queue_name, "warmup", %{"test" => true})
    end, @warmup_sec)
    
    :atomics.put(received, 1, 0)
    
    # Benchmark
    IO.puts("Running benchmark (#{@duration_sec}s)...")
    start_time = System.monotonic_time(:millisecond)
    
    {sent_count, _actual_sec} = BenchmarkHelpers.benchmark_duration(fn ->
      # Publish with headers to trigger full validation
      Malachi.Queue.enqueue(queue_name, "test_payload", %{
        "priority" => 1,
        "type" => "benchmark"
      })
    end, @duration_sec)
    
    end_time = System.monotonic_time(:millisecond)
    duration_s = (end_time - start_time) / 1000
    
    Process.sleep(500)  # Allow consumers to catch up
    
    processed = :atomics.get(received, 1)
    throughput = sent_count / duration_s
    
    overhead_results = %{
      "messages_sent" => sent_count,
      "messages_processed" => processed,
      "duration_s" => Float.round(duration_s, 2),
      "throughput_msgs_per_sec" => Float.round(throughput, 2)
    }
    
    IO.puts("  Sent: #{BenchmarkHelpers.format_number(sent_count)} msgs")
    IO.puts("  Throughput: #{BenchmarkHelpers.format_number(round(throughput))} msgs/s")
    
    # Cleanup
    BenchmarkHelpers.cleanup_queue(queue_name)
    
    put_in(results, ["results", "publish_with_validation"], overhead_results)
  end

  # ============================================================
  # ReDoS Resistance
  # ============================================================

  defp run_redos_resistance(results) do
    IO.puts("\n🛡️  3. ReDoS (Regular Expression DoS) Resistance")
    IO.puts("-" <> String.duplicate("-", 79))
    
    test_cases = [
      {"10k chars", String.duplicate("a", 10_000)},
      {"100k chars", String.duplicate("a", 100_000)},
      {"1M chars", String.duplicate("a", 1_000_000)},
      {"repetitive pattern", String.duplicate("ab", 50_000)}
    ]
    
    redos_results = Enum.map(test_cases, fn {name, input} ->
      {time_us, _result} = :timer.tc(fn ->
        Malachi.Validator.validate_queue_name(input)
      end)
      
      time_ms = time_us / 1000
      IO.puts("  #{name}: #{Float.round(time_ms, 2)}ms")
      
      # Assert < 500ms for even the largest input
      if time_ms > 500 do
        IO.puts("    ⚠️  WARNING: Slow validation detected!")
      end
      
      %{
        "test_case" => name,
        "input_length" => String.length(input),
        "time_ms" => Float.round(time_ms, 2),
        "pass" => time_ms < 500
      }
    end)
    
    all_pass = Enum.all?(redos_results, & &1["pass"])
    
    IO.puts("\n  Result: #{if all_pass, do: "✅ PASS", else: "❌ FAIL"} - All tests < 500ms")
    
    put_in(results, ["results", "redos_resistance"], %{
      "tests" => redos_results,
      "all_pass" => all_pass
    })
  end

  # ============================================================
  # Helpers
  # ============================================================

  defp measure_validation_latency(validator_fn, iterations) do
    latencies = for i <- 1..iterations do
      {time_us, _result} = :timer.tc(fn -> validator_fn.(i) end)
      time_us
    end
    
    sorted = Enum.sort(latencies)
    
    %{
      avg_us: Enum.sum(latencies) / iterations,
      p50_us: Enum.at(sorted, div(iterations, 2)),
      p95_us: Enum.at(sorted, div(iterations * 95, 100)),
      p99_us: Enum.at(sorted, div(iterations * 99, 100)),
      max_us: Enum.max(latencies)
    }
  end

  defp format_us(us) when us < 1, do: "#{Float.round(us * 1000, 2)}ns"
  defp format_us(us) when us < 1000, do: "#{Float.round(us, 2)}µs"
  defp format_us(us), do: "#{Float.round(us / 1000, 2)}ms"

  defp display_results(results) do
    IO.puts("\n📋 Cache Performance:")
    cache = results["cache_performance"]
    IO.puts("  Cold: #{format_us(cache["cold_cache_avg_us"])} avg, #{format_us(cache["cold_cache_p99_us"])} p99")
    IO.puts("  Warm: #{format_us(cache["warm_cache_avg_us"])} avg, #{format_us(cache["warm_cache_p99_us"])} p99")
    IO.puts("  Speedup: #{cache["speedup_factor"]}x")
    
    IO.puts("\n📋 Publish Throughput:")
    publish = results["publish_with_validation"]
    IO.puts("  #{BenchmarkHelpers.format_number(round(publish["throughput_msgs_per_sec"]))} msgs/s")
    
    IO.puts("\n📋 ReDoS Protection:")
    redos = results["redos_resistance"]
    IO.puts("  #{if redos["all_pass"], do: "✅", else: "❌"} All inputs validated < 500ms")
    
    # Target assessment
    IO.puts("\n🎯 Performance Targets:")
    cache_hit_ok = cache["warm_cache_avg_us"] < 1.0
    cache_miss_ok = cache["cold_cache_avg_us"] < 10.0
    redos_ok = redos["all_pass"]
    
    IO.puts("  #{if cache_hit_ok, do: "✅", else: "❌"} Cache hit < 1µs: #{format_us(cache["warm_cache_avg_us"])}")
    IO.puts("  #{if cache_miss_ok, do: "✅", else: "❌"} Cache miss < 10µs: #{format_us(cache["cold_cache_avg_us"])}")
    IO.puts("  #{if redos_ok, do: "✅", else: "❌"} ReDoS protection: all < 500ms")
  end
end

ValidationBenchmark.run()
