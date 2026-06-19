#!/usr/bin/env elixir

# Authentication Baseline Benchmark
# Measures authentication operation performance

Code.require_file("utils/benchmark_helpers.ex", "benchmark")
Code.require_file("utils/percentile.ex", "benchmark")
Code.require_file("utils/reporter.ex", "benchmark")

defmodule AuthBenchmark do
  @warmup_sec BenchmarkHelpers.default_warmup_sec()
  @duration_sec BenchmarkHelpers.default_duration_sec()

  def run do
    IO.puts("\n#{IO.ANSI.cyan()}Starting Authentication Baseline Benchmark#{IO.ANSI.reset()}\n")

    # Start the application
    Application.ensure_all_started(:malachi)
    Process.sleep(1000)

    results = %{
      "benchmark" => "baseline_auth",
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "version" => BenchmarkHelpers.get_version(),
      "system_info" => BenchmarkHelpers.get_system_info(),
      "results" => %{
        "token_validation" => run_token_validation(),
        "permission_checking" => run_permission_checking(),
        "session_creation" => run_session_creation()
      }
    }

    # Save results
    filename = BenchmarkHelpers.timestamped_filename("auth")
    BenchmarkReporter.save_results(results, filename)

    # Display results
    BenchmarkReporter.display_results(results, "console")

    IO.puts("\n#{IO.ANSI.green()}✓ Authentication benchmark complete#{IO.ANSI.reset()}\n")
  end

  defp run_token_validation do
    IO.puts("\n#{IO.ANSI.yellow()}Test: Token Validation Performance#{IO.ANSI.reset()}")

    # Create a session token first
    {:ok, token} = Malachi.Auth.authenticate("producer", "producer123")

    IO.puts("  Token created: #{String.slice(token, 0, 20)}...")

    # Warm-up
    IO.puts("  Warming up...")

    BenchmarkHelpers.warmup(fn ->
      Malachi.Auth.validate_token(token)
    end, @warmup_sec)

    # Benchmark
    IO.puts("  Benchmarking token validation for #{@duration_sec} seconds...")

    {duration_us, {iteration_count, _actual_sec}} =
      BenchmarkHelpers.measure_time(fn ->
        BenchmarkHelpers.benchmark_duration(
          fn ->
            Malachi.Auth.validate_token(token)
          end,
          @duration_sec
        )
      end)

    # Calculate metrics
    duration_sec = duration_us / 1_000_000
    ops_per_sec = iteration_count / duration_sec
    us_per_op = duration_us / iteration_count

    IO.puts("  Operations: #{BenchmarkHelpers.format_number(iteration_count)}")
    IO.puts("  Throughput: #{BenchmarkHelpers.format_number(trunc(ops_per_sec))} ops/sec")
    IO.puts("  Latency: #{Float.round(us_per_op, 3)} μs/op")

    %{
      "total_operations" => iteration_count,
      "duration_sec" => Float.round(duration_sec, 2),
      "ops_per_sec" => Float.round(ops_per_sec, 2),
      "latency_us_per_op" => Float.round(us_per_op, 3)
    }
  end

  defp run_permission_checking do
    IO.puts("\n#{IO.ANSI.yellow()}Test: Permission Checking Performance#{IO.ANSI.reset()}")

    # Create sessions with different permissions
    {:ok, admin_token} = Malachi.Auth.authenticate("admin", "admin123")
    {:ok, producer_token} = Malachi.Auth.authenticate("producer", "producer123")
    {:ok, consumer_token} = Malachi.Auth.authenticate("consumer", "consumer123")

    # Get session data
    {:ok, admin_session} = Malachi.Auth.validate_token(admin_token)
    {:ok, producer_session} = Malachi.Auth.validate_token(producer_token)
    {:ok, consumer_session} = Malachi.Auth.validate_token(consumer_token)

    # Benchmark different permission checks
    tests = [
      {"admin_has_admin", admin_session.permissions, :admin},
      {"producer_has_produce", producer_session.permissions, :produce},
      {"consumer_has_consume", consumer_session.permissions, :consume},
      {"producer_has_admin_fail", producer_session.permissions, :admin}
    ]

    results =
      Enum.map(tests, fn {test_name, permissions, permission} ->
        IO.puts("  Testing: #{test_name}")

        # Warm-up
        BenchmarkHelpers.warmup(fn ->
          Malachi.Auth.has_permission?(permissions, permission)
        end, 5)

        # Benchmark
        {duration_us, {iteration_count, _actual_sec}} =
          BenchmarkHelpers.measure_time(fn ->
            BenchmarkHelpers.benchmark_duration(
              fn ->
                Malachi.Auth.has_permission?(permissions, permission)
              end,
              30
            )
          end)

        ops_per_sec = iteration_count / (duration_us / 1_000_000)
        us_per_op = duration_us / iteration_count

        IO.puts("    #{BenchmarkHelpers.format_number(trunc(ops_per_sec))} ops/sec (#{Float.round(us_per_op, 3)} μs/op)")

        {test_name,
         %{
           "ops_per_sec" => Float.round(ops_per_sec, 2),
           "latency_us_per_op" => Float.round(us_per_op, 3)
         }}
      end)
      |> Enum.into(%{})

    results
  end

  defp run_session_creation do
    IO.puts("\n#{IO.ANSI.yellow()}Test: Session Creation Performance (with Argon2)#{IO.ANSI.reset()}")
    IO.puts("  Note: This is intentionally slow due to Argon2 hashing")

    # Warm-up (just a few iterations since it's slow)
    IO.puts("  Warming up...")

    Enum.each(1..5, fn _ ->
      Malachi.Auth.authenticate("producer", "producer123")
    end)

    # Benchmark - run for shorter duration since it's slow
    iteration_count = 20
    IO.puts("  Running #{iteration_count} authentication operations...")

    {duration_us, results} =
      BenchmarkHelpers.measure_time(fn ->
        Enum.map(1..iteration_count, fn _ ->
          {time, result} =
            BenchmarkHelpers.measure_time(fn ->
              Malachi.Auth.authenticate("producer", "producer123")
            end)

          {time, result}
        end)
      end)

    # Calculate metrics
    successful = Enum.count(results, fn {_time, {:ok, _token}} -> true; _ -> false end)
    auth_times = Enum.map(results, fn {time, _result} -> time end)

    duration_sec = duration_us / 1_000_000
    ops_per_sec = successful / duration_sec
    avg_latency_ms = Enum.sum(auth_times) / length(auth_times) / 1000

    latency_stats = Percentile.calculate(auth_times)

    IO.puts("  Successful: #{successful}/#{iteration_count}")
    IO.puts("  Throughput: #{Float.round(ops_per_sec, 2)} ops/sec")
    IO.puts("  Avg latency: #{Float.round(avg_latency_ms, 2)} ms")
    IO.puts("  P50 latency: #{Float.round(latency_stats.p50 / 1000 * 1.0, 2)} ms")
    IO.puts("  P99 latency: #{Float.round(latency_stats.p99 / 1000 * 1.0, 2)} ms")

    %{
      "total_operations" => iteration_count,
      "successful_operations" => successful,
      "duration_sec" => Float.round(duration_sec, 2),
      "ops_per_sec" => Float.round(ops_per_sec, 2),
      "latency_avg_ms" => Float.round(avg_latency_ms, 2),
      "latency_p50_ms" => Float.round(latency_stats.p50 / 1000 * 1.0, 2),
      "latency_p95_ms" => Float.round(latency_stats.p95 / 1000 * 1.0, 2),
      "latency_p99_ms" => Float.round(latency_stats.p99 / 1000 * 1.0, 2)
    }
  end
end

# Run the benchmark
AuthBenchmark.run()
