#!/usr/bin/env elixir

# Connection Baseline Benchmark
# Measures TCP connection establishment rates and capacity

Code.require_file("utils/benchmark_helpers.ex", "benchmark")
Code.require_file("utils/percentile.ex", "benchmark")
Code.require_file("utils/reporter.ex", "benchmark")

defmodule ConnectionBenchmark do
  @tcp_port 4040

  def run do
    IO.puts("\n#{IO.ANSI.cyan()}Starting Connection Baseline Benchmark#{IO.ANSI.reset()}\n")

    # Start the application
    Application.ensure_all_started(:malachi)
    Process.sleep(1000)

    results = %{
      "benchmark" => "baseline_connections",
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "version" => BenchmarkHelpers.get_version(),
      "system_info" => BenchmarkHelpers.get_system_info(),
      "results" => %{
        "connection_capacity" => run_connection_capacity(),
        "connection_establishment_rate" => run_connection_rate(),
        "connection_churn" => run_connection_churn()
      }
    }

    # Save results
    filename = BenchmarkHelpers.timestamped_filename("connections")
    BenchmarkReporter.save_results(results, filename)

    # Display results
    BenchmarkReporter.display_results(results, "console")

    IO.puts("\n#{IO.ANSI.green()}✓ Connection benchmark complete#{IO.ANSI.reset()}\n")
  end

  defp run_connection_capacity do
    IO.puts("\n#{IO.ANSI.yellow()}Test: Concurrent Connection Capacity#{IO.ANSI.reset()}")

    connection_counts = if(BenchmarkHelpers.ci_mode?(), do: [10, 50, 200], else: [10, 100, 1000, 5000])

    results =
      Enum.map(connection_counts, fn count ->
        IO.puts("  Testing #{count} concurrent connections...")
        result = measure_concurrent_connections(count)

        # Allow time for connections to fully close before next test
        Process.sleep(2000)

        {count, result}
      end)

    # Format results
    Enum.with_index(results, 1)
    |> Enum.map(fn {{count, result}, idx} ->
      {"test_#{idx}",
       %{
         "target_connections" => count,
         "successful_connections" => result.successful,
         "failed_connections" => result.failed,
         "success_rate_percent" => Float.round(result.success_rate * 100, 2),
         "avg_connection_time_ms" => Float.round(result.avg_connection_time_ms, 2)
       }}
    end)
    |> Enum.into(%{})
  end

  defp measure_concurrent_connections(count) do
    # Use Task.async_stream for concurrent connection attempts
    start_time = System.monotonic_time(:millisecond)

    results =
      1..count
      |> Task.async_stream(
        fn _ -> connect_and_authenticate() end,
        max_concurrency: count,
        timeout: 30_000
      )
      |> Enum.to_list()

    duration_ms = System.monotonic_time(:millisecond) - start_time

    # Analyze results
    successful = Enum.count(results, fn {:ok, {:ok, _}} -> true; _ -> false end)
    failed = count - successful
    success_rate = successful / count

    # Log failure reasons for debugging
    if failed > 0 do
      error_reasons =
        results
        |> Enum.reject(fn {:ok, {:ok, _}} -> true; _ -> false end)
        |> Enum.map(fn
          {:ok, {:error, reason}} -> reason
          {:exit, reason} -> {:exit, reason}
          other -> other
        end)
        |> Enum.frequencies()

      IO.puts("    Failure reasons: #{inspect(error_reasons)}")
    end

    # Calculate average connection time from successful connections
    connection_times =
      results
      |> Enum.filter(fn {:ok, {:ok, _time}} -> true; _ -> false end)
      |> Enum.map(fn {:ok, {:ok, time}} -> time end)

    avg_connection_time_ms = if length(connection_times) > 0, do: Enum.sum(connection_times) / length(connection_times), else: 0.0

    IO.puts("    Successful: #{successful}/#{count} (#{Float.round(success_rate * 100, 1)}%)")
    IO.puts("    Avg connection time: #{Float.round(avg_connection_time_ms, 2)} ms")

    %{
      successful: successful,
      failed: failed,
      success_rate: success_rate,
      avg_connection_time_ms: avg_connection_time_ms,
      total_duration_ms: duration_ms
    }
  end

  defp run_connection_rate do
    IO.puts("\n#{IO.ANSI.yellow()}Test: Connection Establishment Rate#{IO.ANSI.reset()}")

    duration_sec = if(BenchmarkHelpers.ci_mode?(), do: 10, else: 30)
    IO.puts("  Measuring connections/sec for #{duration_sec} seconds...")

    # Warm-up
    IO.puts("  Warming up...")

    Enum.each(1..50, fn _ ->
      case connect_and_authenticate() do
        {:ok, _duration} -> :ok
        _ -> :ok
      end
    end)

    # Benchmark
    end_time = System.monotonic_time(:second) + duration_sec
    connection_count = benchmark_connections_until(end_time, 0)

    connections_per_sec = connection_count / duration_sec

    IO.puts("  Established #{connection_count} connections in #{duration_sec}s")
    IO.puts("  Rate: #{Float.round(connections_per_sec, 2)} connections/sec")

    %{
      "duration_sec" => duration_sec,
      "total_connections" => connection_count,
      "connections_per_sec" => Float.round(connections_per_sec, 2)
    }
  end

  defp benchmark_connections_until(end_time, count) do
    if System.monotonic_time(:second) < end_time do
      case connect_and_authenticate() do
        {:ok, _duration} ->
          benchmark_connections_until(end_time, count + 1)

        _ ->
          benchmark_connections_until(end_time, count)
      end
    else
      count
    end
  end

  defp run_connection_churn do
    IO.puts("\n#{IO.ANSI.yellow()}Test: Connection Churn (rapid connect/disconnect)#{IO.ANSI.reset()}")

    cycle_count = if(BenchmarkHelpers.ci_mode?(), do: 200, else: 1000)
    IO.puts("  Running #{cycle_count} connect/disconnect cycles...")

    start_time = System.monotonic_time(:millisecond)

    successful =
      Enum.reduce(1..cycle_count, 0, fn _, acc ->
        case connect_and_authenticate() do
          {:ok, _duration} ->
            acc + 1

          _ ->
            acc
        end
      end)

    duration_ms = System.monotonic_time(:millisecond) - start_time
    cycles_per_sec = successful / (duration_ms / 1000)

    IO.puts("  Completed #{successful}/#{cycle_count} cycles")
    IO.puts("  Rate: #{Float.round(cycles_per_sec, 2)} cycles/sec")

    %{
      "cycle_count" => cycle_count,
      "successful_cycles" => successful,
      "duration_ms" => duration_ms,
      "cycles_per_sec" => Float.round(cycles_per_sec, 2)
    }
  end

  # Helper functions for TCP connection

  defp connect_and_authenticate do
    start = System.monotonic_time(:millisecond)

    case :gen_tcp.connect(~c"localhost", @tcp_port, [:binary, packet: :line, active: false], 5000) do
      {:ok, socket} ->
        # Authenticate
        auth_message =
          Jason.encode!(%{
            "action" => "auth",
            "username" => "producer",
            "password" => "producer123"
          }) <> "\n"

        case :gen_tcp.send(socket, auth_message) do
          :ok ->
            case :gen_tcp.recv(socket, 0, 5000) do
              {:ok, response} ->
                case Jason.decode(response) do
                  {:ok, %{"s" => "ok"}} ->
                    duration = System.monotonic_time(:millisecond) - start
                    :gen_tcp.close(socket)
                    {:ok, duration}

                  _ ->
                    :gen_tcp.close(socket)
                    {:error, :auth_failed}
                end

              _ ->
                :gen_tcp.close(socket)
                {:error, :recv_failed}
            end

          _ ->
            :gen_tcp.close(socket)
            {:error, :send_failed}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end
end

# Run the benchmark
ConnectionBenchmark.run()
