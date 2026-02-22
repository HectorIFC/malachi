#!/usr/bin/env elixir

# TCP Protocol Performance Benchmark
#
# This benchmark measures the performance of the TCPProtocol module,
# focusing on JSON parsing, message routing, and response generation.
#
# Usage:
#   mix run benchmark/tcp_protocol_benchmark.exs

defmodule TCPProtocolBenchmark do
  @moduledoc """
  Benchmarks for MalachiMQ.TCPProtocol performance.
  
  Tests:
  - JSON decoding throughput
  - Message routing performance
  - Response generation speed
  - Permission checking overhead
  
  Note: Uses a mock socket process to avoid network I/O and measure
  pure protocol performance without socket errors.
  """

  alias MalachiMQ.TCPProtocol

  @iterations 10_000

  @session_admin %{
    username: "admin",
    permissions: [:admin, :produce, :consume],
    token: "benchmark_token"
  }

  @session_producer %{
    username: "producer",
    permissions: [:produce],
    token: "benchmark_token"
  }

  @session_consumer %{
    username: "consumer",
    permissions: [:consume],
    token: "benchmark_token"
  }

  # Mock transport module that discards all sends for benchmarking
  defmodule MockTransport do
    def send(_socket, _data), do: :ok
  end

  def run do
    IO.puts("\n" <> String.duplicate("=", 80))
    IO.puts("MalachiMQ TCP Protocol Performance Benchmark")
    IO.puts(String.duplicate("=", 80))
    IO.puts("Iterations per test: #{format_number(@iterations)}")
    IO.puts("Note: Using mock socket to avoid network I/O")
    IO.puts(String.duplicate("=", 80) <> "\n")

    # Use a fake socket - doesn't matter since MockTransport discards all sends
    socket = :benchmark_socket

    # Warm up
    IO.puts("🔥 Warming up...")
    warm_up(socket)
    IO.puts("✓ Warm-up complete\n")

    # Run benchmarks
    benchmark_json_parsing(socket)
    benchmark_simple_actions(socket)
    benchmark_complex_actions(socket)
    benchmark_permission_checking(socket)
    benchmark_shorthand_publish(socket)
    
    IO.puts("\n" <> String.duplicate("=", 80))
    IO.puts("Benchmark Complete")
    IO.puts(String.duplicate("=", 80) <> "\n")
  end

  defp warm_up(socket) do
    json = ~s({"action":"publish","queue_name":"warmup","payload":"data"})
    
    for _ <- 1..1000 do
      TCPProtocol.process_message(socket, json, @session_producer, MockTransport)
    end
  end

  defp benchmark_json_parsing(socket) do
    IO.puts("📊 Benchmark: JSON Parsing + Routing")
    IO.puts(String.duplicate("-", 80))

    test_cases = [
      {"Simple publish", ~s({"action":"publish","queue_name":"test","payload":"data"})},
      {"Publish with headers", ~s({"action":"publish","queue_name":"test","payload":"data","headers":{"priority":1}})},
      {"Subscribe", ~s({"action":"subscribe","queue_name":"test"})},
      {"ACK", ~s({"action":"ack","message_id":"12345"})},
      {"NACK with requeue", ~s({"action":"nack","message_id":"12345","requeue":true})},
      {"Get queue info", ~s({"action":"get_queue_info","queue_name":"test"})},
      {"List queues", ~s({"action":"list_queues"})},
      {"Shorthand publish", ~s({"queue_name":"test","payload":"data"})},
      {"Invalid JSON", "not valid json"},
      {"Invalid request", ~s({"action":"unknown"})}
    ]

    for {name, json} <- test_cases do
      {time_us, _} = :timer.tc(fn ->
        for _ <- 1..@iterations do
          TCPProtocol.process_message(socket, json, @session_admin, MockTransport)
        end
      end)

      ops_per_sec = @iterations / (time_us / 1_000_000)
      avg_latency_us = time_us / @iterations

      IO.puts("  #{name}:")
      IO.puts("    #{format_number(round(ops_per_sec))} ops/sec")
      IO.puts("    #{Float.round(avg_latency_us, 2)} µs/op")
    end

    IO.puts("")
  end

  defp benchmark_simple_actions(socket) do
    IO.puts("📊 Benchmark: Simple Actions (Publish/Subscribe)")
    IO.puts(String.duplicate("-", 80))

    publish_json = ~s({"action":"publish","queue_name":"bench","payload":"test"})
    subscribe_json = ~s({"action":"subscribe","queue_name":"bench"})

    {publish_time, _} = :timer.tc(fn ->
      for _ <- 1..@iterations do
        TCPProtocol.process_message(socket, publish_json, @session_producer, MockTransport)
      end
    end)

    {subscribe_time, _} = :timer.tc(fn ->
      for _ <- 1..@iterations do
        TCPProtocol.process_message(socket, subscribe_json, @session_consumer, MockTransport)
      end
    end)

    publish_ops = @iterations / (publish_time / 1_000_000)
    subscribe_ops = @iterations / (subscribe_time / 1_000_000)

    IO.puts("  Publish:")
    IO.puts("    #{format_number(round(publish_ops))} ops/sec")
    IO.puts("    #{Float.round(publish_time / @iterations, 2)} µs/op")
    
    IO.puts("  Subscribe:")
    IO.puts("    #{format_number(round(subscribe_ops))} ops/sec")
    IO.puts("    #{Float.round(subscribe_time / @iterations, 2)} µs/op")
    
    IO.puts("")
  end

  defp benchmark_complex_actions(socket) do
    IO.puts("📊 Benchmark: Complex Actions (Admin Operations)")
    IO.puts(String.duplicate("-", 80))

    create_queue_json = ~s({"action":"create_queue","queue_name":"bench","delivery_mode":"at_least_once","max_retries":5,"dlq_enabled":true})
    delete_queue_json = ~s({"action":"delete_queue","queue_name":"bench","force":true})
    get_info_json = ~s({"action":"get_queue_info","queue_name":"bench"})
    list_queues_json = ~s({"action":"list_queues"})

    test_cases = [
      {"Create queue (full config)", create_queue_json},
      {"Delete queue (forced)", delete_queue_json},
      {"Get queue info", get_info_json},
      {"List queues", list_queues_json}
    ]

    iterations = @iterations
    reduced_iterations = div(iterations, 10)

    for {name, json} <- test_cases do
      {time_us, _} = :timer.tc(fn ->
        for _ <- 1..reduced_iterations do  # Fewer iterations for complex ops
          TCPProtocol.process_message(socket, json, @session_admin, MockTransport)
        end
      end)

      ops_per_sec = reduced_iterations / (time_us / 1_000_000)
      avg_latency_us = time_us / reduced_iterations

      IO.puts("  #{name}:")
      IO.puts("    #{format_number(round(ops_per_sec))} ops/sec")
      IO.puts("    #{Float.round(avg_latency_us, 2)} µs/op")
    end

    IO.puts("")
  end

  defp benchmark_permission_checking(socket) do
    IO.puts("📊 Benchmark: Permission Checking Overhead")
    IO.puts(String.duplicate("-", 80))

    publish_json = ~s({"action":"publish","queue_name":"test","payload":"data"})

    # With permission
    {time_allowed, _} = :timer.tc(fn ->
      for _ <- 1..@iterations do
        TCPProtocol.process_message(socket, publish_json, @session_producer, MockTransport)
      end
    end)

    # Without permission
    {time_denied, _} = :timer.tc(fn ->
      for _ <- 1..@iterations do
        TCPProtocol.process_message(socket, publish_json, @session_consumer, MockTransport)
      end
    end)

    allowed_ops = @iterations / (time_allowed / 1_000_000)
    denied_ops = @iterations / (time_denied / 1_000_000)

    IO.puts("  Permission granted:")
    IO.puts("    #{format_number(round(allowed_ops))} ops/sec")
    IO.puts("    #{Float.round(time_allowed / @iterations, 2)} µs/op")
    
    IO.puts("  Permission denied:")
    IO.puts("    #{format_number(round(denied_ops))} ops/sec")
    IO.puts("    #{Float.round(time_denied / @iterations, 2)} µs/op")
    
    overhead_pct = ((time_denied - time_allowed) / time_allowed) * 100
    IO.puts("  Denial overhead: #{Float.round(overhead_pct, 2)}%")
    
    IO.puts("")
  end

  defp benchmark_shorthand_publish(socket) do
    IO.puts("📊 Benchmark: Shorthand vs Explicit Publish")
    IO.puts(String.duplicate("-", 80))

    explicit_json = ~s({"action":"publish","queue_name":"test","payload":"data"})
    shorthand_json = ~s({"queue_name":"test","payload":"data"})

    {explicit_time, _} = :timer.tc(fn ->
      for _ <- 1..@iterations do
        TCPProtocol.process_message(socket, explicit_json, @session_producer, MockTransport)
      end
    end)

    {shorthand_time, _} = :timer.tc(fn ->
      for _ <- 1..@iterations do
        TCPProtocol.process_message(socket, shorthand_json, @session_producer, MockTransport)
      end
    end)

    explicit_ops = @iterations / (explicit_time / 1_000_000)
    shorthand_ops = @iterations / (shorthand_time / 1_000_000)

    IO.puts("  Explicit action field:")
    IO.puts("    #{format_number(round(explicit_ops))} ops/sec")
    IO.puts("    #{Float.round(explicit_time / @iterations, 2)} µs/op")
    
    IO.puts("  Shorthand (no action):")
    IO.puts("    #{format_number(round(shorthand_ops))} ops/sec")
    IO.puts("    #{Float.round(shorthand_time / @iterations, 2)} µs/op")
    
    diff_pct = ((shorthand_time - explicit_time) / explicit_time) * 100
    IO.puts("  Difference: #{Float.round(diff_pct, 2)}%")
    
    IO.puts("")
  end

  defp format_number(num) when num >= 1_000_000 do
    "#{Float.round(num / 1_000_000, 2)}M"
  end

  defp format_number(num) when num >= 1_000 do
    "#{Float.round(num / 1_000, 2)}K"
  end

  defp format_number(num) do
    Integer.to_string(num)
  end
end

# Start the application if not already started
unless Process.whereis(MalachiMQ.Auth) do
  IO.puts("Starting MalachiMQ application...")
  {:ok, _} = Application.ensure_all_started(:malachimq)
  Process.sleep(500)
end

# Run the benchmark
TCPProtocolBenchmark.run()
