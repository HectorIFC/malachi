# Benchmark: Overflow Strategies Performance Comparison
#
# This benchmark compares the 4 overflow strategies:
# - drop_newest (O(1) - default, preserves history)
# - drop_oldest (O(log N) - keeps fresh data)
# - reject (O(1) - explicit errors)
# - block (O(1) enqueue, O(N) timeout removal - fairness)
#
# Usage:
#   mix run benchmark/overflow_strategies_benchmark.exs

Application.ensure_all_started(:malachimq)

defmodule OverflowBenchmark do
  alias MalachiMQ.{Queue, QueueConfig}

  def setup_queue(strategy, max_buffer) do
    queue_name = "bench_#{strategy}_#{:erlang.unique_integer([:positive])}"
    
    QueueConfig.create_queue(queue_name,
      max_buffer_size: max_buffer,
      overflow_behavior: strategy,
      block_timeout_ms: 1000
    )
    
    queue_name
  end

  def fill_buffer(queue_name, count) do
    for i <- 1..count do
      Queue.enqueue(queue_name, "init_#{i}", %{})
    end
  end

  def benchmark_overflow(queue_name, message_count) do
    for i <- 1..message_count do
      Queue.enqueue(queue_name, "overflow_#{i}", %{})
    end
  end

  def cleanup(queue_name) do
    try do
      QueueConfig.delete_queue(queue_name, force: true)
    catch
      _, _ -> :ok
    end
  end
end

IO.puts("\n=== Overflow Strategies Benchmark ===\n")
IO.puts("Comparing 4 overflow strategies with buffer size 1000")
IO.puts("Each test enqueues 10,000 messages into a full buffer\n")

buffer_size = 1000
overflow_count = 10_000

Benchee.run(
  %{
    "drop_newest (O(1))" => fn ->
      queue = OverflowBenchmark.setup_queue(:drop_newest, buffer_size)
      OverflowBenchmark.fill_buffer(queue, buffer_size)
      OverflowBenchmark.benchmark_overflow(queue, overflow_count)
      OverflowBenchmark.cleanup(queue)
    end,
    
    "drop_oldest (O(log N))" => fn ->
      queue = OverflowBenchmark.setup_queue(:drop_oldest, buffer_size)
      OverflowBenchmark.fill_buffer(queue, buffer_size)
      OverflowBenchmark.benchmark_overflow(queue, overflow_count)
      OverflowBenchmark.cleanup(queue)
    end,
    
    "reject (O(1))" => fn ->
      queue = OverflowBenchmark.setup_queue(:reject, buffer_size)
      OverflowBenchmark.fill_buffer(queue, buffer_size)
      OverflowBenchmark.benchmark_overflow(queue, overflow_count)
      OverflowBenchmark.cleanup(queue)
    end,
    
    "block (with timeout)" => fn ->
      queue = OverflowBenchmark.setup_queue(:block, buffer_size)
      OverflowBenchmark.fill_buffer(queue, buffer_size)
      
      # Block will timeout all messages since buffer never drains
      OverflowBenchmark.benchmark_overflow(queue, overflow_count)
      OverflowBenchmark.cleanup(queue)
    end
  },
  time: 5,
  warmup: 2,
  memory_time: 2,
  formatters: [
    {Benchee.Formatters.Console, extended_statistics: true}
  ]
)

IO.puts("\n=== Latency at Different Buffer Sizes ===\n")

Benchee.run(
  %{
    "drop_newest @ 100" => fn ->
      queue = OverflowBenchmark.setup_queue(:drop_newest, 100)
      OverflowBenchmark.fill_buffer(queue, 100)
      Queue.enqueue(queue, "test", %{})
      OverflowBenchmark.cleanup(queue)
    end,
    
    "drop_newest @ 1,000" => fn ->
      queue = OverflowBenchmark.setup_queue(:drop_newest, 1000)
      OverflowBenchmark.fill_buffer(queue, 1000)
      Queue.enqueue(queue, "test", %{})
      OverflowBenchmark.cleanup(queue)
    end,
    
    "drop_newest @ 10,000" => fn ->
      queue = OverflowBenchmark.setup_queue(:drop_newest, 10_000)
      OverflowBenchmark.fill_buffer(queue, 10_000)
      Queue.enqueue(queue, "test", %{})
      OverflowBenchmark.cleanup(queue)
    end,
    
    "drop_oldest @ 100" => fn ->
      queue = OverflowBenchmark.setup_queue(:drop_oldest, 100)
      OverflowBenchmark.fill_buffer(queue, 100)
      Queue.enqueue(queue, "test", %{})
      OverflowBenchmark.cleanup(queue)
    end,
    
    "drop_oldest @ 1,000" => fn ->
      queue = OverflowBenchmark.setup_queue(:drop_oldest, 1000)
      OverflowBenchmark.fill_buffer(queue, 1000)
      Queue.enqueue(queue, "test", %{})
      OverflowBenchmark.cleanup(queue)
    end,
    
    "drop_oldest @ 10,000" => fn ->
      queue = OverflowBenchmark.setup_queue(:drop_oldest, 10_000)
      OverflowBenchmark.fill_buffer(queue, 10_000)
      Queue.enqueue(queue, "test", %{})
      OverflowBenchmark.cleanup(queue)
    end,
    
    "reject @ 100" => fn ->
      queue = OverflowBenchmark.setup_queue(:reject, 100)
      OverflowBenchmark.fill_buffer(queue, 100)
      Queue.enqueue(queue, "test", %{})
      OverflowBenchmark.cleanup(queue)
    end,
    
    "reject @ 1,000" => fn ->
      queue = OverflowBenchmark.setup_queue(:reject, 1000)
      OverflowBenchmark.fill_buffer(queue, 1000)
      Queue.enqueue(queue, "test", %{})
      OverflowBenchmark.cleanup(queue)
    end,
    
    "reject @ 10,000" => fn ->
      queue = OverflowBenchmark.setup_queue(:reject, 10_000)
      OverflowBenchmark.fill_buffer(queue, 10_000)
      Queue.enqueue(queue, "test", %{})
      OverflowBenchmark.cleanup(queue)
    end
  },
  time: 3,
  warmup: 1,
  formatters: [
    {Benchee.Formatters.Console, extended_statistics: true}
  ]
)

IO.puts("\n=== Memory Overhead Comparison ===\n")

Benchee.run(
  %{
    "drop_newest (1000 buffer)" => fn ->
      queue = OverflowBenchmark.setup_queue(:drop_newest, 1000)
      OverflowBenchmark.fill_buffer(queue, 1000)
      :timer.sleep(100)  # Let metrics settle
      OverflowBenchmark.cleanup(queue)
    end,
    
    "drop_oldest (1000 buffer)" => fn ->
      queue = OverflowBenchmark.setup_queue(:drop_oldest, 1000)
      OverflowBenchmark.fill_buffer(queue, 1000)
      :timer.sleep(100)
      OverflowBenchmark.cleanup(queue)
    end,
    
    "reject (1000 buffer)" => fn ->
      queue = OverflowBenchmark.setup_queue(:reject, 1000)
      OverflowBenchmark.fill_buffer(queue, 1000)
      :timer.sleep(100)
      OverflowBenchmark.cleanup(queue)
    end,
    
    "block (no blocked producers)" => fn ->
      queue = OverflowBenchmark.setup_queue(:block, 1000)
      OverflowBenchmark.fill_buffer(queue, 1000)
      :timer.sleep(100)
      OverflowBenchmark.cleanup(queue)
    end
  },
  time: 3,
  warmup: 1,
  memory_time: 2,
  formatters: [
    {Benchee.Formatters.Console, extended_statistics: true}
  ]
)

IO.puts("\n✅ Benchmark complete!")
IO.puts("\nKey Insights:")
IO.puts("  • drop_newest: Fastest, O(1), best for high-throughput")
IO.puts("  • drop_oldest: O(log N), good for keeping fresh data")
IO.puts("  • reject: O(1), explicit errors, best for critical data")
IO.puts("  • block: O(1) enqueue, fairness guarantee, variable latency")
