# Backpressure & Overflow Strategies Benchmarks

This directory contains benchmarks for the Resource Management & Backpressure feature.

## Available Benchmarks

### 1. `overflow_strategies_benchmark.exs`

Compares the 4 overflow strategies:
- **drop_newest** (O(1)): Drops new messages, preserves history
- **drop_oldest** (O(log N)): Drops old messages, keeps fresh data  
- **reject** (O(1)): Returns explicit errors
- **block** (O(1) enqueue): Blocks producers with FIFO fairness

**Run:**
```bash
mix run benchmark/overflow_strategies_benchmark.exs
```

**Note:** The "block" strategy test will be slow since it requires timeouts for all 10,000 messages. Consider reducing `overflow_count` in the script for faster results.

### 2. `blocked_producer_benchmark.exs`

Measures blocked producer behavior:
- Block timeout accuracy
- FIFO unblocking latency
- Concurrent blocked producers overhead
- `max_blocked_producers` rejection performance
- FIFO fairness verification

**Run:**
```bash
mix run benchmark/blocked_producer_benchmark.exs
```

## Quick Performance Summary

Based on design analysis (actual benchmarking will vary by workload):

| Strategy | Enqueue Latency | Complexity | Memory | Best For |
|----------|----------------|------------|---------|----------|
| drop_newest | **Fastest** (~1-5μs) | O(1) | Low | High-throughput, history preservation |
| drop_oldest | Fast (~10-50μs) | O(log N) | Low | Keeping fresh data, time-series |
| reject | **Fastest** (~1-5μs) | O(1) | Low | Critical data, explicit handling |
| block | Variable (timeout-dependent) | O(1) | Medium | Fairness, backpressure signaling |

### Key Metrics

**Timeout Accuracy:**
- Target: ±50ms variance
- Tested: 500ms, 1000ms, 2000ms timeouts

**FIFO Unblocking:**
- Strict ordering maintained
- Average latency: < 100ms per unblock

**max_blocked_producers:**
- Rejection: < 100ms (immediate)
- Overhead scales linearly

## Configuration Tips

For benchmarking, consider these settings in your test queue:

```elixir
QueueConfig.create_queue("bench_queue",
  max_buffer_size: 1000,           # Test various sizes: 100, 1k, 10k
  overflow_behavior: :drop_newest,  # Test all 4 strategies
  block_timeout_ms: 1000,           # For :block strategy
  max_blocked_producers: 1000       # Limit concurrent blocking
)
```

## Interpreting Results

### High Throughput Scenarios
- **drop_newest** typically wins (O(1), no locks)
- Expect: 500k+ ops/sec on modern hardware

### Latency-Sensitive Scenarios
- **reject** provides most predictable latency
- **block** adds variable latency (timeout-dependent)

### Memory Constrained
- All strategies use similar memory for buffer
- **block** adds overhead for blocked producer tracking

### Fairness Required
- **block** guarantees FIFO producer ordering
- Other strategies don't guarantee producer fairness

## Running Custom Benchmarks

You can create custom benchmarks using the helper module:

```elixir
alias MalachiMQ.{Queue, QueueConfig}

# Setup
queue = "my_bench_queue"
QueueConfig.create_queue(queue, max_buffer_size: 100, overflow_behavior: :drop_newest)

# Benchmark
{time, _} = :timer.tc(fn ->
  for i <- 1..10_000 do
    Queue.enqueue(queue, "msg_#{i}", %{})
  end
end)

IO.puts("Throughput: #{Float.round(10_000 / (time / 1_000_000), 2)} msg/sec")

# Cleanup
QueueConfig.delete_queue(queue, force: true)
```

## Comparative Analysis

### vs RabbitMQ
- RabbitMQ uses similar strategies (drop-head ≈ drop_oldest)
- MalachiMQ adds explicit `:block` with FIFO fairness
- Both support message TTL and queue length limits

### vs Apache Pulsar
- Pulsar uses backpressure primarily
- MalachiMQ combines backpressure signals + overflow strategies
- MalachiMQ's `:reject` similar to Pulsar's strict validation

### vs Apache Kafka
- Kafka relies on producer-side batching and throttling
- MalachiMQ provides server-side overflow control
- Both support partitioning for scalability

## Performance Tuning

### For Maximum Throughput
1. Use `:drop_newest` (O(1), no overhead)
2. Set `max_buffer_size` to reasonable value (1k-10k)
3. Partition queues across cores
4. Use round-robin consumer dispatch

### For Strict Ordering
1. Use `:block` with appropriate `block_timeout_ms`
2. Set `max_blocked_producers` to prevent memory exhaustion
3. Monitor backpressure signals in producers
4. Implement exponential backoff

### For Critical Data
1. Use `:reject` for explicit error handling
2. Implement retry logic in producers
3. Monitor rejected message metrics
4. Alert on high rejection rates

## Monitoring Recommendations

Track these metrics in production:

```bash
# Via HTTP endpoint
curl http://localhost:4041/metrics | jq '.queues[] | {
  queue, 
  buffer_utilization_pct, 
  blocked_producers_count,
  rejected,
  dropped,
  pressure_status
}'

# Via Dashboard
open http://localhost:4041
```

Key indicators:
- **buffer_utilization_pct > 80%**: High backpressure
- **blocked_producers_count > 0**: Producers waiting
- **rejected > 0**: Messages being rejected
- **dropped > 0**: Messages being silently dropped

## Troubleshooting

### Block Strategy Too Slow
- Reduce `block_timeout_ms` (e.g., 500ms → 100ms)
- Consider using `:drop_newest` or `:reject` instead
- Scale consumers to drain buffer faster

### Memory Growth
- Check `max_blocked_producers` limit
- Verify blocked producers are timing out
- Monitor `blocked_producers_count` metric

### High Rejection Rate
- Increase `max_buffer_size` if appropriate
- Add consumers to drain faster
- Implement backoff in producers
- Consider switching to `:drop_oldest`

## Contributing

To add new benchmarks:

1. Create `benchmark/my_benchmark.exs`
2. Use `Benchee.run/2` for structured tests
3. Document configuration and expected results
4. Update this README with your benchmark

---

**Last Updated:** 2024 (Resource Management & Backpressure feature)
**Maintainer:** MalachiMQ Team
