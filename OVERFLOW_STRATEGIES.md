# Overflow Strategies & Backpressure

Malachi provides comprehensive resource management through configurable overflow behaviors and intelligent backpressure signaling. This document explains the available strategies, their use cases, and how to configure them.

## Table of Contents

- [Overview](#overview)
- [Configuration](#configuration)
- [Overflow Strategies](#overflow-strategies)
  - [Drop Newest (Default)](#drop-newest-default)
  - [Drop Oldest](#drop-oldest)
  - [Reject](#reject)
  - [Block](#block)
- [Backpressure Signaling](#backpressure-signaling)
- [Message Size Limits](#message-size-limits)
- [Dynamic Configuration Updates](#dynamic-configuration-updates)
- [Monitoring & Metrics](#monitoring-metrics)
- [Best Practices](#best-practices)
- [Examples](#examples)

## Overview

When queues reach their configured buffer limits, Malachi needs to decide how to handle additional messages. The overflow strategy determines this behavior. Additionally, the system provides backpressure signals to producers before reaching capacity, allowing graceful degradation.

### Key Features

- **4 Overflow Strategies**: Choose behavior when buffer is full
- **Backpressure Signaling**: Warn producers when queue is nearing capacity
- **Message Size Validation**: Prevent oversized messages from entering the system
- **Dynamic Updates**: Change configuration at runtime with safety checks
- **FIFO Unblocking**: Fair producer resumption when using block strategy
- **Rate-Limited Logging**: Prevent log spam during high overflow events

## Configuration

### Environment Variables

Set these in `config/runtime.exs` or via environment:

```bash
# Maximum message size in bytes (default: 1MB)
export MALACHIMQ_MAX_MESSAGE_SIZE=1048576

# Maximum buffered messages per queue (default: 10,000)
export MALACHIMQ_MAX_BUFFER_SIZE=10000

# Overflow behavior: drop_newest, drop_oldest, reject, block (default: drop_newest)
export MALACHIMQ_OVERFLOW_BEHAVIOR=drop_newest

# Backpressure threshold 0.0-1.0 (default: 0.8 = 80% full)
export MALACHIMQ_BACKPRESSURE_THRESHOLD=0.8

# Timeout for blocked producers in milliseconds (default: 5000)
export MALACHIMQ_BLOCK_TIMEOUT_MS=5000

# Maximum concurrent blocked producers (default: 1000)
export MALACHIMQ_MAX_BLOCKED_PRODUCERS=1000

# Update safety threshold for config changes (default: 0.5 = 50%)
export MALACHIMQ_UPDATE_EXCESS_THRESHOLD=0.5
```

### Per-Queue Configuration

Create queues with specific settings via TCP protocol:

```json
{
  "action": "create_queue",
  "queue_name": "orders",
  "max_buffer_size": 50000,
  "overflow_behavior": "block",
  "backpressure_threshold": 0.7,
  "block_timeout_ms": 10000,
  "max_blocked_producers": 500,
  "max_message_size_bytes": 2097152
}
```

## Overflow Strategies

### Drop Newest (Default)

**Complexity**: O(1)  
**When to Use**: High-throughput scenarios where recent data is less valuable than historical context  
**Behavior**: Incoming message is silently dropped when buffer is full

```elixir
# Configuration
overflow_behavior: :drop_newest
```

**Characteristics**:
- ✅ Fastest performance (no buffer modification)
- ✅ Preserves oldest messages (historical context)
- ✅ No producer blocking
- ❌ Silent message loss
- ❌ Producers not notified of drop

**Metrics Updated**:
- `dropped` counter incremented
- Overflow event logged (rate-limited to 10/min)

**Use Cases**:
- Real-time sensor data where latest readings may duplicate earlier ones
- Log aggregation where initial context is more valuable
- High-frequency trading data where historical context matters

### Drop Oldest

**Complexity**: O(log N) due to ETS first/delete operations  
**When to Use**: Scenarios where freshest data is critical  
**Behavior**: Oldest buffered message is removed, new message is accepted

```elixir
# Configuration
overflow_behavior: :drop_oldest
```

**Characteristics**:
- ✅ Keeps newest data
- ✅ No producer blocking
- ✅ Sliding window behavior
- ❌ Slightly slower than drop_newest
- ❌ Silent message loss
- ❌ Historical context lost

**Metrics Updated**:
- `dropped` counter incremented
- Overflow event logged (rate-limited)

**Use Cases**:
- Stock price updates (latest price is what matters)
- Live dashboards showing current state
- Notification queues (recent alerts more relevant)
- Cache-like message queues

### Reject

**Complexity**: O(1)  
**When to Use**: When message loss is unacceptable and producers must handle backpressure  
**Behavior**: Returns error to producer, message is not accepted

```elixir
# Configuration
overflow_behavior: :reject
```

**Characteristics**:
- ✅ No silent message loss
- ✅ Producer receives explicit error
- ✅ Fastest (no buffer modification)
- ✅ Forces producer-side backpressure handling
- ❌ Requires producer retry logic
- ❌ May cause thundering herd if many producers retry

**Error Response**:
```json
{
  "s": "err",
  "reason": "queue_full"
}
```

**Metrics Updated**:
- `rejected` counter incremented
- Overflow event logged (rate-limited)

**Use Cases**:
- Financial transactions (must not lose)
- Critical command queues
- Audit logging systems
- When producers have retry capability

### Block

**Complexity**: O(1) enqueue, O(N) timeout removal (rare)  
**When to Use**: When guaranteed delivery is required and producers can wait  
**Behavior**: Producer call blocks until buffer space available or timeout occurs

```elixir
# Configuration
overflow_behavior: :block
block_timeout_ms: 5000  # Wait up to 5 seconds
max_blocked_producers: 1000  # Safety limit
```

**Characteristics**:
- ✅ No message loss (within timeout)
- ✅ FIFO unblocking (fair producer resumption)
- ✅ Individual timeouts per producer
- ✅ Graceful degradation under load
- ❌ Producers consume resources while blocked
- ❌ Risk of producer timeout cascades

**Data Structure**:
```elixir
%{
  queue: :queue.new(),           # FIFO queue of blocked requests
  lookup: %{from => {msg, ref, timer_ref}}  # O(1) timeout removal
}
```

**Error Response (timeout)**:
```json
{
  "s": "err",
  "reason": "producer_blocked",
  "timeout_ms": 5000
}
```

**Metrics Updated**:
- `blocked_producers_count` gauge (current blocked)
- `total_producers_blocked` counter (cumulative)
- Unblocked when space available (FIFO order)

**Use Cases**:
- Order processing (can wait briefly)
- Job queues with retry tolerance
- Event sourcing systems
- Scenarios with bursty traffic patterns

**Safety Limits**:
- `max_blocked_producers`: Rejects new block requests when limit reached
- Individual timeouts: Prevents indefinite blocking
- FIFO unblocking: Ensures fairness

## Backpressure Signaling

Malachi provides proactive backpressure signals independent of overflow strategy. Producers receive warnings when queues approach capacity, allowing graceful slowdown.

### Pressure Levels

```elixir
:low_pressure      # Buffer < 50% full
:medium_pressure   # Buffer 50-79% full  
:high_pressure     # Buffer >= threshold (default 80%)
:full              # Buffer at 100% capacity
```

### Publish Response with Backpressure

When `pressure >= backpressure_threshold`:

```json
{
  "s": "ok",
  "backpressure": true,
  "pressure_status": "high_pressure"
}
```

When `pressure < backpressure_threshold`:

```json
{
  "s": "ok"
}
```

### Producer Response Strategies

1. **Exponential Backoff**: Slow publishing rate when backpressure detected
2. **Circuit Breaker**: Stop publishing temporarily, retry after cooldown
3. **Alternative Routing**: Send to backup queue or dead letter
4. **Rate Limiting**: Apply client-side rate limits dynamically

### Example Producer (Node.js)

```javascript
class BackpressureProducer {
  constructor(socket, queueName) {
    this.socket = socket;
    this.queueName = queueName;
    this.backoffMs = 0;
  }

  async publish(payload) {
    if (this.backoffMs > 0) {
      await new Promise(resolve => setTimeout(resolve, this.backoffMs));
    }

    const msg = {
      action: "publish",
      queue_name: this.queueName,
      payload: payload
    };

    const response = await this.sendAndWait(msg);

    if (response.backpressure) {
      // Apply exponential backoff
      this.backoffMs = Math.min(this.backoffMs * 2 || 100, 5000);
      console.warn(`Backpressure detected: ${response.pressure_status}, backing off ${this.backoffMs}ms`);
    } else {
      // Reset backoff on success
      this.backoffMs = 0;
    }

    return response;
  }
}
```

## Message Size Limits

Validate message size **before** creating Message struct to prevent resource exhaustion.

### Validation Order

```elixir
# TCP Protocol publish handler (optimized):
1. Check rate limit (FIRST - cheapest check)
2. Validate message size (prevents large message processing)
3. Validate queue name
4. Validate payload format
5. Validate headers
6. Enqueue message
```

### Size Violation Response

```json
{
  "s": "err",
  "reason": "message_too_large",
  "actual_bytes": 2097152,
  "max_bytes": 1048576
}
```

## Dynamic Configuration Updates

Update queue configuration at runtime with safety checks.

### Hybrid Validation Approach

Inspired by RabbitMQ's drop-head strategy and Pulsar's validation:

- **Excess ≤ 50% of new limit**: Allow update with warning
- **Excess > 50% of new limit**: Require `force: true` flag

### Update Examples

#### Safe Update (Small Excess)

```json
{
  "action": "update_queue_config",
  "queue_name": "orders",
  "max_buffer_size": 8000
}
```

Response (current buffer: 9000):
```json
{
  "s": "ok",
  "status": "updated_with_warning",
  "warning": "Queue buffer size exceeds new limit",
  "excess_messages": 1000
}
```

#### Rejected Update (Large Excess)

```json
{
  "action": "update_queue_config",
  "queue_name": "orders",
  "max_buffer_size": 5000
}
```

Response (current buffer: 9000):
```json
{
  "s": "err",
  "reason": "update_rejected",
  "current_buffer_size": 9000,
  "new_max_buffer_size": 5000,
  "excess_messages": 4000,
  "threshold_pct": 50.0,
  "suggestion": "drain queue or use force: true"
}
```

#### Forced Update

```json
{
  "action": "update_queue_config",
  "queue_name": "orders",
  "max_buffer_size": 5000,
  "force": true
}
```

Response:
```json
{
  "s": "ok",
  "status": "forced_update",
  "message": "Configuration updated with force flag",
  "excess_messages": 4000
}
```

### Updatable Fields

- `max_buffer_size`: Buffer capacity
- `overflow_behavior`: Strategy (`:drop_newest` | `:drop_oldest` | `:reject` | `:block`)
- `backpressure_threshold`: 0.0-1.0 pressure signal threshold
- `block_timeout_ms`: Timeout for blocked producers
- `max_blocked_producers`: Concurrent blocked producer limit
- `max_message_size_bytes`: Maximum message size

<a id="monitoring-metrics"></a>

## Monitoring & Metrics

### Dashboard Visualization

The web dashboard (`http://localhost:4041`) displays:

- **Buffer Utilization**: Animated progress bar with color coding
  - Green (0-50%): `:low_pressure`
  - Yellow (50-80%): `:medium_pressure`
  - Orange (80-100%): `:high_pressure`
  - Red (100%): `:full` with pulsing animation
- **Blocked Producers**: Current count (gauge)
- **Pressure Status**: Badge with real-time status
- **Overflow Behavior**: Current strategy
- **Rejected/Dropped Counters**: Cumulative overflow events

### Metrics Endpoint

`GET /metrics` returns JSON:

```json
{
  "queues": [
    {
      "queue": "orders",
      "buffer_utilization_pct": 85.5,
      "backpressure_status": "high_pressure",
      "overflow_behavior": "block",
      "max_buffer_size": 10000,
      "blocked_producers_count": 5,
      "total_producers_blocked": 120,
      "rejected": 0,
      "dropped": 0,
      "queue_stats": {
        "buffered": 8550,
        "consumers": 3,
        "producers": 10
      }
    }
  ]
}
```

### Server-Sent Events (SSE)

Real-time updates via `/stream`:

```javascript
const source = new EventSource('http://localhost:4041/stream');
source.onmessage = (event) => {
  const data = JSON.parse(event.data);
  // Update UI with queue metrics including backpressure
};
```

## Best Practices

### 1. Choose Strategy by Use Case

| Scenario | Recommended Strategy | Reason |
|----------|---------------------|--------|
| High-frequency sensors | `drop_newest` | Historical context valuable |
| Stock prices | `drop_oldest` | Latest data critical |
| Financial transactions | `reject` | No silent loss |
| Job queues | `block` | Guaranteed delivery |
| Logs (non-critical) | `drop_newest` | Performance priority |

### 2. Set Conservative Thresholds

```elixir
# Start conservative, tune based on metrics
backpressure_threshold: 0.7  # Signal at 70% instead of 80%
block_timeout_ms: 3000       # Fail fast to prevent cascades
max_blocked_producers: 100   # Prevent resource exhaustion
```

### 3. Monitor Overflow Metrics

```bash
# Check for sustained overflow events
curl http://localhost:4041/metrics | jq '.queues[] | select(.dropped > 1000)'

# Alert on high rejection rates
curl http://localhost:4041/metrics | jq '.queues[] | select(.rejected > 100)'
```

### 4. Implement Producer Backoff

Always respond to backpressure signals:

```elixir
# Bad: Ignore backpressure
publish_message(payload)

# Good: Exponential backoff
if backpressure_detected?() do
  :timer.sleep(backoff_ms())
end
publish_message(payload)
```

### 5. Test Overflow Scenarios

```bash
# Benchmark overflow behavior
Malachi.Benchmark.test_overflow("queue", :drop_newest, 100_000)

# Measure blocked producer latency
Malachi.Benchmark.test_blocking("queue", 1000, 5000)
```

### 6. Use Force Updates Carefully

```bash
# Drain queue before reducing buffer size
update_queue_config(queue, max_buffer_size: 5000)  # Check response

# Only force if drain is not possible
update_queue_config(queue, max_buffer_size: 5000, force: true)  # Last resort
```

## Examples

### Example 1: E-Commerce Order Queue

```json
{
  "action": "create_queue",
  "queue_name": "orders",
  "max_buffer_size": 50000,
  "overflow_behavior": "block",
  "backpressure_threshold": 0.75,
  "block_timeout_ms": 10000,
  "max_message_size_bytes": 524288
}
```

**Rationale**:
- `block`: Orders must not be lost, brief wait acceptable
- 50k buffer: Handles Black Friday spikes
- 75% threshold: Early warning for scaling
- 10s timeout: Reasonable wait for customer orders

### Example 2: IoT Sensor Data

```json
{
  "action": "create_queue",
  "queue_name": "sensor_readings",
  "max_buffer_size": 100000,
  "overflow_behavior": "drop_newest",
  "backpressure_threshold": 0.9,
  "max_message_size_bytes": 1024
}
```

**Rationale**:
- `drop_newest`: Duplicate sensor data, historical trends valuable
- 100k buffer: High-frequency data (10 sensors × 100 Hz)
- 90% threshold: Aggressive buffering before backpressure
- 1KB messages: Sensor payloads are small

### Example 3: Stock Price Updates

```json
{
  "action": "create_queue",
  "queue_name": "stock_prices",
  "max_buffer_size": 10000,
  "overflow_behavior": "drop_oldest",
  "backpressure_threshold": 0.8,
  "max_message_size_bytes": 2048
}
```

**Rationale**:
- `drop_oldest`: Latest price is what matters for trading
- 10k buffer: Reasonable for real-time market data
- 80% threshold: Standard backpressure signal
- 2KB messages: Price + metadata

### Example 4: Critical Audit Log

```json
{
  "action": "create_queue",
  "queue_name": "audit_log",
  "max_buffer_size": 200000,
  "overflow_behavior": "reject",
  "backpressure_threshold": 0.7,
  "max_message_size_bytes": 10485760
}
```

**Rationale**:
- `reject`: No message loss, force producer retry
- 200k buffer: Large capacity for audit compliance
- 70% threshold: Early warning for capacity planning
- 10MB messages: May include large audit payloads

## Performance Characteristics

| Strategy | Enqueue | Memory | Producer Impact | Consumer Impact |
|----------|---------|--------|----------------|----------------|
| `drop_newest` | O(1) | Constant | None | None |
| `drop_oldest` | O(log N) | Constant | None | None |
| `reject` | O(1) | Constant | Error handling | None |
| `block` | O(1) | O(N blocked) | Blocking + timeout | None |

### Overflow Event Logging

Rate-limited to **10 events per minute per queue** via ETS-based debouncing:

```elixir
# ETS table: {:queue_name, :last_log_time}
# Only logs if > 6 seconds since last log
```

Prevents log spam during sustained overflow while maintaining visibility.

## Troubleshooting

### High Rejection Rate

**Symptom**: Many `rejected` events in metrics  
**Cause**: Queue using `:reject` strategy, buffer full  
**Solution**:
1. Scale consumers: `Malachi.Consumer.start(queue, handler, count: 10)`
2. Increase buffer: `update_queue_config(queue, max_buffer_size: 20000)`
3. Switch strategy: `update_queue_config(queue, overflow_behavior: "block")`

### Producers Timing Out

**Symptom**: Many `producer_blocked` errors  
**Cause**: Queue using `:block`, consumers too slow  
**Solution**:
1. Increase timeout: `update_queue_config(queue, block_timeout_ms: 15000)`
2. Scale consumers vertically (optimize handler)
3. Scale consumers horizontally (more processes)
4. Consider `:drop_oldest` if data is replaceable

### Silent Message Loss

**Symptom**: `dropped` counter increasing  
**Cause**: Queue using `:drop_newest` or `:drop_oldest`  
**Solution**:
1. Monitor backpressure signals in producers
2. Implement producer-side rate limiting
3. Scale consumers to drain buffer
4. Switch to `:reject` or `:block` if loss unacceptable

### Blocked Producer Count Growing

**Symptom**: `blocked_producers_count` gauge constantly high  
**Cause**: Consumers not draining buffer fast enough  
**Solution**:
1. Scale consumers immediately
2. Lower `block_timeout_ms` to fail fast
3. Reduce `max_blocked_producers` to reject earlier
4. Check consumer handler for bottlenecks

## Related Documentation

- [README.md](README.md) - General Malachi documentation
- [RATE_LIMITING.md](RATE_LIMITING.md) - Rate limiting strategies
- [DOCKER_README.md](DOCKER_README.md) - Container deployment
- Benchmarking: `benchmark/` directory

## License

Malachi is released under the MIT License. See LICENSE for details.
