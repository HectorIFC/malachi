# MalachiMQ Performance Baseline Benchmarks

Comprehensive benchmark suite to measure and track MalachiMQ performance over time.

## 📋 Overview

This benchmark suite establishes performance baselines for:

- **Throughput**: Message processing rate (msgs/sec, MB/sec)
- **Latency**: End-to-end delivery time (p50, p95, p99 in μs)
- **Memory**: Memory usage per connection, message, and queue
- **Connections**: TCP connection capacity and establishment rate
- **Authentication**: Token validation and permission checking performance
- **Edge Cases**: Extreme scenarios (large messages, empty messages, max connections, CPU saturation)
- **Sustained Load**: 24-hour test to detect memory leaks and degradation

## 🚀 Quick Start

### Run All Benchmarks

```bash
./benchmark/run_all_baselines.sh
```

This will execute all benchmarks (except sustained load) sequentially (~10 minutes) and generate a consolidated report in `benchmark/results/`.

### Individual Benchmarks

```bash
# Throughput
elixir benchmark/baseline_throughput.exs

# Latency
elixir benchmark/baseline_latency.exs

# Memory
elixir benchmark/baseline_memory.exs

# Connections
elixir benchmark/baseline_connections.exs

# Authentication
elixir benchmark/baseline_auth.exs

# Edge cases
elixir benchmark/baseline_edge_cases.exs

# Sustained load (24 hours - run manually)
elixir benchmark/baseline_sustained_load.exs
```

## 📊 Generate Reference Baseline

Before implementing new features, generate a reference baseline by running the full suite:

```bash
./benchmark/run_all_baselines.sh
```

The consolidated result will be saved in `benchmark/results/baseline_YYYYMMDD_HHMMSS.json`. Rename it as a reference:

```bash
cp benchmark/results/baseline_YYYYMMDD_HHMMSS.json benchmark/results/baseline_reference.json
```

**Important**: Commit this file to the repository:

```bash
git add benchmark/results/baseline_reference.json
git commit -m "chore: add performance baseline reference for v0.4.6"
```

## 🔍 Compare Results

Compare a new result against the reference baseline:

```bash
./benchmark/compare_baselines.exs \
  benchmark/results/baseline_reference.json \
  benchmark/results/baseline_20260203_120000.json
```

The script:
- Displays colorized report in the console
- Detects degradation >5% in any metric
- Returns exit code 1 if regressions are found (useful for CI)

## 🤖 CI/CD Integration

The `.github/workflows/benchmark.yml` workflow runs automatically on each PR:

1. Compiles the project from scratch (no cache)
2. Runs the full benchmark suite
3. Compares with `baseline_reference.json`
4. Posts comment on PR with results
5. **Fails CI if degradation >5% is detected**

### Disable Regression Check

If a degradation is acceptable (e.g., intentional tradeoff), add to the PR body:

```
[skip benchmark check]
```

Or temporarily modify the threshold in `benchmark/utils/comparator.ex`.

## 📁 File Structure

```
benchmark/
├── utils/
│   ├── benchmark_helpers.ex      # Shared utilities (timing, memory)
│   ├── percentile.ex              # Percentile calculation (p50, p95, p99)
│   ├── reporter.ex                # Results formatting (JSON/CSV/console)
│   ├── comparator.ex              # Baseline comparison and regression detection
│   └── aggregate_results.exs      # Multiple run aggregation
├── baseline_throughput.exs        # Throughput benchmark
├── baseline_latency.exs           # Latency benchmark
├── baseline_memory.exs            # Memory benchmark
├── baseline_connections.exs       # Connections benchmark
├── baseline_auth.exs              # Authentication benchmark
├── baseline_edge_cases.exs        # Edge cases benchmark
├── baseline_sustained_load.exs    # Sustained load benchmark (24h)
├── compare_baselines.exs          # Comparison script
├── run_all_baselines.sh           # Run all benchmarks
└── results/
    ├── .gitkeep
    └── baseline_reference.json    # Reference baseline (committed)
```

## 📈 Results Format

All benchmarks generate results in the format:

```json
{
  "benchmark": "baseline_throughput",
  "timestamp": "2026-02-03T10:00:00Z",
  "version": "0.4.6",
  "system_info": {
    "schedulers_online": 8,
    "process_count": 150,
    "os": "unix linux"
  },
  "results": {
    "single_producer_consumer": {
      "1024": {
        "message_size_bytes": 1024,
        "throughput_msgs_per_sec": 125000.0,
        "throughput_mb_per_sec": 122.07,
        "latency_avg_us": 45.2
      }
    }
  }
}
```

## ⚙️ Configuration

Edit `config/benchmark.exs` to adjust:

```elixir
config :malachimq,
  benchmark_duration_sec: 60,        # Duration of each test
  benchmark_warmup_sec: 10,          # Warm-up before each test
  benchmark_message_sizes: [...],    # Message sizes to test
  benchmark_connection_counts: [...], # Connection counts to test
  benchmark_output_format: "json"    # json | csv | console
```

## 🎯 Regression Threshold

The default threshold is **5%** for all metrics. This can be adjusted in `benchmark/utils/comparator.ex`:

```elixir
@threshold 5.0  # 5% degradation threshold
```

### How It Works

- **Throughput** (msgs/sec, MB/sec): Degradation if **decreases** >5%
- **Latency** (μs): Degradation if **increases** >5%
- **Memory** (MB): Degradation if **increases** >5%
- **Connections** (conn/sec): Degradation if **decreases** >5%

## 📝 Interpreting Results

### Throughput

```
throughput_msgs_per_sec: 125000
throughput_mb_per_sec: 122.07
```

Number of messages processed per second. Higher = better.

### Latency

```
latency_p50_us: 45.2
latency_p95_us: 120.5
latency_p99_us: 250.3
```

- **p50**: 50% of messages were delivered in ≤45.2μs
- **p95**: 95% of messages were delivered in ≤120.5μs
- **p99**: 99% of messages were delivered in ≤250.3μs

Lower = better. p99 is critical to ensure consistent experience.

### Memory

```
memory_per_connection_mb: 0.125
memory_per_1000_messages_mb: 2.5
```

Memory usage per resource. Lower = better.

### Connections

```
connections_per_sec: 1500
max_stable_connections: 5000
```

- **connections_per_sec**: Connection establishment rate
- **max_stable_connections**: Maximum before failures start

## 🔧 Troubleshooting

### Benchmarks Fail with Timeout

Increase `tcp_recv_timeout` in `config/config.exs`:

```elixir
config :malachimq,
  tcp_recv_timeout: 60_000  # 60 seconds
```

### Inconsistent Results

- Run the suite multiple times and compare results
- Close heavy applications during benchmarks
- Run on dedicated machine for CI

### Comparison Fails

Check if `baseline_reference.json` exists:

```bash
ls -la benchmark/results/baseline_reference.json
```

If it doesn't exist, generate it by running `./benchmark/run_all_baselines.sh` and copying the consolidated result.

## 📚 Usage Examples

### Before Implementing Security Feature

```bash
# 1. Generate current baseline
./benchmark/run_all_baselines.sh

# 2. Copy result as reference
cp benchmark/results/baseline_YYYYMMDD_HHMMSS.json benchmark/results/baseline_reference.json

# 3. Commit baseline
git add benchmark/results/baseline_reference.json
git commit -m "chore: baseline before TLS implementation"

# 4. Implement feature...

# 5. Run benchmarks again
./benchmark/run_all_baselines.sh

# 6. Compare
LATEST=$(ls -t benchmark/results/baseline_*.json | head -1)
mix run benchmark/compare_baselines.exs -- benchmark/results/baseline_reference.json "$LATEST"
```

### Investigate Specific Regression

If CI detects a latency regression:

```bash
# Run only latency benchmark
elixir benchmark/baseline_latency.exs

# Check detailed results
cat benchmark/results/latency_*.json | jq '.results'
```

### Test Different Configurations

```bash
# Test with more partitions
export MALACHIMQ_PARTITION_MULTIPLIER=200
./benchmark/run_all_baselines.sh

# Compare with default baseline
LATEST=$(ls -t benchmark/results/baseline_*.json | head -1)
./benchmark/compare_baselines.exs benchmark/results/baseline_reference.json "$LATEST"
```

## 🎓 Best Practices

1. **Always generate baseline before major changes**
2. **Run in consistent environment** (same machine, similar load)
3. **Check CI before merge** - don't ignore regressions without justification
4. **Document tradeoffs** - if accepting degradation, explain why in PR
5. **Update baseline periodically** - at each major release

## 📊 Critical Metrics

These metrics **must not** degrade >5%:

- ✅ `throughput_msgs_per_sec` (single_producer_consumer)
- ✅ `latency_p99_us` (end_to_end_latency)
- ✅ `memory_per_connection_mb`
- ✅ `connections_per_sec`
- ✅ `token_validation.ops_per_sec`

## 🚨 When Regressions Are Acceptable

- **Explicit tradeoff**: E.g., +10% latency but +50% security
- **Complex feature**: E.g., TLS adds expected overhead of 3-8%
- **Improvement in other metric**: E.g., -5% throughput but -50% memory usage

**Always document in PR and update baseline after merge.**

## 📞 Support

For questions about benchmarks:

1. Check this README
2. Read comments in individual scripts
3. Open issue with `performance` label

---

**Last updated**: 2026-02-03
**MalachiMQ Version**: 0.4.6
