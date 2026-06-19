import Config

# Benchmark-specific configuration
# This file is loaded when running benchmarks

config :malachi,
  # Benchmark parameters
  benchmark_duration_sec: 60,
  benchmark_warmup_sec: 10,
  benchmark_message_sizes: [100, 1024, 10_240, 102_400],
  benchmark_connection_counts: [10, 100, 1000, 5000],
  # json | csv | console
  benchmark_output_format: "json",

  # Mnesia schema directory for benchmarks (keeps artifacts under benchmark/, out of project root)
  mnesia_dir: "benchmark/mnesia_data",

  # Disable excessive logging for cleaner benchmark output
  logger_level: :warning,

  # Optimize for benchmarking
  benchmark_log_every: 1_000_000,
  spawn_concurrency: 10_000,
  send_concurrency: 10_000,
  gc_interval_ms: 30_000,

  # Fast metrics collection
  metrics_snapshot_interval_ms: 5_000,
  metrics_history_seconds: 300
