import Config

# Silence debug/info logs during tests
config :logger, level: :warning

# OpenTelemetry: record every span (always_on) via the synchronous simple processor, so a test can attach
# a pid exporter and assert on ended spans (see the LogApi tracing test). Overrides the always_off default.
config :opentelemetry, sampler: :always_on, span_processor: :simple, traces_exporter: :none

# Isolate the NorthGuard log broker's on-disk data per test run. The default dir is fixed and would
# persist between runs; with in-memory (single-node) metadata resetting each run, a topic name reused
# from a prior run would collide with a leftover segment on disk (Log.ensure_active :already_exists).
config :malachi,
  log_data_dir: Path.join(System.tmp_dir!(), "malachi_log_test_#{System.system_time(:nanosecond)}")

# Test-specific tuning for large-scale channel tests
config :malachi,
  channel_send_concurrency: 5_000,
  channel_send_task_timeout_ms: 5_000,
  shard_count: 1_000,
  # TLS configuration for tests (disabled by default)
  enable_tls: false,
  require_tls: false,
  tls_certfile: nil,
  tls_keyfile: nil,
  tls_cacertfile: nil,
  tls_versions: [:"tlsv1.3", :"tlsv1.2"],
  tls_verify: "verify_none",
  tls_fail_if_no_peer_cert: false,
  hsts_include_subdomains: true,
  # Enable dashboard authentication for security tests
  dashboard_auth_enabled: true,
  # Enable rate limiting for security tests
  rate_limit_enabled: true,
  # Enable connection limiting for connection limiter tests
  connection_limit_enabled: true,
  # Longer snapshot interval to avoid crashes during test setup
  metrics_snapshot_interval_ms: 60_000,
  # Very permissive rate limits for testing (integration tests make many connections)
  auth_rate_limit: 10_000,
  auth_rate_window_ms: 1_000,
  publish_rate_limit: 100_000,
  publish_rate_window_ms: 1_000,
  subscribe_rate_limit: 10_000,
  subscribe_rate_window_ms: 1_000,
  # Very high connection limit for testing
  max_connections_per_ip: 10_000,
  max_total_connections: 100_000,
  # Mnesia: use RAM-only mode in tests for speed and isolation
  mnesia_ram_only: true,
  mnesia_dir: nil
