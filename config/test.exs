import Config

# Silence debug/info logs during tests
config :logger, level: :warning

# Argon2 is deliberately slow and memory-hungry, which is the point in production but pure drag in tests:
# at the library defaults (t_cost 3, m_cost 16 = 64 MiB, parallelism 4) a single verify costs ~42 ms, and
# the 100 concurrent verifies in test/attack_simulation_test.exs peak at 6.4 GiB and take ~4.2 s, close
# enough to that test's 10 s budget that suite load pushed it over roughly one run in three.
#
# The cost is a deployment parameter, not application logic: verifying a correct password succeeds and an
# incorrect one fails identically at any cost, and every timing assertion in the suite is an upper bound,
# so cheaper hashing can only help. Measured here: 100 concurrent verifies 4249 ms -> 34 ms, whole suite
# ~55 s -> ~38 s. Test-only, production keeps the library defaults (this file is never loaded there).
config :argon2_elixir, t_cost: 1, m_cost: 8, parallelism: 1

# OpenTelemetry: record every span (always_on) via the synchronous simple processor, so a test can attach
# a pid exporter and assert on ended spans (see the LogApi tracing test). Overrides the always_off default.
config :opentelemetry, sampler: :always_on, span_processor: :simple, traces_exporter: :none

# Isolate the NorthGuard log broker's on-disk data per test run, and the ra data with it. The default dirs
# are fixed and would persist between runs: a run's control plane (a one-member ra cluster named after the
# node, whose name changes every run) would then find the previous run's segments, which it does not know,
# and refuse to start (Malachi.Storage.DataDirGuard). The OS pid keeps two runs started in the same
# nanosecond on one host (two worktrees) apart.
config :malachi,
  log_data_dir: Path.join(System.tmp_dir!(), "malachi_log_test_#{System.pid()}_#{System.system_time(:nanosecond)}"),
  # The app starts ra unconditionally (the control plane and the replicated user store).
  ra_data_dir: Path.join(System.tmp_dir!(), "malachi_ra_test_#{System.pid()}_#{System.system_time(:nanosecond)}"),
  # Deterministic credentials the test suite authenticates with. Test-only, never shipped to prod (the base
  # config seeds nothing, and prod requires explicit passwords via env). Do NOT copy these into any real env.
  default_users: [
    {"admin", "admin123", [:admin], nil},
    {"producer", "producer123", [:produce], nil},
    {"consumer", "consumer123", [:consume], nil},
    {"app", "app123", [:produce, :consume], nil}
  ]

config :malachi,
  # Every listener binds a port the operating system picks, so two `mix test` runs on one host (two
  # worktrees) never fight for 4040/4041/4042. Tests ask the listener which port it got
  # (`Malachi.TCPAcceptorPool.port/0`, `Malachi.Dashboard.port/0`, `Malachi.Console.Endpoint.port/0`),
  # never the config, which stays 0.
  tcp_port: 0,
  dashboard_port: 0,
  console_port: 0,
  # The application's console serves no bundle in tests, whatever a developer built into
  # priv/static/console: tests that need one build a fixture and start an endpoint over it.
  console_static_dir: "/nonexistent/malachi-console-bundle",
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
  max_total_connections: 100_000
