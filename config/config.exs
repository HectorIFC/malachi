import Config

config :malachi,
  tcp_port: 4040,
  dashboard_port: 4041,
  partition_multiplier: 100,
  locale: "en_US",
  # Largest request frame the binary protocol will accept (bytes). A declared length beyond this is
  # rejected at the length prefix, before the body is buffered, bounding per-connection memory.
  max_frame_size: 16_777_216,
  auth_timeout_ms: 10_000,
  session_timeout_ms: 3_600_000,
  session_cleanup_interval_ms: 60_000,
  # No default users are shipped in the base config — credentials must never be hard-coded in source. The
  # dev/test convenience defaults live in config/dev.exs and config/test.exs (never shipped to prod); prod
  # requires explicit passwords via env (see config/runtime.exs). Empty here means "seed nothing".
  default_users: []

# OpenTelemetry: tracing is OFF by default — the sampler drops every span, so `Tracer.with_span` on the
# hot path is a cheap no-op and there is no per-operation cost until an operator opts in. To trace, set
# the sampler to :always_on (or a ratio), add {:opentelemetry_exporter, "~> 1.8"}, and point
# traces_exporter at your collector.
config :opentelemetry, sampler: :always_off, traces_exporter: :none

# Import environment-specific config (test.exs, dev.exs, prod.exs)
# This allows test.exs to override defaults before runtime.exs loads
if File.exists?("config/#{config_env()}.exs") do
  import_config "#{config_env()}.exs"
end
