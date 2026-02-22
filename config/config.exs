import Config

config :malachimq,
  tcp_port: 4040,
  dashboard_port: 4041,
  partition_multiplier: 100,
  locale: "en_US",
  auth_timeout_ms: 10_000,
  session_timeout_ms: 3_600_000,
  session_cleanup_interval_ms: 60_000,
  default_users: [
    {"admin", "admin123", [:admin]},
    {"producer", "producer123", [:produce]},
    {"consumer", "consumer123", [:consume]},
    {"app", "app123", [:produce, :consume]}
  ]

# Import environment-specific config (test.exs, dev.exs, prod.exs)
# This allows test.exs to override defaults before runtime.exs loads
if File.exists?("config/#{config_env()}.exs") do
  import_config "#{config_env()}.exs"
end
