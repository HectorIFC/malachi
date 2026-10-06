import Config

# Convenience credentials for local development only. Imported by config/config.exs when
# `config_env() == :dev`, so these never reach a production release (the base config seeds no users, and
# prod requires explicit passwords via env: see config/runtime.exs). Override for a local run with
# MALACHI_DEFAULT_USERS or the per-user MALACHI_*_PASS env vars. Do NOT use these anywhere real.
config :malachi,
  default_users: [
    {"admin", "admin123", [:admin], nil},
    {"producer", "producer123", [:produce], nil},
    {"consumer", "consumer123", [:consume], nil},
    {"app", "app123", [:produce, :consume], nil}
  ]
