import Config

# Convenience credentials for local development only. Imported by config/config.exs when
# `config_env() == :dev`, so these never reach a production release (the base config seeds no users, and
# prod requires explicit passwords via env: see config/runtime.exs). Override for a local run with
# MALACHIMQ_DEFAULT_USERS or the per-user MALACHIMQ_*_PASS env vars. Do NOT use these anywhere real.
config :malachi,
  default_users: [
    {"admin", "admin123", [:admin]},
    {"producer", "producer123", [:produce]},
    {"consumer", "consumer123", [:consume]},
    {"app", "app123", [:produce, :consume]}
  ]
