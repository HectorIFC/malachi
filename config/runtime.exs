import Config

# Helper function to parse integers with defaults
parse_int = fn val, default ->
  if val do
    case Integer.parse(val) do
      {int, _} -> int
      :error -> default
    end
  else
    default
  end
end

# Helper function to parse floats with defaults
parse_float = fn val, default ->
  if val do
    case Float.parse(val) do
      {float, _} -> float
      :error -> default
    end
  else
    default
  end
end

# Helper function to parse TLS versions from comma-separated string
parse_tls_versions = fn val, default ->
  if val do
    val
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Enum.map(fn v ->
      try do
        String.to_existing_atom(v)
      rescue
        _ -> nil
      end
    end)
    |> Enum.reject(&is_nil/1)
  else
    default
  end
end

# TLS enforcement: required by default in production
# Can be overridden with MALACHIMQ_CONFIG_ENV for testing/CI environments
actual_env =
  case System.get_env("MALACHIMQ_CONFIG_ENV") do
    "dev" -> :dev
    "test" -> :test
    "prod" -> :prod
    _ -> config_env()
  end

require_tls =
  case {actual_env, System.get_env("MALACHIMQ_REQUIRE_TLS")} do
    {:prod, "false"} -> false
    {:prod, _} -> true
    {_, "true"} -> true
    _ -> false
  end

# TLS enabled: always true in production (unless require_tls is false)
enable_tls =
  case {actual_env, System.get_env("MALACHIMQ_ENABLE_TLS")} do
    {:prod, _} -> require_tls
    {_, "true"} -> true
    _ -> false
  end

config :malachi,
  config_env: config_env(),
  tcp_port: String.to_integer(System.get_env("MALACHIMQ_TCP_PORT") || "4040"),
  dashboard_port: String.to_integer(System.get_env("MALACHIMQ_DASHBOARD_PORT") || "4041"),
  locale: System.get_env("MALACHI_LOCALE") || "en_US",
  partition_multiplier: String.to_integer(System.get_env("MALACHIMQ_PARTITION_MULTIPLIER") || "100"),
  session_timeout_ms: String.to_integer(System.get_env("MALACHIMQ_SESSION_TIMEOUT_MS") || "3600000"),
  session_cleanup_interval_ms: String.to_integer(System.get_env("MALACHIMQ_SESSION_CLEANUP_MS") || "60000"),
  auth_timeout_ms: String.to_integer(System.get_env("MALACHIMQ_AUTH_TIMEOUT_MS") || "10000"),
  tcp_recv_timeout: String.to_integer(System.get_env("MALACHIMQ_TCP_RECV_TIMEOUT") || "30000"),
  tcp_send_timeout: String.to_integer(System.get_env("MALACHIMQ_TCP_SEND_TIMEOUT") || "30000"),
  # Largest request frame the binary protocol accepts (bytes); rejected at the length prefix.
  max_frame_size: parse_int.(System.get_env("MALACHIMQ_MAX_FRAME_SIZE"), 16_777_216),
  gc_interval_ms: String.to_integer(System.get_env("MALACHIMQ_GC_INTERVAL_MS") || "10000"),
  enable_tls: enable_tls,
  require_tls: require_tls,
  tls_certfile: System.get_env("MALACHIMQ_TLS_CERTFILE"),
  tls_keyfile: System.get_env("MALACHIMQ_TLS_KEYFILE"),
  tls_cacertfile: System.get_env("MALACHIMQ_TLS_CACERTFILE"),
  tls_versions: parse_tls_versions.(System.get_env("MALACHIMQ_TLS_VERSIONS"), [:"tlsv1.3", :"tlsv1.2"]),
  tls_verify: System.get_env("MALACHIMQ_TLS_VERIFY") || "verify_none",
  tls_fail_if_no_peer_cert: System.get_env("MALACHIMQ_TLS_FAIL_IF_NO_PEER_CERT") == "true",
  default_delivery_mode: System.get_env("MALACHIMQ_DEFAULT_DELIVERY_MODE") || "at_least_once",
  channel_send_concurrency: String.to_integer(System.get_env("MALACHIMQ_CHANNEL_SEND_CONCURRENCY") || "5000"),
  channel_send_task_timeout_ms: String.to_integer(System.get_env("MALACHIMQ_CHANNEL_SEND_TASK_TIMEOUT_MS") || "5000"),
  shard_count: String.to_integer(System.get_env("MALACHIMQ_SHARD_COUNT") || "1000"),
  mnesia_dir: System.get_env("MALACHIMQ_MNESIA_DIR") || "./data/mnesia",
  # NorthGuard log control plane. Absent MALACHIMQ_LOG_CLUSTER => single-node in-memory metadata
  # (the default). Set it (with the peer node names) for a replicated, HA control plane over `ra`.
  # Node/cluster names come from a trusted operator (deploy config), so String.to_atom is fine here.
  log_cluster:
    (case System.get_env("MALACHIMQ_LOG_CLUSTER") do
       cluster when cluster in [nil, ""] -> nil
       cluster -> String.to_atom(cluster)
     end),
  log_nodes:
    (System.get_env("MALACHIMQ_LOG_NODES") || "")
    |> String.split(",", trim: true)
    |> Enum.map(&String.to_atom(String.trim(&1))),
  # Replicas per segment when clustered (clamped to the node count by the broker). Default 3.
  log_replication_factor: String.to_integer(System.get_env("MALACHIMQ_LOG_REPLICATION_FACTOR") || "3"),
  # Control-plane shards. 1 (default) => a single ra cluster holds all metadata. >1 (with a clustered
  # control plane) shards the metadata across that many vnodes, each its own ra cluster routed by
  # topic, so metadata mutations scale past one Raft group.
  log_vnodes: String.to_integer(System.get_env("MALACHIMQ_LOG_VNODES") || "1"),
  # Replicas per control-plane vnode: each vnode's ra cluster is placed on this many nodes (rendezvous,
  # clamped to the node count) for HA per vnode. Default 3.
  log_vnode_replication_factor: String.to_integer(System.get_env("MALACHIMQ_LOG_VNODE_REPLICATION_FACTOR") || "3"),
  # This node's broker attributes (opaque k/v gossiped via membership; e.g. "rack=a,dc=east"), used
  # by rack-aware placement. Parsed by Malachi.Application.parse_attributes/1. Absent => none.
  log_attributes: System.get_env("MALACHIMQ_LOG_ATTRIBUTES"),
  # The attribute key to spread segment replicas over (e.g. "rack"); absent => no spread (plain HRW).
  log_spread_by: System.get_env("MALACHIMQ_LOG_SPREAD_BY"),
  # Static cluster topology "node1=rack_a,node2=rack_b,..." — the per-node value of :log_spread_by,
  # identical on every node. With both set, control-plane vnode replicas spread across distinct
  # racks/zones (A1, deterministic). Absent => plain HRW placement. Parsed by parse_topology/1.
  log_topology: System.get_env("MALACHIMQ_LOG_TOPOLOGY"),
  # A2 global load balancing: cap how unevenly vnodes spread over nodes (max extra replicas a node may
  # hold beyond the even share). Set => balanced placement (takes precedence over :log_spread_by, which
  # it does not combine with). Absent => plain HRW / rack-spread.
  log_max_skew: parse_int.(System.get_env("MALACHIMQ_LOG_MAX_SKEW"), nil),
  # Log retention. Both unset => segments are kept forever (no RetentionCoordinator started). Set
  # either to expire sealed segments older than an age and/or over a per-range byte budget.
  retention_max_age_ms: parse_int.(System.get_env("MALACHIMQ_RETENTION_MAX_AGE_MS"), nil),
  retention_max_bytes: parse_int.(System.get_env("MALACHIMQ_RETENTION_MAX_BYTES"), nil),
  retention_interval_ms: parse_int.(System.get_env("MALACHIMQ_RETENTION_INTERVAL_MS"), 60_000),
  # Rebalancing lease (only used by a sharded control plane). The k8s-style timer triangle must satisfy
  # lease_duration_ms > lease_renew_deadline_ms > lease_retry_period_ms.
  lease_duration_ms: parse_int.(System.get_env("MALACHIMQ_LEASE_DURATION_MS"), 15_000),
  lease_renew_deadline_ms: parse_int.(System.get_env("MALACHIMQ_LEASE_RENEW_DEADLINE_MS"), 10_000),
  lease_retry_period_ms: parse_int.(System.get_env("MALACHIMQ_LEASE_RETRY_PERIOD_MS"), 2_000),
  # How often each node reconciles itself into the lease cluster (self-join, so a staggered boot converges
  # to a fully-replicated lease). Idempotent once joined.
  lease_reconcile_interval_ms: parse_int.(System.get_env("MALACHIMQ_LEASE_RECONCILE_INTERVAL_MS"), 30_000),
  ra_data_dir: System.get_env("MALACHIMQ_RA_DATA_DIR") || Path.join(System.tmp_dir!(), "malachi_ra")

# Only set rate limiting and connection limiting configs in non-test environments
# Test environment sets these in test.exs with permissive values
if config_env() != :test do
  config :malachi,
    # Rate limiting configuration
    auth_rate_limit: parse_int.(System.get_env("MALACHIMQ_AUTH_RATE_LIMIT"), 10),
    auth_rate_window_ms: parse_int.(System.get_env("MALACHIMQ_AUTH_RATE_WINDOW_MS"), 60_000),
    publish_rate_limit: parse_int.(System.get_env("MALACHIMQ_PUBLISH_RATE_LIMIT"), 1_000),
    publish_rate_window_ms: parse_int.(System.get_env("MALACHIMQ_PUBLISH_RATE_WINDOW_MS"), 1_000),
    subscribe_rate_limit: parse_int.(System.get_env("MALACHIMQ_SUBSCRIBE_RATE_LIMIT"), 100),
    subscribe_rate_window_ms: parse_int.(System.get_env("MALACHIMQ_SUBSCRIBE_RATE_WINDOW_MS"), 60_000),
    rate_limit_cleanup_interval_ms: parse_int.(System.get_env("MALACHIMQ_RATE_LIMIT_CLEANUP_INTERVAL"), 300_000),
    # Connection limits
    max_connections_per_ip: parse_int.(System.get_env("MALACHIMQ_MAX_CONN_PER_IP"), 100),
    max_total_connections: parse_int.(System.get_env("MALACHIMQ_MAX_TOTAL_CONN"), 10_000)
end

config :malachi,
  # Security feature flags (test env enabled, prod enabled via env/default)
  rate_limit_enabled:
    (case System.get_env("MALACHIMQ_RATE_LIMIT_ENABLED") do
       "false" -> false
       "true" -> true
       nil -> if actual_env == :test, do: true, else: actual_env != :test
     end),
  connection_limit_enabled:
    (case System.get_env("MALACHIMQ_CONNECTION_LIMIT_ENABLED") do
       "false" -> false
       "true" -> true
       nil -> if actual_env == :test, do: true, else: actual_env != :test
     end),
  dashboard_auth_enabled:
    (case System.get_env("MALACHIMQ_DASHBOARD_AUTH_ENABLED") do
       "false" -> false
       "true" -> true
       # Auth enabled by default in ALL environments for security
       nil -> true
     end),
  # Dashboard configuration  
  dashboard_require_admin_for_html:
    (case System.get_env("MALACHIMQ_DASHBOARD_REQUIRE_ADMIN") do
       "false" -> false
       "true" -> true
       nil -> true
     end),
  dashboard_auth_rate_limit: parse_int.(System.get_env("MALACHIMQ_DASHBOARD_AUTH_RATE_LIMIT"), 10),
  dashboard_auth_rate_window_ms: parse_int.(System.get_env("MALACHIMQ_DASHBOARD_AUTH_RATE_WINDOW_MS"), 60_000),
  # CORS configuration
  dashboard_cors_enabled: System.get_env("MALACHIMQ_DASHBOARD_CORS_ENABLED") == "true",
  dashboard_cors_origins:
    (case System.get_env("MALACHIMQ_DASHBOARD_CORS_ORIGINS") do
       nil -> ["*"]
       str -> String.split(str, ",") |> Enum.map(&String.trim/1)
     end),
  # Security headers
  dashboard_csp:
    System.get_env("MALACHIMQ_DASHBOARD_CSP") ||
      "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'",
  hsts_enabled:
    (case System.get_env("MALACHIMQ_HSTS_ENABLED") do
       "false" -> false
       "true" -> true
       nil -> true
     end),
  hsts_max_age: parse_int.(System.get_env("MALACHIMQ_HSTS_MAX_AGE"), 31_536_000),
  hsts_include_subdomains:
    (case System.get_env("MALACHIMQ_HSTS_INCLUDE_SUBDOMAINS") do
       "false" -> false
       _ -> true
     end),
  # Audit logging configuration
  audit_log_output:
    (case System.get_env("MALACHIMQ_AUDIT_LOG_OUTPUT") do
       "file" -> :file
       "stdout" -> :stdout
       "both" -> :both
       "ets_only" -> :ets_only
       nil -> if(config_env() == :test, do: :ets_only, else: :both)
       _ -> :both
     end),
  audit_log_file: System.get_env("MALACHIMQ_AUDIT_LOG_FILE") || "/var/log/malachi/audit.log",
  audit_log_max_size_mb: parse_int.(System.get_env("MALACHIMQ_AUDIT_LOG_MAX_SIZE_MB"), 1)

# ============================================================
# User Authentication Configuration
# ============================================================
# BREAKING CHANGE: Production requires explicit password configuration
# Default passwords removed for security

default_users_env = System.get_env("MALACHIMQ_DEFAULT_USERS")
disable_default_users = System.get_env("MALACHIMQ_DISABLE_DEFAULT_USERS") == "true"

default_users =
  if disable_default_users do
    # No default users - manage via API
    []
  else
    if default_users_env do
      # Custom user list via env var
      default_users_env
      |> String.split(";")
      |> Enum.map(fn user_str ->
        [username, password, perms] = String.split(user_str, ":")
        permissions = perms |> String.split(",") |> Enum.map(&String.to_atom/1)
        {username, password, permissions}
      end)
    else
      # Standard user list - passwords from individual env vars
      admin_pass = System.get_env("MALACHIMQ_ADMIN_PASS")
      producer_pass = System.get_env("MALACHIMQ_PRODUCER_PASS")
      consumer_pass = System.get_env("MALACHIMQ_CONSUMER_PASS")
      app_pass = System.get_env("MALACHIMQ_APP_PASS")

      # In production, REQUIRE explicit passwords
      if actual_env == :prod do
        unless admin_pass && producer_pass && consumer_pass && app_pass do
          raise """

          ═══════════════════════════════════════════════════════════════
          SECURITY ERROR: Production deployment requires explicit passwords
          ═══════════════════════════════════════════════════════════════

          Default passwords have been removed for security.
          You MUST configure strong passwords via environment variables:

            export MALACHIMQ_ADMIN_PASS="$(openssl rand -base64 32)"
            export MALACHIMQ_PRODUCER_PASS="$(openssl rand -base64 32)"
            export MALACHIMQ_CONSUMER_PASS="$(openssl rand -base64 32)"
            export MALACHIMQ_APP_PASS="$(openssl rand -base64 32)"

          Alternative: Disable default users and manage via API:

            export MALACHIMQ_DISABLE_DEFAULT_USERS=true

          Generate secure passwords:

            openssl rand -base64 32

          ═══════════════════════════════════════════════════════════════
          """
        end
      end

      # Build user list (use provided passwords or dev defaults)
      [
        {"admin", admin_pass || "admin123", [:admin]},
        {"producer", producer_pass || "producer123", [:produce]},
        {"consumer", consumer_pass || "consumer123", [:consume]},
        {"app", app_pass || "app123", [:produce, :consume]}
      ]
    end
  end

config :malachi,
  default_users: default_users,
  disable_default_users: disable_default_users,

  # Account lockout configuration
  max_auth_attempts: parse_int.(System.get_env("MALACHIMQ_MAX_AUTH_ATTEMPTS"), 5),
  lockout_duration_ms: parse_int.(System.get_env("MALACHIMQ_LOCKOUT_DURATION_MS"), 300_000),
  progressive_lockout: System.get_env("MALACHIMQ_PROGRESSIVE_LOCKOUT") != "false",

  # Session security configuration  
  session_timeout_seconds: parse_int.(System.get_env("MALACHIMQ_SESSION_TIMEOUT_SEC"), 3600),
  session_ip_binding: System.get_env("MALACHIMQ_SESSION_IP_BINDING") != "false",
  session_ua_binding: System.get_env("MALACHIMQ_SESSION_UA_BINDING") == "true",

  # Password requirements
  min_password_length: parse_int.(System.get_env("MALACHIMQ_MIN_PASSWORD_LEN"), 12),
  require_strong_passwords: System.get_env("MALACHIMQ_REQUIRE_STRONG_PASSWORDS") == "true",

  # Trusted proxy ranges (CIDR notation, comma-separated)
  trusted_proxy_ranges:
    (case System.get_env("MALACHIMQ_TRUSTED_PROXY_RANGES") do
       nil ->
         []

       ranges_str ->
         ranges_str
         |> String.split(",")
         |> Enum.map(&String.trim/1)
         |> Enum.reject(&(&1 == ""))
     end)

# ============================================================
# Atom & Memory Monitoring Configuration
# ============================================================
# Prevents atom table exhaustion and monitors system memory.
# Atom table in BEAM is never garbage collected (limit: 1,048,576).

config :malachi,
  # Atom table monitoring
  atom_check_interval_ms: parse_int.(System.get_env("MALACHIMQ_ATOM_CHECK_INTERVAL"), 60_000),
  atom_warning_threshold: parse_float.(System.get_env("MALACHIMQ_ATOM_WARNING_THRESHOLD"), 0.7),
  atom_critical_threshold: parse_float.(System.get_env("MALACHIMQ_ATOM_CRITICAL_THRESHOLD"), 0.9),

  # Memory monitoring
  memory_check_interval_ms: parse_int.(System.get_env("MALACHIMQ_MEMORY_CHECK_INTERVAL"), 30_000),
  gc_threshold_mb: parse_int.(System.get_env("MALACHIMQ_GC_THRESHOLD_MB"), 500),
  auto_gc_enabled: System.get_env("MALACHIMQ_AUTO_GC") != "false"

# ============================================================
# Production Security Warnings
# ============================================================
if actual_env == :prod do
  dashboard_auth_enabled = Application.get_env(:malachi, :dashboard_auth_enabled, true)

  if dashboard_auth_enabled do
    # Check if any users are configured
    has_users = default_users != []

    unless has_users do
      IO.warn("""

      ⚠️  SECURITY WARNING: Dashboard authentication is enabled but no users are configured!

      The dashboard will be inaccessible. Configure users via:

      Environment variable:
        export MALACHIMQ_DEFAULT_USERS="admin:strong_password:admin"

      Or to disable authentication (NOT RECOMMENDED for production):
        export MALACHIMQ_DASHBOARD_AUTH_ENABLED=false

      For more information, see the Security section in the README.
      """)
    end
  end
end
