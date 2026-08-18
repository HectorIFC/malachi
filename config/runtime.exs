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

# Reads a file's contents (e.g. the OIDC public-key PEM) from a path, or nil when the path is unset or the
# file cannot be read. A missing/unreadable key leaves :oidc_public_key nil, and OidcConfig fails closed.
read_file = fn
  nil ->
    nil

  path ->
    case File.read(path) do
      {:ok, contents} -> contents
      {:error, _reason} -> nil
    end
end

# mTLS-identity auth policy (P4): which certificate field names the malachi username. "cn" (default), or
# "san:uri" / "san:dns" / "san:email" to use the first Subject Alternative Name of that kind.
parse_mtls_policy = fn val ->
  case val && String.downcase(val) do
    "san:uri" -> {:san, :uri}
    "san:dns" -> {:san, :dns}
    "san:email" -> {:san, :email}
    _cn_or_absent -> :cn
  end
end

# TLS enforcement: required by default in production
# Can be overridden with MALACHI_CONFIG_ENV for testing/CI environments
actual_env =
  case System.get_env("MALACHI_CONFIG_ENV") do
    "dev" -> :dev
    "test" -> :test
    "prod" -> :prod
    _ -> config_env()
  end

require_tls =
  case {actual_env, System.get_env("MALACHI_REQUIRE_TLS")} do
    {:prod, "false"} -> false
    {:prod, _} -> true
    {_, "true"} -> true
    _ -> false
  end

# TLS enabled: always true in production (unless require_tls is false)
enable_tls =
  case {actual_env, System.get_env("MALACHI_ENABLE_TLS")} do
    {:prod, _} -> require_tls
    {_, "true"} -> true
    _ -> false
  end

# Peer nodes for the log control plane (also the :epmd libcluster host list). Trusted operator input.
log_nodes =
  (System.get_env("MALACHI_LOG_NODES") || "")
  |> String.split(",", trim: true)
  |> Enum.map(&String.to_atom(String.trim(&1)))

# Node-discovery strategy for libcluster (connectivity-only). Absent => no clustering supervisor.
cluster_strategy =
  case System.get_env("MALACHI_CLUSTER_STRATEGY") do
    s when s in [nil, ""] -> nil
    "gossip" -> :gossip
    "kubernetes" -> :kubernetes
    "epmd" -> :epmd
    other -> raise "invalid MALACHI_CLUSTER_STRATEGY #{inspect(other)} (expected gossip, kubernetes or epmd)"
  end

config :malachi,
  config_env: config_env(),
  tcp_port: String.to_integer(System.get_env("MALACHI_TCP_PORT") || "4040"),
  dashboard_port: String.to_integer(System.get_env("MALACHI_DASHBOARD_PORT") || "4041"),
  locale: System.get_env("MALACHI_LOCALE") || "en_US",
  auth_timeout_ms: String.to_integer(System.get_env("MALACHI_AUTH_TIMEOUT_MS") || "10000"),
  tcp_recv_timeout: String.to_integer(System.get_env("MALACHI_TCP_RECV_TIMEOUT") || "30000"),
  tcp_send_timeout: String.to_integer(System.get_env("MALACHI_TCP_SEND_TIMEOUT") || "30000"),
  # Largest request frame the binary protocol accepts (bytes); rejected at the length prefix.
  max_frame_size: parse_int.(System.get_env("MALACHI_MAX_FRAME_SIZE"), 16_777_216),
  enable_tls: enable_tls,
  require_tls: require_tls,
  tls_certfile: System.get_env("MALACHI_TLS_CERTFILE"),
  tls_keyfile: System.get_env("MALACHI_TLS_KEYFILE"),
  tls_cacertfile: System.get_env("MALACHI_TLS_CACERTFILE"),
  tls_versions: parse_tls_versions.(System.get_env("MALACHI_TLS_VERSIONS"), [:"tlsv1.3", :"tlsv1.2"]),
  tls_verify: System.get_env("MALACHI_TLS_VERIFY") || "verify_none",
  tls_fail_if_no_peer_cert: System.get_env("MALACHI_TLS_FAIL_IF_NO_PEER_CERT") == "true",
  # mTLS-identity auth (P4): opt-in, and only honored when the listener verifies peer certs (verify_peer),
  # so an unverified/forged certificate can never authenticate. The policy maps a cert field to a username.
  mtls_auth: System.get_env("MALACHI_MTLS_AUTH") == "true",
  mtls_identity_policy: parse_mtls_policy.(System.get_env("MALACHI_MTLS_POLICY")),
  # OIDC/JWT auth (P4): opt-in. The server validates a signed JWT against the IdP's public key (PEM read from
  # MALACHI_OIDC_PUBLIC_KEY_FILE) and the expected issuer/audience, mapping an identity claim to a user.
  # Bearer tokens should travel over TLS; OidcConfig fails closed if the key/issuer/audience are unset.
  oidc_auth: System.get_env("MALACHI_OIDC_AUTH") == "true",
  oidc_public_key: read_file.(System.get_env("MALACHI_OIDC_PUBLIC_KEY_FILE")),
  oidc_issuer: System.get_env("MALACHI_OIDC_ISSUER"),
  oidc_audience: System.get_env("MALACHI_OIDC_AUDIENCE"),
  oidc_algorithm: System.get_env("MALACHI_OIDC_ALGORITHM") || "RS256",
  oidc_identity_claim: System.get_env("MALACHI_OIDC_IDENTITY_CLAIM") || "sub",
  # Per-topic ACL enforcement (P5). Default (false) is backward-compatible: a global produce/consume
  # permission still grants every topic, and ACLs only add access. Strict mode ignores the global
  # permissions and denies by default. A produce/consume needs an explicit ACL grant (or :admin).
  acl_strict: System.get_env("MALACHI_ACL_STRICT") == "true",
  # NorthGuard log control plane. Absent MALACHI_LOG_CLUSTER => single-node in-memory metadata
  # (the default). Set it (with the peer node names) for a replicated, HA control plane over `ra`.
  # Node/cluster names come from a trusted operator (deploy config), so String.to_atom is fine here.
  log_cluster:
    (case System.get_env("MALACHI_LOG_CLUSTER") do
       cluster when cluster in [nil, ""] -> nil
       cluster -> String.to_atom(cluster)
     end),
  log_nodes: log_nodes,
  # libcluster node discovery (parsed by Malachi.Cluster.Topology.build/1; connectivity-only). The
  # strategy-specific keys are read only for the selected strategy. :epmd reuses log_nodes.
  cluster_topology: %{
    strategy: cluster_strategy,
    gossip_port: parse_int.(System.get_env("MALACHI_CLUSTER_GOSSIP_PORT"), 45_892),
    gossip_secret: System.get_env("MALACHI_CLUSTER_GOSSIP_SECRET"),
    gossip_multicast_addr: System.get_env("MALACHI_CLUSTER_GOSSIP_MULTICAST_ADDR"),
    kubernetes_selector: System.get_env("MALACHI_CLUSTER_KUBERNETES_SELECTOR"),
    kubernetes_node_basename: System.get_env("MALACHI_CLUSTER_KUBERNETES_NODE_BASENAME"),
    kubernetes_namespace: System.get_env("MALACHI_CLUSTER_KUBERNETES_NAMESPACE"),
    kubernetes_mode: String.to_atom(System.get_env("MALACHI_CLUSTER_KUBERNETES_MODE") || "hostname"),
    epmd_hosts: log_nodes
  },
  # Replicas per segment when clustered (clamped to the node count by the broker). Default 3.
  log_replication_factor: String.to_integer(System.get_env("MALACHI_LOG_REPLICATION_FACTOR") || "3"),
  # Group commit (NorthGuard fps-store style): when true, a produce buffers its batch and its client
  # reply is deferred until the next flush, so concurrent producers coalesce into one fsync. Trades a
  # little per-produce latency (~the flush interval) for much higher small-batch throughput. Off by
  # default; only active on a single-node (rf=1) broker, which is where it applies today.
  group_commit: System.get_env("MALACHI_GROUP_COMMIT") == "true",
  # Group commit on the REPLICATED path (rf > 1): fsync coalescing on primary and followers with the
  # ack still waiting for a durable quorum (NorthGuard: fsync on all replicas every 10ms/20k/10MB).
  # Separate knob, default off: it pays when many producers hit the same range on fsync-bound disks,
  # and costs reply latency otherwise (measured: on loads spread thin across many segments it lowers
  # throughput, so it must be an explicit operator choice, not coupled to the rf=1 knob above).
  replication_group_commit: System.get_env("MALACHI_REPLICATION_GROUP_COMMIT") == "true",
  # The replicated path's flush period, decoupled from the rf=1 knob above so tuning a single node
  # never silently retunes every replica's fsync cadence. Default 10ms (the NorthGuard time trigger).
  replication_group_commit_interval_ms: parse_int.(System.get_env("MALACHI_REPLICATION_GROUP_COMMIT_INTERVAL_MS"), 10),
  # Active-segment roll size (bytes): the broker seals the active segment once it reaches this many
  # encoded bytes. Unset => the library default (64MB). Smaller values seal faster, which shortens the
  # replication unit and is what the storage-chaos harness uses to exercise sealed-segment recovery
  # within its window; production setups normally leave this at the default.
  segment_max_bytes: parse_int.(System.get_env("MALACHI_SEGMENT_MAX_BYTES"), nil),
  # Data-plane shards (single-node measurement mode): 1 (default) => a single BrokerServer, unchanged. N > 1
  # runs N independent in-memory broker shards, produce routed by hash(topic), to measure how far parallel
  # brokers lift the networked throughput ceiling. Ignored (forced 1) when the control plane is clustered.
  data_shards: parse_int.(System.get_env("MALACHI_DATA_SHARDS"), 1),
  # Eager-flush threshold (records): flush as soon as this many produce records are parked, so each fsync
  # and each reply stays bounded and a produce never waits long enough to time out, even on a slow disk.
  group_commit_flush_max_records: parse_int.(System.get_env("MALACHI_GROUP_COMMIT_FLUSH_MAX_RECORDS"), 8_000),
  # Backpressure valve (records): past this many parked records the broker sheds new produces with an
  # `:overloaded` error instead of letting them queue until the caller times out and the connection drops.
  group_commit_max_inflight: parse_int.(System.get_env("MALACHI_GROUP_COMMIT_MAX_INFLIGHT"), 200_000),
  # 5ms is the measured sweet spot on a fast SSD: same-or-better throughput than 10ms with roughly half
  # the latency, while coalescing groups stay large enough not to swamp a slower disk with fsyncs. Lower
  # (2ms) wins on latency on fast storage; raise it on true-fsync disks that cap fsync IOPS.
  group_commit_interval_ms: parse_int.(System.get_env("MALACHI_GROUP_COMMIT_INTERVAL_MS"), 5),
  # Control-plane shards. 1 (default) => a single ra cluster holds all metadata. >1 (with a clustered
  # control plane) shards the metadata across that many vnodes, each its own ra cluster routed by
  # topic, so metadata mutations scale past one Raft group.
  log_vnodes: String.to_integer(System.get_env("MALACHI_LOG_VNODES") || "1"),
  # Replicas per control-plane vnode: each vnode's ra cluster is placed on this many nodes (rendezvous,
  # clamped to the node count) for HA per vnode. Default 3.
  log_vnode_replication_factor: String.to_integer(System.get_env("MALACHI_LOG_VNODE_REPLICATION_FACTOR") || "3"),
  # This node's broker attributes (opaque k/v gossiped via membership; e.g. "rack=a,dc=east"), used
  # by rack-aware placement. Parsed by Malachi.Application.parse_attributes/1. Absent => none.
  log_attributes: System.get_env("MALACHI_LOG_ATTRIBUTES"),
  # The attribute key to spread segment replicas over (e.g. "rack"); absent => no spread (plain HRW).
  log_spread_by: System.get_env("MALACHI_LOG_SPREAD_BY"),
  # Static cluster topology "node1=rack_a,node2=rack_b,...". The per-node value of :log_spread_by,
  # identical on every node. With both set, control-plane vnode replicas spread across distinct
  # racks/zones (A1, deterministic). Absent => plain HRW placement. Parsed by parse_topology/1.
  log_topology: System.get_env("MALACHI_LOG_TOPOLOGY"),
  # A2 global load balancing: cap how unevenly vnodes spread over nodes (max extra replicas a node may
  # hold beyond the even share). Set => balanced placement (takes precedence over :log_spread_by, which
  # it does not combine with). Absent => plain HRW / rack-spread.
  log_max_skew: parse_int.(System.get_env("MALACHI_LOG_MAX_SKEW"), nil),
  # Failure-domain hardening: the minimum distinct :log_spread_by values (racks/DCs) a segment's replica
  # set must span. With MALACHI_LOG_PLACEMENT_POLICY=hard, a segment that cannot reach it fails the
  # produce (fail-fast on an under-diversified, non-HA placement); soft (default) places best-effort.
  log_min_domains: parse_int.(System.get_env("MALACHI_LOG_MIN_DOMAINS"), nil),
  log_placement_policy:
    (case System.get_env("MALACHI_LOG_PLACEMENT_POLICY") do
       "hard" -> :hard
       _soft_or_absent -> :soft
     end),
  # Log retention. Both unset => segments are kept forever (no RetentionCoordinator started). Set
  # either to expire sealed segments older than an age and/or over a per-range byte budget.
  retention_max_age_ms: parse_int.(System.get_env("MALACHI_RETENTION_MAX_AGE_MS"), nil),
  retention_max_bytes: parse_int.(System.get_env("MALACHI_RETENTION_MAX_BYTES"), nil),
  retention_interval_ms: parse_int.(System.get_env("MALACHI_RETENTION_INTERVAL_MS"), 60_000),
  # Rebalancing lease (only used by a sharded control plane). The k8s-style timer triangle must satisfy
  # lease_duration_ms > lease_renew_deadline_ms > lease_retry_period_ms.
  lease_duration_ms: parse_int.(System.get_env("MALACHI_LEASE_DURATION_MS"), 15_000),
  lease_renew_deadline_ms: parse_int.(System.get_env("MALACHI_LEASE_RENEW_DEADLINE_MS"), 10_000),
  lease_retry_period_ms: parse_int.(System.get_env("MALACHI_LEASE_RETRY_PERIOD_MS"), 2_000),
  # How often each node reconciles itself into the lease cluster (self-join, so a staggered boot converges
  # to a fully-replicated lease). Idempotent once joined.
  lease_reconcile_interval_ms: parse_int.(System.get_env("MALACHI_LEASE_RECONCILE_INTERVAL_MS"), 30_000),
  # Automatic rebalancing (sharded control plane only). Off by default: the operator drives the
  # RebalanceCoordinator. When on, the lease holder commits the plan once it stays the same for
  # `stabilization` reconciles (interval apart), so a transient membership flap does not move vnodes.
  # Graceful-shutdown drain window: after quiescing the acceptor (no new connections), wait this long for
  # in-flight requests to finish before closing connections. Keep k8s terminationGracePeriodSeconds above it.
  shutdown_grace_ms: parse_int.(System.get_env("MALACHI_SHUTDOWN_GRACE_MS"), 5_000),
  auto_rebalance: System.get_env("MALACHI_AUTO_REBALANCE") == "true",
  auto_rebalance_interval_ms: parse_int.(System.get_env("MALACHI_AUTO_REBALANCE_INTERVAL_MS"), 30_000),
  auto_rebalance_stabilization: parse_int.(System.get_env("MALACHI_AUTO_REBALANCE_STABILIZATION"), 3)

# Everything in this block is owned by config/test.exs when running tests. runtime.exs is evaluated after
# the environment file, so setting any of it unconditionally would silently overwrite the test values,
# which for the rate limits are deliberately permissive (integration tests make many connections).
if config_env() != :test do
  config :malachi,
    # Rate limiting configuration
    auth_rate_limit: parse_int.(System.get_env("MALACHI_AUTH_RATE_LIMIT"), 10),
    auth_rate_window_ms: parse_int.(System.get_env("MALACHI_AUTH_RATE_WINDOW_MS"), 60_000),
    publish_rate_limit: parse_int.(System.get_env("MALACHI_PUBLISH_RATE_LIMIT"), 1_000),
    publish_rate_window_ms: parse_int.(System.get_env("MALACHI_PUBLISH_RATE_WINDOW_MS"), 1_000),
    subscribe_rate_limit: parse_int.(System.get_env("MALACHI_SUBSCRIBE_RATE_LIMIT"), 100),
    subscribe_rate_window_ms: parse_int.(System.get_env("MALACHI_SUBSCRIBE_RATE_WINDOW_MS"), 60_000),
    rate_limit_cleanup_interval_ms: parse_int.(System.get_env("MALACHI_RATE_LIMIT_CLEANUP_INTERVAL"), 300_000),
    # Connection limits
    max_connections_per_ip: parse_int.(System.get_env("MALACHI_MAX_CONN_PER_IP"), 100),
    max_total_connections: parse_int.(System.get_env("MALACHI_MAX_TOTAL_CONN"), 10_000)

  # On-disk data directories, set only when the operator supplies one. Malachi.Config.data_dir/3 trims,
  # treats blank as absent, and rejects a relative path in production (it would resolve against the process
  # working directory and lose durable segments to ephemeral storage). The rules live there, with tests,
  # because this file is skipped under config_env() == :test and so cannot be exercised by the suite. With
  # nothing set, `Malachi.Application` supplies the defaults, so dev is unchanged and test.exs keeps the
  # per-run paths that stop a leftover segment from colliding with a reused topic (`:already_exists`).
  data_dir = fn var -> Malachi.Config.data_dir(var, System.get_env(var), actual_env) end

  log_data_dir = data_dir.("MALACHI_LOG_DATA_DIR")
  ra_data_dir = data_dir.("MALACHI_RA_DATA_DIR")

  if log_data_dir, do: config(:malachi, log_data_dir: log_data_dir)
  if ra_data_dir, do: config(:malachi, ra_data_dir: ra_data_dir)
end

config :malachi,
  # Security feature flags (test env enabled, prod enabled via env/default)
  rate_limit_enabled:
    (case System.get_env("MALACHI_RATE_LIMIT_ENABLED") do
       "false" -> false
       "true" -> true
       nil -> if actual_env == :test, do: true, else: actual_env != :test
     end),
  connection_limit_enabled:
    (case System.get_env("MALACHI_CONNECTION_LIMIT_ENABLED") do
       "false" -> false
       "true" -> true
       nil -> if actual_env == :test, do: true, else: actual_env != :test
     end),
  dashboard_auth_enabled:
    (case System.get_env("MALACHI_DASHBOARD_AUTH_ENABLED") do
       "false" -> false
       "true" -> true
       # Auth enabled by default in ALL environments for security
       nil -> true
     end),
  # Dashboard configuration
  dashboard_require_admin_for_html:
    (case System.get_env("MALACHI_DASHBOARD_REQUIRE_ADMIN") do
       "false" -> false
       "true" -> true
       nil -> true
     end),
  dashboard_auth_rate_limit: parse_int.(System.get_env("MALACHI_DASHBOARD_AUTH_RATE_LIMIT"), 10),
  dashboard_auth_rate_window_ms: parse_int.(System.get_env("MALACHI_DASHBOARD_AUTH_RATE_WINDOW_MS"), 60_000),
  # CORS configuration
  dashboard_cors_enabled: System.get_env("MALACHI_DASHBOARD_CORS_ENABLED") == "true",
  dashboard_cors_origins:
    (case System.get_env("MALACHI_DASHBOARD_CORS_ORIGINS") do
       nil -> ["*"]
       str -> String.split(str, ",") |> Enum.map(&String.trim/1)
     end),
  # Security headers
  dashboard_csp:
    System.get_env("MALACHI_DASHBOARD_CSP") ||
      "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'",
  hsts_enabled:
    (case System.get_env("MALACHI_HSTS_ENABLED") do
       "false" -> false
       "true" -> true
       nil -> true
     end),
  hsts_max_age: parse_int.(System.get_env("MALACHI_HSTS_MAX_AGE"), 31_536_000),
  hsts_include_subdomains:
    (case System.get_env("MALACHI_HSTS_INCLUDE_SUBDOMAINS") do
       "false" -> false
       _ -> true
     end),
  # Audit logging configuration
  audit_log_output:
    (case System.get_env("MALACHI_AUDIT_LOG_OUTPUT") do
       "file" -> :file
       "stdout" -> :stdout
       "both" -> :both
       "ets_only" -> :ets_only
       nil -> if(config_env() == :test, do: :ets_only, else: :both)
       _ -> :both
     end),
  audit_log_file: System.get_env("MALACHI_AUDIT_LOG_FILE") || "/var/log/malachi/audit.log",
  audit_log_max_size_mb: parse_int.(System.get_env("MALACHI_AUDIT_LOG_MAX_SIZE_MB"), 1)

# ============================================================
# User Authentication Configuration
# ============================================================
# BREAKING CHANGE: Production requires explicit password configuration
# Default passwords removed for security

default_users_env = System.get_env("MALACHI_DEFAULT_USERS")
disable_default_users = System.get_env("MALACHI_DISABLE_DEFAULT_USERS") == "true"

# Per-user passwords from individual env vars (work in any environment, including prod).
admin_pass = System.get_env("MALACHI_ADMIN_PASS")
producer_pass = System.get_env("MALACHI_PRODUCER_PASS")
consumer_pass = System.get_env("MALACHI_CONSUMER_PASS")
app_pass = System.get_env("MALACHI_APP_PASS")
# The standard users whose password is set explicitly (a nil password is skipped). Producer/consumer/app
# are optional; the admin is *generated* (see `generate_admin` below) when it has no explicit password.
explicit_standard_users =
  [
    {"admin", admin_pass, [:admin]},
    {"producer", producer_pass, [:produce]},
    {"consumer", consumer_pass, [:consume]},
    {"app", app_pass, [:produce, :consume]}
  ]
  |> Enum.filter(fn {_name, pass, _perms} -> pass != nil end)

{default_users, generate_admin} =
  cond do
    disable_default_users ->
      # No default users, manage via the API.
      {[], false}

    default_users_env ->
      # Custom user list via env var: "user:pass:perm,perm;user2:..."
      users =
        default_users_env
        |> String.split(";")
        |> Enum.map(fn user_str ->
          [username, password, perms] = String.split(user_str, ":")
          permissions = perms |> String.split(",") |> Enum.map(&String.to_atom/1)
          {username, password, permissions}
        end)

      {users, false}

    actual_env in [:dev, :test] ->
      # Keep the environment-specific convenience credentials from config/dev.exs or config/test.exs (never
      # in the base/prod path). Override with MALACHI_DEFAULT_USERS or the per-user *_PASS env vars.
      {Application.get_env(:malachi, :default_users, []), false}

    true ->
      # Any other environment (production included): seed the standard users whose password is set, and
      # generate a random admin on first boot when none is configured, Malachi.Auth logs it once, and the
      # replicated store dedups so exactly one node's password wins. No weak fallback and no hard failure;
      # MALACHI_DISABLE_DEFAULT_USERS opts out of default users entirely.
      {explicit_standard_users, admin_pass == nil}
  end

config :malachi,
  default_users: default_users,
  generate_admin: generate_admin,
  disable_default_users: disable_default_users,

  # Account lockout configuration
  max_auth_attempts: parse_int.(System.get_env("MALACHI_MAX_AUTH_ATTEMPTS"), 5),
  lockout_duration_ms: parse_int.(System.get_env("MALACHI_LOCKOUT_DURATION_MS"), 300_000),
  progressive_lockout: System.get_env("MALACHI_PROGRESSIVE_LOCKOUT") != "false",

  # Session security configuration. The TTL is seconds, not milliseconds: a `session_timeout_ms` used to
  # sit alongside this one and was the only variant the README documented, but nothing ever read it, so
  # setting it silently left the TTL at the default. It is gone; this is the knob.
  session_timeout_seconds: parse_int.(System.get_env("MALACHI_SESSION_TIMEOUT_SEC"), 3600),
  session_ip_binding: System.get_env("MALACHI_SESSION_IP_BINDING") != "false",
  session_ua_binding: System.get_env("MALACHI_SESSION_UA_BINDING") == "true",

  # Password requirements
  min_password_length: parse_int.(System.get_env("MALACHI_MIN_PASSWORD_LEN"), 12),
  require_strong_passwords: System.get_env("MALACHI_REQUIRE_STRONG_PASSWORDS") == "true",

  # Trusted proxy ranges (CIDR notation, comma-separated)
  trusted_proxy_ranges:
    (case System.get_env("MALACHI_TRUSTED_PROXY_RANGES") do
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
  atom_check_interval_ms: parse_int.(System.get_env("MALACHI_ATOM_CHECK_INTERVAL"), 60_000),
  atom_warning_threshold: parse_float.(System.get_env("MALACHI_ATOM_WARNING_THRESHOLD"), 0.7),
  atom_critical_threshold: parse_float.(System.get_env("MALACHI_ATOM_CRITICAL_THRESHOLD"), 0.9),

  # Memory monitoring
  memory_check_interval_ms: parse_int.(System.get_env("MALACHI_MEMORY_CHECK_INTERVAL"), 30_000),
  gc_threshold_mb: parse_int.(System.get_env("MALACHI_GC_THRESHOLD_MB"), 500),
  auto_gc_enabled: System.get_env("MALACHI_AUTO_GC") != "false"

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
        export MALACHI_DEFAULT_USERS="admin:strong_password:admin"

      Or to disable authentication (NOT RECOMMENDED for production):
        export MALACHI_DASHBOARD_AUTH_ENABLED=false

      For more information, see the Security section in the README.
      """)
    end
  end
end
