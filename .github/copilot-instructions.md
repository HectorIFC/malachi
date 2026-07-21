# Malachi - AI Coding Agent Instructions

## Project Overview

Malachi is a High-performance message system. It provides persistent queues with ETS-backed storage, pub/sub channels with best-effort delivery, TCP/TLS server for client connections, web dashboard with SSE streaming, session-based authentication with Argon2 hashing, rate limiting, connection limiting, audit logging, input validation, backpressure control, and automatic queue partitioning across CPU cores.

**Runtime:** Elixir 1.19+ / OTP 28+ on the BEAM VM.

## Architecture

### Supervision Tree (`application.ex`)

The application starts 24 child processes in a `one_for_one` supervisor:

```
Malachi.Supervisor (one_for_one)
├── QueueRegistry (partitioned by schedulers)
├── ChannelRegistry (unique keys)
├── QueueSupervisor (DynamicSupervisor, max 100k)
├── ChannelSupervisor (DynamicSupervisor, max 100k)
├── TaskSupervisor (max 200k children for broadcasts)
├── PartitionManager
├── QueueConfig
├── Metrics
├── AuditLog
├── AtomMonitor
├── MemoryMonitor
├── Auth.LockoutManager
├── Validator
├── RateLimiter
├── ConnectionLimiter
├── Auth user store (ra-replicated cluster: UserServer/UserMachine)
├── Auth
├── AckManager
├── ConnectionRegistry
├── TCPAcceptorPool → TCPAcceptors (one per core)
└── Dashboard (HTTP server)
```

### Core Components (`lib/malachi/`)

#### Queue & Messaging
- **`queue.ex`** - Per-partition queue GenServer using ETS for message storage and dispatch
- **`channel.ex`** - Pub/Sub channels with fire-and-forget delivery; broadcasts via Task.Supervisor
- **`partition_manager.ex`** - Distributes queues across CPU cores using `erlang:phash2`; scales with `partition_multiplier` (default 100x)
- **`queue_config.ex`** - Runtime queue configuration: delivery mode, max consumers, buffer size, overflow strategy
- **`consumer.ex`** - Lightweight GenServer with aggressive hibernation and off-heap message queue
- **`ack_manager.ex`** - Tracks pending acknowledgments per queue; requeues on timeout/nack
- **`backpressure.ex`** - Producer backpressure handling: blocks producers when buffer exceeds threshold

#### Networking & Protocol
- **`tcp_acceptor_pool.ex`** - Spawns one acceptor per CPU core
- **`tcp_acceptor.ex`** - GenServer accepting TCP/TLS connections, hands off to protocol handler
- **`tcp_protocol.ex`** - Newline-delimited JSON protocol parser (~37KB); handles all client actions
- **`socket_helper.ex`** - Unified socket utilities for raw TCP and TLS transports
- **`connection_registry.ex`** - Tracks active connections; provides `close_all/0` for graceful shutdown

#### Security & Authentication
- **`auth.ex`** - User authentication with Argon2 hashing; delegates persistence to `UserStore`
- **`auth/user_store.ex`** - Facade over the ra-replicated user cluster (`UserServer`/`UserMachine`/`UserRegistry`); writes go through the Raft log, reads from the local replica
- **`auth/session_manager.ex`** - Session lifecycle: token generation, IP binding, user-agent binding, expiration, cleanup
- **`auth/lockout_manager.ex`** - Account lockout after failed attempts; progressive lockout with configurable duration
- **`auth/config_validator.ex`** - Validates auth config at startup; prevents insecure production deployments
- **`audit_log.ex`** - JSON security event logging to file/stdout/ETS; auto-rotation by size
- **`rate_limiter.ex`** - Token bucket rate limiting per IP per action (auth, publish, subscribe); configurable windows
- **`connection_limiter.ex`** - Max connections per IP and total; prevents resource exhaustion
- **`validator.ex`** - Input validation with ETS cache; XSS prevention, injection protection, name/payload/header constraints

#### Monitoring & Resource Management
- **`metrics.ex`** - ETS-based real-time counters with `decentralized_counters: true`; atomic increments
- **`memory_monitor.ex`** - Periodic memory checks; triggers GC when threshold exceeded
- **`atom_monitor.ex`** - Monitors atom table usage (BEAM atoms are never GC'd); warns at 70%, critical at 90%
- **`backpressure.ex`** - Buffer utilization tracking; blocks producers at configurable threshold

#### Web Dashboard
- **`dashboard.ex`** - HTTP server (~48KB) serving HTML UI, JSON metrics, SSE streaming; requires auth
- **`dashboard/security_headers.ex`** - CSP, HSTS, X-Frame-Options, X-Content-Type-Options headers

#### Utilities
- **`i18n.ex`** - Internationalization with pattern matching; supports `en_US` and `pt_BR`
- **`benchmark.ex`** - Built-in benchmarking: spawn consumers, send messages, system info

### Data Flow

1. Client connects via TCP/TLS → `TCPAcceptorPool` → `TCPAcceptor`
2. `ConnectionLimiter.check_connection/1` → enforce per-IP and total limits
3. Client sends auth request → `RateLimiter.check_rate/3` → `Auth.authenticate/3`
4. `LockoutManager.check_lockout/1` → prevent brute force
5. On success: `SessionManager` creates token bound to IP
6. Producer sends publish → `Validator.validate_*/1` → `RateLimiter.check_rate/3`
7. `PartitionManager.get_partition/1` hashes queue name → routes to partition
8. `Queue` checks backpressure → dispatches to waiting consumer or buffers in ETS
9. `Consumer` processes message → sends ack/nack to `AckManager`
10. All security events → `AuditLog.log_event/5`

### TCP/JSON Protocol

All messages are newline-delimited JSON. Responses use `"s"` for status (`"ok"` or `"err"`).

**Authentication (required first):**
```json
{"action":"auth","username":"producer","password":"producer123"}
→ {"s":"ok","token":"<session_token>"}
```

**Publish message (requires `:produce`):**
```json
{"action":"publish","queue_name":"orders","payload":"...","headers":{"priority":1}}
→ {"s":"ok"}
```

**Shorthand publish (action optional):**
```json
{"queue_name":"orders","payload":"..."}
→ {"s":"ok"}
```

**Subscribe to queue (requires `:consume`):**
```json
{"action":"subscribe","queue_name":"orders"}
→ {"s":"ok"}
← {"queue_message":{"id":1,"payload":"...","headers":{},"timestamp":123456}}
```

**Acknowledge / Negative acknowledge:**
```json
{"action":"ack","message_id":"123","queue_name":"orders"}
→ {"s":"ok"}

{"action":"nack","message_id":"123","queue_name":"orders"}
→ {"s":"ok"}
```

**Queue management (requires `:admin`):**
```json
{"action":"create_queue","queue_name":"orders","delivery_mode":"at_least_once","max_consumers":10,"buffer_size":1000,"overflow_strategy":"drop_old"}
→ {"s":"ok","config":{...}}

{"action":"delete_queue","queue_name":"orders"}
→ {"s":"ok"}

{"action":"list_queues"}
→ {"s":"ok","queues":[...]}

{"action":"get_queue_info","queue_name":"orders"}
→ {"s":"ok","info":{"consumers":5,"buffered":100,...}}
```

**Channel pub/sub (best-effort, no persistence):**
```json
{"action":"channel_publish","channel_name":"news","payload":"...","headers":{}}
→ {"s":"ok"}

{"action":"channel_subscribe","channel_name":"news"}
→ {"s":"ok"}
← {"channel_message":{"payload":"...","headers":{},"timestamp":123456,"channel":"news"}}

{"action":"channel_unsubscribe","channel_name":"news"}
→ {"s":"ok"}
```

**Error codes:**
```
invalid_credentials, permission_denied, auth_required, invalid_request,
queue_not_found, invalid_queue_name_invalid_characters, invalid_queue_name_too_long,
invalid_queue_name_reserved, payload_too_large, invalid_headers_too_many,
invalid_headers_invalid_type, rate_limit_exceeded (retry_after_ms: N),
connection_limit_exceeded, auth_locked_out (retry_after_ms: N)
```

## Developer Commands

```bash
# Development
mix deps.get                    # Install dependencies
mix compile                     # Compile
mix run --no-halt               # Run locally (ports 4040/4041)
mix test                        # Run tests
mix format                      # Format code
mix credo --strict              # Static analysis
mix dialyzer                    # Type checking
mix deps.audit                  # Security audit

# Docker
make docker-build               # Build single-arch Docker image
make docker-buildx              # Build multi-arch (AMD64 + ARM64)
make docker-run                 # Run container (ports 4040, 4041)
make compose-up                 # Docker Compose
make compose-logs               # Follow logs
make clean                      # Clean build artifacts

# Release
MIX_ENV=prod mix deps.get && MIX_ENV=prod mix release

# TLS Development
./scripts/generate-dev-certs.sh # Generate self-signed certs in priv/cert/

# Benchmarks
make benchmark                  # Run all baselines
mix run benchmark/baselines/baseline_throughput.exs   # Single benchmark
```

## Code Conventions

### GenServer Patterns
- Use `{:continue, :action}` for post-init work (see `consumer.ex`)
- Return `:hibernate` from callbacks for long-idle processes
- Use `Process.flag(:message_queue_data, :off_heap)` for consumers
- DynamicSupervisor for dynamic workers (queues, channels)
- Registry for process discovery with partitioned keys

### ETS Usage
- Tables are `:public` with `read_concurrency: true, write_concurrency: true`
- Use `decentralized_counters: true` for hot tables (metrics)
- Prefer `:ets.update_counter/4` with default tuple for atomic increments
- Named tables for shared state (users, sessions, metrics)
- Anonymous tables for per-queue message storage

### Error Handling
- Return tuples: `{:ok, result}` or `{:error, atom_reason}`
- Consumer callbacks returning `:error` or `{:error, _}` trigger nack with requeue
- Guard clauses for type safety
- `with` expressions for chaining validations

### Naming Conventions
- **Modules**: `Malachi.PascalCase` (e.g., `Malachi.TCPProtocol`)
- **Functions/variables**: `snake_case`
- **Module attributes**: `@snake_case` (e.g., `@max_payload_size`)
- **Queue process names**: `{queue_name, partition}` tuples via Registry
- **ETS tables**: `String.to_atom("malachi_#{name}_#{partition}")` for dynamic tables

### Internationalization
- Use `Malachi.I18n.t/2` for **all** log messages
- Translations in `i18n.ex` with both `"pt_BR"` and `"en_US"` entries
- Template interpolation: `I18n.t(:key, binding: "value")`

### Code Quality
- Line length: **120 characters** (`.formatter.exs`)
- Max cyclomatic complexity: **12** (`.credo.exs`)
- Max function arity: **8**
- Max nesting: **3 levels**
- Test coverage: **80% minimum** (ExCoveralls)
- `mix format --check-formatted` in CI

## Configuration

Configuration flows: `config/config.exs` → `config/runtime.exs` (env vars) → `config/test.exs` (test overrides).

### Network & Protocol
| Variable | Default | Description |
|----------|---------|-------------|
| `MALACHI_TCP_PORT` | `4040` | TCP server port |
| `MALACHI_DASHBOARD_PORT` | `4041` | Dashboard HTTP port |
| `MALACHI_TCP_RECV_TIMEOUT` | `30000` | Receive timeout (ms) |
| `MALACHI_TCP_SEND_TIMEOUT` | `30000` | Send timeout (ms) |
| `MALACHI_PARTITION_MULTIPLIER` | `100` | Partitions per CPU core |
| `MALACHI_SHARD_COUNT` | `1000` | Number of shards |

### TLS/SSL
| Variable | Default | Description |
|----------|---------|-------------|
| `MALACHI_ENABLE_TLS` | `false` | Enable TLS encryption |
| `MALACHI_TLS_CERTFILE` | - | Path to TLS certificate |
| `MALACHI_TLS_KEYFILE` | - | Path to TLS private key |
| `MALACHI_TLS_CACERTFILE` | - | Path to CA certificate (mutual TLS) |

### Authentication & Sessions
| Variable | Default | Description |
|----------|---------|-------------|
| `MALACHI_ADMIN_PASS` | `admin123` (dev) | Admin password (**required** in prod) |
| `MALACHI_PRODUCER_PASS` | `producer123` (dev) | Producer password (**required** in prod) |
| `MALACHI_CONSUMER_PASS` | `consumer123` (dev) | Consumer password (**required** in prod) |
| `MALACHI_APP_PASS` | `app123` (dev) | App password (**required** in prod) |
| `MALACHI_DEFAULT_USERS` | - | Custom users: `user:pass:perm1,perm2;...` |
| `MALACHI_DISABLE_DEFAULT_USERS` | `false` | Disable all default users |
| `MALACHI_SESSION_TIMEOUT_SEC` | `3600` | Session token TTL (1 hour) |
| `MALACHI_AUTH_TIMEOUT_MS` | `10000` | Auth request timeout |
| `MALACHI_SESSION_IP_BINDING` | `true` | Bind sessions to client IP |
| `MALACHI_SESSION_UA_BINDING` | `false` | Bind sessions to user-agent |
| `MALACHI_MIN_PASSWORD_LEN` | `12` | Minimum password length |
| `MALACHI_REQUIRE_STRONG_PASSWORDS` | `false` | Enforce strong password policy |

### Account Lockout
| Variable | Default | Description |
|----------|---------|-------------|
| `MALACHI_MAX_AUTH_ATTEMPTS` | `5` | Failed attempts before lockout |
| `MALACHI_LOCKOUT_DURATION_MS` | `300000` | Lockout duration (5 min) |
| `MALACHI_PROGRESSIVE_LOCKOUT` | `true` | Increase lockout on repeated failures |

### Rate Limiting
| Variable | Default | Description |
|----------|---------|-------------|
| `MALACHI_RATE_LIMIT_ENABLED` | `true` (prod) | Enable rate limiting |
| `MALACHI_AUTH_RATE_LIMIT` | `10` | Auth attempts per window |
| `MALACHI_AUTH_RATE_WINDOW_MS` | `60000` | Auth rate window (1 min) |
| `MALACHI_PUBLISH_RATE_LIMIT` | `1000` | Publishes per window |
| `MALACHI_PUBLISH_RATE_WINDOW_MS` | `1000` | Publish rate window (1 sec) |
| `MALACHI_SUBSCRIBE_RATE_LIMIT` | `100` | Subscribes per window |
| `MALACHI_SUBSCRIBE_RATE_WINDOW_MS` | `60000` | Subscribe rate window (1 min) |

### Connection Limiting
| Variable | Default | Description |
|----------|---------|-------------|
| `MALACHI_CONNECTION_LIMIT_ENABLED` | `true` (prod) | Enable connection limiting |
| `MALACHI_MAX_CONN_PER_IP` | `100` | Max connections per IP |
| `MALACHI_MAX_TOTAL_CONN` | `10000` | Max total connections |

### Queue & Message Configuration
| Variable | Default | Description |
|----------|---------|-------------|
| `MALACHI_DEFAULT_DELIVERY_MODE` | `at_least_once` | Default delivery mode |
| `MALACHI_MAX_MESSAGE_SIZE` | `1048576` | Max message size (1MB) |
| `MALACHI_MAX_BUFFER_SIZE` | `10000` | Max messages per queue buffer |
| `MALACHI_OVERFLOW_BEHAVIOR` | `drop_newest` | Overflow: `drop_newest`, `drop_oldest`, `reject`, `block` |
| `MALACHI_BACKPRESSURE_THRESHOLD` | `0.8` | Buffer % to trigger backpressure |
| `MALACHI_BLOCK_TIMEOUT_MS` | `5000` | Timeout for `block` overflow strategy |
| `MALACHI_MAX_BLOCKED_PRODUCERS` | `1000` | Max blocked producers per queue |
| `MALACHI_CHANNEL_SEND_CONCURRENCY` | `5000` | Concurrent channel broadcast tasks |

### Dashboard
| Variable | Default | Description |
|----------|---------|-------------|
| `MALACHI_DASHBOARD_AUTH_ENABLED` | `true` | Require auth for dashboard |
| `MALACHI_DASHBOARD_REQUIRE_ADMIN` | `true` | Require `:admin` permission |
| `MALACHI_DASHBOARD_AUTH_RATE_LIMIT` | `10` | Dashboard login rate limit |
| `MALACHI_DASHBOARD_CORS_ENABLED` | `false` | Enable CORS |
| `MALACHI_DASHBOARD_CORS_ORIGINS` | `["*"]` | Allowed CORS origins (comma-separated) |
| `MALACHI_DASHBOARD_CSP` | (strict policy) | Custom Content-Security-Policy |
| `MALACHI_HSTS_ENABLED` | `true` | Enable HSTS header |
| `MALACHI_HSTS_MAX_AGE` | `31536000` | HSTS max-age (1 year) |

### Audit Logging
| Variable | Default | Description |
|----------|---------|-------------|
| `MALACHI_AUDIT_LOG_OUTPUT` | `both` (prod) | Output: `file`, `stdout`, `both`, `ets_only` |
| `MALACHI_AUDIT_LOG_FILE` | `/var/log/malachi/audit.log` | Log file path |
| `MALACHI_AUDIT_LOG_MAX_SIZE_MB` | `1` | Max file size before rotation |

### Resource Monitoring
| Variable | Default | Description |
|----------|---------|-------------|
| `MALACHI_ATOM_CHECK_INTERVAL` | `60000` | Atom table check interval (ms) |
| `MALACHI_ATOM_WARNING_THRESHOLD` | `0.7` | Atom table warning at 70% |
| `MALACHI_ATOM_CRITICAL_THRESHOLD` | `0.9` | Atom table critical at 90% |
| `MALACHI_MEMORY_CHECK_INTERVAL` | `30000` | Memory check interval (ms) |
| `MALACHI_GC_THRESHOLD_MB` | `500` | Memory threshold for GC trigger |
| `MALACHI_AUTO_GC` | `true` | Enable automatic garbage collection |
| `MALACHI_MAX_DYNAMIC_QUEUES` | `10000` | Max dynamically created queues |
| `MALACHI_MAX_DYNAMIC_CHANNELS` | `1000` | Max dynamically created channels |

### Internationalization
| Variable | Default | Description |
|----------|---------|-------------|
| `MALACHI_LOCALE` | `en_US` | Locale: `en_US` or `pt_BR` |

## Testing

Tests use ExUnit with 80% coverage threshold (ExCoveralls). Tests in `test/` mirror the source structure.

### Test Organization
- **Unit tests**: `test/*_test.exs` - One per module
- **Integration tests**: `test/*_integration_test.exs` - End-to-end workflows
- **Stress tests**: `test/integration/one_to_million_channel_test.exs`
- **Security tests**: `test/security_xss_test.exs`, `test/dashboard_security_test.exs`

### Test Helpers (`test/support/`)
- **`tcp_helper.ex`** - TCP client: `connect/0`, `send_line/2`, `recv_line/1`, `authenticate/1`
- **`dashboard_helper.ex`** - HTTP client for dashboard endpoint testing
- **`mass_spawn_helper.ex`** - Concurrent connection spawning for load tests
- **`test_helpers.ex`** - Common utilities
- **`polling_helper.ex`** - Async polling utilities

### Running Tests
```bash
mix test                                    # All tests
mix test test/queue_test.exs                # Single file
mix test test/queue_test.exs:42             # Single test by line
mix coveralls.html                          # Coverage report
MIX_ENV=test mix test --trace               # Verbose output
```

### CI Matrix
- Elixir 1.19.4 / OTP 28.1 (primary)
- Formatting check, Credo strict, security audit, coverage upload

## Dashboard & Monitoring

### HTTP Endpoints (port 4041)
| Method | Path | Purpose | Auth |
|--------|------|---------|------|
| `POST` | `/login` | Authenticate, returns token | Rate limited |
| `POST` | `/logout` | Revoke session | Bearer token |
| `GET` | `/` | Dashboard HTML UI | Bearer token (admin) |
| `GET` | `/metrics` | JSON metrics snapshot | Bearer token |
| `GET` | `/rate_limits` | Current rate limit state | Bearer token |
| `GET` | `/stream` | SSE real-time metrics | Bearer token |

### Dashboard Authentication
```json
POST /login
{"username":"admin","password":"password"}
→ {"s":"ok","token":"eyJ..."}

POST /logout
Authorization: Bearer <token>
→ {"s":"ok"}
```

### SSE Event Format (`/stream`)
Events pushed every ~1 second:
```json
data: {
  "queues": [{"queue":"orders","processed":1000,"acked":950,"buffered":50,...}],
  "channels": [...],
  "system": {
    "process_count": 500,
    "memory": {"total_mb": 128.5},
    "security": {"failed_auth_attempts": 2, "active_sessions": 15}
  }
}
```

## Security Features

### TLS/mTLS
- Optional TLS 1.2/1.3 encryption for TCP connections
- Mutual TLS with client certificate verification via `MALACHI_TLS_CACERTFILE`
- Dev certs: `./scripts/generate-dev-certs.sh`

### Rate Limiting (Token Bucket)
- Per-IP, per-action rate limiting (auth, publish, subscribe)
- Configurable windows and limits
- Returns `retry_after_ms` in error responses

### Connection Limiting
- Max connections per IP (default 100)
- Max total connections (default 10,000)

### Account Lockout
- Progressive lockout after failed auth attempts
- Configurable max attempts (default 5) and duration (default 5 min)

### Input Validation
- Queue/channel name validation (length, characters, reserved words)
- Payload size limits (10MB max)
- Header count and size limits
- XSS prevention in dashboard responses
- ETS-cached validation results for performance

### Audit Logging
- All security events logged in JSON (auth, access, admin actions)
- Configurable output: file, stdout, both, or ETS-only
- Auto-rotation by file size

### Session Security
- Tokens bound to client IP by default
- Optional user-agent binding
- Automatic expiration and cleanup
- Session hijack detection

### Dashboard Security
- CSP, HSTS, X-Frame-Options, X-Content-Type-Options headers
- CORS disabled by default
- Admin-only access to dashboard UI

## Benchmarking

### Benchmark Structure (`benchmark/`)
- **Baselines**: `baseline_throughput.exs`, `baseline_latency.exs`, `baseline_auth.exs`, `baseline_connections.exs`, `baseline_memory.exs`, `baseline_sustained_load.exs`, `baseline_edge_cases.exs`
- **Specialized**: `rate_limiting_benchmark.exs`, `validation_benchmark.exs`, `overflow_strategies_benchmark.exs`, `atom_safety_benchmark.exs`, `dashboard_security_benchmark.exs`, `blocked_producer_benchmark.exs`
- **Utilities**: `benchmark_helpers.ex`, `percentile.ex`, `reporter.ex`, `comparator.ex`
- **Scripts**: `run_all_baselines.sh`, `compare_baselines.exs`

### Running Benchmarks
```bash
./benchmark/scripts/run_all_baselines.sh           # All baselines
mix run benchmark/baselines/baseline_throughput.exs  # Single benchmark
mix run benchmark/scripts/compare_baselines.exs      # Compare results
```

### IEx Benchmarking
```elixir
iex -S mix
Malachi.Benchmark.spawn_consumers("test_queue", 10_000)
Malachi.Benchmark.send_messages("test_queue", 100_000)
Malachi.Benchmark.system_info()
```

## Adding New Features

### New Queue Operation
1. Add public function in `queue.ex` with `get_partition/1` routing
2. Handle in `handle_call/cast` with proper ETS operations
3. Add action handler in `tcp_protocol.ex` → `process_authenticated/4`
4. Add permission check using `Auth.has_permission?/2`
5. Add input validation in `validator.ex` if needed
6. Add audit log event via `AuditLog.log_event/5` for admin operations
7. Add tests in `test/queue_test.exs` and integration test

### New TCP Protocol Command
1. Add case clause in `tcp_protocol.ex` → `process_authenticated/4`
2. Add rate limit check via `RateLimiter.check_rate/3` if applicable
3. Validate input via `Validator.validate_*/1`
4. Return `{"s":"ok",...}` on success or `{"s":"err","reason":"..."}` on error
5. Add integration test using `TCPHelper`

### New Metric
1. Add `increment_*` or `record_*` function in `metrics.ex`
2. Include in `get_metrics/1` return map
3. Call from the appropriate module
4. The dashboard SSE stream will include it automatically

### New Translation
1. Add key to `@translations` map in `i18n.ex` with both `"pt_BR"` and `"en_US"` values
2. Use `I18n.t(:key, bindings)` in code

### New Security Feature
1. Implement as GenServer if stateful, or pure module if stateless
2. Add to supervision tree in `application.ex` (order matters - see comments)
3. Add audit log events for security-relevant actions
4. Add environment variable configuration in `config/runtime.exs`
5. Add tests including integration and security edge cases

## Contributing Conventions

### Branch Naming
- Feature branches: `feat/<issue-number>-<sequence>-<description>` (e.g., `feat/76-8-tls-enforcement`)
- Bug fixes: `fix/<description>`
- Documentation: `docs/<description>`
- Chores: `chore/<description>`

### Commit Messages
Follow [Conventional Commits](https://www.conventionalcommits.org/):
```
<type>: <description>

Types: feat, fix, docs, test, refactor, chore, perf, ci
Examples:
  feat: add TLS mutual authentication support
  fix: resolve session cleanup race condition
  docs: update configuration reference
```

### PR Requirements
- All tests pass (`mix test`)
- Code formatted (`mix format --check-formatted`)
- Credo checks pass (`mix credo --strict`)
- Security audit clean (`mix deps.audit`)
- PR title follows conventional commits format
- PR description minimum 20 characters
- CHANGELOG updated for version-bumping changes

### What should I do before each commit? Execute these commands in sequence:
```bash
mix deps.audit # Security audit
mix format --check-formatted # Check code formatting
mix credo --strict # Static analysis
mix test # Run tests
make docker-test-all # Run tests in Docker environment
```
## If any of these commands fail, you should not commit.

### Version Bumping
- `feat:` → minor version bump
- `fix:` → patch version bump
- `[major]` in title or `BREAKING CHANGE:` in body → major version bump

### ETS Table Registry

| Table Name | Type | Purpose |
|------------|------|---------|
| `:malachi_users` | `ordered_set` | User credentials and permissions |
| `:malachi_sessions` | `bag` | Active session tokens |
| `:malachi_metrics` | `set` | Atomic counters (enqueued, processed, etc.) |
| `:malachi_validated_names` | `set` | Validator cache for names |
| `:malachi_validation_logs` | `bag` | Validation error throttling |
| `:malachi_overflow_logs` | `bag` | Overflow event throttling |
| `:malachi_audit_log` | `bag` | In-memory security events |
| Per-queue anonymous tables | `bag` | Message storage per partition |
