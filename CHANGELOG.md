# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.3] - 2026-07-29

- Use a monotonic counter for mermaid render ids (the previous value could repeat within a millisecond).

## [0.7.2] - 2026-07-29

- Render mermaid diagrams on ExDoc swup navigation, not just on a full page load.

## [0.7.1] - 2026-07-29

- Fix mermaid node labels that rendered as an Unsupported markdown error.

## [0.7.0] - 2026-07-29

- Unify the benchmarks under `benchmark/` and drop the separate `bench/` directory.
- Add a Demo section with the embedded demo video to the README and the docs site.
- Fix the stale pre-commit hook description in the README.

## [0.6.2] - 2026-07-29

- Retry `mix deps.get` in CI to tolerate transient Hex registry failures.
- Rebuild the docs site after a release bumps the version.
- Scope `actions: write` to the release job and drop the sleep after the final retry.

## [0.6.1] - 2026-07-29

- Use the real Docker Hub credentials in the release workflow.
- Add a Performance Benchmarks workflow that runs on every branch.
- Harden the benchmark workflow: no persisted git credentials, run with `mix run --no-start`.

## [0.6.0] - 2026-07-28

> Historical entries in this section keep the `MALACHIMQ_*` variable names they shipped with; the
> current variables are `MALACHI_*` (the rename is the first item under Changed).

### Added

- **Docker**: Migrated from Debian Trixie to Alpine 3.21 base image
  - Eliminates 87 OS-level CVEs (glibc, util-linux, systemd, etc.)
  - Significantly smaller image size (~5MB base vs ~75MB)
  - Uses musl libc with C.UTF-8 locale support
  - argon2_elixir NIF compiled statically, no system libargon2 needed
  - Configured Erlang native DNS resolver (inet_res) for future clustering support
- **TLS Enforcement & Certificate Validation** (PR #89): Production-grade TLS security
  - TLS required by default in production (`MALACHIMQ_REQUIRE_TLS`, default `true` in prod)
  - Certificate validation at startup (existence, format, expiry, key strength)
  - Certificate expiry monitoring (warns at 30 days, critical at 7 days, blocks when expired)
  - RSA key size validation (minimum 2048 bits)
  - Key-certificate match validation
  - PEM format validation (rejects DER, detects empty files)
  - Private key file permission checks (warns on world-readable)
  - TLS version enforcement (only TLS 1.2 and 1.3 allowed)
  - Configurable TLS versions via `MALACHIMQ_TLS_VERSIONS`
  - Configurable verify mode via `MALACHIMQ_TLS_VERIFY`
  - Configurable `fail_if_no_peer_cert` via `MALACHIMQ_TLS_FAIL_IF_NO_PEER_CERT`
  - HSTS `includeSubDomains` now configurable via `MALACHIMQ_HSTS_INCLUDE_SUBDOMAINS`
  - New `Malachi.TLSValidator` module with comprehensive startup validation
  - TLS metrics: handshake success/failure counters, version distribution
  - TLS section in system metrics (`/metrics` endpoint)
  - 22 new i18n translation keys for TLS messages (pt_BR + en_US)
  - TLS performance benchmark suite
- **Input Validation & Sanitization** (PR #82): Comprehensive validation system with ETS cache
  - Queue/channel name validation (255 char limit, alphanumeric + `_-.` only)
  - Reserved name blocking (`system`, `admin`, `internal`, `_*` prefix)
  - Payload size validation (10MB limit)
  - Header validation (50 max, 1-level only, no nil/arrays/maps)
  - HTML/log sanitization functions
  - Validation metrics (cache hits/misses, errors by type)
  - Rate-limited logging (10 logs/min per error type)
  - ReDoS protection with regex performance < 100ms for 100k char inputs
- **Authentication Hardening** (PR #83): Defense-in-depth authentication system
  - Argon2 password hashing replacing SHA-256
  - Progressive account lockout (`Malachi.Auth.LockoutManager`)
    - 1st lockout (5 failures): 5 minutes
    - 2nd lockout (10 failures): 15 minutes
    - 3rd lockout (15 failures): 45 minutes
    - 4th lockout (20 failures): 2 hours
    - 5th+ lockout (25+ failures): 6 hours (max)
  - Session IP binding for session hijack prevention (`MALACHIMQ_SESSION_IP_BINDING`)
  - Trusted proxy support with configurable CIDR ranges (`MALACHIMQ_TRUSTED_PROXY_RANGES`)
  - Configuration validation at startup (`Malachi.Auth.ConfigValidator`)
  - Enhanced session management (`Malachi.Auth.SessionManager`)
  - Minimum password length enforcement (`MALACHIMQ_MIN_PASSWORD_LEN`, default 12)
  - Timing-safe authentication (non-existent users still trigger hash verification)
- **Rate Limiting & Connection Controls** (PR #81): Token bucket rate limiting
  - Per-IP authentication rate limiting (`MALACHIMQ_AUTH_RATE_LIMIT`, default 10/60s)
  - Per-user publish rate limiting (`MALACHIMQ_PUBLISH_RATE_LIMIT`, default 1000/1s)
  - Per-user subscribe rate limiting (`MALACHIMQ_SUBSCRIBE_RATE_LIMIT`, default 100/60s)
  - Per-IP connection limiting (`MALACHIMQ_MAX_CONN_PER_IP`, default 100)
  - Global connection limiting (`MALACHIMQ_MAX_TOTAL_CONN`, default 10,000)
  - New `Malachi.RateLimiter` module with sub-microsecond latency
  - New `Malachi.ConnectionLimiter` module with atomic operations
  - Automatic bucket cleanup every 5 minutes
  - Dashboard endpoint `/rate_limits` for monitoring blocked identifiers
- **Resource Management & Backpressure** (PR #84): Queue overflow protection
  - Queue buffer size limits (`MALACHIMQ_MAX_BUFFER_SIZE`, default 10,000)
  - Configurable overflow strategies: `drop_newest`, `drop_oldest`, `reject`, `block`
  - Configurable backpressure threshold (`MALACHIMQ_BACKPRESSURE_THRESHOLD`, default 80%)
  - Block timeout for blocked producers
  - Maximum blocked producers limit (prevents memory leak)
  - New `Malachi.Backpressure` module
- **Dashboard Security & Audit Logging** (PR #85): Comprehensive dashboard hardening
  - Security headers module (`Malachi.Dashboard.SecurityHeaders`)
  - Content-Security-Policy (CSP) with configurable policy via `MALACHIMQ_DASHBOARD_CSP`
  - CORS support with origin whitelisting (`MALACHIMQ_DASHBOARD_CORS_ORIGINS`)
  - HSTS enforcement with configurable max-age (`MALACHIMQ_HSTS_MAX_AGE`)
  - X-Frame-Options, X-Content-Type-Options, X-XSS-Protection, Referrer-Policy headers
  - Dashboard authentication enabled by default in production
  - Dashboard rate limiting for auth endpoints
  - Comprehensive audit logging (`Malachi.AuditLog`)
  - Audit log output modes: `file`, `stdout`, `both`, `ets_only`
  - Automatic log rotation with configurable max size (`MALACHIMQ_AUDIT_LOG_MAX_SIZE_MB`)
  - JSON-formatted audit events with full context (hostname, node, actor, metadata)
  - 30-day event retention with automatic cleanup
  - Secondary indexes for querying by type or user
- **Atom Exhaustion Prevention & Memory Monitoring** (PR #87): BEAM resource safety
  - `Malachi.AtomMonitor` with configurable warning (70%) and critical (90%) thresholds
  - `Malachi.MemoryMonitor` with automatic garbage collection triggers
  - Real-time memory statistics (total, processes, ETS, atoms, binary, code, system)
  - Top memory-consuming processes tracking
  - Dynamic queue/channel creation limits (`MALACHIMQ_MAX_DYNAMIC_QUEUES`, `MALACHIMQ_MAX_DYNAMIC_CHANNELS`)
  - Dashboard logout button
- **Performance Benchmark Suite** (PR #80): Comprehensive benchmarking infrastructure
  - Baseline benchmarks: throughput, latency, memory, connections, auth, edge cases, sustained load
  - Benchmark orchestration scripts (`benchmark/run_all_baselines.sh`)
  - Comparison tool (`compare_baselines.exs`) for regression detection
  - Benchmark utilities: `comparator.ex`, `reporter.ex`, `percentile.ex`, `benchmark_helpers.ex`
  - GitHub Actions `benchmark.yml` workflow for CI integration
  - 5% regression threshold in CI (build fails on performance degradation)
  - Specialized benchmarks: rate limiting, validation, overflow, atom safety, dashboard security
  - TLS performance benchmark suite
- **Dependency Security CI/CD Pipeline** (PR #90): Automated security scanning
  - GitHub Actions `security.yml` workflow
  - Gitleaks secret scanning integration
  - Trivy filesystem and Docker image scanning (CRITICAL + HIGH severity)
  - Sobelow security analysis in CI
  - SARIF upload to GitHub Security tab
  - Daily scheduled security scans (2 AM UTC)
  - Security scan summary report in GitHub Actions
- **Comprehensive Security Testing** (PR #91): Attack simulation and fuzzing
  - Attack simulation tests (`attack_simulation_test.exs`)
  - Input fuzzing tests (`input_fuzzing_test.exs`)
  - Protocol fuzzing tests (`protocol_fuzzing_test.exs`)
  - Injection attack tests (`injection_attack_test.exs`)
  - Dependency security tests (`dependency_security_test.exs`)
  - Security performance regression tests (`security_performance_regression_test.exs`)
  - OWASP Top 10 alignment tests (`comprehensive_security_test.exs`)
- Automated versioning system with SEMVER
- GitHub Actions for automatic release after merge to main
- Version bump script (`scripts/bump-version.sh`)
- Versioned Docker tags
- TLS/SSL support for TCP server (optional, configurable)
- Socket abstraction layer for transport-agnostic operations
- Development certificate generation script
- Initial implementation of Malachi
- TCP messaging server
- Authentication system
- Web dashboard
- Internationalization support (i18n)
- Docker and Docker Compose
- Initial documentation

### Changed

- **BREAKING: environment variables renamed from `MALACHIMQ_*` to `MALACHI_*`**, following the project's
  rename from MalachiMQ to Malachi. Every variable keeps its name and meaning apart from the prefix, so
  `MALACHIMQ_LOG_NODES` becomes `MALACHI_LOG_NODES`, and so on for all 129 of them.
  - **There is no fallback**: an old `MALACHIMQ_*` variable is simply ignored, and most settings then take
    their default. Two are worth checking before you upgrade: `MALACHI_ACL_STRICT` silently reverts to
    permissive (global permissions act as a wildcard again) and `MALACHI_LOG_CLUSTER` /
    `MALACHI_LOG_NODES` degrade to a single-node in-memory control plane.
  - This also unifies the two prefixes that had been coexisting: the Node client scripts already read
    `MALACHI_HOST`, `MALACHI_PORT`, `MALACHI_USER`, `MALACHI_PASS`.
- **Docker**: Added OCI labels, HEALTHCHECK instruction, and security hardening documentation
- **Authentication**: Replaced SHA-256 password hashing with Argon2 (BREAKING)
  - Existing password hashes must be regenerated after upgrade
- **Pre-commit hooks**: Added Gitleaks for secret detection, large file warnings, private key detection

### Fixed

- **`docker-compose.yml`: the `producer` service could not reach the broker.** It set `MALACHIMQ_HOST`,
  `MALACHIMQ_PORT`, `MALACHIMQ_USER` and `MALACHIMQ_PASS`, but the Node client reads those names without
  the `MQ`, so all four were ignored and the container fell back to `localhost:4040`. The rename makes
  them match. Also dropped `MALACHIMQ_QUEUE`, dead since the queue model was removed.

### Security

- **CRITICAL**: Fixed cleartext transmission of credentials vulnerability
  - Added TLS encryption for TCP connections (required in production)
  - Implemented strong cipher suites (TLS 1.2/1.3 only)
- **Authentication**: Argon2 password hashing replaces weak SHA-256
- **Input Validation**: Protection against injection attacks
  - XSS prevention in queue/channel names and dashboard rendering
  - SQL injection protection
  - Path traversal protection
  - Command injection protection
  - CRLF injection protection
  - Null byte injection protection
  - Atom table exhaustion prevention
- **Rate Limiting**: Token bucket algorithm prevents brute-force attacks
- **Connection Limiting**: Per-IP and global limits prevent DoS attacks
- **Backpressure**: Queue buffer limits prevent memory exhaustion
- **Dashboard Security**: CSP, HSTS, CORS, X-Frame-Options headers
- **Audit Logging**: Comprehensive JSON security event logging
- **Atom Monitoring**: BEAM atom table exhaustion prevention
- **Memory Monitoring**: Automatic GC triggers and usage alerts
- **CI/CD Security**: Automated scanning with Gitleaks, Trivy, Sobelow

### BREAKING CHANGES

#### TLS Required in Production

TLS is now **required by default** in production environments. The application will fail to start in production without valid TLS certificates configured.

**New environment variables:**

| Variable | Default (prod) | Default (dev/test) | Description |
|----------|---------------|-------------------|-------------|
| `MALACHIMQ_REQUIRE_TLS` | `true` | `false` | Require TLS in production |
| `MALACHIMQ_ENABLE_TLS` | follows `REQUIRE_TLS` | `false` | Enable TLS transport |
| `MALACHIMQ_TLS_VERSIONS` | `tlsv1.3,tlsv1.2` | `tlsv1.3,tlsv1.2` | Allowed TLS versions |
| `MALACHIMQ_TLS_VERIFY` | `verify_none` | `verify_none` | Client cert verification |
| `MALACHIMQ_TLS_FAIL_IF_NO_PEER_CERT` | `false` | `false` | Require client cert |
| `MALACHIMQ_HSTS_INCLUDE_SUBDOMAINS` | `true` | `true` | HSTS includeSubDomains |

**Migration guide:**

```bash
# REQUIRED for production:
export MALACHIMQ_TLS_CERTFILE=/path/to/certificate.pem
export MALACHIMQ_TLS_KEYFILE=/path/to/private_key.pem

# To disable TLS requirement (NOT RECOMMENDED):
export MALACHIMQ_REQUIRE_TLS=false
```

#### Authentication Hardening

Authentication now uses **Argon2** instead of SHA-256. All existing password hashes must be regenerated after upgrading.

```bash
# Set new passwords for all users after upgrade:
export MALACHIMQ_ADMIN_PASS="$(openssl rand -base64 32)"
export MALACHIMQ_PRODUCER_PASS="$(openssl rand -base64 32)"
export MALACHIMQ_CONSUMER_PASS="$(openssl rand -base64 32)"
export MALACHIMQ_APP_PASS="$(openssl rand -base64 32)"
```

#### Input Validation Rules (affects queue/channel names)

Queue and channel names must now follow strict validation rules:

**Allowed characters**: `a-z`, `A-Z`, `0-9`, `_`, `-`, `.`
**Max length**: 255 characters
**Reserved names**: `system`, `admin`, `internal`
**Blocked prefixes**: Names starting with `_`

**Migration guide for existing queue/channel names:**

| Before (INVALID) | After (VALID) | Reason |
|------------------|---------------|--------|
| `my queue` | `my_queue` | Spaces not allowed |
| `api/v1/events` | `api.v1.events` | Slashes not allowed |
| `user:session:123` | `user.session.123` | Colons not allowed |
| `system` | `app_system` | Reserved name |
| `_internal` | `internal_queue` | Underscore prefix blocked |

**Header validation changes:**

- Maximum 50 headers per message
- Headers must be flat (single level only)
- Nested maps/objects are **NOT** allowed
- Arrays are **NOT** allowed
- `null`/`nil` values are **NOT** allowed
- Allowed types: string (max 1024 bytes), number, boolean
- Keys: strings only (max 128 bytes)

**Payload limits:**

- Maximum payload size: **10MB** (10,485,760 bytes) - **NOT** configurable

**Message IDs:**

- Message IDs are auto-generated via `System.unique_integer()`
- Custom message IDs from clients are **NOT** accepted

**Error responses:**

Validation errors now return detailed error codes in English (not internationalized):
- `invalid_queue_name_empty`
- `invalid_queue_name_too_long`
- `invalid_queue_name_invalid_characters`
- `invalid_queue_name_reserved`
- `invalid_channel_name_*` (same variants)
- `payload_too_large`
- `invalid_headers_too_many`
- `invalid_headers_key_too_long`
- `invalid_headers_value_too_long`
- `invalid_headers_invalid_type`
