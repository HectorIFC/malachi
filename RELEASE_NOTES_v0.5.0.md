# Malachi v0.5.0 Release Notes

**Release Date:** 2026-02-12

## Summary

Malachi v0.5.0 is a security-focused release that introduces comprehensive hardening across all system layers. This release adds TLS enforcement, rate limiting, input validation, backpressure controls, dashboard security, audit logging, memory monitoring, and an automated security CI/CD pipeline.

This release represents contributions from PRs #80-91 and includes 29 Elixir modules, 50+ test files, 24+ benchmark files, and 6 CI/CD workflows.

## Highlights

### Security Hardening

- **TLS 1.2/1.3 required in production** by default with certificate validation at startup (PR #89)
- **Argon2 password hashing** replaces SHA-256 with timing-safe authentication (PR #83)
- **Rate limiting** with token bucket algorithm for auth (10/60s), publish (1000/1s), and subscribe (100/60s) operations (PR #81)
- **Input validation** with strict queue/channel name rules, 10MB payload limit, and header validation (PR #82)
- **Dashboard security headers**: CSP, HSTS, CORS, X-Frame-Options, X-Content-Type-Options (PR #85)
- **Comprehensive audit logging** with JSON format, file rotation, and multiple output modes (PR #85)
- **Progressive account lockout** after failed authentication attempts (5 min to 6 hours) (PR #83)
- **Session IP binding** for session hijack prevention (PR #83)
- **Atom exhaustion prevention** monitoring BEAM atom table usage at 70%/90% thresholds (PR #87)
- **Memory monitoring** with automatic garbage collection triggers at configurable thresholds (PR #87)

### Resource Management

- **Backpressure system** with configurable overflow strategies: `drop_newest`, `drop_oldest`, `reject`, `block` (PR #84)
- **Queue buffer limits** (default 10,000 messages) prevent unbounded memory growth (PR #84)
- **Connection limits** per-IP (100) and global (10,000) with atomic operations (PR #81)
- **Dynamic queue/channel creation limits** prevent atom table exhaustion (PR #87)

### Developer Experience

- **Performance benchmark suite** with throughput, latency, memory, connection, and auth benchmarks (PR #80)
- **Automated benchmark CI** with 5% regression detection threshold on pull requests (PR #80)
- **Security CI/CD pipeline** with Gitleaks, Trivy, Sobelow, and daily scheduled scans (PR #90)
- **Comprehensive security test suite** with attack simulation, input fuzzing, and OWASP alignment (PR #91)

### Docker Improvements

- Added OCI labels (title, description, version, source, licenses, vendor)
- Added `HEALTHCHECK` instruction for container orchestration
- Container security hardening: `no-new-privileges`, `read-only` filesystem, tmpfs mounts
- Resource constraints: memory and CPU limits in docker-compose
- Rate limiting, session IP binding, and audit logging enabled by default

## Breaking Changes

### 1. TLS Required in Production

TLS is now **required by default** in production environments. The application will fail to start without valid TLS certificates.

**Migration:**
```bash
# Configure TLS certificates (REQUIRED)
export MALACHIMQ_TLS_CERTFILE=/path/to/certificate.pem
export MALACHIMQ_TLS_KEYFILE=/path/to/private_key.pem

# Or disable TLS requirement (NOT RECOMMENDED)
export MALACHIMQ_REQUIRE_TLS=false
```

### 2. Argon2 Password Hashing

Passwords are now hashed with Argon2 instead of SHA-256. **All existing password hashes must be regenerated** after upgrading.

**Migration:**
```bash
export MALACHIMQ_ADMIN_PASS="$(openssl rand -base64 32)"
export MALACHIMQ_PRODUCER_PASS="$(openssl rand -base64 32)"
export MALACHIMQ_CONSUMER_PASS="$(openssl rand -base64 32)"
export MALACHIMQ_APP_PASS="$(openssl rand -base64 32)"
```

### 3. Input Validation Rules

Queue and channel names now enforce strict validation:
- **Allowed characters**: `a-z`, `A-Z`, `0-9`, `_`, `-`, `.`
- **Max length**: 255 characters
- **Reserved names blocked**: `system`, `admin`, `internal`
- **Underscore prefix** (`_`) blocked

See [CHANGELOG.md](CHANGELOG.md#input-validation-rules-affects-queuechannel-names) for the full migration guide.

## Migration Guide

### From v0.4.x to v0.5.0

1. **Set TLS certificates** or explicitly disable TLS requirement (`MALACHIMQ_REQUIRE_TLS=false`)
2. **Regenerate all passwords** (Argon2 replaces SHA-256 - set all `*_PASS` environment variables)
3. **Audit queue/channel names** for compliance with new validation rules
4. **Update Docker images** - new OCI labels, HEALTHCHECK, and security hardening
5. **Review new environment variables** and configure rate limiting, connection limits, and backpressure
6. **Test in staging** before production deployment

### New Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MALACHIMQ_REQUIRE_TLS` | true (prod) | Require TLS in production |
| `MALACHIMQ_TLS_VERSIONS` | tlsv1.3,tlsv1.2 | Allowed TLS versions |
| `MALACHIMQ_RATE_LIMIT_ENABLED` | true | Enable rate limiting |
| `MALACHIMQ_AUTH_RATE_LIMIT` | 10 | Auth attempts per window |
| `MALACHIMQ_PUBLISH_RATE_LIMIT` | 1000 | Publish messages per window |
| `MALACHIMQ_SUBSCRIBE_RATE_LIMIT` | 100 | Subscribe requests per window |
| `MALACHIMQ_MAX_CONN_PER_IP` | 100 | Max connections per IP |
| `MALACHIMQ_MAX_TOTAL_CONN` | 10000 | Max total connections |
| `MALACHIMQ_MAX_AUTH_ATTEMPTS` | 5 | Failed auth before lockout |
| `MALACHIMQ_SESSION_IP_BINDING` | true | Bind sessions to IP |
| `MALACHIMQ_MIN_PASSWORD_LEN` | 12 | Minimum password length |
| `MALACHIMQ_MAX_BUFFER_SIZE` | 10000 | Max messages per queue |
| `MALACHIMQ_OVERFLOW_BEHAVIOR` | drop_newest | Queue overflow strategy |
| `MALACHIMQ_BACKPRESSURE_THRESHOLD` | 0.8 | Backpressure trigger (80%) |
| `MALACHIMQ_ATOM_WARNING_THRESHOLD` | 0.7 | Atom table warning level |
| `MALACHIMQ_ATOM_CRITICAL_THRESHOLD` | 0.9 | Atom table critical level |
| `MALACHIMQ_GC_THRESHOLD_MB` | 500 | Auto-GC memory threshold |
| `MALACHIMQ_MAX_DYNAMIC_QUEUES` | 10000 | Max dynamic queues |
| `MALACHIMQ_MAX_DYNAMIC_CHANNELS` | 1000 | Max dynamic channels |
| `MALACHIMQ_AUDIT_LOG_OUTPUT` | both | Audit log output mode |
| `MALACHIMQ_AUDIT_LOG_MAX_SIZE_MB` | 1 | Max audit log file size |
| `MALACHIMQ_DASHBOARD_CSP` | (default) | Custom CSP policy |
| `MALACHIMQ_DASHBOARD_CORS_ENABLED` | false | Enable CORS |
| `MALACHIMQ_DASHBOARD_CORS_ORIGINS` | * | Allowed CORS origins |
| `MALACHIMQ_HSTS_ENABLED` | true | Enable HSTS |

## Performance

The benchmark suite (`benchmark/`) provides baseline metrics for:
- Message throughput (publish/consume operations per second)
- End-to-end latency (p50, p95, p99 percentiles)
- Memory usage under load
- Connection handling capacity
- Authentication throughput
- Sustained load behavior

Run benchmarks locally:
```bash
bash benchmark/run_all_baselines.sh
```

## Security CI/CD Pipeline

The automated security pipeline (`security.yml`) runs:
- **On every PR**: Sobelow analysis, dependency audit, Gitleaks secret scanning, Trivy filesystem scan
- **On push to main**: All of the above + Docker image scanning
- **Daily at 2 AM UTC**: Full security scan (catches newly disclosed vulnerabilities)
- **Results**: SARIF uploads to GitHub Security tab, PR dependency review comments

## Full Changelog

See [CHANGELOG.md](CHANGELOG.md#050---2026-02-12) for the complete list of changes.

## Acknowledgments

Security features tracked under GitHub Issues #79-91. All PRs (#80-91) have been merged and tested.
