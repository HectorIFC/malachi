# Security Policy

## Reporting a Vulnerability

**Please DO NOT file public GitHub issues for security vulnerabilities.**

### Reporting Process

1. **Email:** Send details to `hectorwilliancardoso@gmail.com`
2. **Response Time:** We aim to respond within 48 hours
3. **Disclosure:** We follow coordinated disclosure practices

### What to Include

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if available)
- Your contact information

### Security Bug Bounty

We currently do not offer a bug bounty program, but we publicly acknowledge security researchers who responsibly disclose vulnerabilities.

## Security Update Process

### For Critical Vulnerabilities (CVSS 9.0-10.0)

- **Response:** Within 4 hours
- **Patch:** Within 24 hours
- **Release:** Emergency release
- **Notification:** Email to all known production users

### For High Severity (CVSS 7.0-8.9)

- **Response:** Within 24 hours
- **Patch:** Within 1 week
- **Release:** Next scheduled release or patch release
- **Notification:** Security advisory on GitHub

### For Medium/Low Severity

- **Response:** Within 1 week
- **Patch:** Included in next minor release
- **Release:** Regular release cycle

## Security Best Practices

### Production Deployment

```bash
# REQUIRED: Set strong passwords
export MALACHI_ADMIN_PASS="$(openssl rand -base64 32)"
export MALACHI_PRODUCER_PASS="$(openssl rand -base64 32)"
export MALACHI_CONSUMER_PASS="$(openssl rand -base64 32)"

# REQUIRED: Configure TLS
export MALACHI_TLS_CERTFILE=/path/to/cert.pem
export MALACHI_TLS_KEYFILE=/path/to/key.pem

# Dashboard: authentication is on by default and admin is required for the HTML
# pages. It reuses the system users above (log in as admin); there is no separate
# dashboard credential. Keep both enabled in production.
export MALACHI_DASHBOARD_AUTH_ENABLED=true
export MALACHI_DASHBOARD_REQUIRE_ADMIN=true

# RECOMMENDED: Enable all security features
export MALACHI_RATE_LIMIT_ENABLED=true
export MALACHI_SESSION_IP_BINDING=true
export MALACHI_AUDIT_LOG_OUTPUT=both
export MALACHI_HSTS_ENABLED=true

# RECOMMENDED: Configure connection and memory limits
export MALACHI_MAX_CONN_PER_IP=50
export MALACHI_MAX_TOTAL_CONN=5000
export MALACHI_GC_THRESHOLD_MB=500
```

### Network Security

- **Firewall:** Restrict access to ports 4040 (message), 4041 (dashboard)
- **TLS:** Always use TLS in production
- **VPC:** Deploy within private network when possible
- **Reverse Proxy:** Use nginx/HAProxy for additional protection

### Monitoring

- **Audit Logs:** Review regularly for suspicious activity
- **Metrics:** Monitor failed authentication attempts
- **Alerts:** Set up alerts for security events

## Security Features

- TLS 1.2+ enforcement with certificate validation at startup
- TLS version enforcement (TLS 1.2 and 1.3 only, weak versions rejected)
- Certificate expiry monitoring with configurable warnings (30-day/7-day thresholds)
- RSA key size validation (minimum 2048 bits)
- Argon2 password hashing (timing-safe, replaces SHA-256)
- Token bucket rate limiting (auth, publish, subscribe, per-IP and per-user)
- Per-IP and global connection limiting
- Progressive account lockout after failed attempts
- Session IP binding for hijack prevention
- Trusted proxy support with configurable CIDR ranges
- Password strength requirements (configurable minimum length)
- Configuration validation at startup
- Input validation and sanitization (topic names, payloads, headers)
- XSS, SQL injection, path traversal, command injection, CRLF injection protection
- Message size limits (10MB max payload)
- Credit-window flow control (backpressure) on streaming reads
- Comprehensive audit logging (JSON format, file rotation, multiple output modes)
- Security headers (CSP, HSTS, X-Frame-Options, X-Content-Type-Options, Referrer-Policy)
- CORS with origin whitelisting
- Memory monitoring with automatic GC triggers
- Atom exhaustion prevention (BEAM atom table monitoring)
- Automated security CI/CD pipeline (Gitleaks, Trivy, Sobelow)
- Comprehensive security test suite (attack simulation, fuzzing, OWASP alignment)
- Pre-commit hooks (Gitleaks secret detection, private key detection)

## Known Security Limitations

- **Inter-node authentication:** Nodes trust each other through the Erlang distribution cookie (`$RELEASE_COOKIE`). For authenticated, encrypted node-to-node traffic, enable mutual-TLS distribution (see `rel/dist_tls.conf.example` and `MALACHI_DIST_TLS_OPTFILE`) and keep the cookie secret.
- **Persistent Users:** User credentials (Argon2 password hashes) are stored in the Raft (`ra`) log on disk. Secure the ra data directory (`MALACHI_RA_DATA_DIR`) with restricted file permissions (e.g., `chmod 700`).
- **No at-rest encryption:** Records are protected in transit by TLS, but the on-disk log is stored in plaintext (the record CRC guards integrity, not confidentiality). Add application-level encryption if you need encryption at rest.

## Container Image Vulnerabilities

Scanners (Docker Scout, Trivy) report vulnerabilities in the base image packages, not only in Malachi's own
code. Where a reported CVE is not exploitable in this image, the assessment is recorded as a machine-readable
[OpenVEX statement](.vex/malachi.openvex.json) so scanners and auditors can see why, rather than the image
being degraded to silence the alert.

Current assessments:

- **CVE-2025-60876** (BusyBox `wget`, medium): **not affected.** The flaw is HTTP header injection when
  `wget` is handed an attacker-controlled URL. The image runs busybox `wget` only in the Docker
  `HEALTHCHECK`, against the fixed literal URL `http://localhost:4041/health`; no attacker-controlled input
  reaches `wget`, so the vector cannot be triggered. Alpine has published no fixed busybox version on any
  branch, so the package cannot be upgraded to remediate. Docker Scout can consume the VEX file with
  `docker scout cves --vex-location .vex hectorcardoso/malachi:latest`. The VEX product PURL is qualified
  with the `latest` tag, which Scout matches against; for reliable matching across every published tag or a
  specific digest, attach this VEX to the image as an in-toto attestation during the release build.

## Security Advisories

Security advisories are published at:

- **GitHub Security Advisories:** [https://github.com/HectorIFC/malachi/security/advisories](https://github.com/HectorIFC/malachi/security/advisories)

## Compliance

Malachi implements controls aligned with:

- OWASP Top 10 (2021)
- CWE Top 25

## Contact

- **Security:** hectorwilliancardoso@gmail.com
- **GitHub:** [https://github.com/HectorIFC/malachi](https://github.com/HectorIFC/malachi)
