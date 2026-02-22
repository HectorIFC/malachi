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
export MALACHIMQ_ADMIN_PASS="$(openssl rand -base64 32)"
export MALACHIMQ_PRODUCER_PASS="$(openssl rand -base64 32)"
export MALACHIMQ_CONSUMER_PASS="$(openssl rand -base64 32)"

# REQUIRED: Configure TLS
export MALACHIMQ_TLS_CERTFILE=/path/to/cert.pem
export MALACHIMQ_TLS_KEYFILE=/path/to/key.pem

# REQUIRED: Set dashboard credentials
export MALACHIMQ_DASHBOARD_USER="admin"
export MALACHIMQ_DASHBOARD_PASS="$(openssl rand -base64 24)"

# RECOMMENDED: Enable all security features
export MALACHIMQ_RATE_LIMIT_ENABLED=true
export MALACHIMQ_SESSION_IP_BINDING=true
export MALACHIMQ_AUDIT_LOG=true
export MALACHIMQ_HSTS=true

# RECOMMENDED: Configure resource limits
export MALACHIMQ_MAX_BUFFER_SIZE=10000
export MALACHIMQ_OVERFLOW_BEHAVIOR=reject
export MALACHIMQ_MAX_CONN_PER_IP=50
export MALACHIMQ_MAX_TOTAL_CONN=5000
export MALACHIMQ_GC_THRESHOLD_MB=500
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
- Input validation and sanitization (queue/channel names, payloads, headers)
- XSS, SQL injection, path traversal, command injection, CRLF injection protection
- Message size limits (10MB max payload)
- Queue buffer limits with backpressure and overflow strategies
- Comprehensive audit logging (JSON format, file rotation, multiple output modes)
- Security headers (CSP, HSTS, X-Frame-Options, X-Content-Type-Options, Referrer-Policy)
- CORS with origin whitelisting
- Memory monitoring with automatic GC triggers
- Atom exhaustion prevention (BEAM atom table monitoring)
- Dynamic queue/channel creation limits
- Automated security CI/CD pipeline (Gitleaks, Trivy, Sobelow)
- Comprehensive security test suite (attack simulation, fuzzing, OWASP alignment)
- Pre-commit hooks (Gitleaks secret detection, private key detection)

## Known Security Limitations

- **Single-Node Only:** No built-in cluster authentication (use network-level security)
- **Volatile Messages:** Messages are stored in memory ETS tables and do not survive restarts.
- **Persistent Users:** User credentials are stored in Mnesia on disk. Ensure `MALACHIMQ_MNESIA_DIR` is secured with restricted file permissions (e.g., `chmod 700`).
- **No Message-Level Encryption:** Implement application-level encryption if needed

## Security Advisories

Security advisories are published at:

- **GitHub Security Advisories:** [https://github.com/HectorIFC/malachimq/security/advisories](https://github.com/HectorIFC/malachimq/security/advisories)

## Compliance

MalachiMQ implements controls aligned with:

- OWASP Top 10 (2021)
- CWE Top 25

## Contact

- **Security:** hectorwilliancardoso@gmail.com
- **GitHub:** [https://github.com/HectorIFC/malachimq](https://github.com/HectorIFC/malachimq)
