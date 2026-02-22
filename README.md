<p align="center">
  <img src="docs/logo.jpeg" alt="MalachiMQ Logo" width="200"/>
</p>

# MalachiMQ

High-performance message system.

[![CI](https://github.com/HectorIFC/malachimq/actions/workflows/ci.yml/badge.svg)](https://github.com/HectorIFC/malachimq/actions/workflows/ci.yml)
[![Release](https://github.com/HectorIFC/malachimq/actions/workflows/release.yml/badge.svg)](https://github.com/HectorIFC/malachimq/actions/workflows/release.yml)
[![Docker Image](https://img.shields.io/docker/v/hectorcardoso/malachimq?label=Docker%20Hub)](https://hub.docker.com/r/hectorcardoso/malachimq)
[![Docker Pulls](https://img.shields.io/docker/pulls/hectorcardoso/malachimq.svg)](https://hub.docker.com/r/hectorcardoso/malachimq)
[![Security](https://github.com/HectorIFC/malachimq/actions/workflows/security.yml/badge.svg)](https://github.com/HectorIFC/malachimq/actions/workflows/security.yml)
[![Benchmark](https://github.com/HectorIFC/malachimq/actions/workflows/benchmark.yml/badge.svg)](https://github.com/HectorIFC/malachimq/actions/workflows/benchmark.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Sponsor](https://img.shields.io/badge/sponsor-❤-ff69b4)](https://github.com/sponsors/HectorIFC)

## 💖 Sponsors

Become a sponsor and get your logo on our README on GitHub with a link to your site. [[Become a sponsor](https://github.com/sponsors/HectorIFC)]

<!-- sponsors -->
<!-- Add sponsor logos here as they join -->
<!-- Example: <a href="https://sponsor-site.com"><img src="https://sponsor-logo.png" width="64"></a> -->
<!-- sponsors -->

### Backers

Support us with a monthly donation and help us continue our activities. [[Become a backer](https://github.com/sponsors/HectorIFC)]

<!-- backers -->
<!-- Add backer avatars here as they join -->
<!-- backers -->

---

## Demo

Watch MalachiMQ in action:

[![MalachiMQ Demo](https://img.youtube.com/vi/hn26zgRoOUI/0.jpg)](https://www.youtube.com/watch?v=hn26zgRoOUI)

## 🚀 Quick Start with Docker

**Multi-Architecture Support**: Works on AMD64 (Intel/AMD) and ARM64 (Apple Silicon, AWS Graviton)

### Pull and Run

```bash
docker pull hectorcardoso/malachimq:latest

docker run \
  --name malachimq \
  -p 4040:4040 \
  -p 4041:4041 \
  -e MALACHIMQ_ADMIN_PASS=your_secure_password \
  hectorcardoso/malachimq:latest
```

### Using Docker Compose

```bash
git clone https://github.com/HectorIFC/malachimq.git
cd malachimq
docker-compose up -d
```

Access the dashboard at: http://localhost:4041

### Build Locally (All Platforms)

```bash
# Build for your current architecture
make docker-build

# Build for multiple architectures (requires Docker Buildx)
make docker-buildx-setup
make docker-buildx

# Build and push to Docker Hub (multi-arch)
make docker-buildx-push
```

See [Multi-Architecture Build Guide](docs/MULTI_ARCH_BUILD.md) for detailed instructions.

## 🛡️ Security Features

MalachiMQ v0.5.0 includes comprehensive security hardening:

- **TLS 1.2/1.3 Enforcement** - Required by default in production with certificate validation at startup
- **Argon2 Password Hashing** - Industry-standard password hashing replacing SHA-256
- **Rate Limiting** - Token bucket algorithm for auth, publish, and subscribe operations
- **Connection Controls** - Per-IP and global connection limits to prevent DoS
- **Input Validation** - Strict queue/channel name validation, payload size limits, header validation
- **Backpressure** - Queue buffer limits with configurable overflow strategies
- **Dashboard Security** - CSP, HSTS, CORS, X-Frame-Options, authentication required by default
- **Audit Logging** - JSON-formatted security event logging with automatic rotation
- **Account Lockout** - Progressive lockout after failed authentication attempts
- **Atom Exhaustion Prevention** - BEAM atom table monitoring with configurable thresholds
- **Memory Monitoring** - Automatic GC triggers and memory usage alerts
- **Security CI/CD** - Automated scanning with Gitleaks, Trivy, Sobelow, and CodeQL

For complete configuration, see [SECURITY.md](SECURITY.md) and the [Security Hardening](#-security-hardening) section below.

## 📦 Ports

| Port | Description |
|------|-------------|
| 4040 | TCP Message Queue |
| 4041 | Web Dashboard |

## 🔐 Authentication

MalachiMQ requires authentication for all producers and consumers. Users and permissions are **persisted to disk** via Mnesia, surviving server restarts.


### Default Users

| Username | Password | Permissions |
|----------|----------|-------------|
| admin | admin123 | Full access |
| producer | producer123 | Produce only |
| consumer | consumer123 | Consume only |
| app | app123 | Produce & Consume |

> ⚠️ **Important**: Change default passwords in production!

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MALACHIMQ_TCP_PORT` | 4040 | TCP server port |
| `MALACHIMQ_DASHBOARD_PORT` | 4041 | Dashboard port |
| `MALACHIMQ_LOCALE` | en_US | Language (en_US, pt_BR) |
| `MALACHIMQ_ADMIN_PASS` | admin123 | Admin password |
| `MALACHIMQ_PRODUCER_PASS` | producer123 | Producer password |
| `MALACHIMQ_CONSUMER_PASS` | consumer123 | Consumer password |
| `MALACHIMQ_APP_PASS` | app123 | App password |
| `MALACHIMQ_MNESIA_DIR` | ./data/mnesia | User persistence directory |
| `MALACHIMQ_SESSION_TIMEOUT_MS` | 3600000 | Session timeout (1h) |
| `MALACHIMQ_ENABLE_TLS` | false | Enable TLS encryption |
| `MALACHIMQ_TLS_CERTFILE` | - | TLS certificate file path |
| `MALACHIMQ_TLS_KEYFILE` | - | TLS private key file path |
| `MALACHIMQ_TLS_CACERTFILE` | - | TLS CA certificate (optional) |
| `MALACHIMQ_REQUIRE_TLS` | true (prod) | Require TLS in production |
| `MALACHIMQ_TLS_VERSIONS` | tlsv1.3,tlsv1.2 | Allowed TLS versions |
| `MALACHIMQ_RATE_LIMIT_ENABLED` | true | Enable rate limiting |
| `MALACHIMQ_AUTH_RATE_LIMIT` | 10 | Auth attempts per window |
| `MALACHIMQ_AUTH_RATE_WINDOW_MS` | 60000 | Auth rate limit window (ms) |
| `MALACHIMQ_PUBLISH_RATE_LIMIT` | 1000 | Publish messages per window |
| `MALACHIMQ_PUBLISH_RATE_WINDOW_MS` | 1000 | Publish rate limit window (ms) |
| `MALACHIMQ_SUBSCRIBE_RATE_LIMIT` | 100 | Subscribe requests per window |
| `MALACHIMQ_SUBSCRIBE_RATE_WINDOW_MS` | 60000 | Subscribe rate limit window (ms) |
| `MALACHIMQ_MAX_CONN_PER_IP` | 100 | Max connections per IP |
| `MALACHIMQ_MAX_TOTAL_CONN` | 10000 | Max total connections |
| `MALACHIMQ_CONNECTION_LIMIT_ENABLED` | true | Enable connection limiting |
| `MALACHIMQ_MAX_AUTH_ATTEMPTS` | 5 | Failed auth attempts before lockout |
| `MALACHIMQ_LOCKOUT_DURATION_MS` | 300000 | Initial lockout duration (5 min) |
| `MALACHIMQ_PROGRESSIVE_LOCKOUT` | true | Enable progressive lockout |
| `MALACHIMQ_SESSION_IP_BINDING` | true | Bind sessions to source IP |
| `MALACHIMQ_MIN_PASSWORD_LEN` | 12 | Minimum password length |
| `MALACHIMQ_MAX_BUFFER_SIZE` | 10000 | Max messages per queue buffer |
| `MALACHIMQ_OVERFLOW_BEHAVIOR` | drop_newest | Overflow strategy |
| `MALACHIMQ_BACKPRESSURE_THRESHOLD` | 0.8 | Backpressure trigger threshold |
| `MALACHIMQ_ATOM_WARNING_THRESHOLD` | 0.7 | Atom table warning at 70% |
| `MALACHIMQ_ATOM_CRITICAL_THRESHOLD` | 0.9 | Atom table critical at 90% |
| `MALACHIMQ_GC_THRESHOLD_MB` | 500 | Auto-GC memory threshold (MB) |
| `MALACHIMQ_MAX_DYNAMIC_QUEUES` | 10000 | Max dynamic queues |
| `MALACHIMQ_MAX_DYNAMIC_CHANNELS` | 1000 | Max dynamic channels |
| `MALACHIMQ_AUDIT_LOG_OUTPUT` | both | Audit log output (file/stdout/both/ets_only) |
| `MALACHIMQ_AUDIT_LOG_FILE` | /var/log/malachimq/audit.log | Audit log file path |
| `MALACHIMQ_AUDIT_LOG_MAX_SIZE_MB` | 1 | Max audit log file size (MB) |

### Custom Users

```bash
docker run \
  -e MALACHIMQ_ADMIN_PASS="your_admin_password" \
  -e MALACHIMQ_DEFAULT_USERS="user1:pass1:produce,consume;user2:pass2:admin" \
  hectorcardoso/malachimq:latest
```

Format: `username:password:permission1,permission2;...`

Permissions: `admin`, `produce`, `consume`

## 🔒 TLS/SSL Encryption

**⚠️ IMPORTANT**: For production deployments, always enable TLS to encrypt credentials and messages.

### Quick Start with TLS

#### 1. Generate Development Certificates

```bash
./scripts/generate-dev-certs.sh
```

#### 2. Run with TLS Enabled

```bash
docker run \
  -p 4040:4040 \
  -v $(pwd)/priv/cert:/certs \
  -e MALACHIMQ_ADMIN_PASS="your_secure_password" \
  -e MALACHIMQ_ENABLE_TLS=true \
  -e MALACHIMQ_TLS_CERTFILE=/certs/server.crt \
  -e MALACHIMQ_TLS_KEYFILE=/certs/server.key \
  hectorcardoso/malachimq:latest
```

#### 3. Connect with TLS Client (Node.js)

```javascript
const tls = require('tls');

const client = tls.connect({
  host: 'localhost',
  port: 4040,
  rejectUnauthorized: false  // For self-signed certs (dev only)
}, () => {
  console.log('TLS connected');
  client.write(JSON.stringify({
    action: 'auth',
    username: 'producer',
    password: 'producer123'
  }) + '\n');
});
```

### Production TLS Setup

For production, use certificates from:
- **Let's Encrypt** (free, automated)
- **DigiCert**, **GlobalSign** (commercial CAs)
- **Internal PKI** (corporate environments)

See [TLS Security Advisory](docs/SECURITY_ADVISORY_TLS.md) for complete documentation.

### TLS Features

- ✅ TLS 1.2 and 1.3 support
- ✅ Strong cipher suites (ECDHE, AES-GCM)
- ✅ Perfect Forward Secrecy
- ✅ Mutual TLS (mTLS) support
- ✅ Backward compatible (TLS is optional)

## 🔐 Dashboard Security (v0.5.0+)

### ⚠️ BREAKING CHANGE

**Dashboard authentication is now ENABLED BY DEFAULT in production.**

The web dashboard (port 4041) now requires authentication to prevent unauthorized access. This change enhances security but requires configuration updates for existing deployments.

### Required Configuration

You **MUST** configure dashboard credentials when deploying to production:

```bash
# Option 1: Use existing admin user credentials
docker run \
  -e MALACHIMQ_DEFAULT_USERS="admin:your_strong_password:admin" \
  hectorcardoso/malachimq:latest

# Option 2: Separate dashboard credentials (recommended)
docker run \
  -e MALACHIMQ_DASHBOARD_USER="dashboard_admin" \
  -e MALACHIMQ_DASHBOARD_PASS="dashboard_secure_pass_123" \
  hectorcardoso/malachimq:latest
```

### Accessing the Dashboard

#### Browser Access

1. Navigate to `http://localhost:4041/login`
2. Enter your credentials
3. Token is stored in browser localStorage
4. Auto-redirect on token expiry

#### API Access with Bearer Token

```bash
# Get token via login endpoint
TOKEN=$(curl -X POST http://localhost:4041/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"your_password"}' \
  | jq -r '.token')

# Access dashboard with token
curl -H "Authorization: Bearer $TOKEN" http://localhost:4041/metrics

# SSE stream with token
curl -H "Authorization: Bearer $TOKEN" http://localhost:4041/stream
```

#### Using Existing Session Tokens

MalachiMQ session tokens (from TCP authentication) can be used for dashboard access if the user has `:admin` permission:

```bash
# Authenticate via TCP to get token
TOKEN="your_tcp_session_token"

# Use token for dashboard
curl -H "Authorization: Bearer $TOKEN" http://localhost:4041/
```

### Configuration Options

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `MALACHIMQ_DASHBOARD_AUTH_ENABLED` | `true` (prod) | Enable/disable dashboard auth |
| `MALACHIMQ_DASHBOARD_REQUIRE_ADMIN` | `true` | Require `:admin` permission for HTML/SSE |
| `MALACHIMQ_DASHBOARD_AUTH_RATE_LIMIT` | `10` | Max auth attempts per window |
| `MALACHIMQ_DASHBOARD_AUTH_RATE_WINDOW_MS` | `60000` | Rate limit window (1 minute) |
| `MALACHIMQ_DASHBOARD_CORS_ENABLED` | `false` | Enable CORS for `/metrics` and `/stream` |
| `MALACHIMQ_DASHBOARD_CORS_ORIGINS` | `*` | Allowed CORS origins (comma-separated) |
| `MALACHIMQ_DASHBOARD_CSP` | (default) | Custom Content-Security-Policy |
| `MALACHIMQ_HSTS_ENABLED` | `true` | Enable HTTP Strict Transport Security |
| `MALACHIMQ_HSTS_MAX_AGE` | `31536000` | HSTS max-age (1 year) |

### Disabling Authentication (NOT RECOMMENDED)

For development environments only:

```bash
docker run \
  -e MALACHIMQ_DASHBOARD_AUTH_ENABLED=false \
  hectorcardoso/malachimq:latest
```

**⚠️ WARNING**: Never disable authentication in production or internet-facing deployments.

### Permission Model

- **`/` (Dashboard HTML)** and **`/stream` (SSE)**: Require `:admin` permission (configurable)
- **`/metrics`** and **`/rate_limits`**: Allow any authenticated user
- **`/login`**: Public endpoint (no authentication required)

### Security Headers

All dashboard responses include comprehensive security headers:

- **Content-Security-Policy (CSP)**: Prevents XSS attacks
- **X-Frame-Options**: Prevents clickjacking
- **X-Content-Type-Options**: Prevents MIME-sniffing
- **X-XSS-Protection**: Legacy XSS protection
- **Referrer-Policy**: Controls referrer information
- **Strict-Transport-Security (HSTS)**: Enforces HTTPS (when TLS enabled)

### CORS Configuration

For web applications accessing metrics:

```bash
docker run \
  -e MALACHIMQ_DASHBOARD_CORS_ENABLED=true \
  -e MALACHIMQ_DASHBOARD_CORS_ORIGINS="https://app.example.com,https://admin.example.com" \
  hectorcardoso/malachimq:latest
```

## 🔍 Audit Logging

MalachiMQ includes comprehensive audit logging for security-relevant events.

### Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `MALACHIMQ_AUDIT_LOG_OUTPUT` | `both` | Output mode: `file`, `stdout`, `both`, `ets_only` |
| `MALACHIMQ_AUDIT_LOG_FILE` | `/var/log/malachimq/audit.log` | Audit log file path |
| `MALACHIMQ_AUDIT_LOG_MAX_SIZE_MB` | `1` | Max file size in MB (auto-rotation) |

### Output Modes

```bash
# File only (traditional deployments)
-e MALACHIMQ_AUDIT_LOG_OUTPUT=file \
-e MALACHIMQ_AUDIT_LOG_FILE=/var/log/malachimq/audit.log

# Stdout only (container/cloud environments)
-e MALACHIMQ_AUDIT_LOG_OUTPUT=stdout

# Both file and stdout
-e MALACHIMQ_AUDIT_LOG_OUTPUT=both

# ETS only (no file/stdout, in-memory only)
-e MALACHIMQ_AUDIT_LOG_OUTPUT=ets_only
```

### Logged Events

All events are logged in JSON format with full context:

- **Authentication**: `auth_success`, `auth_failure`, `auth_lockout`
- **Sessions**: `session_created`, `session_revoked`, `session_expired`, `session_hijack_attempt`
- **Dashboard**: `dashboard_access`, `dashboard_login_success`, `dashboard_auth_failure`
- **Administrative**: `account_unlocked`, `config_validation_failed`

### Example Audit Log Entry

```json
{
  "timestamp": "2026-02-09T15:30:45.123Z",
  "event_id": "a1b2c3d4e5f6",
  "event_type": "dashboard_access",
  "actor": {
    "username": "admin",
    "ip": "192.168.1.100"
  },
  "action": "http_GET_/metrics",
  "result": "success",
  "metadata": {
    "path": "/metrics",
    "method": "GET"
  },
  "hostname": "malachimq-prod-01",
  "node": "malachimq@localhost"
}
```

### File Rotation

Audit logs automatically rotate when exceeding `AUDIT_LOG_MAX_SIZE_MB`:
- Only the most recent events are kept
- Rotation maintains valid JSON lines
- No external tools required (logrotate not needed)

### Querying Audit Logs

Via Elixir API:

```elixir
# Get recent events
MalachiMQ.AuditLog.get_events(100)

# Get events by type
MalachiMQ.AuditLog.get_events_by_type(:dashboard_access, 50)

# Get events by user
MalachiMQ.AuditLog.get_events_by_user("admin", 50)

# Get statistics
MalachiMQ.AuditLog.get_stats()
```

## 🛡️ Security Hardening

### Production Checklist

- [ ] **Enable TLS encryption** (`MALACHIMQ_ENABLE_TLS=true`)
- [ ] **Use strong passwords** (min 16 characters, mix of letters/numbers/symbols)
- [ ] **Configure dashboard authentication** (never disable in production)
- [ ] **Enable HSTS** when using TLS (`MALACHIMQ_HSTS_ENABLED=true`)
- [ ] **Restrict CORS origins** (whitelist specific domains)
- [ ] **Enable audit logging** (`MALACHIMQ_AUDIT_LOG_OUTPUT=both`)
- [ ] **Monitor audit logs** for suspicious activity
- [ ] **Use firewall rules** to restrict access to ports 4040/4041
- [ ] **Run as non-root user** in containers
- [ ] **Keep software updated** (latest Docker image)
- [ ] **Configure rate limiting** (`MALACHIMQ_RATE_LIMIT_ENABLED=true`)
- [ ] **Set connection limits** (adjust `MALACHIMQ_MAX_CONN_PER_IP` and `MALACHIMQ_MAX_TOTAL_CONN`)
- [ ] **Configure backpressure** (set `MALACHIMQ_MAX_BUFFER_SIZE` and `MALACHIMQ_OVERFLOW_BEHAVIOR`)
- [ ] **Enable session IP binding** (`MALACHIMQ_SESSION_IP_BINDING=true`)
- [ ] **Set memory monitoring** (`MALACHIMQ_GC_THRESHOLD_MB` appropriate for your environment)
- [ ] **Review atom table thresholds** (adjust `MALACHIMQ_ATOM_WARNING_THRESHOLD`)

### Content Security Policy (CSP)

The default CSP allows `'unsafe-inline'` for compatibility. For maximum security, use a stricter policy:

```bash
docker run \
  -e MALACHIMQ_DASHBOARD_CSP="default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:" \
  hectorcardoso/malachimq:latest
```

**Note**: Removing `'unsafe-inline'` requires refactoring dashboard HTML to use external script/style files or nonces. This is planned for a future release.

### Rate Limiting

Dashboard authentication is rate-limited to prevent brute-force attacks:
- Default: 10 attempts per 60 seconds per IP
- Failed attempts trigger account lockout (configurable)
- Rate limits apply to both `/login` endpoint and Bearer token validation

### Security Metrics

Monitor security metrics via `/metrics` endpoint:

```bash
curl -H "Authorization: Bearer $TOKEN" http://localhost:4041/metrics | jq '.system.security'
```

Returns:
```json
{
  "failed_auth_attempts": 5,
  "account_lockouts": 1,
  "active_sessions": 12,
  "dashboard": {
    "auth_success": 150,
    "auth_failed": 5,
    "auth_blocked": 2
  }
}
```

## 📡 Client Example (Node.js)

```javascript
const net = require('net');

const client = net.createConnection(4040, 'localhost', () => {
  client.write(JSON.stringify({
    action: 'auth',
    username: 'producer',
    password: 'producer123'
  }) + '\n');
});

client.on('data', (data) => {
  const response = JSON.parse(data.toString().trim());
  
  if (response.token) {
    // Publish to queue
    client.write(JSON.stringify({
      action: 'publish',
      queue_name: 'my-queue',
      payload: { hello: 'world' },
      headers: {}
    }) + '\n');
    
    // Or publish to channel (best-effort, no buffering)
    client.write(JSON.stringify({
      action: 'channel_publish',
      channel_name: 'news',
      payload: { breaking: 'news!' },
      headers: {}
    }) + '\n');
  }
});
```

#### Channel Subscribe

```javascript
// Subscribe to channel
client.write(JSON.stringify({
  action: 'channel_subscribe',
  channel_name: 'news'
}) + '\n');

// Response: {"s":"ok"}

// Receive messages
client.on('data', (data) => {
  const msg = JSON.parse(data.toString().trim());
  
  if (msg.channel_message) {
    console.log('Channel message:', msg.channel_message);
    // {
    //   payload: {...},
    //   headers: {...},
    //   timestamp: 1234567890,
    //   channel: "news"
    // }
  }
  
  if (msg.kicked_from_channel) {
    console.log('Kicked from:', msg.kicked_from_channel);
  }
});
```

### Using the Node.js Scripts

The `scripts/` directory contains Node.js clients for testing and development.

```bash
cd scripts
npm install
```

#### Producer Script

Send messages to a queue:

```bash
# Send 10 messages (default)
node producer.js

# Send 100 messages
node producer.js 100

# Send messages continuously (1/second)
node producer.js --continuous

# Send 1000 messages in parallel (fast mode)
node producer.js 1000 --fast

# Show help
node producer.js --help
```

#### Consumer Script

Receive messages from a queue:

```bash
# Consume from 'test' queue (default)
node consumer.js

# Consume from a specific queue
node consumer.js orders

# Verbose mode (show full payload and headers)
node consumer.js --verbose

# Combine options
node consumer.js orders --verbose

# Show help
node consumer.js --help
```

#### Environment Variables (Scripts)

| Variable | Default | Description |
|----------|---------|-------------|
| `MALACHIMQ_HOST` | localhost | Server host |
| `MALACHIMQ_PORT` | 4040 | Server port |
| `MALACHIMQ_QUEUE` | test | Default queue name |
| `MALACHIMQ_USER` | producer/consumer | Username |
| `MALACHIMQ_PASS` | producer123/consumer123 | Password |
| `MALACHIMQ_LOCALE` | pt_BR | Locale (pt_BR, en_US) |

#### Example: Producer + Consumer

**Terminal 1** - Start the consumer:
```bash
node consumer.js --verbose
```

**Terminal 2** - Send messages:
```bash
node producer.js 10
```

### Channel Pub/Sub

MalachiMQ supports Pub/Sub channels with best-effort delivery. Messages are broadcast to all active subscribers without persistence.

#### Channel Publisher Script

Publish messages to channels:

```bash
# Publish 10 messages to 'news' channel (default)
node channel-publisher.js

# Publish to a specific channel
node channel-publisher.js sports 20

# Publish continuously (1 msg/second)
node channel-publisher.js alerts --continuous

# Show help
node channel-publisher.js --help
```

#### Channel Subscriber Script

Subscribe to channels and receive messages in real-time:

```bash
# Subscribe to 'news' channel (default)
node channel-subscriber.js

# Subscribe to a specific channel
node channel-subscriber.js sports

# Subscribe to multiple channels
node channel-subscriber.js news sports alerts

# Verbose mode (show full payloads)
node channel-subscriber.js --verbose

# Show help
node channel-subscriber.js --help
```

#### Channel Behavior

- **Best-effort delivery**: Messages are only delivered to active subscribers
- **No buffering**: Messages are dropped if no subscribers are connected
- **Broadcast**: All subscribers receive every message
- **Real-time**: Messages delivered immediately to connected clients

#### Example: Channel Pub/Sub

**Terminal 1** - Start subscriber:
```bash
node channel-subscriber.js news
```

**Terminal 2** - Publish messages:
```bash
node channel-publisher.js news 10
```

**Terminal 3** - Add another subscriber:
```bash
node channel-subscriber.js news sports
```

**Note**: Subscribers only receive messages published *after* they subscribe. Messages published before subscription are lost (no buffering).

## 🛠️ Development

### Prerequisites

- Elixir 1.19+
- Erlang/OTP 28+

**Note**: While MalachiMQ is optimized for Elixir 1.19+ and OTP 28+, it may work with earlier versions (1.16+/OTP 26+) but is not officially tested or supported.

### Initial Setup

After cloning the repository, run the setup script to install git hooks:

```bash
./scripts/setup-dev.sh
```

This will:
- Install [Lefthook](https://github.com/evilmartians/lefthook) (git hooks manager)
- Configure pre-commit hook to automatically update performance baselines
- Ensure all developers have consistent git hooks

The pre-commit hook runs benchmarks (~10 minutes) when you modify files in `lib/malachimq/` or `benchmark/`. To skip: `git commit --no-verify`

### Run Locally

```bash
mix deps.get
mix run --no-halt
```

### Run Tests

```bash
mix test
```

### Build Docker Image Locally

```bash
make docker-build
make docker-run
```

### Available Make Commands

```bash
make build          # Install deps and compile
make run            # Run locally
make test           # Run tests
make release        # Build production release
make docker-build   # Build Docker image
make docker-run     # Run Docker container
make docker-stop    # Stop Docker container
make docker-push    # Push to Docker Hub
make compose-up     # Start with docker-compose
make compose-down   # Stop docker-compose
make clean          # Clean build artifacts
```

### Code Quality Checks

```bash
# Format code
mix format

# Check formatting
mix format --check-formatted

# Run static analysis
mix credo --strict

# Check for security issues
mix deps.audit

# Check for unused dependencies
mix deps.unlock --check-unused
```

### CI/CD

The project uses GitHub Actions for continuous integration:

- ✅ **Automated Tests** - Run on every commit
- ✅ **Multiple Elixir/OTP Versions** - Tested on 3 versions
- ✅ **Code Quality** - Credo, formatting, security checks
- ✅ **Docker Build** - Verified on every PR
- ✅ **Automatic Releases** - On merge to main
- ✅ **Security Scanning** - Gitleaks, Trivy, Sobelow on every PR
- ✅ **Performance Benchmarks** - Automated regression detection on PRs
- ✅ **Daily Security Scans** - Scheduled vulnerability scanning (2 AM UTC)

See [CI/CD Documentation](docs/CI_CD.md) for details.

## 🌍 Internationalization (i18n)

MalachiMQ supports **Brazilian Portuguese (pt_BR)** and **American English (en_US)**.

### Configuration

```elixir
config :malachimq, locale: "pt_BR"
```

### Runtime Change

```elixir
MalachiMQ.I18n.set_locale("en_US")
MalachiMQ.I18n.locale()
```

## 📊 User Management (Elixir)

```elixir
MalachiMQ.Auth.list_users()
MalachiMQ.Auth.add_user("myuser", "mypass", [:produce, :consume])
MalachiMQ.Auth.remove_user("myuser")
MalachiMQ.Auth.change_password("myuser", "newpass")
```

## 🏗️ Architecture

- **ETS Tables**: In-memory storage for maximum performance
- **GenServer**: OTP processes for reliability
- **TCP Server**: Custom protocol for low latency
- **Partitioning**: Automatic load distribution across CPU cores

## 📄 License

MIT License

## 🤝 Contributing

We welcome contributions! Please follow these guidelines:

### Before You Start

1. Check existing issues and PRs
2. Discuss major changes in an issue first
3. Read [CI/CD Documentation](docs/CI_CD.md)

### Development Process

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feat/amazing-feature`)
3. **Make** your changes with tests
4. **Run** quality checks:
   ```bash
   mix format
   mix test
   mix credo --strict
   ```
5. **Commit** using [Conventional Commits](https://www.conventionalcommits.org/):
   ```bash
   git commit -m "feat: add amazing feature"
   ```
6. **Push** to your fork (`git push origin feat/amazing-feature`)
7. **Open** a Pull Request

### PR Requirements

- ✅ **Tests** - All new features must include tests
- ✅ **Documentation** - Update relevant docs
- ✅ **CI Passing** - All checks must pass
- ✅ **Conventional Commits** - Follow commit format
- ✅ **Code Review** - Address review feedback

### Commit Message Format

```
<type>: <description>

Examples:
- feat: add TLS support
- fix: resolve authentication bug
- docs: update README
- test: add unit tests for Auth module
- chore: update dependencies
```

**Types:**
- `feat:` - New feature (→ minor version)
- `fix:` - Bug fix (→ patch version)
- `docs:` - Documentation
- `test:` - Tests
- `refactor:` - Code refactoring
- `chore:` - Maintenance

**Breaking Changes:**
- Add `[major]` to title or `BREAKING CHANGE:` in body

## ️📋 Input Validation Rules

MalachiMQ enforces strict validation on all user-supplied inputs to prevent injection attacks, resource exhaustion, and protocol violations.

### Queue and Channel Names

**Allowed characters**: `a-z`, `A-Z`, `0-9`, `_`, `-`, `.`  
**Length**: 1-255 characters  
**Reserved names**: `system`, `admin`, `internal`  
**Blocked prefixes**: Names starting with `_`

**✅ Valid examples:**
```
orders
user.events
app-logs_v2
api.v1.payments
```

**❌ Invalid examples:**
```
my queue          ← spaces not allowed
api/v1/events     ← slashes not allowed
user:session      ← colons not allowed
system            ← reserved name
_internal         ← underscore prefix blocked
```

### Message Payloads

- **Maximum size**: 10MB (10,485,760 bytes)
- **Type**: Binary/string data
- **Not configurable** - fixed limit for security

### Headers

- **Maximum count**: 50 headers per message
- **Structure**: Single-level map only (no nesting)
- **Key format**: String, max 128 bytes
- **Value types**:
  - ✅ String (max 1024 bytes)
  - ✅ Number (integer or float)
  - ✅ Boolean (true/false)
  - ❌ null/nil
  - ❌ Arrays/lists
  - ❌ Nested maps/objects

**✅ Valid headers:**
```json
{
  "priority": 1,
  "type": "order",
  "urgent": true,
  "customer_id": "abc123"
}
```

**❌ Invalid headers:**
```json
{
  "optional": null,           ← nil not allowed
  "tags": ["a", "b"],         ← arrays not allowed
  "metadata": {"x": 1}        ← nested maps not allowed
}
```

### Error Responses

Validation errors return detailed error codes:

```json
{"s": "err", "reason": "invalid_queue_name_too_long"}
{"s": "err", "reason": "invalid_queue_name_invalid_characters"}
{"s": "err", "reason": "invalid_queue_name_reserved"}
{"s": "err", "reason": "payload_too_large"}
{"s": "err", "reason": "invalid_headers_too_many"}
{"s": "err", "reason": "invalid_headers_invalid_type"}
```

### Performance

- **Cache optimization**: Validated names are cached in ETS
- **Cache hit latency**: < 1µs
- **Cache miss latency**: < 10µs
- **ReDoS protection**: All inputs validated in < 100ms
- **Throughput impact**: < 2% with warm cache

### Migration from Pre-Validation Versions

If upgrading from a version without validation:

1. **Audit existing queue/channel names**:
   ```bash
   curl http://localhost:4041/metrics | jq '.queues[].queue'
   ```

2. **Rename incompatible queues** using the mapping table in [CHANGELOG.md](CHANGELOG.md#breaking-changes)

3. **Update client code** to validate inputs before sending

4. **Test with validation enabled** in staging environment first

## 🔖 Versioning

This project uses [SEMVER](https://semver.org/) with automated releases.

- **Patch**: Bug fixes → Add `patch` label or default
- **Minor**: New features → Add `minor` label or use `feat:` prefix
- **Major**: Breaking changes → Add `major` label or use `[major]` in title

See [VERSIONING.md](docs/VERSIONING.md) for details.