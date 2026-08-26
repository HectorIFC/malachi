<p align="center">
  <img src="docs/logo.svg" alt="Malachi" width="280"/>
</p>

# Malachi

An open-source, 100% Elixir reimplementation of LinkedIn's **NorthGuard** log-storage architecture, a CP
(consistent, partition-tolerant), horizontally-scalable **log broker**. Clients speak topics, keys, and
**opaque cursors**: never partitions or offsets - so the broker can split, merge, and restripe its
storage underneath without breaking clients. Replicated by quorum (Raft via `ra`), with SWIM membership,
self-healing, and rack-aware placement.

> [!WARNING]
> **Active development, not production-ready.** Malachi is a work in progress: interfaces and on-disk
> formats can change, it has not been battle-tested at scale, and there is no stability or durability
> guarantee yet. Use it to learn and experiment, not to run production workloads.

[![CI](https://github.com/HectorIFC/malachi/actions/workflows/ci.yml/badge.svg)](https://github.com/HectorIFC/malachi/actions/workflows/ci.yml)
[![Release](https://github.com/HectorIFC/malachi/actions/workflows/release.yml/badge.svg)](https://github.com/HectorIFC/malachi/actions/workflows/release.yml)
[![Docker Image](https://img.shields.io/docker/v/hectorcardoso/malachi?sort=semver&label=Docker%20Hub)](https://hub.docker.com/r/hectorcardoso/malachi)
[![Docker Pulls](https://img.shields.io/docker/pulls/hectorcardoso/malachi.svg)](https://hub.docker.com/r/hectorcardoso/malachi)
[![Security](https://github.com/HectorIFC/malachi/actions/workflows/security.yml/badge.svg)](https://github.com/HectorIFC/malachi/actions/workflows/security.yml)
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

## Introduction

[![Malachi demo](https://img.youtube.com/vi/u1a_Kxs1yeE/0.jpg)](https://youtu.be/u1a_Kxs1yeE)


## 🧭 The model (a log, not a queue)

A client deals in three things and nothing else:

- **topic**: a named, ordered, replicated log.
- **key**: on produce, routes each record to a range of the topic's keyspace (ordering is per key).
- **opaque cursor**: on consume, a position token the client echoes back. It is deliberately opaque:
  internally it encodes per-range positions, but the client never sees partitions or offsets, so the
  broker can split/merge/restripe ranges underneath without breaking the client. This is the core
  difference from Kafka, which leaks partitions and offsets to the client.

Under the hood a topic is a set of dynamic **ranges** (slices of the keyspace that split as they grow),
each a series of **segments** replicated by quorum across nodes. See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full architecture.

## ⚡ Getting started (local)

Requires Elixir `~> 1.19` / OTP 28+.

```bash
git clone https://github.com/HectorIFC/malachi.git
cd malachi
mix deps.get
iex -S mix        # starts the broker: TCP on 4040, dashboard on 4041
```

The quickest way to see the model is the in-process log API in that `iex` session:

```elixir
alias Malachi.LogApi
broker = Malachi.LogBroker

LogApi.create_topic(broker, "events")

# produce by key: no partitions, no offsets exposed
LogApi.produce(broker, "events", [
  %{"key" => "user-1", "value" => "hello"},
  %{"key" => "user-2", "value" => "world"}
])

# consume from the start: get back records + an opaque cursor to resume from
{:ok, records, cursor} = LogApi.fetch(broker, "events", :start, 100)
Enum.map(records, & &1.value)        #=> ["hello", "world"]

# resume by passing the cursor back, nothing new yet
{:ok, [], _cursor} = LogApi.fetch(broker, "events", cursor, 100)
```

A single node writes its segments to disk under a temp directory by default, so point
`MALACHI_LOG_DATA_DIR` and `MALACHI_RA_DATA_DIR` at a real volume to keep data across restarts; set
`MALACHI_LOG_CLUSTER` / `MALACHI_LOG_NODES` for a replicated, HA control plane over `ra`. Over the network,
external clients speak the [binary protocol](#client-protocol) on port 4040.

### Node discovery (libcluster)

For a multi-node deploy, set `MALACHI_CLUSTER_STRATEGY` to have [libcluster](https://github.com/bitwalker/libcluster)
discover and connect peers over Erlang distribution automatically (run each node distributed, e.g.
`--sname`/`--name`). This is connectivity-only: SWIM and the `ra` control plane still take their initial
member set from `MALACHI_LOG_NODES`, and membership changes ride on the rebalancing coordinator. Absent
the variable, nothing changes (single-node, no distribution required).

- `gossip`: UDP multicast, near-zero config (dev/LAN). Tune with `MALACHI_CLUSTER_GOSSIP_PORT`,
  `MALACHI_CLUSTER_GOSSIP_SECRET`, `MALACHI_CLUSTER_GOSSIP_MULTICAST_ADDR`.
- `kubernetes`: pod discovery via the Kubernetes API. Requires `MALACHI_CLUSTER_KUBERNETES_SELECTOR`
  and `MALACHI_CLUSTER_KUBERNETES_NODE_BASENAME`; optional `MALACHI_CLUSTER_KUBERNETES_NAMESPACE` and
  `MALACHI_CLUSTER_KUBERNETES_MODE` (`hostname`/`ip`/`dns`).
- `epmd`: a static host list, reusing `MALACHI_LOG_NODES`, that libcluster keeps connected.

For a full multi-node deploy, [`deploy/kubernetes/`](deploy/kubernetes/README.md) ships a worked example: a 3-node
CP cluster as a StatefulSet with stable Raft identities, zone-aware placement (`min_domains`), and the
health/readiness probes wired up.

## 🚀 Quick Start with Docker

**Multi-Architecture Support**: Works on AMD64 (Intel/AMD) and ARM64 (Apple Silicon, AWS Graviton)

### Pull and Run

```bash
docker pull hectorcardoso/malachi:latest

docker run \
  --name malachi \
  -p 127.0.0.1:4040:4040 \
  -p 127.0.0.1:4041:4041 \
  -e MALACHI_ADMIN_PASS=your_secure_password \
  -e MALACHI_REQUIRE_TLS=false \
  hectorcardoso/malachi:latest
```

The image runs the production release, which requires TLS unless told otherwise, so without
`MALACHI_REQUIRE_TLS=false` the container exits at boot with `TLS certificate file not configured`.
It is then serving plaintext, which is why the ports are bound to `127.0.0.1` rather than published
on every interface. To serve real traffic, configure TLS instead of widening the binding.

### Using Docker Compose

```bash
git clone https://github.com/HectorIFC/malachi.git
cd malachi
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

Malachi ships with security hardening on by default:

- **TLS 1.2/1.3 Enforcement** - Required by default in production with certificate validation at startup
- **Argon2 Password Hashing** - Industry-standard password hashing replacing SHA-256
- **Rate Limiting** - Token bucket limiting on authentication (TCP and dashboard login), keyed by IP
- **Connection Controls** - Per-IP and global connection limits to prevent DoS
- **Input Validation** - Topic name allowlist (path-traversal safe), a configurable frame-size cap, and malformed-frame handling at the connection boundary
- **Streaming Backpressure** - credit-window flow control on push subscriptions (a slow consumer applies backpressure instead of overflowing)
- **Dashboard Security** - CSP, HSTS, CORS, X-Frame-Options, authentication required by default
- **Audit Logging** - JSON-formatted security event logging with automatic rotation
- **Account Lockout** - Progressive lockout after failed authentication attempts
- **Atom Exhaustion Prevention** - BEAM atom table monitoring with configurable thresholds
- **Memory Monitoring** - Automatic GC triggers and memory usage alerts
- **Security CI/CD** - Automated scanning with Gitleaks, Trivy, Sobelow, and CodeQL

For complete configuration, see [SECURITY.md](SECURITY.md) and the [Security Hardening](#security-hardening) section below.

## 📦 Ports

| Port | Description |
|------|-------------|
| 4040 | TCP log protocol (binary) |
| 4041 | Web Dashboard |

## 📈 Observability

Unauthenticated HTTP endpoints on the dashboard port (for load balancers and k8s probes):

| Endpoint | Purpose | Returns |
|----------|---------|---------|
| `GET /health` | Liveness | `200 {"status":"ok"}` while the node is up |
| `GET /ready` | Readiness | `200 {"status":"ready"}` once the log broker is running, else `503 {"status":"not_ready"}` |

Example k8s probes:

```yaml
livenessProbe:  { httpGet: { path: /health, port: 4041 } }
readinessProbe: { httpGet: { path: /ready,  port: 4041 } }
```

### Prometheus metrics

`GET /metrics` serves the **Prometheus text exposition** (v0.0.4) when the scraper asks for it
(`Accept: text/plain`), and the JSON dashboard payload otherwise, same path, content-negotiated. Series
are namespaced `malachi_`: BEAM health (`malachi_process_count`, `malachi_memory_bytes`,
`malachi_uptime_seconds`, …), security counters (`malachi_rate_limit_blocked_total`,
`malachi_failed_auth_total`, `malachi_tls_handshakes_total`, …), operation totals fed by the telemetry
events (`malachi_records_produced_total`, `malachi_bytes_produced_total`, `malachi_records_consumed_total`,
`malachi_auth_attempts_total{result}`, `malachi_replication_commits_total{result}`,
`malachi_storage_integrity_failures_total{reason}`: segments that failed checksum verification,
`malachi_storage_scrub_segments_total{result}`: segments the integrity scrub processed), and
per-topic gauges
(`malachi_topic_ranges`, `malachi_topic_segments`, `malachi_topic_bytes`,
`malachi_domain_violations`: segments spanning fewer than `min_domains` failure domains, …).

`/metrics` requires authentication (any user), so a scrape config passes a token:

```yaml
scrape_configs:
  - job_name: malachi
    scheme: http
    authorization: { credentials: "<token from POST /login>" }
    static_configs: [{ targets: ["malachi-host:4041"] }]
```

### Telemetry events

Malachi emits `:telemetry` events on its hot paths: attach a handler to feed metrics, logs, or traces
(see `Malachi.Telemetry`):

| Event | Measurements | Metadata |
|-------|--------------|----------|
| `[:malachi, :produce]` | `%{count, bytes}` | `%{topic}` |
| `[:malachi, :consume]` | `%{count}` | `%{topic}` |
| `[:malachi, :auth]` | `%{count: 1}` | `%{result: :ok \| :error}` |
| `[:malachi, :replication, :commit]` | `%{count}` | `%{result: :ok \| :no_quorum}` |
| `[:malachi, :storage, :integrity]` | `%{position, unreadable_bytes}` | `%{result, sealed, source, segment}` |
| `[:malachi, :storage, :scrub]` | `%{verified, damaged, repaired, unrepairable}` | `%{}` |

```elixir
:telemetry.attach("my-handler", [:malachi, :produce], fn _e, m, meta, _ ->
  IO.inspect({meta.topic, m.count, m.bytes})
end, nil)
```

### Tracing (OpenTelemetry)

Client operations are traced with OpenTelemetry: `malachi.produce` and `malachi.consume` spans carry
`malachi.topic`, `malachi.records`, and `malachi.bytes` attributes. A produce is a **distributed trace**:
its context propagates across processes and nodes into child spans `malachi.broker.produce` and
`malachi.replication.commit` (the quorum commit on the primary). Tracing is **off by default**, the
sampler drops every span, so there is no per-operation cost until you opt in. To trace, turn the sampler
on, add `{:opentelemetry_exporter, "~> 1.8"}`, and point it at your collector:

```elixir
config :opentelemetry, sampler: :always_on, span_processor: :batch, traces_exporter: :otlp

config :opentelemetry_exporter,
  otlp_protocol: :http_protobuf,
  otlp_endpoint: "http://collector:4318"
```

## 🔐 Authentication

Malachi requires authentication for all producers and consumers. Users and permissions are **replicated across the cluster** via a dedicated Raft (`ra`) group, so a user created on one node exists on every node and survives restarts.


### Default Users (development)

In **dev/test** these convenience users are seeded:

| Username | Password | Permissions |
|----------|----------|-------------|
| admin | admin123 | Full access |
| producer | producer123 | Produce only |
| consumer | consumer123 | Consume only |
| app | app123 | Produce & Consume |

> ⚠️ These weak defaults exist **only** in dev/test and are never shipped to production.

### First boot in production

No default credentials ship. On first boot, if you have not set `MALACHI_ADMIN_PASS`, Malachi **generates a random admin password and logs it once**: save it from the logs (it cannot be recovered). Provide your own with `MALACHI_ADMIN_PASS` (and `MALACHI_PRODUCER_PASS` / `MALACHI_CONSUMER_PASS` / `MALACHI_APP_PASS` for the other service accounts), or set `MALACHI_DISABLE_DEFAULT_USERS=true` to seed nothing and manage users yourself.

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MALACHI_TCP_PORT` | 4040 | TCP server port |
| `MALACHI_DASHBOARD_PORT` | 4041 | Dashboard port |
| `MALACHI_LOCALE` | en_US | Language (en_US, pt_BR) |
| `MALACHI_ADMIN_PASS` | admin123 | Admin password |
| `MALACHI_PRODUCER_PASS` | producer123 | Producer password |
| `MALACHI_CONSUMER_PASS` | consumer123 | Consumer password |
| `MALACHI_APP_PASS` | app123 | App password |
| `MALACHI_SESSION_TIMEOUT_SEC` | 3600 | Session timeout (1h) |
| `MALACHI_ENABLE_TLS` | false | Enable TLS encryption |
| `MALACHI_TLS_CERTFILE` | - | TLS certificate file path |
| `MALACHI_TLS_KEYFILE` | - | TLS private key file path |
| `MALACHI_TLS_CACERTFILE` | - | TLS CA certificate (optional) |
| `MALACHI_REQUIRE_TLS` | true (prod) | Require TLS in production |
| `MALACHI_TLS_VERSIONS` | tlsv1.3,tlsv1.2 | Allowed TLS versions |
| `MALACHI_RATE_LIMIT_ENABLED` | true | Enable rate limiting |
| `MALACHI_AUTH_RATE_LIMIT` | 10 | Auth attempts per window |
| `MALACHI_AUTH_RATE_WINDOW_MS` | 60000 | Auth rate limit window (ms) |
| `MALACHI_PUBLISH_RATE_LIMIT` | 1000 | Publish messages per window |
| `MALACHI_PUBLISH_RATE_WINDOW_MS` | 1000 | Publish rate limit window (ms) |
| `MALACHI_SUBSCRIBE_RATE_LIMIT` | 100 | Subscribe requests per window |
| `MALACHI_SUBSCRIBE_RATE_WINDOW_MS` | 60000 | Subscribe rate limit window (ms) |
| `MALACHI_MAX_CONN_PER_IP` | 100 | Max connections per IP |
| `MALACHI_MAX_TOTAL_CONN` | 10000 | Max total connections |
| `MALACHI_CONNECTION_LIMIT_ENABLED` | true | Enable connection limiting |
| `MALACHI_MAX_AUTH_ATTEMPTS` | 5 | Failed auth attempts before lockout |
| `MALACHI_LOCKOUT_DURATION_MS` | 300000 | Initial lockout duration (5 min) |
| `MALACHI_PROGRESSIVE_LOCKOUT` | true | Enable progressive lockout |
| `MALACHI_SESSION_IP_BINDING` | true | Bind sessions to source IP |
| `MALACHI_MIN_PASSWORD_LEN` | 12 | Minimum password length |
| `MALACHI_ATOM_WARNING_THRESHOLD` | 0.7 | Atom table warning at 70% |
| `MALACHI_ATOM_CRITICAL_THRESHOLD` | 0.9 | Atom table critical at 90% |
| `MALACHI_GC_THRESHOLD_MB` | 500 | Auto-GC memory threshold (MB) |
| `MALACHI_LOG_CLUSTER` | _(unset)_ | Enable the replicated control plane (peer cluster name) |
| `MALACHI_LOG_NODES` | _(unset)_ | Peer node names for the replicated log |
| `MALACHI_LOG_REPLICATION_FACTOR` | 3 | Segment replicas (clamped to node count) |
| `MALACHI_LOG_SPREAD_BY` | _(unset)_ | Broker attribute to spread replicas over (e.g. `rack`), rack/DC-aware placement |
| `MALACHI_LOG_MIN_DOMAINS` | _(unset)_ | Min distinct `spread_by` domains a segment's replicas must span |
| `MALACHI_LOG_PLACEMENT_POLICY` | soft | `hard` fails a produce that cannot meet `min_domains`; `soft` places best-effort |
| `MALACHI_AUTO_REBALANCE` | false | Auto-commit vnode rebalancing on membership change (else operator-driven) |
| `MALACHI_AUTO_REBALANCE_INTERVAL_MS` | 30000 | Reconcile interval for auto-rebalancing |
| `MALACHI_AUTO_REBALANCE_STABILIZATION` | 3 | Consecutive stable reconciles before an auto-commit (absorbs flaps) |
| `MALACHI_SHUTDOWN_GRACE_MS` | 5000 | Drain window on shutdown after the acceptor quiesces, before closing connections |
| `MALACHI_CLUSTER_STRATEGY` | _(unset)_ | Node discovery: `gossip`, `kubernetes`, or `epmd` (see below) |
| `MALACHI_CLUSTER_KUBERNETES_SELECTOR` | _(unset)_ | k8s pod selector, e.g. `app=malachi` (kubernetes strategy) |
| `MALACHI_CLUSTER_KUBERNETES_NODE_BASENAME` | _(unset)_ | k8s node basename, e.g. `malachi` (kubernetes strategy) |
| `MALACHI_MAX_FRAME_SIZE` | 16777216 | Max request frame bytes (also `:max_frame_size` app env) |
| `MALACHI_AUDIT_LOG_OUTPUT` | both | Audit log output (file/stdout/both/ets_only) |
| `MALACHI_AUDIT_LOG_FILE` | /var/log/malachi/audit.log | Audit log file path |
| `MALACHI_AUDIT_LOG_MAX_SIZE_MB` | 1 | Max audit log file size (MB) |

### Custom Users

```bash
docker run \
  -e MALACHI_ADMIN_PASS="your_admin_password" \
  -e MALACHI_DEFAULT_USERS="user1:pass1:produce,consume;user2:pass2:admin" \
  hectorcardoso/malachi:latest
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
  -e MALACHI_ADMIN_PASS="your_secure_password" \
  -e MALACHI_ENABLE_TLS=true \
  -e MALACHI_TLS_CERTFILE=/certs/server.crt \
  -e MALACHI_TLS_KEYFILE=/certs/server.key \
  hectorcardoso/malachi:latest
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


### TLS Features

- ✅ TLS 1.2 and 1.3 support
- ✅ Strong cipher suites (ECDHE, AES-GCM)
- ✅ Perfect Forward Secrecy
- ✅ Mutual TLS (mTLS) support
- ✅ Backward compatible (TLS is optional)

### Inter-node TLS (Erlang distribution)

The sections above secure the **client** connection (port 4040). In a multi-node cluster the nodes also
talk to each other over **Erlang distribution** (the `ra` control plane and segment replication), by
default that traffic is plaintext, guarded only by the distribution cookie. Set `MALACHI_DIST_TLS=true`
to run distribution over **mutual TLS** instead: each node presents a CA-signed certificate and verifies
its peers, so the inter-node traffic is encrypted *and* authenticated.

```sh
# generate a dev CA + node cert + a ready ssl_dist options file
bash scripts/generate-dist-certs.sh

# run a release with inter-node TLS (the script prints this line with real paths)
MALACHI_DIST_TLS=true \
MALACHI_DIST_TLS_OPTFILE=$PWD/priv/dist_cert/dist_tls.conf \
  bin/malachi start
```

`MALACHI_DIST_TLS_OPTFILE` points at an [`ssl_dist` options file](rel/dist_tls.conf.example) (server +
client cert/key/CA, `verify_peer`); the release's `rel/env.sh.eex` translates the flag into
`-proto_dist inet_tls`. A node without TLS cannot join a TLS cluster: the handshake rejects it. The
[Kubernetes example](deploy/kubernetes/README.md) wires this up (the `malachi-dist-tls` Secret + the two env vars).

## 🔐 Dashboard Security

The web dashboard (port 4041) requires authentication in production, so an unauthenticated client cannot
reach it. Set `MALACHI_DASHBOARD_AUTH_ENABLED=false` only for local development.

### Required Configuration

You **MUST** configure dashboard credentials when deploying to production:

```bash
# Option 1: Use existing admin user credentials
docker run \
  -e MALACHI_DEFAULT_USERS="admin:your_strong_password:admin" \
  hectorcardoso/malachi:latest

# Option 2: Separate dashboard credentials (recommended)
docker run \
  -e MALACHI_DASHBOARD_USER="dashboard_admin" \
  -e MALACHI_DASHBOARD_PASS="dashboard_secure_pass_123" \
  hectorcardoso/malachi:latest
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

Malachi session tokens (from TCP authentication) can be used for dashboard access if the user has `:admin` permission:

```bash
# Authenticate via TCP to get token
TOKEN="your_tcp_session_token"

# Use token for dashboard
curl -H "Authorization: Bearer $TOKEN" http://localhost:4041/
```

### Configuration Options

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `MALACHI_DASHBOARD_AUTH_ENABLED` | `true` (prod) | Enable/disable dashboard auth |
| `MALACHI_DASHBOARD_REQUIRE_ADMIN` | `true` | Require `:admin` permission for HTML/SSE |
| `MALACHI_DASHBOARD_AUTH_RATE_LIMIT` | `10` | Max auth attempts per window |
| `MALACHI_DASHBOARD_AUTH_RATE_WINDOW_MS` | `60000` | Rate limit window (1 minute) |
| `MALACHI_DASHBOARD_CORS_ENABLED` | `false` | Enable CORS for `/metrics` and `/stream` |
| `MALACHI_DASHBOARD_CORS_ORIGINS` | `*` | Allowed CORS origins (comma-separated) |
| `MALACHI_DASHBOARD_CSP` | (default) | Custom Content-Security-Policy |
| `MALACHI_HSTS_ENABLED` | `true` | Enable HTTP Strict Transport Security |
| `MALACHI_HSTS_MAX_AGE` | `31536000` | HSTS max-age (1 year) |

### Disabling Authentication (NOT RECOMMENDED)

For development environments only:

```bash
docker run \
  -e MALACHI_DASHBOARD_AUTH_ENABLED=false \
  hectorcardoso/malachi:latest
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
  -e MALACHI_DASHBOARD_CORS_ENABLED=true \
  -e MALACHI_DASHBOARD_CORS_ORIGINS="https://app.example.com,https://admin.example.com" \
  hectorcardoso/malachi:latest
```

With an explicit list, the request's own `Origin` is echoed back when it is on the list, and nothing is sent
when it is not: `Access-Control-Allow-Origin` carries a single origin, never the whole list. The default `*`
answers every origin. The `OPTIONS` preflight applies the same rules, so it never advertises access the real
request would be denied.

Only `/metrics` and `/stream` are cross-origin endpoints, and a preflight for any other path is refused. Note
what that does and does not buy: CORS decides whether a browser lets a page **read** a response, not what
reaches the server. Refusing the preflight blocks the cross-origin requests that need one (a custom header, a
JSON content type), but a CORS-simple request still arrives and is processed, with its response unreadable to
the caller. What keeps such a request unprivileged is the session cookie being `SameSite=Strict`, so it is not
sent cross-origin at all.

Credentials are never part of a cross-origin exchange here: no `Access-Control-Allow-Credentials` is sent, so
a cross-origin caller authenticates with a `Bearer` token. That also makes `/stream` same-origin only, since
an `EventSource` cannot set headers.

## 🔍 Audit Logging

Malachi includes comprehensive audit logging for security-relevant events.

### Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `MALACHI_AUDIT_LOG_OUTPUT` | `both` | Output mode: `file`, `stdout`, `both`, `ets_only` |
| `MALACHI_AUDIT_LOG_FILE` | `/var/log/malachi/audit.log` | Audit log file path |
| `MALACHI_AUDIT_LOG_MAX_SIZE_MB` | `1` | Max file size in MB (auto-rotation) |

### Output Modes

```bash
# File only (traditional deployments)
-e MALACHI_AUDIT_LOG_OUTPUT=file \
-e MALACHI_AUDIT_LOG_FILE=/var/log/malachi/audit.log

# Stdout only (container/cloud environments)
-e MALACHI_AUDIT_LOG_OUTPUT=stdout

# Both file and stdout
-e MALACHI_AUDIT_LOG_OUTPUT=both

# ETS only (no file/stdout, in-memory only)
-e MALACHI_AUDIT_LOG_OUTPUT=ets_only
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
  "hostname": "malachi-prod-01",
  "node": "malachi@localhost"
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
Malachi.AuditLog.get_events(100)

# Get events by type
Malachi.AuditLog.get_events_by_type(:dashboard_access, 50)

# Get events by user
Malachi.AuditLog.get_events_by_user("admin", 50)

# Get statistics
Malachi.AuditLog.get_stats()
```

<a id="security-hardening"></a>

## 🛡️ Security Hardening

### Production Checklist

- [ ] **Enable TLS encryption** (`MALACHI_ENABLE_TLS=true`)
- [ ] **Use strong passwords** (min 16 characters, mix of letters/numbers/symbols)
- [ ] **Configure dashboard authentication** (never disable in production)
- [ ] **Enable HSTS** when using TLS (`MALACHI_HSTS_ENABLED=true`)
- [ ] **Restrict CORS origins** (whitelist specific domains)
- [ ] **Enable audit logging** (`MALACHI_AUDIT_LOG_OUTPUT=both`)
- [ ] **Monitor audit logs** for suspicious activity
- [ ] **Use firewall rules** to restrict access to ports 4040/4041
- [ ] **Run as non-root user** in containers
- [ ] **Keep software updated** (latest Docker image)
- [ ] **Configure rate limiting** (`MALACHI_RATE_LIMIT_ENABLED=true`)
- [ ] **Set connection limits** (adjust `MALACHI_MAX_CONN_PER_IP` and `MALACHI_MAX_TOTAL_CONN`)
- [ ] **Set the frame-size cap** for your workload (`MALACHI_MAX_FRAME_SIZE`, default 16 MiB)
- [ ] **Enable session IP binding** (`MALACHI_SESSION_IP_BINDING=true`)
- [ ] **Set memory monitoring** (`MALACHI_GC_THRESHOLD_MB` appropriate for your environment)
- [ ] **Review atom table thresholds** (adjust `MALACHI_ATOM_WARNING_THRESHOLD`)

### Content Security Policy (CSP)

The default CSP allows `'unsafe-inline'` for compatibility. For maximum security, use a stricter policy:

```bash
docker run \
  -e MALACHI_DASHBOARD_CSP="default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:" \
  hectorcardoso/malachi:latest
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

<a id="client-protocol"></a>

## 📡 Client protocol

External clients connect to port 4040 and speak a compact **binary protocol** (`Malachi.Wire`). Every
message is a length-prefixed frame:

```
Frame:     <<len::32, body>>
Request:   <<api_key::16, correlation_id::32, payload>>
Response:  <<correlation_id::32, error_code::16, payload>>   # error_code 0 = ok, 1 = error (reason string)
```

`correlation_id` lets a client pipeline (match each response to its request). `api_key` selects the
operation:

| api_key | operation      | notes                                                          |
|--------:|----------------|----------------------------------------------------------------|
| 0       | `auth`         | **required first frame**; `username`/`password` → session token |
| 1       | `create_topic` | topic name + keyspace bits                                      |
| 2       | `produce`      | topic + records (routed by key); returns the produced count    |
| 3       | `fetch`        | topic + opaque cursor / consumer group / **group member** → records + cursor |
| 4       | `commit`       | durably commit a consumer group's position (from a cursor)      |
| 5       | `subscribe`    | open a server-push stream for a group (or a **group member**), bounded by a credit window |
| 6       | `stream_ack`   | ack N streamed records: commit the position **and** return credit (a **member** ack also heartbeats) |
| 7       | `leave_group`  | remove a member from its group (fast rebalance on clean shutdown) |

Records on the wire carry **no offset**: position travels only in the opaque cursor, and permissions
(`:produce`/`:consume`) are enforced per operation against the authenticated session.

A `fetch` with a **consumer-group member id** is server-scoped: the coordinator assigns each member of a
group a share of the topic's ranges, so members consume in **parallel** and disjointly. The client still
only sees records + an opaque cursor: ranges never cross the wire - and the member stays alive by
fetching (or explicitly `leave_group`s on shutdown).

Streaming (`subscribe`/`stream_ack`) is the NorthGuard-style sessionized push: after subscribing, the
server pushes records up to the credit window; the client acks to durably advance the group's position
(at-least-once) and return credit, so a slow consumer applies backpressure instead of overflowing.
A `subscribe` with a **member id** scopes the push stream to that member's ranges (parallel, disjoint,
still opaque); the member ack doubles as a coordinator heartbeat, so an idle member sends a periodic
empty ack to stay alive (and `leave_group`s on shutdown for a fast rebalance). A member's coordination
is owned by one node (the leader of the topic's vnode); during a leadership failover a member request may
briefly get `not_owner`, which is transient: the reference client backs off and retries (the server
re-resolves the new leader).

### Reference client (Node.js)

`scripts/` ships a dependency-free Node.js reference client that speaks the protocol above:

- `scripts/lib/wire.js`: the binary codec, a direct port of `Malachi.Wire` (framing, envelope, records).
- `scripts/lib/client.js`: a connection that multiplexes requests by `correlation_id` and routes push
  frames to a subscription callback.
- `scripts/producer.js` / `consumer.js` / `subscriber.js`, CLIs for append, pull, and server-push.

```bash
# append 100 records to a topic (creating it first)
node scripts/producer.js orders 100 --create

# pull with a resumable consumer group, long-polling for new records
node scripts/consumer.js orders --group workers --follow

# parallel consumption: several members of one group each get a share of the ranges (opaque, disjoint)
node scripts/consumer.js orders --group workers --member c1 &
node scripts/consumer.js orders --group workers --member c2 &

# server-push streaming (subscribe + credit-windowed acks)
node scripts/subscriber.js orders --group live

# parallel server-push: several members of one group, each streamed a disjoint share (opaque)
node scripts/subscriber.js orders --group live --member s1 &
node scripts/subscriber.js orders --group live --member s2 &

# end-to-end demo (append, then stream while producing)
bash scripts/streaming-demo.sh
```

Default credentials: `producer`/`producer123` (produce + create-topic), `consumer`/`consumer123`
(consume), `app`/`app123` (both). Override with `MALACHI_USER`/`MALACHI_PASS`; point at another server
with `MALACHI_HOST`/`MALACHI_PORT`. The same flow is exercised in-VM by `Malachi.Test.TCPHelper`
(`test/support/tcp_helper.ex`).

### Load test

`scripts/loadtest.js` is a load generator built on the same client, in two modes. **Closed-loop**
(default) runs N connections in a tight `op → await` loop to find the ceiling and the latency at
saturation. **Open-loop** (`--rate <rps>`) fires requests at a fixed arrival rate and measures latency
from each request's *scheduled* time: correcting coordinated omission, so a stall shows up as latency on
the requests that queued behind it. Both report throughput (ops/s, records/s, MB/s) and latency
percentiles (p50/p90/p95/p99).

```bash
# closed-loop: max produce throughput, 20 connections for 10s
node scripts/loadtest.js --scenario produce --connections 20 --duration 10

# open-loop: hold 1500 req/s and see the coordinated-omission-corrected latency
node scripts/loadtest.js --scenario produce --rate 1500 --duration 10

# fetch a 50k-record backlog, 200 records/pull
node scripts/loadtest.js --scenario fetch --prepopulate 50000 --max 200

# server-push streaming throughput
node scripts/loadtest.js --scenario stream --connections 4 --window 500

# mixed produce+fetch under contention, 512-byte records
node scripts/loadtest.js --scenario mixed --connections 20 --record-size 512 --json
```

Latency is stored in a bounded reservoir (percentiles stay representative on long runs while min/max
remain exact). `--help` lists every flag.

> **Note on zero-copy:** all throughput numbers published for Malachi so far were measured WITHOUT
> the zero-copy consume optimization (`:file.sendfile` on the fetch path is a planned future
> optimization, not yet implemented; it would first require aligning the wire fetch encoding with the
> on-disk frame, see `benchmark/README.md`). Produce numbers are unaffected; read-side numbers have
> headroom once it lands.

## 🛠️ Development

### Prerequisites

- Elixir 1.19+
- Erlang/OTP 28+

**Note**: `mix.exs` requires `~> 1.19`, so earlier Elixir versions do not compile. OTP 28 is what CI and the
Docker image build against; older OTP releases are not tested or supported.

### Initial Setup

After cloning the repository, run the setup script to install git hooks:

```bash
./scripts/setup-dev.sh
```

This will:
- Install [Lefthook](https://github.com/evilmartians/lefthook) (git hooks manager)
- Configure a pre-commit hook that runs `mix format`
- Ensure all developers have consistent git hooks

The pre-commit hook runs `mix format` and re-stages any `.ex`/`.exs` files it reformats. To skip: `git commit --no-verify`

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

See [the CI workflow](.github/workflows/ci.yml) for details.

## 🌍 Internationalization (i18n)

Malachi supports **Brazilian Portuguese (pt_BR)** and **American English (en_US)**.

### Configuration

```elixir
config :malachi, locale: "pt_BR"
```

### Runtime Change

```elixir
Malachi.I18n.set_locale("en_US")
Malachi.I18n.locale()
```

## 📊 User Management (Elixir)

```elixir
Malachi.Auth.list_users()
Malachi.Auth.add_user("myuser", "mypass", [:produce, :consume])
Malachi.Auth.remove_user("myuser")
Malachi.Auth.change_password("myuser", "newpass")
```

## 🏗️ Architecture

Malachi ports LinkedIn's NorthGuard log-storage design to Elixir/OTP:

- **Control plane**: topic/range/segment metadata as a deterministic state machine, replicated per vnode by Raft (`ra`) and sharded across vnodes by topic.
- **Data plane**: a `Log` of `segments` per range, replicated by quorum across nodes; placement is HRW/rendezvous and rack-aware.
- **Membership**: SWIM (gossip with suspicion) for failure detection; self-healing re-replicates segments and promotes primaries on node loss.
- **Client**: a compact binary protocol over TCP; topics, keys, and opaque cursors (never partitions or offsets).

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full design.

## 📄 License

MIT License

## 🤝 Contributing

We welcome contributions! Please follow these guidelines:

### Before You Start

1. Check existing issues and PRs
2. Discuss major changes in an issue first
3. Read [the CI workflow](.github/workflows/ci.yml)

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

## 📋 Input validation

Malachi validates untrusted input at the connection boundary: a malformed frame is answered with an
error, never a crash.

**Topic names**: an allowlist that is path-traversal safe (a topic name becomes an on-disk directory
name): allowed characters `A-Z a-z 0-9 . _ -`, non-empty, and never `.` or `..`. Enforced
deterministically in the control plane (`Malachi.Metadata`), so it holds identically on every replica.

```
valid:    orders   user.events   app-logs_v2   api.v1.payments
invalid:  "my topic" (space)   api/v1/events (slash)   user:session (colon)   ""   .   ..
```

**Frame size**. The binary protocol rejects a frame whose declared length exceeds `:max_frame_size`
(application config, default 16 MiB) **at the 4-byte length prefix**, before the body is buffered, so a
hostile length prefix cannot exhaust memory.

**Records**: a record's value is arbitrary bytes (non-UTF-8 survives the round trip); headers are
key/value byte-string pairs. Both are bounded by the frame cap, and records carry no client-visible offset.

The underlying security infra: authentication (Argon2), rate limiting, connection limits, account
lockout, audit logging, is covered above and in [SECURITY.md](SECURITY.md).

## 🔖 Versioning

This project uses [SEMVER](https://semver.org/) with automated releases.

- **Patch**: Bug fixes → Add `patch` label or default
- **Minor**: New features → Add `minor` label or use `feat:` prefix
- **Major**: Breaking changes → Add `major` label or use `[major]` in title

See the [GitHub Releases](https://github.com/HectorIFC/malachi/releases) for the per-version change history.