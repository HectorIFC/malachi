# Malachi

[![GitHub](https://img.shields.io/github/v/release/HectorIFC/malachi?label=GitHub)](https://github.com/HectorIFC/malachi)
[![Docker Pulls](https://img.shields.io/docker/pulls/hectorcardoso/malachi)](https://hub.docker.com/r/hectorcardoso/malachi)
[![Docker Image Size](https://img.shields.io/docker/image-size/hectorcardoso/malachi/latest)](https://hub.docker.com/r/hectorcardoso/malachi)
[![License](https://img.shields.io/github/license/HectorIFC/malachi)](https://github.com/HectorIFC/malachi/blob/main/LICENSE)

**Malachi** is an open-source, 100% Elixir reimplementation of LinkedIn's **NorthGuard** log-storage architecture: a CP, horizontally-scalable **log broker**. Clients speak topics, keys, and opaque cursors, never partitions or offsets, so the broker can split and restripe its storage underneath without breaking them. The control plane is replicated by quorum (Raft via `ra`).

## Features

- 🚀 **Append-only log on disk** - CRC-checked segments, sharded into ranges the broker restripes online
- 🔐 **TLS/SSL Support** - Secure connections with certificate-based auth
- 📊 **Real-time Dashboard** - Built-in web UI with live metrics
- 🔄 **Consumer groups and streaming** - Server-committed positions, plus server-push with credit-based flow control
- 🌐 **Multi-language Support** - i18n support (en_US, pt_BR)
- 🐳 **Production Ready** - Alpine-based image for minimal attack surface
- 🏗️ **Multi-Architecture** - Supports AMD64 and ARM64 (Apple Silicon, AWS Graviton)

---

## Quick Start

### Pull the image

```bash
docker pull hectorcardoso/malachi:latest
```

### Run with default settings

```bash
docker run \
  --name malachi \
  -p 4040:4040 \
  -p 4041:4041 \
  -e MALACHI_ADMIN_PASS="your_secure_password" \
  hectorcardoso/malachi:latest
```

**Note**: The image automatically detects your platform (AMD64 or ARM64) and uses the appropriate build.

### Access the dashboard

Open [http://localhost:4041](http://localhost:4041) in your browser.

---

## Supported Platforms

| Architecture | Status | Notes |
|--------------|--------|-------|
| `linux/amd64` | ✅ Supported | x86_64 (Intel/AMD) |
| `linux/arm64` | ✅ Supported | Apple Silicon (M1/M2/M3), AWS Graviton |

---

## Supported Tags

| Tag | Description |
|-----|-------------|
| `latest` | Latest stable release |
| `X.Y.Z` | Specific version (e.g., `0.2.0`) |
| `X.Y` | Latest patch of minor version (e.g., `0.2`) |
| `X` | Latest minor of major version (e.g., `0`) |
| `alpine` | Alpine Linux base |

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MALACHI_TCP_PORT` | `4040` | TCP server port for clients |
| `MALACHI_DASHBOARD_PORT` | `4041` | HTTP dashboard port |
| `MALACHI_LOCALE` | `en_US` | Language (`en_US`, `pt_BR`) |
| `MALACHI_ENABLE_TLS` | `false` | Enable TLS encryption |
| `MALACHI_ADMIN_PASS` | *(generated)* | Admin password; if unset, a random one is generated and logged on first boot. Without a persistent ra volume, this happens again on every restart. See Users and credentials. |
| `MALACHI_LOG_DATA_DIR` | *(tmp)* | Directory for the durable log segments. Must be an absolute path on a volume; see Data Persistence. |
| `MALACHI_RA_DATA_DIR` | *(tmp)* | Directory for the ra log (users, ACLs, lockouts). Must be an absolute path on a volume; see Data Persistence. |

### Data Persistence

The broker keeps its log segments and its ra log (which holds user credentials, ACLs, and lockouts) on
disk. **With no volume, both default to a path under `/tmp` and are lost on every restart.** Point the two
directories at a persistent volume mounted at `/app/data`, which the image already owns:

```bash
docker run \
  --name malachi \
  -p 4040:4040 \
  -p 4041:4041 \
  -e MALACHI_ADMIN_PASS="your_secure_password" \
  -e MALACHI_LOG_DATA_DIR=/app/data/log \
  -e MALACHI_RA_DATA_DIR=/app/data/ra \
  -v malachi-data:/app/data \
  hectorcardoso/malachi:latest
```

In production these must be absolute paths: a relative value is rejected at boot, because it would resolve
against the working directory and put durable data back on ephemeral storage. The bundled
[`docker-compose.yml`](https://github.com/HectorIFC/malachi/blob/main/docker-compose.yml) and the
Kubernetes manifest use these same paths.

### TLS Configuration

```bash
docker run \
  --name malachi \
  -p 4040:4040 \
  -p 4041:4041 \
  -e MALACHI_ADMIN_PASS="your_secure_password" \
  -e MALACHI_ENABLE_TLS=true \
  -v /path/to/certs:/app/priv/cert:ro \
  hectorcardoso/malachi:latest
```

Required certificate files in the mounted volume:
- `server.crt` - Server certificate
- `server.key` - Private key
- `ca.crt` - CA certificate (optional)

---

## Docker Compose

```yaml
version: '3.8'

services:
  malachi:
    image: hectorcardoso/malachi:latest
    container_name: malachi
    ports:
      - "4040:4040"  # TCP server
      - "4041:4041"  # Dashboard
    environment:
      - MALACHI_LOCALE=en_US
      - MALACHI_LOG_DATA_DIR=/app/data/log
      - MALACHI_RA_DATA_DIR=/app/data/ra
    volumes:
      - malachi-data:/app/data
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "wget", "-q", "-O", "/dev/null", "http://localhost:4041/health"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  malachi-data:
```

> These compose defaults are fine for test and development; tune them conservatively for production.

Two ready-made stacks live in the repository, and both go further than the snippet above.

### Single node, observable

```bash
docker compose up -d
```

A broker plus **Jaeger** and **Prometheus**, so the thing you just started is one you can watch:

| What | Where |
|------|-------|
| Broker (binary protocol) | `localhost:4040` |
| Dashboard | http://localhost:4041 |
| Traces | http://localhost:16686 |
| Metrics | http://localhost:9090 |

Produce a little traffic and a produce shows up in Jaeger as a **distributed trace**: `malachi.produce`
at the root, `malachi.broker.produce` under it, and `malachi.replication.commit` under that, carrying
the topic, the record count and the byte count. Tracing is off by default everywhere else, because the
sampler dropping every span is what makes `Tracer.with_span` free on the produce path; this stack sets
`MALACHI_TRACING_ENABLED=true` and samples everything, which is right for watching a trickle and wrong
for a busy node (use `MALACHI_TRACING_SAMPLE_RATIO` there).

The metrics side needs one moving part that is worth understanding before you copy it. `/metrics`
requires an authenticated user and a Malachi session expires, so a token pasted into a scrape config
works right up until it does not. A small `metrics-token` sidecar logs in and rewrites the file
Prometheus reads through `credentials_file`, which Prometheus re-reads on every scrape. It shares
Prometheus's network namespace on purpose: sessions are bound to the IP that created them
(`MALACHI_SESSION_IP_BINDING`), so the token has to be minted from the address that will use it.

### Durable cluster, observable

```bash
docker compose -f docker-compose.cluster-durable.yml up -d
```

Three brokers forming one replicated control plane at replication factor 3, each on its own named
volume, plus the same Jaeger and Prometheus. Restart the whole thing and the data is still there,
which is the difference between this and `docker-compose.cluster.yml`: that one exists for benchmarks
and keeps its data on tmpfs so fsync cost stays uniform across runs, and tmpfs is remounted empty on
every restart.

Only node 1 publishes to the host, so it is the only one a client on your machine can reach. The three
are interchangeable to a client **on the compose network**, which is the sense that matters for how
the cluster behaves: produce through any of them and the write is quorum-replicated across all three. Publish the other two if you want to exercise that from outside. Prometheus scrapes all three,
because most Malachi series are per-node facts (BEAM memory, connections, the integrity scrub's
progress over *that* node's disk) and scraping one would report a third of the cluster while looking
complete. It uses one scrape job per node, each with its own token: users, ACLs and lockouts are
replicated across the cluster through the auth `ra` group, but **sessions are not**, so a session
minted on node 1 is rejected by node 2.

Both stacks ship with placeholder credentials in a public file, with `MALACHI_REQUIRE_TLS=false`, and
with Jaeger and Prometheus carrying no authentication of their own. Every port they publish is bound
to `127.0.0.1` because of that combination rather than in spite of it: on `0.0.0.0` these files would
hand anyone who can route to the host a plaintext broker whose password is on GitHub, plus every trace
it has recorded.

Undoing that takes three separate things, and the middle one is easy to miss:

```bash
MALACHI_ADMIN_PASS="$(openssl rand -base64 32)" \
MALACHI_REQUIRE_TLS=true \
MALACHI_TLS_CERTFILE=/certs/server.pem \
MALACHI_TLS_KEYFILE=/certs/server-key.pem \
  docker compose up -d
```

Real passwords in the environment. `MALACHI_REQUIRE_TLS=true`, which is **not** implied by setting the
certificate paths: with the flag left at `false` the broker reads those files and still serves
plaintext, a security setting failing in the shape that looks configured. And certificates mounted
into the container, because those two paths resolve inside it, so add a
`- /etc/malachi/certs:/certs:ro` volume alongside them.

Only then widen the broker's bindings, and widen only the broker's. Jaeger and Prometheus have no
authentication to turn on, so they stay on `127.0.0.1` until you have put something in front of them
that does.

---

## Client protocol

Malachi speaks a length-framed **binary** protocol on the TCP port: a connection authenticates first, then
exchanges topic, produce, fetch, commit, and subscribe frames. Positions are opaque cursors, never offsets.
Clients do not hand-write frames.

Use one of the supported clients instead:

- the reference **Node CLI** in [`scripts/`](https://github.com/HectorIFC/malachi/tree/main/scripts)
  (`producer.js`, `consumer.js`, `subscriber.js`), which reads `MALACHI_HOST`, `MALACHI_PORT`,
  `MALACHI_USER`, `MALACHI_PASS`, `MALACHI_TOPIC`;
- the in-process Elixir API (`Malachi.LogApi`) for embedded use.

See the [guides](https://hectorifc.github.io/malachi) for producing, consuming, consumer groups, and
streaming with backpressure.

---

## Users and credentials

The image runs in production mode, which ships **no hardcoded passwords**. On first boot, when
`MALACHI_ADMIN_PASS` is not set, the broker generates a random `admin` password and logs it once, so read
the container logs to retrieve it. The generated user lives in the `ra` store, so without a persistent
`MALACHI_RA_DATA_DIR` volume (see Data Persistence) the `admin` user is gone on the next restart and a new
random password is generated and logged again. Set credentials explicitly with either:

- per-user env vars: `MALACHI_ADMIN_PASS`, `MALACHI_PRODUCER_PASS`, `MALACHI_CONSUMER_PASS`,
  `MALACHI_APP_PASS`. A user is seeded only when its password is set; `producer` gets `produce`, `consumer`
  gets `consume`, and `app` gets both.
- or a full list via `MALACHI_DEFAULT_USERS` in the form `user:pass:perm,perm;user2:...`.

Set `MALACHI_DISABLE_DEFAULT_USERS=true` to seed no users and manage them entirely through the API.

> ⚠️ The `admin123` / `producer123` / `consumer123` credentials exist only in the dev and test configs,
> never in this production image.

---

## Health Check

The dashboard exposes two public probe endpoints (no authentication, so a probe never sends credentials):

- `/health` - liveness: returns `200 {"status":"ok"}` once the HTTP server is up.
- `/ready` - readiness: returns `200 {"status":"ready"}` when the log broker is running, else `503`.

```bash
wget -q -O - http://localhost:4041/health
```

The bundled image health check probes `/health` with `wget`; `/metrics` is authenticated and not a health
endpoint.

---

## Image Details

| Property | Value |
|----------|-------|
| **Base Image** | `alpine:3.21` |
| **Runtime** | Erlang/OTP 28, Elixir 1.19 |
| **Architecture** | `linux/amd64`, `linux/arm64` |
| **User** | `malachi` (UID 1000) |
| **Workdir** | `/app` |
| **JIT Compilation** | Enabled (`+JPperf true`) |
| **Runtime Dependencies** | `openssl`, `libstdc++`, `ncurses-libs` |

---

## Testing & Validation

The Docker image includes comprehensive testing scripts:

### Build Validation

```bash
# Validates runtime dependencies, JIT configuration, and performance benchmarks
make docker-validate
```

Checks:
- ✅ Runtime dependencies (argon2 NIF, openssl)
- ✅ ERL_FLAGS configuration with JIT
- ✅ Service availability (TCP + Dashboard)
- ✅ Performance benchmarks with throughput metrics
- ✅ Memory usage validation

### Regression Testing

```bash
# Runs comprehensive regression tests
make docker-regression-test
```

Tests include:
- HTTP/SSE endpoints functionality
- TCP server availability
- Queue publish/consume workflows
- High-volume message throughput (10K+ messages)
- Memory stability under load
- Concurrent multi-queue operations
- JIT compilation verification

### Full Test Suite

```bash
# Build + Validate + Regression tests
make docker-test-all
```

---

## Source Code

- **GitHub**: [https://github.com/HectorIFC/malachi](https://github.com/HectorIFC/malachi)
- **Issues**: [https://github.com/HectorIFC/malachi/issues](https://github.com/HectorIFC/malachi/issues)
- **Documentation**: [https://hectorifc.github.io/malachi](https://hectorifc.github.io/malachi)

---

## License

MIT License - see [LICENSE](https://github.com/HectorIFC/malachi/blob/main/LICENSE) for details.
