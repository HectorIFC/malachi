# MalachiMQ

[![GitHub](https://img.shields.io/github/v/release/HectorIFC/malachimq?label=GitHub)](https://github.com/HectorIFC/malachimq)
[![Docker Pulls](https://img.shields.io/docker/pulls/hectorcardoso/malachimq)](https://hub.docker.com/r/hectorcardoso/malachimq)
[![Docker Image Size](https://img.shields.io/docker/image-size/hectorcardoso/malachimq/latest)](https://hub.docker.com/r/hectorcardoso/malachimq)
[![License](https://img.shields.io/github/license/HectorIFC/malachimq)](https://github.com/HectorIFC/malachimq/blob/main/LICENSE)

**MalachiMQ** is a High-performance message system, designed for low-latency, high-throughput messaging with automatic queue partitioning across CPU cores.

## Features

- 🚀 **High Performance** - ETS-based storage with automatic partitioning
- 🔐 **TLS/SSL Support** - Secure connections with certificate-based auth
- 📊 **Real-time Dashboard** - Built-in web UI with live metrics
- 🔄 **Acknowledgment System** - Reliable message delivery with ack/nack
- 🌐 **Multi-language Support** - i18n support (en_US, pt_BR)
- 🐳 **Production Ready** - Alpine-based image for minimal attack surface
- 🏗️ **Multi-Architecture** - Supports AMD64 and ARM64 (Apple Silicon, AWS Graviton)

---

## Quick Start

### Pull the image

```bash
docker pull hectorcardoso/malachimq:latest
```

### Run with default settings

```bash
docker run \
  --name malachimq \
  -p 4040:4040 \
  -p 4041:4041 \
  -e MALACHIMQ_ADMIN_PASS="your_secure_password" \
  hectorcardoso/malachimq:latest
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
| `MALACHIMQ_TCP_PORT` | `4040` | TCP server port for clients |
| `MALACHIMQ_DASHBOARD_PORT` | `4041` | HTTP dashboard port |
| `MALACHIMQ_LOCALE` | `en_US` | Language (`en_US`, `pt_BR`) |
| `MALACHIMQ_ENABLE_TLS` | `false` | Enable TLS encryption |
| `MALACHIMQ_PARTITION_MULTIPLIER` | `100` | Partitions per CPU core |

### TLS Configuration

```bash
docker run \
  --name malachimq \
  -p 4040:4040 \
  -p 4041:4041 \
  -e MALACHIMQ_ADMIN_PASS="your_secure_password" \
  -e MALACHIMQ_ENABLE_TLS=true \
  -v /path/to/certs:/app/priv/cert:ro \
  hectorcardoso/malachimq:latest
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
  malachimq:
    image: hectorcardoso/malachimq:latest
    container_name: malachimq
    ports:
      - "4040:4040"  # TCP server
      - "4041:4041"  # Dashboard
    environment:
      - MALACHIMQ_LOCALE=en_US
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:4041/metrics"]
      interval: 30s
      timeout: 10s
      retries: 3
```

## Large-scale channel broadcast runtime variables

The runtime can be tuned for high-concurrency channel broadcasts using environment variables. Defaults are chosen to make bulk publishing available in container images while remaining conservative for typical hosts.

Recommended environment variables (defaults shown):

- `MALACHIMQ_CHANNEL_SEND_CONCURRENCY` (default: `5000`)
  - Maximum parallel sends performed by the channel broadcast. Increase for faster bulk deliveries on large machines.

- `MALACHIMQ_CHANNEL_SEND_TASK_TIMEOUT_MS` (default: `5000`)
  - Per-send task timeout in milliseconds. Tune to the characteristics of your environment.

- `MALACHIMQ_SHARD_COUNT` (default: `1000`)
  - Number of shard processes used by the sharded integration test. Each shard represents `1_000_000 / SHARD_COUNT` logical subscribers when using the provided test helper.

- `ERL_FLAGS` (example: `+P 2097152`)
  - Pass Erlang VM flags like `+P` to increase process limit when running with many real processes.

Example `docker-compose.yml` snippet:

```yaml
services:
  malachimq:
    image: hectorcardoso/malachimq:latest
    environment:
      - MALACHIMQ_CHANNEL_SEND_CONCURRENCY=5000
      - MALACHIMQ_CHANNEL_SEND_TASK_TIMEOUT_MS=5000
      - MALACHIMQ_SHARD_COUNT=1000
      - ERL_FLAGS=+P 2097152
```

Notes:
- Increasing `+P` only raises the process limit — ensure the host has sufficient RAM.
- These defaults are safe for test and development; tune conservatively for production.

---

## TCP Protocol

MalachiMQ uses a JSON-over-TCP protocol. All messages are newline-delimited.

### Authentication

```json
{"action": "auth", "username": "producer", "password": "producer123"}
```

### Publish a message

```json
{"action": "publish", "queue_name": "orders", "payload": "{\"order_id\": 123}"}
```

### Subscribe to a queue

```json
{"action": "subscribe", "queue_name": "orders"}
```

---

## Default Users

| Username | Password | Permissions |
|----------|----------|-------------|
| `admin` | `admin123` | Full access |
| `producer` | `producer123` | Publish only |
| `consumer` | `consumer123` | Consume only |

> ⚠️ **Security Note**: Change default credentials in production using `MALACHIMQ_DEFAULT_USERS` environment variable.

---

## Health Check

The dashboard exposes a `/metrics` endpoint for health checks:

```bash
curl http://localhost:4041/metrics
```

Returns JSON with queue statistics and system metrics.

---

## Image Details

| Property | Value |
|----------|-------|
| **Base Image** | `alpine:3.21` |
| **Runtime** | Erlang/OTP 28, Elixir 1.19 |
| **Architecture** | `linux/amd64`, `linux/arm64` |
| **User** | `malachimq` (UID 1000) |
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

- **GitHub**: [https://github.com/HectorIFC/malachimq](https://github.com/HectorIFC/malachimq)
- **Issues**: [https://github.com/HectorIFC/malachimq/issues](https://github.com/HectorIFC/malachimq/issues)
- **Documentation**: [https://hectorifc.github.io/malachimq](https://hectorifc.github.io/malachimq)

---

## License

MIT License - see [LICENSE](https://github.com/HectorIFC/malachimq/blob/main/LICENSE) for details.
