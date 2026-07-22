# Docker Build Testing Guide

How to validate a Malachi Docker image: that it has the runtime it needs, boots, and serves the log
broker correctly. Two scripts back it, both driven by `make`.

## Overview

1. **Build validation** (`scripts/validate-docker-build.sh`) - runtime dependencies, JIT, and that the
   container starts and the dashboard authenticates.
2. **Regression testing** (`scripts/docker-regression-test.sh`) - the dashboard endpoints plus the log
   produce/fetch and consumer-group workflows, over a real container.

## Prerequisites

- Docker installed and running
- `make`
- `curl` and `nc` (netcat)
- `python3` (the scripts parse the login token with it)

## Quick start

```bash
make docker-test-all
```

Builds the image, runs build validation, then the regression suite.

## Build validation

```bash
make docker-validate
```

`validate-docker-build.sh` checks:

- **Runtime dependencies** present: the `argon2_nif.so` NIF and `openssl`.
- **JIT**: `:erlang.system_info(:emu_flavor)` reports `jit`.
- **Service comes up**: the container starts with seeded users (`MALACHI_DEFAULT_USERS`) and the dashboard
  authenticates and answers.

## Regression testing

```bash
make docker-regression-test
```

`docker-regression-test.sh` boots the image, logs in to the dashboard for a token, and runs:

| # | Test | Checks |
|---|------|--------|
| 1 | Dashboard HTTP endpoint | `GET /` returns 200 |
| 2 | Metrics endpoint returns JSON | `GET /metrics` contains `topics` |
| 3 | SSE stream endpoint | `GET /stream` streams |
| 4 | TCP server listening | port 4040 open |
| 5 | Container process health | `bin/malachi pid` |
| 6 | Log produce/fetch workflow | `create_topic`, `produce_records`, then `fetch` by opaque cursor |
| 7 | Consumer-group resume | `fetch_group`, `commit`, resume returns empty (at-least-once) |
| 8 | High-volume throughput | 1000 records in one produce |
| 9 | Memory stability under load | 5000 records, memory before/after |
| 10 | Concurrent multi-topic | 10 topics, 100 records each, all drained back |
| 11 | JIT compilation | `emu_flavor` is `jit` |

**Expected output:**

```
Testing: Dashboard HTTP endpoint... PASS
Testing: Metrics endpoint returns JSON... PASS
...
===================================
Regression Test Summary
===================================
Passed: 11
Failed: 0
===================================
All regression tests passed!
```

## Manual validation

### Build the image

```bash
make docker-build
```

### Verify runtime dependencies

```bash
docker run --rm --entrypoint /bin/sh hectorcardoso/malachi:latest -c "find /app/lib -name 'argon2_nif.so'"
```

### Check JIT

```bash
docker run --rm hectorcardoso/malachi:latest bin/malachi eval ':erlang.system_info(:emu_flavor)'
# Expected: jit
```

### Run the container

```bash
docker run -d --name test-malachi \
  -p 4040:4040 -p 4041:4041 \
  -e MALACHI_ADMIN_PASS="your_admin_password" \
  -e MALACHI_PRODUCER_PASS="your_producer_password" \
  -e MALACHI_CONSUMER_PASS="your_consumer_password" \
  -e MALACHI_APP_PASS="your_app_password" \
  hectorcardoso/malachi:latest
docker logs -f test-malachi
```

Expected startup logs (locale `en_US`):

```
🚀 Malachi TCP Server on port 4040 with 8 acceptors
✅ Metrics system started
🌐 Malachi Dashboard running at http://localhost:4041
```

### Exercise the log broker

The client protocol is binary, so do not hand-write frames. Either use the reference Node client in
[`scripts/`](https://github.com/HectorIFC/malachi/tree/main/scripts) (`producer.js`, `consumer.js`), or
drive the in-process API through `bin/malachi rpc`, which is what the regression script does:

```bash
docker exec test-malachi bin/malachi rpc '
  topic = "smoke"
  _ = Malachi.LogApi.create_topic(Malachi.LogBroker, topic)
  {:ok, 1} = Malachi.LogApi.produce_records(Malachi.LogBroker, topic, [%Malachi.Log.Record{key: "k", value: "hello"}])
  {:ok, records, _cursor} = Malachi.LogApi.fetch(Malachi.LogBroker, topic, :start, 10)
  IO.inspect(Enum.map(records, & &1.value))
'
```

## Troubleshooting

### argon2 NIF not found

Rebuild the image; the NIF is compiled from source during the build stage:

```bash
docker rmi hectorcardoso/malachi:latest
make docker-build
```

### Dashboard not responding

```bash
docker logs <container_name>
```

Common causes: a port conflict on 4040/4041, insufficient memory, or Docker daemon issues.

### JIT not enabled

JIT is unavailable on some platforms/emulation. Tests treat it as a skip, not a failure. Confirm the
platform with `docker run --rm hectorcardoso/malachi:latest uname -m`.

## CI/CD integration

```yaml
name: Docker Build & Test
on: [push, pull_request]
jobs:
  docker-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Build Docker image
        run: make docker-build
      - name: Run validation tests
        run: make docker-validate
      - name: Run regression tests
        run: make docker-regression-test
      - name: Cleanup
        if: always()
        run: docker system prune -f
```

## Script locations

- **Build validation:** `scripts/validate-docker-build.sh`
- **Regression tests:** `scripts/docker-regression-test.sh`
- **Make targets:** `Makefile` (`docker-validate`, `docker-regression-test`, `docker-test-all`)

For multi-architecture builds, see [Multi-arch builds](MULTI_ARCH_BUILD.md).

## Support

- GitHub Issues: https://github.com/HectorIFC/malachi/issues
- Documentation: https://hectorifc.github.io/malachi
