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
- `curl`, `nc` (netcat) and `timeout` (GNU coreutils; `brew install coreutils` on macOS)
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
| 3.5 | Static assets shipped in the image | `scripts/docker-static-assets-check.sh` (see below) |
| 4 | TCP server listening | port 4040 open |
| 5 | Container process health | `bin/malachi pid` |
| 6 | Log produce/fetch workflow | `create_topic`, `produce_records`, then `fetch` by opaque cursor |
| 7 | Consumer-group resume | `fetch_group`, `commit`, resume returns empty (at-least-once) |
| 8 | High-volume throughput | 1000 records in one produce |
| 9 | Memory stability under load | 5000 records, memory before/after |
| 10 | Concurrent multi-topic | 10 topics, 100 records each, all drained back |
| 11 | JIT compilation | `emu_flavor` is `jit` |
| 12 | Image HEALTHCHECK turns healthy | `scripts/docker-image-health-check.sh` (see below) |

**Expected output:**

```
Testing: Dashboard HTTP endpoint... PASS
Testing: Metrics endpoint returns JSON... PASS
...
===================================
Regression Test Summary
===================================
Passed: 13
Failed: 0
===================================
All regression tests passed!
```

## Static assets check

```bash
scripts/docker-static-assets-check.sh <container> <dashboard_url>
```

Run against a started container, it asserts that the image ships `priv/static` and nothing else from
`priv`:

- `GET /logo.svg` answers 200 with `Content-Type: image/svg+xml`, and its body is byte for byte the
  repository's `priv/static/logo.svg`. An image built without `priv/static` answers 404 here.
- The release's `priv` directory (`/app/lib/malachi-*/priv`) holds only `static`. A copy of `priv` as a
  whole would carry whatever the build context holds under `priv` into the image, which on a developer's
  checkout means gitignored development keys (`priv/dist_cert`, `priv/cert`) and dialyzer PLTs, and fails
  this.

Both calls are bounded, so a dashboard that stalls or a container that does not answer `exec` fails the
check instead of holding the run: `STATIC_ASSETS_HTTP_TIMEOUT` caps the logo request (seconds, default
10) and `STATIC_ASSETS_EXEC_TIMEOUT` caps the `priv` listing (default 15). A default applies only
when the variable is unset; one set but empty is refused like any other invalid value.

Each must be a whole number of seconds from 1 to 99999, written without leading zeros: both tools
read 0 as no limit at all, and curl rejects a much longer value outright. It exits 1 naming the check
that failed, and 2 on a usage error (a wrong number of arguments or an empty one, a timeout outside
that rule, or no coreutils `timeout` on `PATH`).

The Docker smoke test in CI (the `docker` job of `ci.yml`) runs it after planting decoy files under
`priv/dist_cert` and `priv/cert`, since a fresh checkout has neither and the second check would
otherwise pass against a copy of `priv` as a whole. `docker-regression-test.sh` runs it as test 3.5
and prints its reason on a failure. `test/scripts/docker_static_assets_check_test.exs` covers each
way it can fail without Docker; it needs coreutils `timeout`, so like the other `:linux` tests it runs
on Linux (CI included) and is excluded elsewhere.

## Image healthcheck check

```bash
scripts/docker-image-health-check.sh <container>
```

Run against a started container, it asserts that the image's own `HEALTHCHECK` turns the container
healthy. The image probes `http://127.0.0.1:4041/health`; it used to probe `localhost`, which resolves
to `::1` first inside the container while the dashboard listens on IPv4 only, so a node that served
fine reported itself unhealthy (#282). Compose files that override the probe hid that, so the check
first refuses a container whose probe is not the image's (an override, or `--no-healthcheck`), and an
image with no `HEALTHCHECK` at all.

It then waits for Docker to report the container healthy, within the image's start period plus one
interval plus one probe timeout (70s for the release image), read from the image itself. It fails at
once on `unhealthy` or a container that stops running. Those two failures, and a spent budget, also
print the output of the last probe Docker recorded (or say that no probe has run yet), when the probe
log can still be read; the other failures name only what went wrong.

- `IMAGE_HEALTH_TIMEOUT` (seconds) replaces the budget read from the image.
- `IMAGE_HEALTH_EXEC_TIMEOUT` (seconds, default 15) bounds each `docker inspect`.
- `IMAGE_HEALTH_POLL` (seconds, default 1) is the pause between two reads of the health status.

They follow the same rule as the static assets check: a whole number of seconds from 1 to 99999 without
leading zeros, and set but empty is refused. It exits 0 once healthy, 1 naming the check that failed,
and 2 on a usage error (a wrong number of arguments or an empty one, a limit outside that rule, or no
coreutils `timeout` on `PATH`).

The Docker smoke test in CI runs it last in the `docker` job of `ci.yml`, and
`docker-regression-test.sh` runs it as its last test, so its wait overlaps the checks before it.
`test/scripts/docker_image_health_check_test.exs` covers each outcome without Docker, on Linux only like
the other `:linux` tests.

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
- **Image checks:** `scripts/docker-static-assets-check.sh`, `scripts/docker-image-health-check.sh` (shared helpers in `scripts/docker_check_lib.sh`)
- **Make targets:** `Makefile` (`docker-validate`, `docker-regression-test`, `docker-test-all`)

For multi-architecture builds, see [Multi-arch builds](MULTI_ARCH_BUILD.md).

## Support

- GitHub Issues: https://github.com/HectorIFC/malachi/issues
- Documentation: https://hectorifc.github.io/malachi
