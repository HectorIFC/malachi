# Malachi

**An open-source, 100% Elixir reimplementation of LinkedIn's NorthGuard log-storage architecture: a
CP, horizontally scalable log broker.**

Clients speak topics, keys and opaque cursors, never partitions or offsets. That indirection is the
point: the broker can split a topic's storage and restripe it across nodes underneath a running
client without breaking it, because the client never held a coordinate that could go stale. The
control plane is replicated by quorum (Raft, via `ra`).

[Documentation](https://hectorifc.github.io/malachi/) &middot;
[Source](https://github.com/HectorIFC/malachi) &middot;
[Benchmarks and chaos results](https://hectorifc.github.io/malachi/loadtest-node-results.html)

---

## Quick start

```bash
docker run -d --name malachi \
  -p 4040:4040 -p 4041:4041 \
  -e MALACHI_ADMIN_PASS="a-long-local-only-password" \
  -e MALACHI_REQUIRE_TLS=false \
  hectorcardoso/malachi:latest
```

Then open the dashboard at [http://localhost:4041](http://localhost:4041), and check
`http://localhost:4041/health`, which should answer `{"status":"ok"}`.

Both of those flags are load-bearing, so it is worth saying why rather than leaving you to find out
from a crash loop.

The image runs a **production** release, and a production release refuses to start on a weak or
default password (`admin123` and friends are rejected outright, and anything under 12 characters is
too short). It also requires TLS unless told otherwise, which is why `MALACHI_REQUIRE_TLS=false` is
there: right for a container whose ports are on your own machine, wrong for anything reachable. For
that, drop the flag and point `MALACHI_TLS_CERTFILE` and `MALACHI_TLS_KEYFILE` at real certificates.

## Ports

| Port | What |
|------|------|
| `4040` | Binary wire protocol: produce, consume, stream |
| `4041` | Dashboard, `/health`, `/ready` and Prometheus `/metrics` |

## Tags

| Tag | Meaning |
|-----|---------|
| `latest` | Latest stable release |
| `X.Y.Z` | An exact version, for example `0.8.1` |
| `X.Y` | Latest patch of a minor line |
| `X` | Latest minor of a major line |
| `alpine` | Same image, named for its base |

Built for `linux/amd64` and `linux/arm64`, so Apple Silicon and Graviton pull a native image rather
than emulating one.

## Persistence

Nothing survives a restart unless you mount a volume and point the two data directories at it. Left
unset, they land under `/tmp`, which is where the benchmark setups deliberately put them and where
nothing keeps.

```bash
docker run -d --name malachi \
  -p 4040:4040 -p 4041:4041 \
  -v malachi-data:/app/data \
  -e MALACHI_LOG_DATA_DIR=/app/data/log \
  -e MALACHI_RA_DATA_DIR=/app/data/ra \
  -e MALACHI_ADMIN_PASS="a-long-local-only-password" \
  -e MALACHI_REQUIRE_TLS=false \
  hectorcardoso/malachi:latest
```

Both directories matter, and for different reasons. `MALACHI_LOG_DATA_DIR` holds the log segments,
the records themselves. `MALACHI_RA_DATA_DIR` holds the replicated control plane: topics, ranges,
and the user accounts, ACLs and lockouts. Persist only the first and the data survives while the
users who could read it do not.

Mount at `/app/data` specifically. A named volume is created owned by root, and Docker only hands it
the mount point's ownership when that directory already exists in the image. `/app/data` does; a
fresh `/data` would leave the broker unable to write into its own volume.

## Configuration

The full list is in the [documentation](https://hectorifc.github.io/malachi/). These are the ones
worth knowing before the first run:

| Variable | Default | What it does |
|----------|---------|--------------|
| `MALACHI_TCP_PORT` | `4040` | Wire protocol port |
| `MALACHI_DASHBOARD_PORT` | `4041` | Dashboard, health and metrics port |
| `MALACHI_ADMIN_PASS` | none | Admin password. Required; weak and default values are refused |
| `MALACHI_REQUIRE_TLS` | `true` in prod | Set `false` only for a local container |
| `MALACHI_TLS_CERTFILE` / `MALACHI_TLS_KEYFILE` | none | Certificate and key, required when TLS is on |
| `MALACHI_LOG_DATA_DIR` | under `/tmp` | Where log segments live |
| `MALACHI_RA_DATA_DIR` | under `/tmp` | Where the replicated control plane lives |
| `MALACHI_TRACING_ENABLED` | `false` | Turn OpenTelemetry sampling on |
| `MALACHI_OTLP_ENDPOINT` | `http://localhost:4318` | Where to ship spans |
| `MALACHI_LOCALE` | `en_US` | `en_US` or `pt_BR` |

## Watching it work

Malachi exports both halves of its own observability natively, and the repository ships a compose
stack that wires them up for you:

```bash
git clone https://github.com/HectorIFC/malachi.git && cd malachi
docker compose up -d
```

That gives you the broker plus **Jaeger** on port 16686 and **Prometheus** on 9090. A single produce
shows up in Jaeger as a distributed trace, `malachi.produce` at the root with the broker's append and
the quorum commit nested under it, and Prometheus carries around thirty `malachi_` series covering
throughput, authentication, replication and the integrity scrub.

For three nodes at replication factor 3 on durable volumes, with the same two:

```bash
docker compose -f docker-compose.cluster-durable.yml up -d
```

Tracing is off by default in every other setup, and deliberately: the sampler drops every span, so
the instrumentation on the produce path costs a function call that declines to record. Turn it on
when you need it, and use `MALACHI_TRACING_SAMPLE_RATIO` on anything busy.

## Metrics

`GET /metrics` on the dashboard port content-negotiates: ask for `text/plain` and you get the
Prometheus text exposition, ask for anything else and you get the dashboard's JSON. It requires an
authenticated user, and Malachi sessions expire, so a scraper needs a token that gets refreshed
rather than one pasted into a config. The repository's compose stack includes a small sidecar that
does exactly that, and it is the shortest working example to copy.

## Health

| Endpoint | Meaning |
|----------|---------|
| `/health` | The process is answering. Always `200`, unauthenticated |
| `/ready` | Ready to serve traffic. Unauthenticated |

```yaml
livenessProbe:  { httpGet: { path: /health, port: 4041 } }
readinessProbe: { httpGet: { path: /ready,  port: 4041 } }
```

Note that `/metrics` is authenticated and is not a health probe.

## License

MIT. Source, issues and the full documentation live at
[github.com/HectorIFC/malachi](https://github.com/HectorIFC/malachi).
