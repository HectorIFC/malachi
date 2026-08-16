# Operations

Running Malachi: the ports it opens, how to scrape it, what to watch, and the settings that must not be
left at their development values.

## Ports

| port | env | serves |
|---|---|---|
| 4040 | `MALACHI_TCP_PORT` | the binary log protocol, all client traffic |
| 4041 | `MALACHI_DASHBOARD_PORT` | dashboard, health checks, metrics |

Only 4040 needs to be reachable by clients. Treat 4041 as an internal port: it exposes operational detail
and user management.

## Health checks

Two endpoints, and the difference matters for orchestrators:

```
GET /health   the process is up
GET /ready    it is ready to serve
```

Use `/health` for a liveness probe and `/ready` for readiness. Wiring readiness to `/health` means traffic
arrives before the node can serve it.

## Metrics

`/metrics` does content negotiation. Ask for `text/plain` and you get the **Prometheus text exposition
format**, every series namespaced `malachi_`:

```bash
curl -H 'Accept: text/plain' http://localhost:4041/metrics
```

Without that header the same endpoint returns the JSON payload the dashboard uses.

Worth alerting on:

- **`malachi_domain_violations`**: segments that cannot meet their placement spread requirement. Non-zero
  means replicas are concentrating where they should not be, which is a real availability risk that is
  otherwise silent.
- Session and auth counters. Note that `:session_expired` and `:session_hijack_attempt` are **not
  disjoint**: one validation can emit both, so summing them does not count failed validations. The hijack
  counter means "a token arrived from an unexpected IP", which ordinary NAT rotation can also trigger, so
  set thresholds against that broader meaning. See `Malachi.Auth.SessionManager`.

## Durability tuning (group commit)

Every produce is fsynced before its ack; group commit coalesces those fsyncs. Two independent knobs,
one per path (the [clustering guide](clustering-and-resharding.md#durability-and-group-commit) explains
the decision rule with examples):

```bash
# rf=1 (single node), broker-level. Recommended on for throughput workloads.
MALACHI_GROUP_COMMIT=true
MALACHI_GROUP_COMMIT_INTERVAL_MS=5           # flush period; ~the latency each produce pays
MALACHI_GROUP_COMMIT_FLUSH_MAX_RECORDS=8000  # eager flush: bound each fsync even on slow disks
MALACHI_GROUP_COMMIT_MAX_INFLIGHT=200000     # backpressure valve: shed with :overloaded past this

# rf>1 (replicated), replication-level. Default OFF: enable only for hot-range, fsync-bound
# workloads (many producers per range); on thin-spread loads it lowers throughput.
MALACHI_REPLICATION_GROUP_COMMIT=false
```

## Retention

Segments are reclaimed by age or total size:

```bash
MALACHI_RETENTION_MAX_AGE_MS=604800000     # 7 days
MALACHI_RETENTION_MAX_BYTES=10737418240    # 10 GiB per range
MALACHI_RETENTION_INTERVAL_MS=60000
```

**Leave a limit unset to disable it.** With both unset, segments are kept forever and no retention
coordinator starts at all. Do not write `0` meaning "unlimited": `0` is a valid budget of zero bytes, and
it expires every sealed segment it can.

Only **sealed** segments are eligible, so the active segment is never deleted. The byte budget is **per
range**, not per topic or per node. With both limits set a segment goes if either says so.

## TLS

```bash
MALACHI_ENABLE_TLS=true
MALACHI_REQUIRE_TLS=true
MALACHI_TLS_CERTFILE=/etc/malachi/server.pem
MALACHI_TLS_KEYFILE=/etc/malachi/server-key.pem
MALACHI_TLS_CACERTFILE=/etc/malachi/ca.pem
MALACHI_TLS_VERIFY=verify_peer
MALACHI_TLS_VERSIONS=tlsv1.3,tlsv1.2
```

`MALACHI_ENABLE_TLS` offers TLS; `MALACHI_REQUIRE_TLS` refuses plaintext.

**In production both are on unless you turn them off.** `REQUIRE_TLS` defaults to true under
`MIX_ENV=prod`, and `ENABLE_TLS` simply follows it. So the risk here is not forgetting to enable TLS, it
is the opposite: setting `MALACHI_REQUIRE_TLS=false` to get past a certificate problem and leaving it
that way, which disables both at once. Outside production both default to off.

Invalid TLS configuration **raises at boot** in production rather than starting insecurely; in dev and
test it only warns.

## Before you go to production

The checks that catch the common mistakes:

- [ ] **Passwords set explicitly.** Production requires `MALACHI_ADMIN_PASS` and friends via environment;
      no credentials ship in the base config. The dev defaults (`admin123`) exist only in `dev.exs` and
      `test.exs`.
- [ ] **`MALACHI_REQUIRE_TLS` not set to `false`.** It defaults to true in production, so the check is
      that nobody disabled it while debugging certificates.
- [ ] **Dashboard not publicly reachable**, and `MALACHI_DASHBOARD_REQUIRE_ADMIN` on.
- [ ] **`MALACHI_LOG_REPLICATION_FACTOR` at least 3** if you want to survive a node loss. With 2, quorum
      is 2, so losing either replica stalls writes.
- [ ] **Retention configured.** The default keeps everything, and the disk fills quietly.
- [ ] **Readiness probe on `/ready`**, not `/health`.
- [ ] **`malachi_domain_violations` alerted on.**
- [ ] If you use ACLs, **`MALACHI_ACL_STRICT=true`**. Without it grants are inert and global permissions
      still allow everything. See [Per-topic ACLs](per-topic-acls.md).

## Docker

Images are published multi-arch. See [Running with Docker](../DOCKER_README.md) for compose files and
the environment matrix.

## Next

For growing the cluster, see [Clustering and re-sharding](clustering-and-resharding.md).
