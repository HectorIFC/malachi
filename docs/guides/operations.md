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
- **`malachi_storage_integrity_failures_total{reason}`**: a stored segment failed checksum verification.
  Non-zero means data at rest is damaged on that node, and the condition is otherwise invisible: a
  damaged copy serves short reads with no error, so a consumer either stalls at the damaged offset or
  silently skips the rest of that segment. `reason="incomplete"` on an active segment is ordinary crash
  recovery (a partial tail that was never acked); on a sealed segment any reason means corruption at
  rest, and that copy needs to be rebuilt from an intact replica. The matching log line names the
  segment and the byte position.
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
MALACHI_REPLICATION_GROUP_COMMIT_INTERVAL_MS=10  # its own flush period, decoupled from the rf=1 one

# Active-segment roll size (bytes); unset keeps the 64MB default. Smaller segments seal (and become
# independently replicable/repairable units) sooner, at the cost of more metadata churn.
MALACHI_SEGMENT_MAX_BYTES=67108864
```

## Integrity scrub

Every node continuously re-verifies the data it stores. A sealed segment is immutable and nothing
re-reads it, so a checksum is otherwise only confirmed when a consumer happens to read that exact
record: bit rot there is silent, and silent in the worst way, because a damaged copy answers reads
with the records *before* the damage and nothing after, with no error. A consumer then stalls at
that offset or skips the rest of the range.

The scrub walks the node's own sealed segments, checks every record's checksum, and repairs a
damaged copy from a replica that still verifies:

```bash
MALACHI_SCRUB_ENABLED=true           # default; set to false to turn the scrub off entirely
MALACHI_SCRUB_INTERVAL_MS=60000      # time between passes
MALACHI_SCRUB_SEGMENTS_PER_TICK=1    # segments verified per pass
```

A full cycle takes `sealed segments on the node x interval / segments per tick`. With the defaults
and 64MB segments a node verifies about 90GB a day, so 10k sealed segments are revisited roughly
weekly, the usual period for disk scrubbing. Raise the interval on a slow or busy disk; lower it
(or raise the per-tick count) to cover a large dataset more often.

**What it costs.** The scan itself was measured directly at 740 MB/s (64-byte records) to 2.7 GB/s
(1KB records) on one core, so a 64MB segment costs 24 to 86ms of a core, and the shipped cadence
works out to under 0.15% of one core and about 1 MB/s of reads. End to end the effect is smaller
than a benchmark can resolve: on the 3-node Docker cluster (`benchmark/docker-scrub.sh`, which
interleaves the cases so ordering cannot bias them) both the default cadence and one three thousand
times faster landed inside the machine's run-to-run spread of roughly 15%. Re-run that sweep on
real hardware before raising the rate a lot, and note the honest caveat: those runs had the whole
dataset in page cache, so the scrub was reading RAM. On a node whose data dwarfs its memory the
scan is real disk I/O and competes with the write path.

**On detection**, the node asks the segment's other replicas to verify their own copies. Only when
one of them confirms an intact copy does the repair proceed: if this node is the segment's primary
it first moves itself to the end of the replica set, so reads go to an intact replica immediately,
and only then is the local copy deleted and refetched, then verified again. If **no** replica
verifies, nothing is deleted and the failure is logged loudly: a partially readable copy is worth
more than no copy. A repair is traced as `malachi.scrub.repair`.

Two series to watch, and they answer different questions.
`malachi_storage_integrity_failures_total{reason}` says whether anything is damaged, and
`malachi_storage_scrub_segments_total{result}` says whether the scrub is even running: a `verified`
total that stops advancing means the checking stopped, which the failure counter alone can never
tell you, since it reads zero both when all is well and when nothing is looking.

A single-node deployment scrubs too, and there the distinction matters more: with no replica there
is nothing to repair from, so every finding lands in the unrepairable path with a loud log. That is
still the difference between knowing and not knowing that data at rest went bad, and because
recovery no longer truncates a damaged sealed segment, the frames after the damage are still on
disk for a manual salvage.

## Chaos certification

`scripts/docker-chaos-test.sh` runs the certification drill on a local 3-node RF=3 Docker cluster:
synthetic traffic flows while a node is power-pulled (SIGKILL), partitioned off the network, stalled
(SIGSTOP, sockets open but mute), and finally every node is rolling-restarted. Three invariants must
hold or the script exits nonzero:

1. **No acknowledged write is ever lost.** A checker produces sequential values through the whole
   window, retrying through the faults, and records only the confirmed ones; at the end every one of
   them must read back (rf=3 quorum durability).
2. **The cluster reconverges** to 3/3 healthy after every event.
3. **Availability recovers**: errors during an event are expected, and a clean produce+fetch must
   pass once the chaos ends.

Run it before releases or after touching replication, failover, or membership code.

`scripts/docker-storage-chaos.sh` extends the drill to storage faults, injected with the target
node stopped: a follower's segment copy suffers a torn write (cut short with a garbage partial
frame appended: recovery clamps at the last CRC-valid frame and catch-up or the healing pass
repairs the tail), a gross truncation to half, and a sealed-segment directory deleted outright.
The deletion exercises the self-healing **integrity probe**: metadata still says the segment has
all its replicas, so only a physical check (on-disk bytes vs the sealed byte size, run each healing
pass) can spot the silent under-replication and re-backfill the copy. On top of the three
invariants above, the storage run requires **physical reconvergence**: every chaos-topic segment
file must end byte-identical across the three nodes. Damage always targets follower copies;
primary damage is seal-on-failure territory (roadmap), and in-place corruption that keeps the byte
size (bit rot) needs the CRC scrub pass (also roadmap). The run sets `MALACHI_SEGMENT_MAX_BYTES`
low so segments seal within the window; the same knob is available for any deployment that wants
smaller roll sizes.

`scripts/docker-config-chaos.sh` certifies config deployments, the way this repo deploys them (one
image, config via env): a harmless setting is rolled across the nodes one at a time, requiring the
checker's acks to keep flowing between every step and the new value to be effective on all three
nodes at the end; then a config that fails fast at boot is pushed to a single node, which must
crash-loop and never go healthy while the other two keep serving quorum writes, and rolling the
env back must bring it home to 3/3. The same closing invariants apply: no acknowledged write lost,
full reconvergence, clean produce+fetch after the chaos.

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
