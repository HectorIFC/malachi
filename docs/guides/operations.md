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
- **`malachi_storage_flush_duration_seconds`**: the group-commit flush latency, as a histogram
  (`_bucket{le}` four per octave from 8us to about 16.8s, plus `_sum` and `_count`). This is the write
  plus sync every acknowledged produce waits behind, so it is the first place to look when produce
  throughput drops without CPU rising: an fsync-bound disk shows up here as a rising p99 before anything
  else moves. Read it windowed, `histogram_quantile(0.99, rate(malachi_storage_flush_duration_seconds_bucket[5m]))`,
  and sum the buckets across nodes before taking the quantile for a cluster-wide view. Only flushes that
  wrote records and succeeded are counted; a failed one is in `malachi_storage_failures_total`.
  `malachi_storage_flushed_records_total` divided by `malachi_storage_flush_duration_seconds_count` is
  records per sync, which is how well group commit is coalescing (near 1 means it is not), and
  `malachi_storage_flushed_bytes_total` over that same count is the average flush size.
  `malachi_storage_flush_duration_seconds_created` is the Unix time the histogram began: it changes only
  when the node restarts, so a tool subtracting two scrapes can tell a restart from counters that simply
  kept growing.
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

# The produce path has TWO refusals, and a client is meant to tell them apart. :overloaded is the valve
# above, the broker saying it is saturated right now; :rate_limited is the OPT-IN publish quota below,
# this user saying it is over its own allowance. Off by default (0 = no limit); see docs/RATE_LIMITING.md.
MALACHI_PUBLISH_RATE_LIMIT=0                 # produce requests per window, per user, PER NODE
MALACHI_PUBLISH_RATE_WINDOW_MS=1000

# rf>1 (replicated), replication-level. Default OFF: enable only for hot-range, fsync-bound
# workloads (many producers per range); on thin-spread loads it lowers throughput.
MALACHI_REPLICATION_GROUP_COMMIT=false
MALACHI_REPLICATION_GROUP_COMMIT_INTERVAL_MS=10  # its own flush period, decoupled from the rf=1 one

# The replication server's minimum heap (words). Every produce batch is encoded and written in that one
# process, and at the VM's default heap it garbage-collects several times per batch; 256K words (2MB per
# node) halves that. Raise it only if a profile shows the server collecting per batch again, e.g. with much
# larger batches; 0 restores the VM default.
MALACHI_REPLICATION_MIN_HEAP_WORDS=256000

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
one of them confirms a copy that is both intact **and** complete does the repair proceed: if this
node is the segment's primary it first moves itself to the end of the replica set, so reads go to an
intact replica immediately, and only then is the local copy deleted and refetched, then verified
again. Complete matters as much as intact, because a replica whose checksums all pass can still be
missing whole records at the end, and repairing from one would delete this copy to refetch less than
it held. So a replica qualifies only when its scan matches the record and byte counts the control
plane recorded when the segment was sealed. If **no** replica qualifies, nothing is deleted and the
failure is logged loudly: a partially readable copy is worth more than no copy. A repair is traced
as `malachi.scrub.repair`.

Two series to watch, and they answer different questions.
`malachi_storage_integrity_failures_total{reason}` says whether anything is damaged, and
`malachi_storage_scrub_segments_total{result}` says whether the scrub is even running: a `verified`
total that stops advancing means the checking stopped, which the failure counter alone can never
tell you, since it reads zero both when all is well and when nothing is looking. Within that second
series, `result="unrepairable"` is the one worth paging on: it counts damage the cluster could not
heal by itself, so unlike `result="repaired"` it does not go away on its own.

Note what the failure counter deliberately does **not** include: a torn frame at the end of an
active segment after a crash. Those bytes were never acknowledged, so recovering past them is
routine, and counting it would make an ordinary restart look like corruption at rest. It is still
logged. Rot in an active segment, and anything at all in a sealed one, is counted.

A single-node deployment scrubs too, and there the distinction matters more: with no replica there
is nothing to repair from, so every finding lands in the unrepairable path with a loud log. That is
still the difference between knowing and not knowing that data at rest went bad, and because
recovery never truncates a copy damaged by rot (a bad checksum or a mangled frame header), the
frames after the damage are still on disk for a manual salvage.

Be precise about which damage that covers, because the two shapes are not treated alike. Rot is left
alone whatever the file's state. A torn tail is dropped unless the file carries its own seal marker,
and a replica usually does not: that marker is written when the segment's log rolls locally, not
when the control plane seals the segment. That asymmetry is deliberate rather than an oversight,
since a torn tail has nothing valid after it by construction, so there is nothing to salvage. Rot is
the case where the bytes past the damage are still worth keeping.

That holds for an **active** segment too. A node that restarts onto rot in the segment it is still
writing keeps the damaged frame and everything behind it, and never appends over them: the copy is
taken out of service as a storage failure (`malachi_storage_failures_total{reason="other"}`, and a
`failed in storage` log line naming the segment and `damaged_tail`). What happens next depends on the
replication factor:

- **RF 3 or more.** The heal pass seals the segment on the intact copies at the furthest end they hold,
  and writing rolls to a new segment, which is what NorthGuard does when a replica of a segment fails.
  Producers see nothing. The damaged copy is replaced by self-healing.
- **RF 2 and RF 1.** Sealing needs a majority of intact copies to answer, and one copy is gone, so the
  range stops taking writes until an operator acts. That is deliberate: the alternative was writing new
  records over ones that may have been acknowledged. Rolling a single-copy segment on without that
  risk is tracked in [#210](https://github.com/HectorIFC/malachi/issues/210).

To recover a blocked range by hand, on the node that reported the damage:

1. Stop the node and copy the segment's `.log` file somewhere safe. The frames after the damage are
   still in it.
2. Either truncate the file at the `position` the integrity log line reported, accepting the loss of
   everything from there on, or, with a replica elsewhere, remove this node's copy of the segment
   directory and let the cluster re-replicate it.
3. Start the node again.

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
invariants above, the storage run requires **physical reconvergence**: every node's copy of every
chaos-topic segment must be readable and hold the same records, byte for byte over the valid part of
its files, and a sealed segment exactly its sealed length. The files themselves need not match: only
a fenced copy has its preallocated tail trimmed, and each node rolls its internal files at its own
sync points. Corruption always targets follower copies;
corruption of a primary copy is seal-on-failure territory (roadmap). A storage FAILURE is not: when a
write or read fails on a node (a full volume, a failing device), that node stops using the segment's
copy, answers `{:error, {:storage, reason}}` for it, counts it in `malachi_storage_failures_total`, and
the healing pass seals the segment on its other replicas so producers move to a new one. From then on
the failed copy counts as a lost replica: the healing pass backfills a replacement on another broker and
deletes the broken copy (which lets that node use the segment id again), and the scrub skips it meanwhile
instead of refetching it onto the disk that failed. With no spare broker the copy stays, behind the
healthy replicas, and the pass reports the segment as `{:no_spare_broker, copies}`. The drill's
last event fills a node's volume to certify it. In-place corruption that keeps the byte size
(bit rot) and a rotted sparse index are covered too, by the integrity scrub described above. The run
sets `MALACHI_SEGMENT_MAX_BYTES` and `MALACHI_LOG_ROLL_MAX_BYTES` low so segments seal and roll
within the window; both knobs are available to any deployment that wants smaller roll sizes.

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

### Retention metrics

Retention is observable from both sides: what the sweep deleted, and which readers were moved past data
that was no longer there.

The sweep (only the node that leads a vnode sweeps it, so read these summed across nodes):

- **`malachi_retention_sweep_duration_seconds`**: a histogram of sweep durations, with the same buckets as
  the flush histogram. Its `_count` is the number of sweeps this node ran. **A `_count` that stops
  advancing on every node means no sweep is running**, the question an operator cannot answer otherwise.
- **`malachi_retention_segments_expired_total{topic}`** and **`malachi_retention_bytes_expired_total{topic}`**:
  what the sweep deleted.
- **`malachi_retention_expire_failures_total{reply}`**: deletes the control plane refused, by reply:
  `migrating` (the topic was moving between vnodes), `segment_active` (still the write head) or `other`
  (anything else, a Raft timeout included). A segment the sweep found already gone is neither expired
  again nor a failure.

The readers:

- **`malachi_retention_skips_total{topic,reader,group,origin,span}`**: how many times a reader was moved
  past data no longer stored (expired by retention, or deleted by an operator). Each distinct skip is
  counted once, however often a group re-reads it before committing. `reader` says which kind of reader
  it was: `group` names it in `group`, `none` is a fetch outside a group and `other` a group folded by
  the label cap, both with an empty `group`. Nothing reserves a group name, so `reader` is what keeps a
  group that calls itself `__other__` or `""` apart from those two buckets. **`origin="cursor"` is the
  one to alert on**: a group that held a position and fell behind retention. `origin="start"` is a
  reader that had no position, a new group or a range's children after a split, which start over the
  range's history; retention reaching them is expected.
- **`malachi_retention_offsets_skipped_total{topic,reader,group,origin,span}`**: how many offsets those skips
  stepped over. `span="exact"` is exact. `span="upper_bound"` is a skip over the ancestor of a split
  range: the count covers the ancestor's whole range, of which this reader would only have received its
  own key slice. `span="unknown"` is an ancestor with nothing left stored whose end this node could not
  recover after a restart, so only the fact of the skip is known and the offsets are 0. It counts
  **offsets, not records lost**.

A suggested alert: `increase(malachi_retention_skips_total{origin="cursor"}[15m]) > 0`, which names the
group that lost data. The first skip of a reader is also logged, then at most once per reader per
`MALACHI_RETENTION_SKIP_LOG_WINDOW_MS` (10 minutes), with the count of skips held back since the last
line.

`group` is a label a client chooses, so it is capped: past `MALACHI_RETENTION_METRICS_MAX_GROUPS` (1000)
topic and group pairs on a node, new groups are folded into `reader="other"` with no name, and the log
line still names them. The sum over readers per topic stays exact. The skip reporter's memory is bounded the same
way by `MALACHI_RETENTION_SKIP_LEDGER_MAX` (10000 skips remembered); a forgotten skip read again is
counted again.

### The orphan sweep

A replica that does not answer its delete keeps the segment's directory, and no later sweep can ask for
it again: a segment gone from the control plane never comes back. A separate worker per node reclaims
those directories, and other leaks of the same shape (a catch-up that failed after creating the
directory, a copy healing moved elsewhere).

```bash
MALACHI_RETENTION_ORPHAN_SWEEP=delete         # delete | report | off
MALACHI_RETENTION_ORPHAN_SWEEP_INTERVAL_MS=300000
MALACHI_RETENTION_ORPHAN_MIN_AGE_MS=600000
MALACHI_RETENTION_ORPHAN_SIGHTINGS=2
MALACHI_RETENTION_ORPHAN_MAX_PER_PASS=50
```

It runs on **every node**, not only the one that sweeps retention: only a node can read its own disk.
It acts only when this node has read every metadata vnode at least once since boot, and a directory is
removed only after it has been unexplained on `SIGHTINGS` consecutive passes **and** is older than
`MIN_AGE_MS`. That minimum has to stay above the worst registration lag: a replica creates a
directory on the first push, which can happen before this node's metadata shows the registration.

`report` does everything except the removal, which is how to see the list before trusting it on a
cluster for the first time. `off` does not even list.

- **`malachi_retention_orphan_directories_left_total{topic}`**: directories an expire left behind
  because the replica did not answer. It moves whether or not the sweep is on.
- **`malachi_retention_orphan_directories_removed_total`**: directories the sweep reclaimed. Read
  against the one above: a gap that keeps growing means the sweep is off, is being held back by a
  guard, or is not keeping up with `MAX_PER_PASS`.

These names are reserved for later retention work and not emitted yet:
`malachi_retention_segments_pinned{topic,group}` (consumer-aware retention) and
`malachi_segment_rolls_total{reason}` (time-based rolls).

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

## Upgrades and the rollback floor

An upgrade has two floors, and a rollback has to clear both: the **on-disk format** below, and the
**control-plane machine version** further down. They move independently, so check them separately
before rolling a release back.

### The on-disk format

The root of the log data directory (`MALACHI_LOG_DATA_DIR`) holds a small text file, `malachi.format`:

```
format=1
written_by=0.12.0
requires=0.12.0
```

`format` is the on-disk format the directory holds, `written_by` the release that wrote the file, and
`requires` the oldest release that can read that format. With several data-plane shards, the one file at
the root covers every `shard_<n>` subdirectory.

**The rule: a release starts on a data directory only when `format` is at or below the highest format it
understands.** Otherwise it refuses to start before opening anything, because an older release does not
know a newer frame, would read it as damage, and could write over records that were acknowledged.

What that means for a rollback:

- A directory without the file (a fresh node, or one last written by a release older than 0.12.0) gets
  one at baseline format 1 on the first start. Starting a newer release does **not** raise it, whatever
  format that release can read.
- The format rises only when a format-changing feature is switched on for the cluster. Until then, a
  rollback to any release from 0.12.0 on is free.
- After a format-changing feature is switched on, the floor is the release named in `requires`. Rolling
  back below it is refused.
- **Never roll back below 0.12.0 once any format above 1 exists.** Releases before 0.12.0 do not read the
  file at all, so nothing stops them from opening newer data, and that is exactly the overwrite this file
  exists to prevent.
- **Do not delete or edit the file to get a node to start.** It is the only thing between an older
  release and data it cannot read.

A refused start exits with status **78** and logs one line that begins with `REFUSING TO START (exit 78):`
and names the directory, both format levels and the release to start instead. The same line is printed
on stderr, so it reaches container logs even when the logger has not flushed. The same status covers a
marker that cannot be parsed (restore it from a backup or another node) and one that cannot be read or
written (fix the volume or its permissions).

A service manager that restarts on failure will restart a refused node again and again. Tell it not to:

- **systemd:** `RestartPreventExitStatus=78`.
- **Docker Compose:** `restart: on-failure` restarts on any non-zero status, so bound it
  (`restart: on-failure:5`) and alert on exit code 78 (`docker inspect -f '{{.State.ExitCode}}'`).
- **Kubernetes:** the pod goes to `CrashLoopBackOff`; the last terminated state shows exit code 78 and
  the log line above.

### The control-plane machine version

The control plane (topic metadata, the lease, the ring, users, lockouts, ACLs and storage policies) lives in Raft groups
whose state machines carry a version. A group moves to a new version only once **every** member runs code
that supports it, and a command a release introduces is refused, the same way on every member, until then.
So a cluster in the middle of a rolling upgrade cannot end up with members that disagree about its state.

The version switch happens on its own when the last node has been upgraded. From that moment, a node started
on the previous build stops applying the control-plane log instead of diverging from it: it stays up, but it
falls behind, and it logs `stopped applying entries` and emits the `[:malachi, :ra, :machine_version]`
telemetry event with `stuck: true` until it is upgraded again. That is this floor: it moves with the
machine version, not with the on-disk format above.

To keep rolling back possible until you are satisfied with a release, hold the version with a pin:

1. Set `MALACHI_RA_MACHINE_VERSION` on every node to the version the cluster runs now, and roll the new build
   out node by node. The group stays at the pinned version, and any node can go back to the previous build.
2. When the release is proven, **finalize**: remove `MALACHI_RA_MACHINE_VERSION` and restart the nodes one at
   a time. The groups switch to the new version once the last node is back, and the rollback floor moves up.

The first release that versions these machines is version 1, and every earlier build counts as version 0.
Version 2 adds the storage policy store, whose commands a version 1 member refuses until the whole group
has moved. To be able to roll back from a release to the build before it, upgrade with
`MALACHI_RA_MACHINE_VERSION` set to the version the cluster runs now and finalize later.

A malformed value (anything but a non-negative integer) stops the node at boot. Silently dropping the pin would
finalize the upgrade, so it is treated as an error. A pin above the version a build implements has no effect.

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
- [ ] **You know your rollback floor.** Read [Upgrades and the rollback floor](#upgrades-and-the-rollback-floor)
      before the first upgrade, and make your service manager stop restarting on exit status 78.
- [ ] **`malachi_domain_violations` alerted on.**
- [ ] If you use ACLs, **`MALACHI_ACL_STRICT=true`**. Without it grants are inert and global permissions
      still allow everything. See [Per-topic ACLs](per-topic-acls.md).

## Docker

Images are published multi-arch. See [Running with Docker](../DOCKER_README.md) for compose files and
the environment matrix.

## Next

For growing the cluster, see [Clustering and re-sharding](clustering-and-resharding.md).
