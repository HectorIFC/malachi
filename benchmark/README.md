# Benchmarks

All benchmarks for the current log-broker architecture live here. The former MalachiMQ
performance suite (queue throughput/latency/memory baselines, overflow strategies,
blocked-producer fairness, and their CI/hook machinery) was removed with the queue
model it measured.

Each script is a standalone `mix run` script (it boots the project's deps, and most do
not touch `lib/`). Results, where written, go to `results/` (gitignored except `.gitkeep`).

> **Note on zero-copy:** every throughput number in this suite was measured WITHOUT the zero-copy
> consume optimization. `:file.sendfile` on the fetch path is a future optimization, not implemented:
> the wire ships records in a compact offset-less encoding distinct from the on-disk frame (see
> `docs/ARCHITECTURE.md`), so consumes read, decode, and re-encode through the BEAM, and sendfile
> would first require aligning the fetch encoding with the on-disk frame. Produce numbers are
> unaffected (zero-copy only applies to reads); consume/fetch/stream numbers have headroom once it
> lands.

## Which harnesses measure the durable path

Every acknowledged produce waits on a write and an fsync, and the options that make that cheaper
(group commit, segment preallocation) exist because the fsync is not free. On tmpfs it is: there is no
journal to commit and no allocation to give up, so a harness whose data lives on tmpfs measures the
produce path with the durable part costing nothing. That is deliberate where it appears (fsync cost is
then identical from run to run), but its numbers say nothing about durability. Group commit below is
what the broker runs, which is off above RF 1 whatever the setting says.

| Harness | Where the data lives | Durable path (a real fsync) | Preallocation | Group commit |
|---|---|---|---|---|
| `docker-cluster.sh` | each node's 1g tmpfs | no | off | on at RF 1, off at RF 3 |
| `docker-cluster.sh` with `REAL_DISK=1` | each node's named volume | yes | 64MB | on at RF 1, off at RF 3 |
| `docker-compare.sh`, `docker-pipeline.sh`, `docker-shards.sh` (`docker-compose.bench.yml`) | the node's 1g tmpfs | no | off | on |
| `docker-ratelimit.sh` | each node's 1g tmpfs | no | off | on (RF 1 by default) |
| `docker-scrub.sh` | each node's named volume | yes | off | on (RF 1 by default) |
| ceiling harness (`scripts/loadtest-ceiling.sh`) | host filesystem (ext4 on the CI runner) | yes | 64MB | off |
| `storage_viability.exs` | host filesystem (ext4 on the CI runner) | yes | per arm | not involved (the store alone) |
| `throughput_1m.exs`, `single_node_scale.exs` | `BENCH_DIR` on the host (the system temp dir by default; tmpfs and ramfs refused) | yes | off | off: one sync per produce |

Numbers from the host filesystem or a named volume are only comparable when the filesystem and the disk
under it are the same, so the harnesses that report them say which they ran on. Only Linux counts: on
Docker Desktop a named volume sits inside the VM's disk image, and on macOS `:file.sync` does not reach
stable media.

### docker-cluster.sh

The 3-node cluster produce benchmark, at RF 1 and RF 3, on a fresh cluster with its volumes removed for
every case. It runs in one of two data modes per invocation:

```bash
benchmark/docker-cluster.sh                                          # tmpfs, the run-to-run comparable mode
REAL_DISK=1 OUT=results/docker-cluster.jsonl benchmark/docker-cluster.sh   # the durable path
```

- **Real disk.** `REAL_DISK=1` puts each node's data on its named volume, with preallocation at the
  production 64MB. A case fails unless the volume is a real filesystem (not tmpfs) and the preallocated
  bytes are on it afterward, because the store quietly falls back to an unpreallocated segment when
  preallocation fails. A case whose host disk cannot hold TOPICS x RF x 64MB (plus a 1GB margin) is
  refused before it runs.
- **Segment creation stays out of the window.** Every topic's segment is created during setup
  (`--prepopulate`, one batch per topic), in both modes, so the 64MB preallocation (about 249ms, see
  below) is not paid inside the warmup or the measured window.
- **RF 3 is the case most likely to struggle.** Group commit is off there and every batch waits on a
  quorum fsync, which is free on tmpfs and paid in full on a disk. Generator errors are reported as a
  lower number (`err=N`), not as a failure. A generator that runs past `CASE_TIMEOUT` (300s) is killed
  and reported as a timeout, and the next case still runs.
- **Where the numbers came from.** The output (and each `OUT` line) records the host, the Docker
  engine, what backs the Docker root (filesystem, mount options, disk), and each node's view of its own
  data mount.
- **Per-flush latency is the servers' own.** Each node's `/metrics` is scraped from inside the node
  (busybox `wget` on `127.0.0.1`, since the compose file publishes no port) when the generator's
  measured window opens and again when the generator exits, and `mix malachi.loadtest.ceiling
  flush-window` subtracts the two, so setup, prepopulate and warmup flushes are left out. Each `OUT` line
  carries `flush.nodes` (per node) and `flush.all` (every node's flushes added up into one
  distribution), p50/p99/p999 and mean in seconds. The opening scrape follows the same half-second
  marker poll as the CPU snapshot, so it starts up to about half a second into the window. A node that
  cannot be scraped leaves `flush.all` null with the reason in `flush.error`; the throughput still counts.
- **Linux only.** The script refuses any other host unless `ALLOW_NON_LINUX=1`, which runs it as a smoke
  test and says so.

**Protocol.** A mode comparison is a manual dispatch of the `Performance Benchmarks` workflow with
`docker_cluster_durability` checked. It runs both modes `docker_cluster_reps` times (3 by default),
both in every repetition with the order rotating (tmpfs first, then disk first, ...) so no position
effect lands on one mode every time, on a 4-core runner (servers on cores 1-3, the generator on core 0). The spread between repetitions of one mode is the noise floor, and a difference
between the modes is claimed only when their min to max ranges do not overlap, and never from a single
run of either mode; the job summary states which, for rec/s, request p99, and the servers' flush p50
and p99, and lists any measured case that has no flush latency with the reason.

## Mechanism investigations

Design-exploration benchmarks that compare mechanisms on the current log model.

### `throughput_1m.exs`

1M-message end-to-end throughput and resource baseline on the real log stack (produce ->
disk via `ReplicationServer` -> consume), measuring throughput, per-batch latency, BEAM
memory, on-disk bytes, and CPU (reductions). This is the system baseline the streaming
alternatives are judged against.

**Regime:** batch 1000 x 100B (97.7KB of values per request), one producer, one produce at a
time. It writes under `BENCH_DIR` (default: the system temp dir) on a real filesystem, so it runs
on the durable path, with segment preallocation off and group commit off: every produce is one
sync, and segments grow as they fill. Production runs with preallocation on, so this measures the
growing-segment path. The script pins both settings rather than inheriting them, and prints the
same label, filesystem type included, next to its result, so an alternative measured in another
regime is visibly not on equal ground. Measurements count on Linux only.

It refuses to run on tmpfs or ramfs, which many Linux distributions mount at `/tmp` and which
have no durable path to measure, unless `BENCH_ALLOW_TMPFS=1`. `benchmark/store_error_path_ab.exs`
reads its produce latency line, so that line keeps its shape.

```bash
mix run benchmark/throughput_1m.exs
BENCH_DIR=/var/tmp mix run benchmark/throughput_1m.exs
```

### `single_node_scale.exs`

N independent produce pipelines (each its own `BrokerServer`, `ReplicationServer` and topic) on
one node, run concurrently, to see how far a sharded data plane lifts the aggregate produce rate.
It sweeps two dimensions, because the answer depends on both: the pipeline count N, and the batch
size. With small batches the per-produce cost dominates and each pipeline's serial broker is the
limit; with large ones the disk is. The table reports, per batch size, the aggregate and
per-pipeline rate, the efficiency against the smallest N, the aggregate p50 and p99 of every
produce, and the disk rate, then whether 1M rec/s aggregate was reached.

**Regime:** 100B values, and every pipeline sends 1000 produces per cell, so every cell has as
many latency samples and batch 1000 is 1M records per pipeline: N=1 at batch 1000 runs the
`throughput_1m.exs` regime. The other settings are the same as there: it writes under `BENCH_DIR`
on a real filesystem, on the durable path, with segment preallocation off and group commit off,
so every produce is one sync; it refuses tmpfs and ramfs unless `BENCH_ALLOW_TMPFS=1`; and each
block of the table is headed by its full regime label, filesystem type included. Measurements
count on Linux only, and a single sweep is one sample, so its numbers are not published here.

| knob | default | meaning |
| --- | --- | --- |
| `SCALE_NS` | `1 2 4 8` | pipeline counts to sweep |
| `SCALE_BATCHES` | `10 100 1000` | records per produce to sweep |
| `BENCH_DIR` | system temp dir | directory to write under; must exist |
| `BENCH_ALLOW_TMPFS` | unset | `1` runs on tmpfs or ramfs anyway, with a warning |

`SCALE_NS` and `SCALE_BATCHES` take distinct positive integers separated by spaces; anything else
stops the run before it starts.

```bash
mix run benchmark/single_node_scale.exs
SCALE_NS="1 2" SCALE_BATCHES="100 1000" mix run benchmark/single_node_scale.exs
```

### `streaming_bench.exs`

Streaming delivery: push (1A) vs push+windowing vs pull (1B), sustained over 1M records,
including the peak subscriber mailbox (the backpressure signal that separates windowing
from raw push).

```bash
mix run benchmark/streaming_bench.exs
```

### `long_poll_bench.exs`

Long-poll notification: waiters inside the `BrokerServer` vs Registry pub/sub, for
1 / 10 / 100 / 1000 consumers waiting on one topic.

```bash
mix run benchmark/long_poll_bench.exs
```

### `protocol_bench.exs`

Wire protocol: JSON+base64 (the old line protocol) vs binary framing (`Record.encode`)
over 1M records: on-wire bytes plus encode/decode throughput and reductions.

```bash
mix run benchmark/protocol_bench.exs
```

### `metadata_index_bench.exs`

Metadata secondary index: `ranges_of_topic`/`segments_of_range` served from the reverse
index vs the old full scan, as the total number of ranges/segments grows (O(k) vs O(n)).

```bash
mix run benchmark/metadata_index_bench.exs
```

## Viability and overhead

### `storage_viability.exs`

Can pure-Elixir/BEAM file I/O meet NorthGuard's storage targets (fsync before ack,
flush every ~10 ms / 20k records / 10 MB, segments up to 1 GB)? Measures the local
single-replica write and read hot path. Standalone, no running server required. This is
the one the CI benchmark workflow runs.

```bash
mix run benchmark/storage_viability.exs
```

#### The preallocation A/B mode (issue #83, blocking #82)

`PREALLOC_AB=1` adds a paired experiment over segment **preallocation**. It exists because a
performance decision here carries a benchmark, and a single before/after run cannot carry one: the
published ceiling sweep moves by more than 30% run to run on unchanged code, while any win in this
area is a few percent.

**What #82 already found, and why #83 exists.** Swapping `fsync` for `fdatasync` measured *nothing*:
noise in all six cases on `ubuntu-latest` with 15 repetitions per arm. In the case that matters
most, batch 10 x 256B, the regime the pinned ceiling harness actually runs:

| arm | median p50 | spread |
| --- | --- | --- |
| fsync | 341us | 329-356 |
| fdatasync | 345us | 323-356 |
| control A1 (fsync) | 343us | 322-391 |
| control A2 (fsync) | 346us | 322-365 |

The fsync-to-fdatasync difference is 4us; the fsync-to-**fsync** control difference is 3us. They are
the same number, which is what the control exists to reveal. The platform was real (a 1-byte fsync
cost 303us on that runner, where tmpfs would be single-digit microseconds), and with n=15 the method
would have resolved a 2% effect, so this is a measured absence rather than a failure to measure.

The mechanism is the useful part: a segment GROWS, so every append changes the file size, and that
size change is metadata `fdatasync` has to journal anyway. What it saves over `fsync` rides along in
a commit it must make regardless. Preallocation is what removes the size change, which makes **#83 a
prerequisite for #82 rather than a companion to it**, and makes the useful observable the PAIR: with
the file already sized, does `fdatasync` finally win?

**What the harness keeps from #82**, because it is what made a null result trustworthy:

- **Interleaved arms.** All arms in one process on one filesystem, so a thermal blip or a noisy
  neighbour on a shared runner hits every arm rather than landing on whichever ran last.
- **An A-A control.** Two arms that are identical, labeled as if they differed. Its spread is the
  harness's noise floor, measured instead of assumed. A delta smaller than it is noise by
  construction.
- **A bootstrapped 95% CI** of the difference of medians, so the answer is an interval.
- **The verdict rule fixed in the script**, before any number exists: signal requires the delta to
  exceed the A-A control delta **and** the interval to exclude zero. "Noise everywhere" is a complete
  answer, not a failed run.

**What it fixes and adds:**

- **Rotated arm order.** #82's arms interleaved but always ran A before B, so any position effect
  landed on B every time (all six of its deltas came out positive, which has no plausible mechanism).
  The order now rotates by repetition.
- **Four mechanisms, not one.** Growing, `:sparse`, `:allocate` and written `:zeros` differ in what
  they leave for the first append to journal, so they are not interchangeable:

  | mechanism | changes i_size per append | allocates a block on first touch | extent conversion |
  | --- | --- | --- | --- |
  | growing | yes | yes | n/a |
  | `:sparse` | no | yes | n/a |
  | `:allocate` | no (Linux) | no | yes, unwritten to written |
  | `:zeros` | no | no | no |

  They run through `Malachi.Storage.Preallocation`, the module the store itself uses, so the
  benchmark measures the code that would ship.
- **Three stages.** Stage 1 triages every mechanism against both syncs in the one regime that
  matters. Stage 2 confirms only the winner against the production baseline across three batch
  shapes, so the number that gets published is not the one that chose the winner. Stage 3 sweeps
  flush sizes from 2.5KB to 1MB, because the answer turned out to depend on that and an operator
  needs to know where.
- **Creation cost and a per-mechanism 1-byte sync floor.** The floor is the direct test of the
  fixed-cost claim; the creation cost is the other side of the zero-write trade-off.

It needs a real filesystem to mean anything. `fsync` and `fdatasync` cost the same on tmpfs, which is
what every Docker compose in this repo deliberately uses, and on macOS neither reaches stable media
(`:file.sync` there does not use `F_FULLFSYNC`). It also has to run **serially**: arms measured in
parallel would contend for the same disk queue and journal, and arms split across runners would be
compared across machines, which is exactly the bias the A-A control exists to expose. Run it on the
CI runner, whose `/tmp` is ext4 on a real disk: `Performance Benchmarks` > `Run workflow` >
`prealloc_ab`.

##### What it found (issue #83)

Stage 1 on `ubuntu-latest`, OTP 28, 15 interleaved repetitions per arm, in the regime the pinned
ceiling harness runs. The A-A control put the noise floor at **1us**:

| arm | p50 | p99 |
| --- | --- | --- |
| grow + fsync (production today) | 316us | 632us |
| sparse + fsync | 309us | 539us |
| allocate + fsync | 292us | 497us |
| **zeros + fsync** | **96us** | 381us |
| zeros + fdatasync | 94us | 182us |
| control A1 / A2 (identical) | 94us / 95us | |

**Preallocation by written zeros takes the per-flush p50 from 316us to 96us, a 70% cut**, with a
95% interval of [-230, -212] against a 1us noise floor. It does not need the syscall swap: `fsync`
alone gets the whole thing.

The mechanism table predicted the ordering exactly. `sparse` and `allocate` barely move, because
they leave work for the first append (a block allocation, an unwritten-extent conversion) and both
are journaled; only written zeros leave an append with nothing to journal. The 1-byte sync floor
says the same thing from the other side: **257us on a growing file, 75us on a sized one**, so the
fixed cost #82 ran into was three quarters the size change.

For #82 the answer is still no in the median: `fdatasync` against `fsync` was -2us (grow), -5us
(sparse), -5us (allocate), -2us (zeros), every interval spanning zero. But in the **tail**, with
zeros, p99 went 381us to 182us, -52%, interval [-263, -130] against an 8us floor. Median untouched,
tail halved, which is what dropping the mtime update would look like. A p99 over n=15 is a noisy
estimator, so that is a lead for #82 to confirm, not a conclusion.

Stage 2 (n=25) confirmed it across batch shapes, and then caught two things in the harness itself.

The first was writeback. Batch 1024 x 1KB came back 18% worse in the median and **three times worse
in the tail** (p99 23.7ms to 72.0ms). Preallocation leaves its whole region dirty in the page cache,
and where the region is large relative to the flushes that follow it, the first flushes paid for
that writeback instead of for their own data. Both the harness and the store now **sync right after
preallocating**. Re-run, that case's p99 went from +204% to **-5.7%**, so that part is settled.

The second was the arm order, and the harness caught it on itself. Re-run, the two small-batch cases
came back stronger than ever, with a noise floor of **zero**:

| case | growing | preallocated | delta | noise floor |
| --- | --- | --- | --- | --- |
| batch 10 x 256B | 372us | 147us | **-60.5%**, ci95 [-230, -222] | 0us |
| batch 100 x 256B | 459us | 162us | **-64.7%**, ci95 [-301, -290] | 2us |

But in batch 1024 x 1KB the three arms with IDENTICAL configuration split 2060us, 2047us and
**2897us**. The two labeled controls agreed to within 13us; the third sat 850us above them. The
cause was the rotation: **a cyclic rotation changes which arm goes first but preserves the circular
order**, so every arm keeps following the same neighbour, and whatever that neighbour leaves behind
lands on the same arm every time. The odd one out was the only arm that always ran straight after
the growing one. It is now a shuffle.

Shuffled, the three identical arms came back at 1997us, 2004us and 2006us, agreeing to within 9us
where they had split by 850us. The bias was the ordering, and with it gone the picture is clean and
not the one expected:

| flush size | growing | preallocated | p50 | p99 |
| --- | --- | --- | --- | --- |
| 2.5KB (batch 10 x 256B) | 231us | 62us | **-73.2%**, ci95 [-176, -162] | -40.4% |
| 25KB (batch 100 x 256B) | 259us | 83us | **-68.0%**, ci95 [-183, -173] | -41.5% |
| 1MB (batch 1024 x 1KB) | 1523us | 1997us | **+31.1%**, ci95 [448, 515] | **+115.8%** |

Noise floor 2us in every case, so all three are real. **Preallocation is not a free win, it is a
trade.** It takes metadata out of the commit, which is most of the bill when a flush is a few KB,
and it gives up the filesystem's delayed allocation, which is what matters when a flush is a
megabyte and the transfer is the bill.

Stage 3 swept the range between them, and the crossover is sharp (n=15, one sync per flush, record
size held at 1KB so the only variable is bytes per flush):

| bytes per flush | growing p50 | preallocated p50 | delta | p99 delta | identical-arm spread |
| --- | --- | --- | --- | --- | --- |
| 2.5KB | 317us | 96us | **-69.7%** | -47.2% | 2us |
| 25KB | 355us | 113us | **-68.2%** | -33.7% | 2us |
| 64KB | 473us | 278us | **-41.2%** | -11.7% | 7us |
| 128KB | 626us | 609us | **-2.7%** | -6.7% | 11us |
| 256KB | 925us | 971us | **+5.0%** | +10.5% | 10us |
| 512KB | 1512us | 1715us | **+13.4%** | +81.7% | 32us |
| 1MB | 2612us | 3034us | **+16.2%** | +258.0% | 58us |

**Both turn over around 170KB per flush.** Interpolating between the measured points, the p50
crosses zero near 173KB and the p99 near 178KB, and at 128KB the p50 delta is 17us against an 11us
noise floor: a tie, which is what a crossover should look like.

What separates the two percentiles is not where they cross but how steeply they fall afterward.
Past the crossover the median drifts (+5.0%, +13.4%, +16.2%) while the tail runs away (+10.5%,
+81.7%, +258.0%). A deployment defending a p99 therefore has far more to lose from being on the
wrong side, even though both percentiles turn over at the same place.

An earlier version of this section claimed the tail turned over first, around 256KB. It does not,
and the measurements above never said so: being more positive at one point is a steeper slope, not
an earlier crossing. The claim inverted the operator guidance for exactly the deployments that care
most, which is worth recording rather than quietly editing.

The flush size is set by what a producer sends per `produce`, or by what group commit coalesces, not
by `:flush_bytes`, which is only a ceiling (10MB by default). The pinned ceiling harness runs 2.5KB
per flush, fifty times below the crossover, which is why the knob ships on.

Keeping a third arm identical to the two controls is what made the ordering bias visible in the
first place, instead of letting 2897us be read as a result.

Creation costs **249ms for 64MB** once the sync is counted, which the saving pays back in roughly
1100 of the ~26k flushes a 64MB segment sees. That is 4% of the segment's life, and it is also a
quarter-second stall inside the first append after a roll.

```bash
# stage 1, triage
PREALLOC_AB=1 PREALLOC_AB_REPS=15 PREALLOC_AB_OUT=/tmp/prealloc-ab.json \
  mix run --no-start benchmark/storage_viability.exs

# stage 2, confirm one mechanism against the production baseline
PREALLOC_AB=1 PREALLOC_AB_STAGE=2 PREALLOC_AB_MECHANISM=zeros PREALLOC_AB_SYNC=datasync \
  PREALLOC_AB_REPS=25 PREALLOC_AB_OUT=/tmp/prealloc-ab-stage2.json \
  mix run --no-start benchmark/storage_viability.exs

# stage 3, sweep flush sizes to find where the win turns into a loss
PREALLOC_AB=1 PREALLOC_AB_STAGE=3 PREALLOC_AB_MECHANISM=zeros PREALLOC_AB_SYNC=sync \
  PREALLOC_AB_REPS=15 PREALLOC_AB_OUT=/tmp/prealloc-ab-stage3.json \
  mix run --no-start benchmark/storage_viability.exs
```

### `dashboard_security_benchmark.exs`

Measures the overhead that authentication, security headers, and audit logging add
to the dashboard HTTP endpoints (`/login`, `/metrics`, `/stream`). Acceptance
criterion: under 25% latency increase. Requires a running server.

```bash
# in one shell
mix run --no-halt
# in another
mix run benchmark/dashboard_security_benchmark.exs
```

### `rate_limit_ab.sh` / `rate_limit_ab.exs`

A paired A/B of the publish quota check (`RateLimiter.check_limit_in_caller/3`), the branch tree
against a baseline tree, with an A-A control and arms interleaved by run in a shuffled order (issue
#151). One sample starts the limiter alone, with no broker and no sockets, and times rounds of checks
on one hot identifier at 1 and 64 concurrent callers. The verdict rule is the one in
`support/paired_stats.exs`, and the driver and analysis are shared with `store_error_path_ab.sh`
through `support/ab_lib.sh` and `support/ab_run.exs`. Only a Linux run counts; in CI it is the
`rate_limit_bench_ab` input of the benchmark workflow.

```bash
AB_REPS=7 benchmark/rate_limit_ab.sh /path/to/main-checkout . /tmp/rate-limit-ab
```
