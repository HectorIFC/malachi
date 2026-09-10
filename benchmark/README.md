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

## Mechanism investigations

Design-exploration benchmarks that compare mechanisms on the current log model.

### `throughput_1m.exs`

1M-message end-to-end throughput and resource baseline on the real log stack (produce ->
disk via `ReplicationServer` -> consume), measuring throughput, per-batch latency, BEAM
memory, on-disk bytes, and CPU (reductions). This is the system baseline the streaming
alternatives are judged against.

```bash
mix run benchmark/throughput_1m.exs
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
- **An A-A control.** Two arms that are identical, labelled as if they differed. Its spread is the
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
**2897us**. The two labelled controls agreed to within 13us; the third sat 850us above them. The
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

What separates the two percentiles is not where they cross but how steeply they fall afterwards.
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
