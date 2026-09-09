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

#### The sync A/B mode (issue #82)

`SYNC_AB=1` adds a paired `fsync`-vs-`fdatasync` experiment. It exists because a performance decision
here carries a benchmark, and a single before/after run cannot carry one: the published ceiling sweep
moves by more than 30% run to run on unchanged code, while any fdatasync win is a few percent.

Three things separate it from a naive before/after:

- **Interleaved arms.** `A, B, A, B, ...` in one process on one filesystem, so a thermal blip or a
  noisy neighbour on a shared runner hits both arms rather than landing on whichever ran second.
- **An A-A control.** Two arms that are both plain `fsync`, labelled as if they differed. Its spread
  is the harness's noise floor, measured instead of assumed. A datasync delta smaller than the
  fsync-vs-fsync delta is noise by construction.
- **A bootstrapped 95% CI** of the difference of medians, so the answer is an interval.

The verdict rule is fixed in the script, before any number exists: signal requires the A/B delta to
exceed the A-A control delta **and** the interval to exclude zero. "Noise in every case" is a
complete answer, not a failed run.

It needs a real filesystem to mean anything. `fsync` and `fdatasync` cost the same on tmpfs, which is
what every Docker compose in this repo deliberately uses, and on macOS neither reaches stable media
(`:file.sync` there does not use `F_FULLFSYNC`). Run it on the CI runner, whose `/tmp` is ext4 on a
real disk: `Performance Benchmarks` > `Run workflow` > `sync_ab`.

```bash
SYNC_AB=1 SYNC_AB_REPS=15 SYNC_AB_OUT=/tmp/sync-ab.json mix run --no-start benchmark/storage_viability.exs
```

##### What it found the first time (issue #82)

Noise in all six cases, on `ubuntu-latest` with 15 repetitions per arm. In the case that matters
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

The mechanism: a segment GROWS, so every append changes the file size, and that size change is
metadata `fdatasync` has to journal anyway. What it saves over `fsync` rides along in a commit it
must make regardless. Segment preallocation (issue #83) is what removes the size change, which makes
#83 a prerequisite for #82 rather than a companion to it. Re-run this against that baseline once
preallocation lands.

One known bias to fix before that re-run: the arms are interleaved but always in the order A then B,
so any position effect lands on B every time. The A-A control carries the same bias, which is why
the conclusion holds, but alternating the order between repetitions would be better.

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
