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
