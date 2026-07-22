# Benchmarks

Two benchmarks target the current log-broker architecture. The former MalachiMQ
performance suite (queue throughput/latency/memory baselines, overflow strategies,
blocked-producer fairness, and their CI/hook machinery) was removed with the queue
model it measured.

## `storage_viability.exs`

Can pure-Elixir/BEAM file I/O meet NorthGuard's storage targets (fsync before ack,
flush every ~10 ms / 20k records / 10 MB, segments up to 1 GB)? Measures the local
single-replica write and read hot path. Standalone, no running server required.

```bash
mix run benchmark/storage_viability.exs
```

## `dashboard_security_benchmark.exs`

Measures the overhead that authentication, security headers, and audit logging add
to the dashboard HTTP endpoints (`/login`, `/metrics`, `/stream`). Acceptance
criterion: under 25% latency increase. Requires a running server.

```bash
# in one shell
mix run --no-halt
# in another
mix run benchmark/dashboard_security_benchmark.exs
```

Results are written to `results/` (gitignored except `.gitkeep`).
