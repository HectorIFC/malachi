# Running the Elixir load test

`mix malachi.loadtest` is the **multi-core** generator: one BEAM process per connection, so N
connections spread across every scheduler instead of sharing one event loop. Reach for it when you
want to find a server's ceiling, when you need to drive a cluster, or when you need to exercise
control-plane operations under load. For the outside-the-BEAM view, use the
[Node.js generator](running-the-node-loadtest.md) instead.

It is a thin wrapper over `Malachi.Loadtest`, so everything here is also callable from IEx.

## Before you start

Like the Node one, it connects to an **already running** server rather than starting one:

```bash
MIX_ENV=dev mix run --no-halt
```

Unlike the Node one, the target and the credentials are flags rather than environment variables, and
the defaults are `--host 127.0.0.1 --port 4040 --user admin --pass admin123`.

## The six scenarios

```bash
# produce: the throughput workload
mix malachi.loadtest --scenario produce --connections 512 --batch 10 --duration 10

# fetch and stream: read paths, seeded with --prepopulate first
mix malachi.loadtest --scenario fetch --connections 64 --max 200 --prepopulate 50000
mix malachi.loadtest --scenario stream --connections 8 --window 500

# mixed: produce and fetch on the same connections
mix malachi.loadtest --scenario mixed --connections 128 --record-size 512

# user and acl: the CONTROL plane under load, which the Node client does not cover
mix malachi.loadtest --scenario user --connections 16
mix malachi.loadtest --scenario acl --connections 16
```

`user` and `acl` drive create/delete and grant/revoke cycles. Those go through the replicated `ra`
state machine rather than the log, so they measure something the data-plane scenarios never touch.

## Driving a cluster

`--host` takes a comma-separated list, and connection `i` targets host `i mod n`. That spreads load
across every node's broker mailbox rather than funneling a cluster's worth of traffic through one
node's socket:

```bash
mix malachi.loadtest --host node1,node2,node3 --scenario produce --connections 192 --batch 100
```

Pair it with `--topics`: a single topic pins to one data-plane shard, so `--topics 12` is what
actually lets placement spread primaries across the nodes.

```bash
mix malachi.loadtest --host node1,node2,node3 --topics 12 --connections 192 --scenario produce
```

## Pipelining

`--pipeline` sets how many produce requests a connection keeps in flight. The default of 1 is a
closed loop: send, wait, send. Raising it measures the server with the round trip taken out of the
picture, which is the difference between measuring the broker and measuring the network between you
and it.

```bash
mix malachi.loadtest --scenario produce --connections 64 --pipeline 8 --batch 100
```

## TLS and tokens

```bash
mix malachi.loadtest --tls --cacert ca.pem --cert client.pem --key client-key.pem
mix malachi.loadtest --token "$(...)"   # instead of --user/--pass
```

A `--cert` without a `--key` (or the reverse) is rejected before a connection is opened, rather than
failing later inside the handshake.

`--tls` authenticates the server as well as encrypting the connection: the certificate is checked
against `--cacert` when you name a CA, and against the operating system's trust store when you do not,
and either way the hostname you connected to has to match the certificate. A broker using a private CA
(what `scripts/generate-dist-certs.sh` produces) therefore needs its `--cacert`, and says so if you
forget it rather than returning a raw TLS alert.

`--insecure` skips that verification. It exists for a development server whose certificate nothing can
vouch for, and it is the only way to get the old behavior back, deliberately: an unverified TLS
connection is encrypted but unauthenticated, so anyone on the path can present their own certificate
and read and rewrite the traffic. Runs that use it record it in the reproduce command, since the
numbers came from that configuration.

## Recording a result

`--json` prints one JSON document with a `meta` block: when, from which commit, on what hardware, and
the command to reproduce it.

```bash
mix malachi.loadtest --scenario produce --connections 20 --duration 10 --batch 10 \
  --record-size 256 --json > /tmp/result.json
```

The [Elixir load test results](../generated/loadtest-elixir-results.md) page renders exactly this
document, from `benchmark/published/loadtest-elixir.json`, as does the Elixir section of the
[benchmark dashboard](https://hectorifc.github.io/malachi/benchmarks/).

That file is written by CI, not by hand: the Publish results workflow measures it on every push to
main and commits the result, so an edit of your own would be overwritten by the next merge. A pull
request runs it too without committing, and posts the numbers as a comment when the branch lives in
this repository; from a fork the comment is skipped and the artifacts carry them.

CI does not run the bare command above. It runs `scripts/loadtest-ceiling.sh` with `GENERATOR=elixir`,
which boots a dedicated server pinned to three cores and pins this generator to the fourth, then sweeps
`--connections` and publishes the peak as the ceiling:

```bash
GENERATOR=elixir SRV_CPUSET=1,2,3 LT_CPUSET=0 OUT=/tmp/loadtest-elixir.json scripts/loadtest-ceiling.sh
```

The single core is deliberate: it holds this multi-core generator to the same one core the Node client
gets, so the published number compares the servers rather than the generators. Node and Elixir run on
**separate** runners, so one load test never influences the other.

Holding a BEAM to one core takes more than `taskset`. The harness passes
`+S 1:1 +SDcpu 1:1 +SDio 1 +sbwt none +sbwtdcpu none +sbwtdio none`: schedulers capped at the pinned
core count, the dirty CPU and IO pools shrunk from their defaults of 4 and 10 threads (a generator does
no file IO inside the window), and busy-wait off everywhere. Without those, roughly sixteen VM threads
timeslice one core with several of them spinning, and the first CI runs showed exactly that pathology:
p50 latency in the tens of milliseconds but p99 above a second, a non-monotonic connection ladder, and
the server nearly idle. That is a harness artifact, not a BEAM verdict: the BEAM's strength is
multi-core scalability, which a one-core pin removes by construction, so a single-threaded event loop
client may still edge it out here. The attribution below says who capped, which is what the comparison
needs.

The run samples both sides' CPU across the peak window and the page reports them: a server near three
of three cores saturated (its ceiling was found), a generator near one of one capped first (the number
is a lower bound). Both generators drive a **single topic**, which in Malachi means a single range and
a serialized append on its primary, so the published figure is the one-topic ceiling; `--topics` spreads
load across ranges when you want the multi-shard picture locally. Run the script the same way locally
when you want the published ceiling; the bare command above is a single point at whatever concurrency
you pass.

How connections are opened is part of the methodology too. Every connection pays an Argon2 credential
verification on the server, so opening hundreds simultaneously is an auth storm: on the 3-core CI
server it saturated all three cores for tens of seconds and timed the high rungs out during setup.
Both generators take the same `--connect-strategy` flag: `bounded` (default; at most
`--connect-concurrency` connects in flight, 32), `stagger` (connection `i` starts after
`i * --connect-stagger-ms`), or `all-at-once` (the storm, kept for reproducing it on purpose). The
ceiling harness pins `bounded` at 32 explicitly, each pacing knob is only accepted with the strategy
that reads it, and a connection that still fails aborts the run with a `SetupError` naming how many of
how many failed, instead of a crashed worker's exit dump.

This generator records fewer latency percentiles than the Node one, which keeps a full histogram, and
counts backpressure that the Node one does not (dropped connections, server-shed produces,
reconnects). Both pages render only what the run recorded, so the two sections legitimately show
different fields rather than one of them looking incomplete.

Two things about that `meta` block are worth knowing. The recorded command is rebuilt from the
**effective** configuration rather than copied from what you typed, so a knob you left at its default
still appears and the run is reproducible from the record alone. And it carries no credentials by
construction: that string gets committed and published, and a password in it would outlive any
rotation.

Write to a path outside the repository and move the file into place afterward. Redirecting straight
onto a tracked file truncates it first, which dirties the working tree while the run is in flight, and
the recorded ref then reads `<sha>-dirty` rather than naming a commit.

## Reading the output

Beyond throughput and the latency tail, this generator reports what the Node one does not: `dropped`
connections, `overloaded` (produces the server shed under backpressure) and `reconnects`. Those three
are the difference between a server that is slow and a server that is shedding, and a throughput
number taken while any of them is climbing describes the backpressure rather than the capacity.

A run that completes with `ops: 0` prints a warning naming the likely cause instead of reporting a
silent zero.

## The heavier benchmarks

`mix malachi.loadtest` measures a live server. The scripts under `benchmark/` measure mechanisms, and
run without one: `storage_viability.exs` (the file I/O floor against NorthGuard's storage targets),
`throughput_1m.exs` (1M records end to end), `single_node_scale.exs` (N pipelines on one node),
`protocol_bench.exs` and `streaming_bench.exs`. See `benchmark/README.md`.
