# Running the Node.js load test

`scripts/loadtest.js` drives load over the binary wire protocol using the reference Node client in
`scripts/lib/`. It is the **external** view of the server: real TCP, real serialization, real
authentication, from a runtime that is not the BEAM. That last part is the point of keeping it
alongside the [Elixir generator](running-the-elixir-loadtest.md): a number produced by a client that
shares nothing with the server is harder to fool yourself with.

It is single-threaded (one Node event loop), so it saturates a server with concurrency rather than
with cores. Unpinned on your own machine, the multi-core [Elixir generator](running-the-elixir-loadtest.md)
pushes a server harder; the **published** ceiling instead pins both generators to a single core (see
[How the published ceiling is measured](#how-the-published-ceiling-is-measured)) so the Node-vs-Elixir
comparison is fair rather than a contest of generator runtimes.

## Before you start

The generator connects to an **already running** server; it does not start one.

```bash
MIX_ENV=dev mix run --no-halt
```

The default credentials are `app` / `app123`, which the dev config seeds with produce and consume
permission. Host, port and credentials come from the environment, not from flags:

```bash
MALACHI_HOST=localhost MALACHI_PORT=4040 MALACHI_USER=app MALACHI_PASS=app123
```

## The four scenarios

```bash
cd scripts

# produce: append batches as fast as the server accepts them
node loadtest.js --scenario produce --connections 20 --batch 10 --record-size 256

# fetch: read a backlog it seeds first
node loadtest.js --scenario fetch --prepopulate 50000 --max 200

# stream: server-pushed delivery with a credit window
node loadtest.js --scenario stream --connections 4 --window 500

# mixed: produce and fetch interleaved on the same connections
node loadtest.js --scenario mixed --connections 20 --record-size 512 --keys 1000
```

`--keys` sets key cardinality, which matters for anything that partitions by key. `--prepopulate`
defaults to 10000 for the scenarios that need a backlog and to nothing for `produce`.

## Closed loop and open loop

By default every connection runs `operation -> await -> operation` in a tight loop. That finds the
**ceiling**: the server is always saturated, and the latency you get is latency at saturation.

`--rate` switches to a fixed arrival rate instead, and this is the mode to use when you care about
latency rather than about the ceiling:

```bash
node loadtest.js --scenario produce --rate 20000 --connections 50 --duration 30
```

Requests are then fired on schedule regardless of whether earlier ones finished, and each one's
latency is measured from the time it was **scheduled**, not from the time it was sent. That is what
corrects coordinated omission: in a closed loop, a server stall stops new requests from being issued,
so the stall hides itself and the percentiles come out flattering. Under `--rate` the requests that
queued behind a stall carry its cost, which is what a client would actually experience.
`--max-inflight` caps how far behind the generator will let itself fall, and hitting it flags the run
as saturated rather than pretending the rate was met.

## Recording a result

`--json` prints the report as a single JSON document with a `meta` block that records the command,
the git ref, the version and the hardware, so the number stays interpretable months later:

```bash
node loadtest.js --scenario produce --connections 20 --duration 10 --batch 10 \
  --record-size 256 --json > /tmp/result.json
```

The [Node.js load test results](../generated/loadtest-node-results.md) page renders exactly this
document, from `benchmark/published/loadtest-node.json`, and so does the headline of the
[benchmark dashboard](https://hectorifc.github.io/malachi/benchmarks/).

## How the published ceiling is measured

You do not update that file by hand, and a hand-edit would not survive: the Publish results workflow
measures it on a CI runner on every push to main and commits what it measured, so the site always
shows the merged code rather than whichever laptop last captured a sample. It does not run the bare
command above. It runs `scripts/loadtest-ceiling.sh` with `GENERATOR=node`, which boots a dedicated
server pinned to three cores, pins this generator to the fourth, then sweeps `--connections` and keeps
the peak as the ceiling:

```bash
GENERATOR=node SRV_CPUSET=1,2,3 LT_CPUSET=0 OUT=/tmp/loadtest-node.json scripts/loadtest-ceiling.sh
```

The pinning is the whole point: on one runner an unpinned generator steals server CPU and flatters the
number. Because the generator gets a single core, a low ceiling can be this client capping rather than
the server, so the run samples both sides' CPU across the peak window and the published page reports
them: a server near three of three cores saturated (its ceiling was found), a generator near one of one
capped first (the number is a lower bound). The load drives a **single topic**, which in Malachi means
a single range and a serialized append on its primary, so the published figure is the one-topic
ceiling. If the peak lands at the top of the sweep the script warns to widen `CONNS_LADDER`, since the
knee may lie beyond it.

How connections are opened is part of the methodology. Every connection pays a server-side credential
verification (Argon2, expensive by design), so opening hundreds simultaneously is an auth storm: on the
3-core CI server it exhausted the high rungs' client timeouts before a single record flowed. Both
generators therefore take the same `--connect-strategy` flag: `bounded` (default; at most
`--connect-concurrency` connects in flight, 32), `stagger` (connection `i` starts after
`i * --connect-stagger-ms`), or `all-at-once` (the storm, kept for reproducing it on purpose). The
ceiling harness pins `bounded` at 32 explicitly, and each pacing knob is only accepted with the
strategy that reads it.

To see what a change does before merging, open a pull request: the workflow runs there too without
committing, because runner variance would otherwise put noise in every diff. Node and Elixir run on
**separate** runners so neither load test influences the other. It posts the numbers as a comment when
the branch is in this repository; from a fork that step is skipped and the numbers come back as the
run's artifacts. Run the script the same way locally when you want that ceiling; run the bare command
above when you want a quick single-point measurement instead.

Write to a path **outside the repository** and move the file into place afterward, as above. Piping
straight onto the tracked file truncates it first, which makes the working tree dirty while the run
is in flight, and the recorded git ref then comes out as `<sha>-dirty` instead of naming a commit
anyone can check out.

## Reading the output

`errors` is the first number to look at: a throughput figure taken from a run with errors is a
measurement of the failure, not of the server. `--warmup` excludes the opening seconds from the
statistics, which matters because the first connections pay for topic creation and JIT warmup.

`--self-test` validates the latency histogram against a brute-force reference without a server, if
you ever doubt the percentiles themselves:

```bash
node loadtest.js --self-test
```
