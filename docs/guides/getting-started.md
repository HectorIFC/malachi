# Getting started

This guide takes you from a clone to your first appended and consumed record, first in-process and then
over the network.

Requires Elixir `~> 1.19` / OTP 26+.

## Run the broker

```bash
git clone https://github.com/HectorIFC/malachi.git
cd malachi
mix deps.get
iex -S mix        # TCP on 4040, dashboard on 4041
```

A single node is in-memory by default. Set `MALACHI_LOG_CLUSTER` / `MALACHI_LOG_NODES` for a replicated,
HA control plane over `ra`; the environment variables are listed in the
[README](../../README.md) (the *Environment Variables* section).

Prefer containers? See [Running with Docker](../DOCKER_README.md).

## Your first records (in-process)

The quickest way to see the model is the in-process API in that `iex` session:

```elixir
alias Malachi.LogApi
broker = Malachi.LogBroker

LogApi.create_topic(broker, "events")

# produce by key: no partitions, no offsets exposed
LogApi.produce(broker, "events", [
  %{"key" => "user-1", "value" => "hello"},
  %{"key" => "user-2", "value" => "world"}
])

# consume from the start: records + an opaque cursor to resume from
{:ok, records, cursor} = LogApi.fetch(broker, "events", :start, 100)
Enum.map(records, & &1.value)        #=> ["hello", "world"]

# resume by passing the cursor back, nothing new yet
{:ok, [], _cursor} = LogApi.fetch(broker, "events", cursor, 100)
```

Two things to notice, because they are the whole design:

1. You addressed records by **key**, never by partition.
2. You resumed with a **cursor the server gave you**, never with an offset you computed.

See `Malachi.LogApi` for the full API.

## Over the network

External clients speak the binary protocol on port **4040**. The repository ships reference clients in
`scripts/` (Node.js, no dependencies):

```bash
cd scripts

# append 10 records to "orders" (creating the topic first)
node producer.js orders --create

# read them back from the start
node consumer.js orders

# keep long-polling for new records
node consumer.js orders --follow
```

Each client authenticates first. In development the seeded users are `producer` / `producer123` and
`consumer` / `consumer123`; override with `MALACHI_USER` / `MALACHI_PASS`.

> #### Development credentials only {: .warning}
> Those seeded users exist in `dev`/`test` only. Production refuses to boot with them: you either supply
> passwords explicitly or let the node generate a random admin password and print it in the logs. That is
> a one-time event only when `MALACHI_RA_DATA_DIR` is a persistent volume; on the temp default it repeats
> on every restart. See the *Authentication* section of the [README](../../README.md).

## The dashboard

Open <http://localhost:4041> for the built-in dashboard (metrics, topics, live stream). It requires an
authenticated admin session: see the dashboard section of the README for the credentials it expects.

## Next steps

- [The log model](log-model.md): why cursors are opaque and what a *range* is.
- [Produce and consume](produce-and-consume.md): batching, keys and ordering, consumer groups, and the
  errors a correct client retries.
- [Streaming with backpressure](streaming-with-backpressure.md): server push and the credit window.
- `Malachi.Wire`: the binary protocol, if you are writing a client.
- The *Observability* section of the [README](../../README.md), Prometheus metrics, telemetry, tracing.
