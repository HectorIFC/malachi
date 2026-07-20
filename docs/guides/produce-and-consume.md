# Produce and consume

[Getting started](getting-started.md) showed the shortest path to a record landing and coming back. This
guide is the working detail: how keys decide ordering, the three ways to track a position, what
at-least-once obliges you to do, and the two errors a correct client must expect.

## Producing

A record is a value, an optional key, and optional headers:

```elixir
LogApi.produce(Malachi.LogBroker, "orders", [
  %{"key" => "user-42", "value" => "created", "headers" => %{"source" => "api"}}
])
#=> {:ok, 1}
```

The reply is a **count, not an offset**. Nothing in the produce path hands the client a position, which is
what lets ranges split underneath without breaking anyone.

### Batch, don't loop

`produce/3` takes a list, and the whole list is one round trip and one quorum wait. Appending 100 records
one call at a time pays that cost 100 times:

```elixir
# one round trip, one quorum ack
LogApi.produce(Malachi.LogBroker, "orders", Enum.map(events, &%{"key" => &1.user, "value" => &1.body}))
```

### Keys decide ordering, not delivery

A key hashes to a position in the topic's keyspace, and that position lands in exactly one range. Records
with the same key therefore go to the same range and are **ordered relative to each other**.

A key is not an address and not a filter. There is no "read the records for key X"; a consumer reads
ranges, and a range holds many keys. If you need per-entity streams, the entity is the key and you filter
client-side, or you use a topic per entity class.

Omit the key and records are distributed across ranges, which maximises parallelism and gives you **no**
ordering between them.

## Consuming

Three ways to track position, in increasing order of what the server does for you.

### 1. Carry the cursor yourself

```elixir
{:ok, records, cursor} = LogApi.fetch(Malachi.LogBroker, "orders", :start, 100)
{:ok, more, cursor} = LogApi.fetch(Malachi.LogBroker, "orders", cursor, 100)
```

`:start` (or `nil`) begins at the beginning. Pass the returned cursor back to continue. The position lives
in your process, so it dies with your process. Good for a one-off scan or an export.

### 2. Consumer group: the server commits for you

```elixir
{:ok, records, cursor} = LogApi.fetch_group(Malachi.LogBroker, "orders", "workers", 100)
:ok = process(records)
:ok = LogApi.commit(Malachi.LogBroker, "orders", "workers", cursor)
```

The group's position is durable, so a restart resumes where the group left off. **Commit after you
process**, never before, or a crash silently skips records.

### 3. Group member: the server also splits the work

Run several members of one group with distinct member ids and the coordinator assigns each a share of the
topic's ranges:

```bash
node consumer.js orders --group workers --member m1 &
node consumer.js orders --group workers --member m2 &
```

Each member reads only its assigned ranges, and the assignment rebalances when members join or leave. The
client still never sees a range id.

### Long polling

`fetch/5` and `fetch_group/5` take a final `wait_ms`. With `0` the call returns immediately, empty if
there is nothing new. With a timeout the server holds the request open until a record arrives or the wait
expires, which is how `--follow` avoids a busy loop:

```bash
node consumer.js orders --follow
```

## At-least-once, and what it costs you

A consumer group commits **after** processing, so a crash in between re-delivers the batch. There is no
configuration that turns this into exactly-once.

**Your handlers must be idempotent.** In practice that means one of:

- a natural idempotency key in the record, and a write that is a no-op the second time (an upsert on that
  key, `INSERT ... ON CONFLICT DO NOTHING`),
- or a dedupe table of processed record ids, checked before the side effect.

The failure this prevents is not theoretical: it happens on every deploy that restarts a consumer between
a fetch and its commit.

## Two errors a correct client handles

Both are transient and both mean retry, not fail:

| error | when | what to do |
|---|---|---|
| `:migrating` | a metadata write hit a topic whose range is being split or migrated | back off and retry; the fence lifts in milliseconds |
| `:not_owner` | the read reached a node that no longer owns that range, after a failover or rebalance | re-resolve the owner and retry |

The bundled scripts handle both: `:migrating` through the `withRetry` helper in
[`scripts/lib/cli.js`](https://github.com/HectorIFC/malachi/blob/main/scripts/lib/cli.js), and
`:not_owner` with a back-off around the fetch. Each prints a grey `~` per attempt, so you can watch a
split or a failover happen live. A client that treats either as fatal will appear to fail at random under
an otherwise healthy resharding.

## Next

Polling is one shape. For server-push with flow control, see
[Streaming with backpressure](streaming-with-backpressure.md).
