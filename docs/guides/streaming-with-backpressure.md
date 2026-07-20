# Streaming with backpressure

[Produce and consume](produce-and-consume.md) covers polling: the client asks, the server answers. This
guide covers the push direction, where the server sends records as they are produced, and the flow control
that keeps a fast producer from burying a slow consumer.

## Why a credit window

Server push without flow control is a loaded gun. If the broker sends every record the moment it lands,
a consumer that processes slower than the producer appends accumulates an unbounded mailbox until the node
runs out of memory. The consumer never gets to say "stop".

Malachi makes the consumer's capacity explicit. A subscription declares a **window**: the maximum number
of records that may be in flight, meaning pushed but not yet acked. The broker will not exceed it.

```
window = 100

  in_flight=0    push 100  ──────────▶  budget exhausted, broker goes quiet
  ack 40         ◀──────────────────    in_flight=60, 40 credit returned
  push 40        ──────────────────▶    in_flight=100 again
```

The budget for each push is exactly:

```elixir
budget = min(max, window - in_flight)
```

`max` caps a single push, `window` caps the total outstanding. When the budget reaches zero the broker
stops pushing to that subscriber and does no further work for it. Backpressure here is the absence of
sends, not a queue building up somewhere.

## Subscribing

```elixir
:ok = LogApi.subscribe(Malachi.LogBroker, "orders", "live", 100, 50)
```

That registers the **calling process** as a push subscriber, resuming from the group's committed position.
Records arrive as ordinary messages:

```elixir
receive do
  {:log_records, topic, records, positions} ->
    :ok = handle(records)
    cursor = LogApi.encode_cursor(positions)
    :ok = LogApi.stream_ack(Malachi.LogBroker, topic, "live", cursor, length(records))
end
```

Note the fourth element is internal **positions**, not the opaque cursor. `stream_ack/5` takes a cursor,
so encode it first. That asymmetry exists because the push path hands the subscriber the raw position and
lets whoever owns the connection mint the client-facing token; the TCP acceptor does exactly this before
putting the batch on the wire.

From the shell:

```bash
node subscriber.js orders --group live --window 500
```

## The ack does two jobs

`stream_ack/5` is one call with two effects, and conflating them is deliberate:

1. **Returns credit.** `count` records of window space, which unblocks further pushes.
2. **Commits durably.** The group's position advances, so a restart resumes here.

You cannot return credit without committing. That is what makes the window safe: the broker can only
consider a record delivered once you have durably said you are done with it. Ack a batch before you
process it and you have converted at-least-once into at-most-once, silently.

Delivery is **at-least-once**, same as polling: a crash between the push and the ack re-delivers. The
idempotency obligation from [Produce and consume](produce-and-consume.md#at-least-once-and-what-it-costs-you)
applies unchanged.

## Sizing the window

The window is a latency/throughput dial, not a correctness setting.

- **Too small** and the consumer idles between pushes, waiting on round trips. The floor is your
  processing time per record times the round-trip time.
- **Too large** and you are back to an unbounded mailbox, with the added cost that everything in flight is
  re-delivered on a crash.

Start at a few times the batch size you actually process at once, and raise it only if the consumer is
demonstrably starved. The default of 100 is a reasonable place to begin.

## Parallel streaming with members

One subscriber per group reads the whole topic. To spread the load, give each process a member id:

```bash
node subscriber.js orders --group live --member m1 &
node subscriber.js orders --group live --member m2 &
```

The coordinator assigns each member a share of the topic's ranges and the broker scopes that member's push
stream to them. Each member carries its **own** window, so total in-flight is the sum across members.

In Elixir, `subscribe_member/7` and `stream_ack_member/7` are the member-scoped equivalents.

### Members must keep acking

A member ack is also a **heartbeat**: it re-polls the coordinator, which both refreshes the member's range
assignment (so a rebalance is picked up on the next ack) and proves the member is alive.

A member that goes quiet, because it is idle rather than dead, would eventually be treated as gone and have
its ranges reassigned. So an idle member sends a periodic empty ack:

```bash
node subscriber.js orders --group live --member m1 --heartbeat 10000
```

If you implement your own client, this is the part most likely to bite: an idle stream that never acks
looks exactly like a dead one.

## Choosing between push and poll

| | poll (`fetch_group`) | push (`subscribe`) |
|---|---|---|
| who drives | client asks | server sends |
| flow control | implicit, you ask when ready | explicit credit window |
| idle cost | a long-poll held open | nothing in flight |
| best for | batch jobs, scans, simple consumers | low-latency fan-out to live consumers |

Both commit through a consumer group, so you can move a workload from one to the other without losing
position.
