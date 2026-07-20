# The log model

Malachi is a log, not a queue. This guide explains the four concepts a client touches, **topic**, **key**,
**cursor**, **consumer group**, and the three the server manages underneath - **range**, **segment**,
**replica set**.

## What a client sees

A client deals in three things and nothing else:

- **topic**: a named, ordered, replicated log.
- **key**: on produce, routes each record to a range of the topic's keyspace. Ordering is guaranteed
  **per key**, not globally.
- **opaque cursor**: on consume, a position token you echo back to continue.

That is the entire client-facing model. There is no partition count to choose, no offset arithmetic, no
rebalance protocol to implement.

## Why the cursor is opaque

Internally a cursor encodes per-range positions. It is deliberately opaque so the broker can **split, merge
and restripe ranges while the cluster is running** without breaking clients.

This is the core departure from Kafka, which exposes partitions and offsets to clients. Once a client knows
"partition 7, offset 12345", the partition count is frozen into your clients' assumptions, resharding
becomes a migration event. Here the equivalent change is a server-side operation the client never notices.

## What the server manages

```
topic
 └── range          slice of the keyspace [0, 2^bits), splits as it grows
      └── segment   a bounded, sealed-when-full chunk of the log
           └── replica set   the nodes holding that segment, committed by quorum
```

- **Range.** A topic's keyspace is divided into ranges. A record's key hashes to a position, and that
  position lands in exactly one range. Ranges **split** as they grow, which is how a topic scales without
  the client choosing a partition count up front.
- **Segment.** Each range is a series of segments. The active segment takes appends until it crosses its
  size threshold, then it is **sealed** and a new one rolls. Sealed segments are immutable, which is what
  makes re-replicating them safe.
- **Replica set.** Each segment is replicated across nodes chosen by rendezvous (HRW) hashing. A write is
  acknowledged when a **quorum** has it durably (fsync before counting), so the system tolerates
  ⌊(N-1)/2⌋ slow or failed replicas.

Reads never expose any of this. `Malachi.LogApi.fetch/5` returns records plus the next cursor.

## Consumer groups

A cursor you carry yourself is fine for a single reader. For a shared, resumable position, use a **consumer
group**: the server commits the group's position, so a restart resumes where the group left off.

```bash
node consumer.js orders --group workers    # position committed server-side
```

For parallel consumption, run several members of the same group with distinct member ids, the server
assigns each member a share of the topic's ranges and scopes its reads to them. The client still never sees
a range id.

## Durability and ordering, precisely

- **Ordering** is per **range**, and therefore per key (a key always hashes to the same range), never
  global across a topic. Note the guarantee is the range's, not the key's: two different keys that land in
  the same range are also ordered relative to each other, but you must not rely on that, because a range
  split can separate them later.
- **Durability**: a produce is acknowledged after a quorum of the segment's replicas has fsynced it.
- **Delivery**: at-least-once. A consumer group commits positions, so a crash between processing and commit
  re-delivers; make your handlers idempotent.

## Where this is going

Ranges splitting is one axis of scale; the **metadata** itself is the other. The control plane is sharded
across virtual nodes (each its own Raft group) and supports online **vnode split** and **grow re-sharding**.
See [NorthGuard port (design)](../NORTHGUARD_PORT.md) for how that works.
