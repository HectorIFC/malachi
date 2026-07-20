# Clustering and re-sharding

A single node is the default and needs no configuration. This guide covers running several, and growing
the shard count while they serve traffic.

## Two independent axes

Confusing these is the main source of configuration mistakes:

- **Node discovery** decides which BEAM nodes can see each other. That is libcluster, and it is
  connectivity only.
- **Cluster membership** decides which nodes hold data. That is `MALACHIMQ_LOG_NODES`, an explicit list,
  because Raft membership must be explicit.

Discovery never adds a node to the data plane. A node that joins the Erlang cluster but is absent from
`MALACHIMQ_LOG_NODES` stores nothing.

```bash
MALACHIMQ_CLUSTER_STRATEGY=gossip     # or kubernetes, epmd. Absent = single node
MALACHIMQ_LOG_CLUSTER=true
MALACHIMQ_LOG_NODES=malachi@10.0.0.1,malachi@10.0.0.2,malachi@10.0.0.3
MALACHIMQ_LOG_REPLICATION_FACTOR=3
```

| strategy | for |
|---|---|
| `gossip` | UDP multicast, good on a flat network |
| `kubernetes` | queries the API for pods matching a selector |
| `epmd` | a fixed list of known hosts |

For Kubernetes use a **StatefulSet**, not a Deployment: each pod needs a stable name so
`MALACHIMQ_LOG_NODES` stays meaningful across restarts.

## What gets replicated, and how

Two planes, replicated by different mechanisms, which is the design's central decision:

- **Metadata** (topics, ranges, segment placement) rides on `ra`, one Raft group per vnode.
- **Records** are replicated by a purpose-built quorum: the primary ships a batch to its followers and
  acknowledges once a quorum has **fsynced** it, tolerating ⌊(N-1)/2⌋ slow or unreachable replicas.

Metadata is small, changes rarely and needs linearizability, which is what Raft is good at. Records are
high-volume and sequential, where routing every batch through a consensus log would pay for a second
durable write to no benefit: the segment already *is* the log.

## Sharding the control plane

With one Raft group for all metadata, that group is a bottleneck. Vnodes shard it:

```bash
MALACHIMQ_LOG_VNODES=8
MALACHIMQ_LOG_VNODE_REPLICATION_FACTOR=3
```

Each vnode owns an arc of the hash ring and runs its own Raft group. A topic's metadata lives in exactly
one vnode, chosen by hashing.

## Growing the shard count

When 8 vnodes are no longer enough, grow while running:

```bash
mix malachi.reshard --to 16
```

Each added vnode is created by **one split**: the vnode owning the largest arc is split at its midpoint,
a new Raft cluster starts, and the displaced topics' metadata migrates to it, fenced and copy-first. The
new ring is then published and gossiped so every node routes to the new shard.

Three properties worth knowing:

- **No existing token moves.** Growth only subdivides, so no topic is relocated except those displaced by
  the one split that covers them.
- **Splits run one at a time**, driven only by the node holding the cluster lease.
- **It is resumable.** The plan is a pure function of the live ring and the target, so if a reshard is
  interrupted, re-run the same `--to` and it continues from where the ring actually is.

Only **growing** is supported. A target below the current count is rejected; draining a vnode into its
successor is a separate, unimplemented operation.

### While a reshard runs

Clients see `:migrating` on metadata writes touching a fenced topic. This is transient and the correct
response is retry, which the bundled scripts already do. See
[Produce and consume](produce-and-consume.md#two-errors-a-correct-client-handles).

### The durability caveat

The ring is **gossiped cluster state, not durable**. On a full-cluster restart it reseeds from
`MALACHIMQ_LOG_VNODES`, whose even geometry does not match a ring grown by splitting, which would orphan
the migrated metadata.

So treat a reshard as **effective while the cluster is up**. This gap is pre-existing and shared with vnode
split; making the ring durable is tracked as follow-up in
[NorthGuard port (design)](../NORTHGUARD_PORT.md).

## Rebalancing

Placement is rendezvous (HRW) hashing, so adding or removing a node changes the desired placement for some
segments. Moving them is **manual by default**:

```bash
MALACHIMQ_AUTO_REBALANCE=true
MALACHIMQ_AUTO_REBALANCE_INTERVAL_MS=30000
MALACHIMQ_AUTO_REBALANCE_STABILIZATION=3
```

With auto-rebalancing on, the lease holder computes a plan each tick and commits it only after the plan is
**non-empty and identical for `STABILIZATION` consecutive ticks** (default 3, so about 90 seconds). A node
that flaps, briefly suspected then alive again, therefore never moves data: the plan changes, the counter
resets, nothing commits.

## Next

For the dashboard, metrics and TLS, see [Operations](operations.md).
