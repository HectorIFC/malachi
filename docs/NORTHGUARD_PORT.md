# Malachi → NorthGuard: an open-source Elixir port design doc

> Status: **proposal** · Chosen strategy: **phased B** (pure Elixir → a Rust NIF only where profiling demands it)
> Feasibility decision recorded in [Feasibility](#1-feasibility-measured) (benchmark in `benchmark/storage_viability.exs`).

Goal: reimplement the **architecture** of [NorthGuard](https://www.linkedin.com/blog/engineering/infrastructure/introducing-northguard-and-xinfra)
(LinkedIn's scalable log storage) as an **open-source Elixir project**, starting from the
malachi of the day (a 100% in-memory TCP broker).

> **Evolved goal (from 2026-06-27):** with the NorthGuard architecture complete and free of any SPOF
> (a multi-node Raft control plane plus a data plane with quorum replication, self-healing, failover,
> catch-up and SWIM membership), the new direction is to make **malachi a scalable, sellable product,
> better than open-source Kafka**, using NorthGuard's concepts. Today there are **two disconnected
> worlds**: the live in-memory TCP broker (a queue/pub-sub model, RabbitMQ style) and the NorthGuard
> stack (a log model, Kafka style). **Priority #1 is connecting them** (exposing the NorthGuard stack
> to the client: "Phase 3" below); scale, sharding included, is no longer out of target.

---

## 1. Feasibility (measured)

An I/O benchmark of the pure BEAM on the storage critical path (Apple M1, OTP 28 JIT, NVMe SSD).
Reproduce it with `elixir benchmark/storage_viability.exs`.

| Scenario (pure Elixir, raw `:file`) | Throughput | Latency/flush |
|---|---|---|
| Durable fsync/batch, 10MB (the NG target) | 472 MB/s · 484k rec/s | p50 **8.0ms** · p99 92ms |
| Durable, 20k×256B (count-driven) | 1234 MB/s · 5M rec/s | p50 3.4ms · p99 12.7ms |
| Non-durable (ceiling) | 612–1507 MB/s | n/a |
| Sequential read | 2487 MB/s | n/a |

**NorthGuard's real target:** ~20 MB/s of writes per broker (steady state), ~60 MB/s with 3x
replication. Pure Elixir delivers **~10 to 50x that** on a laptop. **Throughput is not the bottleneck.**

**Verdict:** phases 0 and 1 in pure Elixir are feasible and fast. The Rust NIF (phase 2) is justified
by three things, none of which is throughput:
1. **Latency tail** (peaks up to ~1.5s observed at max under a large flush; it can get worse under
   real concurrency of thousands of connections plus N segments fsyncing at once).
2. **`O_DIRECT`**: NorthGuard uses Direct I/O to avoid page cache degradation on unconsumed replicas
   and reads of old segments. The pure BEAM does not expose O_DIRECT.
3. **Application-level cache** fed by the consume streams (same reason as item 2).

Caveat: on macOS `:file.sync` does not issue `F_FULLFSYNC`, so the latencies are optimistic against a
Linux server (where fsync is slower but more consistent). Throughput is representative.

---

## 2. Gap analysis: malachi today vs. NorthGuard

Malachi already has **the half the BEAM does better than C++**. What is missing is the durable half.

| Capability | malachi today | NorthGuard | Gap |
|---|---|---|---|
| TCP data plane, connections, sessions | ✅ `tcp_acceptor(_pool)`, `tcp_protocol`, `connection_registry` | sessionized streaming | **Adapt** (pipelining/windowing) |
| Backpressure / flow control | ✅ `backpressure`, `rate_limiter`, `connection_limiter` | per-stream windowing | **Adapt** |
| In-memory queue / partition | ✅ `queue`, `partition_manager`, `consumer`, `ack_manager` | durable range/segment | **Replace the model** |
| **Durable persistence (log)** | ❌ everything in memory | fps-store (WAL, file-per-segment, fsync) | **Build** ← the core |
| **record→segment→range→topic model** | ❌ (queue/partition) | ✅ | **Build** |
| **Sharded metadata (DS-RSM/Raft)** | ❌ | ✅ vnodes + coordinators | **Build** |
| **Gossip membership (SWIM)** | ❌ (local `connection_registry`) | ✅ | **Build** |
| **Striping / self-balancing** | ❌ | ✅ the segment as the unit of replication | **Build** |
| Auth / TLS / metrics / dashboard | ✅ `auth/*`, `tls_validator`, `metrics`, `dashboard` | (LinkedIn-internal) | **Keep** |
| Storage policies / attributes | ❌ | ✅ expressions over attributes | **Build** |

---

## 3. Target architecture

### 3.1 Data model (same as NorthGuard)

```
Topic   ── a named collection of Ranges covering the whole keyspace
 └ Range  ── a LOG abstraction: segments for a contiguous band of keys (active|sealed)
    └ Segment ── THE UNIT OF REPLICATION: a sequence of records (active|sealed; seals at 1GB / 1h / failure)
       └ Record ── key + value + headers (bytes), a logical offset within the segment
```

- **Range split and merge are purely logical metadata operations**, segments are never physically
  combined or copied (confirmed in the meetup video). Merge happens only between *buddy ranges*
  (buddy-allocator style).
- Total ordering is preserved through happens-before on splits and merges.

### 3.2 Storage layer, `Malachi.SegmentStore` (a pluggable behaviour)

NorthGuard says storage is pluggable ("fps-store" is only the primary implementation). We mirror that:

```elixir
@callback open(seg_id, opts) :: {:ok, handle} | {:error, term}
@callback append(handle, batch :: [record]) :: {:ok, base_offset} | {:error, term}
@callback sync(handle) :: :ok | {:error, term}        # fsync, called before the ack
@callback read(handle, offset, max_bytes) :: {:ok, [record]} | :eof
@callback seal(handle) :: :ok                          # makes it immutable
@callback sparse_index(handle, offset) :: {:ok, file_pos}
```

- **Phases 0 and 1:** `Malachi.SegmentStore.Elixir`, `:file` `[:raw, :binary]`, batching by
  10ms / N records / N bytes, fsync before the ack, a sparse index in ETS/DETS or an `.idx` file.
- **Phase 2 (conditional):** `Malachi.SegmentStore.Native`, a Rust NIF (Rustler) with O_DIRECT,
  aligned buffers and an app-level cache; the index in `erlang-rocksdb`.

### 3.3 Metadata, DS-RSM over `ra`

- **vnode** = a Raft group (`ra`) holding one shard of the metadata (topics/ranges/segments).
- **coordinator** = the vnode's leader; it carries the "business logic" (seal or delete a topic,
  split or merge a range, segment replica sets, self-healing of under-replicated segments).
- **DS-RSM** = vnodes over a hash ring (consistent hashing). Hash by topic name, and by range id for
  ranges and segments. **A vnode's position on the ring is stable** even as Raft replicas join and
  leave (a detail from the video). **vnodes can split** (breaking the state into two Raft groups).

### 3.4 Membership, SWIM

- `partisan` (which supports SWIM) or our own implementation. Random probing for detection plus
  infection-style dissemination. It spreads **minimal global state**: broker host/port/attributes plus
  vnode boundaries/leader/term (for routing).

### 3.5 Protocols

- **Metadata = unary** (1 request → 1 response): create/delete/topicMetadata/segmentMetadata.
  Any broker acts as a proxy and routes to the vnode leader using the gossiped state.
- **Data = sessionized streaming** (produce/consume/replication) with pipelining and windowing.
  It reuses malachi's `tcp_protocol` and `backpressure`. Consume can use `:file.sendfile`.

### 3.6 Storage/metadata policies

- A policy is a name plus retention plus constraints. A constraint is an expression over
  **attributes** (opaque k/v that admins attach to brokers). This generalizes rack and DC awareness
  without the core understanding what a "rack" is. It decides segment replica sets and vnode
  replicas.

---

## 4. Phased roadmap

> **AUDIT STATUS (2026-07-19).** The core of the port is functionally **complete**: phase 0 (the durable
> log model) ✅, phase 1 (distribution: DS-RSM over `ra`, SWIM membership, quorum replication) ✅, and
> phase 3 / scale (a **sharded** control plane over vnodes, **vnode split over `ra`** end to end,
> **heal** re-replication, **primary failover**, **multi-node consumer groups** A1 to A5,
> lease/rebalancing including the opt-in auto-rebalancer) ✅. **Auth and security** (phases P1 to P6 plus
> mTLS/OIDC plus per-topic ACL) ✅, **paused on maturity** (2026-07-19). The old 🚧/⏳ markers on the
> split/heal/failover/coordinator lines were **corrected in that audit**. They were stale. LDAP and
> tenant/namespace multi-tenancy (5-1B) were **declined** by decision (see `AUTH_USER_MANAGEMENT.md`).
>
> **Update (2026-07-20).** **Re-sharding (grow)** is no longer open: RS-1/RS-2/RS-3 delivered
> (`ReshardPlan`, `ReshardCoordinator`, the wiring plus `mix malachi.reshard --to N`), see the end of
> §8.4. Section 7, which listed four "open questions", became a **decision record**: all four were
> already implemented. **Genuinely open now:** (1) a **durable ring** (reshard and split do not survive a
> full-cluster restart, ⏳ noted in §8.4), (2) vnode **merge/shrink** (recorded as out of scope), (3)
> **phase 2, native efficiency** (a Rust/RocksDB NIF, **conditional on profiling**). The
> **documentation trail** (§9) is closed: 9 guides, the ExDoc site and the Pages workflow. All that is
> left of it is an **operator action**, switching the Pages source to "GitHub Actions", which no commit
> can perform.

### Phase 0: persistence and the log model (pure Elixir)
- ✅ The `Malachi.Storage.SegmentStore` behaviour plus the `Malachi.Storage.ElixirStore`
  implementation (file-per-segment, batching, fsync-before-ack, sparse index, sealing, crash
  recovery, a cheap `open_read` for sealed segments through the `.idx`).
- ✅ The `Malachi.Log.Record` (framing + CRC32) and `Malachi.Log.Segment` (logical offsets) types.
- ✅ `Malachi.Log`, the multi-segment log: automatic rolling and sealing (size/age), continuous
  reads crossing segments, recovery of the whole directory (scanning only the last segment).
- ✅ Flush by **size** (`:flush_bytes`, default 10MB, NorthGuard's size trigger): `append`
  flushes and fsyncs automatically on reaching the limit.
- ✅ `Malachi.Range`. A log abstraction over the `[0, 2^bits)` keyspace: keys by hash
  (`:erlang.phash2`), ranges as buddy-allocator blocks, **logical split and merge** (they do not
  move data), a single buddy, and lineage (`parents`) for **happens-before**.
- ✅ `Malachi.Topic`. A named collection of ranges that **cover the whole keyspace**: routing
  records by key (`route`/`append`), orchestrating split and merge while preserving coverage,
  sealing, and retaining sealed ranges for reads. Topic metadata is **in memory** for now
  (durable persistence arrives with phase 1's DS-RSM).
- ✅ A sparse index in an `:array` (binary search O(log n), insert O(log n)).
- ✅ **Chunked recovery** (`scan_segment`): it never loads a whole segment into memory.
- ✅ **Flush by count** (`:flush_count`, default 20k) in `ElixirStore`, plus **flush by time**
  (~10ms) and **serialized concurrent access** in `Malachi.TopicServer` (a GenServer wrapping the
  Topic, with a flush timer and a flush on shutdown).
- ✅ **Cross-epoch reads** (`Topic.read_history/2`): the history of one key across a split
  (parent epoch → child epoch), through lineage plus happens-before.
- ✅ Tests: property-based (`stream_data`) plus unit, covering append/read/seal/crash-recovery/roll
  and auto-flush (size and count), split/merge/buddy/happens-before, coverage/routing/cross-epoch,
  and flush-by-time plus concurrency in TopicServer. **66 tests + 2 properties.**

> **Phase 0 complete.** The only item deferred on purpose: **durable persistence of topic
> metadata** (which ranges exist). It goes into **phase 1's DS-RSM**, faithful to NorthGuard, where
> that metadata lives in the coordinator/vnode (Raft).

### Phase 1, distribution

Confirmed strategy: **pure logic first, `ra` afterwards** (the same pattern as
`ElixirStore`→Rust NIF). The metadata state machine is designed against a Raft's `apply/2`
contract, so `ra` plugs in without rework.

**Phase 1a, DS-RSM (pure logic, no dependencies):**
- ✅ `Malachi.Cluster.HashRing`. Consistent hashing: vnodes at tokens on the `[0, 2^bits)` ring,
  `route` by ceiling with wraparound, `boundaries` (a vnode's arc), add/remove. Minimal movement
  when adding or removing a vnode (tested).
- ✅ `Malachi.Keyspace`, the shared keyspace math (`size_for_bits!`, `position_of`, `within?`,
  `splittable?`, `split_point`, `buddies?`); `Range` and `HashRing` were refactored to use it
  (killing the duplicated hashing plus the `1..32` validation).
- ✅ `Malachi.Metadata`: the vnode/coordinator state machine (a **deterministic** `apply/2`, the
  Raft contract): topics, ranges (buddy split/merge through `Keyspace`, lineage) and segments
  (replica set, active/sealed state). Range ids come from a counter in the state (no
  `unique_integer` or timestamp inside `apply`, so it is safe to replicate). **It resolves the
  topic-metadata persistence deferred from phase 0.** Tested including log replay (determinism)
  and a defensive catch-all (an unknown command returns an error instead of taking the replica
  down).
  - ✅ Secondary indexes `%{topic => range_ids}` and `%{range_id => segment_ids}`. Until then
    `ranges_of_topic`/`segments_of_range` scanned **every** range and segment in the vnode (O(n)), and
    they sit on the hot path (every produce routes through `active_ranges_of_topic`; every consume reads
    `segments_of_range`), so the cost grew with the total accumulated metadata (retention keeps sealed
    ones). Sliced up:
    - ✅ **V-idx-a: maintain and validate the index.** `Metadata` gains `topic_ranges`/`range_segments`
      (MapSets, where an entry exists iff it has at least one member), maintained **inside the
      deterministic `apply/2`** (not a side cache: it is replicated identically on every `ra` node) on
      every membership mutation: `create_topic`, `split_range`, `merge_ranges`, `register_segment` (+),
      `delete_segment`, `delete_topic` (−), and the `extract_topic`/`insert_topic` migration;
      seal/set_replicas/commit/policies do not touch it. `DSRSM.merged_metadata` merges the index too
      (shards are disjoint by topic, so `Map.merge` is exact). **No reader uses it yet**, the scans stay
      intact. Property test: the index equals an index rebuilt by scanning, after any sequence of commands
      (catching missing, extra, stale and empty entries); the determinism property already covers the index
      (it compares the whole state). 685 tests, 0 failures; credo and dialyzer clean.
    - ✅ **V-idx-b: switch the readers to the index.** `ranges_of_topic`/`segments_of_range` become a
      `Map.get(index) |> Enum.map(&Map.fetch!(...))` lookup, so **O(1)+O(k)** (the `fetch!` is safe by
      V-idx-a's invariant, and doubles as a canary should it ever diverge). The internal per-topic scans
      became index-based as well: `seal_topic` (sealing only that topic's ranges) and `delete_topic`
      (dropping only that topic's ranges and segments), which removed the scan helpers
      `seal_ranges_of_topic`, `range_ids_of_topic` (the scanning one) and `drop_segments_in`. The `DSRSM`
      path gets the index for free (it delegates to `Metadata`'s readers through `query`; merging the index
      in `merged_metadata` came in V-idx-a). Precondition verified: no `%Metadata{}` is built from data
      without an index (they all come from `apply`, merge or migration). The old readers were already
      unordered, so no caller depended on ordering, hence no regression. Validated by the whole suite
      (broker/DSRSM/produce/consume exercise the readers on live paths) plus the properties. 686 tests, 0
      failures; credo and dialyzer clean.
    - ✅ **V-idx-c: measure the gain.** `bench/metadata_index_bench.exs` compares the index against a scan
      (reimplemented inline) over the same Metadata, with fixed-size topics (3 ranges, 2 segments each) and
      a growing total. Result: the index is **flat** (~0.2 µs, O(k)) while the scan grows **linearly**
      (O(n)), from ~7.5 µs (300 ranges) to **~7.3 ms** (150k ranges) per `ranges_of_topic` call, and to
      ~9.9 ms for `segments_of_range` (100k segments). Speedup ranges from ~12x (small) to **~33,000x**
      (ranges) and **~67,000x** (segments). Since this was a tax **per produce and consume**, at 50k topics
      the scan alone cost milliseconds per message; the index zeroes it. **The secondary index
      (V-idx-a/b/c) is complete.**
- ✅ `Malachi.Cluster.DSRSM`. It ties everything together: a HashRing plus one `Metadata` per vnode;
  `command/3` and queries routed by **topic name** to the owning vnode (sharding topics across vnodes,
  tested). Deterministic (replay). Phase 1a decision: a topic's metadata is **co-located** in one
  vnode (routed by name), a recorded deviation from NorthGuard's range-id sharding.
- ✅ **Vnode split** (the "D", Dynamic, in DS-RSM): `DSRSM.split_vnode/3` adds a vnode and
  **migrates** the displaced topics (topic + ranges + segments) to it. Made possible by the
  `{topic, seq}` range id (globally unique, so no collision during migration); helpers
  `Metadata.extract_topic/2` and `insert_topic/2`. Tested: migration without loss, and ranges and
  segments follow the topic.
  - ⏸️ **A deliberate deviation (unplanned): sharding ranges and segments by range id (cross-vnode).**
    NorthGuard shards the *metadata* by range id; malachi co-locates a topic's metadata in one vnode
    (routed by name). **Decided against** (after evaluation): (1) the **data is already sharded across
    nodes**: every segment has replicas placed by HRW on distinct nodes, so a busy topic already spreads
    data over ranges→segments→nodes; only the *granularity of metadata management* differs. (2) Sharding
    **by topic already spreads the metadata load** across the cluster; range sharding would only scale the
    rate of *structural mutation* (splits and segments per second) for **a single** topic beyond one Raft
    group, which is a very high bar (metadata mutations are not per record). (3) The cost is
    reintroducing **cross-Raft transactions** for topic operations (seal and delete touch ranges in N
    vnodes), distributed range-id allocation, cross-vnode `create`/`split`, and **scatter-gather** reads
    (undoing V-idx's O(1) index), which is exactly the complexity co-location avoids. The worst
    cost/benefit on the roadmap; **revisit only if a concrete single-topic metadata bottleneck appears**.

**Bridging the control plane to the data plane (1a.5):**
- ✅ `Malachi.Broker`: it composes the control plane (`Metadata`, the source of truth for structure)
  with the data plane (one `Log` per range, indexed by range id). `produce` routes by key using
  `Metadata`'s active ranges plus `Keyspace`; `split_range`/`merge_ranges` go through `Metadata` (the
  structure) and seal or flush the affected logs. The logical decisions live in the control plane only.
- ✅ **Cross-epoch** reads moved into `Broker` (`read_history`/`stream_history`, lineage through
  `Metadata.parents`) plus `Broker.pending?`. **Topic-name** validation in `Metadata.create_topic` (an
  allowlist rejecting `..` and `/`) closes the data plane's path traversal, in the control plane.
- ✅ `Malachi.BrokerServer`. A GenServer over `Broker`: flush by **time** (~10ms) plus **serialized**
  access (concurrency), and a flush on shutdown. Parity with the old `TopicServer`.
- ✅ **Duplication removed:** `Topic`, `TopicServer` and `Range` were **deleted** (their
  split/merge/coverage/lineage orchestration duplicated `Metadata`). The single path is now the control
  plane (`Metadata`/`DSRSM`) plus `Broker`/`BrokerServer`. **The bridge is complete.**

**Hardening the DS-RSM (property-based, our substitute for deterministic simulation):**
- ✅ Model-based property tests (`stream_data`) for `Metadata` and `DSRSM`: random sequences of
  create/split/merge/register/seal/delete plus **vnode split**, always picking targets that are
  valid in the current state. Invariants checked: keyspace coverage per active topic, referential
  integrity (range→topic, segment→range, a well-formed `{topic, seq}` id), **no orphan topic**
  (it always lives in the vnode that routes to it, even after a vnode split) and **determinism**
  (the same sequence yields the same state).

**Phase 1b, replication and membership:**
- ✅ **`ra` integrated** (real Raft): `Malachi.Cluster.MetadataMachine` is a `:ra_machine` whose
  `apply/3` **delegates to `Metadata.apply/2`**, so the business logic does not change, it only gains
  replication. `Malachi.Cluster.MetadataServer` is the thin wrapper (starting a single-node cluster,
  `command` through the Raft log, a linearizable `query`, restart). One ra cluster equals one vnode;
  leadership equals the coordinator. Tested: commands replicated, query consistent, a command error
  propagated, and **state surviving a restart** (a durable log). `Metadata`'s determinism (the property
  tests) is what makes that safe.
- ✅ **`Malachi.Cluster.ReplicatedDSRSM`**. The DS-RSM over Raft: a HashRing plus **one `ra` cluster per
  vnode**. `command`/`query` route by topic name to the owning vnode's cluster (sharding plus
  per-shard replication). The production counterpart of the pure `DSRSM` the property tests exercise.
  Tested: routing to the right cluster, consistent query, **sharding across 2 real Raft clusters**
  (each topic exists only in the cluster that routes to it), a command error propagated, and an empty
  ring yielding `:no_vnode`. Scope: **static vnodes** (split-over-ra deferred), single-node (multi-node
  depends on membership).
  - ✅ **Vnode split over `ra` (epic COMPLETE. Migrating metadata between Raft groups, which is what
    NorthGuard does:** *"break the state in half and basically spawn another raft group"*, from the meetup
    transcript). The pure single-process logic (`DSRSM.split_vnode` migrates displaced topics through
    `extract_topic`/`insert_topic`) and the **orchestration over the real `ra` groups were both built end to
    end** (VS/Int/Hardening A-C/1B below: a real split working, see `VnodeSplit`/`SplitCoordinator`).
    **Ring architecture decision (corrected for fidelity):** the ring (topology) is **minimal global state
    disseminated by gossip (SWIM)**, which is what NorthGuard does (transcript: *"we also use this
    dissemination for spreading some minimal global like cluster metadata"*; *"very minimal global states"*),
    **not** a topology `ra` cluster (more CP, but less faithful). It reuses the CRDT attribute gossip
    malachi's SWIM already has. Slicing: **VS-1** pure primitives plus commands · **VS-2b** a versioned,
    disseminated ring (the ring is a prerequisite: without propagation a split is inconsistent between
    nodes) · **VS-2a** split orchestration over `ra` (bring the new group up, migrate through the log,
    attach the ring version) · **VS-2c** consistency (the cutover window) · **VS-3** a multinode test.
    - ✅ **VS-1: complete migration (with offsets) plus deterministic commands.** A finding this slice
      fixed: `Metadata.extract_topic` carried topic, ranges and segments but **not** the **committed
      offsets** (keyed by `{group, topic}`), so on a split a consumer group **lost its position** and
      reprocessed everything (at-least-once, no data loss, but a serious regression). Now
      `extract_topic`/`insert_topic` **carry the offsets** (in the export they are keyed by group, since the
      topic is implicit); the policy binding already travelled in the topic struct. And, so the ra-backed
      split can drive the migration through each vnode's **Raft log**, new deterministic commands
      `:extract_topic` (whose reply is the export) and `:insert_topic` in `Metadata.apply/2` (with the
      defensive catch-all intact). Pure, in-VM, zero network. Tested: extract carries per-group offsets and
      leaves co-located ones intact; the round trip preserves the position; the commands relocate through the
      log; extracting an absent topic is a nil no-op; plus coverage in `DSRSM` (a split preserves offsets end
      to end). Suite at 786 tests, 0 failures (+5); credo, dialyzer and format clean.
    - ✅ **VS-2b-1: a versioned, disseminable ring (a pure CRDT core).** The foundation of propagation: a new
      `Malachi.Cluster.RingTopology`, the routing topology (`HashRing` + `%{vnode => [nodes]}`) tagged with a
      monotonic `version`. **Why gossip rather than an `ra` cluster:** the user asked "isn't B more faithful?"
      and the transcript confirmed it (line 537: it disseminates minimal global metadata over SWIM; line 610:
      minimal global state). I had recommended the `ra` cluster out of CP instinct, and was **corrected** on
      fidelity. `merge/2` is a **CRDT join**: the higher `version` wins (last-version-wins), with a
      deterministic tiebreak by total order of the serialization in the rare same-version clash (it is
      single-writer: only the leader calls `advance`), so the merge is
      **commutative, associative and idempotent** and converges in any gossip order. Pure, zero network.
      Tested: `new`/`advance` (monotonic version); merge keeps the higher version, is idempotent, commutative
      (including in a clash), associative, and converges to the last version in any order: 6 tests. Suite at
      792 tests, 0 failures (+6); credo, dialyzer and format clean.
      - ✅ **VS-2b-2a: disseminating the `RingTopology` over SWIM gossip.** `MembershipServer` now **carries
        the topology along** with every gossip message (the same dissemination path NorthGuard uses for
        minimal global state). A **minimal-churn** design: one `gossip_payload/1` (view + topology) replaces
        the payload builder in all 8 sends, and `merge_updates` now accepts the `{updates, topology}` payload,
        with a **defensive clause** for a peer that sends only the list (new-receives-old, for example test
        injection or pre-topology gossip), plus a nil-tolerant `merge_topology/2` doing VS-2b-1's CRDT join.
        **A recorded limitation:** an **old** node receiving the new payload would break, so a full rolling
        upgrade of the SWIM protocol is not a goal right now (homogeneous deploy). New API: `set_topology/2`
        (adopting the higher version locally, with gossip carrying it onward) and `topology/1`. A test handler
        that inspected the raw payload was adjusted to the new shape. Tested in-VM: `set_topology` on one node
        **propagates** by gossip; the **higher version wins** on every node regardless of who set it
        (last-version-wins); and the membership multinode HA test stays **green** (real gossip with the new
        payload does not regress). Suite at 794 tests, 0 failures (+2); credo, dialyzer and format clean.
        - ✅ **VS-2b-2b. Adopting the ring locally when the version advances.** This closes propagation:
          `MembershipServer` gains an **`on_topology`** seam (the same pattern as the other seams, such as
          `ranges_fun`) that fires **only on a version advance** (`topology_advanced?`: `nil→anything`, or
          `new > old`), not on an equal or lower version, carrying the new `RingTopology`, whether it arrived
          through `set_topology` or through gossip. The app wires `on_topology` (`adopt_ring_topology/1`) to
          **apply the new ring to `CoordinatorRouter`**: it derives `servers` (`%{vnode => {vnode, node}}`,
          any member, since the router resolves the live leader) from the `placements` and calls
          `put_topology`. The hook runs **inline** in the server (so it must be light and must not raise, and
          the app respects that). **Scope:** consumer-group routing only (`CoordinatorRouter`); adopting
          **metadata** routing (`ReplicatedDSRSM`, coupled to the broker runtime) is driven by the split
          orchestration itself (VS-2a), not by gossip. Single-node is unaffected (`MembershipServer` only
          starts in clustered mode). Tested in-VM: the hook fires on an advance (v1, then v2), does **not**
          fire on an equal or lower version (`refute_receive`), and fires on a node that learns a higher
          version **through gossip**; single-node boot fine; membership multinode HA green. Suite at 796
          tests, 0 failures (+2); credo, dialyzer and format clean. **VS-2b (ring propagation by gossip) is
          complete.**
      - ✅ **VS-2a: split orchestration over `ra` (copy-first).** `ReplicatedDSRSM.split_vnode/4` brings the new
        vnode's `ra` group up, works out the **displaced** topics (those that now route to the new one under the
        new ring) and **migrates** them between the `ra` groups, which is what NorthGuard does (*spawn a new raft
        group + break off that half of the state*). A **copy-first** decision (chosen for safety): per topic,
        `:insert_topic` on the new one, then `:extract_topic` on the source, so **no single failure loses a
        topic** (a crash after the insert leaves a harmless duplicate the new ring ignores). A linearizable
        snapshot of the source (`&Function.identity/1`, not a closure, so it runs on the leader) → compute the
        displaced set and the exports **locally** (pure) → apply the commands. Supporting copy-first: a new
        read-only `Metadata.export_topic/2`, with `extract_topic` **refactored to reuse it** (DRY). It returns
        the grown state only on total success; a migration failure yields `{:error, {:migrate, topic, ...}}`
        (a partial split is left to reconcile). **Scope:** the mechanism. **Out of scope:** fencing concurrent
        writes to a migrating topic (VS-2c) and publishing the ring through `set_topology`, plus the coordinator
        wiring (integration). Tested under `:multinode` (local `ra` groups, stable 3x): splitting a vnode with a
        topic and offsets, "orders" starts routing to the new vnode, the topic **and its committed offsets** are
        preserved and read back from the new `ra` group, and the source **no longer** has the topic (copy-first
        extract), plus a unit test for `export_topic` (read-only, nil when absent). Suite at 796 tests, 0
        failures (+1 default, +1 multinode); credo, dialyzer and format clean.
      - ✅ **VS-2c-1a. The migration fence (core, seal-first).** The problem it closes: during a migration, a
        concurrent write to the topic (routed to the source, since the ring is still the old one) **after** the
        snapshot would be lost (the extract removes the current state, but the insert used the snapshot). The
        fix is faithful to NorthGuard (transcript: *"we first seal R1 to create R2 and R3"*, since sealing is
        what gives the ordering guarantees): **fence** the topic. New `migrating` state (a set `%{name => true}`:
        a map rather than a `MapSet`, to avoid dialyzer's opaque-type friction with a struct field) plus
        `:begin_migration`/`:end_migration` commands. The public `apply/2` became a **central guard**: the 15
        clauses were renamed to `do_apply/2`, and `apply/2` now resolves the command's **target topic** before
        dispatching (`command_topic/2`, directly for topic-scoped commands, and through `range`/`segment →
        topic` for range and segment ones) and **rejects with `{:error, :migrating}`** when it is fenced. Read
        commands and the migration commands themselves (create/define_policy/begin/end/extract/insert) are
        **never** fenced. `extract_topic` clears the fence (the topic is gone). Pure, deterministic. Tested:
        `begin_migration` rejects mutating commands (including a range command that resolves to the topic)
        leaving the state intact; reads and extract/insert pass through; `end_migration` lifts it; fencing one
        topic does not affect a co-located one; begin on an absent topic errors: 5 tests. Suite at 802 tests, 0
        failures (+5); credo, dialyzer and format clean.
      - ✅ **VS-2c-1b: `split_vnode` fences the source before the snapshot.** It uses VS-2c-1a's fence in the
        orchestration: `migrate_from` now (1) reads the source to find the displaced topics, (2) issues
        **`:begin_migration` on each** (`fence_topics`), (3) **re-reads** the now-stable source (the re-read
        catches any write that slipped into the window before the fence), and (4) migrates copy-first from the
        fenced snapshot. `:extract_topic` **lifts** the fence (the topic is gone). So no concurrent write can
        race the copy, which closes the gap VS-2a documented. On a partial failure the remaining fences **stay
        up** (writes to those topics blocked) for reconciliation. Fail-safe. A recorded caveat: a topic
        **created** during the split that routes to the new vnode is not caught (create is not fenced); the
        caller quiesces. Tested (`:multinode`, stable 2x): the happy split stays green (fence applied, then
        lifted), and, new here, the migrated topic is **writable on the new vnode** (the fence did not leak to
        the destination; a fenced topic would answer `{:error, :migrating}`). Suite at 802 tests, 0 failures;
        credo, dialyzer and format clean.
      - ✅ **Int-1a: `BrokerServer.adopt_topology/2` (pure adoption of metadata routing).** The integration
        challenge: the broker's metadata routing (the `dsrsm` cache holding the ring, plus the `command_fun`
        over `replicated`) is **captured at boot** with a fixed ring, so a runtime split is never adopted.
        `adopt_topology` is the **pure function** (the pure-first sub-slice) that, given the `Broker` plus a
        `RingTopology`, rebuilds the routing: the cache adopts the **new ring** (existing vnodes keep their
        cached `Metadata`, a new vnode starts **empty** until the next `ra` refresh) and `command_fun` is
        rebuilt over the `%{vnode => server_id}` derived from the `placements` (skipping an empty placement, as
        `adopt_ring_topology` does). Pure: catching the new vnode's metadata up is the separate side effect
        (refresh). Tested in-VM: it adopts the new ring (v0+v1), v0 keeps the cached topic, v1 is empty, and
        `command_fun` is rebuilt (arity 3). Suite at 803 tests, 0 failures (+1); credo, dialyzer and format
        clean.
      - ✅ **Int-1b: `BrokerServer` adopts the topology through gossip (async).** This closes runtime adoption
        of metadata routing: a `handle_cast({:adopt_topology, topology})` rebuilds the routing (Int-1a's
        `adopt_topology/2`) **and**, the part that was missing for correctness, points `metadata_refresh` and
        `bootstrap.replicated` at the new `replicated`. Otherwise the periodic `reconcile_metadata` (which does
        `snapshot(old_replicated)` plus `put_cache`, replacing the whole cache, ring included) would **revert**
        the adopted ring. The `on_topology` hook (VS-2b-2b, `adopt_ring_topology`) now, besides updating
        `CoordinatorRouter` inline, issues a **`GenServer.cast`** to `Malachi.LogBroker`: **async** (so it does
        not block the membership server, which runs the hook inline) and a **no-op when the broker does not
        exist** (a `cast` to an absent name is `:ok`, verified; single-node does not run the sharded broker).
        DRY: deriving `servers` became `RingTopology.servers/1` (used by both the broker and the router). Tested
        in-VM: the `handle_cast` adopts the new ring in the cache **and** points `bootstrap.replicated` and the
        refresh at the new ring (reconcile does not revert it); `servers/1` is pure (skipping an empty
        placement). Suite at 805 tests, 0 failures (+2); credo, dialyzer and format clean. **Int-1 (the broker
        adopts at runtime) is complete.**
      - ✅ **Int-3: an end-to-end split coordinator (under the lease) plus a boot baseline.** **Int-3a:** boot
        (in sharded mode) now **seeds a version-0 topology** into membership (`membership_child` passes the
        `:topology` built from the `vnode_placement`), so gossip carries the ring and a split can **advance**
        from it; single-node or 1-vnode has nothing to route, so it gets no topology. **Int-3b:** a new
        `Malachi.Cluster.VnodeSplit.split/5`. Only the **lease holder** acts (the `leader?` seam; otherwise
        `{:error, :not_leader}`): it reads the current `RingTopology` from membership, rebuilds the
        `ReplicatedDSRSM`, calls `split_vnode` (the fenced copy-first migration over `ra`, VS-2a/2c),
        **advances** the version and **publishes** it through `set_topology`. From there gossip disseminates it
        (VS-2b) and every node adopts the new ring for metadata routing (the broker, Int-1) and consumer-group
        routing (the router, VS-2b-2b). It ties every VS and Int slice into a real split. Tested under
        `:multinode` (stable 2x): a non-leader refuses; the leader splits, and membership's topology
        **advances to v1** with the new vnode published, **and** the migration happened over `ra` ("orders"
        starts routing to and living in the new vnode's cluster). Suite at 805 tests, 0 failures (+1
        multinode); credo, dialyzer and format clean. **Vnode split over `ra` (VS + Int): what NorthGuard does,
        working end to end.**
      - ✅ **Hardening C. Node client retry on `:migrating`.** This closes client resilience during a split,
        the analogue of the `:not_owner` retry (A5) and of what NorthGuard does on a change (transcript:
        *"seal it, make a new one, move the producers over"*, that is: seal it, create the new one and **move
        the producers**). `:migrating` shows up on **metadata** writes to a migrating topic, on **produce**
        (when rolling a segment) and on **commit** (offset). Node-only: `isMigrating(err)` in `client.js` (a
        `MalachiError` whose message is `"migrating"`) plus a **shared** `withRetry(fn, retryable, ms, onRetry)`
        helper in `cli.js` (retrying with back-off while the error is transient; a non-retryable one
        propagates). `producer.js` (the produce batch) and `consumer.js` (the commit before advancing the
        cursor) go through `withRetry(..., isMigrating, ...)`, print `~` and retry against the new location once
        the split finishes. Validated with `node --check` on the scripts plus sanity checks of `isMigrating` and
        `withRetry` (retry-then-ok; a non-retryable error propagates). No JS test harness (the standard for Node
        client slices). **Client resilience to the split lifecycle (`:migrating` + `:not_owner`) is closed.**
      - ✅ **Hardening A: `SplitCoordinator` (driving the split under the lease).** This closes the gap of
        `VnodeSplit` being only a function: a new `Malachi.Cluster.SplitCoordinator` (GenServer) **serializes**
        splits (one at a time, since a split mutates the ring and two would race) and is wired into the tree
        under `rebalance_children` (the sharded control plane). It is the NorthGuard model, *"the coordinator is
        responsible for carrying it out"*: only the **lease holder** acts (the `leader?` seam being
        `LeaseHolder.leader?`, as in `RebalanceCoordinator`), it reads and publishes the topology through
        membership (`Malachi.LogMembership`), and `split/4` delegates to `VnodeSplit.split/5` (a fenced
        migration over `ra` → advance the version → `set_topology` → gossip propagates → every node adopts).
        **Operator-driven** (nothing splits on its own; an operator, or a future policy, calls `split/4`), like
        `RebalanceCoordinator`. The seams keep it testable without a lease or `ra`. Single-node and non-sharded
        do not start the coordinator. Tested in-VM (with injected seams): it refuses with
        `{:error, :not_leader}` when not the leader; as leader it delegates to `VnodeSplit` (with no baseline
        topology, `{:error, :no_topology}`). Single-node boot fine. Suite at 807 tests, 0 failures (+2); credo,
        dialyzer and format clean. **Vnode split is now driven at runtime under the lease. Hardening still
        open: reconciling a partial split (VS-2c-2: the coordinator resuming or completing an incomplete split
        after a failover, NorthGuard's "carrying it out to the end").**
      - ✅ **Hardening B1: `split_vnode` rolls back on a migration failure.** Before, a migration that failed
        midway left partial state (a topic already on the new vnode plus fences up), all of it for future
        reconciliation. Now `migrate_displaced` is **all or nothing**: if any source fails, it **reverts**,
        moving every topic that reached the new vnode back to its owner under the **old** ring (`move_topic` in
        the reverse direction: `insert` on the source → `extract` from the new one, the same copy-first) and
        **releasing** any fence left on a source (scanning each one's `meta.migrating` and issuing
        `:end_migration`). The rollback is **derived from state** (nothing is tracked step by step): it reads
        the new vnode's metadata and re-routes each topic through the old ring, so it is idempotent; a rollback
        step that fails is swallowed (left to manual recovery or the coordinator), but the common case never
        **orphans** a topic nor **traps** a fence. A DRY unification: the outbound `migrate_topic` became
        `move_topic(from, to, export, name)`, reused in both directions. Iteration over sources is now
        **ordered by id** (`Enum.sort`), so a split is reproducible and a partial-failure state is predictable.
        Tested (`:multinode`): (1) a split that fails to bring the new vnode up does not touch the source (it
        fails **before** fencing); (2) the new one, a split that fails **midway** (the second source with its
        `ra` cluster down, after the first had already migrated "orders"), **reverts**: "orders" returns to its
        source with offsets intact and no trapped fence (writable again, not `{:error, :migrating}`), and the
        new vnode is left **empty** (nothing orphaned). Suite at 807 tests, 0 failures (+1 multinode); credo,
        dialyzer and format clean. **Hardening still open: B2, saga/intent reconciliation for a coordinator
        crash *in the middle* of a split (VS-2c-2, NorthGuard's "carrying it out to the end").**
      - ✅ **Hardening B2: reconciling a split interrupted by a coordinator crash** (strategy **1A: a durable
        intent plus abort**, as chosen). B1 covers *logical* failures inside one `split_vnode` call; it does
        **not** cover the coordinator (process or node) **dying midway**, where B1's in-process rollback never
        runs and the durable state (in the `ra` logs) is left half done: (sub-case 1) everything was copied but
        the topology was not published, so the ring still routes to the emptied source, an **apparent loss**;
        (sub-case 2) only part migrated, so fences are trapped and writes blocked forever. NorthGuard solves
        this because the split is driven by the **vnode's coordinator** (the leader of a raft group): the intent
        lives in the raft log and the **new leader resumes** it ("carrying it out"). In malachi the split is
        driven by the **global lease**, so we need a **durable intent** plus a **reconcile on failover**.
        Decision (1A): reconcile by **abort** (reverting through B1's `roll_back`) rather than completing
        forward, which is simpler and safer, and the operator re-issues; aborting sub-case 1 brings the
        furniture back (nothing disappears). Promotable to *complete-forward* (1B) later on the same
        foundation.
        - ✅ **B2-1: the durable intent in `RingTopology` (pure core).** A new `pending` field
          (`%{new_vnode, token, nodes}` | `nil`) carrying what a reconciler needs in order to identify and undo
          the split (the *old* ring and placements are the topology's own, since a pending split has **not**
          advanced the ring yet). Transitions: `begin_split/4` records the intent at `version + 1` **without
          touching the ring** (gossip disseminates it *before* the migration, so a failover on any node can find
          it); `advance/3` (completing) moves the ring forward **and clears** the intent; a new
          `clear_pending/1` (aborting) clears the intent while **keeping** the ring. The intent travels over the
          same gossip/CRDT (`merge/2` carries the higher version's `pending`, with no change to the merge
          logic). Tested: `new` and `advance` leave nothing pending; `begin_split` records the intent without
          moving the ring; `advance` completes (ring forward plus intent cleared); `clear_pending` aborts
          (intent cleared, ring intact); the intent converges through `merge`. Suite at 812 tests, 0 failures
          (+5); credo, dialyzer and format clean.
        - ✅ **B2-2: `VnodeSplit` records and clears the intent around the migration.** `do_split` now
          **publishes** the intent before migrating (`begin_split` → `set_topology`, gossiped with the *old*
          ring, v+1) so a failover can find it; on **completion**, `advance` (from the `pending`, not from
          `current`, which would collide on the version) moves the ring forward **and clears** the intent
          (v+2); on a **logical failure**, `split_vnode` has already reverted in-process (B1), so only
          `clear_pending` (v+2, ring intact) discards the now-obsolete intent and returns the error, meaning no
          failover reconciler acts on a split that was already undone. Only a **crash** before the clear leaves
          the intent pending (which is what B2-3 reconciles). A happy split now bumps **two** versions (begin
          plus advance). Tested (`:multinode`): the happy split raises the topology to **v2** with the new vnode
          and `pending == nil`; a logical failure (the new vnode on an unreachable node) returns `{:error, _}`,
          raises to **v2** with `pending == nil` and leaves the ring **unchanged** (source only). Suite at 812
          tests, 0 failures (+1 multinode); credo, dialyzer and format clean.
        - ✅ **B2-3. The reconcile on failover (trigger 3A: `on_acquired` plus `init`).** This closes 1A. A new
          public `ReplicatedDSRSM.abort_split/3` reuses B1's `roll_back` (moving the migrated topics back and
          releasing fences) and then **deletes the new vnode's orphan cluster, but only once confirmed empty**:
          deleting a vnode that still holds topics (a move-back that failed, or was unreachable) would lose
          them, so in that case the cluster is left **intact** and it returns `{:error, :incomplete}` (the data
          is safe there). A new leader-gated `VnodeSplit.reconcile/2` reads the topology and, if there is a
          `pending`, builds the `ReplicatedDSRSM` of the *old* ring (a pending split did not advance the ring),
          aborts, and **only on `:ok`** (a complete rollback) publishes the `clear_pending`; on an
          `:incomplete` it **keeps the intent pending** for a future retry (never abandoning stranded data).
          `SplitCoordinator` gains `reconcile/1` (a cast, which **serializes** with splits in the same process)
          fired at **two** points: on `LeaseHolder`'s `on_acquired` (failover between nodes) **and** in its own
          `init` (a GenServer restart under the same leader); both are guarded by `leader?`. It is the
          NorthGuard analogue (transcript 500-508: the coordinator is the raft group's leader, *"responsible for
          carrying it out"*; recovery is native to raft, since the new leader re-reads the log, with no periodic
          poll): the **trigger axis** (3A) is faithful, while on the **strategy axis** NorthGuard completes
          forward (1B) and we abort (1A) for simplicity. Wired into `application.ex` (`on_acquired` →
          `SplitCoordinator.reconcile`). Tested: reconcile aborts a split whose coordinator "died" midway
          (migration done, intent `pending`), so the topic returns to its source with offsets, un-fenced and
          writable, the orphan cluster is deleted, and the abort is published (v2, `pending` cleared, ring
          intact); with the new vnode **unreachable** the intent stays pending (v1, not cleared) for a retry,
          without loss; a no-op when there is no `pending`; a refusal when not the leader; and the coordinator
          survives the reconcile in `init` plus the cast. Suite at 815 tests, 0 failures (+5, of which +2 are
          multinode); credo, dialyzer and format clean. **B2 complete: the split is resilient to a coordinator
          crash (abort plus retry). Optional future promotion: 1B (complete-forward, the literal "carrying it
          out to the end") and/or 3C (a periodic sweep).**
      - ✅ **Hardening 1B: reconcile by *complete-forward* (NorthGuard's literal "carrying it out")**
        (policy **1B-fwd** chosen). It promotes B2-3's reconcile from *aborting* to *completing forward*:
        instead of reverting an interrupted split and having the operator re-issue it, the new coordinator
        **resumes the migration where it stopped and finishes it** (publishing the new ring), which is exactly
        what NorthGuard does (the raft group's new leader re-reads the log and carries the operation to the
        end). Discovered while investigating: the migration **is already idempotent by construction**
        (`migrate_from` computes the displaced set from the source's *current* topics, so anything already
        migrated is skipped; `begin_migration` is idempotent when present; `extract` of an absent topic is a
        no-op; `insert` overwrites through `Map.merge` plus `MapSet` indexes), so resuming "only finishes what
        is left". The only friction: (1) `MetadataServer.start` errors if the new vnode's cluster already
        exists; (2) `split_vnode` reverts on a failure (B1), whereas on a resume we want to **keep it pending
        and retry**, not revert. Policy 1B-fwd: always complete forward; if it cannot right now (unreachable),
        keep the intent pending for a retry; `abort_split` (B2-3) becomes the operator's **manual abort API**.
        Sub-sliced:
        - ✅ **1B-1. The idempotent foundation.** A new `MetadataServer.ensure_started/2` brings the vnode's
          cluster up the way `start/2` does, but **idempotently**. If it is already running (reachable through
          `:ra.members`) it returns the `server_id` without restarting; otherwise it starts it. That closes
          friction (1): a resume does not fail against an already-formed cluster, and it errors only when the
          cluster neither runs nor starts (an unreachable placement, so the caller retries). `insert_topic`'s
          idempotence was confirmed and locked down by a test (re-inserting the same export is a no-op:
          `Map.merge` overwrites and the `MapSet.put` indexes de-duplicate). That is the invariant that makes
          the resume safe. Tested: `ensure_started` forms a fresh cluster and then **reuses** the running one
          without restarting (state preserved); `insert_topic` is idempotent (pure). Suite at 817 tests, 0
          failures (+2); credo, dialyzer and format clean.
        - ✅ **1B-2: `ReplicatedDSRSM.complete_split` (idempotent resume, no rollback).** Refactored: the
          migration loop became `do_migrate/4` (walking the sources in order, without rollback), shared by
          `migrate_displaced` (a fresh split being `do_migrate` **plus rollback** on failure, B1) and by the new
          `complete_split/4`. `complete_split` mirrors `split_vnode` but (1) uses `ensure_started/2` (reusing
          the new vnode's cluster if it is already up) and (2) **does not revert** on a failure: it returns the
          error and **leaves the partial state in place** for the next resume to finish (keep-trying, so a
          transient outage does not undo progress). Idempotent by construction (`migrate_from` skips what has
          already migrated; `insert` and `begin_migration` are no-ops). Tested (`:multinode`): `complete_split`
          from scratch migrates the displaced topic (the same result as `split_vnode`, with offsets, writable);
          and **resuming** an already-migrated split (previously driven by `split_vnode`, with the destination
          cluster up) finishes idempotently: nothing disappears, no duplicate on the source, and a third drive
          returns exactly the same state (structural equality). Suite at 817 tests, 0 failures (+2 multinode);
          credo, dialyzer and format clean.
        - ✅ **1B-3: `VnodeSplit.reconcile` now completes forward.** `abort_split` was swapped for
          `complete_split` in the reconcile: on finding a `pending` intent (now reading the `token` as well) it
          resumes the split, and on `{:ok, grown}` publishes the **complete** topology (`advance`, which moves
          the ring forward and clears the intent), just as a normal split would; on a failure (an unreachable
          vnode, where `complete_split` cannot even bring the cluster up) it **keeps it pending** for a retry,
          without undoing progress. This is NorthGuard's *"carrying it out to the end"*: the coordinator that
          takes over **carries the split to completion** rather than unwinding it. `abort_split` (B2-3) remains
          as the operator's **manual escape hatch** (the inverse of the reconcile), now with a direct test.
          Tests updated: reconciling an interrupted split now **completes** it (the topic stays on the new
          vnode, the topology advances to v2 with the new vnode, offsets intact, writable) instead of
          reverting; the unreachable-new-vnode case still keeps the intent pending (now because it cannot
          complete); and a new test covers `abort_split` directly (reverting the migrated topic to its source
          and deleting the orphan). Suite at 817 tests, 0 failures (+1 multinode); credo, dialyzer and format
          clean. **1B complete: reconciling an interrupted split now completes forward (NorthGuard's faithful
          model), with abort left as a manual tool. Split hardening (A/B/C/1A/1B) is closed; only the optional
          3C remains (a periodic sweep, more k8s operability than NorthGuard).**
- ✅ **`Malachi.Cluster.Placement`**: the **pure** placement and segment-replica self-healing policy
  (the data plane's *decision* layer; `Metadata` already holds the segments' *state*). It uses
  **rendezvous (HRW) hashing**: `place/3` picks the replica set (deterministic, so it is safe for Raft,
  with minimal reshuffle), `under_replicated/3` detects segments with a dead replica (the target clamped
  to the number of live brokers), and `heal/3` emits `:set_segment_replicas` commands that restore
  replication. It covers **sealed** segments too (durability). `available_brokers` is an abstract
  parameter. It pins down the contract the future membership will serve. Property tests: the replica
  set's size and distinctness, determinism independent of ordering, **retention under broker removal
  (min-reshuffle)**, and **`heal` reaching a fixpoint in one pass**.
- ✅ **Segment lifecycle in `Broker`**: the data plane now *creates* segments. For each active range the
  `Broker` keeps one segment open (a logical span of offsets over the range's single `Log`, A1): on the
  first produce it registers the segment, choosing the `replica_set` through `Placement.place` over
  `:brokers`/`:replication_factor` (defaulting to `[node()]`/`1`); it accounts for the appended bytes
  (`Malachi.Log.Record.encoded_size/1`, matched byte for byte against `encode/1`) and **seals and rolls**
  on crossing `:segment_max_bytes` (a *soft* threshold, checked at the batch boundary). `split` and
  `merge` seal the affected range's active segment. `segment_id = {range_id, seq}` (globally unique; the
  per-range seq persists across seals, so an id is never reused). The `produce`/`read` API is unchanged
  (segments are additive bookkeeping). Tested: registration on the first produce, the replica set through
  HRW, byte-driven rollover (one record per segment, and soft overshoot), sealing on split and merge, and
  policy validation.
  - ✅ **`heal` (re-replication) wired in.** `Malachi.Cluster.SelfHealing.heal_sealed/4` (backfilling
    under-replicated sealed segments through `Catchup`) plus a reactive trigger on the write path for the
    active segment (`reactive_healing_test`), both applied by the `HealCoordinator` (a level-triggered
    reconcile against the SWIM membership's live set), wired into `application.ex` (a node-wide
    `healer_child` plus a per-vnode `heal_vnode_child`). *Minor: segment rollover by time or count in
    addition to bytes (a refinement).*
- ✅ **`Malachi.Cluster.ReplicaTracker`**: the **pure** quorum-commit core for replicating **one
  segment** (the mechanism's deterministic logic, with no processes or network). An ordered `replica_set`
  (the first being the primary); `ack/3` records each replica's durable offset (monotonic, ignoring
  regression); `commit_offset/1` is the highest offset present on a **majority** (⌊N/2⌋+1) of the
  replicas, or `:none`; plus `committed?/2`. It tolerates ⌊(N-1)/2⌋ failures without waiting for the
  slowest replica. The counterpart (for segment *data*) of the deterministic `Metadata` (for
  *metadata*). Property tests: the commit is the highest offset held by a quorum, **commit
  monotonicity**, and `committed?` iff a quorum has it. Scope: a fixed replica set (primary failover and
  set changes come later).
- ✅ **`Malachi.Cluster.ReplicationServer`**. Replication's **transport**: one `GenServer` per broker
  that ships the active segment's stream from the **primary** to the **followers** and acks once a
  **quorum** has stored it durably. A broker is a process reference (a local name in the tests;
  `{name, node}` across nodes, since `GenServer.call` accepts both, so the same path runs in-process and
  multi-node). On the primary, `replicate/4` appends and fsyncs locally, fans out concurrently to the
  followers, feeds the `ReplicaTracker` and returns `{:ok, last}` once the quorum has the batch
  (tolerating ⌊(N-1)/2⌋ slow or downed followers), or `{:error, :no_quorum}`. Primary and followers both
  `fsync` before counting toward the quorum, so "committed" means durable on a majority. Each segment
  opens its `Log` at `base_offset = start_offset` (passed in `replicate/5`), so the offsets of a range's
  segments are **contiguous** (they do not restart at 0 per segment). Tested in-process: replicating to
  all plus the commit, tolerating 1 downed follower, `:no_quorum` without a majority, single-replica,
  contiguous offsets across batches, a **non-zero base** (`:out_of_range` below the base),
  `:not_primary`, an empty batch, an empty replica set (no crash) and a duplicated set (no deadlock).
  Scope: the active segment's happy path.
- ✅ **`ReplicationServer` wired into `Broker`/`BrokerServer` (A2+A3)**: `Broker` became a **pure
  router** (control plane), losing `logs` and `directory` and keeping only `Metadata` plus the segment
  lifecycle plus a per-range offset counter. Storage is 100% `ReplicationServer`'s. The connection uses
  **injected effect functions**: `produce` takes a `replicate_fun`, and `read`/`stream_history` take a
  `read_fun`, so the whole orchestration (routing, offset→segment, commit, pagination and the
  cross-epoch filter) lives in one module, tested with **in-memory fakes**
  (`Malachi.Test.FakeSegmentStore`). `BrokerServer` injects `&ReplicationServer.replicate/5` and
  `&ReplicationServer.read/4`, starts a local `ReplicationServer` (its ref being the pid) and opens the
  `Broker` with `brokers: [that_ref]`. Since a write is fsync-by-quorum by the time it returns, the
  **10ms time-based flush is gone** and `sync/1` became a no-op. `produce` returns
  `{broker, {:ok, placements} | {:error, reason}}` (a per-group commit; an immutable value makes the
  transaction free). No duplicated storage. Full suite green (831 tests).
- ✅ **`Malachi.Cluster.Catchup` (the catch-up/backfill primitive)** copies a segment's records from a
  **source** replica to a **target** one over the `[from, to)` interval. It serves both cases: a lagging
  follower closing the active segment's gap, and a new replica (from `Placement.heal`) **backfilling** an
  entire sealed segment. It runs by **external orchestration** (in the caller's process, through
  `ReplicationServer.read/4` on the source plus `follow/4` on the target), so nothing is nested inside a
  `handle_call` that is being awaited, which is what avoids the primary↔follower deadlock. Exposed on
  `ReplicationServer`: `follow/4` (a replica append) and `end_offset/2` (how far the target has got, or
  `:empty`). If the source is itself behind, it stops at what it has and returns the offset reached.
  Tested in-process: backfilling a fresh replica, catching up only the gap, preserving a non-zero base,
  an incomplete source, a no-op when already caught up, and `:empty`. Scope: the primitive itself (driven
  by the caller).
- ✅ **`Malachi.Cluster.SelfHealing` (heal → backfilling sealed segments)** closes the self-healing loop
  for **sealed** segments: `Placement` decides, `Catchup` executes. `heal_sealed/4` (metadata plus live
  brokers plus rf) finds the under-replicated sealed segments, picks the healed replica set through
  `Placement.place`, **backfills** each new replica from a live one (`Catchup.run`), and returns the
  `:set_segment_replicas` commands (for those whose backfill succeeded) for the control plane to apply,
  along with the segments it could not heal (for example `:no_live_source`). Sealed only (a fixed
  `[start, start+length)` range makes the backfill well defined); the active segment recovers through the
  write-path trigger (slice (a), deferred). Live brokers come in as a parameter (membership comes later).
  Tested in-process: backfilling the new replica plus the command plus the loop closing (a re-heal is
  empty), all replicas dead yielding `:no_live_source`, nothing to do when healthy, and the active
  segment ignored.
- ✅ **Automatic catch-up on the write path (active segment)** closes the missing half of recovery (the
  sealed half being `SelfHealing`). When the primary's fan-out reaches a follower whose
  `next_offset < expected_first` (it fell behind), `ReplicationServer` **spawns a monitored background
  process** that runs `Catchup` pulling from the primary (`from = its own end`, `to = the primary's
  end`), answers `:out_of_sync` (the write commits through the up-to-date replicas) and the follower
  **rejoins the quorum on a later batch**. A `catching_up` set de-duplicates catch-ups per segment; the
  monitor's `:DOWN` clears the flag on success or failure; and `follow`'s offset check keeps it safe
  under a race (a concurrent append makes the catch-up abort and the next gap re-triggers it, so it
  converges without duplicating or corrupting). The `follow` message carries the primary's ref as the
  source; `Catchup`'s own `follow` calls pass `nil` (so they do not re-trigger). Tested in-process: a
  follower misses a batch, is chased back to the full log, and rejoins the quorum.
- ✅ **Backfilling a new replica on an active segment (missed-start)**, a minimal extension of the
  trigger: the `follow` message now carries the segment's `base`, and a fresh log opens **at the `base`**
  rather than at the batch's offset. So a **newly added** replica on an active segment sees the gap from
  the start (`next = base < expected`), the existing trigger pulls `[base, head)`, and it **converges on
  the moving head** through the missed-middle re-triggers, entering the write path once it catches up
  (ISR style: it does not count toward the quorum until it is in sync). Zero new trigger code. A known
  limitation: under sustained very fast writes it may never catch up (no throttling, a future
  refinement). Tested in-process: a new replica on an active segment backfills from the start and then
  follows live.
  - ✅ **Primary failover wired in.** `Malachi.Cluster.Failover.plan/2` (promoting a live replica to the
    head of the `replica_set` when an **active** segment's primary dies: the data is already there, so no
    bytes move), applied by the `HealCoordinator` alongside the heal, against the SWIM membership's
    multi-node live set. Tested (`failover_test`, `replicated_dsrsm_ha_test`). *(The pure layer already
    noted "failover later" in `ReplicaTracker`/`ReplicationServer`; done.)*
- ✅ **`Malachi.Cluster.Membership` (the pure SWIM state machine)**: the deterministic membership view,
  `alive`/`suspect`/`dead` per member plus an **incarnation**, with no processes, timers or network. The
  single rule is a *join* on the lexicographic order `{incarnation, rank}` (`alive < suspect < dead`),
  which gives **SWIM precedence** (a higher incarnation wins; on a tie, suspect beats alive and dead
  beats both; equal is idempotent) and guarantees the gossip merge **converges** in any order
  (CRDT-like). The exception: an update about **self** as suspect or dead is **refuted** by raising our
  own incarnation and re-announcing alive (a `{:refute, …}` effect to disseminate). `apply_update/2`,
  `merge/2`, `suspect/2`, `confirm/2`, `alive_members/1`. Property tests: **order-independent
  convergence** and **incarnation monotonicity**, plus unit tests for precedence and self-refute.
- ✅ **`Malachi.Cluster.MembershipServer` (the SWIM server: detector plus gossip)**, one `GenServer` per
  broker running `Membership` live. Each *protocol period* it **pings** a random live peer; no **ack**
  within `ack_timeout` means `suspect`; no refute within `suspicion_timeout` means `dead`. Every
  ping and ack **piggybacks** the view (a list of updates), giving anti-entropy, so the views
  **converge**; a falsely suspected node finds out through the ack to its own ping and refutes (raising
  its incarnation). Refs are location-transparent (testable in-process, multi-node without changes);
  sends are fire-and-forget (a dead peer means no ack, which is the detection). Seed peers come from
  `:peers`; the timers are configurable. Tested in-process: **gossip spreads** partial knowledge until
  everyone knows everyone, a **stopped node is detected and becomes `dead`** across the cluster, and a
  false suspicion about self is **refuted**.
- ✅ **Indirect ping**: when the direct ack does not arrive, the node asks `indirect_fanout` peers
  (default 3) to **ping the target on its behalf** (`ping_req` → the relay probes the target → it relays
  the ack back to the requester); it only suspects when neither the direct nor the indirect path answers
  within `indirect_timeout`. This cuts false positives under transient loss of the direct route. Tested:
  the **relay chain** delivers the target's ack to the requester (deterministically), and the dead-node
  test now exercises the full path (direct→indirect→suspect→dead).
- ✅ **A formal join**. On start, a node sends `{:join}` to each seed (`:peers`); the seed **registers
  the joiner as alive** and replies with its **complete view**, so the joiner learns the cluster at once
  rather than only converging through gossip. Best-effort (gossip is the safety net). Tested: the
  handshake (the seed replies `join_ok` with its view and comes to know the joiner) and a **new node
  learning the whole cluster through a single seed**. A caveat: rejoin-after-death (restarting with
  incarnation 0 < `dead@n`) is left for later. **SWIM is closed:** direct and indirect ping, suspicion,
  gossip and join.
- ✅ **`Malachi.Cluster.HealCoordinator` (reactive self-healing)**, a periodic `GenServer` that closes
  the loop **broker dies → detected → healed**: each interval it runs `SelfHealing.heal_sealed` against
  the **live** set and applies the commands. Decoupled through **injected seams** (`live_brokers`,
  `metadata_source`, `apply_command`, `rf`, `interval`), so it is testable in-process and can later be
  wired to `MembershipServer` (live) and to the control plane (apply) without changes. `heal_now/1` runs
  one synchronous pass (for tests or a manual trigger). Tested in-process: healing after a lost replica
  (backfill plus the command applied plus the loop closing), `:no_live_source`, and **automatic healing
  on the timer**.
- ✅ **Fine wiring 1a: membership → reactive self-healing (a single control node plus N data brokers).**
  The reactive loop now runs end to end on the **1 control node + N data brokers** topology:
  `BrokerServer` accepts external `:brokers` (refs to `ReplicationServer`) and is the metadata
  authority; it gained `metadata/1` and `apply_heal/2` (which applies `set_segment_replicas` through a
  new `Broker.apply_heal/2`). `HealCoordinator` is wired with `metadata_source`/`apply` ←
  `BrokerServer` and `live_brokers` ← a **bridge**,
  `MembershipServer.alive_members |> map(member-id → broker-ref)`. Tested: **a data broker dies →
  membership detects it → sealed segments are re-replicated** onto the live set (no under-replication
  left, and the data verified on the new replicas), and the bridge drops the dead node's broker ref.
- ✅ **Dynamic placement**: `BrokerServer` now periodically **refreshes** `Broker`'s placement brokers
  from `:live_brokers` (`Broker.set_brokers/2`), so a **new segment is born on the live set** rather
  than on the configured one (with a fallback that keeps the last non-empty set, and no cost on the hot
  path). It completes the reactive symmetry (both healing **and** creation react to what is live).
  Robustness alongside it: `ReplicationServer.replicate/5` no longer **takes down** the caller when the
  primary is dead (it returns `{:error, :unreachable}` and `produce` propagates the error), and a failed
  `produce` **discards** the newly opened segment (a free rollback through the immutable value, so there
  is no phantom segment; the retry re-places onto live brokers). Tested: after a data broker dies and
  leaves the live set, a freshly produced segment **excludes the dead one** from the `replica_set`.
- ✅ **Primary failover**, `Malachi.Cluster.Failover.plan/2` (pure): for every **active** segment whose
  primary (`hd(replica_set)`) is dead, it emits a **reordered** `set_segment_replicas` with a live
  replica at the head (the dead one stays as a follower that never acks; `heal` swaps it out after the
  seal, so no data moves). `Broker.apply_heal` was generalized to update the **active segment's cache**
  as well (one apply path serves both heal and failover), and `HealCoordinator` became a
  **reconciliation** loop (healing sealed segments plus failing over active ones on each tick). Tested:
  a pure `plan` (promoting a live replica, ignoring sealed segments, skipping an all-dead set),
  `apply_heal` updating both cache and metadata, and **integration**: an active segment's primary dies →
  a replica is promoted → writes continue on the new primary (both records read back).
- ✅ **1b: metadata authority through `ra` (in slices).**
  - ✅ **`Malachi.Cluster.ReplicatedMetadata`** (the component) pairs a `MetadataServer` (an `ra` cluster
    running `Metadata.apply`) with a **local `Metadata` cache**: `command/2` goes to the Raft log and, on
    commit, applies the **same** command to the cache (determinism means the cache equals the replicated
    state), so reads are local (no Raft round trip on the hot path) with **read-your-writes**.
    `metadata/1` (a read), `refresh/1` (re-reading the replicated state, for multi-writer or recovery),
    `start/1`, `delete/1`. Tested: a committed command updates the cache, a rejected command leaves the
    cache intact and returns the machine's error, and the cache equals the replicated state (refresh is a
    no-op for a single writer). A single-node `ra` cluster (durability; multi-node HA comes later).
  - ✅ **`BrokerServer` wired to metadata through `ra`**: `Broker` gained an **injected `command_fun`**
    `(metadata, command) -> {metadata, reply}` (defaulting to `&Metadata.apply/2`, so in-memory behaviour
    is untouched); **every** mutation (`create_topic`/`split`/`merge`/`register`/`seal`/
    `set_segment_replicas`) goes through it, while reads stay on `broker.metadata` (the cache). With the
    `:metadata_cluster` option, `BrokerServer` starts a `MetadataServer`, **seeds** the cache from the
    replicated state, and injects `command_fun = ReplicatedMetadata.apply_command(server_id, …)`: a Raft
    command plus a cache apply **threaded through `broker.metadata`** (so one produce can open and seal a
    segment with read-your-writes within the operation). Tested (over ra): mutations (topic plus segment)
    land in the **replicated state** (queried directly on `ra`) and the cache equals it; a rejected
    command propagates the machine's error. The default (without `:metadata_cluster`) stays in-memory.
  - ✅ **Hardening `register`'s crash path**: `open_segment` now returns `{:ok, broker}` or
    `{:error, reason}`; a `register_segment` failure (an `ra` timeout, say) is propagated through
    `ensure_segment` to `produce` as `{:error, reason}` (rolling back the open, so there is no phantom
    segment) instead of taking `BrokerServer` down. Tested: a `command_fun` that fails the register makes
    produce return `{:error, :ra_down}`, with no segment registered and the offset not advanced.
  - ✅ **A multi-node `ra` cluster (control-plane HA, the last SPOF eliminated).**
    `MetadataServer.start/2` forms the `ra` cluster across **several nodes** (`server_ids` spanning
    nodes); `BrokerServer` accepts `:metadata_nodes`. With 3 or more members the metadata is replicated
    and **survives losing a control node** (a follower is elected leader). Genuinely tested
    (`test/.../metadata_ha_test.exs`, tagged `:multinode`, opt-in through
    `mix test --include multinode`): 3 real BEAM nodes through `:peer`, a command replicated to all, an
    **abrupt kill of the leader node**, and a following command still committing (under a new leader)
    with the previous metadata intact; flake-checked 5x. (Excluded from the default `mix test` because it
    needs epmd and distribution.) **1b complete.**

### Phase 3. Product: connecting the two worlds plus scale (the new direction)
Make the NorthGuard stack the **live**, scalable broker, better than OSS Kafka.

- ✅ **B: connect the TCP client to the NorthGuard stack (a log API with an opaque cursor).** At the time
  `tcp_protocol` spoke queues (`publish`/`subscribe`/`ack`/`channel_*`) over `Queue`/`Channel`, while the
  NorthGuard stack spoke log over `BrokerServer`. **The client contract is the NorthGuard way, NOT
  Kafka's:** the client uses `topic` plus a **key** (produce) plus an **opaque cursor** (consume), and
  **never** sees a partition or offset (hidden on purpose, so the system can split, merge and restripe
  underneath without breaking the client; it is the differentiator against Kafka). The cursor is a token
  that today encodes `%{range_id => offset}` internally, but the client treats it as opaque.
  **Priority #1.** Slices:
  - ✅ **First slice, the `Malachi.LogApi` core:** `create_topic`/`produce` (by key)/`fetch` (opaque
    cursor) over `BrokerServer`; a **safe** cursor decode (`binary_to_term [:safe]` plus shape
    validation). The client never sees a partition or offset. Tested in isolation.
  - ✅ **First slice, the wiring:** `BrokerServer` (`Malachi.LogBroker`) started in the supervision tree
    (`application.ex`, single-node and in-memory by default; the data dir through `:log_data_dir`); the
    `create_topic`/`produce`/`fetch` actions added to `tcp_protocol` (additively, so queues and channels
    kept working), with auth (`:produce`/`:consume`), a bounded `max`, and records rendered to JSON
    **without an offset**. **The client reaches the NorthGuard stack end to end**: tested e2e over real
    TCP (produce by key → fetch by opaque cursor; permissions; an empty re-fetch).
  - ✅ **Consumer groups plus a server-side committed position (option A, offsets in the Metadata RSM).**
    A group consumes with `fetch_group(topic, group)` (resuming from the group's **durably** committed
    position) and advances it with `commit(topic, group, cursor)` (Kafka's at-least-once, manual-commit
    style). The cursor stays **opaque** (the client never sees an offset). The offsets live in the
    deterministic `Metadata` (`{:commit_offset, group, topic, offsets}` plus a `committed_offsets/3`
    query), so they get durability and HA **for free** through the existing `command_fun`/`ra` path.
    Exposed on `BrokerServer`/`LogApi` and in `tcp_protocol` (`fetch` accepts a `"group"`; a new `commit`
    action). Tested: a Metadata unit test (last-commit-wins,
    independent groups and topics), a `LogApi` round trip (resuming after a commit; re-reading without a
    commit is at-least-once), and e2e over TCP (produce → fetch by group → commit → the fetch resumes
    empty). Committed offsets grow the Metadata state (at scale, a future log-based store in the
    `__consumer_offsets` style, aligned with roadmap D).
  - ✅ **Split-aware consumption (a cursor spanning ranges that split).** Before, consumption read only
    each **active** range's linear offset, so records written **before** a split (which live in the parent
    range's segments, now sealed and out of the active set) were **lost**. Now `read_consume/5` in
    `Broker` does a **live cross-epoch** read: it drains the sealed ancestors (filtered to the range's
    keyspace slice, through `parents` plus `Keyspace`) and then **tails** the active range, without
    marking `:done` on self (unlike `stream_history/5`, which is for *bounded* history), reusing
    `history_sources`/`filter_records` (DRY). The per-range position becomes a `consume_cursor`
    (`:start | {source_index, source_offset}`), carried opaquely in the client's cursor and in the
    committed offsets (the `Metadata.position` type). `fetch`/`fetch_group` inherit the fix. **A split
    during consumption (decision A):** the children do not inherit the parent's position, so they restart
    from the beginning and **reprocess** their slice (at-least-once, zero loss; splits are rare and
    administrative). Tested: `Broker` (exact cross-epoch delivery after a split, tailing new records, a
    nonexistent range) plus `LogApi`/`BrokerServer` integration (a fresh consumer sees everything from
    before and after the split; a committed group reprocesses after a split).
  - ✅ **Binary payloads (base64 over the JSON protocol).** Storage and `LogApi` always accepted
    arbitrary bytes (`build_record` guards on `is_binary(value)`); the bottleneck was only the JSON edge:
    `value` came from `Jason.decode` (always UTF-8) and, on fetch, a non-UTF-8 `value` **crashed
    `Jason.encode!`**. Now **every `value` on the wire is base64** (a uniform scheme, by choice):
    `produce` decodes each `value` to bytes (`Base.decode64`) before calling `LogApi`, and
    `record_to_json` encodes it (`Base.encode64`) on fetch. base64 lives **only in `tcp_protocol`**
    (`LogApi` stays binary-native); `key` and `headers` remain UTF-8. Invalid base64 yields
    `:invalid_base64`. **Breaking** (JSON clients now send and receive `value` as base64). Tested e2e: a
    round trip of non-UTF-8 bytes, invalid base64 rejected, and the existing tests migrated to base64.
  - ✅ **Long polling on `fetch`/`fetch_group` (event-driven, with *waiters* in `BrokerServer`).**
    Before, a *caught-up* consumer got `[]` immediately and had to re-poll (busy polling). Now an optional
    `wait_ms` (clamped to 30s in `tcp_protocol`) makes the call **block** until a produce to the topic
    delivers data or the timeout expires (`[]`). The mechanism was chosen after a **benchmark**
    (`bench/`): A, *waiters* inside `BrokerServer` (the GenServer that already serializes produce and
    fetch) hold the pending empty fetches (`{from, topic, positions, max}` plus a timer); a produce to the
    topic re-consumes each waiter and replies (`GenServer.reply`) to those that now have data, while the
    timeout replies `[]`. Measurements: A delivers ~35-47% faster than Registry-based pub/sub (B) at every
    scale and is **self-contained** (no global state); A's downside (the producer blocking on the fan-out)
    only matters with thousands of consumers on the same topic, which is beyond today's single node, and
    going multi-node moves the fan-out to a distributed mechanism anyway. The multi-range read
    orchestration **moved** from `LogApi` into `BrokerServer` (`consume_ranges`): one cohesive call
    instead of N+1, and it is what produce re-runs to wake waiters. Tested: `BrokerServer` (waking on
    produce, an empty timeout, waking only the topic produced to), `LogApi` (blocking until a produce; the
    timeout), and e2e over TCP (woken by a concurrent producer; the timeout).
  - ✅ **Re-architecting the client layer (binary protocol plus streaming), driven by benchmark.** The log
    functionality over TCP was complete, but the **style** was JSON+base64 request/response, not
    NorthGuard's "sessionized streaming with windowing". Decided **empirically** (`bench/`, 1M messages):
    (a) a **binary protocol** against JSON+base64: **29% fewer bytes on the wire, 8.9x less CPU to
    encode, 17.3x less to decode** (`protocol_bench.exs`); (b) **push delivery** (subscribers in the
    broker) 35-44% faster than Registry pub/sub, **but without windowing the mailbox explodes** (a slow
    consumer peaked at 190,276) and hits **OOM**, while with a window it stays at 999
    (`streaming_bench.exs`); (c) a system baseline of 657k produce and 1.2M consume records/s, with flat
    memory (`throughput_1m.exs`). The NorthGuard-faithful target is **push plus windowing** over a
    **binary protocol**. Authorized to **remove the legacy queue model** (queues and channels) and
    **replace** JSON with the binary one. Fronts: **B1** the binary protocol → **B2** push streaming with
    windowing → **B3** removing the legacy queues. Each sliced up (a testable core plus wiring).
    - ✅ **B1a: the binary codec (`Malachi.Wire`, a pure core).** Length-prefixed framing
      (`<<len::32, body>>`), a request envelope `<<api_key::16, correlation_id::32, payload>>` and a
      response one `<<correlation_id::32, error_code::16, payload>>` (the `correlation_id` enables
      **pipelining**), plus codecs for the 4 log operations (create_topic/produce/fetch/commit). Records
      carry **no offset** on the wire (the client never sees one; the opaque cursor holds the position),
      with its own encoding, distinct from the on-disk `Malachi.Log.Record.encode/1`. Cursor and key are
      byte strings with a presence flag (`nil` is not the same as empty). Pure; the socket wiring is B1b.
      Tested: a frame round trip (plus `:incomplete` on a partial prefix, and two frames in one buffer),
      the envelope, the wire record (a nil key versus an empty one, non-UTF-8 bytes, a round-trip
      property), and the 4 operations.
    - ✅ **B3a. Removing the queue *protocol* (before B1b).** The order was inverted: switching the
      connection loop to binary frames makes the 14 JSON queue and channel actions unreachable and would
      break their tests, so the legacy queue *protocol* goes first, leaving `tcp_protocol` with log only,
      and then B1b converts log from JSON to binary cleanly. Removed the 14 queue and channel
      `handle_action` clauses and their helpers
      (`publish`/`enqueue`/`build_queue_options`/backpressure/…) from `tcp_protocol` (1130 lines down to
      ~200, just create_topic/produce/fetch/commit plus the shared auth and permission code); removed the
      `subscribed`/`receive_active_loop` mode from `tcp_acceptor` (it was queue push: B2 reintroduces its
      own *log streaming* loop). The `Queue`/`Channel` **modules** stay (B3b deletes them and adjusts
      metrics and the dashboard). Tests: `tcp_queue_management` (100% queue) deleted; the 7
      protocol/security tests that go over the socket (`tcp_protocol`, `channel_integration`,
      `comprehensive_security`, `protocol_fuzzing`, `rate_limiting`, `validation`, `penetration`) were
      **skipped** (`@moduletag :skip`) to be rewritten against the binary protocol in B1b (avoiding
      migrating them onto a log-JSON that is about to disappear): the infrastructure
      (`Auth`/`RateLimiter`/`Validator`) stays covered by its own unit tests, and the **log e2e**
      (`log_protocol_test`) stays green. Suite: 1087 tests, 0 failures, 94 skipped; credo and dialyzer
      clean.
    - ✅ **B1b-i: the binary wiring (e2e).** `tcp_acceptor` reads length-prefixed `Wire` frames (the
      listen socket moved from `packet: :line` to `packet: 0`, raw, since `Wire` does the framing) into a
      buffer that tolerates frames split across several `recv` calls (`decode_frame` → `:incomplete`);
      **auth** went binary too (a new `api_key` 0, a username/password request and a token response).
      `tcp_protocol.process_frame/4` decodes the request **at the edge inside a `try`** (a malformed frame
      yields an error frame rather than a crash), routes by `api_key` and replies in binary through
      `Wire.encode_ok`/`encode_error` (error_code 0 for ok, 1 for an error with the reason as a string). A
      new `LogApi.produce_records/3` accepts a `%Record{}` straight off the wire, skipping the map→record
      step of the JSON `produce/3`; `fetch_req` gained a `group` (with the cursor taking precedence).
      `TCPHelper` gained the binary helpers (`request`/`recv_frame`/`authenticate_wire`) and
      `log_protocol_test` was **rewritten** for the binary protocol (the base64 case went away, since the
      value is native bytes, and the malformed-cursor one became invalid bytes). Tested: binary log e2e
      (create/produce/fetch/groups/commit/binary/long-poll/permissions) plus `Wire` (auth/ok/error round
      trips). Suite: 1055 tests, 0 failures, 94 skipped; credo and dialyzer clean.
    - ✅ **B1b-ii: security coverage for the binary protocol.** The 6 protocol tests skipped in B3a were
      ~100% queue and JSON (queue/channel/publish/subscribe as the vehicle, **JSON** fuzzing,
      `queue_name` validation: all obsolete on a binary protocol with no queues), so rather than adapting
      them one to one they were **deleted** (along with `channel_integration`) and replaced by a cohesive
      `binary_protocol_security_test` over the protocol that **actually** runs: auth (invalid credentials
      yield an error frame; a first frame that is not auth yields `auth_required`; a malformed first frame
      does not crash), permissions (produce needs `:produce`, fetch needs `:consume`), malformed frames
      once authenticated (an unknown `api_key` yields an error and the connection keeps serving; a
      truncated payload yields `malformed_request` with the correlation id preserved) and fuzzing (random
      bytes plus a lying length prefix: the server survives, proven by a fresh connection). One acceptor
      adjustment: invalid credentials now answer with an error frame (the old JSON auth just closed).
      `TCPHelper`'s JSON helpers (`send_line`/`recv_line`/`authenticate`) were removed (only binary
      remains); the infrastructure (`Auth`/`RateLimiter`/`Validator`/`LockoutManager`) stays covered by
      its own unit tests (`input_fuzzing`/`attack_simulation`/`security_performance_regression`, through
      the pure `SecurityHelper`). **Suite: 969 tests, 0 failures, 0 skipped; credo and dialyzer clean. B1
      is complete.**
    - **B2: push streaming with windowing (NorthGuard's sessionized streaming).** The gap in the table
      ("per-stream windowing"). Decisions (aligned with NorthGuard, revisited after a question from the
      user): flow control through an **explicit credit ack** (what the benchmark validated; without it,
      OOM) and position by **durable group** (NorthGuard's *consumer-group management*, not an ephemeral
      cursor, reusing the existing `commit`). The ack does **double duty: it returns window credit AND
      commits the group's position** (at-least-once).
      - ✅ **B2-a. Subscribers in `BrokerServer` (the core).** State `subscribers: %{topic => [sub]}`
        (`sub = %{pid, ref, topic, group, positions, window, in_flight, max}`). `subscribe/5` registers
        (plus a `Process.monitor`), loads the group's committed position (`committed_offsets`) and does an
        initial push; `produce` calls `wake_subscribers/3` (the sibling of `wake_waiters/3`), which pushes
        whatever **fits in the window** (`min(max, window - in_flight)` through `consume_ranges`, sending
        `{:log_records, topic, records, next_positions}`); `stream_ack/5` **commits** the position
        (`commit_offset`, durable) and **returns `count` credit**, unblocking further pushes; `:DOWN` and
        `unsubscribe/2` remove it. Free of any socket concerns: a subscriber is just a pid. Tested
        (in-process): the backlog on subscribe plus a push on produce; the window bounding in-flight until
        the ack; the ack committing durably; a new subscription for the group resuming from the committed
        position; a dead subscriber removed through `:DOWN`. Dialyzer and credo clean; broker and log e2e
        green.
      - ✅ **B2-b: the binary wiring for streaming.** `Wire` gains the `subscribe` (5) and `stream_ack`
        (6) `api_key`s plus codecs (`encode/decode_subscribe_req` for topic/group/window/max;
        `encode/decode_stream_ack_req` for topic/group/cursor/count); the **push reuses
        `encode_fetch_resp`** (records plus the opaque cursor). Decisions (natural technical defaults):
        **one stream per connection** (a `subscribe` takes over the connection), **a push is a response
        carrying the subscribe's `correlation_id`** (the client associates that corr_id with the stream,
        gRPC-streaming style), and a **fire-and-forget ack** (no response: the "result" is more pushes).
        `TCPProtocol.process_frame` returns `{:stream, corr}` on a `subscribe` (after the `:consume` gate
        plus registration through `LogApi.subscribe`); `tcp_acceptor` then switches the connection to
        **active mode** and enters the full-duplex `stream_loop`: one `receive` handles the broker's
        `{:log_records, ...}` (forwarded as push frames, with the now-public `LogApi.encode_cursor`
        turning positions into a cursor) **and** the client's ack frames (`process_stream_frame/3` →
        `LogApi.stream_ack`, decoding the cursor into positions). There is no unsubscribe frame: closing
        the connection kills the process, so the `:DOWN` in `BrokerServer` removes the subscriber (B2-a's
        cleanup). Window and batch are bounded server-side (`stream_window`/`fetch_max`).
        E2e over TCP (`log_streaming_test`): the backlog on subscribe plus a push from a produce on
        another connection; the window bounding in-flight until the ack returns credit; a `subscribe`
        without `:consume` getting an error (and not becoming a stream). Dialyzer and credo clean; 385
        tests green.
    - ✅ **B3b: deleting the legacy queue model (sub-sliced; the order forced by compilation
      dependencies: callers before callees).** The model (queues and channels) is **dead on the live
      path**: nothing outside its own modules plus the peripherals
      (metrics/dashboard/backpressure/benchmark/application) calls `Queue`/`Channel`/`Consumer` and the
      rest. The dashboard decision: **trim it to system-only** (1A) rather than remove it (preserving the
      HTTP server, auth and the system/TLS panel, all model-agnostic; a NorthGuard panel for topics and
      streams comes later).
      - ✅ **B3b-i: trimming the dashboard.** `dashboard.ex` stops rendering and fetching queues and
        channels: `serve_metrics`/`stream_metrics` emit only `%{system: get_system_metrics()}` (with no
        enrichment through `ConnectionRegistry`); removed the Queues and Channels HTML cards, the queue
        and channel JS
        (`renderQueues`/`renderChannels`/`renderConnectionList`/`changeQueuePage`/`escapeHtml`/`formatTime`),
        the dead CSS (queue-card/channel-card/pressure/utilization/connection/pagination) and the orphan
        `ConnectionRegistry` alias. The routes (`/`, `/metrics`, `/stream`, `/rate_limits`, auth) stay
        intact. This decouples the dashboard from `Metrics`'s queue getters (a prerequisite for B3b-iii).
        Dashboard tests (status and routing) green (30); credo and dialyzer clean. `security_xss_test`
        went stale (it references the removed `escapeHtml` and queue names; it still passes, but
        tautologically), cleaned up separately.
      - ✅ **B3b-ii: deleting the queue model's core.** Removed the 6 core modules
        (`queue`/`channel`/`consumer`/`ack_manager`/`partition_manager`/`queue_config`) plus `benchmark.ex`
        (superseded by `bench/*.exs`) plus `backpressure.ex`, and the 7 supervision entries in
        `application.ex`
        (`QueueRegistry`/`ChannelRegistry`/`Queue`/`ChannelSupervisor`/`PartitionManager`/`QueueConfig`/
        `AckManager`). In `metrics.ex`, the **getters** that depended on those modules went here too
        (forced by compilation:
        `get_metrics`/`get_all_metrics`/`get_channel_metrics`/`get_all_channel_metrics` plus the orphan
        privates `get_gauge`/`get_latency_stats`/`get_all_queues`/`get_all_channels`; `take_snapshot` now
        captures the system only); the pure ETS **counters** (increment_*/record_latency/reset) were left
        for B3b-iii. Tests: deleted the purely queue ones
        (queue/channel/consumer/ack_manager/partition_manager/queue_config/integration/at_most_once/
        one_to_million/atom_exhaustion/overflow_integration/backpressure plus the mass_spawn and
        test_helpers helpers); adapted the mixed ones (`application_test` and `malachimq_test` now point
        at the log stack; `attack_simulation` lost its 2 queue tests and kept the security ones;
        `atom_safety` kept only Validator and AtomMonitor; `metrics_test` shrank to system metrics plus
        history). Also removed the **stale duplicate** `test/application_test.exs` (it collided with
        `test/malachi/application_test.exs` on the same `Malachi.ApplicationTest` module: a latent bug the
        parallel compiler exposed). 783 tests, 0 failures; credo and dialyzer clean (23 files fewer).
      - ✅ **B3b-iii: removing the dead queue and channel counters from `metrics.ex`.** A pure dead-code
        deletion (the test surface was already cleaned in B3b-ii): out go
        `increment_enqueued`/`processed`/`errors`/`acked`/`nacked`/`retried`/`dead_lettered`/`rejected`/
        `dropped`, `increment_channel_*`,
        `set_blocked_producers_count`/`increment_total_producers_blocked`/`record_buffer_utilization`,
        `record_latency` and `reset_metrics`. What remains is operational and security related
        (rate limiting, connection limiting, validation, auth and lockout, audit, dashboard auth, TLS)
        plus `get_system_metrics` and `get_history`; the moduledoc was updated. 783 tests green (with 1
        **pre-existing, unrelated** flake in `tls_enforcement_test`: that file does 6
        `put_env(:enable_tls)` calls without an `on_exit` to restore, and passes in isolation; fixed in a
        separate commit); credo and dialyzer clean. **The client's layer B is complete.**
      - ✅ **A NorthGuard panel on the dashboard (with on-demand drill-down).** It gives back, in the right
        model, the visibility B3b-i's trim removed, in **two levels** so the stream stays light. **Pure**
        functions in `Metadata`: `overview/1` (a per-topic summary: state, keyspace, policy, range and
        segment counts, total bytes, consumer groups) and `topic_detail/2` (the drill-down for **one**
        topic, its ranges, each with its segments; `nil` when the topic does not exist). Both reuse
        `ranges_of_topic`/`segments_of_range` and flatten the tuple ids for JSON. `/metrics` and `/stream`
        (every 1s) send only the **summary** (through `dashboard_metrics/0`); the detail is fetched
        **on demand** per topic from a new `GET /topic?name=` endpoint (authenticated like `/metrics`;
        `serve_json/3` for DRY; the query string is now preserved through routing). On the front end,
        expanding a topic fires a single `fetch` (`loadTopicDetail`, cached in `topicDetails`, showing
        "loading…" until it arrives); the summary header stays live through the stream. Topic and group
        names are **escaped** (XSS, `escapeHtml`; the toggle is by index; the URL goes through
        `encodeURIComponent`). Tested: `overview/1` and `topic_detail/2` units (4); e2e for `/metrics`
        (the summary) and `/topic` (the detail plus a 404); a functional check of the on-demand flow in
        node (an encoded fetch, loading, the post-fetch render, escaping). The full-stream tradeoff is
        gone: zero segment traffic unless a topic is expanded. 790 tests, 0 failures; credo and dialyzer
        clean.
      - ✅ **A frame size cap on the binary protocol (a DoS fix).** Found while investigating the orphan
        `Validator`: the binary path had **no frame ceiling**. Size enforcement had gone out with the queue
        model in B3b and the binary path never replicated it. `tcp_acceptor` accumulated `buffer <> data`
        until `Wire.decode_frame` matched (`<<len::32, body::binary-size(len)>>`, with no ceiling), so a
        client could announce `len = 4 GB` and force unbounded buffering. A new `Wire.decode_frame/2`
        rejects with `{:error, :frame_too_large}` **as soon as the 4 length-prefix bytes arrive** (before
        buffering the body); the acceptor applies it at all 3 read points (auth, the request loop,
        streaming), answering an error and closing. The ceiling is configurable (`:max_frame_size`,
        defaulting to 16 MiB). Tests: a giant frame rejected (both after auth and during the handshake)
        without buffering, plus the server surviving; and the boundary (with a reduced cap: a frame at the
        ceiling processes, one byte over is rejected). 793 tests, 0 failures; credo and dialyzer clean.
      - ✅ **Removing the orphan `Malachi.Validator`.** With the DoS fix done (the one real piece of
        hardening it still performed now lives in the right place, the wire boundary), the whole
        `Validator` was dead: 7 functions with **0 live callers**, a supervised GenServer with an ETS table
        nobody used (the queue model had been its only client; the binary path validates topic names in
        `Metadata.valid_topic_name?` itself). Deleted: `validator.ex` plus its supervision entry; the 3
        validation metrics in `Metrics`
        (`increment_validation_error`/`cache_hit`/`cache_miss`) plus the `validation` section of
        `get_system_metrics`; and the dead config in runtime.exs (the Validator's name and header
        validation block, plus the resource and backpressure block and `max_dynamic_*`, leftovers of the
        queue model, all with 0 readers). Tests: deleted the pure Validator ones (`validator_test`,
        `injection_attack_test`, `input_fuzzing_test`); adapted the mixed ones (`atom_safety` kept only
        AtomMonitor; `attack_simulation` lost its Validator-based name test;
        `security_performance_regression` lost the Validator benchmarks and kept
        Auth/RateLimiter/ConnectionLimiter/lockout). Live coverage is intact: the binary path is already
        fuzzed by `binary_protocol_security_test`. 685 tests, 0 failures; credo and dialyzer clean (4
        files fewer).
      - ✅ **Observability (A: Prometheus plus health/ready · B: telemetry · C: OTel, each sliced).**
        - ✅ **O1: health and readiness.** HTTP endpoints with **no auth** on the dashboard port (probes do
          not authenticate): `GET /health` (liveness, always 200 `{"status":"ok"}`) and `GET /ready`
          (readiness: 200 `{"status":"ready"}` when `LogBroker` is alive, otherwise 503 `not_ready`, so a
          load balancer or k8s stops routing to a node that is still booting or has no broker). Added to
          `is_public_route` (the auth bypass) along with `serve_status/4` (a variable status code). Tested:
          the happy path (dashboard_test) and the **key property** (dashboard_security: 200 without a token
          even with auth enabled). The README carries a k8s probe example.
        - ✅ **O2, the Prometheus endpoint.** A **pure** `Malachi.Metrics.Prometheus` module
          (`export(system, topics) → iodata`) renders the v0.0.4 text exposition format from
          `get_system_metrics` together with `Metadata.overview`: BEAM health
          (process/memory/uptime/io/atom), security counters (rate limiting per action, failed auth,
          lockouts, dashboard auth, TLS handshakes) and per-topic gauges (ranges/segments/bytes/groups).
          The dashboard's `/metrics` became **content-negotiated**: `Accept: text/plain` or `openmetrics`
          yields the Prometheus text, anything else the usual JSON (preserving both the dashboard and the
          JSON test), under the same auth (any user; a scraper passes a token). Labels are escaped
          (defensively). Tested: a module unit test (HELP/TYPE, labels, int and float values, escaping, no
          topics) plus e2e (`Accept: text/plain` yielding an exposition with `malachi_up` and the created
          topic's gauge). 695 tests, 0 failures; credo and dialyzer clean.
        - ✅ **O3. `:telemetry` events on the hot paths.** The `:telemetry` dependency plus a
          `Malachi.Telemetry` module (a catalog plus wrappers, DRY) emitting on: **produce**
          (`LogApi.produce_records` → `%{count, bytes}`/`%{topic}`), **consume** (`LogApi.do_fetch` →
          `%{count}`/`%{topic}`), **auth** (`Auth.authenticate/3` refactored into a thin wrapper →
          `%{count:1}`/`%{result: :ok|:error}`), and **replication** (`ReplicationServer` on the quorum
          reply → `%{count}`/`%{result: :ok|:no_quorum}`). Emitting is a no-op when nothing is attached
          (safe on the hot path). Tested: an attached handler plus produce, consume and auth (plus the
          replication commit that single-node triggers). Found along the way: a **pre-existing flake** in
          `connection_limiter_test` (a **global** limit, so state is shared between tests; it passes in
          isolation and is unrelated to O3), recorded for a separate fix. 698 tests (credo and dialyzer
          clean).
        - ✅ **O4: a default telemetry handler feeding Metrics.** `Malachi.Telemetry.MetricsReporter`
          attaches (idempotently, in Metrics.init) to the 4 events and folds each into ETS counters:
          produce (`records_produced` and `bytes_produced`), consume (`records_consumed`), auth
          (`{:auth_result, :ok|:error}`) and replication (`{:replication_result, :ok|:no_quorum}`).
          `get_system_metrics` gains an `operations` section, and Prometheus emits
          `malachi_records_produced_total`/`bytes_produced_total`/`records_consumed_total`,
          `malachi_auth_attempts_total{result}` and `malachi_replication_commits_total{result}`, so O2's
          endpoint gains throughput, auth and replication figures without every operator writing a handler
          (they can attach their own alongside). Tested: the reporter e2e (an event reaching a counter
          through `get_system_metrics`) plus Prometheus carrying the section. 700 tests, 0 failures; credo
          and dialyzer clean. **Observability blocks A and B are complete.**
        - ✅ **O5: OpenTelemetry tracing (block C; C-lite then C-full).** A recorded tradeoff: OTel is
          heavy (dependencies plus it needs a collector) and O3's events already provide a baseline, so the
          decision was **C-lite first**.
          - ✅ **O5a: spans on the client operations.** The `opentelemetry_api` and `opentelemetry`
            dependencies (API 1.5 plus SDK 1.7; no exporter or grpcbox, keeping the footprint lean).
            Tracing is **off by default** (`sampler: :always_off` plus `traces_exporter: :none`), so
            `with_span` on the hot path is a **no-op**, costing nothing per operation until an operator
            opts in (an `:always_on` sampler plus an OTLP exporter). `LogApi` wraps
            `produce_records`/`do_fetch` in `Tracer.with_span` (`malachi.produce`/`malachi.consume`) with
            `malachi.topic`/`records`/`bytes` attributes. No cross-process propagation yet (one root span
            per operation). Tested: behaviour unchanged plus a **real span capture** (test.exs uses
            `sampler: :always_on` plus the `simple` processor plus `otel_exporter_pid`; it asserts the name
            and attributes through the OTel header's record plus `otel_attributes.map`). 702 tests, 0
            failures; credo and dialyzer clean.
          - ✅ **O5b: cross-process and cross-node context propagation.** A produce trace now crosses
            process boundaries: `BrokerServer.produce` captures the caller's `otel_ctx` (`LogApi`'s
            `malachi.produce` span) and passes it in the GenServer message; `handle_call` **attaches** the
            ctx and wraps the work in a child `malachi.broker.produce` span. The same at the deeper hop:
            `ReplicationServer.replicate` captures the ctx (by then the broker's span) and passes it (the
            message became a 6-tuple across the 3 clauses); `handle_call` attaches it and opens a
            `malachi.replication.commit` span. Since the ctx is a serializable map, this **links across
            nodes** (a produce on one node, replication on the remote primary, one trace). With tracing off
            by default, `get_current`/`attach`/`detach` are cheap process-dictionary operations when there
            is no span. Tested: one produce generates the 3 spans with the **same trace_id** and the right
            parent chain (produce → broker.produce → replication.commit). 703 tests, 0 failures; credo and
            dialyzer clean. **Observability (A/B/C) is complete.**
  - ✅ **Cliente de referência (Node.js) reformulado para a nova arquitetura.** Os scripts Node.js antigos
    falavam o protocolo JSON de fila/canal (removido no B3b); reescritos para o **protocolo binário
    (`Malachi.Wire`) + modelo de log** (topic/key/cursor opaco). Estrutura: `scripts/lib/wire.js`, port
    fiel do codec (framing length-prefixed, envelope request/response, `put_str` com flag de presença,
    records **sem offset**, todos os payloads das 7 operações); `scripts/lib/client.js`, conexão TCP que
    **multiplexa requests por `correlation_id`** e roteia os frames de **push** (o servidor reusa o corr_id
    do `subscribe`) para o callback da subscription, não para um request one-shot; `scripts/lib/cli.js`:
    cores/config-de-env/parse-de-args compartilhados (DRY, os scripts antigos duplicavam). CLIs: `producer.js`
    (append por chave, `--create`/`--key`/`--continuous`), `consumer.js` (pull dirigido por cursor;
    `--group` resume + commita server-side; `--follow` long-poll), `subscriber.js` (server-push streaming
    subscribe+ack com janela de crédito: substitui o `channel-*` pub/sub, que sumiu com o modelo de canal).
    **Deletados:** `channel-publisher.js`/`channel-subscriber.js`/`channel-demo.sh` (modelo de canal
    removido) e `i18n.js` (órfão; os novos scripts usam strings inglesas inline). `channel-demo.sh` virou
    `streaming-demo.sh` (append → stream ao vivo). Validado e2e contra o servidor real: auth →
    create_topic → produce → fetch-por-cursor (avança/drena) → commit+resume-de-grupo (2ª run consome 0) →
    streaming push+ack (pré-existentes + ao vivo), e caminhos de erro limpos (`permission_denied`,
    `invalid_credentials`: sem crash). README com a seção do cliente; `package.json` atualizado (v2, scripts
    produce/consume/subscribe/demo). **Sem dependências** (só `net` da stdlib).
  - ✅ **Load-test harness (Node.js, closed-loop).** `scripts/loadtest.js` gera carga sobre o cliente de
    referência (escolhas: 1A Node reusando o cliente · 2C closed-loop agora, estruturado p/ open-loop depois
    · 3A os quatro cenários). N conexões concorrentes, cada uma num loop `op → await` até o deadline; o driver
    `closedLoop` é **separado das ops** (`produceOp`/`fetchOp`) pra um driver open-loop reusá-las. Cenários:
    **produce** (append por chave), **fetch** (drena backlog dirigido por cursor, rebobina no fim), **stream**
    (server-push subscribe+ack, throughput-only: latência de push não é comparável a round-trip), **mixed**
    (metade produz, metade lê). Métricas: throughput (ops/s, records/s, MB/s) + latência p50/p90/p95/p99 via
    **reservoir sampling** (Algoritmo R, cap `--samples`) com min/max/count/sum exatos à parte (o reservoir
    clipa a cauda). Flags: `--connections/--duration/--batch/--record-size/--keys/--max/--window/--prepopulate/
    --warmup/--json`; tópico único auto-criado por run; `--prepopulate` semeia backlog p/ fetch/stream/mixed.
    Três correções achadas na revisão/validação: (1) backoff no erro não-fatal do `closedLoop` (senão
    busy-spin pegando CPU); (2) `clearTimeout` no `streamDriver` quando `onError` resolve antes; (3) **grupo
    único por invocação** no stream: o warmup commita (ack) até o fim do backlog, então compartilhar grupo
    com o run medido o deixava vazio. Validado e2e (single-node local, records pequenos, ilustrativo):
    produce ~42k rec/s, fetch ~307k rec/s (drain), stream ~26k rec/s (push), mixed ~25k rec/s / 7.5k ops/s:
    0 erros; `--json` e `--warmup` (com reconexão dos clients p/ soltar a subscription) OK. Nota operacional:
    muitas conexões estouram o rate-limit de auth (10/min/IP default), subir `MALACHIMQ_AUTH_RATE_LIMIT` pra
    testes de escala. README com a seção de load test; `package.json` com o script `loadtest`.
    - ✅ **Driver open-loop (o 2C).** `--rate <rps>` dispara requests a uma **taxa de chegada fixa**,
      independente de respostas anteriores, medindo a latência a partir do **tempo agendado** de cada request
      (não do envio real). **correção de coordinated omission**: um stall do servidor aparece como latência
      alta nos requests que empilharam atrás, o que o closed-loop esconderia (o worker parado não emite). O
      driver `openLoop` reusa as mesmas ops (`scenarioOps` extraído, DRY entre os dois drivers); fetch fica
      stateless (ctx novo por request → lê do início). Requests espalhados round-robin no pool (o cliente
      multiplexa por corr_id). Guard de memória: `--max-inflight` (default 100k), ao atingir, para de
      empilhar e **flag `saturated`** (servidor não sustenta a taxa). Report ganha modo, alvo vs. atingido,
      saturação e rótulo CO-corrected (texto + JSON). Não se aplica a `stream` (push; `--rate` ignorado com
      aviso). Validado e2e: taxa **sustentável** (1200 rps → atingiu 1199, latência estável p50 1.5ms);
      **sobrecarga** (5000 rps acima da capacidade single-node → latência CO explode, mean 2.6s/p99 5.1s, o
      sinal que o closed-loop mascara); **guard** (`--max-inflight 200` flagou saturação e parou); closed-loop
      inalterado. Três correções da revisão anterior seguem (backoff, timer, grupo único). README/help
      atualizados.
  - ✅ **Deploy multi-nó/replicado (incremental: D1 → D2 → D3).** As peças de HA já existiam e eram
    testadas isoladamente (SWIM membership, replicação por quórum cross-node, `ra`, self-healing,
    failover); esta fase **liga-as na aplicação**. Descoberta de nós **estática via config** (o SWIM
    detecta falhas em runtime; `libcluster` fica para depois).
    - ✅ **D1: Control plane HA (metadata via `ra`).** `application.ex` sobe o `Malachi.LogBroker` com
      `metadata_cluster`/`metadata_nodes` quando `:log_cluster` está configurado (env
      `MALACHIMQ_LOG_CLUSTER`/`MALACHIMQ_LOG_NODES`/`MALACHIMQ_RA_DATA_DIR`), iniciando `ra`; **ausente
      = single-node in-memory** (default preservado). A decisão config→opções é uma função pura
      testável (`Malachi.Application.metadata_cluster_opts/2`). O mecanismo (metadata sobrevive à perda do
      líder) já é provado por `metadata_ha_test`/`broker_server_ra_test`, não duplicado. Também
      **isolado o `log_data_dir` por execução de teste** (`config/test.exs`): o dir fixo persistia entre
      runs e, com metadata in-memory reiniciando, um topic reusado colidia com um segment em disco
      (`Log.ensure_active :already_exists`) → flakiness e2e; agora cada run usa um dir próprio, limpo no
      `after_suite`.
    - ✅ **D2: Data plane replicado.** Em modo cluster, `application.ex` sobe um `ReplicationServer`
      **nomeado** (`Malachi.LogReplication`) por nó e liga o `LogBroker` a `brokers: [{Malachi.LogReplication,
      n} | n ← log_nodes]` + `replication_factor` (env `MALACHIMQ_LOG_REPLICATION_FACTOR`, default 3,
      clampado ao nº de nós pelo broker). O `Placement` (HRW) escolhe o `replica_set` entre esses brokers;
      o primário replica cross-node via `{name, node}` e commita por **quórum**. Broker set **estático**
      (todos os nós da config); um follower caído é tolerado pelo quórum (o `live_brokers` ao vivo é D3).
      Fiação testável por funções puras (`Malachi.Application.broker_refs/1`, `data_plane_opts/2`); o mecanismo de
      quórum/tolerância já é coberto por `replication_server_test`, e a integração (BrokerServer + 3 brokers
      + rf=3 + ra → produce/consume por quórum ponta a ponta) por `broker_server_ra_test`.
    - ✅ **D3, Membership + healing/failover ao vivo.**
      - ✅ **D3a: `MembershipServer` cross-node.** O SWIM identificava cada membro pelo `self_ref`, que
        era o `:name` de registro: os testes eram todos in-process (átomos únicos). Cross-node isso
        colidia: o mesmo átomo `Malachi.LogMembership` resolve para o servidor **local** em cada nó, então
        o remetente gossipado apontava para o próprio receptor e a view não convergia. Agora o
        `MembershipServer` aceita `:self_ref` (identidade **node-qualified** `{name, node()}`, gossipada)
        distinto do `:name` (registro local), + `start/1` (não-linkado, p/ iniciar em nós remotos). Provado
        por **teste multinode** real (`membership_ha_test`): 3 nós `:peer` convergem via seeds e detectam a
        morte de um nó (SWIM: suspeita → dead → sai do alive set).
      - ✅ **D3b: Fiação na app.** Em modo cluster, `application.ex` sobe (nesta ordem)
        `MembershipServer` → `ReplicationServer` → `LogBroker` → `HealCoordinator`. O `MembershipServer`
        usa `self_ref: {Malachi.LogMembership, node()}` e `peers: membership_seeds(log_nodes)` (os outros
        nós). O `live_brokers` (fun) deriva de `alive_members` → refs `{Malachi.LogReplication, node}`, e é
        passado ao `LogBroker` (que estreita o placement de novos segments ao conjunto vivo; `:brokers`
        estático é o placement inicial) **e** ao `HealCoordinator`. O `HealCoordinator` (`metadata_source:
        BrokerServer.metadata`, `apply_command: BrokerServer.apply_heal`, `replication_factor`) fecha o loop
        *broker morre → membership marca gone → segments re-replicados + primário promovido*. Fiação pura
        testável (`membership_seeds/1`, `live_replication_refs/1`); o loop de healing/failover já é coberto
        por `heal_coordinator_test`/`self_healing_test`/`failover_test`, e o membership cross-node por D3a.
  - ✅ **Deploy multi-nó/replicado completo** (D1 control plane HA + D2 data plane replicado + D3
    membership/healing/failover ao vivo). Ligado por config estática; `libcluster` (descoberta dinâmica)
    fica como conveniência futura.
- ✅ **C. Features NorthGuard restantes.** Decisão: começar por **C1 - retenção (tempo+tamanho)**;
  attributes (C2) e policies (C3) depois. Design aprovado: `sealed_at` explícito no segment (idade),
  retenção por tamanho **por range**, e consumidor num dado expirado **avança para o início disponível**
  (mantém o cursor opaco). Sub-fatias: C1a (primitiva de delete) → C1b (coordenador + política + fiação).
  - ✅ **C1a: control plane.** `segment_meta` ganha `sealed_at` (epoch ms, `nil` enquanto ativo); o
    comando `seal_segment` carrega o `sealed_at` (gerado no `Broker` como os timestamps de `Record`, então
    determinístico entre réplicas). Novo comando `{:delete_segment, segment_id}`: remove um segment
    **selado** do control plane (`:segment_active` se ainda ativo, nunca dropa o ativo; `:no_such_segment`
    se ausente). Testado: unit do `delete_segment` (selado/ativo/inexistente), `sealed_at` no seal,
    determinismo preservado (property tests).
  - ✅ **C1a: storage delete.** Cada segment do broker é um `Log` num subdiretório próprio, e o
    `ReplicationServer` guarda `logs: %{segment_id => Log}`; então deletar é **fechar o `Log` + apagar o
    diretório**: sem precisar de delete granular no `SegmentStore`. `Log.delete/1` (fecha + `rm_rf` do
    diretório, best-effort). `ReplicationServer.delete/2` (client + handle_call): fecha/apaga se o log
    está aberto, senão limpa arquivos órfãos em disco (pós-restart); **idempotente** (deletar um segment
    desconhecido é `:ok`). Testado: `Log.delete` (diretório some), `ReplicationServer.delete` (dados
    somem → read vira `:eof`; idempotência).
  - ✅ **C1b: coordenador + read path + fiação** (incremental).
    - ✅ **C1b-1: política + `RetentionCoordinator`.** `segment_meta` ganha `byte_size` (via `seal_segment`,
      do `active.bytes` do Broker: determinístico, como `sealed_at`; retenção por tamanho precisa de bytes).
      Módulo **puro** `Malachi.Cluster.Retention`: `expired(metadata, now_ms, policy)` → ids de segments
      **selados** a expirar por **idade** (`sealed_at` > `max_age_ms`) e por **tamanho** (soma `byte_size`
      por **range** > `max_bytes` → mais antigos primeiro), unidos; nunca o ativo; bound `nil` desliga a
      regra. `RetentionCoordinator` (GenServer periódico, modelo `HealCoordinator`) com seams
      (`metadata_source`, `expire_segment`, `policy`, `clock`, `interval`): cada sweep resolve os ids para
      seus metas e chama `expire_segment`. Testado: `Retention` (idade, tamanho por-range, união, nunca o
      ativo, `nil` desliga) e o coordenador (sweep via `run_now` e via tick, meta completa ao seam).
    - ✅ **C1b-2: read path (avança em dado expirado).** Retenção deleta sempre um **prefixo contíguo**
      (os segments mais antigos), então o read path só precisa saber o menor `start_offset` ainda armazenado
      (`earliest_offset/2`) e **clampar o offset a ele** antes de ler. `consume_page` (consumo ao vivo) e
      `read_history_page` (admin) clampam: um consumidor cujo cursor caiu num buraco expirado avança
      transparentemente para o dado vivo (at-least-once, cursor opaco intacto) em vez de ver `:eof` enganoso;
      como não há buracos internos, `offset + length` continua correto (sem mudar `read`/`locate_segment`).
      Testado: consumidor abaixo do earliest pula os segments expirados e lê o que resta.
    - ✅ **C1b-3: config + fiação (retenção C1 completa).** `Broker.delete_segment/2` +
      `BrokerServer.delete_segment/2` (aplicam `:delete_segment` pelo control plane, Raft-backed quando
      configurado). A `expire_segment` real (na `application.ex`): remove do control plane e então deleta
      o storage em cada réplica (`ReplicationServer.delete`), **best-effort** (control plane idempotente,
      storage tolera segment ausente). Config por env: `MALACHIMQ_RETENTION_MAX_AGE_MS` /
      `MALACHIMQ_RETENTION_MAX_BYTES` (ambos ausentes = **guarda para sempre**, coordenador não sobe) /
      `MALACHIMQ_RETENTION_INTERVAL_MS`. O `RetentionCoordinator` sobe na árvore **sempre que há política**
      (importa single-node também), após o `LogBroker`. Testado: `BrokerServer.delete_segment` (drop do
      selado), e **e2e** (produce → sela → sweep do coordenador → segment some do control plane **e** do
      storage). **C1 (retenção tempo+tamanho) completa.**
- ✅ **C2: Attributes** (k/v opacos que o admin liga a brokers; base de rack/DC-awareness).
  **Decisão:** disseminar via **Membership/SWIM** (fiel ao NorthGuard: "membership piggyback host/port/
  attributes"), não no Metadata. O usuário priorizou fidelidade. Incremental: C2a (Membership puro) →
  C2b (server + API + gossip) → C2c (fiação + config).
  - ✅ **C2a: `Membership` puro com attributes.** Os attrs de um membro **viajam com o update**,
    governados pela mesma **incarnation**: o `update` vira 4-tupla `{member, status, incarnation,
    attributes}` e o `member_state` ganha `attributes`. Só o dono muda seus attrs, via `set_attributes/2`,
    que **sobe a própria incarnation** para a mudança vencer o merge em todo lugar; uma suspeita/confirmação
    de outro nó carrega os attrs **já conhecidos** (preserva-os). O `overrides?` (precedência `{incarnation,
    rank}`) não muda. `new/2` aceita `:attributes` do self; query `attributes/2`. Testado: propagação/troca
    por incarnation, `set_attributes`, preservação em suspect, gossip via `updates`; convergência
    order-independent preservada (property; attrs são consistentes por-incarnation, então os generators
    usam `%{}`). Multinode SWIM (D3a) segue convergindo com a 4-tupla.
  - ✅ **C2b. `MembershipServer` com attributes.** Opção `:attributes` (attrs iniciais do self,
    passados ao `Membership.new`); API `set_attributes/2` (muda os próprios attrs em runtime, sobe a
    incarnation) e `attributes/2` (lê os de um membro). Disseminação é **passiva** (anti-entropy): o
    server ignora os effects e o gossip periódico (ping/ack piggyback `updates`) propaga, nenhum push
    proativo, consistente com o resto do server. Testado: attrs iniciais legíveis, e `set_attributes` num
    nó propaga a um peer via gossip.
  - ✅ **C2c: fiação na app (C2 completa).** `application.ex` liga os attributes do self no
    `MembershipServer` do cluster: `MALACHIMQ_LOG_ATTRIBUTES` (formato `"rack=a,dc=east"`) é parseado por
    `Malachi.Application.parse_attributes/1` (função pura testável: ignora entradas sem `=`, trima, preserva `=` no
    valor) e passado como `:attributes`. Ausente → `%{}`. Testado: parse (vazio, pares, trim, entradas
    inválidas, valor com `=`). **C2 (attributes via SWIM) completa**: os brokers disseminam seus attrs por
    gossip, prontos para o placement rack-aware de C3.
- ✅ **C3: Policies** (nome + retenção + constraints sobre attributes → replica sets; fiel ao NorthGuard,
  que unifica tudo em *policies*). Incremental: C3a (Placement puro com spread) → C3b (integração: attrs do
  membership → placement) → C3c (policies por-topic: definição + associação + retenção por-topic).
  - ✅ **C3a. `Placement` puro com spread (rack-aware).** `place/4` ganha a opção `:spread =
    {attribute_key, attributes}`: sobre o ranking HRW (determinístico), faz **round-robin pelos valores
    distintos** do attribute: o melhor-rankeado de cada valor primeiro, depois o próximo de cada, até `rf`.
    Com `rf ≤ nº de valores`, cada réplica num rack/DC distinto; senão best-effort round-robin. Determinístico
    (ranking HRW + agrupamento estável); sem `:spread` → top-`rf` de antes (todos os callers de `place/3`
    intactos). Testado: valores distintos, prioriza diversidade sobre rank puro (rf=2 em a,a,b → a,b),
    best-effort com rf > valores, brokers sem o attribute agrupam à parte, determinismo.
  - ✅ **C3b: integração (attributes → placement).** O `Broker` ganha `spread_by` (a chave de attribute)
    e `broker_attributes` (map broker→attrs); `open` os aceita, `set_broker_attributes/2` os atualiza (como
    `set_brokers`), e `open_segment` passa `:spread` ao `Placement.place` quando `spread_by` está setado
    (senão placement normal. Todos os callers intactos). O `BrokerServer` aceita `:spread_by` + uma fun
    `:broker_attributes` e a **refresca periodicamente** (mesmo timer de `:live_brokers`) para o broker:
    então os attrs disseminados pelo membership (C2) fluem ao placement. Testado: produce espalha réplicas
    por rack, `set_broker_attributes` afeta o placement seguinte, refresh do `BrokerServer` puxa os attrs.
  - ✅ **C3c-1: fiação do rack-aware na app.** `data_plane_opts` liga `spread_by` (env
    `MALACHIMQ_LOG_SPREAD_BY`, ex: `"rack"`) e uma fun `broker_attributes` derivada do `MembershipServer`:
    `broker_attributes_for/2` (pura, testável) mapeia cada membro vivo `{LogMembership, node}` →
    `{LogReplication, node}` com os attrs gossipados (C2). Com isso o **placement rack-aware funciona ponta
    a ponta na aplicação** (attrs do membership → spread do placement). Ausente `spread_by` → HRW puro.
  - ✅ **C3c-2. Policies por-topic** (o guarda-chuva NorthGuard). Decisão: policies **dinâmicas no
    Metadata (`ra`)** + escopo **ambos** (retenção + placement por-topic). Incremental: 2a (Metadata:
    policies + associação) → 2b (retenção por-topic) → 2c (placement por-topic). **Fecha C3.**
    - ✅ **C3c-2a. Metadata: policies + associação.** `Metadata` ganha `policies: %{name => policy}`
      (`policy = %{optional(:retention) => %{max_age_ms, max_bytes}, optional(:spread_by) => term}`), o
      `topic_meta` ganha `policy: name | nil`, e dois comandos: `{:define_policy, name, policy}` (valida
      name binário não-vazio + policy map; `:invalid_policy` senão) e `{:set_topic_policy, topic,
      name | nil}` (`:no_such_topic`/`:no_such_policy`; `nil` desassocia → volta aos globais). Queries
      `get_policy/2` e `topic_policy/2` (resolve a policy do topic). Determinismo preservado (property).
      **Sem uso ainda**: 2b/2c ligam retenção/placement à policy do topic.
    - ✅ **C3c-2b: retenção por-topic.** `Retention.expired/3` passa a resolver, **por range**, a retenção
      efetiva = a `:retention` da policy do topic (`Metadata.topic_policy/2`) **mesclada sobre** a policy
      global (a policy sobrepõe só as chaves que define; `Map.merge`), ou a global quando o topic não tem
      policy. O `RetentionCoordinator` não muda (já passa o metadata + a global). Testado: policy do topic
      sobrepõe a global, merge (chave não-definida cai na global), topic sem policy usa a global.
    - ✅ **C3c-2c: placement por-topic.** `Broker.open_segment` resolve, por range, o `spread_by`
      efetivo = o `:spread_by` da policy do topic (`Metadata.topic_policy/2`) quando a policy **define**
      essa chave (`nil` explícito opta o topic **fora** do spread, rendezvous puro), sobrepondo o global;
      senão o `spread_by` global do broker. Simétrico ao 2b (chave definida vence, `nil` incluso). Só
      `place_opts/effective_spread_by` mudam. Testado: policy liga o spread sobre um global-off; `nil`
      explícito desliga sobre um global-on (== rendezvous puro).
- ✅ **D. Sharding via `ReplicatedDSRSM`** (agora **no alvo**): metadata sharded (um cluster `ra`
  por vnode) para **escalar o control plane** além de um cluster Raft único. Decisão: **1A**, o cache
  do `Broker` vira um `DSRSM` (espelha o par `Metadata`/`ReplicatedMetadata`), roteando leituras/escritas
  por topic; e **2A**. Incremental, núcleo puro primeiro. Infra já pronta: `HashRing`, `DSRSM` (puro),
  `ReplicatedDSRSM` (ra), `MetadataMachine`/`MetadataServer`.
  - ✅ **D-a: `Broker` sobre `DSRSM` (in-memory, 1 vnode).** O cache do `Broker` deixa de ser um
    `Metadata` e passa a ser um `DSRSM` (`broker.dsrsm`), roteado por topic (derivável do `range_id`
    `{topic, seq}`/`segment_id`). Novo combinador puro `DSRSM.update_vnode/3` (roteia + aplica uma
    função ao `Metadata` do vnode); `DSRSM.command/3` delega a ele com `&Metadata.apply/2` (property
    tests intactos). `command_fun` do `Broker` vira `(DSRSM, topic, command) -> {DSRSM, reply}` (default
    `&DSRSM.command/3`); `apply_metadata` deriva o topic via `command_topic/1`. Acessores novos no
    `DSRSM`: `single/1` (forma trivial 1-vnode p/ seed), `committed_offsets/3`, `topic_policy/2`,
    `merged_metadata/1` (união dos shards → `Broker.metadata/1` p/ retention/healing). No `BrokerServer`,
    o caminho Raft embrulha o cluster único como `DSRSM.single(seed)` + um `command_fun/3` que injeta
    `ReplicatedMetadata.apply_command` no `update_vnode` (D-b troca pelo `ReplicatedDSRSM` real). Com 1
    vnode, comportamento idêntico: suite completa verde (981) como rede de segurança.
  - ✅ **D-b** (runtime/`BrokerServer` sobre `ReplicatedDSRSM`, N vnodes). Decisão: **1A**, D-b-1
    (N vnodes **single-node**) primeiro; HA-por-vnode (D-b-2) depois.
    - ✅ **D-b-1. Control plane sharded single-node.** `BrokerServer` ganha o caminho `:metadata_vnodes`
      (`[{cluster_name, token}]`): inicia N clusters `ra` via `ReplicatedDSRSM` (um por vnode), materializa
      o cache local com `ReplicatedDSRSM.snapshot/1` (novo: lê o `Metadata` de cada vnode → `DSRSM.seed/2`,
      novo, compartilhando o ring), e um `command_fun/3` sharded que roteia por topic ao cluster `ra` do
      vnode (`ReplicatedDSRSM.server_for/2`, novo) aplicando via `ReplicatedMetadata.apply_command` no
      `update_vnode`. O caminho `:metadata_cluster` (1 vnode, D-a/D1) segue intacto. Config: `log_vnodes`
      (inteiro N; `Malachi.Application.sharded_vnodes/2` gera N vnodes com tokens uniformes no ring de 32 bits).
      Testado: 2 vnodes, cada topic vive em exatamente o cluster `ra` que seu nome roteia (nunca no outro),
      e os topics se distribuem entre os vnodes; `sharded_vnodes/2` (tokens distintos, em range).
    - ✅ **D-b-2: HA por vnode.** `ReplicatedDSRSM.add_vnode/4` passa a receber os `nodes`, iniciando o
      cluster `ra` de cada vnode sobre eles (`MetadataServer.start/2`), então cada vnode sobrevive à perda
      de um membro. Modelo: **todos os vnodes sobre o mesmo conjunto de M nós** (espelha o D1; placement de
      vnodes por subconjuntos de nós fica para depois). `BrokerServer` passa os `metadata_nodes` ao caminho
      sharded (`start_vnodes/2`); `Application.metadata_opts` inclui `metadata_nodes` no caminho sharded.
      `snapshot/1` usa `&Function.identity/1` (query linearizável roda no líder, possivelmente remoto).
      Testado (`:multinode`): 2 vnodes sobre 3 nós, mata um membro do vnode dono (o líder se for peer →
      failover; senão um follower), o vnode ainda commita e os metadados (dele e do outro vnode) intactos.
  - ✅ **D-c: gestão do control plane por vnode** (retention/healing/failover). **Concluído por 1C-a +
    1C-b** (coordinators só-no-líder + manager per-vnode-leader; ver as sub-fatias abaixo). O texto a
    seguir é o **contexto do débito** que motivou 1C. O estado *antes* de 1C. **Estado pré-1C:** as
    *escritas* de metadata já eram sharded (D-b), mas a *gestão* seguia **centralizada**, um
    `RetentionCoordinator` e um `HealCoordinator` no nó do `BrokerServer` leem `merged_metadata` (a
    **união** de todos os shards) e emitem comandos (`delete_segment`/`set_segment_replicas`) que
    **roteiam de volta** por topic ao vnode dono (via `command_topic/1`). Isso é **correto** sob
    sharding (a união é exata; os comandos roteiam), mas reintroduz conceitualmente o ponto único que
    o sharding elimina: um **débito de fidelidade de sequenciamento**, não de correção.

    O alvo fiel ao NorthGuard é **1C: um coordinator vivendo na liderança do grupo Raft de cada vnode**
    (cada nó gerencia retention/healing dos vnodes que lidera). Isso **pertence à Fase 1** (distribuição),
    **não** à Fase 2 (eficiência nativa/profiling). O motivo de 1C não vir já não é ser "otimização",
    e sim ter **pré-requisitos**:
      1. **Placement de vnodes por subconjuntos de nós** (hoje todos os vnodes vivem nos mesmos M nós:
         adiado em D-b-2). Sem espalhar os vnodes, "o líder do vnode" é qualquer um dos M nós e há pouca
         distribuição real a fazer. **Fatia D-c-1** (decisão: **1A** HRW reusando `Placement`; **2A**
         núcleo puro primeiro):
           - ✅ **D-c-1a: núcleo puro.** `Malachi.Application.place_vnodes/3` atribui a cada vnode
             (`{vnode_id, token}`) os `R` nós do seu cluster `ra`, escolhidos de `nodes` por rendezvous
             (o mesmo HRW `Placement.place/4` dos segments) → `{vnode_id, token, nodes}`; determinístico,
             mínimo movimento, `R` efetivo = `min(R, M)`. Testado isolado (HRW espalha, determinismo,
             clamp). **Sem uso ainda**, D-c-1b liga ao `ReplicatedDSRSM`/`BrokerServer`.
           - ✅ **D-c-1b: roteamento cross-node.** `MetadataServer.start/2` passa a devolver o server de um
             **membro real** (o nó local quando é membro; senão o primeiro do placement) em vez do local
             sempre, então um vnode colocado num subconjunto de nós é alcançável de um nó que **não** hospeda
             réplica dele (o `ra` roteia `command`/`query` desse membro ao líder; o chamador não precisa ser
             membro). `ReplicatedDSRSM` armazena esse server; `command`/`query`/`snapshot`/`server_for`
             passam a funcionar cross-node. Testado (`:multinode`): 2 vnodes em subconjuntos **disjuntos** de
             3 nós, orquestrados de um nó que **não** hospeda nenhum, commits/queries roteiam ao membro certo
             e o `snapshot` materializa tudo. (Decisão **1A**: mecanismo isolado do bootstrap distribuído.)
           - ✅ **D-c-1c: bootstrap distribuído (seed estático).** `Application.metadata_opts` liga o
             `place_vnodes` (`metadata_vnodes` vira `[{vnode_id, token, nodes}]`, R = `log_vnode_replication_factor`)
             e injeta a política `bootstrap_orchestrator?` = `Malachi.Application.static_seed/1` (verdade só no menor nó).
             No `BrokerServer`, o **orquestrador** faz `add_vnode` (start_cluster) de cada vnode; os **não-orquestradores**
             fazem `ReplicatedDSRSM.route_vnode/4` (novo: registra no ring + server de um membro, **sem** iniciar), de
             modo que exatamente um nó bootstrapa cada vnode (padrão RabbitMQ/`ra`). O `snapshot/1` ficou **tolerante**
             (vnode não-pronto → `Metadata` vazio, sem crash) e o `BrokerServer` **re-seeda o cache** dos clusters `ra`
             logo após o boot (janela de eleição) e periodicamente (`Broker.put_cache/2`), o que também cobre o
             multi-writer. Escolha **1B/seed estático** (vs 1A concorrente, arriscado no `ra`; vs orquestração-pelo-líder,
             que é a D-c-1d com fencing). Testado: `static_seed` (só o menor nó), `route_vnode` + `snapshot` tolerante
             (single-node), e `:multinode`: orquestrador inicia sobre 2 nós, não-orquestrador só roteia e lê/escreve
             cross-node. **Config:** `MALACHIMQ_LOG_VNODE_REPLICATION_FACTOR`.
           - ✅ **D-c-1d: `membership_leader` + reconcile loop.** A política de orquestração passa do seed
             estático para `Malachi.Application.membership_leader/1`: verdade só no menor nó **vivo** (`MembershipServer.
             alive_members`, SWIM), então o papel **faz failover** quando o líder cai (tolerante: se a membership
             não responde → não-líder, nunca dois). O bootstrap vira **reconcile** (controller-style, k8s): no
             boot **todo** nó só faz `route_vnode` (`build_replicated` sem `start`); o `BrokerServer` reconcilia
             (level-triggered, idempotente) logo após o boot e periodicamente, e **só no líder** - chamando
             `MetadataServer.start/2` nos vnodes cujo cluster ainda não está pronto (`MetadataServer.ready?/1`,
             novo). O **fencing** é o nome do cluster `ra` (um segundo `start` do mesmo vnode falha sem
             duplicar: validado empiricamente); o *lease* (jeito k8s literal) fica para o **1C**, onde o líder
             passa a fazer trabalho **contínuo** (retention/healing/rebalancing). `static_seed/1` permanece como
             alternativa (testada). Testado: `membership_leader` (menor-vivo, tolerância); integração, o líder
             bootstrapa os vnodes via reconcile e um `create_topic` commita. Ver seção 8 (referências k8s/riak_core).
      2. **Detecção/reação a liderança Raft por vnode**: um supervisor que sobe/derruba coordinators
         conforme a liderança muda (via eventos do `ra`), tolerando oscilação e split-brain momentâneo.
    Sequência: D-b ✅ → **D-c-1 placement de vnodes** ✅ → **1C-a coordinators só-no-líder** ✅ →
    **1C-b-i detecção de liderança Raft por vnode** ✅ → **1C-b-ii-α coordinator apontado a um vnode** ✅
      → **1C-b-ii-β supervisor/manager per-vnode-leader** ✅. **1C-b completo.**

    - ✅ **1C-a: coordinators só-no-líder (sem lease).** `RetentionCoordinator` e `HealCoordinator`
      ganham o seam `:leader?` (`(-> boolean())`, default sempre); a cada tick, só varrem/curam se
      `leader?()`: senão o tick corre mas pula. O `Application` injeta `membership_leader(Malachi.
      LogMembership)` (reusa D-c-1d) nos dois quando clustered (`coordinator_leader?/1`; single-node =
      sempre age). Elimina a **redundância** (N nós faziam o mesmo trabalho) mantendo o modelo
      level-triggered. **Sem lease:** o trabalho é idempotente + roteado ao `ra` (serial), então dois
      coordinators transitórios (convergência SWIM) só refazem trabalho, não corrompem, o mesmo
      raciocínio do bootstrap. `run_now/1`/`heal_now/1` ignoram o gate (triggers manuais). Testado:
      não-líder pula o tick; o trigger manual age mesmo assim.
    - ✅ **1C-b-i: detecção de liderança Raft por vnode (núcleo puro).** `MetadataServer.leader?/1`
      espelha `ready?/1`: lê o líder que `:ra.members` reporta (qualquer membro alcançável responde) e
      é verdade só se ele for o **próprio** `server_id`, passe o server **local** (`{vnode_id, node()}`)
      para perguntar "este nó lidera este vnode?". Cluster não-formado/inalcançável → false (nunca
      assume liderança). `Malachi.Application.leading_vnodes/3` é o seletor puro: dado o placement
      (`[{vnode_id, token, nodes}]` do bootstrap), o nó local e o predicado `leader?` (default
      `MetadataServer.leader?/1`), retorna os vnodes que o nó **hospeda** (placement o inclui) **e**
      **lidera**: curto-circuitando `leader?` para vnodes não-hospedados. É onde 1C-b-ii vai rodar os
      coordinators, um por vnode, no líder Raft dele (o NorthGuard literal, distribuindo a carga vs o
      líder único de membership do 1C-a). Testado: `leader?` no líder single-node (ra real) + não-formado
      → false; `leading_vnodes` filtra host×lidera, preserva ordem, não consulta liderança de não-hospedado.
    - ✅ **1C-b-ii-α: coordinator apontado a um vnode.** `Malachi.Application.vnode_metadata_source/1` é um
      `metadata_source` ligado a **um** vnode: lê a visão local do `Metadata` daquele vnode via
      `MetadataServer.query({vnode_id, node()}, & &1)` (consistent query ao ra do vnode), **tolerante**
      (vnode não-formado/inalcançável → `Metadata.new()`, sem crashar o coordinator). Como o tipo é o
      mesmo (`Metadata.t()`) e `expire_segment`/`apply_heal` já roteiam ao vnode dono por topic, o
      `RetentionCoordinator`/`HealCoordinator` **não mudam**: basta trocar o source (por-vnode em vez do
      merge global) e o gate (`MetadataServer.leader?({vnode_id, node()})`). Testado (ra real): source
      tolerante em vnode não-formado; lê só o shard do vnode; um `RetentionCoordinator` ligado a um vnode
      expira só os segments **daquele** vnode.
    - ✅ **1C-b-ii-β, supervisor/manager per-vnode-leader.** `Malachi.Cluster.VnodeCoordinatorManager`
      (GenServer genérico, testável por seams `leading`/`spawn`/`stop`) reconcilia por **polling
      level-triggered** (logo após o boot via `handle_continue`, depois a cada `:vnode_reconcile_interval_ms`,
      default 5s): compara os vnodes que este nó lidera agora (`leading_vnodes/3` sobre `MetadataServer.
      leader?`) com os que já roda, **sobe** um par retention+heal para os recém-liderados e **derruba** os
      que deixou de liderar. Cada par vive sob um **supervisor por-vnode** (`:one_for_one`) num
      `DynamicSupervisor` (`Malachi.LogVnodeCoordinatorSupervisor`), para que um coordinator que crashe
      reinicie sem o manager perder o handle. No `Application`, `coordinator_children/2` **substitui** os
      coordinators únicos do 1C-a pelo par supervisor+manager **quando sharded** (`vnode_placement/2` ≠
      nil, extraído e reusado por `metadata_opts`); single-node / cluster de 1 vnode seguem no 1C-a.
      Cada coordinator mantém o gate `MetadataServer.leader?({vnode_id, node()})` como **defesa em
      profundidade** (se o manager atrasar numa oscilação, o coordinator não age após perder a liderança).
      **Idempotente + sem lease** (mesmo raciocínio do 1C-a): um flap transitório só refaz trabalho
      roteado ao `ra`, não corrompe. Testado: reconcile por seams (sobe/derruba/idempotente/esvazia) +
      integração (ra real, single-node): o manager sobe um `RetentionCoordinator` real por vnode liderado
      e cada um expira só os segments do **seu** vnode. O **lease sobre `ra`** fica para quando o
      coordinator ganhar trabalho **não-idempotente** (rebalancing com movimento de dados).

    (A alternativa **1B**: coordinators iterando por-vnode mas ainda centralizados - evita materializar
    o merge, mas é um meio-termo sem gargalo medido; preterida em favor de ir direto ao placement.)

### Fase 2, Eficiência nativa (condicional, guiada por profiling)
- `Malachi.SegmentStore.Native` em Rust (Rustler): O_DIRECT, cache de app, `erlang-rocksdb`.
- Só implementar se a Fase 1 mostrar cauda de latência/page-cache como gargalo sob concorrência.

### (Futuro) Camada Xinfra-like
- Virtual topics com epochs, offsets opacos, migração dual-write, consumer-group management.
  Fora do escopo inicial.

---

## 5. Candidate dependencies

| Need | Library | Notes |
|---|---|---|
| Raft (DS-RSM/vnodes) | [`ra`](https://github.com/rabbitmq/ra) | RabbitMQ's production Raft |
| SWIM/gossip membership | [`partisan`](https://github.com/lasp-lang/partisan) | or our own implementation |
| Native sparse index (phase 2) | [`erlang-rocksdb`](https://github.com/emqx/erlang-rocksdb) | RocksDB binding |
| Native NIF (phase 2) | [`rustler`](https://github.com/rusterlium/rustler) | memory-safe Rust NIFs + dirty schedulers |
| Property testing | `stream_data` / `PropEr` | a partial substitute for deterministic simulation |

---

## 6. What we will NOT replicate (and the substitute)

**Deterministic simulation** (single-threaded cluster and clients, swappable time/net/disk/RNG, exact
replay of failures) is one of NorthGuard's reliability pillars, and it is **essentially unfeasible on
the BEAM**, because we do not control the scheduler (preemptive, multicore). It is a real downgrade in
guarantees and has to be accepted explicitly.

Substitutes:
- Property-based stateful testing (`stream_data`/`PropEr`) of the log model and the state machine.
- `Concuerror` for concurrency checking (limited scale).
- Jepsen-style tests for distributed consistency.
- Fault injection through `partisan` (network partitions) plus storage chaos (corruption, I/O errors).

---

## 7. Architecture decisions (the four questions that opened the port)

This section began life as "open questions". All four were **decided and implemented**; they stay here
with the answer and the reason, which is the part that still has value. Each points at the code that
implements it.

1. **Replication: over `ra` or from scratch?** → **Both, in separate planes.** The **control plane**
   (metadata) runs over `ra`: each vnode is a Raft cluster, with the machine in
   `Malachi.Cluster.MetadataMachine` (`@behaviour :ra_machine`). The **data plane** (records in
   segments) does **not** go through `ra`: it is our own quorum replication in
   `Malachi.Cluster.ReplicationServer`, which ships from the primary to the followers and only
   acknowledges the write once a quorum has `fsync`ed it, tolerating up to ⌊(N-1)/2⌋ slow or
   unreachable followers (`{:error, :no_quorum}` beyond that). The reason for the split: metadata is
   small, needs linearizability and changes rarely, which is exactly Raft's strength; records are high
   volume and sequential, where routing every batch through a consensus log would pay for a second
   durable write and gain nothing, since the segment **is** the log.

2. **Sparse index: our own `.idx`, persisted ETS, or DETS?** → **Our own `.idx` file.** One sidecar per
   segment (`Malachi.Log.Segment.index_path/1` returns `<id>.idx`), loaded into memory as an `:array`
   sorted by offset for an O(log n) floor lookup, with one entry every
   `@default_index_interval` (4096) bytes. Persisted ETS and DETS were both discarded: either would
   couple the on-disk format to BEAM structures, and the point of the port is a segment format that a
   native implementation (phase 2) can reopen without speaking BEAM.

3. **Protocol: keep malachi's or sessionize it like NorthGuard?** → **Kept and extended.** It is still
   `<<len::32, body>>` carrying `<<api_key::16, correlation_id::32, payload>>`, where the
   `correlation_id` already gives pipelining (matching a response to its request), which was the
   concrete gain sessionizing would have brought. Extended to 17 api_keys, covering the log, consumer
   groups, auth (password, mTLS, token) and ACLs. See `Malachi.Wire`.

4. **Offset: opaque or a plain `long`?** → **An opaque cursor from the start**, Xinfra style. The
   client never sees an offset: records on the wire carry none, and the position travels in the cursor
   (`Malachi.LogApi`, `@type cursor :: String.t()`). That is what makes it possible to reshard and
   split ranges without breaking clients, since the position is no longer a number they could have
   interpreted. Because the cursor comes back from an untrusted client, `decode_cursor/1` deserializes
   with `binary_to_term(_, [:safe])` and validates the shape as well, so a forged cursor cannot mint a
   new atom or an arbitrary term.

---

## 8. Referências de design externas: riak_core e Kubernetes

Analisamos três repositórios **riak_core** (a biblioteca de consistent hashing / vnodes / membership /
handoff do Riak) e o **Kubernetes**, para aprender como sistemas maduros resolvem os problemas que a
Fase 1 (distribuição) enfrenta: bootstrap distribuído, leader election com fencing, placement com
tolerância a falha, e reconcile loops.

**Enquadramento comum.** Os três convergem no mesmo padrão para coordenar shards: um **coordenador único
eleito**, com **fencing via consenso**. RabbitMQ (mesma lib `ra` que usamos) fencia pelo **nome do
cluster**; riak_core usa um *claimant* (fencing **fraco**, gossip); k8s usa um *Lease* sobre etcd
(fencing **forte**, CAS linearizável). **Veredito geral: referência de design, não dependência**, o
riak_core é **AP** (gossip + vector clocks; consenso forte só no `riak_ensemble`, externo) e o k8s
centraliza o metadata num **único** cluster **etcd** (Raft, não-sharded). O malachi é **CP e sharded**
(um cluster `ra` por vnode), então nenhum serve como dependência direta, mas os **algoritmos e padrões
são portáveis**: trocando "gossip" por "Raft" e preservando determinismo.

### 8.1 riak_core (AP). Referência: OpenRiak (`~/riak_core`, Apache 2.0, fork ativo)

- **Onde o malachi já está à frente (por ser CP):** metadata em Raft (> gossip); SWIM com suspicion
  (> gossip simples); failover automático; retention; atributos rack/DC. *Não regredir ao portar.*
- **`riak_core_claimant` → D-c-1d + rebalancing:** o modelo **staged → planned → committed** (o *plan*
  computa o ring novo sem mudar estado; o *commit* valida que nada divergiu) + eleição **lexicográfica**
  (menor node = o nosso `static_seed`). O fencing do claimant é **fraco** (gossip + vclock, split-brain
  possível): o que **valida** a decisão do malachi de fenciar o bootstrap pelo **nome** do cluster `ra`.
- **`riak_core_claim_binring` V4 → upgrade do `place_vnodes`:** garante réplicas em nós **e** *locations*
  (rack/DC) distintos (`target_n_val`), balanceamento uniforme (k ou k+1 por nó) e rebalanceamento com
  **movimento mínimo** (`update()` antes de `solve()`). Doc bem comentada: `~/riak_core/docs/claim-version4.md`.
- **Ring versioning + ciclo de claim → re-clustering dinâmico** (add/remove nós), **feito** no rebalancing
  R1→R3 (diff do placement vivo + `apply_plan` sob o lease); o gatilho segue **manual** (ver 8.4).
- **Ignorar:** gossip do ring, vector clocks (o Raft já dá ordem/consenso), `node_watcher` (o SWIM
  resolve), preflist-sobre-vnodes (desvio intencional: o malachi sharda **metadata por topic**, não
  distribui keys de dados sobre o ring).

### 8.2 Kubernetes (CP via etcd único)

- **Leader election + Lease → D-c-1d e, sobretudo, 1C (coordinators no líder):** o "triângulo"
  `LeaseDuration > RenewDeadline > RetryPeriod` + **CAS linearizável** (etcd; o nosso `ra` dá o mesmo) +
  **desistir proativo** ao não conseguir renovar (`OnStoppedLeading`, evita split-brain) + **relógio
  local** (tolera clock skew; premissa: NTP, drift ≤ ~`lease/10`). Traduz para um **lease armazenado num
  cluster `ra`** (comando CAS versionado). **Nuance-chave:** o fencing-por-nome do `ra` **basta para o
  bootstrap** (start único, auto-fencido); o **lease** só é necessário para o **trabalho contínuo** do
  líder, retention/healing/rebalancing (o 1C). Arquivos: `client-go/tools/leaderelection/`
  (`leaderelection.go`, `resourcelock/leaselock.go`), `api/coordination/v1/types.go`.
- **Controller/reconcile → coordinators + reconcile de bootstrap:** **level-triggered** (reconciliar o
  estado completo *desejado × atual*: os coordinators do malachi já fazem isso); **idempotência**;
  **workqueue** com dedupe + rate-limit + retry/backoff; **expectations** (rastrear operações em voo com
  TTL, para não re-agir cedo demais); **só-o-líder-age**; **observabilidade** (healthz de convergência).
  Referência: `pkg/controller/replicaset/replica_set.go`, `client-go/util/workqueue`.
- **Scheduler / PodTopologySpread → `place_vnodes`:** `topologyKey` (= o nosso `spread_by`/atributos),
  **`maxSkew`** (desbalanceamento máximo entre racks/zonas), **`minDomains`** (domínios distintos
  mínimos), **`whenUnsatisfiable`** (hard `DoNotSchedule` vs soft `ScheduleAnyway`), e pipeline
  **Filter → Score**. **Ressalva crítica:** o k8s **randomiza** o tie-break; o `place_vnodes` deve
  permanecer **determinístico** (raft-safe: toda réplica computa o mesmo placement). Portar as *ideias*
  (maxSkew / minDomains / hard-soft) sobre funções puras, sem randomização; e adotar *sticky preference*
  no `heal()` (preferir réplicas sobreviventes → menos churn). Referência:
  `pkg/scheduler/framework/plugins/podtopologyspread/`.
- **Trade-off registrado:** o etcd único é simples, mas é o **gargalo de escala** do k8s (~5000 nodes por
  cluster). O sharding do malachi paga complexidade (bootstrap / leader / fencing) justamente para
  **escalar além de um quórum**: a motivação da fatia D.

### 8.3 Síntese: como isso informou as fatias (o histórico de execução)

> Nota: as fatias abaixo (D-c-1d, 1C, place_vnodes, rebalancing) **estão feitas**, este bloco é o
> histórico. O resumo do que foi adotado × desviado está em **8.4**.

- **D-c-1d (`membership_leader`):** eleição pelo menor nó **vivo** (SWIM) + fencing por nome do `ra` no
  bootstrap; reconcile loop **level-triggered / idempotente** (padrão *controller* do k8s).
- **1C (coordinators no líder):** aqui entra o **Lease sobre `ra`** (k8s) para fenciar o trabalho
  **contínuo**, e o modelo **staged / planned / committed** (riak_core claimant) para mudanças de ring.
- **Upgrade do `place_vnodes`:** `target_n_val` / location-awareness (riak_core binring) + `maxSkew` /
  `minDomains` / hard-soft (k8s topology spread), **mantendo o determinismo**.
  - ✅ **A1: rack-spread (feito).** `place_vnodes/4` ganha `place_opts`, repassado a `Placement.place/4`;
    com `[spread: {attribute_key, node_attributes}]` as R réplicas de cada vnode caem em **racks/zonas
    distintos** (`target_n_val`/`minDomains`), então perder um rack inteiro não leva a maioria das réplicas
    de um vnode. Reusa o `spread` já existente (round-robin por valor de atributo, do C3a). A topologia é
    **config estática** (`log_topology`, `"node=rack,..."`, `parse_topology/1`), idêntica em todo nó →
    placement **determinístico**. `Application.metadata_opts` liga o spread via `vnode_place_opts/0`
    (`:log_spread_by` + `:log_topology`). Testado: cada vnode com R=2 abrange os 2 racks; determinismo.
  - ✅ **A2: balanceamento global de carga (`maxSkew`).** `Placement.place_balanced/4` coloca o
    **conjunto inteiro** de vnodes com **carga limitada** (visão global, não por-vnode): cada vnode ainda
    rankeia por HRW, mas um nó no teto (`ceil(total/nós) + max_skew - 1`) é **pulado** para o próximo, de
    modo que nenhum nó fica sobrecarregado. Determinístico (ranking + ordem + contadores iguais em todo
    nó). O teto é **best-effort**: o RF **nunca** é sacrificado por balanceamento, então um vnode que não
    alcançaria `min(rf, nós)` réplicas distintas pega o nó menos-carregado mesmo acima do teto (possível
    com `rf > 1`, onde o greedy por-vnode não empacota perfeito); com `rf = 1` o teto é **rígido**. Com
    `max_skew` grande degrada a HRW puro (movimento mínimo). É **standalone** (não combina com o
    `:spread` do A1: mutuamente exclusivos; A2 tem precedência). `place_vnodes/4` usa `place_balanced`
    quando `[max_skew: n]`; `vnode_place_opts` liga via `:log_max_skew` (`MALACHIMQ_LOG_MAX_SKEW`).
    Testado: HRW puro empilha 6 de 9 vnodes num nó, balanceado espalha 3/3/3; property do teto rígido
    (rf=1) e de que todo vnode recebe `min(rf,nós)` distintas; determinismo; degradação a HRW com folga.
    Resolve **carga**, não perda de dados (a segurança de rack é o A1).
- **Rebalancing dinâmico**: quando a membership muda (nó entra/sai), redistribuir os vnodes ao vivo.
  Escopo: **control plane** (os membros dos clusters `ra` de cada vnode); adicionar/remover membro
  (`:ra.add_member`/`:ra.remove_member`) faz o **próprio `ra` transferir o estado** (Raft log/snapshot),
  então não movemos dados de metadata à mão; o **data plane** (segments) já é coberto pelo *healing*
  (1C-b). Modelo **manual** *staged → planned → committed* (riak_core; gatilho automático fica para
  depois, por cima do mesmo motor). Decomposto em:
  - ✅ **R1, `desired_placement` (núcleo puro).** `Malachi.Application.desired_placement/5` recomputa o placement
    desejado sobre um conjunto de nós arbitrário (a membership **viva**, vs a config estática `:log_nodes`):
    compõe `sharded_vnodes/2` (vnodes lógicos fixos) + `place_vnodes/4` (HRW). Determinístico e
    **movimento mínimo**: um vnode só muda se **adotar** um nó que entrou ou **detinha** um que saiu; o
    resto fica posto. Testado: determinismo; ao **adicionar** um nó, um vnode só muda se adota o novo nó
    (e algum adota); ao **remover**, só muda quem o detinha (e ele some do placement); clamp a `min(rf, |nós|)`.
  - ✅ **R2: plano de rebalanceamento (núcleo puro).** `Malachi.Application.rebalance_plan/2` faz o diff do
    placement **atual** × `desired_placement` (R1) por vnode: para cada vnode cujo conjunto de nós difere,
    devolve `%{vnode_id:, add:, remove:}` (nós a entrar / a sair do cluster `ra`); vnodes já corretos são
    omitidos (plano vazio = nada a fazer). *Staged/planned*: computa sem aplicar. A ordem segura é
    **add-before-remove** (o R3 adiciona antes de remover, então um vnode nunca cai abaixo do quórum no
    meio da mudança; com RF constante, `add` e `remove` têm o mesmo tamanho). Assume o **mesmo conjunto de
    vnode ids** em atual e desejado (mudar a contagem é re-sharding, fora de escopo). Determinístico
    (segue a ordem do desejado). Testado: vazio quando nada muda; add/remove por vnode alterado (omite os
    iguais); num *join* o RF fica constante (add/remove equilibrados → nunca abaixo do quórum); num
    *leave* só os vnodes que detinham o nó que saiu entram no plano e nenhum re-adiciona o nó removido.
  - **R0. Lease sobre `ra`** (fencing forte, k8s): pré-requisito de R3 (o movimento de vnodes é
    **não-idempotente**: é aqui que o lease finalmente entra, como antecipado no 1C-b).
    - ✅ **R0-a: máquina de estado do lease (núcleo puro + `ra`).** `Malachi.Cluster.Lease` é o estado
      puro (`holder`, `fence`, `renew_at`, `duration_ms`): `acquire_or_renew` concede se **livre**, **já é
      o holder** (renovação) ou **expirado** (`now >= renew_at + duration_ms`), senão `{:error, {:held,
      holder}}`; `release` é idempotente. O **fencing token** (`fence`) é monotônico e sobe **só quando o
      holder muda** (renovação mantém): o holder o carrega para o trabalho que fencia, e uma escrita de
      ex-holder com token obsoleto pode ser rejeitada (a proteção contra dois chefes). O tempo (`now`) é
      **injetado**, nunca lido dentro do `apply` (seria não-determinístico e quebraria o Raft):
      `LeaseMachine` (`:ra_machine`) alimenta o `meta.system_time` do `ra` (relógio do **líder**,
      carimbado uma vez e replicado no log), então um único relógio decide a expiração, sem o skew
      entre nós que um tempo vindo do cliente carregaria. `LeaseServer` espelha o `MetadataServer` sobre
      um cluster `ra` **dedicado** (isolado do metadata). Testado: `Lease` puro exaustivo (aquisição/
      renovação mantém fence/roubo na expiração incrementa fence/fronteira exata do deadline/release
      idempotente com token obsoleto) + integração `ra` real (acquire/renew/held/release/durável a restart).
    - ✅ **R0-b: `LeaseHolder` (o client).** GenServer que roda o triângulo de timers `duração >
      renew_deadline_ms > retry_period_ms`: a cada `retry_period` chama o seam `renew` (acquire-or-renew);
      um *follower* que adquire vira *leader* e chama `on_acquired(fence)`; um *leader* que renova segue
      líder (marca o instante do renew no **relógio local**); se lhe dizem que o lease está com **outro**
      larga na hora (`on_lost`); se **não alcança** o lease, segue tentando até passar `renew_deadline_ms`
      desde o último renew bem-sucedido e então **larga proativo** (`on_lost`, o *OnStoppedLeading* do k8s:
      desiste antes de o lease poder expirar/ser roubado, para nunca haver dois líderes). Um salto do
      **fencing token** durante a liderança (gap: perdeu e reganhou) dispara `on_lost` seguido de
      `on_acquired` sob o novo token. No shutdown normal, um líder **libera** o lease (failover sem
      esperar expiração). Tudo por **seams injetados** (`renew`/`release`/`clock`/callbacks), então a
      lógica de tempo é testada sem `ra`, controlando o relógio. Testado: adquire→líder; renova sem
      re-`on_acquired`; segura até o deadline e larga; largada imediata quando held-por-outro; troca de
      token → lost+acquired; release no shutdown do líder (e não do follower). **R0 completo.**
  - **R3. Execução** (*committed*): aplica o plano por vnode via `ra`, **sob o lease**. Escopo: control
    plane (o `ra` transfere o estado ao adicionar membro); o data plane fica com o *healing*.
    - ✅ **R3-a: executor de uma mudança (núcleo com seams).** `Malachi.Cluster.Rebalance`: `apply_change/3`
      aplica cada `add` **antes** de cada `remove` (add-before-remove, para o vnode nunca cair abaixo do
      quórum) via seams `add_member`/`remove_member` (`(vnode_id, node -> :ok | {:error, _})`); **idempotente**
      (add de quem já é membro / remove de quem já saiu = `:ok`, então um commit interrompido é
      re-executável); **fail-fast** (um `add` que falha **não** tenta os removes, protege o quórum).
      `apply_plan/4` aplica o plano mudança-a-mudança, fail-fast entre vnodes (para no 1º erro, devolve
      `{:error, {aplicados, falha}}`), e **revalida `leader?` antes de cada mudança** (para com
      `:lost_leadership` se o holder soltou o lease no meio). Testado (seams que gravam a ordem): add
      antes de remove; add que falha não remove; erro no remove reportado; idempotência; plano completo;
      fail-fast entre vnodes; parada por perda de liderança.
    - **R3-b, coordenador plan/commit + ops `ra`/wiring.**
      - ✅ **R3-b-i: plano do estado vivo (núcleo com seams).** `Malachi.Application.readable_placement/2` monta o
        placement **atual** a partir das memberships `ra` dos vnodes via o seam `members_of`
        (`vnode_id -> {:ok, nodes} | {:error, _}`), **omitindo** vnodes ilegíveis (conservador: nunca
        planejar um vnode que não conseguimos ver). `Malachi.Application.live_rebalance_plan/5` é o *plan*: faz o
        diff do atual (legível) contra o `place_vnodes` **desejado** sobre os nós **vivos**, só para os
        vnodes legíveis, e devolve o plano (`rebalance_plan/2`) que alimenta `Rebalance.apply_plan/4` (o
        *commit*, sob o lease). Fica junto de R1/R2 no `Application` (evita ciclo com `Rebalance`, que só
        executa). Testado: `readable_placement` omite ilegível; `live_rebalance_plan` = diff atual×desejado
        sobre vivos; vazio quando já casa; nunca planeja vnode ilegível.
      - ✅ **R3-b-ii: ops `ra` reais + coordenador (motor).** `Rebalance.ra_add_member/3` e
        `ra_remove_member/3` são os seams reais do `apply_plan`: **add** = `add_member` (anuncia o membro:
        ele nem precisa estar rodando) **depois** `start_server` no nó via `:erpc` (a ordem que a doc do
        `ra` prescreve; o líder então replica log/snapshot ao novo membro); **remove** = `remove_member`
        depois `stop_server`. **Idempotentes** (`already_member`/`not_member`/`already_started` aninhado →
        `:ok`) e com **retry** em `:cluster_change_not_permitted`: o `ra` só permite **uma** mudança de
        membership por vez, então o `add`-then-`remove` de um mesmo change (e ops repetidas) esperam a
        anterior assentar. `RebalanceCoordinator` (GenServer, seams `plan_fun`/`add_member`/`remove_member`/
        `leader?`) expõe `plan/1` (calcula, não aplica) e `commit/1` (**recusa `:not_leader`** se não for o
        holder do lease; senão `apply_plan` fail-fast, passando o mesmo `leader?` para parar se o lease cair
        no meio). Commit **sempre manual**. Testado: coordenador por seams (plan/commit/recusa/fail-fast) +
        **`:multinode`** real: `ra_add_member` cresce um vnode para um novo nó e o `ra` transfere o estado,
        `ra_remove_member` encolhe, ambos idempotentes.
      - ✅ **R3-b-iii: wiring (\"ligar na tomada\").** Quando **sharded**, `Application.log_children`
        adiciona `rebalance_children`: bootstrapa o `LeaseServer` (cluster `ra` dedicado `Malachi.LogLease`,
        **auto-fencido** no boot: todo nó chama, um forma) e sobe o `LeaseHolder` (`Malachi.LogLeaseHolder`,
        triângulo default 15s/10s/2s via `lease_duration_ms`/`lease_renew_deadline_ms`/`lease_retry_period_ms`,
        `renew`/`release` reais sobre o `LeaseServer`) e o `RebalanceCoordinator` (`Malachi.LogRebalanceCoordinator`)
        com os seams reais: `plan_fun`=`live_rebalance_plan`, `add_member`/`remove_member`=`Rebalance.ra_*`
        resolvendo os membros via `try_members/2` (tenta cada nó como ponto de entrada: o holder pode não
        hospedar o vnode; qualquer membro roteia ao líder), `leader?`=`LeaseHolder.leader?`. Adicionei
        `LeaseHolder.leader?/1` (lê o papel sem forçar tick). O **commit segue manual** (o operador chama
        `RebalanceCoordinator.plan/1`/`commit/1`); o `LeaseHolder` só mantém a eleição rodando (k8s). O
        caminho **não-sharded é inalterado**. Testado: `leader?/1` e `try_members/2` isolados + suíte
        completa (1048 testes) verde = boot não regride; comportamento das ops apoiado no `:multinode` de
        R3-b-ii. **Rebalancing dinâmico completo: a Fase 1 (distribuição) fecha aqui.**
      - ✅ **Reconcile do lease (endurecimento).** O bootstrap `auto-fencido` acima forma o cluster do
        lease só com a **maioria** (Raft); um nó que estava down quando o cluster formou fica membro da
        **config** (o `start_cluster` inicial lista todos os nós) mas sem servidor rodando, reduzindo a
        tolerância a falha do lease. `LeaseServer.reconcile/2` faz **self-join**, best-effort e idempotente:
        (re)tenta formar o cluster (`start/2`, auto-fencido) e iniciar o servidor **local** (`:ra.start_server`),
        que se re-junta ao cluster existente (já é membro da config) e o `ra` replica o estado do lease a
        ele. `Malachi.Cluster.LeaseReconciler` (GenServer genérico, seam `:reconcile`) o chama após o boot
        e a cada `lease_reconcile_interval_ms` (default 30s), *level-triggered*, mantém o `LeaseHolder`
        livre de `ra`/membership. Subido no `rebalance_children` (primeiro, antes do holder). Testado:
        reconcile bootstrapa quando não iniciado + é no-op idempotente num cluster formado (não perturba o
        lease); o reconciler reconcilia no boot e sob demanda.
    - ✅ **Descoberta dinâmica de nós (libcluster). Connectivity-only.** Fecha a lacuna de operabilidade: a
      descoberta de peers era **estática** (`MALACHIMQ_LOG_NODES` + `Node.connect` manual / hostnames fixos).
      Dep `{:libcluster, "~> 3.5"}` + um `Cluster.Supervisor` **opcional** na árvore (só quando
      `MALACHIMQ_CLUSTER_STRATEGY` está setado; ausente = single-node, sem exigir distribuição, default
      intacto). Decisão (**1A**): **connectivity-only**, libcluster só descobre+conecta nós (distribuição
      Erlang); SWIM e o `ra` seguem usando o `log_nodes` para o *member set* inicial, e a mudança de
      membership do `ra` continua pelo **R3 (rebalancing sob lease)** já feito, não duplica, de forma menos
      segura, a formação Raft. Estratégias (**2A**): `gossip` (UDP multicast, dev/LAN), `kubernetes`
      (descobre pods via API; `selector`+`node_basename` obrigatórios), `epmd` (lista estática reusando
      `log_nodes`). Módulo **puro** `Malachi.Cluster.Topology.build/1` mapeia config→topologies (fail-fast:
      raise em campo obrigatório faltante), unit-testável sem abrir socket multicast/k8s. Env parseado no
      `runtime.exs` (`log_nodes` extraído p/ binding, reusado no `epmd`). Testado: `build/1` por estratégia
      (defaults, campos obrigatórios, nils omitidos, unknown raise): 12 testes; smoke de boot real (default
      → sem `ClusterSupervisor` e sem distribuição; `gossip` + `--sname` → `ClusterSupervisor` vivo). Suíte
      completa 715 testes 0 falhas (boot não regride); credo/dialyzer limpos. README com a seção de node
      discovery. **Fatia de operabilidade multi-nó fechada.**
    - ✅ **Hardening de placement: garantia de domínios de falha (`min_domains`/`policy`).** O `Placement`
      já fazia spread rack/DC + `max_skew`, mas o `:spread` é **best-effort**: com menos domínios que `rf`,
      ou atributos faltando, as réplicas concentravam **silenciosamente**: um furo de HA (3 réplicas no
      mesmo rack sobrevivem a zero falhas de rack). Decisão **1A**: `:hard` **falha rápido na colocação
      inicial**; heal segue best-effort (durabilidade primeiro) + reporta. **Core (puro)**: `place/4` ganha
      `:min_domains` (nº mínimo de valores distintos do atributo que o replica set deve cobrir; sem
      `:spread`, brokers distintos) + `:policy` (`:soft` default = comportamento atual; `:hard` retorna
      `{:error, {:insufficient_domains, coberto, exigido}}`). Broker sem atributo cai no domínio `nil` único
      (conservador: não conta como domínio extra). Novo `domain_violations/4` reporta segmentos cujo replica
      set cobre < `min_domains` domínios (alerta/observabilidade). **Fiação**: broker (`min_domains`/
      `placement_policy` no struct/open; `place_opts` injeta; `open_segment` trata `{:error, ...}` → produce
      aborta limpo, `register_segment` extraído), broker_server (threading), application (`data_plane_opts`
      lê `log_min_domains`/`log_placement_policy`), config (`MALACHIMQ_LOG_MIN_DOMAINS`/
      `MALACHIMQ_LOG_PLACEMENT_POLICY`). **Fix relacionado (Issue 2)**: o heal era **rack-blind**:
      `self_healing` chamava `place/3` sem `:spread`; agora `HealCoordinator` resolve o spread por pass (via
      `heal_spread/0`, atributos vivos) e o `self_healing` forwarda **só `:spread`** (strip de
      `min_domains`/`policy`. Heal nunca hard-falha). Testado: `place/4` min_domains/policy (soft/hard,
      met/unmet, sem-spread, nil-domain) + `domain_violations/4` (5+3); broker hard-fail e2e (produce aborta
      com 2 racks/min_domains 3; soft coloca; hard passa com min_domains 2, 3); heal rack-aware forwardando
      `:spread` (1). Suíte 727 testes 0 falhas; credo/dialyzer limpos. README com os env vars.
      - ✅ **Surfacing de `domain_violations` (métrica + painel).** Fecha o loop da metade "reportar" do 1A: o
        `domain_violations/4` era uma função pura que **nada chamava**, então com política **soft** o operador
        ficava cego para a degradação de HA. `Broker.domain_violations/1` (puro) computa, do próprio broker
        (merged metadata + `spread_by` + `broker_attributes` vivos + `min_domains`), as violações **por topic**
        (`%{topic => count}` via `Enum.frequencies_by(&topic_of_segment/1)`; `%{}` se spread/min_domains não
        configurados); `BrokerServer.domain_violations/1` expõe via call. O `dashboard` anexa o count a cada
        topic no `topics_overview` (default 0), o `Prometheus.export` emite o gauge por-topic
        `malachi_domain_violations` (`Map.get(.., 0)` defensivo), e o painel mostra um badge `⚠ N HA` **só
        quando > 0** (alto sinal, sem clutter). Testado: `Broker.domain_violations` (soft abaixo do alvo → 1;
        no alvo → vazio; não configurado → vazio) + gauge do Prometheus (emite por-topic, default 0 na
        ausência da chave): 4. Suíte 731 testes 0 falhas; credo/dialyzer limpos; badge JS validado. README
        com o gauge.
    - ✅ **Exemplo de deploy Kubernetes (amarra libcluster + placement num deploy real).** `deploy/kubernetes/`:
      um manifest (`malachi.yaml`, 8 docs) de um **cluster CP de 3 nós** com placement rack (zona) aware,
      + README explicando o racional. Decisões: **1A** descoberta por **epmd (lista estática de FQDNs)**:
      um StatefulSet CP/Raft tem identidades **estáveis** (idiomático, e os node names casam com o
      `RELEASE_NODE`: determinístico, o que importa já que não há cluster real p/ testar); **2A** rack-aware
      **de verdade** via init container. Peças: **StatefulSet** (identidade/DNS/PV estáveis p/ o `ra`;
      `podManagementPolicy: Parallel` forma o quórum junto), **headless Service** (`publishNotReadyAddresses`
      p/ os peers se resolverem durante a formação), **client Service** (4040/4041), **PDB** `minAvailable: 2`
      (maioria Raft em drains), **ClusterRole** least-privilege (`get nodes`) + SA/binding p/ o init. Node name
      distribuído via `RELEASE_NODE=malachi@$(POD_NAME).malachi-headless.$(POD_NAMESPACE)...` +
      `RELEASE_DISTRIBUTION=name` + `ERL_AFLAGS` fixando a porta de dist; peer set do `ra` = os 3 FQDNs em
      `MALACHIMQ_LOG_NODES`; `MALACHIMQ_CLUSTER_STRATEGY=epmd` reusa a lista. Rack-awareness:
      `topologySpreadConstraints` por zona + init container (`kubectl get node`) escreve a zona num emptyDir
      que o main container dobra em `MALACHIMQ_LOG_ATTRIBUTES=zone=<z>`, com `LOG_SPREAD_BY=zone`/
      `MIN_DOMAINS=2`/`PLACEMENT_POLICY=soft` (violações viram o gauge do slice anterior; nó sem label →
      fallback informativo). Probes `/health`+`/ready` (do O1). Sem mudança de código Elixir (node name via
      `RELEASE_*`; o `vm.args` já usa `inet_res`). Validado: YAML parseia (8 docs) + spot-check dos valores
      críticos (ordem do env com POD_NAME antes do RELEASE_NODE, refs `$(VAR)`, command colapsado). Doc da
      alternativa `kubernetes` (dinâmica, RBAC em pods) p/ deploys autoescaláveis. README principal aponta.
      (Não testável sem um cluster k8s real, config determinística por construção.)
    - ✅ **TLS na distribuição Erlang inter-nó (G3).** Fecha um gap de segurança de produção: metadata
      (`ra`) + replicação de dados trafegavam em **texto puro** entre nós (só o cookie autenticava).
      Decisão **1A**: **TLS mútuo** (`verify_peer` + `fail_if_no_peer_cert`), CA compartilhada, cert por nó,
      cifra **e** autentica. Config de VM/release (não código Elixir): `rel/env.sh.eex` traduz
      `MALACHIMQ_DIST_TLS=true` em `-proto_dist inet_tls -ssl_dist_optfile $MALACHIMQ_DIST_TLS_OPTFILE`
      (via `ELIXIR_ERL_OPTIONS`), **fail-fast** se o optfile faltar/for ilegível; default off = texto puro
      atual intacto. Artefatos: `rel/dist_tls.conf.example` (template do ssl_dist optfile), helper de dev
      `scripts/generate-dist-certs.sh` (CA + cert de nó com EKU server/clientAuth + emite um optfile pronto),
      `.gitignore` de `priv/dist_cert/` (nunca commitar chaves). Fiado no exemplo k8s (Secret `malachi-dist-tls`
      com ca/node cert+key+optfile, volume readOnly em `/etc/malachi/dist`, 2 env). **Validado localmente de
      fato** (≠ k8s): 2 nós BEAM sobre TLS dist se pingam (`:pong`), e um nó **sem** TLS é **rejeitado** no
      handshake (`:pang`: prova que a TLS é imposta, não silenciosamente plaintext); os 3 caminhos do
      `env.sh.eex` (off/on/fail-fast); optfile é term Erlang válido (`:file.consult`); YAML k8s parseia (9
      docs). Sem mudança de código Elixir; README (seção inter-node TLS) + deploy README.
    - ✅ **Shutdown gracioso / rolling-upgrade (G4).** O `prep_stop` antigo **fechava tudo de imediato**
      (sem quiesce nem janela → in-flight cortado, e race com accepts novos durante o fechamento). Decisão
      **1A** (janela limitada: o modelo certo para um broker com **streaming**, onde drenar-até-0-conexões
      nunca converge). Novo `Malachi.Shutdown.graceful/1` orquestra 3 passos: **quiesce** (`terminate_child`
      do `TCPAcceptorPool` no root supervisor: para de aceitar e **não** reinicia; as conexões, que são
      `spawn` unlinked registradas no `ConnectionRegistry`, sobrevivem) → **drain** (sleep
      `shutdown_grace_ms`, default 5s, janela para in-flight terminar) → **close** (`close_all`). O lease já
      é liberado pelo LeaseHolder.terminate na teardown seguinte (failover rápido) e o `ra` persiste em
      disco (o pod volta e re-join como o mesmo membro). Passos são **seams** → a orquestração
      (ordem + janela) é unit-testável sem parar o app real. k8s: `terminationGracePeriodSeconds: 40` +
      `preStop` (`sleep 5`: kube-proxy tira o pod dos endpoints do Service **antes** do SIGTERM, então
      clientes param de ser roteados antes do drain). Config `MALACHIMQ_SHUTDOWN_GRACE_MS`. Testado:
      `graceful/1` roda quiesce→sleep(drain_ms)→close **em ordem**; pula o sleep com `drain_ms: 0`;
      default vem do config: 3. Suíte verde; credo/dialyzer limpos. README (env var) + k8s (grace/preStop).
    - ✅ **Consumer group coordination (G1: épico, fatiado; concluído S1–S5 + Str-1/Str-2).** Antes um grupo era
      uma **posição única compartilhada** (todos os consumidores liam a mesma posição commitada, sem paralelismo).
      Alvo NorthGuard/Kafka **atingido**: cada **range** do topic atribuída a **exatamente um** membro do grupo,
      consumo paralelo, com rebalance no join/leave: **tudo server-internal e opaco** (o cliente nunca vê ranges).
      Achado que aterrou o design: o `commit_offset` fazia `Map.put` (substituía o mapa de offsets do
      `{group, topic}`) → virou **merge por-range** (S2). Fatiamento: **S1** núcleo de assignment (puro) · **S2**
      commit por-range · **S3** coordinator (membership + heartbeat/session + expõe assignment) · **S4** integração
      no servidor (fetch respeita a assignment, opaco) · **S5** protocolo wire + cliente · **Str-1/Str-2**
      member-scoping do streaming (push server-side + wire/cliente com heartbeat). **Escopo restante (fatia própria,
      não-G1): o coordinator é hoje um GenServer local único: o roteamento/replicação multi-nó da membership fica
      para uma fatia de wiring de cluster.**
      - ✅ **S1, núcleo de assignment (puro).** `Malachi.Consumer.Assignment.assign(range_ids, members)`
        → `%{member => [range_id]}`, cada range sob **exatamente um** membro, **determinístico** (ranges
        ordenadas em ordem canônica → um coordinator replicado/failover computa o mesmo em todo nó). Decisão
        **1A (HRW sticky)**, mas **corrigida por medição empírica** (o memory de medir antes de decidir): a
        opção dizia "reusa `place_balanced`", porém o property test revelou que ele **não é fortemente
        sticky** (N pequeno: o rebalance-pro-cap move ranges de sobreviventes, 4 ranges/4 membros, remover
        1 moveu 2 de 3 sobreviventes), contrariando a prioridade *sticky*. Troquei para **HRW puro**
        (`Placement.place(range, members, 1)` por range → o membro top-HRW), que dá **min-reshuffle
        estrito**: um leave move **só** as ranges do que saiu (sobreviventes mantêm **todas**), um join move
        ranges **só** para o novo membro (existentes só perdem, nunca trocam entre si): a stickiness que
        "HRW sticky" promete, com balanço **estatístico** (ótimo com muitas ranges, o caso NorthGuard). Ainda
        reusa `Placement.place` (o ranking HRW). Testado (property): partição (cada range 1×), determinismo
        sob shuffle, **sticky-on-leave** (sobreviventes mantêm tudo), **sticky-on-join** (inalterado ou o
        novo) + edges (sem membros → `%{}`, sem ranges → membros idle, dedup). Suíte 746 testes 0 falhas;
        credo/dialyzer limpos.
      - ✅ **S2. Commit por-range (merge).** O `Metadata.apply({:commit_offset, group, topic, offsets})`
        deixava de fazer `Map.put` (substituía o mapa inteiro de offsets do `{group, topic}`) e passa a
        **`Map.update` + `Map.merge`**: mescla os offsets recebidos por-range (last-commit-wins por range).
        Assim um membro de um grupo particionado commita **só as ranges que possui** sem apagar as posições
        das ranges de outros membros. O pré-requisito que o S1 apontou. Backward-compatible: um consumidor
        único que commita o mapa completo funciona igual (merge cobre tudo), e os testes existentes (que
        commitam uma range ou a mesma 2×) passam sem mudança. Caminho único (o `DSRSM` roteia por topic pro
        `Metadata.apply`; `merged_metadata` une topics disjuntos). Tradeoff registrado: o merge deixa keys
        **stale** quando uma range faz split/merge (o offset da range antiga persiste), mas é **bounded**
        pelo keyspace (máx ~2^keyspace_bits range_ids históricos) e **inócuo** na leitura (o fetch só consome
        ranges atuais; ranges mortas são ignoradas); um prune (reusando o índice `topic_ranges`) fica como
        otimização futura, não S2. Testado: novo teste de merge (dois membros, um commita só sua range → a do
        outro é preservada) + os existentes (last-wins por range). Suíte 747 testes 0 falhas; credo/dialyzer
        limpos. **Próximo: S3 (coordinator: membership + heartbeat + expõe a assignment do S1).**
      - ✅ **S3: coordinator de grupo (membership + heartbeat + assignment).** `Malachi.Consumer.GroupCoordinator`
        (GenServer) rastreia os membros por `{group, topic}` e atribui as ranges do topic via S1. API: `join`
        (adiciona membro → rebalance → devolve `{:ok, generation, ranges}`), `heartbeat` (renova a sessão,
        devolve a assignment atual + generation, ou `{:error, :unknown_member}` se foi evictado → re-join),
        `leave`, `assignment` (leitura sem renovar), `reconcile_now` (roda um tick sync, seam de teste).
        **Eager (decisão 1A)**: qualquer mudança de membership (join/leave/eviction) ou de ranges recomputa a
        assignment inteira e **bump da `generation`** (epoch à la Kafka): o membro re-lê no heartbeat e vê a
        generation nova = reassumir; **level-triggered** (só bumpa se a assignment mudou de fato, então o tick
        é idempotente). Session-timeout: um membro silencioso por `session_ms` é **evictado** no reconcile
        (tick periódico), suas ranges reatribuídas; grupo sem membros é **dropado** (sem leak de estado).
        Só a **lógica** do coordinator, como instância única, com seams (`clock`/`ranges_fun`) → testável sem
        cluster; o **roteamento no cluster** (qual nó coordena qual grupo, Kafka hasheia group→broker, ou
        replicar a membership) fica para a fatia de wiring. Estado de membro é **soft** (restart → membros
        re-join). Testado: membro sozinho pega tudo (gen 1); dois membros particionam (disjunto/completo, gen
        avança); leave devolve as ranges; **eviction** por session-timeout + re-join obrigatório; grupo vazio
        dropado; mudança de ranges rebalanceia no reconcile; reconcile idempotente (sem mudança → mesma gen);
        heartbeat de membro desconhecido rejeitado, 8. credo/dialyzer limpos.
      - ⚠️ **Correção de rumo (fidelidade NorthGuard) antes do S4.** Revisão apontou que S1–S3 importaram o
        **modelo Kafka** (assignment de `range_ids` **visível ao cliente**). Isso **violaria** o princípio
        central do projeto (doc §B: *"contrato de cliente = jeito NorthGuard, NÃO Kafka: … cursor opaco …
        nunca vê partition/offset"*). Ranges são o equivalente NorthGuard de partitions → têm de ficar
        **escondidas**. O maquinário S1–S3 é **server-side e correto** (o coordinator computa a assignment
        internamente); o `[range_ids]` que ele devolve é **detalhe interno**, não vai ao wire. Plano de S4/S5
        **reajustado para opaco**: o servidor escopa o fetch à assignment do membro e devolve records +
        **cursor opaco**; o cliente **nunca** vê range_id. Membership **implícita via fetch** (o fetch é o
        heartbeat) + `leave` explícito depois.
      - ✅ **S4: consumo particionado server-side (opaco, in-VM).** `GroupCoordinator.poll/4` é o entry-point
        do fetch: registra o membro se novo (rebalance) ou só renova a sessão se conhecido (sem rebalance) e
        devolve as ranges dele, então o membro fica vivo **buscando**, sem heartbeat separado. O caminho do
        `consume` ganhou um filtro **`ranges`** (`consume_ranges/5` + `selected_ranges/3`): `nil` = todas as
        ranges ativas (grupo inteiro / consumidor único, comportamento atual); uma lista = **só** essas,
        **interseccionadas com as ativas** (uma range atribuída que já fez split é pulada). Threadado por
        `BrokerServer.consume/6` + o `handle_call({:consume})` (6-tupla) + o waiter do long-poll (guarda
        `ranges`): subscriber de streaming inalterado (usa o default `nil`). Novo `LogApi.fetch_member/7`:
        `poll` no coordinator → as ranges do membro → consume escopado das posições commitadas (S2), retorno
        = records + **cursor opaco** (o cliente nunca vê range_id; o `commit` avança só as ranges do membro).
        Backward-compat: `fetch_group` sem membro = grupo inteiro. Testado: `poll` (registra novo/heartbeat
        conhecido/re-registra evictado: 2); **integração e2e in-VM** (topic com 2 ranges via split, 2 membros
        pré-registrados buscam **disjunto e completo**: cada record por exatamente um membro; backward-compat
        do fetch_group): 2. Suíte 759 testes 0 falhas; credo/dialyzer limpos.
      - ✅ **S5: wire + cliente (opaco). Consumer group coordination completo.** Expõe o consumo
        particionado do S4 sobre o protocolo binário **sem vazar range_id**. **Wire**: `fetch_req` ganha um
        **member id** (`put_str`, após o group; nil = grupo inteiro / consumidor único, backward-compat):
        precedência member (grupo, escopado) > cursor (paging do cliente) > group (resume); nova op
        `leave_group` (api_key 7, `topic/group/member`, ack vazio). O `tcp_protocol` despacha `fetch` com
        member → `LogApi.fetch_member` (retorno = `encode_fetch_resp`, records + **cursor opaco**, idêntico ao
        fetch normal: zero range_id no wire) e trata `leave_group` → `GroupCoordinator.leave`. **Wiring**: o
        `GroupCoordinator` sobe na árvore (`Malachi.LogGroupCoordinator`, `ranges_fun` = `active_range_ids` do
        `LogBroker`). **Cliente Node**: `wire.js`/`client.js` (member no `fetch` + `leaveGroup`), `consumer.js`
        modo **`--member`** (fetch escopado + commit + `leave` no exit; vários membros do mesmo `--group` com
        `--member` distintos = consumo paralelo). Achado do dialyzer: o `@type api_key :: 0..6` fazia inferir
        que o branch `leave_group` (7) era morto → atualizado p/ `0..7`. Testado: wire round-trip (member +
        leave_group), **e2e via TCP** (member fetch server-scoped + cursor opaco + records sem offset +
        leave_group ack) + suíte binária existente (backward-compat do fetch sem member); smoke Node real
        (produce → consumer `--member` consome tudo como membro único + `leave`; consumer sem member =
        backward-compat). Suíte 761 testes 0 falhas; credo/dialyzer/format limpos. README (tabela api_key +
        exemplo de membros paralelos). **G1 (consumer group coordination) concluído** (S1–S5); pendências
        anotadas: member-scoping do **streaming** (push) e prune de offsets stale (fatias futuras).
      - ✅ **Prune de offsets stale (dívida do S2).** O merge por-range do S2 deixava uma key **morta** por
        split (o offset da range-pai persistia no mapa do grupo). O `apply({:commit_offset, ...})` agora,
        após o merge, **pruna** os offsets para as ranges **ativas** do topic (`prune_offsets/3` +
        `active_range_id_set/2`, reusando o índice `topic_ranges` do V-idx + o filtro `state == :active`):
        uma range retirada por split/merge tem o offset **descartado**, é seguro porque os filhos ativos
        resumem de `:start` (o consume só lê ranges ativas; a semântica at-least-once de cross-epoch não
        muda, só some a key morta). **Pulado quando o topic não está roteado** (offset commitado antes do
        `create_topic`: `topic_ranges` sem a entrada → mantém como está), preservando o comportamento
        pré-routing dos testes existentes. Bounda o mapa de offsets ao nº de ranges **ativas** (antes crescia
        ~2^keyspace_bits com splits). Testado: split sela o root → o offset do root é prunado no commit
        seguinte (fica só o do filho); os testes de merge/pre-routing do S2 seguem verdes (sem topic → sem
        prune). Suíte 762 testes 0 falhas; credo/dialyzer/format limpos.
      - ✅ **Streaming member-scoping: Str-1 (server-side, opaco).** Leva o consumo paralelo por grupo ao
        push/subscribe (antes whole-group). **Restrição arquitetural** que ditou o design: o
        `push_subscriber` roda **dentro** do handle_call do broker, mas o `ranges_fun` do coordinator
        **chama de volta** o broker (`active_range_ids`) → se o broker chamasse o coordinator sincronamente,
        deadlock (cada GenServer esperando o outro). Regra: **o broker nunca chama o coordinator.** Design:
        toda coordenação no **`LogApi`**: `subscribe_member/7` faz `poll` (registra + ranges) e passa
        `member`/`ranges`/`coordinator` ao `BrokerServer.subscribe` (via `group_opts`); o subscriber
        **armazena** as ranges e o `push_subscriber` escopa o consume com elas (`consume_ranges/5`);
        `stream_ack_member/7` re-poll (heartbeat + ranges frescas) → o broker **atualiza** as ranges do
        subscriber no ack (pega rebalance). **Liveness**: o `:DOWN` do broker dispara um **`Task` assíncrono**
        que chama `coordinator.leave` (async → não bloqueia o broker → sem deadlock) para rebalance rápido na
        desconexão; membro idle fica vivo por ack periódico do cliente (Str-2). Positions escopadas por
        `Map.take` no subscribe (como no `fetch_member`); **opaco** (o push segue `{:log_records, records,
        cursor}`. Zero range_id). Testado in-VM: 2 membros pré-registrados recebem push **disjunto e
        completo** (topic com 2 ranges via split); processo de um membro morrendo → **leave** async → o
        membro some do coordinator. Suíte 764 testes 0 falhas; credo/dialyzer/format limpos. **Próximo: Str-2
        (wire: member no subscribe/stream_ack + cliente Node subscriber com member + heartbeat).**
      - ✅ **Streaming member-scoping. Str-2 (wire + cliente Node).** Expõe o Str-1 na borda: o `member`
        (opcional) entra no **subscribe** e no **stream_ack** do protocolo binário, depois do `group`, como
        no `fetch` (Str-1): `encode_subscribe_req(topic, group, member, window, max)` e
        `encode_stream_ack_req(topic, group, member, cursor, count)` (`put_str(member)` = flag de presença;
        `nil` = subscription whole-group, sem quebrar o caminho antigo). O `TCPProtocol` despacha por
        presença: `subscribe`/`process_stream_frame` chamam `LogApi.subscribe_member`/`stream_ack_member`
        quando `member != nil and group != nil`, senão o caminho whole-group, **zero range/offset no fio**
        (o push segue records + cursor opaco). Cliente Node: `subscriber.js --member <m>` abre um stream
        escopado, e: fechando o **gap de liveness do membro idle** anotado no Str-1 - um **heartbeat
        periódico** (`setInterval` a 10s < os 30s de session timeout) emite um **ack vazio** (`cursor` nil,
        `count` 0) só quando não houve ack real recente (`lastAck`), mantendo a membership viva; o `SIGINT`
        faz `leaveGroup` (rebalance rápido). `streamAck` ganhou o `member` na assinatura (callers
        whole-group. `loadtest.js` - passam `null`). Testes: round-trip de wire para subscribe/stream_ack
        com/sem member; e2e TCP (`log_streaming_test`): subscribe como membro único recebe o backlog
        inteiro **opaco** (offset nil), um member ack (commit + heartbeat + credit) é aceito e um produce
        posterior ainda faz push. Suíte 767 testes 0 falhas; credo/dialyzer/format limpos. **G1 (consumer
        groups) + streaming member-scoping concluídos.**
    - ✅ **Coordinator cluster wiring (épico CONCLUÍDO: consumer groups corretos multi-nó; A1–A5 fecham, ver
      abaixo).** Gap que o G1 deixou
      explícito: o `GroupCoordinator` é um GenServer **local por nó** (`Malachi.LogGroupCoordinator`), com
      membership em memória. Num cluster, membros conectados a nós diferentes veem assignments **divergentes**
      → a invariante "cada range sob exatamente um membro" quebra entre nós. Alvo: rotear a coordenação de um
      topic a **um** nó dono, como o NorthGuard roteia requests (broker consulta sua visão local da metadata
      shardada. O `HashRing` sobre vnodes - e encaminha ao vnode dono). Fatiamento: **A1** roteamento +
      encaminhamento · **A2** coordinator no líder do vnode · **A3** teste multi-nó.
      - ✅ **A1: roteamento do coordinator ao nó dono do vnode + encaminhamento.** Novo módulo **puro**
        `Malachi.Consumer.CoordinatorRouter`: `location(topic, topology, this_node, leader_fn)` roteia
        `topic → vnode` (via `HashRing`), resolve o **líder** do vnode e decide `:local | {:remote, node}`;
        `ref/2` vira o ref de `GenServer` (`{name, node}` se remoto). Roteia por **topic** (co-loca a
        coordenação com o vnode/metadata do topic, onde o `ranges_fun`/`active_range_ids` do coordinator
        resolve contra o broker local). **Fail-safe para `:local`** em toda lacuna de resolução: sem topologia
        (single-node/in-memory), ring vazio, vnode ausente do mapa, ou líder não-resolvível, verificado que
        `:ra.members` num server inexistente **retorna `{:error, :noproc}`** (não levanta), então um vnode
        momentaneamente indisponível degrada a local em vez de derrubar o request. Topologia estática
        (ring + vnode→server_id) publicada **1× no boot** do control plane shardado (`with_metadata_authority`)
        via `:persistent_term` (read lock-free; ausente = single-node → `nil` → local). O `tcp_protocol`
        resolve o ref do coordinator **por request** (`coordinator_for/1`) nos 4 sites (subscribe/fetch/
        stream_ack/leave) e o passa ao `LogApi`; o ref resolvido também vira o `sub.coordinator` (o `leave`
        async do `:DOWN` encaminha ao dono). **Single-node inalterado** (resolve → `:persistent_term` miss →
        nome local). Testado (puro, 9): topologia nil/ring vazio/vnode ausente/líder nil → local; este-nó →
        local; outro-nó → `{:remote}`; `ref/2`; round-trip do `put_topology`/`topology`. Suíte 776 testes 0
        falhas; credo/dialyzer/format limpos. **Limitação conhecida (resolvida no A2):** o `sub.coordinator` era
        o ref resolvido **no subscribe** e não era atualizado nos acks → numa troca de liderança o `leave` do
        `:DOWN` iria ao líder antigo. **Próximo: A2 (consistência de failover) → A3 (teste `:multinode`).**
      - ✅ **A2 (parte A). Consistência de failover: refresh do coordinator no ack + guard de ownership.**
        Fecha a limitação do A1 e endurece a janela de failover, com **membership soft** e **coordinator = líder
        do vnode**: ambos **confirmados pela transcrição do NorthGuard no repo** (`northguard_meetup_transcript.txt`:
        *"this coordinator is the leader of a given VNode... manages all the metadata owned by VNode"*; o Conductor
        do Xinfra faz client-management por conexão/heartbeat, só offsets/checkpoints são duráveis). Decisão
        **A2-A** (foco em correção; o lifecycle "rodar só no líder via `VnodeCoordinatorManager"` entra no A3, junto
        do teste multinode que o exercita: comportamento observável é idêntico, então B é fidelidade de detalhe
        interno só testável multi-nó). **Parte 1, refresh:** `BrokerServer.stream_ack/7` ganha o param
        `coordinator`; o `handle_call` atualiza `sub.coordinator` (além de `sub.ranges`), então após uma troca de
        líder o `stream_ack_member` (que já re-resolve o líder fresco no `tcp_protocol`) grava o ref novo e o
        `leave` async do `:DOWN` acerta o **dono atual**. **Parte 2, guard:** o `GroupCoordinator` ganha o seam
        `owns_fun` (default `fn _ -> true end`; no boot, `CoordinatorRouter.owns?/1`); `join`/`poll` rejeitam com
        `{:error, :not_owner}` **sem** registrar quando o nó não lidera o topic (defende contra roteamento stale na
        janela de failover: sem assignment fantasma). O `LogApi` (subscribe/fetch/stream_ack member) propaga o
        `:not_owner` e o `tcp_protocol.subscribe` responde erro em vez de entrar em stream (cliente re-resolve e
        re-subscreve); heartbeat/fetch **auto-curam** no próximo request (roteamento é por-request). Single-node:
        `owns?` é sempre `:local` → nunca rejeita → inalterado. Testado: guard (poll/join → `:not_owner`; sem
        registro fantasma; owns_fun por-topic. 3) + refresh (ack via coordinator diferente → `:DOWN` leave acerta
        o novo: 1). Suíte 780 testes 0 falhas; credo/dialyzer/format limpos. **Próximo: A3 (validação `:multinode`).**
      - ✅ **A3. Validação `:multinode` do roteamento (A1+A2 contra `ra` real).** Prova a máquina do A1/A2 entre nós
        BEAM reais (harness `:peer` + `:erpc`, como o `rebalance_multinode_test`): sobe 3 peers, forma o cluster
        `ra` de um vnode (quorum 2, tolera 1 falha), publica a topologia (`put_topology`) em cada nó, e verifica
        contra a **liderança `ra` viva**: (1) `owns?`/`resolve` concordam: no líder `owns? == true` e `resolve`
        devolve o nome local, em cada follower `owns? == false` e `resolve` devolve `{name, líder}`; (2)
        **forwarding cross-node**: um coordinator no líder recebe dois membros (poll a partir do nó primário via
        `{name, líder}`) e a assignment é **disjunta e completa**; (3) **guard**: um follower rejeita `poll` com
        `{:error, :not_owner}`; (4) **failover**: mata o server `ra` do líder (`:ra.stop_server`) → os 2 restantes
        elegem um novo líder → `owns?`/`resolve` **reconvergem** nele. Detalhes que aterraram o teste (registrados
        p/ o A4): o `server_id` da topologia aponta um **probe** (follower que nunca morre) para o `:ra.members`
        resolver o líder mesmo após o failover; o coordinator no peer sobe via `GenServer.start` **unlinked** (o
        worker do `:erpc` morre e levaria junto um filho linkado); o `ranges_fun` é uma **captura de módulo em
        `test/support`** (`&Fixtures.ranges/1`): fun anônima do `_test.exs` não é resolvível no peer (não está no
        code path / MD5). `@moduletag :multinode` (excluído por default; roda com `--include multinode`). Suíte 780
        testes 0 falhas (+1 multinode excluído); credo/dialyzer/format limpos. **G1/coordinator: A1+A2+A3 fecham a
        correção multi-nó dos consumer groups. Próximo: A4 (lifecycle, coordinator por-vnode no líder via
        `VnodeCoordinatorManager`, re-validado por este teste; + retry do cliente Node no `:not_owner`).**
      - ✅ **A4: coordinator por-vnode no líder (lifecycle, o modelo NorthGuard "coordinator = líder do vnode").**
        Antes (A1–A3) **todo nó** rodava um `GroupCoordinator` único e o roteamento mandava o cliente ao dono;
        agora, no control plane **shardado**, cada vnode roda o **seu** coordinator **no líder**, gerido pelo
        `VnodeCoordinatorManager` que já sobe heal/retention por-vnode, start/stop no gate de liderança
        (`MetadataServer.leader?`). Ganhos sobre A1–A3: fidelidade, **isolamento de crash** (um vnode não derruba
        os outros) e **handoff de failover mais limpo** (o manager para no líder velho e sobe fresco no novo).
        **Naming**: o coordinator por-vnode registra sob `CoordinatorRouter.coordinator_name(base, vnode_id)`
        (`Module.concat`, nome local por-vnode. Não `:global`, consistente com o roteamento explícito de
        metadata do sistema); o `resolve/2` passa a **derivar** esse nome do vnode roteado (single-node sem
        topologia → nome base). Refactor: `route/2` extraído (DRY entre `location`/`resolve`); o
        `Malachi.LogGroupCoordinator` único vira **condicional** (só non-sharded, em `coordinator_children`);
        `group_coordinator_vnode_child/1` entra no `start_vnode_coordinators/1` (id no supervisor por-vnode,
        `owns_fun` como defesa na janela de flap). O `tcp_protocol` é inalterado (segue passando o nome base ao
        `resolve`). **Single-node inalterado** (boot smoke: coordinator base registrado, `resolve` sem topologia
        → base). O teste `:multinode` do A3 foi **re-validado** com os nomes por-vnode (`coordinator_name`).
        Testado: `coordinator_name/2` puro; multinode 2x verde; boot single-node. Suíte 781 testes 0 falhas
        (+1 `coordinator_name`); credo/dialyzer/format limpos. **Próximo: A5 (retry do cliente Node no
        `:not_owner`: resiliência de cliente na janela de failover).**
      - ✅ **A5. Resiliência do cliente Node no `:not_owner` (janela de failover).** Fecha o item de cliente do
        A4: quando um request de membro é encaminhado a um coordinator que acabou de perder a liderança do vnode,
        o servidor responde `:not_owner` (guard do A2); é **transitório** (o servidor re-resolve o líder atual no
        próximo request), então o cliente deve **retry** em vez de falhar. Node-only (nenhum Elixir): `client.js`
        exporta `isNotOwner(err)` (um `MalachiError` com mensagem `"not_owner"`); `cli.js` ganha `sleep/1`.
        `consumer.js` (fetch por membro) faz **try/catch** no fetch, em `:not_owner`, back-off de 200ms e
        `continue` (retenta; imprime `~`). `subscriber.js` (subscribe por membro) reestrutura o subscribe num
        `startStream()` e, no `onError`, se `:not_owner`, **re-subscreve** após 200ms (em vez de sair) contra o
        novo dono. O `stream_ack` (heartbeat) já era fire-and-forget e auto-cura no próximo ack, sem mudança.
        Validado: `node --check` nos scripts + sanity de `isNotOwner` (não-owner, outra razão, erro não-Malachi).
        Sem harness de teste JS (padrão das fatias de cliente Node anteriores). **A1–A5 fecham o épico de
        coordinator cluster wiring: consumer groups corretos e resilientes multi-nó, fiéis ao NorthGuard.**

### 8.4 Status de adoção e desvios deliberados (retrospectiva)

As ideias de riak_core e k8s acima **já foram absorvidas** pelas fatias da Fase 1/3. O que foi adotado, e
o que foi deliberadamente **não** adotado (com o porquê):

**Adotado (com a fatia que o realizou):**
- **Fencing via consenso**: bootstrap auto-fencido pelo **nome do cluster `ra`** + **Lease sobre `ra`**
  para o trabalho contínuo do líder (R0): triângulo `duração > renew_deadline > retry_period`, token de
  fencing versionado (CAS), largar **proativo** (o *OnStoppedLeading* do k8s) e **relógio do líder** (não
  do cliente: carimbado uma vez e replicado no log, sem clock skew). [k8s Lease + RabbitMQ/`ra`]
- **Staged → planned → committed**: R1 (`desired_placement`) → R2 (`rebalance_plan`) → R3 (`apply_plan`
  sob o lease). [riak_core claimant]
- **Eleição pelo menor nó vivo + fencing**, `membership_leader` / `LeaseHolder`. [ambos]
- **Reconcile level-triggered / idempotente**: os coordinators (heal/retention/rebalance/lease) reconciliam
  **desejado × atual**, idempotentes, só-o-líder-age. [k8s controller]
- **Placement determinístico, rack/DC-aware**: A1 (`spread`) + A2 (`maxSkew` via `place_balanced`) +
  `min_domains`/hard-soft (fatia de hardening de placement), **sem randomização** (raft-safe: toda réplica
  computa o mesmo). [k8s PodTopologySpread `topologyKey`/`maxSkew`/`minDomains`/`whenUnsatisfiable` +
  riak_core binring `target_n_val`]
- **Movimento mínimo**: o HRW é *min-reshuffle* (remover um broker só move o que ele detinha; survivors
  mantêm rank); R1/R2 só movem vnodes afetados; o `heal` preserva réplicas vivas. [riak_core]
- **Add-before-remove** no rebalancing (o quórum nunca cai abaixo no meio da mudança). [riak_core/`ra`]

**Desvios deliberados / não adotado:**
- **workqueue + expectations** (k8s controller): **não** adotado. Os coordinators são **síncronos, por
  tick** (level-triggered simples), sem fila com dedupe/rate-limit nem rastreamento de operações em voo com
  TTL. Justificativa: a cardinalidade do reconcile é baixa (poucos vnodes por tick), o `ra` já **serializa**
  as mudanças de membership (uma por vez, com retry em `:cluster_change_not_permitted`) e o commit de
  rebalancing é **manual**: não há a explosão de eventos que motiva workqueue/expectations no k8s.
  Reavaliar **se** um gatilho automático de rebalancing for adicionado.
- **sticky preference no `heal`** (k8s): não precisou de código dedicado: o HRW já prefere réplicas
  sobreviventes **inerentemente** (min-reshuffle). Mesma propriedade, de graça.
- **`target_n_val` "nós E locations distintos"** (riak_core): coberto por **composição**, não por um
  parâmetro único: `Placement.place` já devolve brokers **distintos** (nós distintos) e `min_domains`
  garante **domínios distintos**; juntos ≡ `target_n_val`.
- **binring V4 (min-movement exato)** (riak_core `update()` antes de `solve()`): usamos HRW (movimento
  mínimo **estatístico**) + `maxSkew` (A2) para uniformidade, suficiente para o control plane (poucos
  vnodes); não portamos o algoritmo exato do binring.
- **etcd único** (k8s): não adotado: é justamente o **gargalo de escala** (~5000 nodes) que o sharding por
  vnode (fatia D) evita.

**Ainda aberto (por cima do mesmo motor):**
- ✅ **Gatilho automático de rebalancing (feito, opt-in).** `Malachi.Cluster.AutoRebalancer`, **política
  level-triggered** por cima do mecanismo `RebalanceCoordinator` (que segue manual-by-default). A cada tick
  (default 30s), **só no holder do lease**: pega o `plan`; se **não-vazio e igual** por `stabilization`
  ticks consecutivos (default 3 ≈ 90s), chama `commit` (que re-gate o líder). Plan vazio ou que mudou →
  reseta o contador: assim um **flap do SWIM** (nó brevemente suspeito → volta) **nunca** move vnode. É o
  padrão que a tabela acima registrou: SWIM faz a *detecção* event-driven; a *decisão* de mover reconcilia
  e converge (nada de eventos perdidos). Seams (`plan_fun`/`commit_fun`/`leader?`) → testável sem `ra`/lease
  dirigindo `reconcile_now`. **Opt-in** (`MALACHIMQ_AUTO_REBALANCE`, default off = comportamento manual
  atual intacto); `interval`/`stabilization` configuráveis; subido no `rebalance_children` só quando
  sharded + habilitado. Testado: commit após N ticks estáveis; plan que muda reseta a janela; plan vazio
  nunca commita; não-líder não commita; `stabilization: 1` commita na 1ª observação; perda/reganho de
  liderança reseta; resultado de falha-parcial repassado ao `on_result`, 7. Suíte 738 testes 0 falhas;
  credo/dialyzer limpos. README com os env vars. *(workqueue/expectations do k8s seguem **não** adotados:
  a cardinalidade é baixa, o `ra` serializa a membership e a estabilização já dá o debounce.)*
- 🚧 **Re-sharding**: mudar a **contagem** de vnodes (R1/R2 assumem o mesmo conjunto de vnode ids; por isso
  o re-sharding **não** passa pelo rebalancer e sim pelo caminho de **split**). **Grow implementado**
  (decisão: só aumentar; geometria **split-natural**): **RS-1** `Malachi.Cluster.ReshardPlan`, plano puro que
  leva o ring à contagem-alvo repetindo "splitar o **maior arco** no midpoint", sem mover token existente
  (property: cresce a exatamente N, determinístico, o maior arco nunca aumenta) → **RS-2**
  `Malachi.Cluster.ReshardCoordinator`: GenServer lease-gated que executa **um** split por vez pelo
  `SplitCoordinator`; **level-triggered**, re-planeja do ring vivo a cada passe, então um reshard
  interrompido converge só re-emitindo o mesmo alvo (nada de intent multi-passo novo); guard
  `:ring_did_not_advance` contra loop e recusa de placement vazio → **RS-3** fiação no `application.ex`
  (`reshard_coordinator_child`, junto do split coordinator sob o lease) + **`mix malachi.reshard --to N`**
  via RPC (reusa `Malachi.CLI.Rpc`) + `:multinode` sobre `ra` real (1→3 vnodes com topic **e offsets
  commitados** preservados; resume de grow parcial).
  - ⏳ **Dependência anotada: ring durável.** O ring é estado global mínimo **gossip-only** (fiel ao
    NorthGuard) e num **restart de cluster inteiro** reseeda do config (`MALACHIMQ_LOG_VNODES` via
    `sharded_vnodes/2`), cuja geometria **não** bate com o ring split-natural: o metadado migrado ficaria
    órfão. **Gap pré-existente, compartilhado com o split.** Escopo atual: reshard **em runtime**. Tornar o
    ring durável (e o reshard sobrevivente a restart total) é o follow-up natural, e beneficia o split também.
  - **Fora de escopo (registrado):** **merge/diminuir** vnodes (drenar pro sucessor + `remove_vnode` +
    deletar o grupo `ra`: genuinamente novo) e **retoken-to-even** (geometria exata de `sharded_vnodes/2`,
    exigiria primitiva de mover-token migrando todo vnode).

---

## 9. Documentation and tutorials (GitHub Pages)

The port has dense `@moduledoc` coverage, but none of it was **published**: `ex_doc` was in the project
without configuration, and CI's `mix docs` threw the output away. The trail below turns that into a site
at `hectorifc.github.io/malachi`, with an API reference and guides.

- ✅ **DOC-1: configure ExDoc plus a `LICENSE`.** A `docs:` block in `mix.exs` (`main: "introduction"`,
  `formatters: ["html"]` only, since the epub doubled the CI build), `extras` with 16 documents grouped
  by `groups_for_extras`, and `groups_for_modules` organizing the ~93 modules into 8 groups. Created the
  `LICENSE` (MIT), which `package/0` had always declared without the file existing.
- ✅ **DOC-2a: the first three guides.** `introduction`, `getting-started` and `log-model` under
  `docs/guides/`. The `introduction` was **rewritten**, not inherited from the `docs/index.html` landing
  page: that page claimed "Message Queue", "ETS tables" and "SHA256 password hashing", and all three are
  false (it is a log, the storage is durable segments plus `ra`, and the hashing is **Argon2**).
- ✅ **DOC-3: publish.** `.github/workflows/pages.yml`, building on push to `main` plus
  `workflow_dispatch`, with the same `--warnings-as-errors` gate CI uses (a broken reference fails the
  build instead of publishing a dead link). Permissions are **per job**: the build only reads the tree,
  and only the deploy carries the Pages credentials, since the build runs third-party code
  (`mix deps.get`). `cancel-in-progress: false`, because cancelling a run mid-upload would leave the
  site half published.
- ✅ **DOC-2b: produce/consume and streaming.** `produce-and-consume` (batching in one call, what a key
  does and does not decide, the three ways to track position, the idempotency at-least-once obliges,
  and the two transient errors `:migrating`/`:not_owner` a correct client retries) and
  `streaming-with-backpressure` (why the credit window exists, the exact budget
  `min(max, window - in_flight)`, why the ack fuses credit with commit, and the member ack that doubles
  as a heartbeat).
- ✅ **DOC-2c: security and operations.** `authentication` (the three mechanisms and why identity is
  pluggable while authorization is not), `per-topic-acls` (the three-rule decision and an adoption order
  for strict mode that does not lock every client out), `clustering-and-resharding` (discovery versus
  data membership, the two replication planes, growing the ring) and `operations` (ports, probes,
  metrics, retention, TLS, a production checklist). Total: **9 guides**, 22 extras, 119 pages.
- ✅ **Removed the `docs/index.html` landing page and its three orphans.** With the Pages source moving
  to "GitHub Actions" it would have stopped being served anyway, and it sat in the repository asserting
  the three false claims above. Two assets only it referenced went with it (`style.css`,
  `favicon.ico`), plus the `.nojekyll` that existed only for the "Pages from `docs/`" mode: on the
  Actions path the artifact is served directly and Jekyll never runs. The `logo.jpeg` **stayed**,
  because `README.md` and `mix.exs` (the ExDoc logo) use it. The dashboard does not count here: it
  serves its own copy at `priv/static/logo.jpeg`. The demo video was not lost either: it is still in
  `README.md`, which ships as a site extra. The only content that goes is the "How It Compares" table
  (against RabbitMQ and Redis Pub/Sub), whose queue framing was part of the error.

> **What verifying the guides caught.** The examples were **executed** against a running broker rather
> than read back, and every env var, route and default was checked against the source. That produced
> five corrections no tool would flag, because `mix docs` compiles wrong prose without complaint: the
> broker named `Malachi.Broker` instead of `Malachi.LogBroker` across all eight Elixir snippets; a
> `stream_ack` that passed the pushed positions straight through, when the call wants a cursor and needs
> `encode_cursor/1` first; `RETENTION_MAX_BYTES=0` documented as "unlimited", when the disable value is
> the **absent** variable and `0` is a real budget that expires every sealed segment; the checklist
> telling operators to enable `REQUIRE_TLS`, which in production **already** defaults to true, missing
> the real risk (someone setting `false` to work around a certificate); and the lockout described as
> per user when the key is `{user, IP}`, with the real ladder `base → ×3 → ×9 → ×24 → ×72`.

> **An operator action, not a commit:** switch the Pages source to "GitHub Actions" under Settings →
> Pages. And `workflow_dispatch` only appears in the UI once the file is on the default branch, so the
> order that works is merge to `main` first, switch the setting afterwards.
