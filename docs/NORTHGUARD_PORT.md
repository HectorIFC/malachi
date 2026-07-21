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
  - ✅ **The reference client (Node.js) reworked for the new architecture.** The old Node.js scripts spoke
    the JSON queue and channel protocol (removed in B3b); they were rewritten for the **binary protocol
    (`Malachi.Wire`) plus the log model** (topic/key/opaque cursor). The structure:
    `scripts/lib/wire.js`, a faithful port of the codec (length-prefixed framing, the request and
    response envelope, `put_str` with a presence flag, records **without an offset**, and all 7
    operations' payloads); `scripts/lib/client.js`, a TCP connection that **multiplexes requests by
    `correlation_id`** and routes the **push** frames (the server reuses the `subscribe`'s corr_id) to the
    subscription's callback rather than to a one-shot request; and `scripts/lib/cli.js`, with shared
    colours, env config and argument parsing (DRY, since the old scripts duplicated all of it). The CLIs:
    `producer.js` (appending by key, `--create`/`--key`/`--continuous`), `consumer.js` (a cursor-driven
    pull; `--group` resumes and commits server-side; `--follow` long-polls) and `subscriber.js`
    (server-push streaming, subscribe plus ack with a credit window, replacing the `channel-*` pub/sub
    that went away with the channel model). **Deleted:**
    `channel-publisher.js`/`channel-subscriber.js`/`channel-demo.sh` (the channel model is gone) and
    `i18n.js` (orphaned; the new scripts use inline English strings). `channel-demo.sh` became
    `streaming-demo.sh` (append → a live stream). Validated e2e against the real server: auth →
    create_topic → produce → fetch-by-cursor (advancing and draining) → commit plus group resume (a second
    run consumes 0) → streaming push and ack (both pre-existing records and live ones), plus clean error
    paths (`permission_denied`, `invalid_credentials`, with no crash). The README gained a client section;
    `package.json` was updated (v2, with produce/consume/subscribe/demo scripts). **No dependencies** (just
    stdlib `net`).
  - ✅ **Load-test harness (Node.js, closed loop).** `scripts/loadtest.js` generates load through the
    reference client (the choices: 1A Node reusing the client · 2C closed loop for now, structured so an
    open-loop driver can follow · 3A all four scenarios). N concurrent connections, each in an
    `op → await` loop until the deadline; the `closedLoop` driver is **separate from the ops**
    (`produceOp`/`fetchOp`) so an open-loop driver can reuse them. Scenarios: **produce** (appending by
    key), **fetch** (draining the backlog cursor-driven, rewinding at the end), **stream** (server-push
    subscribe plus ack, throughput only, since push latency is not comparable to a round trip) and
    **mixed** (half produce, half read). Metrics: throughput (ops/s, records/s, MB/s) plus p50/p90/p95/p99
    latency through **reservoir sampling** (Algorithm R, capped by `--samples`) with exact
    min/max/count/sum kept separately (the reservoir clips the tail). Flags:
    `--connections/--duration/--batch/--record-size/--keys/--max/--window/--prepopulate/--warmup/--json`; a
    single topic auto-created per run; `--prepopulate` seeds a backlog for fetch, stream and mixed. Three
    fixes found during review and validation: (1) a backoff on `closedLoop`'s non-fatal error (otherwise it
    busy-spins and eats CPU); (2) a `clearTimeout` in `streamDriver` when `onError` resolves first; and (3)
    a **unique group per invocation** in the stream scenario, because the warmup acks through to the end of
    the backlog, so sharing a group with the measured run left it empty. Validated e2e (local single node,
    small records, illustrative): produce ~42k rec/s, fetch ~307k rec/s (drain), stream ~26k rec/s (push),
    mixed ~25k rec/s and 7.5k ops/s, with 0 errors; `--json` and `--warmup` (reconnecting the clients to
    release the subscription) both fine. An operational note: many connections blow through the auth rate
    limit (10/min/IP by default), so raise `MALACHIMQ_AUTH_RATE_LIMIT` for scale tests. The README gained a
    load-test section; `package.json` gained a `loadtest` script.
    - ✅ **The open-loop driver (2C).** `--rate <rps>` fires requests at a **fixed arrival rate**,
      independent of previous responses, measuring latency from each request's **scheduled time** rather
      than from when it was actually sent. This is the **coordinated-omission correction**: a server stall
      shows up as high latency on the requests that queued behind it, which the closed loop would hide (a
      stalled worker simply emits nothing). The `openLoop` driver reuses the same ops (`scenarioOps` was
      extracted, DRY across both drivers); fetch becomes stateless (a fresh ctx per request, so it reads
      from the beginning). Requests are spread round-robin across the pool (the client multiplexes by
      corr_id). A memory guard: `--max-inflight` (default 100k), on reaching which it stops queueing and
      raises the **`saturated` flag** (the server cannot sustain the rate). The report gained the mode,
      target versus achieved, saturation and a CO-corrected label (in both text and JSON). It does not
      apply to `stream` (which is push; `--rate` is ignored with a warning). Validated e2e: a
      **sustainable** rate (1200 rps reaching 1199, with stable p50 latency of 1.5ms); **overload** (5000
      rps beyond single-node capacity, where CO latency explodes to a mean of 2.6s and p99 of 5.1s, the
      signal the closed loop masks); and the **guard** (`--max-inflight 200` flagged saturation and
      stopped). The closed loop is unchanged. The three fixes from the previous review still hold (backoff,
      timer, unique group). README and help updated.
  - ✅ **A multi-node, replicated deploy (incrementally: D1 → D2 → D3).** The HA pieces already existed
    and were tested in isolation (SWIM membership, cross-node quorum replication, `ra`, self-healing,
    failover); this phase **wires them into the application**. Node discovery is **static, through config**
    (SWIM detects failures at runtime; `libcluster` comes later).
    - ✅ **D1: control-plane HA (metadata over `ra`).** `application.ex` starts `Malachi.LogBroker` with
      `metadata_cluster`/`metadata_nodes` when `:log_cluster` is configured (env
      `MALACHIMQ_LOG_CLUSTER`/`MALACHIMQ_LOG_NODES`/`MALACHIMQ_RA_DATA_DIR`), starting `ra`; **absent
      means single-node in-memory** (the default is preserved). The config-to-options decision is a pure,
      testable function (`Malachi.Application.metadata_cluster_opts/2`). The mechanism itself (metadata
      surviving the loss of the leader) is already proven by
      `metadata_ha_test`/`broker_server_ra_test`, so it is not duplicated. Also **isolated `log_data_dir`
      per test run** (`config/test.exs`): the fixed directory persisted between runs and, with in-memory
      metadata restarting, a reused topic collided with a segment on disk
      (`Log.ensure_active :already_exists`), causing e2e flakiness; now each run gets its own directory,
      cleaned in `after_suite`.
    - ✅ **D2: a replicated data plane.** In cluster mode, `application.ex` starts one **named**
      `ReplicationServer` (`Malachi.LogReplication`) per node and connects `LogBroker` to
      `brokers: [{Malachi.LogReplication, n} | n ← log_nodes]` plus a `replication_factor` (env
      `MALACHIMQ_LOG_REPLICATION_FACTOR`, default 3, clamped to the node count by the broker). `Placement`
      (HRW) picks the `replica_set` among those brokers; the primary replicates cross-node through
      `{name, node}` and commits by **quorum**. The broker set is **static** (every node in the config); a
      downed follower is tolerated by the quorum (the live `live_brokers` is D3). The wiring is testable
      through pure functions (`Malachi.Application.broker_refs/1`, `data_plane_opts/2`); the
      quorum-and-tolerance mechanism is already covered by `replication_server_test`, and the integration
      (BrokerServer plus 3 brokers plus rf=3 plus ra, producing and consuming by quorum end to end) by
      `broker_server_ra_test`.
    - ✅ **D3, live membership plus healing and failover.**
      - ✅ **D3a: a cross-node `MembershipServer`.** SWIM identified each member by its `self_ref`, which
        was the registration `:name`: every test was in-process (with unique atoms). Cross-node that
        collided, because the same `Malachi.LogMembership` atom resolves to the **local** server on every
        node, so a gossiped sender pointed at the receiver itself and the view never converged. Now
        `MembershipServer` accepts a `:self_ref` (a **node-qualified** identity `{name, node()}`, which is
        what gets gossiped) distinct from `:name` (the local registration), plus `start/1` (unlinked, for
        starting on remote nodes). Proven by a real **multinode test** (`membership_ha_test`): 3 `:peer`
        nodes converge through the seeds and detect one node's death (SWIM: suspect → dead → out of the
        alive set).
      - ✅ **D3b: the application wiring.** In cluster mode, `application.ex` starts (in this order)
        `MembershipServer` → `ReplicationServer` → `LogBroker` → `HealCoordinator`. `MembershipServer` uses
        `self_ref: {Malachi.LogMembership, node()}` and `peers: membership_seeds(log_nodes)` (the other
        nodes). The `live_brokers` function derives from `alive_members` into
        `{Malachi.LogReplication, node}` refs, and is passed both to `LogBroker` (which narrows new
        segments' placement to the live set, with the static `:brokers` being the initial placement) **and**
        to `HealCoordinator`. `HealCoordinator` (`metadata_source: BrokerServer.metadata`,
        `apply_command: BrokerServer.apply_heal`, `replication_factor`) closes the loop *a broker dies →
        membership marks it gone → segments are re-replicated and a primary is promoted*. The wiring is
        pure and testable (`membership_seeds/1`, `live_replication_refs/1`); the healing and failover loop
        is already covered by `heal_coordinator_test`/`self_healing_test`/`failover_test`, and cross-node
        membership by D3a.
  - ✅ **The multi-node, replicated deploy is complete** (D1 control-plane HA plus D2 a replicated data
    plane plus D3 live membership, healing and failover). Wired through static config; `libcluster`
    (dynamic discovery) is left as a future convenience.
- ✅ **C. The remaining NorthGuard features.** Decision: start with **C1, retention (by time and size)**;
  attributes (C2) and policies (C3) come after. The approved design: an explicit `sealed_at` on the segment
  (for age), size retention **per range**, and a consumer sitting on expired data **advancing to the
  earliest available point** (keeping the cursor opaque). Sub-slices: C1a (the delete primitive) → C1b (the
  coordinator plus the policy plus the wiring).
  - ✅ **C1a: the control plane.** `segment_meta` gains `sealed_at` (epoch ms, `nil` while active); the
    `seal_segment` command carries the `sealed_at` (generated in `Broker` the way `Record` timestamps are,
    so it stays deterministic across replicas). A new `{:delete_segment, segment_id}` command removes a
    **sealed** segment from the control plane (`:segment_active` if it is still active, so it never drops
    the active one; `:no_such_segment` when absent). Tested: a `delete_segment` unit test
    (sealed/active/nonexistent), `sealed_at` on the seal, and determinism preserved (property tests).
  - ✅ **C1a: the storage delete.** Each of the broker's segments is a `Log` in its own subdirectory, and
    `ReplicationServer` keeps `logs: %{segment_id => Log}`, so deleting means **closing the `Log` and
    removing the directory**, with no need for a granular delete in `SegmentStore`. `Log.delete/1` (closes
    it and `rm_rf`s the directory, best-effort). `ReplicationServer.delete/2` (client plus handle_call)
    closes and removes it when the log is open, otherwise it cleans up orphan files on disk (after a
    restart); it is **idempotent** (deleting an unknown segment is `:ok`). Tested: `Log.delete` (the
    directory disappears) and `ReplicationServer.delete` (the data disappears, so a read becomes `:eof`;
    plus idempotence).
  - ✅ **C1b: the coordinator plus the read path plus the wiring** (incrementally).
    - ✅ **C1b-1: the policy plus `RetentionCoordinator`.** `segment_meta` gains `byte_size` (through
      `seal_segment`, from the Broker's `active.bytes`: deterministic, like `sealed_at`, since size
      retention needs bytes). A **pure** `Malachi.Cluster.Retention` module: `expired(metadata, now_ms,
      policy)` returns the ids of **sealed** segments to expire by **age** (`sealed_at` older than
      `max_age_ms`) and by **size** (where the `byte_size` sum **per range** exceeds `max_bytes`, oldest
      first), unioned; never the active one; a `nil` bound disables that rule. `RetentionCoordinator` (a
      periodic GenServer on the `HealCoordinator` model) with seams (`metadata_source`, `expire_segment`,
      `policy`, `clock`, `interval`): each sweep resolves the ids to their metas and calls
      `expire_segment`. Tested: `Retention` (age, per-range size, the union, never the active one, `nil`
      disabling) and the coordinator (a sweep through `run_now` and through a tick, with the complete meta
      reaching the seam).
    - ✅ **C1b-2: the read path (advancing past expired data).** Retention always deletes a **contiguous
      prefix** (the oldest segments), so the read path only needs to know the smallest `start_offset` still
      stored (`earliest_offset/2`) and to **clamp the offset to it** before reading. `consume_page` (live
      consumption) and `read_history_page` (admin) both clamp: a consumer whose cursor landed in an expired
      hole advances transparently to live data (at-least-once, with the opaque cursor intact) instead of
      seeing a misleading `:eof`; and since there are no interior holes, `offset + length` stays correct
      (with no change to `read`/`locate_segment`). Tested: a consumer below the earliest offset skips the
      expired segments and reads what remains.
    - ✅ **C1b-3: config plus wiring (C1 retention complete).** `Broker.delete_segment/2` and
      `BrokerServer.delete_segment/2` (applying `:delete_segment` through the control plane, Raft-backed
      when configured). The real `expire_segment` (in `application.ex`) removes it from the control plane
      and then deletes the storage on each replica (`ReplicationServer.delete`), **best-effort** (the
      control plane is idempotent and storage tolerates an absent segment). Config through env:
      `MALACHIMQ_RETENTION_MAX_AGE_MS` / `MALACHIMQ_RETENTION_MAX_BYTES` (both absent means **keep
      forever**, and the coordinator does not start) / `MALACHIMQ_RETENTION_INTERVAL_MS`.
      `RetentionCoordinator` joins the tree **whenever there is a policy** (it matters single-node too),
      after `LogBroker`. Tested: `BrokerServer.delete_segment` (dropping a sealed segment) and **e2e**
      (produce → seal → a coordinator sweep → the segment disappears from the control plane **and** from
      storage). **C1 (retention by time and size) is complete.**
- ✅ **C2: attributes** (opaque k/v that an admin attaches to brokers; the basis of rack and DC
  awareness). **Decision:** disseminate them through **Membership/SWIM** (faithful to NorthGuard:
  "membership piggyback host/port/attributes") rather than in Metadata. The user prioritized fidelity.
  Incrementally: C2a (pure Membership) → C2b (the server plus the API plus gossip) → C2c (wiring plus
  config).
  - ✅ **C2a: pure `Membership` with attributes.** A member's attrs **travel with the update**, governed
    by the same **incarnation**: `update` becomes a 4-tuple `{member, status, incarnation, attributes}`
    and `member_state` gains `attributes`. Only the owner changes its own attrs, through
    `set_attributes/2`, which **raises its own incarnation** so the change wins the merge everywhere; a
    suspicion or confirmation from another node carries the attrs it **already knows** (preserving them).
    `overrides?` (the `{incarnation, rank}` precedence) is unchanged. `new/2` accepts the self's
    `:attributes`; `attributes/2` queries them. Tested: propagation and replacement by incarnation,
    `set_attributes`, preservation under suspicion, gossip through `updates`; order-independent
    convergence preserved (a property; attrs are consistent per incarnation, so the generators use `%{}`).
    Multinode SWIM (D3a) keeps converging with the 4-tuple.
  - ✅ **C2b. `MembershipServer` with attributes.** An `:attributes` option (the self's initial attrs,
    passed to `Membership.new`); a `set_attributes/2` API (changing our own attrs at runtime, raising the
    incarnation) and `attributes/2` (reading a member's). Dissemination is **passive** (anti-entropy): the
    server ignores the effects and the periodic gossip (ping and ack piggybacking `updates`) propagates
    them, with no proactive push, consistent with the rest of the server. Tested: the initial attrs are
    readable, and `set_attributes` on one node propagates to a peer through gossip.
  - ✅ **C2c: the application wiring (C2 complete).** `application.ex` connects the self's attributes to
    the cluster's `MembershipServer`: `MALACHIMQ_LOG_ATTRIBUTES` (in the form `"rack=a,dc=east"`) is
    parsed by `Malachi.Application.parse_attributes/1` (a pure, testable function: it ignores entries
    without `=`, trims, and preserves `=` inside the value) and passed as `:attributes`. Absent yields
    `%{}`. Tested: the parse (empty, pairs, trimming, invalid entries, a value containing `=`). **C2
    (attributes over SWIM) is complete**: brokers disseminate their attrs by gossip, ready for C3's
    rack-aware placement.
- ✅ **C3: policies** (a name plus retention plus constraints over attributes, yielding replica sets;
  faithful to NorthGuard, which unifies all of it under *policies*). Incrementally: C3a (pure Placement
  with spread) → C3b (integration: membership attrs feeding placement) → C3c (per-topic policies:
  definition plus association plus per-topic retention).
  - ✅ **C3a. Pure `Placement` with spread (rack-aware).** `place/4` gains a `:spread =
    {attribute_key, attributes}` option: over the (deterministic) HRW ranking it **round-robins across the
    attribute's distinct values**, taking the best-ranked broker of each value first, then the next of
    each, until `rf`. With `rf` at most the number of values, every replica lands in a distinct rack or
    DC; beyond that it round-robins best-effort. Deterministic (the HRW ranking plus stable grouping);
    without `:spread` it is the previous top-`rf` (so every `place/3` caller is untouched). Tested:
    distinct values, prioritizing diversity over pure rank (rf=2 over a,a,b gives a,b), best-effort when
    rf exceeds the value count, brokers lacking the attribute grouping separately, and determinism.
  - ✅ **C3b: integration (attributes feeding placement).** `Broker` gains `spread_by` (the attribute key)
    and `broker_attributes` (a broker-to-attrs map); `open` accepts them, `set_broker_attributes/2` updates
    them (like `set_brokers`), and `open_segment` passes `:spread` to `Placement.place` when `spread_by` is
    set (otherwise placement is unchanged, so every caller is untouched). `BrokerServer` accepts
    `:spread_by` plus a `:broker_attributes` function and **refreshes it periodically** (on the same timer
    as `:live_brokers`) into the broker, so the attrs the membership disseminates (C2) flow into placement.
    Tested: a produce spreads replicas across racks, `set_broker_attributes` affects the next placement,
    and `BrokerServer`'s refresh pulls the attrs.
  - ✅ **C3c-1: wiring rack awareness into the application.** `data_plane_opts` connects `spread_by` (env
    `MALACHIMQ_LOG_SPREAD_BY`, for example `"rack"`) and a `broker_attributes` function derived from
    `MembershipServer`: `broker_attributes_for/2` (pure and testable) maps each live member
    `{LogMembership, node}` to `{LogReplication, node}` carrying the gossiped attrs (C2). With that,
    **rack-aware placement works end to end in the application** (membership attrs feeding placement
    spread). Without `spread_by` it is plain HRW.
  - ✅ **C3c-2. Per-topic policies** (NorthGuard's umbrella concept). Decision: policies live
    **dynamically in Metadata (`ra`)**, with **both** scopes (per-topic retention and placement).
    Incrementally: 2a (Metadata: policies plus association) → 2b (per-topic retention) → 2c (per-topic
    placement). **This closes C3.**
    - ✅ **C3c-2a. Metadata: policies plus association.** `Metadata` gains `policies: %{name => policy}`
      (`policy = %{optional(:retention) => %{max_age_ms, max_bytes}, optional(:spread_by) => term}`),
      `topic_meta` gains `policy: name | nil`, and two commands arrive: `{:define_policy, name, policy}`
      (validating a non-empty binary name plus a policy map, `:invalid_policy` otherwise) and
      `{:set_topic_policy, topic, name | nil}` (`:no_such_topic`/`:no_such_policy`; `nil` disassociates,
      falling back to the globals). Queries: `get_policy/2` and `topic_policy/2` (resolving a topic's
      policy). Determinism preserved (a property). **Unused so far**: 2b and 2c connect retention and
      placement to the topic's policy.
    - ✅ **C3c-2b: per-topic retention.** `Retention.expired/3` now resolves, **per range**, the effective
      retention: the topic policy's `:retention` (`Metadata.topic_policy/2`) **merged over** the global
      policy (the policy overrides only the keys it defines, through `Map.merge`), or the global one when
      the topic has no policy. `RetentionCoordinator` is unchanged (it already passes the metadata plus the
      global). Tested: a topic policy overriding the global, the merge (an undefined key falling through to
      the global), and a topic without a policy using the global.
    - ✅ **C3c-2c: per-topic placement.** `Broker.open_segment` resolves, per range, the effective
      `spread_by`: the topic policy's `:spread_by` (`Metadata.topic_policy/2`) when the policy **defines**
      that key (an explicit `nil` opts the topic **out** of spreading, back to plain rendezvous),
      overriding the global; otherwise the broker's global `spread_by`. Symmetric with 2b (a defined key
      wins, `nil` included). Only `place_opts` and `effective_spread_by` change. Tested: a policy turning
      spread on over a global-off; an explicit `nil` turning it off over a global-on (that is, plain
      rendezvous).
- ✅ **D. Sharding through `ReplicatedDSRSM`** (now **in scope**): sharded metadata (one `ra` cluster per
  vnode) to **scale the control plane** beyond a single Raft cluster. Decision: **1A**, where the `Broker`'s
  cache becomes a `DSRSM` (mirroring the `Metadata`/`ReplicatedMetadata` pair), routing reads and writes by
  topic; plus **2A**. Incremental, pure core first. The infrastructure was already in place: `HashRing`,
  `DSRSM` (pure), `ReplicatedDSRSM` (over ra), and `MetadataMachine`/`MetadataServer`.
  - ✅ **D-a: `Broker` over `DSRSM` (in-memory, 1 vnode).** The `Broker`'s cache stops being a `Metadata`
    and becomes a `DSRSM` (`broker.dsrsm`), routed by topic (derivable from the `range_id` `{topic, seq}`
    or the `segment_id`). A new pure combinator `DSRSM.update_vnode/3` (routes, then applies a function to
    that vnode's `Metadata`); `DSRSM.command/3` delegates to it with `&Metadata.apply/2` (leaving the
    property tests intact). `Broker`'s `command_fun` becomes `(DSRSM, topic, command) -> {DSRSM, reply}`
    (defaulting to `&DSRSM.command/3`); `apply_metadata` derives the topic through `command_topic/1`. New
    accessors on `DSRSM`: `single/1` (the trivial 1-vnode form, for seeding), `committed_offsets/3`,
    `topic_policy/2` and `merged_metadata/1` (the union of the shards, feeding `Broker.metadata/1` for
    retention and healing). In `BrokerServer`, the Raft path wraps the single cluster as
    `DSRSM.single(seed)` plus a `command_fun/3` that injects `ReplicatedMetadata.apply_command` into
    `update_vnode` (D-b swaps in the real `ReplicatedDSRSM`). With 1 vnode the behaviour is identical: the
    full suite stayed green (981) as the safety net.
  - ✅ **D-b** (the runtime, `BrokerServer` over `ReplicatedDSRSM`, N vnodes). Decision: **1A**, D-b-1
    (N vnodes **single-node**) first; per-vnode HA (D-b-2) after.
    - ✅ **D-b-1. A sharded control plane, single-node.** `BrokerServer` gains the `:metadata_vnodes` path
      (`[{cluster_name, token}]`): it starts N `ra` clusters through `ReplicatedDSRSM` (one per vnode),
      materializes the local cache with `ReplicatedDSRSM.snapshot/1` (new: it reads each vnode's `Metadata`
      into `DSRSM.seed/2`, also new, sharing the ring), and uses a sharded `command_fun/3` that routes by
      topic to the vnode's `ra` cluster (`ReplicatedDSRSM.server_for/2`, new) applying through
      `ReplicatedMetadata.apply_command` inside `update_vnode`. The `:metadata_cluster` path (1 vnode,
      D-a/D1) is untouched. Config: `log_vnodes` (an integer N;
      `Malachi.Application.sharded_vnodes/2` generates N vnodes with tokens spread uniformly over the
      32-bit ring). Tested: with 2 vnodes, each topic lives in exactly the `ra` cluster its name routes to
      (never the other), and topics distribute across the vnodes; plus `sharded_vnodes/2` (distinct tokens,
      within range).
    - ✅ **D-b-2: per-vnode HA.** `ReplicatedDSRSM.add_vnode/4` now takes the `nodes`, starting each
      vnode's `ra` cluster across them (`MetadataServer.start/2`), so every vnode survives losing a member.
      The model: **all vnodes over the same set of M nodes** (mirroring D1; placing vnodes on subsets of
      nodes is left for later). `BrokerServer` passes the `metadata_nodes` into the sharded path
      (`start_vnodes/2`); `Application.metadata_opts` includes `metadata_nodes` on the sharded path.
      `snapshot/1` uses `&Function.identity/1` (a linearizable query runs on the leader, possibly remote).
      Tested (`:multinode`): 2 vnodes over 3 nodes, killing one member of the owning vnode (the leader if
      it is a peer, triggering failover, otherwise a follower), and the vnode still commits with the
      metadata intact, both its own and the other vnode's.
  - ✅ **D-c: per-vnode control-plane management** (retention, healing, failover). **Closed by 1C-a plus
    1C-b** (leader-only coordinators plus a per-vnode-leader manager; see the sub-slices below). What
    follows is the **context of the debt** that motivated 1C, that is, the state *before* it.
    **Pre-1C state:** metadata *writes* were already sharded (D-b), but *management* stayed
    **centralized**: one `RetentionCoordinator` and one `HealCoordinator` on the `BrokerServer`'s node
    read `merged_metadata` (the **union** of every shard) and emit commands
    (`delete_segment`/`set_segment_replicas`) that **route back** by topic to the owning vnode (through
    `command_topic/1`). That is **correct** under sharding (the union is exact and the commands route),
    but it conceptually reintroduces the single point that sharding removes: a **sequencing-fidelity
    debt**, not a correctness one.

    The NorthGuard-faithful target is **1C: a coordinator living on the leadership of each vnode's Raft
    group** (so each node manages retention and healing for the vnodes it leads). That **belongs to phase
    1** (distribution), **not** phase 2 (native efficiency and profiling). The reason 1C did not come
    sooner is not that it is an "optimization", it is that it has **prerequisites**:
      1. **Placing vnodes on subsets of nodes** (until then every vnode lived on the same M nodes,
         deferred in D-b-2). Without spreading the vnodes, "the vnode's leader" is any of the M nodes and
         there is little real distribution to do. **Slice D-c-1** (decision: **1A** HRW reusing
         `Placement`; **2A** pure core first):
           - ✅ **D-c-1a: the pure core.** `Malachi.Application.place_vnodes/3` assigns each vnode
             (`{vnode_id, token}`) the `R` nodes of its `ra` cluster, chosen from `nodes` by rendezvous
             (the same HRW `Placement.place/4` the segments use), yielding `{vnode_id, token, nodes}`;
             deterministic, minimal movement, with an effective `R` of `min(R, M)`. Tested in isolation
             (HRW spreads, determinism, the clamp). **Unused so far**: D-c-1b connects it to
             `ReplicatedDSRSM`/`BrokerServer`.
           - ✅ **D-c-1b: cross-node routing.** `MetadataServer.start/2` now returns the server of a **real
             member** (the local node when it is one, otherwise the first in the placement) rather than
             always the local one, so a vnode placed on a subset of nodes is reachable from a node that
             hosts **no** replica of it (`ra` routes that member's `command`/`query` to the leader; the
             caller need not be a member). `ReplicatedDSRSM` stores that server, and
             `command`/`query`/`snapshot`/`server_for` all start working cross-node. Tested
             (`:multinode`): 2 vnodes on **disjoint** subsets of 3 nodes, orchestrated from a node hosting
             neither, with commits and queries routing to the right member and `snapshot` materializing
             everything. (Decision **1A**: the mechanism kept separate from distributed bootstrap.)
           - ✅ **D-c-1c: distributed bootstrap (a static seed).** `Application.metadata_opts` wires in
             `place_vnodes` (so `metadata_vnodes` becomes `[{vnode_id, token, nodes}]`, with R being
             `log_vnode_replication_factor`) and injects the `bootstrap_orchestrator?` policy as
             `Malachi.Application.static_seed/1` (true only on the lowest node). In `BrokerServer`, the
             **orchestrator** does `add_vnode` (start_cluster) for each vnode, while the
             **non-orchestrators** call `ReplicatedDSRSM.route_vnode/4` (new: it registers the ring entry
             plus a member's server **without** starting anything), so exactly one node bootstraps each
             vnode (the RabbitMQ and `ra` pattern). `snapshot/1` became **tolerant** (a vnode that is not
             ready yields an empty `Metadata` rather than a crash) and `BrokerServer` **re-seeds the
             cache** from the `ra` clusters right after boot (the election window) and periodically
             (`Broker.put_cache/2`), which also covers the multi-writer case. The choice was **1B, the
             static seed** (against 1A concurrent, which is risky on `ra`; and against leader-driven
             orchestration, which is D-c-1d with fencing). Tested: `static_seed` (only the lowest node),
             `route_vnode` plus the tolerant `snapshot` (single-node), and `:multinode`: the orchestrator
             starts across 2 nodes while a non-orchestrator only routes yet reads and writes cross-node.
             **Config:** `MALACHIMQ_LOG_VNODE_REPLICATION_FACTOR`.
           - ✅ **D-c-1d: `membership_leader` plus a reconcile loop.** The orchestration policy moves from
             the static seed to `Malachi.Application.membership_leader/1`: true only on the lowest **live**
             node (`MembershipServer.alive_members`, SWIM), so the role **fails over** when the leader
             dies (tolerantly: if membership does not answer, the node is not the leader, so there are
             never two). Bootstrap becomes a **reconcile** (controller style, k8s): at boot **every** node
             only calls `route_vnode` (`build_replicated` without `start`); `BrokerServer` reconciles
             (level-triggered, idempotent) right after boot and periodically, and **only on the leader**,
             calling `MetadataServer.start/2` for the vnodes whose cluster is not ready yet
             (`MetadataServer.ready?/1`, new). The **fencing** is the `ra` cluster's name (a second `start`
             of the same vnode fails without duplicating: validated empirically); the *lease* (the literal
             k8s way) is left for **1C**, where the leader starts doing **continuous** work (retention,
             healing, rebalancing). `static_seed/1` stays available as an alternative (and is tested).
             Tested: `membership_leader` (lowest-live, plus tolerance); and integration, where the leader
             bootstraps the vnodes through the reconcile and a `create_topic` commits. See section 8 (the
             k8s and riak_core references).
      2. **Detecting and reacting to per-vnode Raft leadership**: a supervisor that starts and stops
         coordinators as leadership changes (through `ra` events), tolerating oscillation and momentary
         split brain.
    The sequence: D-b ✅ → **D-c-1 vnode placement** ✅ → **1C-a leader-only coordinators** ✅ →
    **1C-b-i per-vnode Raft leadership detection** ✅ → **1C-b-ii-α a coordinator aimed at one vnode** ✅
      → **1C-b-ii-β the per-vnode-leader supervisor/manager** ✅. **1C-b is complete.**

    - ✅ **1C-a: leader-only coordinators (no lease).** `RetentionCoordinator` and `HealCoordinator` gain
      a `:leader?` seam (`(-> boolean())`, defaulting to always); on each tick they only sweep or heal if
      `leader?()`, otherwise the tick runs but skips. `Application` injects
      `membership_leader(Malachi.LogMembership)` (reusing D-c-1d) into both when clustered
      (`coordinator_leader?/1`; single-node always acts). This removes the **redundancy** (N nodes doing
      the same work) while keeping the level-triggered model. **No lease:** the work is idempotent and
      routed through `ra` (which serializes), so two transient coordinators (during SWIM convergence)
      merely redo work rather than corrupting anything, the same reasoning as the bootstrap.
      `run_now/1` and `heal_now/1` bypass the gate (they are manual triggers). Tested: a non-leader skips
      the tick; the manual trigger acts regardless.
    - ✅ **1C-b-i: per-vnode Raft leadership detection (the pure core).** `MetadataServer.leader?/1`
      mirrors `ready?/1`: it reads the leader `:ra.members` reports (any reachable member answers) and is
      true only when that leader is the **given** `server_id`, so passing the **local** server
      (`{vnode_id, node()}`) asks "does this node lead this vnode?". An unformed or unreachable cluster
      yields false (it never assumes leadership). `Malachi.Application.leading_vnodes/3` is the pure
      selector: given the placement (`[{vnode_id, token, nodes}]` from the bootstrap), the local node and
      a `leader?` predicate (defaulting to `MetadataServer.leader?/1`), it returns the vnodes the node
      **hosts** (the placement includes it) **and** **leads**, short-circuiting `leader?` for vnodes it
      does not host. That is where 1C-b-ii will run the coordinators, one per vnode, on that vnode's Raft
      leader (the literal NorthGuard model, distributing the load rather than 1C-a's single membership
      leader). Tested: `leader?` on a single-node leader (real ra) plus an unformed cluster yielding
      false; and `leading_vnodes` filtering host-and-leads, preserving order, and never querying
      leadership for a vnode it does not host.
    - ✅ **1C-b-ii-α: a coordinator aimed at one vnode.** `Malachi.Application.vnode_metadata_source/1` is
      a `metadata_source` bound to **one** vnode: it reads that vnode's local `Metadata` view through
      `MetadataServer.query({vnode_id, node()}, & &1)` (a consistent query against the vnode's ra),
      **tolerantly** (an unformed or unreachable vnode yields `Metadata.new()` rather than crashing the
      coordinator). Since the type is the same (`Metadata.t()`) and `expire_segment`/`apply_heal` already
      route to the owning vnode by topic, `RetentionCoordinator` and `HealCoordinator` **do not change**:
      it is enough to swap the source (per-vnode instead of the global merge) and the gate
      (`MetadataServer.leader?({vnode_id, node()})`). Tested (against real ra): the source is tolerant of
      an unformed vnode; it reads only that vnode's shard; and a `RetentionCoordinator` bound to one vnode
      expires only **that** vnode's segments.
    - ✅ **1C-b-ii-β, the per-vnode-leader supervisor and manager.**
      `Malachi.Cluster.VnodeCoordinatorManager` (a generic GenServer, testable through the
      `leading`/`spawn`/`stop` seams) reconciles by **level-triggered polling** (right after boot through
      `handle_continue`, then every `:vnode_reconcile_interval_ms`, default 5s): it compares the vnodes
      this node leads right now (`leading_vnodes/3` over `MetadataServer.leader?`) with the ones it
      already runs, **starts** a retention-plus-heal pair for the newly led ones and **stops** those it no
      longer leads. Each pair lives under a **per-vnode supervisor** (`:one_for_one`) inside a
      `DynamicSupervisor` (`Malachi.LogVnodeCoordinatorSupervisor`), so a coordinator that crashes
      restarts without the manager losing its handle. In `Application`, `coordinator_children/2`
      **replaces** 1C-a's single coordinators with the supervisor-plus-manager pair **when sharded**
      (`vnode_placement/2` is not nil, extracted and reused by `metadata_opts`); single-node and 1-vnode
      clusters stay on 1C-a. Each coordinator keeps the `MetadataServer.leader?({vnode_id, node()})` gate
      as **defence in depth** (if the manager lags during an oscillation, the coordinator still will not
      act after losing leadership). **Idempotent and lease-free** (the same reasoning as 1C-a): a
      transient flap merely redoes work routed through `ra`, it does not corrupt. Tested: the reconcile
      through seams (start, stop, idempotence, draining) plus integration (real ra, single-node), where
      the manager starts one real `RetentionCoordinator` per led vnode and each expires only **its own**
      vnode's segments. A **lease over `ra`** is left for when the coordinator gains **non-idempotent**
      work (rebalancing that moves data).

    (The **1B** alternative, coordinators iterating per vnode but still centralized, avoids materializing
    the merge but is a halfway house with no measured bottleneck behind it; passed over in favour of going
    straight to placement.)

### Phase 2, native efficiency (conditional, driven by profiling)
- `Malachi.SegmentStore.Native` in Rust (Rustler): O_DIRECT, an app-level cache, `erlang-rocksdb`.
- To be implemented only if phase 1 shows the latency tail or the page cache to be the bottleneck under
  concurrency.

### (Future) An Xinfra-like layer
- Virtual topics with epochs, opaque offsets, dual-write migration, consumer-group management.
  Out of the initial scope.

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

## 8. External design references: riak_core and Kubernetes

We studied three repositories, **riak_core** (Riak's consistent hashing, vnodes, membership and handoff
library) and **Kubernetes**, to learn how mature systems solve the problems phase 1 (distribution) runs
into: distributed bootstrap, leader election with fencing, failure-tolerant placement, and reconcile
loops.

**The common framing.** All three converge on the same pattern for coordinating shards: a **single
elected coordinator**, with **fencing through consensus**. RabbitMQ (the same `ra` library we use) fences
on the **cluster name**; riak_core uses a *claimant* (**weak** fencing, over gossip); k8s uses a *Lease*
over etcd (**strong** fencing, a linearizable CAS). **The overall verdict: a design reference, not a
dependency.** riak_core is **AP** (gossip plus vector clocks; strong consensus only in `riak_ensemble`,
which is external) and k8s centralizes metadata in a **single** **etcd** cluster (Raft, not sharded).
Malachi is **CP and sharded** (one `ra` cluster per vnode), so neither serves as a direct dependency, but
the **algorithms and patterns are portable**: swap "gossip" for "Raft" and preserve determinism.

### 8.1 riak_core (AP). Reference: OpenRiak (`~/riak_core`, Apache 2.0, an active fork)

- **Where malachi is already ahead (by being CP):** metadata in Raft (over gossip); SWIM with suspicion
  (over plain gossip); automatic failover; retention; rack and DC attributes. *Do not regress while
  porting.*
- **`riak_core_claimant` → D-c-1d plus rebalancing:** the **staged → planned → committed** model (the
  *plan* computes the new ring without changing state; the *commit* validates that nothing diverged) plus
  **lexicographic** election (the lowest node, which is our `static_seed`). The claimant's fencing is
  **weak** (gossip plus a vclock, so split brain is possible), which **validates** malachi's decision to
  fence the bootstrap on the `ra` cluster's **name**.
- **`riak_core_claim_binring` V4 → an upgrade for `place_vnodes`:** it guarantees replicas on distinct
  nodes **and** distinct *locations* (rack/DC) (`target_n_val`), uniform balancing (k or k+1 per node) and
  rebalancing with **minimal movement** (`update()` before `solve()`). Well-commented documentation:
  `~/riak_core/docs/claim-version4.md`.
- **Ring versioning plus the claim cycle → dynamic re-clustering** (adding and removing nodes), **done**
  in rebalancing R1 through R3 (diffing the live placement plus `apply_plan` under the lease); the trigger
  is still **manual** (see 8.4).
- **Ignore:** ring gossip, vector clocks (Raft already provides ordering and consensus), `node_watcher`
  (SWIM covers it), and preflist-over-vnodes (an intentional deviation: malachi shards **metadata by
  topic** rather than distributing data keys over the ring).

### 8.2 Kubernetes (CP through a single etcd)

- **Leader election plus Lease → D-c-1d and, above all, 1C (coordinators on the leader):** the "triangle"
  `LeaseDuration > RenewDeadline > RetryPeriod`, plus a **linearizable CAS** (etcd; our `ra` gives the
  same), plus **proactively giving up** when a renewal fails (`OnStoppedLeading`, which avoids split
  brain), plus a **local clock** (tolerating clock skew; the premise being NTP, with drift at most about
  `lease/10`). It translates into a **lease stored in an `ra` cluster** (a versioned CAS command). **The
  key nuance:** `ra`'s fencing-by-name **suffices for the bootstrap** (a single, self-fencing start); the
  **lease** is only necessary for the leader's **continuous** work, retention, healing and rebalancing
  (1C). Files: `client-go/tools/leaderelection/` (`leaderelection.go`,
  `resourcelock/leaselock.go`), `api/coordination/v1/types.go`.
- **Controller/reconcile → the coordinators plus the bootstrap reconcile:** **level-triggered**
  (reconciling the complete *desired versus actual* state, which malachi's coordinators already do);
  **idempotence**; a **workqueue** with dedupe, rate limiting and retry/backoff; **expectations**
  (tracking in-flight operations with a TTL, so as not to react again too early); **only the leader
  acts**; and **observability** (a convergence healthz). Reference:
  `pkg/controller/replicaset/replica_set.go`, `client-go/util/workqueue`.
- **Scheduler / PodTopologySpread → `place_vnodes`:** `topologyKey` (equivalent to our `spread_by` and
  attributes), **`maxSkew`** (the maximum imbalance across racks or zones), **`minDomains`** (the minimum
  number of distinct domains), **`whenUnsatisfiable`** (hard `DoNotSchedule` versus soft
  `ScheduleAnyway`), and the **Filter → Score** pipeline. **A critical caveat:** k8s **randomizes** the
  tie-break, whereas `place_vnodes` must stay **deterministic** (raft-safe: every replica computes the
  same placement). So port the *ideas* (maxSkew, minDomains, hard versus soft) onto pure functions,
  without randomization; and adopt a *sticky preference* in `heal()` (preferring surviving replicas, which
  means less churn). Reference: `pkg/scheduler/framework/plugins/podtopologyspread/`.
- **A recorded trade-off:** a single etcd is simple, but it is k8s's **scale bottleneck** (roughly 5000
  nodes per cluster). Malachi's sharding pays complexity (bootstrap, leader, fencing) precisely to
  **scale beyond one quorum**, which is the motivation behind slice D.

### 8.3 Synthesis: how this informed the slices (the execution history)

> Note: the slices below (D-c-1d, 1C, place_vnodes, rebalancing) **are done**; this block is the
> history. The summary of what was adopted versus deviated from is in **8.4**.

- **D-c-1d (`membership_leader`):** election by the lowest **live** node (SWIM) plus fencing on the `ra`
  name during bootstrap; a **level-triggered, idempotent** reconcile loop (the k8s *controller* pattern).
- **1C (coordinators on the leader):** this is where the **Lease over `ra`** (k8s) comes in to fence
  **continuous** work, along with the **staged / planned / committed** model (riak_core's claimant) for
  ring changes.
- **Upgrading `place_vnodes`:** `target_n_val` and location awareness (riak_core's binring) plus
  `maxSkew`, `minDomains` and hard-versus-soft (k8s topology spread), **while keeping determinism**.
  - ✅ **A1: rack spread (done).** `place_vnodes/4` gains `place_opts`, forwarded to `Placement.place/4`;
    with `[spread: {attribute_key, node_attributes}]` each vnode's R replicas land in **distinct racks or
    zones** (`target_n_val`/`minDomains`), so losing a whole rack does not take out a majority of any
    vnode's replicas. It reuses the existing `spread` (round-robin by attribute value, from C3a). The
    topology is **static config** (`log_topology`, `"node=rack,..."`, `parse_topology/1`), identical on
    every node, which keeps placement **deterministic**. `Application.metadata_opts` wires the spread
    through `vnode_place_opts/0` (`:log_spread_by` plus `:log_topology`). Tested: each vnode with R=2
    spans both racks; plus determinism.
  - ✅ **A2: global load balancing (`maxSkew`).** `Placement.place_balanced/4` places the **whole set**
    of vnodes under a **load cap** (a global view rather than a per-vnode one): each vnode still ranks by
    HRW, but a node at the cap (`ceil(total/nodes) + max_skew - 1`) is **skipped** for the next one, so
    no node ends up overloaded. Deterministic (the ranking, the order and the counters are identical on
    every node). The cap is **best-effort**: RF is **never** sacrificed for balance, so a vnode that would
    otherwise fail to reach `min(rf, nodes)` distinct replicas takes the least-loaded node even above the
    cap (which can happen with `rf > 1`, where the per-vnode greedy does not pack perfectly); with
    `rf = 1` the cap is **hard**. With a large `max_skew` it degrades to plain HRW (minimal movement). It
    is **standalone** (it does not combine with A1's `:spread`: the two are mutually exclusive, and A2
    takes precedence). `place_vnodes/4` uses `place_balanced` when given `[max_skew: n]`;
    `vnode_place_opts` wires it through `:log_max_skew` (`MALACHIMQ_LOG_MAX_SKEW`). Tested: plain HRW
    piles 6 of 9 vnodes onto one node while the balanced version spreads them 3/3/3; a property for the
    hard cap (rf=1) and for every vnode receiving `min(rf, nodes)` distinct replicas; determinism; and
    degradation to HRW given slack. It solves **load**, not data loss (rack safety is A1).
- **Dynamic rebalancing**: when membership changes (a node joins or leaves), redistribute the vnodes
  live. Scope: the **control plane** (the members of each vnode's `ra` cluster); adding or removing a
  member (`:ra.add_member`/`:ra.remove_member`) makes **`ra` itself transfer the state** (the Raft log or
  a snapshot), so we never move metadata by hand; the **data plane** (segments) is already covered by
  *healing* (1C-b). The model is **manual** *staged → planned → committed* (riak_core; an automatic
  trigger comes later, on top of the same engine). Decomposed into:
  - ✅ **R1, `desired_placement` (the pure core).** `Malachi.Application.desired_placement/5` recomputes
    the desired placement over an arbitrary node set (the **live** membership, as opposed to the static
    `:log_nodes` config): it composes `sharded_vnodes/2` (the fixed logical vnodes) with `place_vnodes/4`
    (HRW). Deterministic and **minimal movement**: a vnode only changes if it **adopts** a node that
    joined or **held** one that left; everything else stays put. Tested: determinism; on **adding** a
    node, a vnode only changes if it adopts the new one (and some do); on **removing**, only the holders
    change (and the node disappears from the placement); plus the clamp to `min(rf, |nodes|)`.
  - ✅ **R2: the rebalancing plan (the pure core).** `Malachi.Application.rebalance_plan/2` diffs the
    **current** placement against `desired_placement` (R1) per vnode: for each vnode whose node set
    differs it returns `%{vnode_id:, add:, remove:}` (the nodes to join or leave that `ra` cluster);
    vnodes that are already correct are omitted (an empty plan means nothing to do). *Staged/planned*: it
    computes without applying. The safe order is **add before remove** (R3 adds first, so a vnode never
    drops below quorum mid-change; with a constant RF, `add` and `remove` are the same size). It assumes
    the **same set of vnode ids** in current and desired (changing the count is re-sharding, out of
    scope). Deterministic (it follows the desired order). Tested: empty when nothing changes; add and
    remove per changed vnode (omitting the unchanged); on a *join* the RF stays constant (add and remove
    balanced, so never below quorum); and on a *leave* only the vnodes that held the departing node enter
    the plan, and none re-adds the removed node.
  - **R0. A lease over `ra`** (strong fencing, the k8s way): a prerequisite for R3 (moving vnodes is
    **non-idempotent**, which is where the lease finally comes in, as 1C-b anticipated).
    - ✅ **R0-a: the lease state machine (pure core plus `ra`).** `Malachi.Cluster.Lease` is the pure state
      (`holder`, `fence`, `renew_at`, `duration_ms`): `acquire_or_renew` grants it when the lease is
      **free**, when the caller **already holds it** (a renewal) or when it has **expired**
      (`now >= renew_at + duration_ms`), otherwise `{:error, {:held, holder}}`; `release` is idempotent.
      The **fencing token** (`fence`) is monotonic and rises **only when the holder changes** (a renewal
      keeps it): the holder carries it into the work it fences, so a former holder writing with a stale
      token can be rejected, which is the protection against two bosses. Time (`now`) is **injected**,
      never read inside `apply` (which would be non-deterministic and would break Raft): `LeaseMachine`
      (a `:ra_machine`) is fed by `ra`'s `meta.system_time` (the **leader's** clock, stamped once and
      replicated in the log), so a single clock decides expiry, without the inter-node skew a
      client-supplied time would carry. `LeaseServer` mirrors `MetadataServer` over a **dedicated** `ra`
      cluster (isolated from the metadata). Tested: the pure `Lease` exhaustively (acquisition, a renewal
      keeping the fence, a steal on expiry incrementing it, the exact deadline boundary, an idempotent
      release with a stale token) plus real `ra` integration (acquire, renew, held, release, and
      durability across a restart).
    - ✅ **R0-b: `LeaseHolder` (the client).** A GenServer running the timer triangle
      `duration > renew_deadline_ms > retry_period_ms`: every `retry_period` it calls the `renew` seam
      (acquire-or-renew); a *follower* that acquires becomes *leader* and calls `on_acquired(fence)`; a
      *leader* that renews stays leader (stamping the renewal instant on the **local clock**); if it is
      told the lease is held by **someone else** it drops immediately (`on_lost`); and if it **cannot
      reach** the lease it keeps trying until `renew_deadline_ms` has passed since the last successful
      renewal, then **drops proactively** (`on_lost`, the k8s *OnStoppedLeading*: giving up before the
      lease could expire or be stolen, so there are never two leaders). A jump in the **fencing token**
      during leadership (a gap where it lost and regained) fires `on_lost` followed by `on_acquired` under
      the new token. On a normal shutdown, a leader **releases** the lease (failover without waiting for
      expiry). All of it through **injected seams** (`renew`/`release`/`clock`/callbacks), so the timing
      logic is tested without `ra`, by controlling the clock. Tested: acquiring makes it leader; renewing
      does not re-fire `on_acquired`; it holds until the deadline then drops; it drops immediately when
      held by another; a token change yields lost then acquired; and a leader releases on shutdown (while
      a follower does not). **R0 is complete.**
  - **R3. Execution** (*committed*): applies the plan per vnode through `ra`, **under the lease**. Scope:
    the control plane (`ra` transfers the state when a member is added); the data plane stays with
    *healing*.
    - ✅ **R3-a: the single-change executor (a core with seams).** `Malachi.Cluster.Rebalance`:
      `apply_change/3` applies every `add` **before** every `remove` (add-before-remove, so the vnode
      never drops below quorum) through the `add_member`/`remove_member` seams
      (`(vnode_id, node -> :ok | {:error, _})`); it is **idempotent** (adding an existing member or
      removing one that already left is `:ok`, so an interrupted commit is re-runnable) and **fail-fast**
      (an `add` that fails does **not** attempt the removes, protecting the quorum). `apply_plan/4`
      applies the plan change by change, fail-fast across vnodes (stopping at the first error and
      returning `{:error, {applied, failure}}`), and **re-checks `leader?` before each change** (stopping
      with `:lost_leadership` if the holder released the lease midway). Tested (with seams that record
      the order): add before remove; a failing add does not remove; an error on remove is reported;
      idempotence; a complete plan; fail-fast across vnodes; and stopping on lost leadership.
    - **R3-b, coordenador plan/commit + ops `ra`/wiring.**
      - ✅ **R3-b-i: planning from live state (a core with seams).**
        `Malachi.Application.readable_placement/2` builds the **current** placement from the vnodes' `ra`
        memberships through the `members_of` seam (`vnode_id -> {:ok, nodes} | {:error, _}`),
        **omitting** unreadable vnodes (conservatively: never plan for a vnode we cannot see).
        `Malachi.Application.live_rebalance_plan/5` is the *plan*: it diffs the current (readable)
        placement against the **desired** `place_vnodes` over the **live** nodes, for the readable vnodes
        only, and returns the plan (`rebalance_plan/2`) that feeds `Rebalance.apply_plan/4` (the *commit*,
        under the lease). It sits alongside R1 and R2 in `Application` (avoiding a cycle with `Rebalance`,
        which only executes). Tested: `readable_placement` omits the unreadable; `live_rebalance_plan` is
        the current-versus-desired diff over live nodes; empty when they already match; and it never plans
        for an unreadable vnode.
      - ✅ **R3-b-ii: the real `ra` ops plus the coordinator (the engine).** `Rebalance.ra_add_member/3` and
        `ra_remove_member/3` are `apply_plan`'s real seams: **add** is `add_member` (announcing the member,
        which need not even be running) **followed by** `start_server` on that node through `:erpc` (the
        order `ra`'s documentation prescribes; the leader then replicates the log or a snapshot to the new
        member); **remove** is `remove_member` followed by `stop_server`. Both are **idempotent** (a nested
        `already_member`/`not_member`/`already_started` becomes `:ok`) and **retry** on
        `:cluster_change_not_permitted`: `ra` permits only **one** membership change at a time, so the
        add-then-remove within a single change (and repeated ops) wait for the previous one to settle.
        `RebalanceCoordinator` (a GenServer with `plan_fun`/`add_member`/`remove_member`/`leader?` seams)
        exposes `plan/1` (computing without applying) and `commit/1` (**refusing with `:not_leader`** when
        it does not hold the lease; otherwise a fail-fast `apply_plan`, passing the same `leader?` so it
        stops if the lease drops midway). Commit is **always manual**. Tested: the coordinator through
        seams (plan, commit, refusal, fail-fast) plus a real **`:multinode`** run where `ra_add_member`
        grows a vnode onto a new node and `ra` transfers the state, `ra_remove_member` shrinks it, and both
        are idempotent.
      - ✅ **R3-b-iii: the wiring ("plugging it in").** When **sharded**, `Application.log_children` adds
        `rebalance_children`: it bootstraps the `LeaseServer` (a dedicated `ra` cluster,
        `Malachi.LogLease`, **self-fencing** at boot, since every node calls and one forms it) and starts
        the `LeaseHolder` (`Malachi.LogLeaseHolder`, with the default 15s/10s/2s triangle through
        `lease_duration_ms`/`lease_renew_deadline_ms`/`lease_retry_period_ms`, and real `renew`/`release`
        over the `LeaseServer`) plus the `RebalanceCoordinator`
        (`Malachi.LogRebalanceCoordinator`) with the real seams: `plan_fun` is `live_rebalance_plan`,
        `add_member`/`remove_member` are `Rebalance.ra_*` resolving members through `try_members/2` (which
        tries each node as an entry point, since the holder may not host the vnode and any member routes
        to the leader), and `leader?` is `LeaseHolder.leader?`. `LeaseHolder.leader?/1` was added (reading
        the role without forcing a tick). The **commit stays manual** (an operator calls
        `RebalanceCoordinator.plan/1` and `commit/1`); the `LeaseHolder` merely keeps the election running
        (the k8s way). The **non-sharded path is unchanged**. Tested: `leader?/1` and `try_members/2` in
        isolation plus the full suite (1048 tests) green, showing boot does not regress; the ops'
        behaviour rests on R3-b-ii's `:multinode` run. **Dynamic rebalancing is complete: phase 1
        (distribution) closes here.**
      - ✅ **Lease reconcile (hardening).** The `self-fencing` bootstrap above forms the lease cluster with
        only a **majority** (Raft); a node that was down when the cluster formed remains a member of the
        **config** (the initial `start_cluster` lists every node) but with no server running, which reduces
        the lease's fault tolerance. `LeaseServer.reconcile/2` performs a **self-join**, best-effort and
        idempotent: it re-attempts to form the cluster (`start/2`, self-fencing) and to start the **local**
        server (`:ra.start_server`), which rejoins the existing cluster (it is already a config member) and
        `ra` replicates the lease state to it. `Malachi.Cluster.LeaseReconciler` (a generic GenServer with
        a `:reconcile` seam) calls it after boot and every `lease_reconcile_interval_ms` (default 30s),
        *level-triggered*, keeping `LeaseHolder` free of `ra` and membership concerns. It is started in
        `rebalance_children` (first, before the holder). Tested: the reconcile bootstraps when nothing has
        started and is an idempotent no-op against a formed cluster (it does not disturb the lease); and
        the reconciler reconciles both at boot and on demand.
    - ✅ **Dynamic node discovery (libcluster). Connectivity only.** This closes an operability gap: peer
      discovery used to be **static** (`MALACHIMQ_LOG_NODES` plus a manual `Node.connect` or fixed
      hostnames). The `{:libcluster, "~> 3.5"}` dependency plus an **optional** `Cluster.Supervisor` in the
      tree (only when `MALACHIMQ_CLUSTER_STRATEGY` is set; absent means single-node, requiring no
      distribution, so the default is untouched). Decision (**1A**): **connectivity only**, so libcluster
      merely discovers and connects nodes (Erlang distribution); SWIM and `ra` keep using `log_nodes` for
      the initial *member set*, and `ra` membership changes still go through the **R3 (rebalancing under
      the lease)** already built, rather than duplicating Raft formation in a less safe way. Strategies
      (**2A**): `gossip` (UDP multicast, for dev and LAN), `kubernetes` (discovering pods through the API;
      `selector` and `node_basename` are required) and `epmd` (a static list reusing `log_nodes`). A
      **pure** `Malachi.Cluster.Topology.build/1` module maps config to topologies (fail-fast: it raises on
      a missing required field), unit-testable without opening a multicast or k8s socket. The env is parsed
      in `runtime.exs` (`log_nodes` extracted into a binding, reused by `epmd`). Tested: `build/1` per
      strategy (defaults, required fields, nils omitted, an unknown one raising): 12 tests; plus a real
      boot smoke test (the default yields no `ClusterSupervisor` and no distribution; `gossip` plus
      `--sname` yields a live `ClusterSupervisor`). Full suite at 715 tests, 0 failures (boot does not
      regress); credo and dialyzer clean. The README gained a node-discovery section. **The multi-node
      operability slice is closed.**
    - ✅ **Placement hardening: a fault-domain guarantee (`min_domains`/`policy`).** `Placement` already did
      rack and DC spread plus `max_skew`, but `:spread` is **best-effort**: with fewer domains than `rf`, or
      with attributes missing, replicas concentrated **silently**, which is an HA hole (3 replicas in the
      same rack survive zero rack failures). Decision **1A**: `:hard` **fails fast on initial placement**,
      while heal stays best-effort (durability first) and reports. **The (pure) core**: `place/4` gains
      `:min_domains` (the minimum number of distinct attribute values the replica set must cover; without
      `:spread`, distinct brokers) plus `:policy` (`:soft`, the default, being current behaviour; `:hard`
      returning `{:error, {:insufficient_domains, covered, required}}`). A broker with no attribute falls
      into a single `nil` domain (conservatively: it does not count as an extra domain). A new
      `domain_violations/4` reports the segments whose replica set covers fewer than `min_domains` domains
      (for alerting and observability). **The wiring**: the broker (`min_domains` and `placement_policy` in
      the struct and in `open`; `place_opts` injects them; `open_segment` handles `{:error, ...}` so a
      produce aborts cleanly, with `register_segment` extracted), broker_server (threading them through),
      application (`data_plane_opts` reads `log_min_domains`/`log_placement_policy`) and config
      (`MALACHIMQ_LOG_MIN_DOMAINS`/`MALACHIMQ_LOG_PLACEMENT_POLICY`). **A related fix (Issue 2)**: heal was
      **rack-blind**, since `self_healing` called `place/3` without `:spread`; now `HealCoordinator`
      resolves the spread per pass (through `heal_spread/0`, using live attributes) and `self_healing`
      forwards **only `:spread`** (stripping `min_domains` and `policy`, so heal never hard-fails). Tested:
      `place/4` min_domains and policy (soft and hard, met and unmet, without spread, the nil domain) plus
      `domain_violations/4` (5+3); the broker hard-fail e2e (a produce aborts with 2 racks and min_domains
      3; soft places it; hard passes with min_domains 2 and 3); and rack-aware heal forwarding `:spread`
      (1). Suite at 727 tests, 0 failures; credo and dialyzer clean. The README gained the env vars.
      - ✅ **Surfacing `domain_violations` (a metric plus the panel).** This closes the "report" half of 1A:
        `domain_violations/4` was a pure function **nothing called**, so under the **soft** policy an
        operator was blind to HA degradation. `Broker.domain_violations/1` (pure) computes, from the broker
        itself (merged metadata plus `spread_by` plus live `broker_attributes` plus `min_domains`), the
        violations **per topic** (`%{topic => count}` through
        `Enum.frequencies_by(&topic_of_segment/1)`; `%{}` when spread or min_domains are not configured);
        `BrokerServer.domain_violations/1` exposes it through a call. The `dashboard` attaches the count to
        each topic in `topics_overview` (defaulting to 0), `Prometheus.export` emits the per-topic
        `malachi_domain_violations` gauge (with a defensive `Map.get(.., 0)`), and the panel shows a
        `⚠ N HA` badge **only when the count exceeds 0** (high signal, no clutter). Tested:
        `Broker.domain_violations` (soft and below target yields 1; at target yields empty; unconfigured
        yields empty) plus the Prometheus gauge (emitted per topic, defaulting to 0 when the key is
        absent): 4 tests. Suite at 731 tests, 0 failures; credo and dialyzer clean; the JS badge validated.
        The README gained the gauge.
    - ✅ **A Kubernetes deploy example (tying libcluster and placement into a real deploy).**
      `deploy/kubernetes/`: a manifest (`malachi.yaml`, 8 documents) for a **3-node CP cluster** with
      rack-aware (zone-aware) placement, plus a README explaining the rationale. Decisions: **1A**
      discovery through **epmd (a static list of FQDNs)**, since a CP/Raft StatefulSet has **stable**
      identities (idiomatic, and the node names match `RELEASE_NODE`, so it is deterministic, which matters
      given there is no real cluster to test against); **2A** genuinely rack-aware, through an init
      container. The pieces: a **StatefulSet** (stable identity, DNS and PV for `ra`;
      `podManagementPolicy: Parallel` so the quorum forms together), a **headless Service**
      (`publishNotReadyAddresses` so peers resolve each other during formation), a **client Service**
      (4040/4041), a **PDB** with `minAvailable: 2` (preserving the Raft majority through drains), and a
      least-privilege **ClusterRole** (`get nodes`) with an SA and binding for the init container. The
      distributed node name comes from
      `RELEASE_NODE=malachi@$(POD_NAME).malachi-headless.$(POD_NAMESPACE)...` plus
      `RELEASE_DISTRIBUTION=name` plus `ERL_AFLAGS` pinning the distribution port; `ra`'s peer set is the 3
      FQDNs in `MALACHIMQ_LOG_NODES`; and `MALACHIMQ_CLUSTER_STRATEGY=epmd` reuses that list.
      Rack-awareness: `topologySpreadConstraints` by zone plus an init container (`kubectl get node`) that
      writes the zone into an emptyDir, which the main container folds into
      `MALACHIMQ_LOG_ATTRIBUTES=zone=<z>`, with `LOG_SPREAD_BY=zone`, `MIN_DOMAINS=2` and
      `PLACEMENT_POLICY=soft` (violations surface through the previous slice's gauge; a node without the
      label falls back informatively). Probes `/health` and `/ready` (from O1). No Elixir code changed (the
      node name comes through `RELEASE_*`, and `vm.args` already uses `inet_res`). Validated: the YAML
      parses (8 documents) plus a spot check of the critical values (the env ordering with POD_NAME before
      RELEASE_NODE, the `$(VAR)` references, the collapsed command). The `kubernetes` alternative (dynamic,
      with RBAC over pods) is documented for autoscaling deploys. The main README points at it. (Not
      testable without a real k8s cluster; the config is deterministic by construction.)
    - ✅ **TLS on inter-node Erlang distribution (G3).** This closes a production security gap: metadata
      (`ra`) and data replication travelled in **plaintext** between nodes (only the cookie authenticated).
      Decision **1A**: **mutual TLS** (`verify_peer` plus `fail_if_no_peer_cert`), a shared CA and a
      per-node certificate, which both encrypts **and** authenticates. It is VM and release config (not
      Elixir code): `rel/env.sh.eex` translates `MALACHIMQ_DIST_TLS=true` into
      `-proto_dist inet_tls -ssl_dist_optfile $MALACHIMQ_DIST_TLS_OPTFILE` (through `ELIXIR_ERL_OPTIONS`),
      **failing fast** if the optfile is missing or unreadable; the default is off, leaving today's
      plaintext untouched. Artefacts: `rel/dist_tls.conf.example` (an ssl_dist optfile template), a dev
      helper `scripts/generate-dist-certs.sh` (a CA plus a node certificate with server and clientAuth EKUs,
      emitting a ready optfile), and a `.gitignore` entry for `priv/dist_cert/` (never commit keys). Wired
      into the k8s example (a `malachi-dist-tls` Secret carrying the ca, the node cert and key and the
      optfile, mounted read-only at `/etc/malachi/dist`, plus 2 env vars). **Genuinely validated locally**
      (unlike the k8s parts): 2 BEAM nodes over dist TLS ping each other (`:pong`), and a node **without**
      TLS is **rejected** at the handshake (`:pang`, which proves TLS is enforced rather than silently
      falling back to plaintext); the 3 paths through `env.sh.eex` (off, on, fail-fast); the optfile is a
      valid Erlang term (`:file.consult`); and the k8s YAML parses (9 documents). No Elixir code changed;
      the README gained an inter-node TLS section, as did the deploy README.
    - ✅ **Graceful shutdown / rolling upgrade (G4).** The old `prep_stop` **closed everything at once**
      (no quiesce, no window, so in-flight work was cut off, and it raced with new accepts during the
      close). Decision **1A** (a bounded window: the right model for a broker with **streaming**, where
      draining to zero connections never converges). A new `Malachi.Shutdown.graceful/1` orchestrates 3
      steps: **quiesce** (a `terminate_child` of `TCPAcceptorPool` on the root supervisor, so it stops
      accepting and does **not** restart; the connections, which are unlinked `spawn`s registered in
      `ConnectionRegistry`, survive) → **drain** (sleeping `shutdown_grace_ms`, default 5s, the window for
      in-flight work to finish) → **close** (`close_all`). The lease is already released by
      `LeaseHolder.terminate` in the teardown that follows (making failover quick) and `ra` persists to
      disk (so the pod comes back and rejoins as the same member). The steps are **seams**, so the
      orchestration (order plus window) is unit-testable without stopping the real app. On k8s:
      `terminationGracePeriodSeconds: 40` plus a `preStop` (`sleep 5`, since kube-proxy removes the pod
      from the Service endpoints **before** the SIGTERM, so clients stop being routed before the drain).
      Config: `MALACHIMQ_SHUTDOWN_GRACE_MS`. Tested: `graceful/1` runs quiesce → sleep(drain_ms) → close
      **in order**; it skips the sleep with `drain_ms: 0`; and the default comes from config: 3 tests.
      Suite green; credo and dialyzer clean. The README gained the env var, and k8s the grace and preStop.
    - ✅ **Consumer group coordination (G1: an epic, sliced; S1 to S5 plus Str-1/Str-2 complete).** A group
      used to be a **single shared position** (every consumer read the same committed position, with no
      parallelism). The NorthGuard/Kafka target is **reached**: each of the topic's **ranges** is assigned to
      **exactly one** group member, consumption runs in parallel, and it rebalances on join and leave, all of
      it **server-internal and opaque** (the client never sees a range). The finding that grounded the
      design: `commit_offset` did a `Map.put` (replacing the `{group, topic}` offset map), so it became a
      **per-range merge** (S2). The slicing: **S1** the assignment core (pure) · **S2** per-range commit ·
      **S3** the coordinator (membership plus heartbeat/session, exposing the assignment) · **S4** server
      integration (fetch respects the assignment, opaquely) · **S5** the wire protocol plus the client ·
      **Str-1/Str-2** member scoping for streaming (server-side push plus wire and client with a heartbeat).
      **What remains (its own slice, not G1): the coordinator is currently a single local GenServer, so
      multi-node routing and replication of the membership is left to a cluster-wiring slice.**
      - ✅ **S1, the assignment core (pure).** `Malachi.Consumer.Assignment.assign(range_ids, members)`
        returns `%{member => [range_id]}`, each range under **exactly one** member, **deterministically**
        (ranges sorted into a canonical order, so a replicated coordinator computes the same thing on every
        node after a failover). Decision **1A (sticky HRW)**, but **corrected by empirical measurement**
        (the habit of measuring before deciding): the option said "reuse `place_balanced`", yet the property
        test revealed it is **not strongly sticky** (at small N the rebalance-toward-the-cap moves surviving
        members' ranges: with 4 ranges over 4 members, removing 1 moved 2 of the 3 survivors' ranges),
        contradicting the *sticky* priority. It was switched to **plain HRW**
        (`Placement.place(range, members, 1)` per range, taking the top-HRW member), which gives **strict
        min-reshuffle**: a leave moves **only** the departing member's ranges (survivors keep **all** of
        theirs), and a join moves ranges **only** to the new member (existing ones only lose, never swap
        between themselves). That is the stickiness "sticky HRW" promises, with **statistical** balance
        (optimal with many ranges, which is the NorthGuard case). It still reuses `Placement.place` (the HRW
        ranking). Tested (as properties): partitioning (each range exactly once), determinism under
        shuffling, **sticky-on-leave** (survivors keep everything), **sticky-on-join** (unchanged or the new
        member), plus edges (no members yields `%{}`, no ranges leaves members idle, dedup). Suite at 746
        tests, 0 failures; credo and dialyzer clean.
      - ✅ **S2. Per-range commit (a merge).** `Metadata.apply({:commit_offset, group, topic, offsets})`
        stopped doing a `Map.put` (which replaced the whole offset map for `{group, topic}`) and now does
        **`Map.update` plus `Map.merge`**: it merges the received offsets per range (last-commit-wins per
        range). That way a member of a partitioned group commits **only the ranges it owns**, without
        erasing other members' range positions. This is the prerequisite S1 identified. Backward
        compatible: a single consumer committing the full map behaves identically (the merge covers
        everything), and the existing tests (which commit one range, or the same one twice) pass unchanged.
        A single path (`DSRSM` routes by topic to `Metadata.apply`; `merged_metadata` unions disjoint
        topics). A recorded tradeoff: the merge leaves **stale** keys when a range splits or merges (the old
        range's offset persists), but it is **bounded** by the keyspace (at most ~2^keyspace_bits historical
        range ids) and **harmless** on read (a fetch only consumes current ranges; dead ones are ignored); a
        prune (reusing the `topic_ranges` index) is left as a future optimization, not S2. Tested: a new
        merge test (two members, one commits only its range, and the other's is preserved) plus the existing
        ones (last-wins per range). Suite at 747 tests, 0 failures; credo and dialyzer clean. **Next: S3
        (the coordinator: membership plus heartbeat, exposing S1's assignment).**
      - ✅ **S3: the group coordinator (membership plus heartbeat plus assignment).**
        `Malachi.Consumer.GroupCoordinator` (a GenServer) tracks the members of each `{group, topic}` and
        assigns the topic's ranges through S1. The API: `join` (adds a member, rebalances, returns
        `{:ok, generation, ranges}`), `heartbeat` (renews the session and returns the current assignment
        plus generation, or `{:error, :unknown_member}` if the member was evicted, meaning it must
        re-join), `leave`, `assignment` (a read without renewing) and `reconcile_now` (running a
        synchronous tick, a test seam). **Eager (decision 1A)**: any membership change (join, leave,
        eviction) or range change recomputes the whole assignment and **bumps the `generation`** (an epoch,
        Kafka style): the member re-reads on its heartbeat, sees the new generation and takes over again;
        it is **level-triggered** (the bump only happens if the assignment actually changed, so the tick is
        idempotent). Session timeout: a member silent for `session_ms` is **evicted** during the reconcile
        (a periodic tick) and its ranges reassigned; a group with no members is **dropped** (no state
        leak). This is only the coordinator's **logic**, as a single instance, with seams
        (`clock`/`ranges_fun`) so it tests without a cluster; **cluster routing** (which node coordinates
        which group, where Kafka hashes group to broker, or replicating the membership) is left to the
        wiring slice. Member state is **soft** (on restart, members re-join). Tested: a lone member takes
        everything (generation 1); two members partition it (disjoint and complete, with the generation
        advancing); a leave returns the ranges; **eviction** by session timeout plus a mandatory re-join;
        an empty group dropped; a range change rebalancing on reconcile; the reconcile being idempotent
        (no change means the same generation); and a heartbeat from an unknown member rejected: 8 tests.
        Credo and dialyzer clean.
      - ⚠️ **A course correction (NorthGuard fidelity) before S4.** Review pointed out that S1 through S3
        had imported the **Kafka model** (a `range_ids` assignment **visible to the client**). That would
        **violate** the project's central principle (doc §B: *"the client contract is the NorthGuard way,
        NOT Kafka's: … an opaque cursor … never sees a partition or offset"*). Ranges are NorthGuard's
        equivalent of partitions, so they must stay **hidden**. The S1 to S3 machinery is **server-side and
        correct** (the coordinator computes the assignment internally); the `[range_ids]` it returns is an
        **internal detail** and never reaches the wire. The S4/S5 plan was **readjusted to be opaque**: the
        server scopes the fetch to the member's assignment and returns records plus an **opaque cursor**;
        the client **never** sees a range id. Membership becomes **implicit through the fetch** (the fetch
        is the heartbeat), with an explicit `leave` later.
      - ✅ **S4: server-side partitioned consumption (opaque, in-VM).** `GroupCoordinator.poll/4` is the
        fetch's entry point: it registers the member if new (rebalancing) or merely renews the session if
        known (no rebalance), and returns that member's ranges, so a member stays alive by **fetching**,
        with no separate heartbeat. The `consume` path gained a **`ranges`** filter (`consume_ranges/5` plus
        `selected_ranges/3`): `nil` means every active range (the whole group, or a single consumer, which
        is the existing behaviour); a list means **only** those, **intersected with the active ones** (so an
        assigned range that has since split is skipped). It is threaded through `BrokerServer.consume/6`
        plus the `handle_call({:consume})` (now a 6-tuple) plus the long-poll waiter (which stores
        `ranges`); the streaming subscriber is unchanged (it uses the `nil` default). A new
        `LogApi.fetch_member/7`: `poll` the coordinator → the member's ranges → a consume scoped to the
        committed positions (S2) → returning records plus an **opaque cursor** (the client never sees a
        range id; the `commit` advances only that member's ranges). Backward compatible: a `fetch_group`
        without a member is still the whole group. Tested: `poll` (registering a new member, a known
        member's heartbeat, re-registering an evicted one: 2) and **in-VM e2e integration** (a topic with 2
        ranges through a split, where 2 pre-registered members fetch **disjointly and completely**, each
        record going to exactly one member; plus `fetch_group` backward compatibility): 2. Suite at 759
        tests, 0 failures; credo and dialyzer clean.
      - ✅ **S5: the wire plus the client (opaque). Consumer group coordination complete.** This exposes
        S4's partitioned consumption over the binary protocol **without leaking a range id**. **The wire**:
        `fetch_req` gains a **member id** (`put_str`, after the group; nil means the whole group or a single
        consumer, for backward compatibility), with precedence member (group-scoped) > cursor (client
        paging) > group (resume); plus a new `leave_group` op (api_key 7, carrying `topic/group/member`,
        with an empty ack). `tcp_protocol` dispatches a `fetch` carrying a member to
        `LogApi.fetch_member` (returning `encode_fetch_resp`, records plus an **opaque cursor**, identical
        to a normal fetch: zero range ids on the wire) and handles `leave_group` through
        `GroupCoordinator.leave`. **The wiring**: `GroupCoordinator` joins the tree
        (`Malachi.LogGroupCoordinator`, with `ranges_fun` being `LogBroker`'s `active_range_ids`). **The
        Node client**: `wire.js` and `client.js` (a member on `fetch`, plus `leaveGroup`), and
        `consumer.js` gains a **`--member`** mode (a scoped fetch plus commit plus a `leave` on exit;
        several members of the same `--group` with distinct `--member` ids give parallel consumption). A
        dialyzer finding: `@type api_key :: 0..6` made it infer the `leave_group` (7) branch was dead, so
        it was updated to `0..7`. Tested: a wire round trip (member plus leave_group), **e2e over TCP** (a
        server-scoped member fetch plus an opaque cursor plus records without offsets plus the leave_group
        ack) plus the existing binary suite (backward compatibility of a fetch without a member); and a
        real Node smoke test (produce, then `consumer --member` consuming everything as a lone member and
        leaving; a consumer without a member staying backward compatible). Suite at 761 tests, 0 failures;
        credo, dialyzer and format clean. The README gained the api_key table plus a parallel-members
        example. **G1 (consumer group coordination) is complete** (S1 to S5); recorded as pending: member
        scoping for **streaming** (push) and pruning stale offsets (future slices).
      - ✅ **Pruning stale offsets (S2's debt).** S2's per-range merge left a **dead** key behind on every
        split (the parent range's offset persisted in the group's map). `apply({:commit_offset, ...})` now
        **prunes**, after the merge, down to the topic's **active** ranges (`prune_offsets/3` plus
        `active_range_id_set/2`, reusing V-idx's `topic_ranges` index plus a `state == :active` filter): a
        range retired by a split or merge has its offset **discarded**, which is safe because the active
        children resume from `:start` (a consume only reads active ranges; the at-least-once cross-epoch
        semantics do not change, only the dead key disappears). It is **skipped when the topic is not
        routed** (an offset committed before `create_topic`, where `topic_ranges` has no entry, is left as
        is), preserving the pre-routing behaviour of the existing tests. This bounds the offset map to the
        number of **active** ranges (it previously grew toward ~2^keyspace_bits with splits). Tested: a
        split seals the root, so the root's offset is pruned on the next commit (leaving only the child's);
        and S2's merge and pre-routing tests stay green (no topic means no prune). Suite at 762 tests, 0
        failures; credo, dialyzer and format clean.
      - ✅ **Streaming member scoping: Str-1 (server-side, opaque).** This brings per-group parallel
        consumption to push/subscribe (which was whole-group before). **The architectural constraint** that
        dictated the design: `push_subscriber` runs **inside** the broker's handle_call, but the
        coordinator's `ranges_fun` **calls back into** the broker (`active_range_ids`), so if the broker
        called the coordinator synchronously it would deadlock (each GenServer waiting on the other). The
        rule: **the broker never calls the coordinator.** The design puts all coordination in **`LogApi`**:
        `subscribe_member/7` does the `poll` (registering plus getting ranges) and passes
        `member`/`ranges`/`coordinator` to `BrokerServer.subscribe` (through `group_opts`); the subscriber
        **stores** the ranges and `push_subscriber` scopes the consume with them (`consume_ranges/5`);
        `stream_ack_member/7` re-polls (a heartbeat plus fresh ranges) so the broker **updates** the
        subscriber's ranges on the ack (picking up a rebalance). **Liveness**: the broker's `:DOWN` fires an
        **async `Task`** that calls `coordinator.leave` (async, so it does not block the broker and cannot
        deadlock) for a quick rebalance on disconnect; an idle member stays alive through the client's
        periodic ack (Str-2). Positions are scoped by `Map.take` at subscribe time (as in `fetch_member`);
        and it stays **opaque** (the push is still `{:log_records, records, cursor}`, with zero range ids).
        Tested in-VM: 2 pre-registered members receive **disjoint and complete** pushes (a topic with 2
        ranges through a split); and killing one member's process fires the async **leave**, so the member
        disappears from the coordinator. Suite at 764 tests, 0 failures; credo, dialyzer and format clean.
        **Next: Str-2 (the wire: a member on subscribe and stream_ack, plus a Node subscriber client with a
        heartbeat).**
      - ✅ **Streaming member scoping. Str-2 (the wire plus the Node client).** This exposes Str-1 at the
        edge: an optional `member` joins **subscribe** and **stream_ack** in the binary protocol, after the
        `group`, exactly as in `fetch` (Str-1): `encode_subscribe_req(topic, group, member, window, max)`
        and `encode_stream_ack_req(topic, group, member, cursor, count)` (`put_str(member)` acting as the
        presence flag; `nil` means a whole-group subscription, leaving the old path unbroken). `TCPProtocol`
        dispatches on presence: `subscribe` and `process_stream_frame` call
        `LogApi.subscribe_member`/`stream_ack_member` when `member != nil and group != nil`, otherwise the
        whole-group path, with **zero ranges or offsets on the wire** (the push is still records plus an
        opaque cursor). The Node client: `subscriber.js --member <m>` opens a scoped stream and closes the
        **idle-member liveness gap** Str-1 recorded, through a **periodic heartbeat** (a `setInterval` at
        10s, below the 30s session timeout) that emits an **empty ack** (`cursor` nil, `count` 0) only when
        there has been no recent real ack (`lastAck`), keeping the membership alive; `SIGINT` calls
        `leaveGroup` (for a quick rebalance). `streamAck` gained the `member` in its signature (whole-group
        callers, such as `loadtest.js`, pass `null`). Tests: wire round trips for subscribe and stream_ack
        with and without a member; and e2e over TCP (`log_streaming_test`), where subscribing as a lone
        member receives the whole backlog **opaquely** (a nil offset), a member ack (commit plus heartbeat
        plus credit) is accepted, and a later produce still pushes. Suite at 767 tests, 0 failures; credo,
        dialyzer and format clean. **G1 (consumer groups) plus streaming member scoping are complete.**
    - ✅ **Coordinator cluster wiring (epic COMPLETE: correct multi-node consumer groups; A1 to A5 close it,
      see below).** The gap G1 made explicit: `GroupCoordinator` is a **per-node local** GenServer
      (`Malachi.LogGroupCoordinator`) with in-memory membership. In a cluster, members connected to
      different nodes see **divergent** assignments, so the "each range under exactly one member" invariant
      breaks across nodes. The target: route a topic's coordination to **one** owning node, the way
      NorthGuard routes requests (a broker consults its local view of the sharded metadata, the `HashRing`
      over vnodes, and forwards to the owning vnode). The slicing: **A1** routing plus forwarding · **A2**
      the coordinator on the vnode's leader · **A3** a multi-node test.
      - ✅ **A1: routing the coordinator to the vnode's owning node, plus forwarding.** A new **pure**
        `Malachi.Consumer.CoordinatorRouter` module: `location(topic, topology, this_node, leader_fn)`
        routes `topic → vnode` (through `HashRing`), resolves the vnode's **leader** and decides
        `:local | {:remote, node}`; `ref/2` turns that into a `GenServer` ref (`{name, node}` when remote).
        It routes by **topic** (co-locating coordination with the topic's vnode and metadata, where the
        coordinator's `ranges_fun`/`active_range_ids` resolves against the local broker). It **fails safe to
        `:local`** at every resolution gap: no topology (single-node or in-memory), an empty ring, a vnode
        absent from the map, or an unresolvable leader, having verified that `:ra.members` against a
        nonexistent server **returns `{:error, :noproc}`** (rather than raising), so a momentarily
        unavailable vnode degrades to local instead of failing the request. The static topology (the ring
        plus vnode→server_id) is published **once at boot** of the sharded control plane
        (`with_metadata_authority`) through `:persistent_term` (a lock-free read; absent means single-node,
        so `nil`, so local). `tcp_protocol` resolves the coordinator ref **per request**
        (`coordinator_for/1`) at all 4 sites (subscribe, fetch, stream_ack, leave) and passes it to
        `LogApi`; the resolved ref also becomes `sub.coordinator` (so the `:DOWN`'s async `leave` forwards
        to the owner). **Single-node is unchanged** (resolution misses `:persistent_term` and falls back to
        the local name). Tested (pure, 9): a nil topology, an empty ring, an absent vnode and a nil leader
        all yield local; this node yields local; another node yields `{:remote}`; plus `ref/2` and a
        `put_topology`/`topology` round trip. Suite at 776 tests, 0 failures; credo, dialyzer and format
        clean. **A known limitation (resolved in A2):** `sub.coordinator` was the ref resolved **at
        subscribe** and was not refreshed on acks, so after a leadership change the `:DOWN`'s `leave` would
        go to the old leader. **Next: A2 (failover consistency) → A3 (a `:multinode` test).**
      - ✅ **A2 (part A). Failover consistency: refreshing the coordinator on the ack, plus an ownership
        guard.** This closes A1's limitation and hardens the failover window, with **soft membership** and
        **coordinator = the vnode's leader**, both **confirmed by the NorthGuard transcript in the repo**
        (`northguard_meetup_transcript.txt`: *"this coordinator is the leader of a given VNode... manages
        all the metadata owned by VNode"*; Xinfra's Conductor does client management by connection and
        heartbeat, and only offsets and checkpoints are durable). Decision **A2-A** (focused on
        correctness; the "run only on the leader through `VnodeCoordinatorManager`" lifecycle goes into A3,
        alongside the multinode test that exercises it, since observable behaviour is identical, making B a
        fidelity-of-internal-detail matter that is only testable multi-node). **Part 1, the refresh:**
        `BrokerServer.stream_ack/7` gains a `coordinator` parameter, and `handle_call` updates
        `sub.coordinator` (as well as `sub.ranges`), so after a leader change `stream_ack_member` (which
        already re-resolves the fresh leader in `tcp_protocol`) records the new ref and the `:DOWN`'s async
        `leave` reaches the **current owner**. **Part 2, the guard:** `GroupCoordinator` gains an
        `owns_fun` seam (defaulting to `fn _ -> true end`; at boot, `CoordinatorRouter.owns?/1`), and
        `join`/`poll` reject with `{:error, :not_owner}` **without** registering when this node does not
        lead the topic (defending against stale routing in the failover window, so there is no phantom
        assignment). `LogApi` (subscribe, fetch, stream_ack for a member) propagates the `:not_owner`, and
        `tcp_protocol.subscribe` answers an error rather than entering a stream (the client re-resolves and
        re-subscribes); heartbeat and fetch **self-heal** on the next request, since routing is per request.
        Single-node: `owns?` is always `:local`, so it never rejects and nothing changes. Tested: the guard
        (poll and join yielding `:not_owner`; no phantom registration; a per-topic `owns_fun`: 3) plus the
        refresh (an ack through a different coordinator, so the `:DOWN` leave reaches the new one: 1). Suite
        at 780 tests, 0 failures; credo, dialyzer and format clean. **Next: A3 (`:multinode` validation).**
      - ✅ **A3. `:multinode` validation of the routing (A1 plus A2 against real `ra`).** This proves the A1
        and A2 machinery across real BEAM nodes (a `:peer` plus `:erpc` harness, like
        `rebalance_multinode_test`): it starts 3 peers, forms one vnode's `ra` cluster (quorum 2, tolerating
        1 failure), publishes the topology (`put_topology`) on each node, and checks against **live `ra`
        leadership**: (1) `owns?` and `resolve` agree, so on the leader `owns? == true` and `resolve`
        returns the local name, while on each follower `owns? == false` and `resolve` returns
        `{name, leader}`; (2) **cross-node forwarding**, where a coordinator on the leader takes two members
        (polled from the primary node through `{name, leader}`) and the assignment is **disjoint and
        complete**; (3) the **guard**, where a follower rejects a `poll` with `{:error, :not_owner}`; and
        (4) **failover**, where killing the leader's `ra` server (`:ra.stop_server`) makes the remaining 2
        elect a new leader and `owns?`/`resolve` **reconverge** on it. Details that grounded the test
        (recorded for A4): the topology's `server_id` points at a **probe** (a follower that never dies) so
        `:ra.members` can resolve the leader even after the failover; the coordinator on a peer starts
        through an **unlinked** `GenServer.start` (the `:erpc` worker dies and would take a linked child
        with it); and `ranges_fun` is a **module capture in `test/support`** (`&Fixtures.ranges/1`), because
        an anonymous fun from a `_test.exs` is not resolvable on the peer (it is not on the code path, and
        the MD5 differs). `@moduletag :multinode` (excluded by default; run it with `--include multinode`).
        Suite at 780 tests, 0 failures (plus 1 excluded multinode); credo, dialyzer and format clean.
        **G1/coordinator: A1 plus A2 plus A3 close the multi-node correctness of consumer groups. Next: A4
        (the lifecycle, a per-vnode coordinator on the leader through `VnodeCoordinatorManager`, revalidated
        by this test; plus a Node client retry on `:not_owner`).**
      - ✅ **A4: a per-vnode coordinator on the leader (the lifecycle, NorthGuard's "coordinator = the
        vnode's leader" model).** Before (A1 to A3) **every node** ran a single `GroupCoordinator` and the
        routing sent the client to the owner; now, on a **sharded** control plane, each vnode runs **its
        own** coordinator **on its leader**, managed by the `VnodeCoordinatorManager` that already starts
        per-vnode heal and retention, starting and stopping on the leadership gate
        (`MetadataServer.leader?`). The gains over A1 to A3: fidelity, **crash isolation** (one vnode does
        not take the others down) and a **cleaner failover handoff** (the manager stops it on the old leader
        and starts it fresh on the new one). **Naming**: the per-vnode coordinator registers under
        `CoordinatorRouter.coordinator_name(base, vnode_id)` (a `Module.concat`, so a per-vnode local name;
        not `:global`, which stays consistent with the system's explicit metadata routing), and `resolve/2`
        now **derives** that name from the routed vnode (single-node without a topology falls back to the
        base name). A refactor: `route/2` was extracted (DRY between `location` and `resolve`); the single
        `Malachi.LogGroupCoordinator` became **conditional** (non-sharded only, in `coordinator_children`);
        and `group_coordinator_vnode_child/1` joins `start_vnode_coordinators/1` (with an id in the
        per-vnode supervisor, and `owns_fun` as a defence during a flap window). `tcp_protocol` is unchanged
        (it still passes the base name to `resolve`). **Single-node is unchanged** (a boot smoke test: the
        base coordinator is registered, and `resolve` without a topology gives the base). A3's `:multinode`
        test was **revalidated** with the per-vnode names (`coordinator_name`). Tested: a pure
        `coordinator_name/2`; multinode green 2x; single-node boot. Suite at 781 tests, 0 failures (plus 1
        for `coordinator_name`); credo, dialyzer and format clean. **Next: A5 (a Node client retry on
        `:not_owner`, for client resilience during the failover window).**
      - ✅ **A5. Node client resilience on `:not_owner` (the failover window).** This closes A4's client
        item: when a member request is forwarded to a coordinator that has just lost the vnode's leadership,
        the server answers `:not_owner` (A2's guard); it is **transient** (the server re-resolves the
        current leader on the next request), so the client should **retry** rather than fail. Node-only (no
        Elixir): `client.js` exports `isNotOwner(err)` (a `MalachiError` whose message is `"not_owner"`) and
        `cli.js` gains `sleep/1`. `consumer.js` (member fetch) wraps the fetch in a **try/catch** and, on
        `:not_owner`, backs off 200ms and `continue`s (retrying, printing `~`). `subscriber.js` (member
        subscribe) restructures the subscribe into a `startStream()` and, in `onError`, **re-subscribes**
        after 200ms against the new owner on `:not_owner` (instead of exiting). `stream_ack` (the heartbeat)
        was already fire-and-forget and self-heals on the next ack, so it needed no change. Validated:
        `node --check` on the scripts plus a sanity check of `isNotOwner` (a non-owner error, a different
        reason, and a non-Malachi error). No JS test harness (the standard for previous Node client
        slices). **A1 through A5 close the coordinator cluster-wiring epic: consumer groups that are correct
        and resilient across nodes, faithful to NorthGuard.**

### 8.4 Adoption status and deliberate deviations (a retrospective)

The riak_core and k8s ideas above have **already been absorbed** by the phase 1 and 3 slices. What was
adopted, and what was deliberately **not** adopted (with the reason):

**Adopted (with the slice that delivered it):**
- **Fencing through consensus**: a bootstrap self-fenced by the **`ra` cluster's name**, plus a **Lease
  over `ra`** for the leader's continuous work (R0): the `duration > renew_deadline > retry_period`
  triangle, a versioned fencing token (CAS), **proactively** giving up (k8s's *OnStoppedLeading*) and the
  **leader's clock** (not the client's: stamped once and replicated in the log, so there is no clock
  skew). [k8s Lease + RabbitMQ/`ra`]
- **Staged → planned → committed**: R1 (`desired_placement`) → R2 (`rebalance_plan`) → R3 (`apply_plan`
  under the lease). [riak_core's claimant]
- **Election by the lowest live node plus fencing**, `membership_leader` / `LeaseHolder`. [both]
- **Level-triggered, idempotent reconcile**: the coordinators (heal, retention, rebalance, lease)
  reconcile **desired versus actual**, idempotently, with only the leader acting. [k8s controller]
- **Deterministic, rack- and DC-aware placement**: A1 (`spread`) plus A2 (`maxSkew` through
  `place_balanced`) plus `min_domains` and hard-versus-soft (the placement hardening slice), **without
  randomization** (raft-safe: every replica computes the same). [k8s PodTopologySpread
  `topologyKey`/`maxSkew`/`minDomains`/`whenUnsatisfiable` + riak_core binring `target_n_val`]
- **Minimal movement**: HRW is *min-reshuffle* (removing a broker only moves what it held; survivors keep
  their rank); R1 and R2 only move affected vnodes; and `heal` preserves live replicas. [riak_core]
- **Add-before-remove** during rebalancing (the quorum never drops mid-change). [riak_core/`ra`]

**Deliberate deviations / not adopted:**
- **workqueue plus expectations** (the k8s controller): **not** adopted. The coordinators are
  **synchronous and per tick** (plain level-triggered), with no queue carrying dedupe and rate limiting,
  and no TTL-based tracking of in-flight operations. The justification: reconcile cardinality is low (a
  few vnodes per tick), `ra` already **serializes** membership changes (one at a time, retrying on
  `:cluster_change_not_permitted`), and the rebalancing commit is **manual**, so there is none of the
  event explosion that motivates workqueue and expectations in k8s. Revisit **if** an automatic
  rebalancing trigger is added.
- **Sticky preference in `heal`** (k8s): no dedicated code was needed, because HRW **inherently** prefers
  surviving replicas (min-reshuffle). The same property, for free.
- **`target_n_val`, "distinct nodes AND distinct locations"** (riak_core): covered by **composition**
  rather than a single parameter: `Placement.place` already returns **distinct** brokers (distinct nodes)
  and `min_domains` guarantees **distinct domains**; together they are equivalent to `target_n_val`.
- **binring V4 (exact minimal movement)** (riak_core's `update()` before `solve()`): we use HRW
  (**statistical** minimal movement) plus `maxSkew` (A2) for uniformity, which suffices for the control
  plane (few vnodes); we did not port binring's exact algorithm.
- **A single etcd** (k8s): not adopted, since it is precisely the **scale bottleneck** (~5000 nodes) that
  per-vnode sharding (slice D) avoids.

**Still open (on top of the same engine):**
- ✅ **An automatic rebalancing trigger (done, opt-in).** `Malachi.Cluster.AutoRebalancer` is a
  **level-triggered policy** on top of the `RebalanceCoordinator` mechanism (which stays manual by
  default). Each tick (default 30s), **only on the lease holder**: it takes the `plan`; if that plan is
  **non-empty and identical** for `stabilization` consecutive ticks (default 3, so about 90s), it calls
  `commit` (which re-gates on leadership). An empty or changed plan resets the counter, so a **SWIM flap**
  (a node briefly suspected, then back) **never** moves a vnode. It is the pattern the table above
  recorded: SWIM does the *detection* event-driven, while the *decision* to move reconciles and converges
  (no lost events). Seams (`plan_fun`/`commit_fun`/`leader?`) make it testable without `ra` or a lease, by
  driving `reconcile_now`. **Opt-in** (`MALACHIMQ_AUTO_REBALANCE`, defaulting to off, leaving today's
  manual behaviour untouched); `interval` and `stabilization` are configurable; and it only starts in
  `rebalance_children` when sharded and enabled. Tested: a commit after N stable ticks; a changed plan
  resetting the window; an empty plan never committing; a non-leader never committing; `stabilization: 1`
  committing on the first observation; losing and regaining leadership resetting it; and a partial-failure
  result passed to `on_result`: 7 tests. Suite at 738 tests, 0 failures; credo and dialyzer clean. The
  README gained the env vars. *(k8s's workqueue and expectations remain **not** adopted: cardinality is
  low, `ra` serializes membership, and the stabilization window already provides the debounce.)*
- 🚧 **Re-sharding**: changing the vnode **count** (R1 and R2 assume the same set of vnode ids, which is
  why re-sharding does **not** go through the rebalancer but through the **split** path). **Grow is
  implemented** (decision: grow only; **split-natural** geometry): **RS-1**
  `Malachi.Cluster.ReshardPlan`, a pure plan that takes the ring to a target count by repeatedly
  "splitting the **largest arc** at its midpoint", without moving any existing token (as properties: it
  grows to exactly N, deterministically, and the largest arc never increases) → **RS-2**
  `Malachi.Cluster.ReshardCoordinator`, a lease-gated GenServer that performs **one** split at a time
  through `SplitCoordinator`; it is **level-triggered**, re-planning from the live ring on each pass, so an
  interrupted reshard converges simply by re-issuing the same target (with no new multi-step intent); plus
  a `:ring_did_not_advance` guard against looping and a refusal on empty placement → **RS-3** the wiring in
  `application.ex` (`reshard_coordinator_child`, alongside the split coordinator under the lease) plus
  **`mix malachi.reshard --to N`** over RPC (reusing `Malachi.CLI.Rpc`) plus a `:multinode` run against
  real `ra` (1 to 3 vnodes with the topic **and its committed offsets** preserved; plus resuming a partial
  grow).
  - ⏳ **A recorded dependency: a durable ring.** The ring is minimal global state, **gossip-only**
    (faithful to NorthGuard), and on a **full-cluster restart** it reseeds from config
    (`MALACHIMQ_LOG_VNODES` through `sharded_vnodes/2`), whose geometry does **not** match a split-natural
    ring, so the migrated metadata would be orphaned. **A pre-existing gap, shared with split.** Current
    scope: reshard **at runtime**. Making the ring durable (and a reshard survive a full restart) is the
    natural follow-up, and it benefits split too.
  - **Out of scope (recorded):** vnode **merge/shrink** (draining into the successor plus `remove_vnode`
    plus deleting the `ra` group: genuinely new) and **retoken-to-even** (the exact geometry of
    `sharded_vnodes/2`, which would need a move-token primitive migrating a whole vnode).

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
