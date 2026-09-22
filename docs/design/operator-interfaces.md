# Operator interfaces: web console, terminal UI, and workspace

Status: specification, not yet implemented. Written against `main` at `edbba90` (v0.13.1).

This document specifies three operator interfaces for Malachi and the contract that keeps them one
product: a **web console**, a **terminal UI**, and a **workspace layout** of the web console that
also ships as a desktop application.

Every screen below names, per field, either a **SOURCE** (an endpoint that exists today, with its
real shape) or a **GAP** (an endpoint that has to be built, with the work it implies). Nothing in
this document assumes data the server does not have or cannot cheaply produce.

---

## 1. Context and audience

### 1.1 What exists today

Malachi serves one hand written HTML page from `lib/malachi/dashboard.ex`, a 1469 line module of
which 420 lines are HTML, CSS and JavaScript inside Elixir string literals. There is no Phoenix, no
Plug, no `package.json` and no asset pipeline. HTTP is parsed by hand on `:gen_tcp` with
`packet: :http`.

The page renders exactly two fields, `system.process_count` and `system.memory.total_mb`
(`dashboard.ex:1281-1282`), plus a topic list. The SSE stream it already consumes carries roughly
one hundred fields every second, including the whole storage flush percentile block, twenty one
integrity and storage failure counters, the security and audit blocks, and twenty four unexpected
message counters. None of it is rendered. Separately, `Malachi.Metrics.get_history/1`
(`metrics.ex:717`) holds three hundred per second snapshots in ETS and no route reaches it.

### 1.2 Why this is worth building

Row 11 of the readiness table in issue #183 records that a console with per topic metrics and an
admin API is demanded by paying customers and **has no owner**. The audit behind this document found
**37 reads that exist as public functions on a running node and are reachable from no HTTP route, no
wire operation and no mix task**. Among them: the node list, the ring, the lease holder, the Raft
state machine versions, the list of damaged segments, the per copy replication state, the sessions,
and the locked accounts.

The operator tasks in `docs/guides/operations.md` make the same point from the other side. Of about
sixty documented tasks, nine have a command, roughly forty are "set an environment variable and
restart", and at least six are read only questions the product answers nowhere at all: which ranges
are blocked (#91), how many sealed segments this node holds, what the pending rebalance plan is,
what control plane version each member runs, which users would lose access if strict ACL mode were
enabled, and what retention is in force for a topic (#194).

### 1.3 The three audiences, and which interface serves each

| Interface | Reached from | Serves | Wins at |
|---|---|---|---|
| Web console | A browser, on the dashboard port | The operator investigating, and the developer exploring | Charts, dense tables, record inspection, sharing a link |
| Terminal UI | A shell on the host or a jump box | The operator on call, and anyone already in a terminal | No install, works over ssh, fast keyboard loop, no browser |
| Workspace | The same browser, or a desktop window | The operator working a long incident across several clusters | Persistent tree, tabs per object, always visible health log |

These are not three products. They are three renderings of one object model, one navigation
hierarchy, one keyboard model, one vocabulary, one set of design tokens and one translation
catalog. Section 9 makes that contract explicit and testable.

### 1.4 What this document is not

It does not design a Kafka console. Sections 3.7 and 3.8 list, with reasons, the screens and
concepts from Kpow, Conduktor, Kafbat UI and KafkIO that exist only because Kafka works the way it
does, and that would produce permanently misleading screens if ported.

---

## 2. Principles

Each principle below is followed by where it comes from, so a future reader can weigh it.

**P1. A panel that fails does not blank the page.** Render every panel that can be rendered, and let
the one that failed say which part it could not load, in place. This rule is already written in this
repository, in `benchmark/dashboard/README.md`: *a page that goes blank tells a reader less than one
that says which part it could not load*. Kafbat UI implements the same idea as an inline error
component with a vertical offset so the heading, the tabs and the metric band stay interactive.

**P2. A number never appears without a verdict.** Conduktor publishes its thresholds beside its
percentages (skew under 25 percent, 25 to 75, over 75) so a figure is never left to the reader to
interpret. Malachi does the same for replica counts, seal lag, skip rates and headroom.

**P3. A measurement that was not taken is absent, not zero.** The benchmark page in this repository
already refuses to plot an unmeasured percentile, because a curve that dives to the floor reads as a
measured latency of zero. PostgreSQL returns NULL rather than 0 for replication lag on an idle
replica for the same reason. A caught up consumer shows "up to date", never "0 ms".

**P4. A destructive action states what will be lost, not that it is dangerous.** Issue #91's own plan
requires that the confirmation for sealing a blocked range state **the number of records that will
be discarded**, and calls a generic warning insufficient. Conduktor's "Add partitions" dialog is the
reference for the opposite of a generic warning: it names the semantic consequence.

**P5. Deny with an explanation, do not hide.** Kafbat UI keeps a denied action visible but inert
with a tooltip naming the missing permission, and adds a sub line naming the domain reason. That
teaches the permission model. Conduktor hides nav items the user cannot use, which is the opposite
choice; Malachi follows Kafbat for actions and Conduktor for whole sections that would be empty.

Redpanda Console makes this a type rather than a habit, which is the version to build: one
`DisabledReason` enum, one message map, and one explicit allow list of the few cases that may hide
instead of grey out. The default is then a disabled control with a reason, and hiding is a decision
someone had to write down.

**P6. A section with nothing in it is not rendered.** Kpow only renders its under replicated
partition table when the count exceeds zero, and only shows the simple consumers tab when simple
consumers exist. An empty table is noise that the operator must learn to ignore.

**P7. The cost of a query is visible.** KafkIO prints "Showing: 60 | Received: 8,433 | Received size:
1.1M" under its result table, so the operator can see the filter scanned 8,433 records to find 60.
Conduktor splits the same idea into matching, scanned, consumed and elapsed, which makes a filter
that matches nothing visually different from a query that has stalled.

**P8. The interface never does arithmetic the system forbids.** Cursors compare but do not subtract.
There is no offset field, no "shift by", no record count derived from a difference of positions, and
a cursor renders as an opaque copyable chip, never as a number an operator could try to add to.

**P9. One artifact per shared fact.** Colors, key bindings, command names and translated strings each
live in exactly one versioned file, and every interface is generated from it or tested against it.
Kafbat UI is the counterexample: a 1664 line `theme.ts` of per component nested tokens
(`theme.acl.table.deleteIcon`) that no second interface could ever consume.

**P10. Restraint is the aesthetic.** Tabular numerals, right aligned numbers, left aligned everything
else (Kpow states this as a rule), no animation that is not communicating state, and density that
survives a ten column table. KafkIO's status bar showing "Mem used: 821.42 MB | Max: 14.45 GB" is
the tone: an honest instrument, not a marketing screenshot.

---

## 3. The object model

This section is the vocabulary. Every interface uses these nouns, in these words, in both languages.

### 3.1 Topic

`Malachi.Metadata`, `metadata.ex:37-44`.

| Field | Type | Meaning |
|---|---|---|
| `name` | string | Matches `~r/\A[A-Za-z0-9._-]+\z/`; becomes a directory path segment |
| `keyspace_size` | positive integer | `2^bits`, bits in 1..32. Today always 256, because `LogApi` forces 8 bits (`log_api.ex:43,51`) |
| `state` | `active` or `sealed` | |
| `next_range_seq` | integer | The per topic range id counter |
| `policy` | policy name or nil | Storage policy. Nil means the global policy applies |

SOURCE: `GET /metrics` (JSON) and `GET /stream` carry the overview shape per topic: `name`, `state`,
`policy`, `keyspace_size`, `groups`, `range_count`, `active_range_count`, `segment_count`,
`active_segment_count`, `total_bytes`, `domain_violations`.

GAP: there is no way to delete a topic. `{:delete_topic, name}` and `{:seal_topic, name}` exist as
metadata commands (`metadata.ex:136-137`) with **no caller anywhere in `lib/`**.

### 3.2 Range

`metadata.ex:46-54`. The unit of parallelism, and the concept with no Kafka equivalent.

| Field | Type | Meaning |
|---|---|---|
| `id` | `{topic, seq}` | Globally unique, survives a vnode migration |
| `key_start` | integer | Inclusive |
| `key_end` | integer | Exclusive |
| `keyspace_size` | positive integer | |
| `state` | `active` or `sealed` | A parent is sealed by a split |
| `parents` | list of range ids | The full ancestor lineage, oldest first |

Splits are buddy allocation: `Keyspace.buddies?/4` (`keyspace.ex:69`) is
`size_a == size_b and bxor(start_a, size_a) == start_b`, and a split takes the midpoint
(`keyspace.ex:61`). A split requires the range to be active, splittable, and to have **no active
segment**; a merge requires both to be active buddies with no write head.

**A child range restarts its offsets at zero.** This is the single most dangerous fact for an
interface to render carelessly, and section 6.4 specifies how it is made visible.

SOURCE: `GET /topic?name=<t>` returns per range `seq`, `key_start`, `key_end`, `state`, `parents`
(as a **count**, not the ids) and the segment list.

GAP: the parent ids are reduced to a count by the private `range_detail/1`, so lineage cannot be
drawn. Nothing exposes range size or load. Issue #29 states the harder problem plainly: **there is no
per range load signal anywhere**, because telemetry carries only `%{topic}` and the reporter drops
even that. `BrokerServer.split_range/2` and `merge_ranges/3` exist and are reachable from no
surface at all (#30).

### 3.3 Segment

The unit of replication, and a first class object. Three representations exist and the interface must
not conflate them.

**Control plane record**, `metadata.ex:56-69`, the authority:

| Field | Meaning |
|---|---|
| `id` | `{range_id, seq}` |
| `replica_set` | Ordered list; the **head is the primary** |
| `state` | `active` or `sealed` |
| `start_offset` | Range relative base |
| `length` | Record count; **nil while active** |
| `byte_size` | **nil while active**; set from the seal command |
| `sealed_at` | Epoch ms; nil while active |

**On disk**, `Malachi.Log.Segment` (`log/segment.ex:23-45`): `byte_size` and `record_count` count
**flushed** bytes and records only. Files are `<base_offset>.log`, `.idx`, `.sealed`, with a
whole log `SEALED` marker written with fsync of the directory (`log.ex:186-199`).

**Per copy health**, from `Malachi.Storage.ElixirStore` and `Malachi.Cluster.ReplicationServer`:

| Condition | Where it lives today |
|---|---|
| Damaged tail, appends refused | `append_refusal: :damaged_tail`, `elixir_store.ex:281` |
| Integrity verdict | `%{reason, position, unreadable_bytes, sealed?}`, `elixir_store.ex:118-123` |
| Tail classification | `:clean`, `:blank`, `:torn`, `:rot` (`elixir_store.ex:316`) |
| Fenced | The write fence set by `seal/4`; reported by `fenced_segments/3` |
| Failed | The storage failure latch; reported by `failed_segments/3` |
| Short copy | Stored bytes below the sealed `byte_size` (`SelfHealing`) |
| Ahead of sealed end | #175, a copy holding a record past the sealed length |

GAP: every one of those per copy facts is reachable only by cross node Erlang. `Scrubber.damaged/1`
(`scrubber.ex:141`), described in its own docstring as a gauge for operators, has no route.
`ReplicaTracker` instances, which hold the commit offset and each replica's match offset, live in a
private field with no accessor at all.

### 3.4 Cursor

`log_api.ex:259`: `Base.url_encode64(:erlang.term_to_binary(positions))`, where positions is
`%{range_id => :start | {source_index, source_offset}}`.

`source_index` indexes the range's history sources: every sealed ancestor filtered to this range's
key slice, followed by the range itself. Validation is four guards before deserialization, including
a 262144 byte cap and an explicit rejection of the compressed term tag because a 2.6 KB token
inflates about six thousand times (`log_api.ex:324-373`).

**There is no cursor comparison function anywhere in the codebase**, and no way to translate a cursor
to a position an operator could reason about. The interface therefore treats a cursor as an opaque
identity: copy it, paste it, seek to it, and show what is around it.

### 3.5 Consumer group

`Malachi.Consumer.GroupCoordinator`. Membership, assignment and generation live **only in one
process's memory on one node** (`group_coordinator.ex:16` says so: member state is soft, a restart
makes members re-join). Committed positions are the part that is replicated, stored as
`%{{group, topic} => %{range_id => position}}`.

Assignment is by range, via rendezvous hashing with replication factor 1
(`assignment.ex:28-41`). Generation increments only when the computed assignment actually changed.

**Lag cannot be computed.** No function anywhere subtracts a committed position from a range end.
The committed position is a `{source_index, source_offset}` pair against a sources list whose shape
changes on every split, and the range's end offset lives in `Malachi.Broker.offsets`, read by the
private `next_offset/2`. Section 6.5 specifies what the interface shows instead.

### 3.6 Cluster, ring and control plane

| Object | Module | Reachability today |
|---|---|---|
| Membership (SWIM) | `Malachi.Cluster.Membership` | Cross node Erlang only. No node list anywhere |
| Ring topology | `RingTopology`, version plus placements plus pending split | `mix malachi.ring --show`, tab separated text, no JSON |
| Lease | `Malachi.Cluster.Lease`, holder plus fence | Cross node Erlang only |
| Vnodes | `DSRSM`, `ReplicatedDSRSM` | Only through `mix malachi.ring` |
| Machine version | `MachineVersion`, `@code_version 1`, six machines | `version_status/1`, cross node only. The `[:malachi, :ra, :machine_version]` event feeds **no metric** |
| Format marker | `malachi.format`, `format=` / `written_by=` / `requires=` | Boot log only |

The six versioned Raft machines are metadata, lease, ring, users, lockouts and ACLs. Every command
in all six was introduced at version 0 and the code version is 1.

### 3.7 Kafka to Malachi, and what does not translate

| Kafka concept | Malachi | Transfers? |
|---|---|---|
| Partition | Range | Partly. A range has no stable integer identity: it splits and merges at runtime |
| Offset | Opaque cursor | **No.** No field, no column, no arithmetic, no seek by number |
| Consumer lag as a record count | Time lag plus records behind, both server computed | **No**, not as a subtraction |
| Log segment | Segment | **Upgraded.** In Kafka it is an incidental per broker storage statistic; here it is the replication unit with a lifecycle |
| Replication factor, ISR, URP | Segment replica count, sealed versus open, seal lag | **No.** Replication is a property of a segment's lifecycle, not an integer on a topic |
| Controller broker | Raft term, leader, quorum, commit index, machine version | **No.** Different screen entirely |
| Partition skew | Split activity | **Inverted.** Skew is the trigger for a split, not a misconfiguration |
| Increase partitions | Nothing | **Gone.** Splitting is automatic. There is a history, not a button |
| Truncate to an offset | Drop segments sealed before T, or below a cursor | Reshaped |
| Topic config keys | Per topic retention policy | Reshaped, and unreachable today (#194) |

### 3.8 Screens that are deleted outright

Schema Registry in its entirety, Kafka Connect in its entirety, ksqlDB in its entirety, Kafka Streams
topology and RocksDB metrics, producer transactions and idempotence, Kafka ACLs as a second
authorization system beside RBAC, client and IP quotas, tombstones and compaction as a cleanup
policy, preferred leader election, partition reassignment, static group membership, simple consumers,
internal topic hiding, per message serde selection as a required input, and every vendor flavour
adapter.

Four of those deletions carry a lesson worth keeping:

- **Auto serde inference ports and becomes more important, not less.** The problem the registry
  solves for the UI is "what shape is this data so I can render and filter it", and that problem
  survives.
- **Kpow's exact key search** reimplements Kafka's partitioner inside the UI, with a documented
  failure mode where a trailing space sends the search to the wrong partition. Malachi can look the
  key up in the range map. Keep the feature, delete the caveat.
- **Kpow's KRaft tab** is an optional mode specific view. Here it is the control plane screen, which
  is an upgrade rather than a port.
- **Conduktor's efficiency check** (is your partition count divisible by your broker count) would
  fire permanently and mean nothing. Importing it would be actively misleading.

---

## 4. Design system

### 4.1 The one artifact

`docs/design/design-tokens.json` is the single source of truth for color, and it generates every
consumer. Nothing hand mirrors a palette.

```
docs/design/design-tokens.json
        |
        +--> assets/src/styles/tokens.css      :root, .dark, @theme inline
        +--> tui/src/theme/generated.rs        Palette with TRUECOLOR / ANSI256 / ANSI16
        +--> lib/malachi/ui/tokens.ex          for any server rendered fallback
        +--> docs/design/tokens.snapshot.json  flat name -> {oklch, srgb8, theme, platforms}
```

Colors are authored in **OKLCH**, which is what shadcn itself now emits, and quantised to the 256
color cube and to the 16 color ANSI set **at generation time**, in a perceptual space, never by
naive sRGB distance.

**The 16 color mapping is hand declared, never computed.** A nearest neighbour search collapses
`state.damaged` and `state.fenced` onto the same red, which is precisely the distinction an operator
needs at 3 a.m. Those mappings are written in the token file as explicit values.

Four CI gates prove the sharing, and they are the reason this is one design system rather than three
that look similar:

1. Generated files are committed, and CI fails on `git diff --exit-code` after regenerating.
2. A grep over `assets/src` and `tui/src` for raw color literals (`#`, `oklch(`, `rgb(`, `hsl(`,
   `Color::Rgb`, `Color::Indexed`, named `Color::`) outside the generated files fails on any hit.
   Redpanda Console runs the same idea as a CI job that audits drift from its own component registry,
   flagging locally modified components, off token colors and ad hoc utility classes, and posts an
   advisory comment rather than failing the build. Malachi fails the build, because there are three
   renderers here and a drifted color is a broken contract rather than a style lapse.
3. A cross language contract test: the web test parses `tokens.css` and the Rust test deserialises
   `tokens.snapshot.json`, both asserting field by field in both directions, so a missing token and
   an extra token both fail.
4. The generator computes contrast for every declared foreground and background pair in both themes
   and fails below threshold, including muted foreground on muted, and every `state.*` on both
   `background` and `card`.

### 4.2 Tokens

The shadcn base set is adopted unchanged: `background`, `foreground`, `card`, `popover`, `primary`,
`secondary`, `muted`, `accent`, `destructive`, `border`, `input`, `ring`, `chart-1` through
`chart-5`, and the sidebar group.

**`--primary` is not redefined for branding.** It is bound to the default button, selected states and
badges, and a saturated primary fights the status colors that carry the actual meaning. Malachi adds
its own named tokens instead:

| Token | Meaning |
|---|---|
| `--state-active` | A segment taking writes, a range serving, a node alive |
| `--state-sealed` | Sealed and settled. Calm, not celebratory |
| `--state-fenced` | Write fenced. Neither healthy nor broken |
| `--state-damaged` | Rot, torn tail, bad CRC, bad magic |
| `--state-behind` | A copy short of the sealed length, a consumer behind |
| `--state-ahead` | A copy past the sealed length (#175). Rare and serious |
| `--state-blocked` | A range that cannot take writes (#91) |
| `--state-unknown` | The control plane cannot currently answer |

`--chart-1` through `--chart-5` stay reserved for time series, and are checked for deuteranopia
distinguishability by the same generator gate.

### 4.3 Themes

Three settings, following Kafbat UI rather than a binary toggle: **auto** (follows
`prefers-color-scheme`, the default), **light**, **dark**. The existing benchmark page in this
repository already uses exactly this token plus media query shape, with an amber accent, so the
console is continuous with the one page in the project that was designed on purpose.

The terminal adds a fourth consideration the web does not have: the default background token is the
sentinel `default`, which emits no color and inherits the terminal's own background. A TUI that
paints a dark background inside a configured light terminal reads as broken, and it destroys
transparency setups. k9s uses this sentinel for the same reason.

### 4.4 Typography and density

- Interface text: the system stack, as today.
- Every identifier, cursor, key, offset, byte size, hash and duration: a monospace stack, with
  `font-variant-numeric: tabular-nums`.
- Numeric columns and their headers are right aligned; everything else is left aligned. This is
  Kpow's published rule and it stops an id reading as a quantity.
- One density control, a three value switch (comfortable, compact, dense) writing a single token
  block, because a ten column segment table and a two column signal list want different row heights.

### 4.5 Component inventory

shadcn/ui, with the **Base UI** primitive family, which is where upstream is heading. This is a one
way door and it is recorded here deliberately: Base UI uses `render={<Button/>}` where Radix uses
`asChild`, so most existing examples and most generated code will not compile unchanged for a while.

| Need | Component |
|---|---|
| Shell | `Sidebar` with `variant="sidebar" collapsible="icon"` |
| Tables | TanStack Table v9 through the DiceUI faceted data table registry item |
| Charts | The shadcn chart primitive over Recharts |
| Key visualizer | **Canvas, not Recharts.** See 6.4 |
| Any code editor | **CodeMirror, not Monaco.** Monaco needs `worker-src blob:`, which fights the CSP in section 11. Redpanda Console uses Monaco throughout and reached for CodeMirror on the one screen it added most recently |
| Command palette | `Command` over cmdk, bound to Cmd+K or Ctrl+K |
| Row inspector | `Sheet` |
| Ordinary create and edit | `Dialog` |
| Anything irreversible | `AlertDialog`, which has no outside click dismiss |
| Panels | `Resizable`, whose `autoSaveId` persists geometry |
| Notifications | `sonner` |

`components/ui/*` is vendored code that is never edited, so `shadcn add --overwrite` stays safe.
Every Malachi specific change lives in `components/malachi/*` or in `tokens.css`.

---

## 5. Information architecture

One hierarchy, rendered three ways. The terminal binds the top level to number keys, the workspace
binds it to the activity rail, and the web renders it as the sidebar.

| # | Section | Sub views |
|---|---|---|
| 1 | **Overview** | Cluster health in one screen |
| 2 | **Topics** | List, Detail, Ranges, Segments, Groups, Retention, ACLs, Records, Produce |
| 3 | **Ranges** | Keyspace map, Lineage, Splits and merges, Blocked |
| 4 | **Segments** | Inventory, Copies, Scrub, Rebuilds |
| 5 | **Consumers** | Groups, Assignment, Positions, Skips |
| 6 | **Records** | Inspect, Produce |
| 7 | **Cluster** | Nodes, Ring and vnodes, Raft, Compatibility, Upgrade |
| 8 | **Signals** | Insights, Issues |
| 9 | **Access** | Users, ACLs, Sessions, Lockouts, Audit |
| 10 | **Settings** | Appearance, Language, Density, Keymap, About |

Three structural rules, all adopted from the research:

- **The screen grammar repeats.** Every list screen is a summary strip of large figures, then visual
  panels, then a filterable sortable table. Kpow's own demo shows broker, topic, group and connect
  as literally the same layout with different nouns, which is what makes a large product learnable.
- **The breadcrumb carries a live count.** `demo > Ranges (13/13)` becomes `(1/13)` when a filter is
  applied, and the filter shows as a removable chip inside the field.
- **A section that would be empty is not rendered.** No consumer groups means no Groups tab, not an
  empty one.

---

## 6. The web console

### 6.1 Shell

Left sidebar, icon collapsible, with the ten sections above. Top bar carries the breadcrumb with its
live count, the context filter, a live mode toggle, a refresh control, the theme switch, the language
switch and the user menu. Status of the cluster is a persistent chip in the top bar, not a page.

The cluster gets a user chosen **color and icon**, propagated to the chip, the favicon and the
workspace tab strip. Both Conduktor and KafkIO ship this, and KafkIO's release notes are explicit
that a strip of tabs across prod, staging and sandbox becomes parseable by color alone. It is a
two minute feature that prevents a whole class of incident.

SOURCE for identity: none today. GAP: `GET /api/v1/me` returning username, permissions, locale, and
the cluster's display name, color and icon. Without it the SPA cannot render by role, because today
a non admin receives a raw JSON 403 on `/` and the page has no way to know what it may show.

### 6.2 Overview

Summary strip: nodes alive of total, topics, ranges active of total, segments sealed of total, bytes
on disk, produce rate, consume rate, and a health score.

The health score follows Kpow's published arithmetic so nobody has to reverse engineer it: OK counts
1, warning 0.5, error 0, rolled up to a worst child badge at every level. It is also recorded as an
ordinary time series so a point in time checks engine still yields a trend.

Panels: produce and consume rate over the last hour; storage flush latency p50, p99 and p999 from
`system.storage_flush`, which are **the only precomputed latency numbers the server has**; disk by
topic as a treemap; and the signal summary.

SOURCE: `GET /metrics` JSON and `GET /stream` cover the rates, the flush percentiles and the topic
totals. GAP: the node count, the health score and the signal summary.

### 6.3 Topics

List: the standard three layer screen. Columns are name, state, ranges active of total, segments
sealed of total, bytes, groups, domain violations, and activity.

**There is no message count column.** KafkIO's `# Msgs` and Kafbat's "Number of messages" are both
`offsetMax - offsetMin`, arithmetic that requires dense integer offsets. Malachi has no cheap count.
The column is omitted rather than filled with an expensive scan or a wrong number.

Detail tabs: Overview, Ranges, Segments, Groups, Retention, ACLs, Records, Produce.

SOURCE: the overview fields listed in 3.1, and `GET /topic?name=` for ranges and segments. Note that
`/topic` has **no paging**, and returned nineteen segments in one response for a one gigabyte topic
on the dev node; a topic with thousands of segments returns them all. GAP: a paged segment endpoint.

### 6.4 Ranges: the keyspace map

This is the screen with no prior art in any Kafka console, and the one where Malachi has an
advantage over every precedent studied.

**The drawing is a keyspace by time heatmap, not a ring.** The Y axis is the hashed keyspace, which
in Malachi is a fixed numeric interval of `keyspace_size`, so range boundaries are exactly plottable
on it. The X axis is time. Cell brightness encodes a chosen metric. This is TiDB's Key Visualizer
model, itself derived from Bigtable's, and the control set is copied deliberately: a metric dropdown,
a brightness slider that lowers the scale minimum rather than applying a filter, select and zoom,
rectangular zoom, backspace to go back one view, R to reset, and a hover tooltip that pins on click.

**The part no precedent can do:** because the Y axis is a real numeric keyspace, split and merge
history overlays directly on it. A horizontal line appears at a buddy midpoint at the instant of a
split, and two lines fuse at the instant of a merge. To the left, pixel aligned to the same Y axis,
an icicle rail draws the buddy tree. One widget delivers both the current map and the history.

Bigtable publishes five named patterns as in product help, and Malachi ships the same idea in its own
vocabulary: a bright horizontal band is one range taking all the traffic, a bright diagonal is a scan
walking the keyspace, and so on.

**Lineage** copies the Kinesis shard model exactly, because a merge has two parents and a tree cannot
express that: `parent_range_id`, `adjacent_parent_range_id`, an explicit `{key_start, key_end}`
object, and open versus closed expressed by the presence of an end marker rather than a status enum.
It renders as a DAG.

**The offset reset is annotated, always.** A child range restarts at zero. An operator who sees a
small number without that annotation reads "barely any data" instead of "new range". The lineage
rail marks the reset on every child edge.

Pixelation happens **server side**, in Elixir, and the browser receives a small matrix. TiDB splits
the work this way for the same reason.

SOURCE: `key_start`, `key_end`, `state` and a parent count. GAP: everything else. The parent ids, the
range size, and above all the per range load signal, which #29 records as not existing at all. The
heatmap is specified here and **cannot be built until that signal exists**; it is the largest single
item in the API gap list.

### 6.5 Consumers: position without subtraction

Four things shown side by side, never a single lag number:

1. **The cursor**, as a copyable monospace chip showing a short opaque prefix, with the decoded
   `{range, source_index, source_offset}` breakdown on hover. Never a number to add to.
2. **Time lag**, derived from a server published timestamp of the oldest unacknowledged record.
   Pulsar publishes exactly this as `earliestMsgPublishTimeInBacklog` for the same reason.
3. **Records and bytes behind**, server computed, explicitly labelled as an estimate.
4. **Headroom**, a four state ladder copied from PostgreSQL replication slots, which is the best
   existing answer to "how close is this consumer to losing its position": reserved, extended,
   unreserved, lost, rendered as a segmented badge, with a scalar of bytes remaining before loss and
   a machine readable reason when it is already lost.

A caught up consumer reads "up to date", not "0 ms", per P3.

The seek control is a five option radio group, copied from Kinesis shard iterator types and MongoDB
resume options: earliest, latest, at timestamp, at cursor, after cursor. **No free text numeric
field anywhere**, which enforces the cursor contract in the interface itself.

SOURCE: the list of group names per topic, and nothing else. GAP: members, assignment, generation,
committed positions, and every one of the four numbers above.

### 6.6 Segments and copies

The inventory table: segment id, range, base offset, records, bytes, state, replica count, primary,
age, and a composite health label.

**Health is a composite flag set, not an enum.** Druid's orthogonal booleans and Ceph's composite
rendering are the model: a copy can be sealed and behind, or fenced and damaged, and a combinatorial
enum would need dozens of names. The interface shows a resolved severity color, a short composite
label such as `sealed+behind`, and an expandable per condition breakdown. The condition vocabulary is
capped at what the control plane can actually distinguish: `active`, `sealed`, `fenced`, `damaged`,
`behind`, `ahead`, `failed`, `unknown`. Ceph's thirty plus names are a warning, not a model: a
vocabulary larger than the state machine becomes a lie in the docs and in the translation catalog.

**Per copy detail** shows each replica's recorded length and digest, the union of errors across
copies, and an explicit marker for which copy was selected as authoritative. `rados
list-inconsistent-obj` is the reference for showing all three levels at once, and it pairs with
exactly one remediation that names its target.

**Rebuild progress is three bars, never one**: bytes copied, records verified, CRC checked, with the
phase name above them and the source and target node named. Elasticsearch's `_cat/recovery` stage
vocabulary is the shape. A single percentage over an estimated denominator is dishonest, and Vitess
publicly warns its own copy percentage can be off by 50 to 60 percent.

**"Explain this placement"** is a button on every range and every segment copy. It returns the per
node list of named deciders, each with a yes, no or throttle and one sentence. Malachi's placement
decider already computes exactly this in `Malachi.Cluster.Placement`; surfacing it verbatim is the
highest leverage single feature in this document, and it is Elasticsearch's allocation explain API.

**The lifecycle timeline** follows Temporal's three coordinated views: a timeline where event groups
collapse create, seal and roll into one span per copy with points for discrete events, a compact
grouped list which is what people actually read, and a JSON view which is what people paste into a
bug report. Grafana's distinction decides the encoding per screen: a state timeline that merges equal
consecutive values answers "how long was this copy in this state", and a status history that never
merges answers "how did each scrub pass turn out". Both exist, on different screens.

SOURCE: the sealed segment list with `start_offset`, `length`, `byte_size`, `state`, `sealed_at`,
`primary` and `replica_set`, where primary and replica set arrive as **inspected PID strings** such
as `"#PID<0.807.0>"`, which is not an operator facing identifier. GAP: node names instead of PIDs,
and all per copy health.

### 6.7 Records

The screen every reference product is built around, and the one Malachi cannot do at all today.

The form: topic or topics, a seek mode from the five option group in 6.5, a limit, a key or key
prefix, and a filter. The result strip is always visible and always shows four numbers, following
Conduktor: **matching, scanned, consumed, elapsed**, so a filter that matches nothing is visually
distinct from a query that has stalled. A stop control sits adjacent to the moving rows, not in a
distant toolbar, which is KafkIO's placement.

**Exact key search** is a first class mode and it is trivially correct here: look the key up in the
range map, rather than reimplementing a partitioner in the browser.

**Live tail is not a mode.** It falls out of the limit control having no end bound, which is
Conduktor's design and KafkIO's seventh search type. The same form, filters and decoders serve
historical search and live tail.

Each record gets a URL and a standalone page, which is the only way to view one too large to inline,
and which turns sharing a bug into pasting a link.

**Reproduce** is one click from any record into a prefilled produce form.

Default safety, copied from yozefu, the Rust Kafka TUI: **the inspector never commits a position**,
so looking at data cannot alter cluster state.

#### The scan is streamed, stoppable, and resumable

AKHQ's search is the right shape for any expensive unbounded query, and it is cheap: SSE, no
WebSocket, no job queue, no result store. The server emits an event per poll carrying the partial
records and a **resumable cursor**, so rows appear as they are found, and Stop is not "cancel and
lose everything": the cursor for where the operator stopped is already in the client's hand, and Next
continues from there. The export endpoint loops over the same cursor rather than having a second code
path. The client throttles its own re-render so a fast scan does not melt the browser.

Two adaptations, both forced by the cursor contract:

- **There is no percentage.** AKHQ computes percent as the sum of `current - begin` over the sum of
  `end - begin` across partitions, which is offset arithmetic. Malachi cannot do that, which is
  exactly why the four numbers from Conduktor (matching, scanned, consumed, elapsed) are the progress
  display and not a bar. A bar over an unknowable denominator would be a lie.
- **Exhausted and timed out are different endings, and the interface says which.** AKHQ has a bug
  worth naming so it is not copied: an empty poll sets a flag and terminates the scan, so one slow
  poll silently ends the search and reports done at whatever point it reached. Malachi's scan
  terminates with an explicit reason, and "reached the end" never renders the same as "stopped
  early". This is principle P3 applied to a query.

#### The view is a saved object, not a URL the operator has to keep

Kadeck's single best idea, and Conduktor ships a version of it as shareable filters. A **view**
bundles everything about how an operator was looking at a stream (the seek mode, the filter, the
decoder choice, the promoted columns, the limit, and a free text description) into a named object
that hangs under the topic, is listed on the topic overview, is shareable as a link with private or
organization visibility, and is itself audited on create, update and delete.

That one primitive is what turns a one off debugging session into a durable team artifact: the DLQ
triage view, the stuck consumer view. It is cheap to build, because it is the query state serialised,
and it is the feature that makes the console multi player rather than a personal tool.

#### Decoding is remembered per topic

Offset Explorer persists the key and value decoder **as a property of the topic**, applied to both
reads and writes, so nobody re-picks a codec while debugging. Kpow reaches the same place from the
other direction with an Auto mode that infers the format and then remembers the choice per topic.
Malachi does both: infer by default, remember the override per topic and per user.

The extension point for a payload nobody anticipated stays deliberately small. Offset Explorer's is
one interface with two methods, bytes to string, no broker types in the signature, dropped into a
directory with a worked example shipped in the box. That is the bar; Kadeck's lifecycle heavy codec
interface is the counterexample.

One detail from AKHQ's client that is easy to get wrong and expensive to discover: record payloads
are parsed with a lossless JSON reader, because a standard JSON parse silently wrecks a 64 bit
integer. Malachi's offsets, sizes and timestamps are all 64 bit.

Two more from Redpanda Console, both about honesty when decoding fails or a record is too big:
each payload carries a **troubleshoot report** listing which decoders were tried and why each one
failed, rendered above the payload, and an `is_payload_too_large` flag with a per request override so
an operator can pull one oversized record on demand without raising the global cap. Live tail keeps a
**hard display window of 150 rows** plus a buffer cap and a throttled flush, rather than letting the
table grow until the tab dies.

#### If a filter language is added later

Not in the first version, but the shape is decided now so it is not improvised under pressure.

Redpanda's is the most complete implementation studied: the predicate is authored in a real editor
with a generated type declaration so the bindings get IntelliSense, transpiled by the browser's own
language worker, sent as the transpiled form, and run **server side** in an embedded JavaScript VM
with a watchdog that interrupts the VM after 400 milliseconds per record. Multiple saved filters are
combined by generating one function each and ANDing them.

What the research found in that implementation and what Malachi must not copy: the sandbox is
"the VM has no host bindings" plus that interrupt, with **no instruction counter, no memory cap, no
globals allow list, and no per record VM reset**, so state one record's script sets persists into the
next. Any Malachi equivalent declares its resource limits explicitly and resets between records, or
it uses a language that cannot carry state at all, which is what Kafbat UI chose with CEL.

GAP: all of it. There is no read by cursor on the wire and no HTTP route. This is the single largest
feature gap between Malachi and every product studied.

### 6.8 Cluster

Nodes: name, status from the SWIM view, attributes, vnodes held, segments held, disk, uptime, binary
version and per machine effective version.

**Mixed version state uses etcd's two number model**: the per node binary version and the cluster
effective state machine version are always shown as a pair, never one alone, plus an explicit
rollback floor sentence such as "rollback to 0.12.0 is still possible because two members remain on
version 1". The format marker's `requires` field is the disk side of the same sentence.

**Compatibility** is a page modelled directly on RabbitMQ's feature flags screen, with the same four
states (enabled, disabled, state changing, unsupported) and a stability column. This is not an
arbitrary choice: RabbitMQ runs on the same BEAM and the same `ra` library, so Malachi's audience
already knows the screen.

**The version table is inverted**, the way `nodetool describecluster` does it: group by version and
list the nodes, with a separate unreachable bucket, so divergence is a one glance count of groups.
Six rows, one per state machine.

**Upgrade** follows Consul's autopilot model: a named status from a small ordered set, members
bucketed by version as a stacked segmented bar, and current failure tolerance as a first class number
beside it, so the operator sees the fault tolerance the upgrade is temporarily consuming.

GAP: every field on this screen. The node list, the ring, the lease, the vnode placement and the
machine versions are all cross node Erlang only today, and `mix malachi.ring --show` is the sole
existing read, in tab separated text with no JSON mode.

### 6.9 Signals

The catalogue below replaces Kpow's, which is almost entirely about partition count and would fire
permanently and meaninglessly here.

| Signal | Severity | Derived from |
|---|---|---|
| Range blocked, no replica set majority | Error | #91. No read exists |
| Segment under replicated | Error | `Placement.under_replicated/3` |
| Orphaned fence, seal did not land | Error | `malachi_cluster_orphaned_fences_total` versus `fences_reconciled` |
| Copy damaged, rot or torn tail on a sealed segment | Error | `malachi_storage_integrity_failures_total{reason}` |
| Scrub found something unrepairable | Error | `malachi_storage_scrub_segments_total{result="unrepairable"}` |
| Storage failure | Error | `malachi_storage_failures_total{reason}` |
| Replicas in one failure domain | Warning | `malachi_domain_violations{topic}` |
| Consumer lost data to retention | Warning | `malachi_retention_skips_total{origin="cursor"}` |
| Retention sweep not running | Warning | `malachi_retention_sweep_duration_seconds_count` flat on every node |
| Control plane version stuck | Warning | `[:malachi, :ra, :machine_version]` with `stuck: true` |
| Rollback floor above this binary | Warning | `malachi.format` |
| Vnode without coordinators | Warning | #217 |
| Unexpected messages rising | Info | `malachi_unexpected_messages_total{server,kind}` |
| Split not keeping up | Info | GAP, needs the per range load signal |

Two rules from Kpow's Signals screen carry over. Each issue row's **first column is Actions**, a short
list of next best investigative steps specific to that check and that resource, so the row says what
to do and not only what is wrong. And the counters are recorded as ordinary time series, so trends
exist without keeping issue history.

Note the deliberate omission: a scrub failure counter alone reads zero both when all is well and when
nothing is looking, which `operations.md` already warns about. The signal therefore pairs the failure
counter with the liveness of `result="verified"`.

### 6.10 Access

Users with their permissions and ACL grants; ACLs with pattern scoping; sessions with the ability to
revoke one; lockouts with the ability to clear one; and the audit log.

One screen answers a question the guides currently make an operator answer by hand, one user at a
time: **who would lose access if strict ACL mode were enabled now**. `per-topic-acls.md` describes
that rollout as a per user loop with no aggregate view.

The shape for that screen comes from Redpanda Console's Permissions view, which is a list of
collapsible **principal cards** rather than a flat table. Each card header carries the principal, a
deny count when there is one, and a summary line such as "3 direct grants, 2 via roles". Expanded, the
direct grants are editable and the inherited ones sit under a spanning sub header naming the role
they came from, read only. Reading a permission model by principal is what answers the question an
operator actually has, which is never "who can read this topic" but "what can this account do".

One default from that product is worth naming so it is not copied: deleting a role there always
deletes its ACLs as well. Malachi separates the two, and a delete that would orphan or remove grants
says so and counts them, per P4.

### 6.10.1 The console role is not a wire permission

This is the one place where the reference product found a real defect in Malachi's model rather than
a missing screen.

Malachi has exactly three permission atoms, `:admin`, `:produce` and `:consume`
(`auth.ex:229-241`). The last two are **wire protocol** permissions, and they incidentally confer
dashboard read access: a `:produce` account can read `/metrics`, `/topic` and `/rate_limits` today
(`dashboard.ex:292-316`). A data plane capability is leaking into the operator surface, and the only
alternative the model offers is `:admin`, which grants everything.

Redpanda Console keeps the two separate and says so in its documentation in as many words: a console
role does not grant access to the data APIs, and a console admin is not a cluster superuser by
default. Its console roles are exactly three, cluster wide, and strictly nested: **viewer** reads,
**editor** adds the mutations that are not security, **admin** adds users, ACLs and diagnostics.

Malachi adopts the same separation. The console gets its own three roles with the same nesting, the
existing atoms stay what they are, and being able to produce to a topic stops implying being able to
read the cluster's operational state. Until that lands, every screen in section 6 renders under the
current coarse model, which section 6.1 already has to work around with `GET /api/v1/me`.

SOURCE: `GET /users`, `GET /users/:u/acls`, and the counters. GAP: `AclStore.list_all/0` has no
surface, sessions and lockouts expose only a count, a user's `created_at` and `updated_at` are never
returned by any of the three existing surfaces, and the console role separation above does not exist
at all.

### 6.11 States

Every screen specifies four states, and they are not decoration.

- **Loading.** A skeleton of the layout, not a spinner, and a thin top progress bar for a refresh
  rather than a dimming overlay, so a fast refresh does not flash.
- **Empty.** The empty state contains the action. KafkIO's "Search to display messages" with a
  clickable Search link beneath it is the model, not a bare blank table.
- **Error.** Inline, in the panel that failed, with the page heading, tabs and metric band still
  interactive (P1). Toasts are deduplicated by request URL so a flapping endpoint produces one
  notification, not a stack.
- **Denied.** The control stays visible and inert, with a tooltip naming the missing permission and a
  sub line naming the domain reason (P5).

A fifth state is specific to this product: **stale**. Conduktor makes the freshness of its own view a
first class widget on the home page. Malachi's SSE stream can drop, and when it does the interface
says so, marks the gap honestly, and does not quietly render old numbers as current.

---

## 7. The workspace layout and the desktop application

### 7.1 What is bought, and what is not

Malachi buys the IDE layout **contract**: named regions, tabs keyed by a resource identity, a command
palette, and layout that survives a restart. It does not buy an IDE **host**. A VS Code extension is
specified in 7.6 and deliberately not built, for reasons recorded there.

The workspace is a second layout of the same React application, reachable in the browser and shipped
as a Tauri desktop application from the identical `dist/`.

### 7.2 The resource URI comes first

Every rule in this section derives from one decision, so it is made first:

```
malachi://<cluster>/topic/<t>/range/<id>?view=records#cursor=<opaque>
```

Same URI opens the same tab. A different `view=` opens a second tab of the same resource. Tab
deduplication, tree synchronisation, layout persistence and deep links are all consequences.

### 7.3 Regions

| Region | Holds | Cap |
|---|---|---|
| Activity rail | The ten sections of section 5, collapsed to icons | 6 visible, rest in an overflow |
| Side bar | The cluster tree, keyed by range id | 3 to 5 views per container |
| Document area | Tabs, splittable | Tabs are resource URIs |
| Bottom panel | Health log, Output, Query results, Problems | Tabs |
| Status bar | Left: cluster scope. Right: active document scope | 6 items |

The health log is the KafkIO idea worth copying outright: an ambient dock that polls **every
connected cluster** on a cycle and prints a labelled block per cluster, so degradation on a cluster
the operator is not currently looking at still reaches them. Its only control is an autoscroll
checkbox.

Panels get numeric addresses, `Alt+1` through `Alt+6`, following JetBrains rather than VS Code.
Skilled operators memorise that 4 is segments; hunting an icon rail is slower.

The status bar's warning slot carries the mixed version rolling upgrade state from 6.8, and only
`errorBackground` and `warningBackground` are used as colours.

### 7.4 The split and merge tombstone contract

This is the part of the layout no Kafka console can teach, because a Kafka partition never stops
existing.

When a range splits while a tab for it is open, the tab is **not closed**. It is marked tombstoned,
its actions are disabled, its cached cursor is invalidated **loudly**, and an inline action offers to
open the children. Merge is mirrored. This is VS Code's `closeOnFileDelete: false` posture, applied
to an object that genuinely ceased to be writable.

Silently re-anchoring a cached cursor after a split would show an operator the wrong records during
an incident, which is the worst possible moment. A child restarts at zero, so a cursor from the
parent is not merely stale, it is meaningless.

The tree is keyed by range id and expansion state is an id set, so a split or merge storm cannot
destroy expansion and scroll position. Auto reveal is defeatable, for the same reason VS Code makes
explorer auto reveal optional.

### 7.5 Dirty state, and the one document that has it

**A read only inspector never shows a dirty indicator.** A dot on a segment inspector reads as
pending writes to a Raft sealed object, which is a lie.

Exactly one kind of document may go dirty, and it is borrowed from the `vscode-kafka` extension,
whose best idea beats every web console studied: **produce and consume are a file, not a dialog**. A
`.malachi` request document is versionable, diffable, shareable and reviewable. Config editors for
ACLs and retention are the only other dirty capable documents.

### 7.6 Command palette

`Cmd+K` or `Ctrl+K`, not `Cmd+B`, which `SIDEBAR_KEYBOARD_SHORTCUT` already owns. A prefix grammar,
which no reference console has, so it is a differentiator rather than a copy:

| Prefix | Selects |
|---|---|
| `>` | Commands |
| `@` | Ranges |
| `#` | Segments |
| `:` | Seek to cursor |

### 7.7 The desktop shell

Tauri 2, wrapping the identical web build. `single-instance` is registered first, which the plugin
documentation requires. The tray is menu only, because **Linux emits no tray click events at all**,
and Linux is Malachi's only production platform. The native menu is authored twice, because macOS
uses submenus where Windows and Linux use a window menu. File system permissions are scoped per
window label.

**The Tauri phase is gated on one measurement, taken before any certificate is bought.** Tailwind v4
requires Safari 16.4 with no documented fallbacks, WebKitGTK on Linux is whatever the distribution
ships, and Linux is the only platform that matters here. Build the console and open it in the oldest
WebKitGTK 4.1 Malachi supports. If it renders wrong, the desktop shell is not viable in this form and
the decision is revisited rather than worked around.

Linux distribution is planned around **AppImage**, because `.deb` and `.rpm` cannot self update
through the updater plugin. Shipping them anyway is a deliberate choice to be made in the open, not
a default.

Code signing is an organisational cost, not a configuration line: an Apple Developer ID certificate
plus mandatory notarisation, and a Windows certificate that has been hardware token or cloud HSM
bound since June 2023.

### 7.8 The VS Code extension, specified and not built

If it is ever demanded by name, it is built without a webview: a `TreeView`, a
`TextDocumentContentProvider` on a `malachi:` scheme so a detail opens as a plain read only editor
(which gives free theming, find, split, and diffing two segment copies as text, which is genuinely
useful), a `LogOutputChannel` per subsystem, one or two status bar items, and `QuickPick` commands.

It is not built now because shadcn theming and the VS Code theming API are mutually hostile: one is
eight tokens, the other is roughly a thousand `--vscode-*` variables from an arbitrary user theme
with no documented change event. Bridging them adds a fourth divergent styling target. Microsoft's
own `@vscode/webview-ui-toolkit` was archived in January 2025 with no supported successor, and
Microsoft retired Azure Data Studio rather than maintain a forked shell beside an extension.

---

## 8. The terminal UI

### 8.1 What ratatui is, and what it is not

ratatui is a **renderer, not a framework**. It gives no event loop, no focus manager, no view router,
no modal stack, no keymap registry and no theming. Choosing it means writing those. That is not a
reason to avoid it, but it is a scheduled deliverable rather than a discovery:

**`malachi-tui-core` is built first**, before a single screen is drawn: event loop, focus manager,
view router, modal stack, keymap registry, help generator, token resolver.

The decisive argument for Rust is not the library. It is that **Tauri's backend is Rust**, so a
shared `malachi-client` crate (API client, domain types, token resolver, translation catalog, command
registry) pays for two of the three surfaces.

The highest value pattern to port from elsewhere is Bubble Tea's `key` plus `help` pairing: key
bindings are declared as data and the help footer is **derived** from that data, so help cannot drift
from bindings. For a bilingual product this is worth more still, because the binding's description is
then the only thing that needs translating, in one place.

### 8.2 Prior art

The real prior art is **yozefu** (`MAIF/yozefu`), a Rust and ratatui Kafka explorer with a single
binary, an SQL like query language, user defined filters in WASM and a headless CLI mode; and
**kaskade**, in Textual, which publishes its whole keymap. Both are worth reading before writing
`malachi-tui-core`.

yozefu's default is adopted directly: **it does not commit offsets automatically, to avoid altering
cluster state**. The Malachi inspector does the same.

A claim that Kpow shipped a CLI and a terminal UI in September 2026 was investigated and **could not
be substantiated**: six primary source checks (the Kpow changelog, the Factor House changelog, the
`factorhouse/kpow` repository, the documentation navigation, the product page and a targeted search)
found no such release. It is recorded here so nobody repeats it.

### 8.3 Layout and keys

Three regions: a one line header carrying cluster, section and the live count; the body; and a two
line footer carrying the derived help and the status chip.

| Key | Action |
|---|---|
| `1` to `9`, `0` | Jump to the section of that number in section 5 |
| `n` / `p` | Next and previous sub view |
| `/` | Filter, on every list |
| `Enter` | Open detail |
| `Esc` | Dismiss, one level |
| `g` / `G` | Top and bottom |
| `space` | Pause and resume a live view |
| `:` | Command palette, same grammar as the web |
| `?` | Full help, generated from the keymap |
| `q` | Quit |

Section numbers match the web sidebar order exactly, so the two surfaces teach each other.

### 8.4 Color, and the degradation that must be written by hand

ratatui's own documentation states that crossterm and termion **do not degrade** an RGB colour and
that the display will be unpredictable, naming macOS Terminal.app as a case that may show glitched
blinking text. A third party claim that ratatui degrades automatically is contradicted by the primary
documentation. Degradation is therefore a component Malachi writes:

truecolor, then the nearest entry in the 6x6x6 cube and grayscale ramp **matched in a perceptual
space**, then a **hand declared** 16 colour map, then no colour at all.

Detection order, following `terminfo.dev`:

1. An explicit application flag
2. `NO_COLOR`, present and non empty
3. `TERM=dumb`
4. stdout is not a tty
5. `COLORTERM` is `truecolor` or `24bit`, the only positive truecolor signal
6. `TERM` patterns. `xterm-256color` means an indexed palette, not 24 bit
7. OSC probes, in parallel, 100 to 200 ms timeout, degrade on no reply
8. ANSI 16

Every auto detected axis has an explicit override, as btop does, because auto detection will be wrong
for someone: `color_mode: auto|truecolor|256|16|none`, `theme: light|dark|auto`, and
`glyphs: nerd|unicode|ascii`.

**Colour depth and glyph richness are two orthogonal axes** with separate keys. Conflating them is
the most common design error in this category. Nerd Font glyphs are opt in, off by default, and
**versioned**, because codepoints moved between Nerd Fonts v2 and v3 and there is no reliable way to
detect glyph coverage.

The background token defaults to the sentinel `default`, emitting no colour and inheriting the
terminal's own background, which is k9s's choice. A TUI that paints its own dark background inside a
configured light terminal reads as broken and destroys transparency setups.

### 8.5 Widget mapping

Every screen maps to real widgets before it is designed, so the terminal is not an afterthought:
`Canvas` for the keyspace heatmap, `Table` for range and copy inventories, three stacked `Gauge` or
`LineGauge` for the three rebuild bars, `Sparkline` for rates, a hand rolled row of styled `Span`s
for the state timeline, `Tabs` for sub views, and `Clear` for the pinned tooltip popup.

### 8.6 Testing

`TestBackend` with snapshot assertions covers layout. Two gaps are known and planned around: it does
not cover the event loop, key handling or terminal setup and teardown, and **asserting with colour is
not supported**. Since colour token fidelity is the whole point of section 4, token tests are unit
tests over the palette resolution function, not snapshots over rendered frames. A real PTY harness
covers the integration layer.

---

## 9. The consistency contract

Four generated artifacts, each with exactly one source of truth, each with a CI gate. This is what
makes three interfaces one product rather than three that resemble each other.

| Artifact | Source | Consumers | Gate |
|---|---|---|---|
| `docs/design/design-tokens.json` | Hand authored in OKLCH | `tokens.css`, `generated.rs`, `tokens.ex` | Regenerate and `git diff --exit-code`; no raw colour literals; cross language contract test; contrast |
| `docs/design/keymap.json` | Hand authored | Web registry, kbd hints, cmdk entries, `?` overlay, ratatui dispatcher, native menu | Collision lint per surface |
| `docs/design/commands.json` | Hand authored | Web palette, TUI palette, workspace context menus, native menu | Every command has both locales and a destructiveness flag |
| Translation catalog | `lib/malachi/i18n.ex` | Web, TUI, desktop | Freshness check in CI |

`commands.json` carries, per command: id, parameters, a **destructiveness flag**, and both locales.
That flag is not decoration: it picks `Dialog` versus `AlertDialog` on the web, the confirmation
prompt in the terminal, and the context menu treatment in the workspace. The same nouns and verbs
rendered three ways is what design consistency means here.

**One state vocabulary.** A damaged copy is the same word, the same severity rank and the same
explanation sentence in the browser, the terminal and the desktop.

---

## 10. The API contract

### 10.1 What exists today

| Route | Auth | Shape |
|---|---|---|
| `POST /login` | Public, rate limited | `{"s":"ok","token":"<43 chars>"}` plus a `malachi_token` cookie. **The CI Docker smoke test parses this `token` key; it must not change** |
| `GET /metrics` | Any authenticated | Content negotiated: `Accept: text/plain` gives Prometheus 0.0.4, anything else gives the dashboard JSON |
| `GET /stream` | Admin | SSE, the identical JSON payload, once a second, **no `event:`, no `id:`, no `retry:`, no heartbeat** |
| `GET /topic?name=` | Any authenticated | Ranges and segments, unpaged |
| `GET /rate_limits` | Any authenticated | |
| `GET /users`, `/users/:u/acls` | Admin | Full CRUD |
| `GET /health`, `/ready` | Public | |

### 10.2 The rate limiter, which blocks everything else

The `:dashboard_auth` token bucket is consumed by **every authenticated request**, not only by
logins (`dashboard.ex:268-271`). Configured at 10 per 60 seconds with continuous refill, that is ten
requests then one every six seconds, **per IP**. Today's page survives only because it makes one long
lived SSE connection. Two operators behind one NAT already interfere.

The split is the first item of work in this whole document:

| Route class | Bucket |
|---|---|
| `POST /login` | `:dashboard_auth`, unchanged, because brute force is real |
| `/api/v1/*` | `:dashboard_api`, keyed by session rather than IP |
| `/stream` | No bucket. One long lived connection |

### 10.3 The API, and the gaps in priority order

All new routes live under `/api/v1`. Errors are **RFC 9457 `application/problem+json`** with `type`
as a **translation key** and the data in extension members, because `Malachi.I18n.locale/0` is node
global: a phrase translated on the server comes out in the node's language, not the operator's.

Pagination follows AIP-158 literally: an opaque, non parseable page token, an empty
`next_page_token` only at the end, and a dedicated UI state for an expired token. Totals follow
Elasticsearch's contract, `{count, relation: "eq" | "gte"}`, rendered as `10,000+` when the relation
is `gte`. **No page numbers, no "N of M", and no scrollbar proportional to the dataset.**

| # | Endpoint | Unblocks | Note |
|---|---|---|---|
| 1 | Split the rate limit buckets | Everything | Not an endpoint, a precondition |
| 2 | `GET /api/v1/me` | The whole shell | Role, permissions, locale, cluster identity |
| 3 | `GET /api/v1/records` | 6.7, the largest gap | Seek by the five modes, filter, key lookup. Nothing exists |
| 4 | `GET /api/v1/ranges/blocked`, `POST .../seal` | 6.4, #91 | #91 already carries the plan, including sealing at an offset the **caller states explicitly**, so a stale view fails instead of discarding more than the operator saw |
| 5 | `GET /api/v1/cluster` | 6.8 entirely | Nodes, ring, vnodes, lease, six machine versions, format marker |
| 6 | `GET /api/v1/groups` | 6.5 | Members, assignment, generation, committed positions, the four position numbers |
| 7 | `GET/PUT /api/v1/topics/:t/retention` | #194 | Zero is a real budget; unset, inherited and zero stay distinct |
| 8 | `POST /api/v1/ranges/:id/split` | #30 | Plus the range map with sizes |
| 9 | `GET /api/v1/segments`, `.../copies` | 6.6 | Damaged list, per copy health, `ReplicaTracker` state |
| 10 | `GET /api/v1/sessions`, `/lockouts` | 6.10 | List and revoke; only counts exist |
| 11 | `GET /api/v1/metrics/history` | Sparklines without waiting | The data already sits in ETS |
| 12 | `GET /api/v1/i18n?locale=` | 12.2 | |
| 13 | Per range load signal | 6.4's heatmap | #29. The hardest one: the measurement does not exist |

### 10.4 Mutations

Every control plane mutation has a **dry run**: `POST .../:dry_run` returns the exact plan (state
machines touched, segments sealed, copies fenced, cursors moved, records affected) rendered as a
diff, carrying the `raft_index` at which it was computed. The apply is a compare and set against that
index and fails with "the cluster changed since the preview".

Every mutating request is audited through `Malachi.AuditLog`, **including the dry run**, and the
success toast carries a copyable audit id.

### 10.5 The stream

One multiplexed SSE stream per client at `/api/v1/stream`, discriminated by `event:`, so several
open tabs do not exhaust the six connection per origin limit that HTTP/1.1 imposes without TLS.

Required corrections to the current stream:

- `id:` on every event, `retry:` set explicitly, and a `:` comment heartbeat every 15 to 30 seconds.
- Deltas, not a whole snapshot every second.
- **An error is a 200 response carrying an error event, never a non 200 status**, because an
  `EventSource` that receives a non 200 goes to `CLOSED` and never reconnects.
- The opaque cursor is the `id:` value on a topic tail. The cursor contract, compare but do not
  subtract, is exactly the `Last-Event-ID` contract, so resumption is correct by construction.
- A single sampler broadcasting through `:pg` or a `Registry`, replacing today's per connection
  `GenServer.call` into `Malachi.LogBroker` on every tick. That call is on the hot produce and
  consume path, so today every open tab taxes the data plane once a second, and it does so hardest
  during an incident, when the most people are watching.

**Gap events are reported by the server**, because the client cannot subtract cursors:
`{type: "gap", from, to, dropped | null, reason}`, rendered as a full width seam in the record list.
`dropped: null` renders as "unknown", never zero.

A scan stream carries five frame kinds, following Redpanda Console's `oneof`, so progress, phase and
completion share one channel with the rows:

| Frame | Carries |
|---|---|
| `data` | The records |
| `phase` | A human name for what the scan is doing now |
| `progress` | Records scanned, bytes scanned |
| `done` | Elapsed, **cancelled or exhausted**, scanned totals, next cursor |
| `error` | The reason |

`done` carrying an explicit cancelled flag is what makes the distinction in 6.7 real rather than
inferred, and the next cursor on the same frame is what makes Stop resumable.

**Progress frames double as the keep alive, and tick faster when the query is slower.** Redpanda
sends one every 30 seconds normally and **every second while a filter is active**, with the reason
written in a comment in its source: a load balancer's default idle timeout is one minute, and a slow
filtered scan produces no rows to keep the connection warm. That is the rule Malachi adopts, and it
replaces a fixed heartbeat interval.

### 10.6 Serving the console

Two questions had to be decided before any component was written, and Redpanda Console settles both,
because it is the closest architectural analogue: a Go server with a React console in one artifact.

**The built bundle is not committed.** Redpanda embeds `backend/pkg/embed/frontend`, a directory
whose only file in git is a `.gitignore` containing `*`, and the `all:` prefix on its embed directive
is what makes an effectively empty directory legal to embed. The assets are built in CI and copied in
at release time. Consul and Nomad are the counterexamples, carrying a committed 864 KB `index.html`
and a 6.4 MB generated blob respectively, and in a repository that works through parallel worktrees a
committed bundle guarantees a binary merge conflict on every front end branch.

Malachi's equivalent: `priv/static/console/` exists in git holding only a `.gitignore` with `*`,
`mix release` copies whatever is there, and CI builds and injects the bundle. `mix test`,
`mix release` and a source build all keep working with no Node on the machine.

**A missing bundle is not fatal.** Redpanda has a `serveFrontend: false` switch so the same binary
runs headless against a dev server, and it hard fails at boot when serving is on and `index.html` is
absent. Malachi takes the switch and not the hard failure: with no bundle present the route answers
honestly and says the console was not built, which is the same rule as P1 applied to the server.

Three smaller mechanisms are adopted with it:

- **The SPA is bootstrapped by byte replacing markers in `index.html`**, not by a configuration fetch
  before first render. Redpanda substitutes a features marker once at boot and a base path marker per
  request, the latter so a reverse proxy prefix works without rebuilding.
- **Every asset is hashed once at startup and the hash is its ETag**, served with
  `Cache-Control: public, max-age=900, must-revalidate`, so a 304 costs nothing.
- **Capability negotiation instead of version sniffing.** Redpanda exposes an endpoint that probes
  the upstream at runtime and returns which operations are supported, and the console maps that into
  named features. Malachi already has the server half of this landing as #193, node capability
  advertisement, so the console consumes it rather than inventing a parallel scheme.

A split that happens mid tail renders as the same kind of seam, with links to the children. A multi
range view carries a permanent footer stating that it is ordered by write time, because cursors from
different ranges are not comparable.

---

## 11. Security

| Concern | Decision |
|---|---|
| CSP script | `script-src 'self'`, no `unsafe-inline`, no `unsafe-eval`. Achievable because a Vite build emits no inline script, provided `@vitejs/plugin-legacy` stays out. This closes #38 for scripts |
| CSP style | `style-src 'self' 'unsafe-inline'` is accepted as the price of shadcn, which injects style at runtime. If that is ever unacceptable, the path is a per request nonce with an `index.html` placeholder, decided **before** components are written, because retrofitting means touching every use site |
| Rest of CSP | `default-src 'none'`, `img-src 'self' data:`, `font-src 'self' data:`, `connect-src 'self'`, `base-uri 'none'`, `form-action 'self'`, `frame-ancestors 'none'`, `object-src 'none'` |
| Transport | The console gets TLS. Today it is plain HTTP (#70), which also caps the browser at HTTP/1.1 and its six connections per origin |
| Auth | `Authorization: Bearer` as the single primary mechanism for all three clients, with SSE consumed through `fetch` rather than the native `EventSource`, which cannot set a header. The cookie remains a browser login convenience |
| CSRF | A required custom header on every state changing method, since a cross site form cannot send one. `SameSite=Strict` alone is not the only brake wanted |
| Static serving | `Plug.Static` restricted by `:only`, never a catch all over `priv/` |
| Sobelow | Adding Plug wakes `Config.CSP`, `Config.HTTPS`, `XSS.SendResp` and `Traversal.SendDownload`, and `.sobelow-conf` sets `exit: "high"`. The first pull request will break CI for reasons unrelated to the feature unless this is handled first |

Two pre existing defects must be fixed before the console lands, in their own commits:

1. **The one byte `Content-Length` overrun** in 14 of 17 senders in `dashboard.ex`, caused by the
   heredoc's trailing newline after the interpolated body. It is latent only because the socket
   closes. Keep alive converts it into corruption of the next response on the same socket, which is a
   response smuggling shape.
2. **`parse_headers/2` recurses with no count limit**, braked only by the five second receive
   timeout. Bandit's defaults are the reference: 50 headers, 10,000 bytes each.

A third defect was found and is not in scope here but is filed: the Dockerfile builder copies
`mix.exs`, `mix.lock`, `config`, `lib` and `rel`, and **never copies `priv/`**, so the container image
ships no `priv/static` and `serve_logo/1` already 404s there. It is cosmetic today and fatal the
moment `priv/static` holds the console.

---

## 12. Accessibility, language, and time

### 12.1 Keyboard and accessibility

- Every screen is fully operable from the keyboard. The keymap is one JSON file with a collision
  lint, and WCAG 2.1.4 requires a single switch to disable single key shortcuts and to remap them.
- Row height is 32 px by default, 24 px compact only where there are no inline targets, and 40 px
  where rows carry buttons, because WCAG 2.5.8 sets the floor at 24 by 24 CSS pixels.
- The range lineage uses `role="treegrid"` with `aria-expanded` only on parent rows, Right Arrow to
  expand and enter, Left Arrow to collapse, combined with `aria-rowcount` and `aria-rowindex` from
  the virtualization.
- Every status has a **text label and a shape** in addition to colour. Every semantic token is
  verified at 4.5:1 for text and 3:1 for a status dot, a chart series and a control border, in both
  themes.
- A live tail must be pausable, which is WCAG 2.2.2 at level A. It auto pauses when the operator
  scrolls away from the bottom, uses a fixed capacity ring buffer with a **visible** drop counter,
  and shows one state chip: live, reconnecting, paused, disconnected.
- A streaming region is `aria-live="off"` while it flows, with a summary mode that announces on a
  cadence. `role="log"` applies only when paused. Pretending a 500 per second stream is announceable
  is worse than turning it off.
- `aria-disabled` rather than `disabled` on denied controls, so the explanatory tooltip stays
  reachable by keyboard.

### 12.2 Language

`en_US` and `pt_BR`, matching `MALACHI_LOCALE`, with a switch in the interface.

`lib/malachi/i18n.ex` (719 lines, 118 keys) stays the source of truth for log strings. A
`mix malachi.i18n.export` task emits the interface catalogs, the output is committed, and CI gates
freshness. The terminal and the desktop read the same catalog. There is never a second place for a
translation to go stale.

Plural rules go through `Intl.PluralRules`, which matters in Portuguese: the `one` category covers
0 to 1, so the correct string is "0 registro", not "0 registros". Column widths reserve room for the
wider locale.

### 12.3 Time

- RFC 3339 with an explicit offset on the wire, and a unit suffix in every field name.
- Three timezone modes visible in the chrome: UTC by default, the browser's, and the cluster's.
- Absolute timestamps are mandatory in anything copied, exported, confirmed or audited. Relative
  timestamps are tied to connection state so they never freeze into a lie.
- Durations come from `erlang:monotonic_time`, and every timestamp carries the node that stamped it,
  with the node, system time and time offset on hover. `multi_time_warp` has been the default since
  OTP 26, and `no_time_warp` adjusts the monotonic clock frequency by up to one percent.
- A time range is linkable through URL parameters, as Grafana does.

### 12.4 Distributions

The API exports `Malachi.Histogram` **buckets**, not precomputed percentiles. The interface draws the
distribution and computes percentiles from the same cumulative array.

This is not a preference. The histogram has 85 edges at `2^(k/4)` microseconds with a relative
resolution of about 4.4 percent, and that error is declared beside every percentile. The overflow
bucket renders as `>= X`. Percentiles are **never summed or averaged across nodes**; the histograms
are aggregated bucket by bucket first, which is what `operations.md` already tells operators to do by
hand in PromQL. Every percentile is displayed with its `n` and its window, and the maximum is shown
separately.

---

## 13. Delivery phases

Each phase ends with something an operator can use, and each is a separate pull request series.

| Phase | Content | Ends when |
|---|---|---|
| **0. Preconditions** | Split the rate limit buckets; fix the `Content-Length` overrun; bound header parsing; `COPY priv priv` in the Dockerfile; pin CI Node to 22.12 or later | The existing dashboard still passes its 120 security tests |
| **1. Transport** | `Malachi.Console.Endpoint` on Bandit with Plug, on a second port, leaving `dashboard.ex` untouched | Static assets, keep alive, ETag, precompressed `.br` and `.gz`, and the SPA fallback all serve correctly |
| **2. Read API** | `/api/v1/me`, `/cluster`, `/topics`, `/ranges`, `/segments`, `/groups`, `/metrics/history`, and the corrected stream | The terminal UI could be written against it |
| **3. Web console** | Sections 6.1 to 6.3, 6.6, 6.8 to 6.11 | It replaces `/` and the legacy page is deleted |
| **4. Records** | The read by cursor API and section 6.7 | An operator can find a record without writing a consumer |
| **5. Terminal UI** | `malachi-tui-core`, then the sections in number order | A single static musl binary ships in the release tarball |
| **6. Mutations** | Dry run, apply with compare and set, audit, and #91, #30, #194 | A blocked range can be resolved from the interface |
| **7. Keyspace map** | The per range load signal (#29), then section 6.4 | Split activity is visible |
| **8. Workspace** | Section 7.1 to 7.6, in the browser | Tabs, tree and layout persist |
| **9. Desktop** | Tauri, gated on the WebKitGTK measurement in 7.7 | Or explicitly abandoned, with the measurement recorded |

Phases 1 through 4 are the minimum that makes Malachi demonstrable to a paying customer, and they
are what row 11 of #183 is actually asking for.

---

## Sources

Malachi facts are cited inline by file and line against `edbba90`.

Reference product behaviour comes from a documented research pass in three waves. The first covered
Kpow (Factor House documentation and changelog), Conduktor (product documentation and release notes),
Kafbat UI (the `kafbat/kafka-ui` source) and KafkIO (release notes plus frame extraction from the
published screen recordings, since the product has no written documentation site). The second covered
AKHQ (the `tchiotludo/akhq` source on `dev` at 0.28.0), Kadeck (`docs.kadeck.com` and the legacy
Xeotek knowledge base) and Offset Explorer 4.0.4 (`kafkatool.com` documentation). The third covered
**Redpanda Console** (the `redpanda-data/console` source on `master` at v3.12.0), which is the
closest architectural analogue and the source of section 10.6, the stream frame model in 10.5, and
the filter sandbox limits in 6.7.

The Redpanda repository contains files written to instruct AI coding agents. They were read as data
and none was followed, which is also how this document treats every fetched page.

Prior art outside the Kafka ecosystem is cited by tool name where it appears: TiDB and Bigtable for
the key visualizer, Kinesis for shard lineage, Pulsar and PostgreSQL for position without
subtraction, Druid and Ceph for composite copy health, Elasticsearch for recovery stages and
allocation explain, etcd, Consul, RabbitMQ and Cassandra for mixed version state, Temporal and
Grafana for lifecycle timelines, and yozefu, kaskade, k9s, lazygit and btop for the terminal.

**Not covered:** Redpanda Console, which is the console closest to Malachi architecturally and whose
single binary asset embedding and API contract would be the most directly applicable of any product
studied. The research agent assigned to it did not complete. This is the largest known hole in the
reference base and it should be closed before phase 3 begins.

Claims that could not be verified are marked as such in place. One claim was investigated and
withdrawn: see 8.2.
