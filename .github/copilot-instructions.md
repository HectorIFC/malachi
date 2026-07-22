# Malachi - AI Coding Agent Instructions

## Project overview

Malachi is an open-source, 100% Elixir reimplementation of LinkedIn's **NorthGuard** log-storage
architecture: a CP (consistent, partition-tolerant), horizontally-scalable **log broker**. Clients speak
**topics, keys, and opaque cursors**, never partitions or offsets, so the broker can split, merge, and
restripe its storage underneath without breaking clients. The control plane is replicated by quorum (Raft
via `ra`); cluster membership uses SWIM.

It is **not** a queue/channel message system. Any mention of queues, channels, `PartitionManager`,
`AckManager`, `QueueConfig`, or `Validator` describes a former design (MalachiMQ) that was removed. If you
find such references outside historical files (`CHANGELOG.md`, release notes), they are stale.

**Runtime:** Elixir 1.19+ / OTP 28+ on the BEAM.

## The log model (read this first)

- A **topic** has a **keyspace**. A record's **key** hashes to a position in that keyspace; the position
  falls in exactly one **range** (a vnode). Records with the same key land in the same range and are
  ordered relative to each other. Omit the key and records spread across ranges (max parallelism, no
  ordering between them).
- Ranges hold **segments**; a segment is an append-only sequence of **records**.
- Clients never see a partition or an offset. Position is an **opaque cursor** (`LogApi.encode_cursor/1` /
  `decode_cursor/1`, decoded with `binary_to_term(_, [:safe])`).
- **Producing** returns a **count, not an offset**. That is what lets ranges split underneath.
- **Consuming** has three shapes: carry the cursor yourself; a **consumer group** (the server commits the
  position durably); or a **group member** (the server also assigns the member a share of the ranges and
  rebalances on join/leave).
- Streaming push uses **credit-window backpressure**: `budget = min(max, window - in_flight)`.
- Delivery is **at-least-once**; consumers must be idempotent. There is no exactly-once mode.

The guides in `docs/guides/` are the conceptual source of truth. `docs/ARCHITECTURE.md` is the design
reference.

## Architecture

### Supervision tree (`application.ex`)

`Malachi.Supervisor` is `one_for_one`. Startup order matters and is commented in `application.ex`:

```
Malachi.Supervisor (one_for_one)
├── Cluster.Supervisor            # only if MALACHI_CLUSTER_STRATEGY is set (libcluster, connectivity only)
├── Task.Supervisor              # large parallel broadcasts
├── Metrics, AuditLog            # observability first
├── AtomMonitor, MemoryMonitor
├── RateLimiter, ConnectionLimiter
├── ra lockout store             # LockoutServer + LockoutManager facade (+ reconciler when clustered)
├── ra ACL store                 # AclServer (per-topic ACLs)
├── ra user store                # UserServer (+ reconciler when clustered); precedes Auth (seeds users)
├── Auth, ConnectionRegistry
├── log stack (log_children/0)   # NorthGuard: metadata control plane, vnodes, replication,
│                                 # consumer-group coordinator (node-wide, or one per led vnode when sharded)
├── TCPAcceptorPool → TCPAcceptors (one per core)
└── Dashboard (HTTP server)
```

`ra` is started (`start_ra!/0`) before any child that forms an ra cluster, including single-node (a
1-member cluster is cheap). The user, ACL, and lockout stores each form an ra cluster across the configured
nodes.

### Module map (`lib/malachi/`), by concern

- **Log and storage:** `log.ex`, `log_api.ex`, `broker.ex`, `broker_server.ex`, `keyspace.ex`,
  `metadata.ex`, `log/record.ex`, `log/segment.ex`, `storage/segment_store.ex`, `storage/elixir_store.ex`.
- **Cluster and Raft** (`cluster/`): `metadata_machine`/`metadata_server`/`replicated_metadata`,
  `dsrsm`/`replicated_dsrsm`, `replication_server`/`replica_tracker`/`catchup`, `membership`/
  `membership_server`, `lease*` (holder/machine/server/reconciler), `placement`/`hash_ring`/`ring_topology`/
  `topology`, `reshard_*`/`split_*`/`rebalance*`/`vnode_split`, `retention*`, `failover`/`self_healing`/
  `heal_coordinator`, `auto_rebalancer`.
- **Consumer groups** (`consumer/`): `group_coordinator`, `assignment`, `coordinator_router`.
- **Auth and security** (`auth/` + top level): `auth.ex`, `audit_log.ex`, `tls_validator.ex`; per-store ra
  trios `user_*`, `acl_*`, `lockout_*` (each `machine`/`server`/`registry`/`store`); `session_manager`,
  `authorization`, `config_validator`, `cert_identity`; providers `password_provider`, `mtls_provider`,
  `jwt_provider`/`jwt_validator`, `oidc_config`, `auth_provider`.
- **Wire and networking:** `wire.ex`, `tcp_protocol.ex`, `tcp_acceptor.ex`, `tcp_acceptor_pool.ex`,
  `socket_helper.ex`, `connection_registry.ex`, `connection_limiter.ex`, `rate_limiter.ex`.
- **Observability:** `metrics.ex`, `telemetry.ex`, `metrics/prometheus.ex`, `telemetry/metrics_reporter.ex`.
- **Operations:** `application.ex`, `shutdown.ex`, `memory_monitor.ex`, `atom_monitor.ex`, `i18n.ex`,
  `config.ex`, `cli/rpc.ex`.
- **Mix tasks** (`lib/mix/tasks/`): `malachi.user`, `malachi.acl`, `malachi.reshard`.

### The `ra` (Raft) pattern

Every replicated store follows the same shape, and new ones must too:

- a **pure state machine** (`*_machine.ex`) implementing the `:ra_machine` behaviour,
- a **server** (`*_server.ex`) that starts/forms the cluster and issues commands,
- a stateless **facade** (`*_store.ex` / manager) the rest of the app calls.

Reads go through `:ra.local_query` (the local replica); writes go through consensus. **Machines must be
deterministic:** never read the wall clock, `Application.get_env`, or `node()` inside a machine. Anything
time- or config-dependent travels **inside the command** (the machine reads `meta.system_time` that the
server feeds it). The ra cluster name doubles as a **fencing token**: a second `start` of the same vnode
fails rather than duplicating.

## Wire protocol (binary, framed)

The protocol is **length-framed binary**, owned by `Malachi.Wire`. It is not newline-delimited JSON. A
request frame is `api_key::16, correlation_id::32, payload::binary`; responses are ok/error frames keyed by
the same `correlation_id`. `Wire` owns all `encode_*`/`decode_*`.

A connection **authenticates first** (in `tcp_acceptor.ex`), which creates a session via
`SessionManager`; subsequent frames are dispatched with that session (`tcp_protocol.ex`).

| api_key | Op | Permission |
|--------:|----|------------|
| 0 | auth (username/password) | - |
| 12 | mTLS auth (client cert identity) | - |
| 13 | token auth (OIDC/JWT) | - |
| 1 | create_topic | `:produce` |
| 2 | produce | `:produce` |
| 3 | fetch | `:consume` |
| 4 | commit | `:consume` |
| 5 | subscribe (enter push/stream mode) | `:consume` |
| 6 | stream_ack (credit back) | (gated at subscribe) |
| 7 | leave_group | `:consume` |
| 8-11 | create_user / delete_user / change_password / list_users | `:admin` |
| 14-16 | grant_acl / revoke_acl / list_acls | `:admin` |

Permissions are `:produce`, `:consume`, `:admin`. Access is enforced per topic through
`with_topic_permission/…` and `with_permission/…`; per-topic ACLs are managed with grant/revoke and stored
in the ra ACL cluster (strict mode via `MALACHI_ACL_STRICT`).

**Transient errors a correct client retries** (do not treat as fatal):

- `:migrating` - a range is being split or migrated; back off and retry (the fence lifts in ms).
- `:not_owner` - the request reached a node that no longer owns that range after a failover/rebalance;
  re-resolve the owner and retry.

## In-process API

Server-side and tests call `Malachi.LogApi` against the running broker `Malachi.LogBroker`:
`produce/3`, `fetch/5`, `fetch_group/5`, `commit/4`, `subscribe/…`, `create_topic/2`, plus
`encode_cursor/1` / `decode_cursor/1`. `produce/3` takes a **list** (one round trip, one quorum ack) - batch,
do not loop.

## Reference client

`scripts/` holds a Node CLI used by the guides and docker-compose: `producer.js`, `consumer.js`,
`subscriber.js`, `acl.js`, `user.js`, `loadtest.js`. It reads `MALACHI_HOST`, `MALACHI_PORT`,
`MALACHI_USER`, `MALACHI_PASS`, `MALACHI_TOPIC`, `MALACHI_LOCALE` (no `MQ` in the names). `scripts/lib/cli.js`
has the `withRetry` helper that handles `:migrating`/`:not_owner`.

## Configuration

Flow: `config/config.exs` → `config/runtime.exs` (env vars, `MALACHI_*`) → `config/test.exs` (test
overrides). **`config/runtime.exs` is the authoritative list**; it is skipped entirely under
`config_env() == :test`. Do not add a data directory or security default unconditionally there: it would
overwrite `config/test.exs`. Normalize on-disk directories through `Malachi.Config.data_dir/3`, which trims,
treats blank as absent, and **rejects a relative path in production**.

Env vars group into: network/TLS (`MALACHI_TCP_PORT`, `MALACHI_ENABLE_TLS`, `MALACHI_TLS_*`,
`MALACHI_MAX_FRAME_SIZE`); auth/sessions/lockout (`MALACHI_*_PASS`, `MALACHI_DEFAULT_USERS`,
`MALACHI_SESSION_*`, `MALACHI_MAX_AUTH_ATTEMPTS`, `MALACHI_MTLS_*`, `MALACHI_OIDC_*`); rate and connection
limits; the **log cluster** (`MALACHI_LOG_CLUSTER`, `MALACHI_LOG_NODES`, `MALACHI_LOG_REPLICATION_FACTOR`,
`MALACHI_LOG_VNODES`, `MALACHI_LOG_PLACEMENT_POLICY`/`SPREAD_BY`/`TOPOLOGY`); retention
(`MALACHI_RETENTION_*` - note `MAX_BYTES=0` is a real budget that expires every sealed segment, not
"unlimited"; the disable value is the absent variable); rebalance/lease; audit; resource monitors; and
`MALACHI_LOCALE` (`en_US` / `pt_BR`). Some settings **fail permissive** on a rename mismatch, e.g.
`MALACHI_ACL_STRICT` and `MALACHI_LOG_CLUSTER`/`MALACHI_LOG_NODES`.

### Persistence

- Log segments live under `MALACHI_LOG_DATA_DIR`.
- ra state (users, ACLs, lockouts, and, when the control plane is sharded, replicated metadata) lives under
  `MALACHI_RA_DATA_DIR`.

Both default to a path under the system temp dir. In production point them at a **persistent volume** with
**absolute** paths (the deploy manifests mount `/app/data` and use `/app/data/log` and `/app/data/ra`).

## Developer commands

```bash
mix deps.get
mix compile --warnings-as-errors
mix test                        # ExUnit; multinode tests opt in with --include multinode
mix format --check-formatted
mix credo --strict
mix dialyzer
mix deps.audit
mix docs                        # ExDoc site (published via GitHub Pages)

mix malachi.user ...            # user CRUD against the running node
mix malachi.acl ...             # per-topic ACL CRUD
mix malachi.reshard ...         # trigger/inspect resharding

make build | run | test | release
make docker-build | docker-buildx | compose-up | compose-logs | clean
./scripts/generate-dev-certs.sh # self-signed certs in priv/cert/
```

## Conventions

- **i18n:** every user-facing log message goes through `Malachi.I18n.t/2`, with `en_US` and `pt_BR`
  entries. Interpolate with bindings: `I18n.t(:key, name: value)`.
- **Language:** all code, comments, and documentation are in American English. The `i18n.ex` translation
  strings are the only place other languages appear. Do not use the em-dash character (U+2014) in code,
  docs, or commit messages; use a comma, colon, or parentheses instead.
- **Naming:** modules `Malachi.PascalCase`; functions/vars `snake_case`; module attributes `@snake_case`.
- **Error handling:** return `{:ok, result}` / `{:error, atom_reason}`; use `with` to chain validations;
  guard clauses for type safety.
- **Code quality:** 120-char lines (`.formatter.exs`); Credo strict (`.credo.exs`). Tests are
  non-negotiable; coverage threshold is **85%** (`mix.exs`, ExCoveralls). `test/` mirrors `lib/`.
- Pre-existing debt (Credo/Dialyzer findings unrelated to your change) goes in its **own commit**, never
  mixed into a feature.

## Contributing

- **Branches:** `feat/<issue>-<seq>-<desc>`, `fix/<desc>`, `docs/<desc>`, `chore/<desc>`.
- **Commits:** Conventional Commits (`feat`, `fix`, `docs`, `test`, `refactor`, `chore`, `perf`, `ci`).
- **Before committing**, all of these must pass:
  ```bash
  mix format --check-formatted
  mix credo --strict
  mix deps.audit
  mix test
  ```
- **PRs:** tests green, formatted, Credo clean, audit clean; conventional-commit title; CHANGELOG updated
  for version-bumping changes. `feat:` → minor, `fix:` → patch, `BREAKING CHANGE:` → major.

## Adding features

- **New protocol op:** add the api_key and its `encode_*`/`decode_*` in `wire.ex`; add a `dispatch` (or
  `dispatch_admin`) clause in `tcp_protocol.ex`; gate it with `with_topic_permission`/`with_permission`;
  call the matching `LogApi` function; add integration tests over the wire.
- **New replicated state:** follow the ra pattern (pure `*_machine`, `*_server`, stateless facade); keep the
  machine deterministic; feed time/config through the command; form the cluster in `application.ex` before
  the facade starts.
- **New config:** add it to `config/runtime.exs` (not inline in the `:test` block); if it is a directory,
  route it through `Malachi.Config.data_dir/3`; document it here and in the deploy manifests.
- **New metric:** add to `metrics.ex`, include it in the snapshot, and the dashboard stream picks it up.
