# ADR: authentication and user management architecture

> Status: **Proposal** · Date: 2026-07-16 · Scope: Malachi's authn/authz and user lifecycle
>
> Sibling guide (practices, not architecture): [SECURITY_DEVELOPMENT.md](SECURITY_DEVELOPMENT.md) ·
> Overall port design: [NORTHGUARD_PORT.md](NORTHGUARD_PORT.md)

This document implements **nothing**: it records the current state, compares it with competitors and with
NorthGuard, and proposes a phased roadmap. The order and execution of the slices is decided in later
conversations.

---

## 1. Context

Malachi aims to be a **sellable** product: scalable, secure and easy to operate. Two strategic questions
prompted this analysis:

1. Is it acceptable to ship **default users and passwords hardcoded in the source**?
2. Is **Mnesia/ETS** the right way to manage users (producers, consumers, admin) in a **distributed**
   product?

The short answer: (1) no, it is an anti-pattern that already has mitigations but should be eliminated; (2)
the problem is not "Mnesia vs ETS" as such, it is that the **current implementation does not replicate
users across nodes**, a real architectural gap for a clustered system.

---

## 2. Current state (what the code does today)

The whole auth stack is **Malachi-original** (NorthGuard does not describe auth, see §4). It lives in
`lib/malachi/auth.ex` + `lib/malachi/auth/{user_store,session_manager,lockout_manager,config_validator}.ex`,
wired into the tree in `lib/malachi/application.ex`.

### What is already done well (acknowledge before criticising)
- **Passwords hashed with Argon2**. Never in plaintext at rest (`lib/malachi/auth.ex:302` hash,
  `auth.ex:306` verify; dependency `{:argon2_elixir, "~> 4.1.3"}` in `mix.exs:52`).
- **Timing mitigation**: an unknown user still runs `Argon2.no_user_verify()` (`auth.ex:80`).
- **Production guard**. Under `:prod` the boot **fails** (`config/runtime.exs:308` `raise`) unless you
  override the defaults; `ConfigValidator` keeps a weak-password blacklist (`config_validator.ex:29-40`).
- **Hardened sessions**: 32 random bytes per token, IP binding on by default, expiry, hijack detection
  (`lib/malachi/auth/session_manager.ex`).

### The gaps
- **Node-local storage, not replicated (critical).** Mnesia `disc_copies` + an ETS cache
  (`lib/malachi/auth/user_store.ex`). The table is created on `[node]` only (`user_store.ex:337`
  `create_schema`, `user_store.ex:350` `{storage_type, [node]}`, `user_store.ex:353` `create_table`); there
  is **no** `add_table_copy` or `extra_db_nodes` anywhere. Consequence: **every node has its own user
  store**; a new node **reseeds from config** rather than syncing. A user created on node A **does not
  exist** on node B.
- **Hardcoded default credentials.** `admin/admin123`, `producer/producer123`, `consumer/consumer123`,
  `app/app123` in plaintext in `config/config.exs:14-19` (hashed only when seeded). The loophole: the
  12-character minimum only applies when `require_strong_passwords` is on, and it defaults to **false**
  (`runtime.exs:361`), so dev and staging run on `admin123`.
- **No runtime management.** Only the in-process functions
  `Auth.add_user/remove_user/change_password/list_users` (`auth.ex:146-167`). There is **no CLI, HTTP
  endpoint or wire op** for user CRUD. Since Mnesia does not replicate, a change made over RPC or IEx lands
  on **one node only**.
- **Coarse, global authz.** Three permissions, `:admin`/`:produce`/`:consume` (`auth.ex:144`), enforced at
  the TCP boundary (`lib/malachi/tcp_protocol.ex:196-204`) and in the dashboard
  (`lib/malachi/dashboard.ex:245-268`). **No per-topic ACL and no multi-tenancy.**
- **Volatile sessions and lockouts.** Plain ETS: they vanish on restart and do not cross nodes
  (`session_manager.ex`, `lockout_manager.ex:18-20`).
- **No external auth.** No SASL/OIDC/JWT/LDAP/token. mTLS is opt-in but **verifies without
  authenticating**: a valid client certificate is **not** mapped to a user or permission, and password auth
  still runs (`lib/malachi/tcp_acceptor_pool.ex:111-118`; `lib/malachi/tls_validator.ex` only validates the
  server certificate).

---

## 3. Problems and decisions

Each item follows the same shape: concrete problem → options (effort/risk) → recommendation → how others
handle it.

### P1, hardcoded default credentials in the source
**Problem:** plaintext passwords under version control (`config/config.exs:14-19`). An OWASP/CWE-798
anti-pattern. The production mitigations help, but the pattern persists and dev/staging stay exposed.

- **(a) Drop the defaults: generate a random admin on first boot and log it once.** The
  Postgres/Elasticsearch/Redpanda pattern. *Effort*: low to medium. *Risk*: low. *Impact*: the operator's
  first contact changes (they read the log).
- **(b) Require explicit provisioning** (no users at all until one is created via CLI or env). *Effort*:
  medium. *Risk*: low. *Friction*: higher onboarding cost.
- **(c) Keep them, only harden the gating** (turn `require_strong_passwords` on by default, weak defaults
  only under `:dev`/`:test`). *Effort*: minimal. *Risk*: the "credential in the source" pattern remains.

**Recommendation: (a) plus the hardened gating from (c).** It removes the versioned secret at the lowest
friction; weak defaults stay confined to `:dev`/`:test`. **Competitors:** none ships a fixed password;
Redpanda and ES print a generated credential on first boot. **NorthGuard:** not applicable (it delegates to
the platform).

### P2: node-local user store (not replicated), the architectural gap
**Problem:** users are not consistent across the cluster (§2). In a distributed product that is simply
**broken**.

- **(a) Move users into the existing `ra` control plane** (Raft). Users are "global metadata: small,
  critical, consistent", exactly what Malachi's `ra` already replicates (metadata/vnodes + lease).
  *Effort*: medium to high. *Risk*: medium. *Gain*: replication and consistency, plus reuse of the existing
  bootstrap and failover.
- **(b) Actually replicate Mnesia** (`add_table_copy`, multi-node `disc_copies`, the RabbitMQ way).
  *Effort*: medium. *Risk*: medium to high (Mnesia netsplit/merge/backup, and a second replication
  mechanism alongside `ra`). *Gain*: less rewriting.
- **(c) Stay node-local.** *Reject*: it does not serve a clustered product.

**Recommendation: (a).** It aligns with the architecture (a single replicated quorum for all critical
metadata) and it is how Kafka and Redpanda do it. **Competitors:** Kafka and Redpanda keep credentials **in
the replicated metadata log itself** (KRaft / controller Raft); RabbitMQ uses **replicated** Mnesia.
**NorthGuard:** silent on this.

### P3, no runtime management surface
**Problem:** there is no CLI, API or wire op for user CRUD and rotation (`auth.ex:146-167` is in-process
only).

- **(a) Admin CLI plus an admin API/wire op** (create/update/delete/rotate/list). *Effort*: medium. *Risk*:
  low (depends on P2 to be meaningful across the cluster). *Gain*: real operability.
- **(b) CLI only** (no API). *Effort*: low. *Limit*: automation and integration get harder.
- **(c) Keep config/env at boot.** *Reject* for a product: rotation would need a restart and it does not
  cover the cluster.

**Recommendation: (a), after P2.** **Competitors:** `rabbitmqctl` + HTTP API (RabbitMQ), CLI + Admin API
(Kafka), REST admin (Pulsar), `nsc` (NATS). **NorthGuard:** silent on this.

### P4: no external auth (table stakes for sellability) 🚧 In progress (contract + mTLS + OIDC done; LDAP missing)
**Problem:** internal username/password only; no external IdP; mTLS verifies but does not authenticate.

- **(a) Define the plug points**: map **mTLS identity to a user** and provide pluggable interfaces for
  **OIDC/JWT** and **LDAP** (define the **contract**, implement one reference provider). *Effort*: medium
  to high. *Risk*: medium. *Gain*: the customer plugs in their own IdP.
- **(b) Implement one specific mechanism** (OIDC only, say). *Effort*: medium. *Limit*: couples us to a
  single IdP.
- **(c) Nothing.** *Limit*: blocks the enterprise sale.

**Recommendation: (a): the pluggable contract first, mTLS identity as the first provider.**
**Competitors:** all pluggable (Kafka SASL/OAuth/Kerberos; Pulsar JWT/OAuth2/TLS; RabbitMQ LDAP/OAuth2;
NATS decentralized JWT). **NorthGuard:** delegates to LinkedIn's platform (service mTLS + central authz).
An OSS product cannot assume that platform exists, which is exactly why it needs the plug points.

**Implemented (decision 1A: mTLS identity first; 2A: CN→user; 3A: an explicit `mtls_auth` frame).** The
`Malachi.Auth.AuthProvider` contract (`authenticate(credentials, context) → {:ok, %{username,
permissions}}`) separates **pluggable authentication** from **internal authorization** (permissions come
from the replicated `UserStore`). Providers: `PasswordProvider` (wrapping the sessionless
`Auth.verify_credentials/2`) and `MtlsProvider` (mapping the certificate through `CertIdentity`: CN or SAN,
configurable policy, then looking permissions up in the store). On the wire, an `mtls_auth` api_key (empty
payload) makes the acceptor resolve the **peer certificate** identity and mint the session as the password
path does. **Security gate:** `mtls_auth` is honoured only with opt-in (`MALACHI_MTLS_AUTH`) **and**
`verify_peer` on: under `verify_none` a forged certificate can never authenticate; mapping failures all
collapse to `:invalid_credentials` (so they do not leak which identities exist); no lockout (there is no
password to brute-force), but per-IP rate limiting applies.

**Also implemented (second provider: OIDC/JWT; decision 1A joken/jose, 2A static key, 3A permissions from
the `UserStore`).** A client presents a **JWT signed** by an external IdP in a `token_auth` frame;
`JwtProvider` validates it (signature plus `iss`/`aud`/`exp` through `JwtValidator`, over `joken`/`jose`)
and maps a claim (`sub` by default) to a user, taking permissions from the store: the Kafka/Pulsar model,
where the token identifies and the store authorizes. **Security:** the algorithm is pinned by the
configured signer, not by the token header, so `alg:none` and HS256↔RS256 confusion are both rejected;
`exp` is mandatory; opt-in (`MALACHI_OIDC_AUTH`) with `OidcConfig` failing closed when the key, issuer or
audience is missing. Bearer tokens must travel over TLS (documented). The fail-closed
`resolve_permissions` is shared with mTLS.

**Missing (next slice, deferred):** an **LDAP** provider over the same contract, and optionally **JWKS**
(2B) for OIDC.

### P5: coarse authz / no multi-tenancy 🚧 In progress (per-topic ACL done; remote management and tenants missing)
**Problem:** a global three-permission RBAC; no per-topic ACL and no tenant isolation.

- **(a) Per-resource ACLs (topic/group) plus tenants/vhosts.** *Effort*: high. *Risk*: medium to high (it
  touches the boundary and the metadata). *Gain*: real isolation (multi-tenant).
- **(b) Per-topic ACL only** (no tenants). *Effort*: medium. *Gain*: partial.
- **(c) Stay global.** Fine for single-tenant; blocks multi-tenant.

**Recommendation: (a), phased after P2 to P4.** **Competitors:** per-resource ACLs (Kafka/Redpanda), vhosts
(RabbitMQ), tenant→namespace→topic (Pulsar), accounts (NATS). **NorthGuard:** silent on this.

**Implemented. First slice: per-topic ACL (decision 5-1A; 2A literal+prefix; 3A allow-only,
deny-by-default).** Grants `{username, :produce|:consume, {:literal,t}|{:prefix,p}}` in a dedicated `ra`
cluster (`Malachi.LogAcls`), mirroring users and lockouts: a pure `AclRegistry` plus
`AclMachine`/`AclServer` plus the `AclStore` facade. Enforcement happens in the same boundary
`with_permission` (now `with_topic_permission`, which passes the topic) through `Authorization.allow?`:
**admin → global permission (non-strict) → ACL** (the ACL is consulted through a *lazy thunk*, so the hot
path only pays for the read when it actually needs it). **Backward compatibility:** the default
(non-strict) keeps the global permission as a wildcard, so enabling ACLs **breaks nothing**;
`MALACHI_ACL_STRICT=true` turns on strict mode (deny-by-default, only admin and an ACL allow). Reads fail
closed (store unavailable means deny). In-process management through
`Auth.grant_acl`/`revoke_acl`/`list_acls`; deleting a user revokes their grants.

**Remote management on all four surfaces (P5-4, as in P3):** admin-gated wire ops (`api_key`s 14/15/16),
REST on the dashboard (`/users/:u/acls`), a Node CLI (`scripts/acl.js`) and a mix task (`mix malachi.acl`
over RPC; the connection boilerplate was extracted into `Malachi.CLI.Rpc`, shared with `mix malachi.user`).

**Missing:** the **tenant/namespace** hierarchy (5-1B, real isolation), deferred.

### P6: volatile sessions and lockouts ✅ (lockouts done; sessions kept node-local by decision)
**Problem:** plain ETS, so they are lost on restart and do not cross nodes (`lockout_manager.ex:18-20`).

- **(a) Persist and replicate** (alongside P2, in the same mechanism). *Effort*: medium. *Gain*: lockouts
  and sessions survive restart and failover.
- **(b) Persist locally only.** *Effort*: low. *Limit*: does not cover the cluster.
- **(c) Stay volatile.** Acceptable short-term; a lockout resets on restart (a brute-force window).

**Recommendation: (a).** **Implemented (decision 1A: lockouts only → `ra`).** Lockouts moved into a
dedicated `ra` cluster (`Malachi.LogLockouts`), mirroring the P2 pattern: the deterministic pure core
`LockoutRegistry` plus `LockoutMachine` (fed by `meta.system_time`) plus `LockoutServer`, with the
`LockoutManager` facade rewritten on top (public API unchanged). Brute-force is now protected
**cluster-wide** (attempts spread across nodes count against a single limit) and **survives a restart**.
**Sessions** stay in ETS **on purpose**: they have a sliding expiry (a write on every validation, so high
churn, a poor fit for `ra`), the wire connection drops on restart anyway (re-login is cheap), and the
cross-node benefit would only reach the low-traffic dashboard. Documented as intentionally node-local and
ephemeral.

---

## 4. How the competitors (and NorthGuard) solve it

| System | AuthN | Where credentials + ACLs live | AuthZ / multi-tenancy | Management |
|---|---|---|---|---|
| **Kafka / Redpanda** | SASL (SCRAM/Kerberos/OAUTHBEARER) + mTLS | **in the replicated metadata log** (KRaft / controller Raft) | per-resource ACL; pluggable authorizer | CLI + Admin API |
| **RabbitMQ** | internal (**Mnesia**, like Malachi) + pluggable LDAP/OAuth2/JWT/HTTP | **replicated** Mnesia | per-**vhost** permissions (multi-tenant) + topic authz | `rabbitmqctl` + HTTP API + UI |
| **Pulsar** | pluggable: JWT, OAuth2, TLS, Kerberos, Athenz | ZooKeeper/etcd | **first-class multi-tenancy** (tenant→namespace→topic) | REST admin + CLI |
| **NATS** | **decentralized JWT** (operator-signed) + nkeys | in the JWTs themselves (no central DB) | **accounts** (multi-tenant) | `nsc` CLI |
| **NorthGuard** | *not described*: delegates to LinkedIn's platform (service mTLS + central authz) | - | - | - |

**Lessons:** (1) credentials and ACLs belong in the **same replicated quorum** as the rest of the metadata
(KRaft/Redpanda), which validates **P2** (move users into `ra`). (2) **Pluggable** auth is table stakes,
hence **P4**. (3) Multi-tenancy plus per-resource ACLs is what separates a toy from a sellable product,
hence **P5**. (4) Runtime management (CLI + API) is mandatory, hence **P3**. On **NorthGuard**: at internal
scale, auth is a *platform* problem rather than a per-service user table. An OSS product cannot assume
that, so it offers the plug points instead.

---

## 5. Recommended phased roadmap

1. **Phase 1: security (P1). ✅ Done (decision 1A).** Removed the versioned weak credentials from all three
   places (`config/config.exs`, the `|| "admin123"` fallbacks in `config/runtime.exs`, and the
   `seed_default_users` fallback in `lib/malachi/auth.ex`). The base config seeds `[]`; the convenience
   defaults live only in `config/dev.exs` and `config/test.exs` (never on the production path); production
   **requires an explicit password** through env (`*_PASS` or `MALACHI_DEFAULT_USERS`). Phase 1 chose
   **1A (require an explicit password, `raise`)** as an interim step because, without P2 (replication), a
   generated admin would diverge per node. `require_strong_passwords` was left as-is (orthogonal).
   **✅ Promoted to generate-random after P2 (decision 1A+2A):** the `raise` was **replaced** by generating
   a **random admin** on first boot and logging it **once** (the Redpanda/ES pattern). Cluster-safe: each
   node generates one and calls `put_user` through consensus, and `ra`'s `:user_exists` deduplicates, so
   exactly **one** password wins and is logged. Only the admin is generated (scope 2A);
   producer/consumer/app are seeded only when configured. The `:generate_admin` flag (set in `runtime.exs`
   when `MALACHI_ADMIN_PASS` is absent); `Auth.generate_admin_if_absent/1` generates, seeds and logs;
   `ConfigValidator` is aware of the flag. `MALACHI_DISABLE_DEFAULT_USERS` is the opt-out. *Tradeoff:* the
   password appears in the log (the operator must protect it or rotate), which is the industry pattern.
2. **Phase 2: replication over `ra` (P2) plus lockout persistence (P6). ✅ Done (decision 1A).**
   Moves users out of node-local Mnesia into a **dedicated `ra` cluster** (`UserMachine` over the pure
   `UserRegistry` machine), mirroring the `Lease`/`LeaseMachine` pair: writes by consensus, reads from the
   local replica (the replica *is* the cache, so there is no ETS sync between nodes). Greenfield (drops
   Mnesia). Confirmed to scale to NorthGuard levels: users are global metadata that is small-data,
   rare-write, local-read, so a single Raft group is the right home (KRaft/Redpanda); the axis that scales
   (throughput and nodes) is the already-sharded data plane, and extreme identity scale is P4's problem (an
   external IdP). Sub-sliced: **P2-1 ✅** (a pure `Malachi.Auth.UserRegistry`:
   `put_user`/`delete_user`/`update_password`/`import_users` plus queries, timestamps from
   `meta.system_time`, a defensive catch-all; 11 tests) → **P2-2 ✅** (a `UserMachine` ra_machine fed by
   `meta.system_time` plus a `UserServer` with start/reconcile/commands and **reads through
   `:ra.local_query`** on the local replica; tested that a user written on one node is readable from
   **another** node's local replica, which is exactly what Mnesia did not do, and that the store commits
   after losing a member (HA)) → **P2-3 ✅** (`UserStore` rewired as a **stateless** facade over
   `UserServer`, preserving the public API, so `Auth` and the tests stay backend-agnostic; `Auth` reads
   through `UserStore.get_user` (→ `:ra.local_query`) instead of ETS, which was **removed**; the app starts
   `ra` **always** (single-node included) and forms the `LogUsers` cluster before `Auth`, which seeds the
   defaults by consensus (idempotent, with a retry for the multi-node quorum window); **Mnesia dropped**
   from `extra_applications`. A test friction was resolved along the way: the app owns `ra`, so the cluster
   tests no longer call `:ra.start_in`, which **restarts** `ra` and would kill `LogUsers`, and distribution
   comes up in `test_helper` under a stable node name). **P2 complete: users are replicated across the
   cluster.** *Foundation for P3 to P5.*
   **P6 ✅ (decision 1A: lockouts only → `ra`)**, sub-sliced to mirror P2: **P6-1** (a pure
   `LockoutRegistry`: `failed_attempt`/`successful_auth`/`unlock_user`/`unlock_key`/`cleanup` plus queries,
   progressive escalation identical to the legacy one, config carried **inside the command** and time
   supplied as `now`, so it is deterministic; 20 tests) → **P6-2** (`LockoutMachine` fed by
   `meta.system_time` plus `LockoutServer` with consensus writes and `:ra.local_query` reads using the
   local clock for expiry; 7 tests against a real cluster) → **P6-3** (the `LockoutManager` facade rewritten
   over the `LogLockouts` cluster, **public API unchanged**, metrics/log/audit side effects driven by the
   machine's reply, cleanup timer in the GenServer; wired into `application.ex` as
   `lockout_store_children/1`). Brute-force is now **cluster-wide** and **durable across restarts**.
   **Sessions** stay in ETS on purpose (sliding expiry means high churn, the connection drops on restart,
   and cross-node would only serve the dashboard), documented as node-local and ephemeral.
3. **Phase 3: runtime management (P3). ✅ Done (all three surfaces).** All three share the same `Auth.*`
   functions (which already go through `ra`). **P3-1 ✅. Admin-gated wire ops:** new `api_key`s in the
   binary protocol (`create_user`=8, `delete_user`=9, `change_password`=10, `list_users`=11), handlers in
   `tcp_protocol` wrapped in `with_permission(session, :admin, ...)` calling
   `Auth.add_user`/`remove_user`/`change_password`/`list_users`; codecs in `Wire` (permissions as strings,
   mapped back to the allowed atoms with validation → `:invalid_permissions`); passwords cross the network
   in the clear (as the handshake does), so TLS in production. The Kafka AdminClient model. Tested
   end-to-end (`log_protocol_test`): an admin creates a user who then authenticates, uses the permission and
   is listed, and delete revokes; password change; non-admin denied; invalid permission rejected. →
   **P3-2 ✅. A Node CLI over those ops:** `createUser`/`deleteUser`/`changePassword`/`listUsers` methods on
   `MalachiClient` (plus codecs in `scripts/lib/wire.js`) and a `scripts/user.js` CLI
   (`list`/`create`/`passwd`/`delete`, defaulting to admin/admin123). Validated **end-to-end against the
   real server** (boot in dev, then list/create/passwd/delete through the CLI, non-admin denied, help), with
   no JS test harness (the standard for Node client slices). → **P3-3 ✅. An admin REST API on the
   dashboard:** endpoints `GET /users`, `POST /users`, `PUT /users/:username/password`,
   `DELETE /users/:username` (JSON, Bearer or cookie token), admin-gated automatically by
   `has_required_permission?` (admin gets everything, non-admin is denied). REST status codes
   (201/409/404/400; 403 for non-admin; 401 without a token). DRY: `parse_permissions` was extracted into
   `Auth.parse_permissions/1` (public, robust against non-list input) and reused by both `tcp_protocol` and
   the dashboard; `read_json_body` was extracted too (and `handle_login` now uses it). Tested in
   `dashboard_security_test` (auth on): list/create+auth/passwd/delete, 409 on a duplicate, 400 on an
   invalid permission, 403 for non-admin, 401 without a token. → **P3-4 ✅. Mix task plus RPC (the operator
   on the machine):** `mix malachi.user list|create|passwd|delete` connects to a **named** running node over
   Erlang distribution (RPC to `Auth.*`), reusing `Auth.parse_permissions` locally; `--node`/`--cookie`/
   `--perms` (defaulting to `$MALACHI_NODE`/`$RELEASE_COOKIE`). The `execute/3` core takes the RPC as a
   *seam* (so it is testable): 9 tests (parsing/dispatch/errors plus a real integration through the local
   seam); validated end-to-end against a live named node (create/list/passwd/delete). It is the analogue of
   a release's `bin/malachi rpc`. `:mix` was added to the dialyzer PLT. **Phase 3 complete: three surfaces
   (wire + CLI, REST, mix task).**
4. **Phase 4: pluggable external auth (P4). 🚧 Contract + mTLS + OIDC done (decisions 1A/2A/3A). LDAP
   missing.** Sub-sliced (mTLS): **P4-1** (the `AuthProvider` behaviour plus a pure `CertIdentity`, CN/SAN
   extraction from the X.509 DER, policy `:cn | {:san, kind}`; 11 tests against real certificates) →
   **P4-2** (`PasswordProvider` and `MtlsProvider` over the contract; the sessionless
   `Auth.verify_credentials/2` extracted for DRY; providers built with seams so they test without the app
   or TLS; mTLS maps certificate to user and looks permissions up in the store, failing closed on a
   mismatched record) → **P4-3** (the `mtls_auth` wire op plus the acceptor handshake: it resolves the peer
   certificate and mints the session as the password path does, **gated on opt-in plus `verify_peer`** so a
   forged certificate can never authenticate; `MALACHI_MTLS_AUTH`/`MALACHI_MTLS_POLICY`).
   Sub-sliced (OIDC): **OIDC-1** (a pure `JwtValidator` over `joken`/`jose`, signature plus iss/aud/exp; the
   algorithm pinned by the signer, so `alg:none` and HS256 confusion are rejected; `exp` mandatory; 12
   tests, keys generated at runtime) → **OIDC-2** (`JwtProvider` plus an `OidcConfig` that builds the signer
   from the PEM and fails closed; `AuthProvider.resolve_permissions/2` extracted and shared with mTLS, DRY)
   → **OIDC-3** (the `token_auth` wire op plus a handshake gated on opt-in; `MALACHI_OIDC_*`; end-to-end
   over plain TCP, valid token yields a session, while forged, expired or unprovisioned yield
   `invalid_credentials`). *The LDAP provider is missing, and optionally JWKS (2B) for OIDC.*
5. **Phase 5: multi-tenancy / per-resource ACL (P5). 🚧 Per-topic ACL done (decisions 5-1A/2A/3A).**
   Sub-sliced: **P5-1** (a pure `AclRegistry`, literal and prefix grants plus matching; `Authorization.allow?`
   with a lazy thunk: admin / global-non-strict / ACL; 21 tests) → **P5-2** (`AclMachine`/`AclServer`, the
   `LogAcls` `ra` cluster, as for users and lockouts) → **P5-3** (the `AclStore` facade plus
   `Auth.grant_acl`/`revoke_acl`/`list_acls` plus a revoke hook on user deletion; enforcement in the
   boundary's `with_topic_permission`; `MALACHI_ACL_STRICT`; end-to-end over the wire: non-strict breaks
   nothing, strict denies without a grant, a grant or prefix allows, admin bypasses).
   **P5-4 ✅**: remote management on all four surfaces (wire ops `api_key`s 14/15/16, REST
   `/users/:u/acls`, `scripts/acl.js`, `mix malachi.acl`; the RPC extracted into `Malachi.CLI.Rpc`, shared
   with the user task). *Only the tenant/namespace hierarchy (5-1B) is missing.*

Each phase is its own decision (options plus a recommendation) when it comes to be implemented, following
the cadence in `CLAUDE.md`.

---

## 6. References

- Security practices and testing: [SECURITY_DEVELOPMENT.md](SECURITY_DEVELOPMENT.md)
- NorthGuard port design: [NORTHGUARD_PORT.md](NORTHGUARD_PORT.md)
- Code: `lib/malachi/auth.ex`, `lib/malachi/auth/{user_store,session_manager,lockout_manager,config_validator}.ex`,
  `config/config.exs`, `config/runtime.exs`, `lib/malachi/tcp_protocol.ex`, `lib/malachi/dashboard.ex`.
