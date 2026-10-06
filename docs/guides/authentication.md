# Authentication

Malachi separates **who you are** from **what you may do**. Three mechanisms answer the first question;
all three then look up permissions in the same place.

That split is deliberate and follows Kafka and Pulsar: authentication is pluggable because organisations
already have an identity system, while authorization stays internal because the broker is the only thing
that knows what a topic is.

## The three mechanisms

| mechanism | credential | enable with |
|---|---|---|
| password | username + password | on by default |
| mTLS identity | a client certificate | `MALACHI_MTLS_AUTH=true` + `verify_peer` |
| OIDC / JWT | a signed bearer token | `MALACHI_OIDC_AUTH=true` + issuer, audience, key |

Whichever one runs, it produces a **username**, and permissions come from the user store. A certificate or
a token never carries permissions of its own. This means you provision a user once and can change how it
authenticates without touching what it can do.

```mermaid
flowchart LR
  Cred["credential (password / certificate / token)"] --> Prov["a provider checks it: who are you?"]
  Prov --> User["username"]
  User --> Store["user store: what may this user do?"]
  Store --> Sess["session with permissions"]
```

> **Analogy.** Authentication is the ID check at the door: it only decides who you are. Authorization is the
> guest list: it decides what you are allowed to do once inside. Malachi lets you swap the door (password,
> certificate, or token) without touching the guest list.

## Password

The default. Users live in a replicated store backed by `ra`, and passwords are hashed with **Argon2**.

```bash
mix malachi.user create alice s3cret --perms produce,consume
mix malachi.user list
mix malachi.user passwd alice newsecret
mix malachi.user delete alice
```

The permissions are `admin`, `produce`, `consume`. `admin` is a superuser and bypasses every later check.
They are **wire** permissions: what a client may do over the binary protocol. What an operator may do
through the dashboard and the console is a separate thing, the console role below.

Repeated failures trigger a **progressive lockout**. After `MALACHI_MAX_AUTH_ATTEMPTS` failures the
pair is locked for the base duration, and each further multiple of that attempt count escalates the
multiplier: **base → ×3 → ×9 → ×24 → ×72**, then capped. A guessing loop slows to uselessness within a few
rounds, while a legitimate user who mistypes once waits only the base duration.

Lockouts are keyed on **user *and* client IP**, not user alone, so one attacker cannot lock a real user out
of their own account by failing logins from elsewhere. The state is replicated over `ra`, so an attacker
gains nothing by reconnecting to a different node.

```
MALACHI_MAX_AUTH_ATTEMPTS      attempts before the first lock
MALACHI_LOCKOUT_DURATION_MS    base lock duration
MALACHI_PROGRESSIVE_LOCKOUT    escalate on repeat (default true)
MALACHI_MIN_PASSWORD_LEN       minimum length
MALACHI_REQUIRE_STRONG_PASSWORDS
MALACHI_AUTH_RATE_LIMIT        per-IP attempts per window
```

## mTLS identity

The client presents a certificate during the TLS handshake and the broker derives the username from it. No
password crosses the wire.

```bash
MALACHI_ENABLE_TLS=true
MALACHI_TLS_VERIFY=verify_peer
MALACHI_TLS_CACERTFILE=/etc/malachi/ca.pem
MALACHI_MTLS_AUTH=true
MALACHI_MTLS_POLICY=cn            # or san:uri, san:dns, san:email
```

`MALACHI_MTLS_POLICY` selects which field names the user: `cn` (default) uses the subject Common Name,
and the `san:` forms use the first Subject Alternative Name of that kind. `san:uri` is the one to reach for
with SPIFFE identities (`spiffe://malachi/svc-producer`).

### The safety gate that matters

mTLS auth is honoured **only** when the listener actually verifies peer certificates. With
`MALACHI_TLS_VERIFY=verify_none` a client could present any certificate it liked, so the broker refuses
the mechanism outright rather than trusting an unverified name:

| answer | meaning |
|---|---|
| `mtls_auth_disabled` | the feature is off |
| `mtls_auth_unavailable` | enabled, but the listener is not `verify_peer`, so the identity is untrustworthy |
| `no_peer_certificate` | verified listener, but the client sent no certificate |

The middle one is the interesting case: enabling `MALACHI_MTLS_AUTH` alone does nothing. Both switches
must be on.

## OIDC / JWT

The client presents a bearer token from your identity provider. The broker verifies the signature against
a public key you configure, then maps a claim to a username.

```bash
MALACHI_OIDC_AUTH=true
MALACHI_OIDC_PUBLIC_KEY_FILE=/etc/malachi/idp-public.pem
MALACHI_OIDC_ISSUER=https://idp.example.com
MALACHI_OIDC_AUDIENCE=malachi
MALACHI_OIDC_IDENTITY_CLAIM=sub     # default
MALACHI_OIDC_ALGORITHM=RS256        # default
```

Validation is deliberately strict, because the classic JWT failures are all "the library accepted
something it should not have":

- The **algorithm is pinned** by the configured signer, so a token claiming `alg: none`, or an HS256 token
  crafted to be verified with the RSA public key as an HMAC secret, is rejected.
- **Expiry is required.** A token with no `exp` is refused rather than treated as never expiring.
- Issuer and audience must both match.
- The config **fails closed**: if the key, issuer or audience is missing, the broker answers
  `oidc_misconfigured` instead of falling back to accepting tokens.

Send bearer tokens over TLS. A token is a bearer credential: whoever holds it is the user.

### Identities must still exist

A perfectly valid token for a subject with no corresponding user is rejected as `invalid_credentials`,
the same answer a wrong password gets. That is intentional: distinguishing "no such user" from "wrong
credential" tells an attacker which usernames are real.

## Console roles

The dashboard and the console decide by **console role**, not by wire permission. There are three, cluster
wide and strictly nested, the model Redpanda Console uses:

| role | adds | capabilities (`GET /api/v1/me`) |
|---|---|---|
| `viewer` | every read of the cluster's operational state: `/`, `/stream`, `/metrics`, `/topic`, `/rate_limits`, the storage policies | `read_cluster` |
| `editor` | the mutations that are not security: defining, deleting and binding storage policies | `manage_policies` |
| `admin` | users, ACLs and diagnostics | `manage_users`, `manage_acls`, `diagnostics` |

The two are orthogonal. `produce` and `consume` grant **no** console role, so an account that publishes
records cannot read the cluster's state over HTTP; and a console role grants nothing on the wire, so a
`viewer` with no wire permission cannot produce. The one bridge is the superuser: the wire `admin` is also
a console `admin`. A user with no role and no `admin` can still log in, and `GET /api/v1/me` answers it with
`role: null`, so the console can say why it shows nothing.

Assign a role on any of the four surfaces:

```bash
mix malachi.user create ops s3cret --perms "" --role viewer   # a read only operator, no wire permission
mix malachi.user role alice editor
mix malachi.user role alice none                               # remove it
node scripts/user.js role alice viewer
curl -X PUT -H "Authorization: Bearer $TOKEN" -d '{"role":"viewer"}' http://localhost:4041/users/alice/role
MALACHI_DEFAULT_USERS="admin:pw:admin;ops:pw2::viewer"         # user:password:permissions[:role]
```

Managing roles is user management, so it takes the console `admin` role over HTTP and the wire `admin`
permission over the binary protocol. Every change is audited as `user_role_changed`, with who made it.

A role change takes effect on the user's **next request** on each node, as soon as that node's replica of
the user store has applied it: the session only proves who the user is, and the role is read on each
request from the replica on the node that received it. That read is eventually consistent. A follower
behind the leader answers with the old role for the replication lag, and a node cut off from the leader by
a partition keeps the old role, and still accepts logins under it, until it rejoins. An open `/stream`
connection was authorized when it opened, so a role taken away reaches it when it reconnects or its
session expires. If a node's replica is not running, that node answers 503 (`errors.auth.unavailable`)
rather than falling back to the session.

Which route needs which role is one table, `Malachi.Console.Access.routes/0`, read by both HTTP endpoints. A
refusal is an `application/problem+json` body naming what is missing:

```json
{"type": "errors.auth.missing_role", "status": 403, "required_role": "viewer", "role": null}
```

With `MALACHI_DASHBOARD_AUTH_ENABLED=false` every request is an anonymous `admin`, as before roles existed;
it is for local work only.

### Upgrading to console roles

This is a deliberate break, announced here and in the release notes:

- An account with `admin` keeps everything, unchanged.
- An account with only `produce` and/or `consume` **loses** dashboard reads (`/metrics`, `/topic`,
  `/rate_limits`), which it only had as a side effect. Give it `viewer` if it needs them. A Prometheus
  scraper that logs in with such an account needs `viewer` (the scraper in `deploy/prometheus` uses
  `admin`).
- `MALACHI_DASHBOARD_REQUIRE_ADMIN` is gone: `/` and `/stream` take `viewer`, like every other read.
- Every HTTP error, on the dashboard as on the console, is now `application/problem+json` with `type` as a
  translation key; the old `{"s": "err", "reason": ...}` body is gone. A failed login and an invalid or
  expired session are a 401 rather than a 403.
- Roles are stored at machine version 5. While the cluster is still rolling (a member on the previous
  release, or `MALACHI_RA_MACHINE_VERSION` pinned below 5), setting a role is refused on every surface: as
  HTTP 409 `errors.cluster.upgrade_pending`, naming the version it waits for, on the dashboard and the
  console, and as the same "finish the rolling upgrade" message over the wire and in `mix malachi.user`.
  Creating a user without a role keeps working; `node scripts/user.js create --role` creates the user first
  and sets the role second, so there it leaves the user without a role. A default user that carries a role is not created at all on such a cluster, and the boot
  log says why.

## Sessions

Authentication returns a session token used for the rest of the connection.

```
MALACHI_SESSION_TIMEOUT_SEC   session TTL in seconds (default 3600)
MALACHI_SESSION_IP_BINDING    bind a session to its origin IP (default true)
```

With IP binding on, a session presented from a different address is refused and the mismatch is audited as
a hijack attempt. If your clients sit behind NAT or a proxy whose address rotates, configure
`trusted_proxy_ranges` (see `Malachi.Auth.SessionManager`) rather than turning binding off. The default is
an empty list, meaning **nothing** is trusted and binding applies to every session.

Expired sessions are reaped lazily, on the next use of the token, not by a background sweeper.

## Next

Permissions so far are global: `produce` means produce to *any* topic. To scope them per topic, see
[Per-topic ACLs](per-topic-acls.md).
