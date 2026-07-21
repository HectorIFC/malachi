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

## Password

The default. Users live in a replicated store backed by `ra`, and passwords are hashed with **Argon2**.

```bash
mix malachi.user create alice s3cret --perms produce,consume
mix malachi.user list
mix malachi.user passwd alice newsecret
mix malachi.user delete alice
```

The permissions are `admin`, `produce`, `consume`. `admin` is a superuser and bypasses every later check.

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
