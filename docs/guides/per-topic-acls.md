# Per-topic ACLs

[Authentication](authentication.md) ends with a user holding global permissions: `produce` lets it produce
to **any** topic. ACLs narrow that to named topics.

## The decision, in three rules

Every produce and consume runs the same check, in this order:

1. **`admin` is allowed.** Always, no further questions.
2. **If strict mode is off** (the default), a global `produce`/`consume` permission allows the operation on
   any topic, exactly as it worked before ACLs existed.
3. **Otherwise, an explicit ACL grant decides.**

Rule 2 is the compatibility hinge. Turning ACLs on changes nothing until you also turn on strict mode,
which is what lets you add grants to a running deployment before enforcing them.

In **strict mode** rules 1 and 3 are the only paths: global permissions are ignored and access is
**deny-by-default**. Only an explicit grant, or `admin`, allows anything.

```bash
MALACHIMQ_ACL_STRICT=true
```

## Grants are allow-only

A grant is a triple: **user**, **operation** (`produce` or `consume`), **resource**.

There are **no deny rules**. Absence is denial. This is a deliberate simplification over systems with both
allow and deny plus precedence ordering, where the practical question "can alice write to orders?" needs
you to evaluate the whole rule set in the right order. Here you either find a matching grant or you do
not.

Two resource shapes, following the Kafka prefixed-ACL model:

| pattern | matches |
|---|---|
| `orders.eu` | that topic exactly |
| `orders.*` | any topic whose name starts with `orders.` |

## Managing grants

Four surfaces, all hitting the same replicated store:

```bash
# mix task, over Erlang distribution
mix malachi.acl grant alice produce 'orders.*'
mix malachi.acl list alice
mix malachi.acl revoke alice produce 'orders.*'

# node script, over the binary protocol
node acl.js grant alice produce 'orders.*'
```

Quote the pattern. An unquoted `orders.*` is glob-expanded by your shell against the working directory,
and you will silently grant something other than what you typed.

There is also an HTTP surface on the dashboard (`/users/:username/acls`, GET/POST/DELETE) and the wire
protocol api_keys for clients that manage their own tenancy.

Grants live in their own `ra` cluster, replicated the same way users and lockouts are, so a grant applies
cluster-wide and survives a node restart.

## Rolling out strict mode without an outage

Strict mode flips a deployment from allow-by-default to deny-by-default. Done carelessly, every client
loses access at once.

1. Leave strict mode **off**. Nothing changes yet.
2. Add the grants every client will need. With strict mode off these are inert, since rule 2 already
   allows the operation.
3. Verify coverage with `mix malachi.acl list <user>` for each user.
4. Turn strict mode **on**. Rule 2 stops applying and the grants you already placed take over.

Doing it in the other order, enabling strict mode first, denies everything until you catch up.

Note that `admin` users are unaffected throughout, which makes admin a useful break-glass path but a poor
choice for routine service accounts: an `admin` service account silently ignores every ACL you write.

## What ACLs cost on the hot path

The grant lookup is a **thunk**, evaluated only when the decision actually reaches rule 3. When `admin` or
a global permission already settles the outcome, no ACL query happens at all.

So with strict mode off the ACL store is not consulted on the produce path for a user holding the global
permission, and enabling ACLs does not slow down a deployment that has not adopted them. In strict mode
every check consults the store, which is a local replicated read rather than a network round trip.

## Next

For running a cluster and growing it, see [Clustering and re-sharding](clustering-and-resharding.md). For
metrics, the dashboard and TLS, see [Operations](operations.md).
