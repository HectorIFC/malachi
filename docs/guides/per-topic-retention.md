# Per-topic retention

[Operations](operations.md) sets one retention for the whole cluster, from the environment. A **storage
policy** overrides it for the topics you choose, at runtime, without a restart.

## Policies and bindings

A policy is a named, cluster-wide definition. It can set:

| field | type | meaning |
|---|---|---|
| `retention.max_age_ms` | non-negative integer | a sealed segment older than this expires |
| `retention.max_bytes` | non-negative integer | per range, the oldest sealed segments expire until the range fits |
| `retention.segment_max_age_ms` | integer, at least 60000 | an active segment older than this is sealed, so age retention can see it |
| `spread_by` | broker attribute key | the failure domain new segments of the topic are spread across |

A topic is **bound** to a policy by name. One policy can serve any number of topics, and changing it
changes all of them at once. Definitions live in their own `ra` cluster; which policy a topic uses is the
topic's own state, and travels with it when a vnode split moves it.

## Inherit, off, and zero are three different things

Each field of a policy is in one of three states, and every surface keeps them apart:

- **Left out**: the topic inherits the cluster's global value (`MALACHI_RETENTION_MAX_AGE_MS`,
  `MALACHI_RETENTION_MAX_BYTES`, `MALACHI_SEGMENT_MAX_AGE_MS`, `MALACHI_LOG_SPREAD_BY`).
- **Off** (`null`, `--off`): that rule does not apply to the policy's topics, whatever the global says.
- **Zero** is a real budget. `retention.max_bytes=0` expires every sealed segment the rule can see.

## Managing policies

Four surfaces, all going through the same checks and the same audit trail:

```bash
# mix task, over Erlang distribution
mix malachi.policy define short --set retention.max_age_ms=3600000 --off retention.max_bytes
mix malachi.policy bind clicks short
mix malachi.policy get clicks
mix malachi.policy list
mix malachi.policy unbind clicks
mix malachi.policy delete short

# node script, over the binary protocol
node policy.js define short --set retention.max_age_ms=3600000
node policy.js bind clicks short
```

On the dashboard: `GET /policies`, `PUT /policies/:name` with `{"fields": {"retention.max_age_ms": 3600000}}`,
`DELETE /policies/:name` (add `?force=true`, see below), and `GET`, `PUT` (`{"policy": "short"}`) and
`DELETE` on `/topics/:name/policy`. Every route needs the `admin` permission and spends the session's own
request budget. On the wire, api keys 17 to 21 (`define_policy`, `delete_policy`, `list_policies`,
`bind_topic_policy`, `get_topic_policy`), also `admin` only.

Each change is written to the audit log (`policy_defined`, `policy_deleted`, `topic_policy_bound`) with
the user who made it, or `cli@<node>` for the mix task.

## Reading back what a topic actually keeps

`get` answers the question an operator actually has: what applies to this topic, and why.

```
topic	clicks
policy	short
retention.max_age_ms	3600000	(policy)
retention.max_bytes	off	(policy)
spread_by	rack	(global)
```

Each value comes from the same function the retention sweep applies, so what `get` prints is what the
data sees. The origin is `policy`, `global`, or `unresolved_backstop` (below).

## A binding to a name nothing defines

A topic bound to a name the store does not hold **expires nothing**: the name exists because someone
wanted something other than the default, and the usual something is to keep data longer. So:

- `bind` refuses a name the store does not define (`no_such_policy`).
- `delete` refuses a policy a topic is still bound to (`policy_in_use: <topics>`), and `--force` (or
  `?force=true`, or the wire's force byte) deletes it anyway.

The delete asks every vnode which topics are bound to the policy, and when one of them does not answer, or
a vnode split is moving topics between them, it is refused (`bindings_unavailable`) rather than
allowed: retry it. Both checks are still made before the change is submitted, so a bind racing a delete
can leave a topic bound to a name that is gone. `get` then shows `(undefined: this topic holds its data)`, the topic is
counted in `malachi_retention_unresolved_policy_sweeps_total{topic}`, and the only bound that applies to
it is `MALACHI_RETENTION_UNRESOLVED_POLICY_MAX_AGE_MS` (see [Operations](operations.md)).

## During a rolling upgrade

Binding a topic needs control-plane machine version 4; defining and deleting policies needs 3. Until
every node runs a release that implements version 4 and the version pin is lifted, a bind is refused on
every node alike, and each surface says to finish the rolling upgrade. Policies can be defined ahead of
time and bound once the upgrade is finalized.

A field added to policies by a later release is refused the same way (`unsupported_policy_field`) until
the cluster reaches the version that introduced it. `retention.segment_max_age_ms` is the first: it needs
version 6.

## Rolling quiet topics by age

Age retention only ever expires **sealed** segments, and a segment seals by size on its own, at
`MALACHI_SEGMENT_MAX_BYTES` (64 MiB). A topic that writes 1 MB a day would keep its first record for
about two months before its segment filled, and a topic that never reaches 64 MiB would never expire
anything at all. `retention.segment_max_age_ms` closes that: the retention sweep asks for an active
segment older than this to be sealed, through the same fence a size roll uses, whether or not anything
is producing to it. A record then lives at most `segment_max_age_ms` plus `max_age_ms`, plus up to two
retention sweep intervals (`MALACHI_RETENTION_INTERVAL_MS`: the sweep that asks for the roll and the one
that expires the segment) and the time the fence takes to answer.

The cluster's value is `MALACHI_SEGMENT_MAX_AGE_MS`, 7 days unless set. Lower it for a topic whose
`max_age_ms` is short, so the bound is close to what the policy says; `--off` turns the roll off for a
topic, which then seals by size alone. A segment that is never written to after the roll is not
replaced: the next produce opens the successor, so an idle topic does not grow one empty segment per
interval.

The floor is 60000 (one minute), the retention sweep's default cadence, and it is refused below that.
Every roll is one more segment: in the metadata until retention removes it, and one preallocated file
(`MALACHI_SEGMENT_PREALLOC_BYTES`, 64 MiB by default) written when the produce after it opens the
successor. A topic produced to once a minute with a one-minute roll makes 1440 segments a day per
range, so keep the interval well above the topic's write rhythm unless its retention is short too.

This is not `MALACHI_LOG_ROLL_MAX_AGE_MS`, which rolls files inside one segment's storage and never
seals the segment.
