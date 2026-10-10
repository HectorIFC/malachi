# Rate Limiting & Connection Controls

Rate limiting and connection control system for Malachi.

## Enforcement status

All six actions are enforced:

| action | keyed by | applied at | default |
|---|---|---|---|
| `:auth` | client IP | the TCP auth handshake | 10 per 60s |
| `:dashboard_auth` | client IP | dashboard `POST /login`, and dashboard tokens that do not validate | 10 per 60s |
| `:dashboard_api` | session (a SHA-256 digest of its token) | every authenticated dashboard request, `/stream` once when it opens | 300 per 60s (`0` turns it off) |
| `:publish` | authenticated username | the records of a `produce` frame or a stream `append` | **off** (limit `0`) |
| `:publish_bytes` | authenticated username | the bytes of a `produce` request as received (topic and framing included; the key carries no compression), or the inflated bytes of a stream `append` | **off** (limit `0`) |
| `:subscribe` | authenticated username | the `subscribe` frame, an `open_consume` and a `fetch_range` (one token each) | **off** (limit `0`) |

Read the publish and subscribe rows carefully, because four things about them are deliberate:

**Off by default.** A limit of `0` means no limit, and that is what ships. An operator opts in. Enabling
them by default would have capped every deployment at the old configured value (1000 produce requests a
second) on a broker that measures hundreds of thousands of records a second.

**Per node, not per cluster.** Each node counts its own traffic. A client spread across three nodes can use
up to three times the configured limit cluster-wide. Distributed enforcement is a separate future item
below, and nothing here should be read as a cluster-wide quota.

**Records and bytes, not requests.** A produce is charged one unit of `:publish` per record and one unit
of `:publish_bytes` per byte. A stream `append` is charged after its batch is inflated and decoded, so a
compressed batch costs what it holds, not what it weighs on the wire. Both charges are taken together or
not at all: an append over either quota spends neither. A subscribe costs one token.

**The attempt is charged.** A produce or append is charged before the broker takes it, and a refusal from
the broker (`overloaded`, a sealed segment, a key outside the stream's range) does not give the charge
back. On the paths that can fail after part of a batch was written (several ranges, a replication
timeout) nothing could tell which records landed, so refunding some refusals and not others would let a
user past the limit. With a publish quota set, a client retrying an `overloaded` batch spends the batch
again; back off long enough for the quota's window to have room.

Two things are explicitly **out of scope** for these limits, so that the one that is implemented has a
single, documented meaning: keying by **IP** (the network-level control is the auth limit plus
`ConnectionLimiter`) and keying by **topic** (listed as future work). What a consumer reads is not charged
by records or bytes, and `fetch` is not rate limited: a push stream's credit window bounds a consumer far
better than a request count would, so opening a consume stream or fetching a range spends one subscribe
token and the rest is credit.

### What a client sees

A rate-limited request is answered with the error reason `rate_limited`. This is deliberately distinct from
`overloaded`, which the group-commit valve sheds when the broker is saturated, because they call for
different client behaviour:

| reason | means | client should |
|---|---|---|
| `rate_limited` | this user is over its configured quota | back off until the window rolls over |
| `quota_too_small` | this one produce or append costs more records or bytes than a whole window of the quota | send smaller batches; retrying the same one never succeeds |
| `overloaded` | the broker is saturated right now | back off briefly and retry (with a publish quota set, the retry is charged again) |

A `produce` frame gets these as its error frame. A stream `append` gets them in its `append_ack`, as the
error at its sequence, and the stream goes on.

The response carries the reason only. `retry_after_ms` is computed server-side (it drives the metrics and
logs) but is not on the wire: the error frame's payload is a bare reason string and `Malachi.Wire` freezes
that encoding, so carrying a structured retry-after means a new `api_key`. That is listed as future work.

### What an operator sees

`rate_limit_blocked{action="publish"}`, `{action="publish_bytes"}` and `{action="subscribe"}` in the
Prometheus export (a refusal counts under the quota that refused it), and the top
blocked identifiers under `/rate_limits`. Before this was enforced those counters could only ever read
zero, which was indistinguishable from "nobody hit the limit".

## Features

### Rate Limiting

- **Token Bucket Algorithm**: Efficient, memory-optimized rate limiting
- **Per-Action Limits**: Separate limits per action (`:auth`, `:dashboard_auth`, `:dashboard_api`,
  `:publish`, `:subscribe`)
- **Per-IP, per-session or per-user tracking**: the auth limits are keyed by IP, the dashboard API limit by
  session, the publish/subscribe quotas by authenticated username (see Enforcement status)
- **Automatic Token Refill**: Time-based token replenishment
- **Periodic Cleanup**: Automatic removal of expired buckets every 5 minutes
- **Real-time Metrics**: Track blocked requests per action
- **Dashboard Integration**: `/rate_limits` endpoint with top blocked identifiers

### Connection Limiting

- **Per-IP Limits**: Prevent resource exhaustion from single source
- **Global Limits**: Cap total concurrent connections
- **Automatic Cleanup**: Process monitoring with automatic decrement on death
- **Atomic Operations**: Thread-safe counter management with rollback
- **Zero Memory Leaks**: ETS-based tracking with guaranteed cleanup

## Configuration

All limits are configurable via environment variables:

### Rate Limiting

```bash
# Enable/disable rate limiting (default: true in production, false in test)
MALACHI_RATE_LIMIT_ENABLED=true

# Authentication rate limits, TCP path (per IP) - ENFORCED
MALACHI_AUTH_RATE_LIMIT=10              # Max attempts per window
MALACHI_AUTH_RATE_WINDOW_MS=60000       # Window duration (60 seconds)

# Dashboard authentication rate limits, HTTP path (per IP) - ENFORCED
MALACHI_DASHBOARD_AUTH_RATE_LIMIT=10        # Max attempts per window
MALACHI_DASHBOARD_AUTH_RATE_WINDOW_MS=60000 # Window duration (60 seconds)

# The two auth limits above have no off switch: a limit of 0 (or less) refuses every attempt.

# Dashboard API rate limits, authenticated HTTP requests (per session) - ENFORCED
MALACHI_DASHBOARD_API_RATE_LIMIT=300        # Max requests per window; 0 = no limit
MALACHI_DASHBOARD_API_RATE_WINDOW_MS=60000  # Window duration (60 seconds)

# Publish rate limits (per authenticated user, per node) - ENFORCED, OFF BY DEFAULT
MALACHI_PUBLISH_RATE_LIMIT=0            # Max produced RECORDS per window; 0 = no limit (the default)
MALACHI_PUBLISH_RATE_WINDOW_MS=1000     # Window duration (1 second)
MALACHI_PUBLISH_BYTES_RATE_LIMIT=0      # Max produced bytes (inflated) per window; 0 = no limit (the default)
MALACHI_PUBLISH_BYTES_RATE_WINDOW_MS=1000

# Subscribe rate limits (per authenticated user, per node) - ENFORCED, OFF BY DEFAULT
MALACHI_SUBSCRIBE_RATE_LIMIT=0          # Max subscribe requests per window; 0 = no limit (the default)
MALACHI_SUBSCRIBE_RATE_WINDOW_MS=60000  # Window duration (60 seconds)

# Cleanup interval
MALACHI_RATE_LIMIT_CLEANUP_INTERVAL=300000  # 5 minutes
```

### Connection Limiting

```bash
# Enable/disable connection limiting
MALACHI_CONNECTION_LIMIT_ENABLED=true

# Per-IP connection limit
MALACHI_MAX_CONN_PER_IP=100

# Global connection limit
MALACHI_MAX_TOTAL_CONN=10000
```

## Architecture

### RateLimiter GenServer

**File**: `lib/malachi/rate_limiter.ex`

**ETS Schema**:
- `{{identifier, action}, {count, last_refill_ms, window_start_ms}}` - Token buckets
- `{{:blocked, identifier, action}, count}` - Blocked request counters

**Key Functions**:
- `check_limit/3` - Validate a request against a limit, through the GenServer (exact; the auth paths)
- `check_limit_in_caller/3` - The same contract on a sharded fixed window, in the calling process, one
  token per call (the subscribe quota; see "Two algorithms" below)
- `charge_in_caller/2` - Charges a cost against one or more sharded windows at once, all or nothing (the
  publish quotas: records and bytes)
- `action_config/1` - The configured limit for `:publish`, `:publish_bytes` or `:subscribe`, or `nil` when
  unlimited
- `reset_bucket/2` - Manual bucket reset
- `get_top_blocked/2` - Dashboard statistics
- `get_stats/0` - System-wide statistics

### Two algorithms, and why

The auth limits are cold (one check per connection or per login) and they are security controls, so they
take the exact path: a token bucket read and written through the limiter GenServer.

The publish quotas sit on the hottest path in the system, and they are keyed by user, so every connection
belonging to one client contends for one quota. The obvious approach, running the same bucket body in the
caller instead of the GenServer, is the wrong answer: ETS `write_concurrency` buys nothing when every
caller writes the *same* key. Measured on an 8-core machine against one hot key, at 64 concurrent
processes:

| implementation | checks/s |
|---|---|
| bucket lookup + insert, in the caller | 120k (seven times *worse* than the GenServer) |
| bucket through the GenServer | 868k |
| one atomic `update_counter`, one key | 229k |
| `update_counter` sharded per scheduler | 23.8M |

So the hot path counts a **fixed window sharded per scheduler**: racing callers land on different ETS keys
and the check scales with cores. The shard caps sum to exactly the configured limit. A one-token check
claims each token by one atomic operation, so its count is exact under concurrency (measured: 400
concurrent callers against a limit of 50 admit exactly 50). A charge of several units (the publish quotas)
adds its whole cost to a shard and gives back what went past the cap, so it never admits more than the
limit in a window, but a charge racing another one's give-back can see the shard fuller than it is and be
refused while a few units are still free: under contention near the limit it errs toward admitting less.
What the fixed window gives up is smoothing, not
arithmetic: a client can spend the tail of one window and the head of the next back to back, so a burst of
up to 2x the limit is possible across a window boundary. That is acceptable for a throughput quota and is
why the auth controls keep the token bucket.

End to end, on the 3-node cluster with 48 connections, a batch of 100 and 256-byte records
(`benchmark/docker-ratelimit.sh`, Linux under Colima with 4 CPUs, servers on cpuset `1,2,3` and the client
on `0` via `SRV_CPUSET` and `LT_CPUSET`, 5 interleaved rounds, a fresh cluster for every round, every case
`errors=0` and `rate_limited=0`). The rate is each case's best round; the spread is its own five rounds,
worst to best, as a share of its best:

| case | rec/s (best of 5) | vs off | spread |
|---|---|---|---|
| limiter off | 504,825 | baseline | 6.8% |
| on, publish quotas unconfigured (the shipped default) | 505,250 | +0.1% | 5.0% |
| on, records quota far above the offered load | 502,563 | -0.4% | 6.0% |
| on, records and bytes quotas far above the offered load | 503,563 | -0.2% | 3.9% |

Every delta is well inside the noise floor of the same run, 3.9% to 6.8%. The floor is not stable from run
to run: an earlier five-round sweep on the same machine had spreads of 9.0% to 18.4%, and its
records-and-bytes case reported 12,591 errors across its five rounds, which the harness did not break down
by round or by reason at the time (it prints both now). Five more rounds of that case on fresh clusters had no errors,
and neither did the sweep above. The charge disappears at this scale because it runs once per produce
request: a batch of 100 at 500k records a second is about 5k charges a second.

The charge in isolation (`benchmark/rate_limit_bench.exs`, same Linux machine, range over 3 runs, ns per
call on one hot user):

| call | 1 process | 64 processes |
|---|---|---|
| one-token check (`check_limit_in_caller/3`, the door the subscribe quota uses) | 436 to 479 | 118 to 121 |
| produce charge, records quota set | 1,215 to 1,298 | 317 to 375 |
| produce charge, records and bytes quotas set | 1,813 to 1,917 | 447 to 457 |
| produce charge, both quotas unconfigured | 671 to 812 | 162 to 215 |

A produce charge costs about three times the one-token check (four with both quotas), and still sustains
over 2 million charges a second at 64 concurrent processes. The rows do not do the same work: the one-token
check is timed with its limit already in hand, while every produce charge reads both quotas' limits and
windows and builds its list of charges first. The unconfigured row is the config reads with no quota to
charge, and taking it away leaves the per-quota work, mostly the claim (the window and shard bookkeeping the
one-token check also does, and the ETS update), at about one one-token check per configured quota. On the
mean ns of each row over the 3 runs, at the 1 and 64 process ends: about 1.1x (1 process) to 1.4x (64
processes) with the records quota, and about 2.5x (1 process) to 2.3x (64 processes) with both; at 4
processes the both-quotas figure reaches 2.9x to 3.0x, depending on whether the runs are averaged before
or after dividing. Run by run the subtraction varies more (0.8x to 1.8x with the records quota at 64 processes),
because both rows it combines move from run to run: at 64 processes the records row by 58 ns and the
unconfigured row by 53. How the work splits inside the unconfigured row, or inside a claim, has not been
measured. Every charge timed there fits in the caller's own shard; a cost bigger than one
shard's slice sweeps the other shards, an update or two per shard it takes from, and is not timed.

Reproduce on Linux with `MIX_ENV=test mix run benchmark/rate_limit_bench.exs` (the check in isolation) and
`REPEATS=5 SRV_CPUSET=1,2,3 LT_CPUSET=0 benchmark/docker-ratelimit.sh` (what it costs as a share of real
produce throughput, with the settings the table above used on a 4-CPU machine; the script's own defaults
put the servers on cpus 4 to 7 and need 8).

**Token Bucket Algorithm**:
```elixir
tokens_to_add = elapsed_ms * (limit / window_ms)
new_count = min(limit, count + tokens_to_add)

if new_count > 0 do
  allow_and_consume_token()
else
  block_with_retry_after(window_start + window_ms - now)
end
```

### ConnectionLimiter GenServer

**File**: `lib/malachi/connection_limiter.ex`

**ETS Schema**:
- `:malachi_conn_limits_ip` - `{ip, count}`
- `:malachi_conn_limits_global` - `{:total, count}`
- `:malachi_conn_pids` - `{pid, ip, monitor_ref}`

**Key Features**:
- Atomic counter increment with rollback on limit exceeded
- Process.monitor for automatic cleanup
- Separate per-IP and global limit enforcement
- Lock-free using ETS atomic operations

## TCP Protocol Integration

### Error Responses

The two client surfaces report a rate limit differently.

**TCP wire protocol.** The broker answers with a binary error frame built by
`Wire.encode_error(correlation_id, reason)`, where `reason` is an atom serialized as a string. The rate
limiter computes a `retry_after_ms` internally, but the wire error carries only the reason, so a TCP client
does not receive that value (carrying it would need a new `api_key`; see "What a client sees").

The two rate-limit reasons are **not** the same word, because they happen at different points and mean
different things to a client:

| reason | when | what the client should do |
|---|---|---|
| `rate_limit_exceeded` | the auth handshake, per IP | stop reconnecting; the connection was never established |
| `rate_limited` | a `produce`, stream `append`, `subscribe`, `open_consume` or `fetch_range`, per authenticated user | back off until the window rolls over |
| `quota_too_small` | a `produce` or stream `append` bigger than a whole window of a publish quota | send smaller batches |
| `overloaded` | a `produce`, when the broker is saturated | back off briefly and retry |

A connection cap answers `connection_limit_exceeded` (the per-IP cap) or `global_limit_exceeded` (the total
cap), sent just before the socket is closed.

**Dashboard HTTP.** The dashboard and the console reply with `HTTP/1.1 429 Too Many Requests`, a
`Retry-After` header in whole seconds, rounded up, and an `application/problem+json` body:

```json
{
  "type": "errors.http.rate_limited",
  "status": 429,
  "retry_after_ms": 200
}
```

The answer is the same whichever bucket ran out: in both cases the right move is to wait. `retry_after_ms`
is the time until the bucket's next token, not the time left in its window. The buckets refill
continuously, so at 300 a minute a limited console waits 200 ms, and a limited login waits 6 seconds.
Time short of a whole token carries over to the next request, so a client that never gets ahead of the
rate is never refused, however unevenly it spaces its requests.

The page served at `/` treats any failure of its `/stream` connection as a lost session and returns to the
login form, so a session that has spent its API budget and then reopens the stream is sent to log in
again.

### Flow

1. **Connection** → ConnectionLimiter checks per-IP + global limits
2. **TCP authentication** → RateLimiter checks the `:auth` limit by IP
3. **Dashboard** → a `POST /login` spends the `:dashboard_auth` limit by IP before the password is checked.
   A session token is first checked without side effects: one that validates spends its session's
   `:dashboard_api` budget and never the address's; one that does not validate spends the address's
   `:dashboard_auth` budget **before** it is validated, so the expiry and hijack audit events that
   validation writes are capped by the login budget
4. **Publish/Subscribe** → RateLimiter checks the `:publish` / `:subscribe` limit by authenticated
   username, after the permission check, on the `produce` and `subscribe` frames. Skipped entirely when
   the action is unconfigured, which is the default
5. **Metrics** → Blocked counters incremented for every action that blocked
6. **Cleanup** → Process death triggers automatic connection decrement

## Dashboard

### GET /rate_limits

Returns JSON with rate limiting statistics:

```json
{
  "enabled": true,
  "top_blocked": {
    "auth": [
      ["192.168.1.100", 523],
      ["10.0.0.50", 312]
    ],
    "publish": [],
    "publish_bytes": [],
    "subscribe": [],
    "channel_publish": [],
    "channel_subscribe": []
  },
  "config": {
    "auth": {
      "limit": 10,
      "window_ms": 60000
    },
    "publish": {
      "limit": 1000,
      "window_ms": 1000
    },
    "publish_bytes": {
      "limit": null,
      "window_ms": null
    },
    "subscribe": {
      "limit": null,
      "window_ms": null
    }
  }
}
```

`top_blocked` always carries the keys `auth`, `dashboard_auth`, `dashboard_api`, `publish`,
`publish_bytes`, `subscribe`, `channel_publish` and `channel_subscribe`. `publish`, `publish_bytes` and
`subscribe` fill once an operator configures those quotas, each with the users that quota refused. `channel_publish` and `channel_subscribe` are vestigial and stay empty, since
nothing blocks on them.

The `config` object lists the `auth`, `dashboard_auth`, `publish`, `publish_bytes`, `subscribe` and
`dashboard_api` limits, read through the same
`RateLimiter.action_config/1` the enforcement path uses so the two can never disagree. An **unconfigured**
action reports `null` for both fields, as `subscribe` does above, rather than a default nobody applies.

### GET /metrics

System metrics include rate limiting section:

```json
{
  "system": {
    "rate_limiting": {
      "auth_blocked": 1523,
      "publish_blocked": 0,
      "publish_bytes_blocked": 0,
      "subscribe_blocked": 0,
      "dashboard_api_blocked": 0,
      "connection_blocks": 45
    }
  }
}
```

`publish_blocked`, `publish_bytes_blocked` and `subscribe_blocked` move once an operator configures those
quotas and a client exceeds one (a `quota_too_small` refusal counts too, under the quota that refused it).
They read `0` while the quotas are unconfigured, which is the default, so on a stock deployment a zero
means "no quota is set" rather than "nobody hit it": `config.publish.limit` (or `config.publish_bytes.limit`)
in `/rate_limits` is what tells the two apart.

`rate_limiting.auth_blocked` counts only the TCP `:auth` blocks; dashboard `:dashboard_auth` blocks are
counted separately and exposed under `system.dashboard.auth_blocked`, and in Prometheus as
`malachi_dashboard_auth_total{outcome="blocked"}`. That is why `malachi_rate_limit_blocked_total` has an
`action="dashboard_api"` series and no `action="dashboard_auth"` one: a second series for the same event
would count it twice in a `sum by (action)`.

`rate_limiting.dashboard_api_blocked` counts authenticated dashboard requests refused because their
session spent its budget. Each one is also audited as `dashboard_api_rate_limited`, with the user and the
session digest that `/rate_limits` lists under `top_blocked.dashboard_api`; the token itself is never
stored. A session's blocked counter is reaped with its bucket, unlike the per-IP ones, which are kept as
history.

## Testing

### Unit Tests

```bash
# RateLimiter tests
mix test test/rate_limiter_test.exs

# ConnectionLimiter tests
mix test test/connection_limiter_test.exs
```

**Coverage**:
- Token bucket refill logic
- Concurrent access patterns (the heavy ones are tagged `@tag :concurrent`)
- Different identifiers/actions independence
- Cleanup and expiration
- Statistics and top blocked queries
- Process monitoring and cleanup
- Excessive auth attempts, publish bursts, and subscribe spam are blocked past the limit
- Connection floods are rejected at accept, and metrics track every block

### Running All Tests

```bash
mix test
```

## Implementation Details

### State Map Pattern

The TCP acceptor threads a state map so the client IP propagates cleanly:

```elixir
%{
  socket: socket,
  transport: transport,
  client_ip: client_ip,    # Extracted on connection
  session: nil,            # Filled in once the client authenticates
  buffer: ""
}
```

### IP Extraction

`Malachi.IPAddress.from_socket/2` reads the peer for either transport and formats it once, at the
edge. Every limiter key, lockout key and audit-log address in the system comes from that one
function:

```elixir
client_ip = Malachi.IPAddress.from_socket(socket, transport)
```

The canonical form is `:inet.ntoa/1`, which is RFC 5952: lowercase hex with zero runs compressed,
and an IPv4-mapped address written as `::ffff:127.0.0.1`. It is the form that reads back through
`:inet.parse_address/1`, which is what a CIDR allowlist and an operator grepping the audit log both
expect.

| Input | Key |
| --- | --- |
| `{192, 168, 1, 1}` | `192.168.1.1` |
| `{0, 0, 0, 0, 0, 0, 0, 1}` | `::1` |
| `{0, 0, 0, 0, 0, 0xFFFF, 0x7F00, 1}` | `::ffff:127.0.0.1` |
| a tuple that is not an address | `invalid` |
| no address at all, peer unreadable | `unknown` |

### Metrics Integration

```elixir
# Increment blocked counter
Malachi.Metrics.increment_rate_limit_blocked(:auth)
Malachi.Metrics.increment_connection_limit_blocked()

# Query in dashboard
system_metrics = Malachi.Metrics.get_system_metrics()
system_metrics.rate_limiting.auth_blocked  #=> 1523
```

## Debugging

### Check Current Limits

```elixir
# In IEx
iex> Application.get_env(:malachi, :auth_rate_limit)
10

iex> Application.get_env(:malachi, :rate_limit_enabled)
true
```

### Inspect Buckets

```elixir
iex> Malachi.RateLimiter.get_stats()
%{total_buckets: 1523, total_blocked_entries: 234}

iex> Malachi.RateLimiter.get_top_blocked(:auth, 5)
[{"192.168.1.100", 523}, {"10.0.0.50", 312}, ...]
```

### Check Connections

```elixir
iex> Malachi.ConnectionLimiter.get_stats()
%{
  total_connections: 347,
  unique_ips: 52,
  max_per_ip: 100,
  max_total: 10_000
}

iex> Malachi.ConnectionLimiter.list_connections()
%{"192.168.1.10" => 15, "10.0.0.5" => 23, ...}
```

### Manual Reset

```elixir
# Reset rate limit for specific identifier
iex> Malachi.RateLimiter.reset_bucket("192.168.1.100", :auth)
:ok

# Unregister connection
iex> Malachi.ConnectionLimiter.unregister_connection(pid)
:ok
```

## Production Recommendations

### Default Limits

The default limits are conservative and suitable for most deployments:

- **Auth**: 10 attempts per minute per IP (prevents brute force), TCP and dashboard
- **Publish**: off. Set `MALACHI_PUBLISH_RATE_LIMIT` to opt in; it counts produced RECORDS per window per
  authenticated user, per node. `MALACHI_PUBLISH_BYTES_RATE_LIMIT` (with
  `MALACHI_PUBLISH_BYTES_RATE_WINDOW_MS`) does the same for inflated bytes
- **Subscribe**: off. Set `MALACHI_SUBSCRIBE_RATE_LIMIT` to opt in; same key and scope
- **Connections**: 100 per IP, 10K global (prevents DoS)

The publish and subscribe quotas ship off because the right value depends entirely on the workload: a
quota below what your producers legitimately send turns into `rate_limited` errors on healthy traffic.
Size it from what your clients actually do, with headroom, and watch `rate_limit_blocked` after enabling
it. Remember the count is per node: with N nodes a client can use up to N times the number you set.

### Tuning Guidelines

**High-traffic scenarios**:
```bash
MALACHI_PUBLISH_RATE_LIMIT=1000000
MALACHI_PUBLISH_BYTES_RATE_LIMIT=1073741824
MALACHI_MAX_TOTAL_CONN=50000
```

**Security-focused**:
```bash
MALACHI_AUTH_RATE_LIMIT=5
MALACHI_AUTH_RATE_WINDOW_MS=120000  # 2 minutes
MALACHI_MAX_CONN_PER_IP=50
```

**Development/Testing**:
```bash
MALACHI_RATE_LIMIT_ENABLED=false
MALACHI_CONNECTION_LIMIT_ENABLED=false
```

### Monitoring

Key metrics to monitor:
- `rate_limiting.auth_blocked` - Potential brute force against the TCP auth
- `dashboard.auth_blocked` - Potential brute force against the dashboard login
- `rate_limiting.publish_blocked` / `publish_bytes_blocked` / `subscribe_blocked` - A client over its
  configured quota
- `connection_blocks` - Network issues or DoS attempts
- Top blocked IPs (via `/rate_limits` endpoint)

## Future Enhancements

Shipped:

- [x] Enforce the configured publish/subscribe rate limits (per-user quotas on produce/subscribe)
- [x] Publish quotas counted in records and inflated bytes

Potential improvements (not currently implemented):

- [ ] Carry `retry_after_ms` on the wire (needs a new `api_key`; see "What a client sees")
- [ ] Rate limit `fetch` (today only streaming credit bounds a consumer)
- [ ] Persistent ban list (Redis/ETS backed)
- [ ] Adaptive limits based on system load
- [ ] Whitelist/blacklist IP ranges
- [ ] Per-queue publish rate limits
- [ ] Circuit breaker integration
- [ ] Distributed rate limiting (multi-node)
- [ ] Custom rate limit per user/tenant

## Contributing

When adding new rate-limited operations:

1. Add action to `RateLimiter` @moduledoc
2. Configure limit via environment variable
3. Add check in protocol handler with client_ip
4. Update metrics to track new action
5. Add to dashboard `/rate_limits` response
6. Write unit + integration tests
7. Update this README

## License

Part of Malachi - see main project LICENSE.
