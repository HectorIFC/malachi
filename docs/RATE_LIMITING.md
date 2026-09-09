# Rate Limiting & Connection Controls

Rate limiting and connection control system for Malachi.

## Enforcement status

All four actions are enforced:

| action | keyed by | applied at | default |
|---|---|---|---|
| `:auth` | client IP | the TCP auth handshake | 10 per 60s |
| `:dashboard_auth` | client IP | the dashboard HTTP login/session path | 10 per 60s |
| `:publish` | authenticated username | the `produce` frame | **off** (limit `0`) |
| `:subscribe` | authenticated username | the `subscribe` frame | **off** (limit `0`) |

Read the publish and subscribe rows carefully, because three things about them are deliberate:

**Off by default.** A limit of `0` means no limit, and that is what ships. An operator opts in. Enabling
them by default would have capped every deployment at the old configured value (1000 produce requests a
second) on a broker that measures hundreds of thousands of records a second.

**Per node, not per cluster.** Each node counts its own traffic. A client spread across three nodes can use
up to three times the configured limit cluster-wide. Distributed enforcement is a separate future item
below, and nothing here should be read as a cluster-wide quota.

**Per request, not per record.** A produce of one record and a produce of a thousand each cost one token.
The batch size is already bounded by `MALACHI_MAX_FRAME_SIZE`. A per-record or per-byte quota is a
different control and is listed as future work.

Two things are explicitly **out of scope** for these two limits, so that the one that is implemented has a
single, documented meaning: keying by **IP** (the network-level control is the auth limit plus
`ConnectionLimiter`) and keying by **topic** (listed as future work). `fetch` is not rate limited either:
streaming already has credit-based backpressure, which bounds a consumer far better than a request count
would.

### What a client sees

A rate-limited request is answered with the error reason `rate_limited`. This is deliberately distinct from
`overloaded`, which the group-commit valve sheds when the broker is saturated, because they call for
different client behaviour:

| reason | means | client should |
|---|---|---|
| `rate_limited` | this user is over its configured quota | back off until the window rolls over |
| `overloaded` | the broker is saturated right now | back off briefly and retry |

The response carries the reason only. `retry_after_ms` is computed server-side (it drives the metrics and
logs) but is not on the wire: the error frame's payload is a bare reason string and `Malachi.Wire` freezes
that encoding, so carrying a structured retry-after means a new `api_key`. That is listed as future work.

### What an operator sees

`rate_limit_blocked{action="publish"}` and `{action="subscribe"}` in the Prometheus export, and the top
blocked identifiers under `/rate_limits`. Before this was enforced those counters could only ever read
zero, which was indistinguishable from "nobody hit the limit".

## Features

### Rate Limiting

- **Token Bucket Algorithm**: Efficient, memory-optimized rate limiting
- **Per-Action Limits**: Separate limits per action (`:auth`, `:dashboard_auth`, `:publish`, `:subscribe`)
- **Per-IP or per-user tracking**: the auth limits are keyed by IP, the publish/subscribe quotas by
  authenticated username (see Enforcement status)
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

# Publish rate limits (per authenticated user, per node) - ENFORCED, OFF BY DEFAULT
MALACHI_PUBLISH_RATE_LIMIT=0            # Max produce REQUESTS per window; 0 = no limit (the default)
MALACHI_PUBLISH_RATE_WINDOW_MS=1000     # Window duration (1 second)

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
- `check_limit_in_caller/3` - The same contract on a sharded fixed window, in the calling process (the
  publish/subscribe quotas; see "Two algorithms" below)
- `action_config/1` - The configured limit for `:publish`/`:subscribe`, or `nil` when unlimited
- `reset_bucket/2` - Manual bucket reset
- `get_top_blocked/2` - Dashboard statistics
- `get_stats/0` - System-wide statistics

### Two algorithms, and why

The auth limits are cold (one check per connection or per login) and they are security controls, so they
take the exact path: a token bucket read and written through the limiter GenServer.

The publish quota sits on the hottest path in the system, and it is keyed by user, so every connection
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
and the check scales with cores. The shard caps sum to exactly the configured limit, and each token is
claimed by one atomic operation, so the count stays exact under concurrency (measured: 400 concurrent
callers against a limit of 50 admit exactly 50). What the fixed window gives up is smoothing, not
arithmetic: a client can spend the tail of one window and the head of the next back to back, so a burst of
up to 2x the limit is possible across a window boundary. That is acceptable for a throughput quota and is
why the auth controls keep the token bucket.

End to end, on the 3-node cluster with 48 connections and a batch of 100 (`benchmark/docker-ratelimit.sh`,
best of 2, all cases `errors=0` and `rate_limited=0`):

| case | rec/s | vs off |
|---|---|---|
| limiter off | 304,913 | baseline |
| on, publish limit unconfigured (the shipped default) | 344,613 | +13.0% |
| on, publish limit far above the offered load | 308,788 | +1.3% |

Both are inside the run-to-run noise floor, and the `+13%` on a case that cannot possibly be faster than
the baseline is what makes that floor visible. The reason the check disappears at this scale is worth
being explicit about: a token is spent per produce REQUEST, so a batch of 100 at 300k records a second is
only about 3k checks a second, against the 4.2M/s the check sustains. The headroom, not the reference
load, is what the microbench is for.

Reproduce with `mix run benchmark/rate_limit_bench.exs` (the check in isolation) and
`benchmark/docker-ratelimit.sh` (what it costs as a share of real produce throughput).

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
| `rate_limited` | a `produce` or `subscribe`, per authenticated user | back off until the window rolls over |
| `overloaded` | a `produce`, when the broker is saturated | back off briefly and retry |

A connection cap answers `connection_limit_exceeded` (the per-IP cap) or `global_limit_exceeded` (the total
cap), sent just before the socket is closed.

**Dashboard HTTP.** The dashboard replies with `HTTP/1.1 429 Too Many Requests`, a `Retry-After` header in
seconds, and a JSON body:

```json
{
  "s": "err",
  "reason": "rate_limit_exceeded",
  "retry_after_ms": 58432
}
```

### Flow

1. **Connection** → ConnectionLimiter checks per-IP + global limits
2. **TCP authentication** → RateLimiter checks the `:auth` limit by IP
3. **Dashboard authentication** → RateLimiter checks the `:dashboard_auth` limit by IP before validating the
   login or the session token
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
    "subscribe": {
      "limit": null,
      "window_ms": null
    }
  }
}
```

`top_blocked` always carries all five action keys (`auth`, `publish`, `subscribe`, `channel_publish`,
`channel_subscribe`). Three of them can be populated: `auth`, and `publish` / `subscribe` once an operator
configures those quotas. `channel_publish` and `channel_subscribe` are vestigial and stay empty, since
nothing blocks on them.

The `config` object lists the `auth`, `publish`, and `subscribe` limits, read through the same
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
      "subscribe_blocked": 0,
      "connection_blocks": 45
    }
  }
}
```

`publish_blocked` and `subscribe_blocked` move once an operator configures those quotas and a client
exceeds one. They read `0` while the quotas are unconfigured, which is the default, so on a stock
deployment a zero means "no quota is set" rather than "nobody hit it": `config.publish.limit` in
`/rate_limits` is what tells the two apart.

`rate_limiting.auth_blocked` counts only the TCP `:auth` blocks; dashboard `:dashboard_auth` blocks are
counted separately and exposed under `system.dashboard.auth_blocked`.

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

The transport (`:ssl` or `:gen_tcp`) selects the peername lookup, and both IPv4 and IPv6 addresses are
formatted:

```elixir
defp get_client_ip(socket, transport) do
  case transport do
    :ssl ->
      case :ssl.peername(socket) do
        {:ok, {address, _port}} -> format_ip(address)
        {:error, _} -> "unknown"
      end

    :gen_tcp ->
      case :inet.peername(socket) do
        {:ok, {address, _port}} -> format_ip(address)
        {:error, _} -> "unknown"
      end
  end
end

defp format_ip({a, b, c, d}), do: "#{a}.#{b}.#{c}.#{d}"
defp format_ip({a, b, c, d, e, f, g, h}), do: "#{hex}:#{hex}:..."
```

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
- **Publish**: off. Set `MALACHI_PUBLISH_RATE_LIMIT` to opt in; it counts produce REQUESTS per window per
  authenticated user, per node
- **Subscribe**: off. Set `MALACHI_SUBSCRIBE_RATE_LIMIT` to opt in; same key and scope
- **Connections**: 100 per IP, 10K global (prevents DoS)

The publish and subscribe quotas ship off because the right value depends entirely on the workload: a
quota below what your producers legitimately send turns into `rate_limited` errors on healthy traffic.
Size it from what your clients actually do, with headroom, and watch `rate_limit_blocked` after enabling
it. Remember the count is per node: with N nodes a client can use up to N times the number you set.

### Tuning Guidelines

**High-traffic scenarios**:
```bash
MALACHI_PUBLISH_RATE_LIMIT=10000
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
- `rate_limiting.publish_blocked` / `subscribe_blocked` - A client over its configured quota
- `connection_blocks` - Network issues or DoS attempts
- Top blocked IPs (via `/rate_limits` endpoint)

## Future Enhancements

Potential improvements (not currently implemented):

- [x] Enforce the configured publish/subscribe rate limits (per-user quotas on produce/subscribe)
- [ ] Per-record or per-byte publish quotas (today a produce costs one token whatever its batch size)
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
