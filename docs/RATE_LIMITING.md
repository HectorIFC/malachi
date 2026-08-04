# Rate Limiting & Connection Controls

Rate limiting and connection control system for Malachi.

## Enforcement status

Only **authentication** is rate limited today: by IP on the TCP path (the `:auth` action) and by IP on the
dashboard HTTP login/session path (the `:dashboard_auth` action). The **publish** and **subscribe** limits
described below are configurable and surfaced in `/rate_limits`, but the broker does not currently call the
limiter on the produce/consume paths, so those limits are not applied and their blocked counters stay at
zero. They are kept as the base for a future per-user quota (streaming already has credit-based backpressure,
which is a separate mechanism). Wherever a limit is publish or subscribe, read it as *configured, not
enforced*.

## Features

### Rate Limiting

- **Token Bucket Algorithm**: Efficient, memory-optimized rate limiting
- **Per-Action Limits**: Separate limits per action: the enforced `:auth` and `:dashboard_auth`, and the
  configured-only `:publish` / `:subscribe` (see Enforcement status)
- **IP-based tracking**: The enforced auth limits are keyed by IP; the publish/subscribe configuration is
  keyed by username, for the future per-user quota
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

# Publish rate limits (per username) - CONFIGURED, NOT ENFORCED (see Enforcement status)
MALACHI_PUBLISH_RATE_LIMIT=1000         # Max publishes per window
MALACHI_PUBLISH_RATE_WINDOW_MS=1000     # Window duration (1 second)

# Subscribe rate limits (per username) - CONFIGURED, NOT ENFORCED (see Enforcement status)
MALACHI_SUBSCRIBE_RATE_LIMIT=100        # Max subscribes per window
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
- `check_limit/3` - Validate request against limit
- `reset_bucket/2` - Manual bucket reset
- `get_top_blocked/2` - Dashboard statistics
- `get_stats/0` - System-wide statistics

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
does not receive that value. When rate limited the reason is `rate_limit_exceeded`; when a connection cap
is hit it is `connection_limit_exceeded` (the per-IP cap) or `global_limit_exceeded` (the total cap), sent
just before the socket is closed.

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
4. **Publish/Subscribe** → not rate limited today (the `:publish` / `:subscribe` limits are configured but
   not applied; see Enforcement status)
5. **Metrics** → Blocked counters incremented for the enforced actions
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
      "limit": 100,
      "window_ms": 60000
    }
  }
}
```

`top_blocked` always carries all five action keys (`auth`, `publish`, `subscribe`, `channel_publish`,
`channel_subscribe`), but only `auth` is ever populated: nothing blocks on the other four, so they stay empty
(see Enforcement status). The `config` object lists only the `auth`, `publish`, and `subscribe` limits.

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

`publish_blocked` and `subscribe_blocked` are always `0`: nothing increments them because publish/subscribe
are not rate limited (see Enforcement status). `rate_limiting.auth_blocked` counts only the TCP `:auth`
blocks; dashboard `:dashboard_auth` blocks are counted separately and exposed under
`system.dashboard.auth_blocked`.

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

- **Auth**: 10 attempts per minute per IP (prevents brute force) - enforced (TCP and dashboard)
- **Publish**: 1000 messages per second per user - configured, not enforced
- **Subscribe**: 100 subscriptions per minute per user - configured, not enforced
- **Connections**: 100 per IP, 10K global (prevents DoS)

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
- `connection_blocks` - Network issues or DoS attempts
- Top blocked IPs (via `/rate_limits` endpoint)

## Future Enhancements

Potential improvements (not currently implemented):

- [ ] Enforce the configured publish/subscribe rate limits (per-user quotas on produce/consume)
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
