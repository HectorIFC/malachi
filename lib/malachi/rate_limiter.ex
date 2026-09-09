defmodule Malachi.RateLimiter do
  @moduledoc """
  Token bucket rate limiter using ETS for high-performance limits.
  Tracks requests per IP/user across different actions.

  ## Schema

  The ETS table stores three types of entries:

  - `{{identifier, action}, {count, last_refill_ms, window_start_ms, window_ms}}` - Token buckets
    (`check_limit/3`); as with the sharded counters, the entry carries the window it was written under so
    that ageing it never depends on configuration read somewhere else
  - `{{identifier, action, window_start_ms, shard}, used, window_ms}` - Sharded window counters
    (`check_limit_in_caller/3`); the entry carries the window it was counted under so the cleanup can
    age it without having to re-read config that may since have changed
  - `{{:blocked, identifier, action}, count}` - Blocked request counters

  ## Actions

  - `:auth` - TCP authentication attempts (tracked by IP)
  - `:dashboard_auth` - Dashboard login attempts (tracked by IP)
  - `:publish` - Produce requests (tracked by authenticated username)
  - `:subscribe` - Subscribe requests (tracked by authenticated username)

  All four are **enforced**, per node. `:publish` and `:subscribe` are off by default (limit `0`); an
  operator opts in by configuring a limit. Enforcement is per node, not a cluster-wide quota, and a
  produce costs one token per request, not per record.

  ## Two doors, on purpose

  `check_limit/3` is the token bucket above, read and written **through the GenServer**, so the
  read-modify-write is serialized and the limit is exact. The auth paths use it: they are cold (one check
  per connection or per login) and they are security controls, so exactness is worth a round-trip.

  `check_limit_in_caller/3` is a different algorithm for a different problem. Produce is the hottest path
  in the system, and the publish quota is keyed by **user**, so every connection belonging to one client
  contends for one quota. The obvious implementation (this module's own bucket body, just run in the caller
  instead of the GenServer) is the wrong answer, because `write_concurrency` buys nothing when every caller
  writes the SAME key. Measured on an 8-core machine against one hot key, at 64 concurrent processes:

      bucket lookup + insert, in the caller     120k checks/s   <- seven times WORSE than the GenServer
      bucket through the GenServer              868k checks/s
      one atomic update_counter, one key        229k checks/s
      update_counter, sharded per scheduler    23.8M checks/s

  So this door counts a **fixed window sharded per scheduler**: the key carries
  `:erlang.system_info(:scheduler_id)`, so racing callers land on different ETS keys and the check scales
  with cores instead of against them. Each shard holds its slice of the quota; a caller whose own shard is
  exhausted sweeps the others before rejecting, so an unevenly spread load does not reject early.

  End to end (`benchmark/rate_limit_bench.exs`, the whole public function rather than the bare ETS op) this
  door measures 4.2M checks/s at 64 concurrent processes against the serialized door's 868k, and an
  unconfigured action, the shipped default, costs 51ns because it never reaches the table at all.

  Every token is still claimed by one atomic `update_counter`, and the shard caps sum to exactly `limit`
  (the remainder is spread across the low shards, not dropped), so the count itself is exact: measured at
  200 concurrent callers, a limit of `n` admits exactly `n`.

  What this door gives up is the *shape* of the limit, not its arithmetic. A fixed window does not refill
  gradually, so a client can spend the tail of one window and the head of the next back to back and burst
  to 2x the limit across a boundary; the token bucket smooths that. Bursting is acceptable for a
  throughput quota and not for an auth control, which is why the two doors exist rather than one.

  ## Configuration

  Set via environment variables or runtime config:

  - `rate_limit_enabled` - Enable/disable rate limiting (default: true)
  - `auth_rate_limit` - Max auth attempts per window (default: 10)
  - `auth_rate_window_ms` - Auth window duration (default: 60000)
  - `publish_rate_limit` - Max produce requests per window (default: 0, meaning no limit)
  - `publish_rate_window_ms` - Publish window duration (default: 1000)
  - `subscribe_rate_limit` - Max subscribe requests per window (default: 0, meaning no limit)
  - `subscribe_rate_window_ms` - Subscribe window duration (default: 60000)
  - `rate_limit_cleanup_interval_ms` - Cleanup interval (default: 300000)
  """

  use GenServer
  require Logger
  alias Malachi.I18n

  @table :malachi_rate_limits

  # How long an entry may sit untouched before the periodic cleanup reaps it. This is the right rule for a
  # token bucket, which has fully refilled after an hour of idleness under any window shorter than that.
  # A sharded window counter is measured against its own action's window instead (see `sharded_ttl/1`).
  @bucket_ttl 3_600_000

  # ============================================================
  # PUBLIC API
  # ============================================================

  @doc """
  Starts the rate limiter GenServer.
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Check if request is within rate limits.

  Returns `:ok` if allowed, or `{:error, :rate_limit_exceeded, retry_after_ms}` if blocked.

  ## Parameters

  - `identifier` - IP address (string) for auth, username (atom/string) for publish/subscribe
  - `action` - Action atom (`:auth`, `:publish`, `:subscribe`)
  - `config` - Map with `:limit` (max requests) and `:window_ms` (time window)

  ## Examples

      check_limit("192.168.1.1", :auth, %{limit: 10, window_ms: 60_000})
      #=> :ok

      check_limit("user1", :publish, %{limit: 1000, window_ms: 1000})
      #=> {:error, :rate_limit_exceeded, 850}
  """
  def check_limit(identifier, action, config) do
    if enabled?() do
      GenServer.call(__MODULE__, {:check_limit, identifier, action, config})
    else
      :ok
    end
  end

  @doc """
  Like `check_limit/3` in contract, but built for a hot path: a fixed window counted in per-scheduler
  shards, read and written in the **calling process** so concurrent callers do not serialize on one ETS
  key or on the limiter process.

  Use this for the publish/subscribe quotas and `check_limit/3` everywhere else. It admits slightly over
  the limit under concurrency and across a window boundary, and never under it. See "Two doors, on
  purpose" above.
  """
  @spec check_limit_in_caller(term(), atom(), %{limit: pos_integer(), window_ms: pos_integer()}) ::
          :ok | {:error, :rate_limit_exceeded, non_neg_integer()}
  def check_limit_in_caller(identifier, action, %{limit: limit, window_ms: window_ms}) do
    if enabled?() do
      do_check_sharded(identifier, action, limit, window_ms)
    else
      :ok
    end
  end

  @doc """
  The configured limit for an opt-in action, or `nil` when that action is not limited.

  `:publish` and `:subscribe` are off unless an operator configures a positive limit and window, so a
  limit of `0` (the default) means "no limit" and reads back as `nil`. This is the single reader of
  those config keys: the enforcement path and the dashboard both go through it so they cannot diverge.

  ## Examples

      action_config(:publish)
      #=> nil                              # unconfigured, the default
      #=> %{limit: 1000, window_ms: 1000}  # MALACHI_PUBLISH_RATE_LIMIT=1000
  """
  @spec action_config(:publish | :subscribe) :: %{limit: pos_integer(), window_ms: pos_integer()} | nil
  def action_config(:publish), do: build_action_config(:publish_rate_limit, :publish_rate_window_ms)
  def action_config(:subscribe), do: build_action_config(:subscribe_rate_limit, :subscribe_rate_window_ms)

  @doc """
  Reset bucket for specific identifier and action.

  Used for testing or manual intervention.
  """
  def reset_bucket(identifier, action) do
    GenServer.call(__MODULE__, {:reset_bucket, identifier, action})
  end

  @doc """
  Get statistics about rate limiting.

  Returns map with total buckets and blocked request counts.
  """
  def get_stats do
    GenServer.call(__MODULE__, :get_stats)
  end

  @doc """
  Get top N blocked identifiers for an action.

  Returns list of `{identifier, blocked_count}` tuples sorted by count descending.

  ## Examples

      get_top_blocked(:auth, 10)
      #=> [{"192.168.1.100", 523}, {"10.0.0.50", 312}, ...]
  """
  def get_top_blocked(action, limit \\ 20) do
    GenServer.call(__MODULE__, {:get_top_blocked, action, limit})
  end

  # ============================================================
  # GENSERVER CALLBACKS
  # ============================================================

  @impl true
  def init(_opts) do
    :ets.new(@table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    schedule_cleanup()
    Logger.info(I18n.t(:rate_limiter_started))
    {:ok, %{}}
  end

  @impl true
  def handle_call({:check_limit, identifier, action, config}, _from, state) do
    {:reply, do_check_limit(identifier, action, config), state}
  end

  @impl true
  def handle_call({:reset_bucket, identifier, action}, _from, state) do
    key = {identifier, action}
    :ets.delete(@table, key)
    :ets.delete(@table, {:blocked, identifier, action})
    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:get_stats, _from, state) do
    # Each head is `{key_pattern, value_pattern...}`, matching the whole ETS object. Getting that shape
    # wrong does not raise, it just never matches, so a broken spec reports a confident zero: exactly what
    # `total_blocked_entries` did before, wrapping its object pattern in one tuple too many.
    total_buckets =
      :ets.select_count(@table, [
        {{{:_, :_}, {:_, :_, :_, :_}}, [], [true]}
      ])

    total_blocked =
      :ets.select_count(@table, [
        {{{:blocked, :_, :_}, :_}, [], [true]}
      ])

    total_window_counters =
      :ets.select_count(@table, [
        {{{:_, :_, :_, :_}, :_, :_}, [], [true]}
      ])

    stats = %{
      total_buckets: total_buckets,
      total_window_counters: total_window_counters,
      total_blocked_entries: total_blocked
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_call({:get_top_blocked, action, limit}, _from, state) do
    # Match all blocked entries for this action
    results =
      :ets.match(@table, {{:blocked, :"$1", action}, :"$2"})
      |> Enum.map(fn [identifier, count] -> {identifier, count} end)
      |> Enum.sort_by(fn {_id, count} -> count end, :desc)
      |> Enum.take(limit)

    {:reply, results, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    cleanup_expired_buckets()
    schedule_cleanup()
    {:noreply, state}
  end

  # ============================================================
  # PRIVATE FUNCTIONS
  # ============================================================

  # A limit is in force only when both the limit and its window are positive integers; anything else
  # (0, a negative, a non-integer from a malformed env var) reads as "not limited".
  defp build_action_config(limit_key, window_key) do
    limit = cfg(limit_key, 0)
    window_ms = cfg(window_key, 0)

    if positive_integer?(limit) and positive_integer?(window_ms) do
      %{limit: limit, window_ms: window_ms}
    end
  end

  defp positive_integer?(value), do: is_integer(value) and value > 0

  # A fixed window sharded per scheduler. The window is identified by its START in wall-clock ms, derived
  # from the clock rather than from stored state, so a new window needs no reset: its counters simply live
  # under a new key, and the periodic cleanup reaps the old ones by age.
  #
  # The fast path is a single atomic `update_counter` on this scheduler's own shard, which is why this
  # scales with cores.
  defp do_check_sharded(identifier, action, limit, window_ms) do
    shards = shard_count()
    now = System.system_time(:millisecond)
    elapsed_in_window = rem(now, window_ms)
    window_start = now - elapsed_in_window
    shard = rem(:erlang.system_info(:scheduler_id), shards)

    if take_token(identifier, action, window_start, shard, shard_cap(limit, shards, shard), window_ms) do
      :ok
    else
      steal_token_or_block(identifier, action, window_start, shard, shards, limit, window_ms, elapsed_in_window)
    end
  end

  # How much of the quota one shard holds. The remainder is spread over the low shards rather than
  # dropped, so the caps sum to EXACTLY `limit`: rounding down would make the sharded window reject
  # before the configured limit was reached, which is the one direction this path must never err in.
  # It also means a limit smaller than the scheduler count leaves some shards at zero, which is correct
  # (they hold none of it) and costs only a sweep, on a limit too small for throughput to matter.
  defp shard_cap(limit, shards, shard) do
    div(limit, shards) + if(shard < rem(limit, shards), do: 1, else: 0)
  end

  # Claims one token from a shard's window counter: true when there was room. The counter pins at
  # `cap + 1` rather than at `cap`, so "used exactly the whole shard" (`cap`) stays distinguishable from
  # "asked once too often" (`cap + 1`) while a rejected caller retrying cannot run the counter away.
  # A shard holding none of the quota (`cap == 0`) always answers false, and the sweep finds the rest.
  #
  # The third element of the entry is the window this counter was written under. It is never updated (the
  # counter op targets position 2 only) and exists so `cleanup_expired_buckets/0` can age the entry
  # against its OWN window rather than re-reading configuration that may have changed, or that the caller
  # may never have taken from the configuration in the first place.
  defp take_token(identifier, action, window_start, shard, cap, window_ms) do
    key = {identifier, action, window_start, shard}
    :ets.update_counter(@table, key, {2, 1, cap, cap + 1}, {key, 0, window_ms}) <= cap
  end

  # This scheduler's shard is empty, but the quota is spread across all of them and the load need not be:
  # sweep the siblings before rejecting, so an uneven spread does not reject while quota is still unused.
  # Only reached once a shard is exhausted, so the cost sits on the rejection path, not the hot one.
  defp steal_token_or_block(identifier, action, window_start, shard, shards, limit, window_ms, elapsed) do
    stolen? =
      Enum.any?(0..(shards - 1), fn other ->
        other != shard and
          take_token(identifier, action, window_start, other, shard_cap(limit, shards, other), window_ms)
      end)

    if stolen? do
      :ok
    else
      increment_blocked_counter(identifier, action)
      # Time left in the current window: the whole quota comes back when it rolls over.
      {:error, :rate_limit_exceeded, window_ms - elapsed}
    end
  end

  # One shard per scheduler: the point is that concurrent callers write DIFFERENT keys, and the scheduler
  # id is the cheapest identifier that already tracks how much concurrency there actually is.
  defp shard_count, do: :erlang.system_info(:schedulers_online)

  defp do_check_limit(identifier, action, %{limit: limit, window_ms: window_ms}) do
    now = System.monotonic_time(:millisecond)
    key = {identifier, action}

    case :ets.lookup(@table, key) do
      [] ->
        # New bucket - allow and initialize
        :ets.insert(@table, {key, {limit - 1, now, now, window_ms}})
        :ok

      [{^key, {count, last_refill, window_start, _window_ms}}] ->
        # Calculate tokens to add based on time passed
        refill_amount = calculate_refill(now, last_refill, window_ms, limit)
        new_count = min(limit, count + refill_amount)
        new_window_start = if refill_amount > 0, do: now, else: window_start

        if new_count > 0 do
          # Allow request and consume token
          :ets.insert(@table, {key, {new_count - 1, now, new_window_start, window_ms}})
          :ok
        else
          # Rate limit exceeded
          increment_blocked_counter(identifier, action)
          retry_after_ms = max(0, window_start + window_ms - now)
          {:error, :rate_limit_exceeded, retry_after_ms}
        end
    end
  end

  defp calculate_refill(now, last_refill, window_ms, limit) do
    elapsed = now - last_refill

    if elapsed >= window_ms do
      # Full window passed, reset to max
      limit
    else
      # Partial refill based on time
      tokens_per_ms = limit / window_ms
      trunc(elapsed * tokens_per_ms)
    end
  end

  defp increment_blocked_counter(identifier, action) do
    key = {:blocked, identifier, action}
    :ets.update_counter(@table, key, {2, 1}, {key, 0})
  end

  # A bucket idle for a whole window has refilled to full, so reaping it then is indistinguishable from
  # keeping it, which is what makes the flat hour safe for every window shorter than one. It is NOT safe
  # for a longer window: an auth allowance measured over more than an hour would have its tokens handed
  # back after an hour of silence, well before they were due. Hence the floor.
  defp bucket_ttl(window_ms), do: max(@bucket_ttl, window_ms)

  # A token bucket is reaped after a flat hour of idleness; a sharded window counter is reaped a full
  # window after its own window stopped being current. The flat hour is wrong for the sharded entries in
  # BOTH directions, which is why they carry their width: too long for a narrow window (a 1-second
  # publish limit would keep an hour of dead counters, one per shard per window per user, for nothing)
  # and too short for a wide one (a window over an hour would have its LIVE counters deleted, refunding
  # the quota mid-window).
  defp cleanup_expired_buckets do
    now = System.monotonic_time(:millisecond)
    now_ms = System.system_time(:millisecond)

    expired_count =
      :ets.foldl(
        fn
          {{_identifier, _action} = key, {_count, last_refill, _window_start, window_ms}}, acc ->
            if now - last_refill > bucket_ttl(window_ms) do
              :ets.delete(@table, key)
              acc + 1
            else
              acc
            end

          # Sharded window counters: the entry carries both the start and the width of the window it
          # counted, so each one is aged against that window and nothing else.
          {{_identifier, _action, window_start, _shard} = key, _used, window_ms}, acc ->
            if window_start < now_ms - 2 * window_ms do
              :ets.delete(@table, key)
              acc + 1
            else
              acc
            end

          # Skip blocked counter entries
          {{:blocked, _identifier, _action}, _count}, acc ->
            acc
        end,
        0,
        @table
      )

    if expired_count > 0 do
      Logger.debug(I18n.t(:rate_limiter_cleanup, count: expired_count))
    end
  end

  defp schedule_cleanup do
    interval = cfg(:rate_limit_cleanup_interval_ms, 300_000)
    Process.send_after(self(), :cleanup, interval)
  end

  defp enabled?, do: cfg(:rate_limit_enabled, true)

  defp cfg(key, default) do
    Application.get_env(:malachi, key, default)
  end
end
