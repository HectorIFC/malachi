defmodule MalachiMQ.RateLimiter do
  @moduledoc """
  Token bucket rate limiter using ETS for high-performance limits.
  Tracks requests per IP/user across different actions.

  ## Schema

  The ETS table stores three types of entries:

  - `{{identifier, action}, {count, last_refill_ms, window_start_ms}}` - Token buckets
  - `{{:blocked, identifier, action}, count}` - Blocked request counters

  ## Actions

  - `:auth` - Authentication attempts (tracked by IP)
  - `:publish` - Message publishing (tracked by username)
  - `:subscribe` - Queue subscriptions (tracked by username)

  ## Configuration

  Set via environment variables or runtime config:

  - `rate_limit_enabled` - Enable/disable rate limiting (default: true)
  - `auth_rate_limit` - Max auth attempts per window (default: 10)
  - `auth_rate_window_ms` - Auth window duration (default: 60000)
  - `publish_rate_limit` - Max publish per window (default: 1000)
  - `publish_rate_window_ms` - Publish window duration (default: 1000)
  - `subscribe_rate_limit` - Max subscribe per window (default: 100)
  - `subscribe_rate_window_ms` - Subscribe window duration (default: 60000)
  - `rate_limit_cleanup_interval_ms` - Cleanup interval (default: 300000)
  """

  use GenServer
  require Logger
  alias MalachiMQ.I18n

  @table :malachimq_rate_limits

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
    if cfg(:rate_limit_enabled, true) do
      GenServer.call(__MODULE__, {:check_limit, identifier, action, config})
    else
      :ok
    end
  end

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
    result = do_check_limit(identifier, action, config)
    {:reply, result, state}
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
    total_buckets =
      :ets.select_count(@table, [
        {{{:_, :_}, {:_, :_, :_}}, [], [true]}
      ])

    total_blocked =
      :ets.select_count(@table, [
        {{{{:blocked, :_, :_}, :_}}, [], [true]}
      ])

    stats = %{
      total_buckets: total_buckets,
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

  defp do_check_limit(identifier, action, %{limit: limit, window_ms: window_ms}) do
    now = System.monotonic_time(:millisecond)
    key = {identifier, action}

    case :ets.lookup(@table, key) do
      [] ->
        # New bucket - allow and initialize
        :ets.insert(@table, {key, {limit - 1, now, now}})
        :ok

      [{^key, {count, last_refill, window_start}}] ->
        # Calculate tokens to add based on time passed
        refill_amount = calculate_refill(now, last_refill, window_ms, limit)
        new_count = min(limit, count + refill_amount)
        new_window_start = if refill_amount > 0, do: now, else: window_start

        if new_count > 0 do
          # Allow request and consume token
          :ets.insert(@table, {key, {new_count - 1, now, new_window_start}})
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

  defp cleanup_expired_buckets do
    now = System.monotonic_time(:millisecond)
    # 1 hour
    bucket_ttl = 3_600_000

    expired_count =
      :ets.foldl(
        fn
          {{_identifier, _action} = key, {_count, last_refill, _window_start}}, acc ->
            if now - last_refill > bucket_ttl do
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

  defp cfg(key, default) do
    Application.get_env(:malachimq, key, default)
  end
end
