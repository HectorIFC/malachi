defmodule Malachi.Auth.LockoutManager do
  @moduledoc """
  Manages account lockouts after failed authentication attempts.

  Implements progressive lockout with increasing duration to prevent brute-force
  attacks. Tracking by username + IP combination for granular locking.

  ## Progressive Lockout

  - 1st lockout attempt (5 failures): 5 minutes
  - 2nd lockout attempt (10 failures): 15 minutes
  - 3rd lockout attempt (15 failures): 45 minutes
  - 4th lockout attempt (20 failures): 2 hours
  - 5th+ lockout attempt (25+ failures): 6 hours (maximum)

  ## Storage

  Uses volatile ETS - lockouts are lost on server restart.
  Definitive persistence will be implemented in future task.
  """
  use GenServer
  require Logger
  alias Malachi.I18n

  @table_attempts :malachi_failed_attempts
  @table_lockouts :malachi_locked_accounts
  # 1 minute
  @cleanup_interval_ms 60_000

  ## Client API

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Checks if an account is locked.

  ## Returns

  - `:not_locked` - Account is not locked
  - `{:locked, time_remaining_ms}` - Account locked, remaining time in ms

  ## Examples

      iex> LockoutManager.locked?("user", {192, 168, 1, 1})
      :not_locked
      
      iex> LockoutManager.locked?("attacker", {10, 0, 0, 1})
      {:locked, 285000}
  """
  def locked?(username, ip) do
    key = {username, format_ip(ip)}
    now = System.system_time(:millisecond)

    case :ets.lookup(@table_lockouts, key) do
      [{^key, locked_until, _count}] when locked_until > now ->
        {:locked, locked_until - now}

      _ ->
        :not_locked
    end
  end

  @doc """
  Records a failed authentication attempt.

  Increments the attempt counter and applies lockout if limit is reached.
  """
  def record_failed_attempt(username, ip) do
    GenServer.cast(__MODULE__, {:failed_attempt, username, ip})
  end

  @doc """
  Records a successful authentication.

  Clears failed attempts and lockouts for the username + IP.
  """
  def record_successful_auth(username, ip) do
    GenServer.cast(__MODULE__, {:successful_auth, username, ip})
  end

  @doc """
  Unlocks an account manually (administrative action).

  ## Parameters

  - `username` - Username
  - `ip` - Specific IP or `:all` to unlock all IPs
  """
  def unlock_account(username, ip \\ :all) do
    GenServer.call(__MODULE__, {:unlock, username, ip})
  end

  @doc """
  Returns the number of failed attempts for a username + IP.
  """
  def get_failed_attempts(username, ip) do
    key = {username, format_ip(ip)}

    case :ets.lookup(@table_attempts, key) do
      [{^key, count, _first, _last}] -> count
      [] -> 0
    end
  end

  @doc """
  Lists all currently locked accounts.

  ## Returns

  List of maps with lockout information:
  ```elixir
  [
    %{username: "user", ip: "192.168.1.1", locked_until: 1234567890, attempt_count: 5},
    ...
  ]
  ```
  """
  def list_locked_accounts do
    now = System.system_time(:millisecond)

    @table_lockouts
    |> :ets.tab2list()
    |> Enum.filter(fn {{_username, _ip}, locked_until, _count} ->
      locked_until > now
    end)
    |> Enum.map(fn {{username, ip}, locked_until, count} ->
      %{
        username: username,
        ip: ip,
        locked_until: locked_until,
        time_remaining_ms: locked_until - now,
        attempt_count: count
      }
    end)
  end

  ## GenServer Callbacks

  @impl true
  def init(_opts) do
    attempts_table =
      :ets.new(@table_attempts, [
        :set,
        :public,
        :named_table,
        read_concurrency: true,
        write_concurrency: true
      ])

    lockouts_table =
      :ets.new(@table_lockouts, [
        :set,
        :public,
        :named_table,
        read_concurrency: true,
        write_concurrency: true
      ])

    schedule_cleanup()

    Logger.info(Malachi.I18n.t(:lockout_manager_started))

    {:ok, %{attempts: attempts_table, lockouts: lockouts_table}}
  end

  @impl true
  def handle_cast({:failed_attempt, username, ip}, state) do
    key = {username, format_ip(ip)}
    now = System.system_time(:millisecond)
    max_attempts = Application.get_env(:malachi, :max_auth_attempts, 5)

    count =
      case :ets.lookup(state.attempts, key) do
        [{^key, current_count, first_time, _last_time}] ->
          :ets.insert(state.attempts, {key, current_count + 1, first_time, now})
          current_count + 1

        [] ->
          :ets.insert(state.attempts, {key, 1, now, now})
          1
      end

    # Increments global metric
    Malachi.Metrics.increment_failed_auth_attempt()

    if count >= max_attempts do
      apply_lockout(username, ip, count, state)
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast({:successful_auth, username, ip}, state) do
    key = {username, format_ip(ip)}
    :ets.delete(state.attempts, key)
    :ets.delete(state.lockouts, key)
    {:noreply, state}
  end

  @impl true
  def handle_call({:unlock, username, :all}, _from, state) do
    # Unlocks all IPs for this user
    deleted_attempts =
      :ets.select_delete(state.attempts, [
        {{{username, :"$1"}, :_, :_, :_}, [], [true]}
      ])

    deleted_lockouts =
      :ets.select_delete(state.lockouts, [
        {{{username, :"$1"}, :_, :_}, [], [true]}
      ])

    Logger.info(I18n.t(:account_unlocked_all_ips, username: username),
      username: username,
      attempts_cleared: deleted_attempts,
      lockouts_cleared: deleted_lockouts
    )

    # Audit log
    Malachi.AuditLog.log_event(
      :account_unlocked,
      %{username: username},
      "unlock_all_ips",
      :success,
      %{cleared_ips: deleted_lockouts}
    )

    {:reply, {:ok, deleted_lockouts}, state}
  end

  @impl true
  def handle_call({:unlock, username, ip}, _from, state) do
    key = {username, format_ip(ip)}
    :ets.delete(state.attempts, key)
    :ets.delete(state.lockouts, key)

    Logger.info(I18n.t(:account_unlocked, username: username, ip: format_ip(ip)),
      username: username,
      ip: format_ip(ip)
    )

    # Audit log
    Malachi.AuditLog.log_event(
      :account_unlocked,
      %{username: username, ip: ip},
      "unlock_account",
      :success,
      %{}
    )

    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    cleanup_expired_lockouts(state)
    cleanup_old_attempts(state)
    schedule_cleanup()
    {:noreply, state}
  end

  ## Private Functions

  defp apply_lockout(username, ip, attempt_count, state) do
    key = {username, format_ip(ip)}
    lockout_duration = calculate_lockout_duration(attempt_count)
    now = System.system_time(:millisecond)
    locked_until = now + lockout_duration

    :ets.insert(state.lockouts, {key, locked_until, attempt_count})

    # Increments metric
    Malachi.Metrics.increment_account_lockout()

    Logger.warning(I18n.t(:account_locked, username: username, time_remaining_ms: lockout_duration),
      username: username,
      ip: format_ip(ip),
      attempts: attempt_count,
      duration_ms: lockout_duration,
      locked_until: locked_until
    )

    # Audit log
    Malachi.AuditLog.log_event(
      :auth_lockout,
      %{username: username, ip: ip},
      "account_locked",
      :automatic,
      %{
        attempt_count: attempt_count,
        lockout_duration_ms: lockout_duration,
        locked_until: locked_until
      }
    )
  end

  defp calculate_lockout_duration(attempt_count) do
    base_duration = Application.get_env(:malachi, :lockout_duration_ms, 300_000)
    progressive = Application.get_env(:malachi, :progressive_lockout, true)
    max_attempts = Application.get_env(:malachi, :max_auth_attempts, 5)

    if progressive do
      # Calculates how many times the account was locked
      # 5 attempts = 1st lockout, 10 = 2nd lockout, 15 = 3rd, etc.
      lockout_number = div(attempt_count, max_attempts)

      # Exponential base 3: 5min → 15min → 45min → 135min → ...
      # But limited: 5min → 15min → 45min → 2h → 6h (maximum)
      case lockout_number do
        # 5 minutes
        1 -> base_duration
        # 15 minutes
        2 -> base_duration * 3
        # 45 minutes
        3 -> base_duration * 9
        # 2 hours (120 min)
        4 -> base_duration * 24
        # 6 hours (360 min) - maximum
        _ -> base_duration * 72
      end
    else
      base_duration
    end
  end

  defp cleanup_expired_lockouts(state) do
    now = System.system_time(:millisecond)

    deleted =
      :ets.select_delete(state.lockouts, [
        {{{:_, :_}, :"$1", :_}, [{:<, :"$1", now}], [true]}
      ])

    if deleted > 0 do
      Logger.debug(I18n.t(:lockout_cleanup_expired, count: deleted))
    end
  end

  defp cleanup_old_attempts(state) do
    # Remove attempts older than 1 hour
    cutoff = System.system_time(:millisecond) - 3_600_000

    deleted =
      :ets.select_delete(state.attempts, [
        {{{:_, :_}, :_, :_, :"$1"}, [{:<, :"$1", cutoff}], [true]}
      ])

    if deleted > 0 do
      Logger.debug(I18n.t(:lockout_cleanup_attempts, count: deleted))
    end
  end

  defp schedule_cleanup do
    Process.send_after(self(), :cleanup, @cleanup_interval_ms)
  end

  defp format_ip(ip) when is_tuple(ip) do
    case tuple_size(ip) do
      4 -> :inet.ntoa(ip) |> to_string()
      8 -> :inet.ntoa(ip) |> to_string()
      _ -> "invalid"
    end
  end

  defp format_ip(ip) when is_binary(ip), do: ip
  defp format_ip(_), do: "unknown"
end
