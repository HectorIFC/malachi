defmodule Malachi.Auth.LockoutManager do
  @moduledoc """
  Account-lockout facade over the **replicated** lockout store (`Malachi.Auth.LockoutServer`, a dedicated
  `ra` cluster). Applies progressive lockout after repeated failed logins, keyed by `{username, ip}`.

  Replaces the old node-local ETS store (P6): lockouts now replicate across the cluster, so brute-force
  protection is **cluster-wide** (an attacker cannot spread attempts across nodes to dodge the limit) and
  **survives a restart** (a restart cannot reset a lockout). The pure lockout logic lives in
  `Malachi.Auth.LockoutRegistry`; this module reads the lockout config, routes reads to the local replica
  and writes through the Raft log, and performs the observable side effects (metrics, logging, audit) for a
  newly applied or cleared lockout.

  ## Progressive Lockout

  - 1st lockout (5 failures): 5 minutes
  - 2nd lockout (10 failures): 15 minutes
  - 3rd lockout (15 failures): 45 minutes
  - 4th lockout (20 failures): 2 hours
  - 5th+ lockout (25+ failures): 6 hours (maximum)

  ## Process role and blocking

  Reads (`locked?`/`get_failed_attempts`/`list_locked_accounts`) are direct local queries, no consensus,
  no GenServer round-trip. The hot-path **writes** (`record_failed_attempt`/`record_successful_auth`) run in
  a **background task** on `Malachi.TaskSupervisor`, so the auth path never blocks on a consensus round-trip
  or an ra leader election, and no single process funnels every write; `ra` serializes the concurrent writes
  itself, and the rate/connection limiters bound the task volume. `unlock_account` is a synchronous admin
  action (the operator wants the result). The GenServer owns only the periodic **cleanup** timer.
  `Malachi.Application` forms the ra lockout cluster before this process starts.
  """
  use GenServer
  require Logger
  alias Malachi.Auth.LockoutServer
  alias Malachi.I18n

  # The dedicated ra cluster's name (formed in Malachi.Application). Reads/writes address the local member.
  @cluster Malachi.LogLockouts
  # Drop failed-attempt counters idle for longer than this on each cleanup (mirrors the legacy 1h window).
  @attempt_ttl_ms 3_600_000
  @cleanup_interval_ms 60_000

  @typedoc "An IP as the acceptor supplies it (tuple) or already formatted (string)."
  @type ip :: :inet.ip_address() | String.t()

  ## Client API

  @doc "Starts the lockout manager, which owns the periodic cleanup timer for the replicated store."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Checks if an account is locked.

  Returns `:not_locked`, or `{:locked, time_remaining_ms}` when the account is locked. A direct read of the
  local replica (no consensus round-trip).
  """
  @spec locked?(String.t(), ip()) :: :not_locked | {:locked, non_neg_integer()}
  def locked?(username, ip) do
    case LockoutServer.locked?(server_id(), key(username, ip), now()) do
      {:ok, status} ->
        status

      {:error, reason} ->
        # Fail open: a transient store hiccup must not lock out legitimate users: the rate limiter is the
        # backstop. Logged so an operator sees the enforcement gap.
        Logger.warning(I18n.t(:lockout_store_unavailable, operation: "locked?", reason: inspect(reason)))
        :not_locked
    end
  end

  @doc """
  Records a failed authentication attempt, applying a lockout once the limit is reached.

  Non-blocking: the consensus write runs in a background task (see the module doc), so the auth path is
  never coupled to ra latency. Returns `:ok` immediately.
  """
  @spec record_failed_attempt(String.t(), ip()) :: :ok
  def record_failed_attempt(username, ip) do
    background(fn -> do_failed_attempt(username, ip) end)
  end

  @doc """
  Records a successful authentication, clearing failed attempts and any lockout for the username + IP.

  Non-blocking (background task). Skips the write entirely on the common case (a clean login with no prior
  failures to reset). Returns `:ok` immediately.
  """
  @spec record_successful_auth(String.t(), ip()) :: :ok
  def record_successful_auth(username, ip) do
    background(fn -> do_successful_auth(username, ip) end)
  end

  @doc """
  Unlocks an account manually (administrative action).

  `ip` is a specific IP or `:all` to unlock every IP for the user. Synchronous, returns `:ok` (single IP)
  or `{:ok, cleared_count}` (`:all`) once the change is committed, or `{:error, reason}` if the store is
  unreachable.
  """
  @spec unlock_account(String.t(), ip() | :all) :: :ok | {:ok, non_neg_integer()} | {:error, term()}
  def unlock_account(username, ip \\ :all)
  def unlock_account(username, :all), do: unlock_all(username)
  def unlock_account(username, ip), do: unlock_one(username, ip)

  @doc "Returns the number of failed attempts for a username + IP."
  @spec get_failed_attempts(String.t(), ip()) :: non_neg_integer()
  def get_failed_attempts(username, ip) do
    case LockoutServer.failed_attempts(server_id(), key(username, ip)) do
      {:ok, count} -> count
      {:error, _reason} -> 0
    end
  end

  @doc "Lists all currently locked accounts as info maps (`username`, `ip`, `locked_until`, ...)."
  @spec list_locked_accounts() :: [map()]
  def list_locked_accounts do
    case LockoutServer.list_locked(server_id(), now()) do
      {:ok, locked} -> locked
      {:error, _reason} -> []
    end
  end

  ## GenServer Callbacks (cleanup timer only)

  @impl true
  def init(_opts) do
    schedule_cleanup()
    Logger.info(I18n.t(:lockout_manager_started))
    {:ok, %{}}
  end

  @impl true
  def handle_info(:cleanup, state) do
    # Compaction only: expiry is evaluated lazily on read, so a failed cleanup is harmless (retried next tick).
    _ = LockoutServer.cleanup(server_id(), @attempt_ttl_ms)
    schedule_cleanup()
    {:noreply, state}
  end

  ## Writes (run in a background task via `record_*`)

  defp do_failed_attempt(username, ip) do
    Malachi.Metrics.increment_failed_auth_attempt()

    case LockoutServer.record_failed_attempt(server_id(), key(username, ip), config()) do
      {:ok, %{locked: nil}} ->
        :ok

      {:ok, %{count: count, locked: %{duration_ms: duration, locked_until: locked_until}}} ->
        on_locked(username, ip, count, duration, locked_until)

      {:error, reason} ->
        Logger.warning(I18n.t(:lockout_store_unavailable, operation: "record_failed_attempt", reason: inspect(reason)))
    end
  end

  defp do_successful_auth(username, ip) do
    key = key(username, ip)

    # Skip the consensus write on the common case (a clean login with nothing to reset). The guard read is a
    # fast local query; only a user with prior failed attempts pays for the clearing write.
    case LockoutServer.failed_attempts(server_id(), key) do
      {:ok, count} when count > 0 -> clear_attempts(key)
      _none_or_unreadable -> :ok
    end
  end

  defp clear_attempts(key) do
    case LockoutServer.record_successful_auth(server_id(), key) do
      {:ok, :ok} ->
        :ok

      {:error, reason} ->
        Logger.warning(I18n.t(:lockout_store_unavailable, operation: "record_successful_auth", reason: inspect(reason)))
    end
  end

  defp unlock_all(username) do
    case LockoutServer.unlock_user(server_id(), username) do
      {:ok, {:ok, cleared}} ->
        Logger.info(I18n.t(:account_unlocked_all_ips, username: username),
          username: username,
          lockouts_cleared: cleared
        )

        Malachi.AuditLog.log_event(
          :account_unlocked,
          %{username: username},
          "unlock_all_ips",
          :success,
          %{cleared_ips: cleared}
        )

        {:ok, cleared}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp unlock_one(username, ip) do
    case LockoutServer.unlock_key(server_id(), key(username, ip)) do
      {:ok, :ok} ->
        Logger.info(I18n.t(:account_unlocked, username: username, ip: format_ip(ip)),
          username: username,
          ip: format_ip(ip)
        )

        Malachi.AuditLog.log_event(
          :account_unlocked,
          %{username: username, ip: ip},
          "unlock_account",
          :success,
          %{}
        )

        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  ## Private Functions

  # Fire-and-forget on the shared task supervisor: keeps the caller (the auth path) non-blocking while the
  # consensus write happens off to the side. Returns `:ok` immediately.
  defp background(fun) do
    _ = Task.Supervisor.start_child(Malachi.TaskSupervisor, fun)
    :ok
  end

  # Metrics + structured log + audit for a lockout the just-recorded attempt (re)applied. Runs once, on the
  # node that handled the attempt (driven by the machine reply), so the deterministic apply/3 stays pure.
  defp on_locked(username, ip, attempt_count, duration, locked_until) do
    Malachi.Metrics.increment_account_lockout()

    Logger.warning(I18n.t(:account_locked, username: username, time_remaining_ms: duration),
      username: username,
      ip: format_ip(ip),
      attempts: attempt_count,
      duration_ms: duration,
      locked_until: locked_until
    )

    Malachi.AuditLog.log_event(
      :auth_lockout,
      %{username: username, ip: ip},
      "account_locked",
      :automatic,
      %{attempt_count: attempt_count, lockout_duration_ms: duration, locked_until: locked_until}
    )
  end

  # The lockout config, read once per write and carried inside the command so every replica applies the
  # identical decision (the machine itself reads no config: that would be non-deterministic).
  defp config do
    %{
      max_attempts: Application.get_env(:malachi, :max_auth_attempts, 5),
      base_duration_ms: Application.get_env(:malachi, :lockout_duration_ms, 300_000),
      progressive: Application.get_env(:malachi, :progressive_lockout, true)
    }
  end

  defp server_id, do: {@cluster, node()}
  defp key(username, ip), do: {username, format_ip(ip)}
  defp now, do: System.system_time(:millisecond)
  defp schedule_cleanup, do: Process.send_after(self(), :cleanup, @cleanup_interval_ms)

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
