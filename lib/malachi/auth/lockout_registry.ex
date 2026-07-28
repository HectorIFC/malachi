defmodule Malachi.Auth.LockoutRegistry do
  @moduledoc """
  The pure state of **account lockouts**: failed-auth counters and progressive lockouts, keyed by
  `{username, ip}`. It is the deterministic core replicated by `Malachi.Auth.LockoutMachine` over a
  dedicated `ra` cluster (like `Malachi.Auth.UserRegistry`), so brute-force protection is **cluster-wide**
  (an attacker cannot spread attempts across nodes to dodge the limit) and **survives a restart** (a restart
  cannot reset a lockout): what the old node-local ETS store could not do.

  Progressive lockout after `max_attempts` failures, escalating on each further multiple: base → ×3 → ×9 →
  ×24 → ×72 (capped). Because `apply/3` must be deterministic across replicas, it reads **no** config and
  **no** clock: the config (`max_attempts`, `base_duration_ms`, `progressive`) travels **in the command**
  (the caller reads it once) and the time comes from the ra leader's `system_time` (`now`).
  """

  defstruct attempts: %{}, lockouts: %{}

  @type ip :: String.t()
  @type key :: {String.t(), ip()}
  @type config :: %{max_attempts: pos_integer(), base_duration_ms: pos_integer(), progressive: boolean()}
  @type attempt :: %{count: pos_integer(), first_at: integer(), last_at: integer()}
  @type lockout :: %{locked_until: integer(), count: pos_integer()}
  @type t :: %__MODULE__{attempts: %{key() => attempt()}, lockouts: %{key() => lockout()}}

  @type command ::
          {:failed_attempt, key(), config()}
          | {:successful_auth, key()}
          | {:unlock_user, String.t()}
          | {:unlock_key, key()}
          | {:cleanup, attempt_ttl_ms :: non_neg_integer()}

  @doc "An empty registry."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  Applies a `command` at time `now` (the ra leader's `system_time`, ms). Returns `{new_state, reply}`.
  Deterministic given `now`: never reads a clock or config itself.
  """
  @spec apply(t(), command(), integer()) :: {t(), term()}
  def apply(%__MODULE__{} = state, {:failed_attempt, key, config}, now) do
    prior = Map.get(state.attempts, key, %{count: 0, first_at: now, last_at: now})
    count = prior.count + 1
    attempts = Map.put(state.attempts, key, %{prior | count: count, last_at: now})
    state = %{state | attempts: attempts}

    if count >= config.max_attempts do
      duration = lockout_duration(count, config)
      locked_until = now + duration
      lockouts = Map.put(state.lockouts, key, %{locked_until: locked_until, count: count})
      {%{state | lockouts: lockouts}, %{count: count, locked: %{duration_ms: duration, locked_until: locked_until}}}
    else
      {state, %{count: count, locked: nil}}
    end
  end

  def apply(%__MODULE__{} = state, {:successful_auth, key}, _now) do
    {clear(state, key), :ok}
  end

  def apply(%__MODULE__{} = state, {:unlock_user, username}, _now) do
    cleared = state.lockouts |> Map.keys() |> Enum.count(fn {u, _ip} -> u == username end)
    state = %{state | attempts: drop_user(state.attempts, username), lockouts: drop_user(state.lockouts, username)}
    {state, {:ok, cleared}}
  end

  def apply(%__MODULE__{} = state, {:unlock_key, key}, _now) do
    {clear(state, key), :ok}
  end

  def apply(%__MODULE__{} = state, {:cleanup, attempt_ttl_ms}, now) do
    lockouts = for {k, l} <- state.lockouts, l.locked_until > now, into: %{}, do: {k, l}
    cutoff = now - attempt_ttl_ms
    attempts = for {k, a} <- state.attempts, a.last_at >= cutoff, into: %{}, do: {k, a}
    {%{state | attempts: attempts, lockouts: lockouts}, :ok}
  end

  # Defensive catch-all: an unknown command must not crash a replica (rolling upgrade safety).
  def apply(%__MODULE__{} = state, _unknown_command, _now), do: {state, {:error, :unknown_command}}

  @doc "`:not_locked`, or `{:locked, time_remaining_ms}` when `key` is locked at `now`."
  @spec locked?(t(), key(), integer()) :: :not_locked | {:locked, non_neg_integer()}
  def locked?(%__MODULE__{lockouts: lockouts}, key, now) do
    case Map.get(lockouts, key) do
      %{locked_until: until} when until > now -> {:locked, until - now}
      _absent_or_expired -> :not_locked
    end
  end

  @doc "The number of failed attempts recorded for `key` (0 if none)."
  @spec failed_attempts(t(), key()) :: non_neg_integer()
  def failed_attempts(%__MODULE__{attempts: attempts}, key) do
    case Map.get(attempts, key) do
      %{count: count} -> count
      nil -> 0
    end
  end

  @doc "Every account currently locked at `now`, as info maps."
  @spec list_locked(t(), integer()) :: [map()]
  def list_locked(%__MODULE__{lockouts: lockouts}, now) do
    for {{username, ip}, %{locked_until: until, count: count}} <- lockouts, until > now do
      %{username: username, ip: ip, locked_until: until, time_remaining_ms: until - now, attempt_count: count}
    end
  end

  # --- internals ---

  defp clear(state, key) do
    %{state | attempts: Map.delete(state.attempts, key), lockouts: Map.delete(state.lockouts, key)}
  end

  defp drop_user(map, username) do
    for {{u, _ip} = k, v} <- map, u != username, into: %{}, do: {k, v}
  end

  # Progressive: the lockout escalates on each multiple of `max_attempts`: base → ×3 → ×9 → ×24 → ×72 (cap).
  defp lockout_duration(count, %{progressive: true, max_attempts: max, base_duration_ms: base}) do
    case div(count, max) do
      1 -> base
      2 -> base * 3
      3 -> base * 9
      4 -> base * 24
      _ -> base * 72
    end
  end

  defp lockout_duration(_count, %{base_duration_ms: base}), do: base
end
