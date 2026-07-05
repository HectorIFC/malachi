defmodule Malachi.Cluster.Lease do
  @moduledoc """
  The pure state of a **lease** — a fenced, expiring lock that elects a single holder for the
  non-idempotent work of rebalancing (R3). It is the deterministic core replicated by `LeaseMachine`
  over a dedicated `ra` cluster (exactly as `Malachi.Metadata` sits behind `MetadataMachine`), so every
  replica reaches the same lease state from the same command log.

  A candidate `acquire_or_renew`s the lease; it is granted when the lease is **free**, **already held by
  that candidate** (a renewal), or **expired** (`now >= renew_at + duration_ms`). The candidate identity
  must be a **non-nil** term (`nil` is the free-lease sentinel); in practice it is the node. Time is supplied by the
  caller — `LeaseMachine` passes the ra leader's `system_time` — and never read inside `apply/3`: reading
  a wall clock there would be non-deterministic and break Raft. So a single clock (the lease cluster's
  current leader) decides expiry, avoiding the cross-node clock skew a client-supplied time would carry.

  `fence` is a monotonic **fencing token**: it advances only when the holder changes (a renewal keeps
  it). A holder carries its token into the work it fences; if the token has since advanced, a stale
  ex-holder's writes can be rejected — the guard against two simultaneous holders.
  """

  defstruct holder: nil, fence: 0, renew_at: nil, duration_ms: nil

  @type t :: %__MODULE__{
          holder: term() | nil,
          fence: non_neg_integer(),
          renew_at: integer() | nil,
          duration_ms: pos_integer() | nil
        }

  @type command ::
          {:acquire_or_renew, holder :: term(), duration_ms :: pos_integer()}
          | {:release, holder :: term(), fence :: non_neg_integer()}

  @type reply :: {:ok, fence :: non_neg_integer()} | {:error, {:held, holder :: term()}} | :ok

  @doc "A free lease (no holder, fence 0)."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  Applies a lease `command` at time `now` (epoch ms, from the ra leader's `system_time`). Returns
  `{new_state, reply}`. Deterministic given `now`.
  """
  @spec apply(t(), command(), integer()) :: {t(), reply()}
  def apply(%__MODULE__{} = state, {:acquire_or_renew, candidate, duration_ms}, now) do
    cond do
      state.holder == candidate ->
        # renewal: same holder keeps its fence and extends the term
        {%{state | renew_at: now, duration_ms: duration_ms}, {:ok, state.fence}}

      free?(state) or expired?(state, now) ->
        # new acquisition: the holder changes, so the fence advances
        fence = state.fence + 1
        {%{state | holder: candidate, fence: fence, renew_at: now, duration_ms: duration_ms}, {:ok, fence}}

      true ->
        # held by someone else and still valid
        {state, {:error, {:held, state.holder}}}
    end
  end

  def apply(%__MODULE__{} = state, {:release, candidate, fence}, _now) do
    if state.holder == candidate and state.fence == fence do
      {%{state | holder: nil, renew_at: nil, duration_ms: nil}, :ok}
    else
      # not the current holder (or a stale token): releasing is idempotent and benign
      {state, :ok}
    end
  end

  @doc "The current holder, or `nil` when the lease is free."
  @spec holder(t()) :: term() | nil
  def holder(%__MODULE__{holder: holder}), do: holder

  defp free?(%__MODULE__{holder: nil}), do: true
  defp free?(%__MODULE__{}), do: false

  defp expired?(%__MODULE__{renew_at: nil}, _now), do: false

  defp expired?(%__MODULE__{renew_at: renew_at, duration_ms: duration_ms}, now) do
    now >= renew_at + duration_ms
  end
end
