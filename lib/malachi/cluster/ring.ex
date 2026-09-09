defmodule Malachi.Cluster.Ring do
  @moduledoc """
  The pure state of the cluster's **durable routing topology**: the `Malachi.Cluster.RingTopology`
  of record, plus the highest lease fence that has written it. It is the deterministic core replicated
  by `Malachi.Cluster.RingMachine` over a dedicated `ra` cluster, exactly as `Malachi.Cluster.Lease`
  sits behind `LeaseMachine` and `Malachi.Metadata` behind `MetadataMachine`.

  Gossip disseminates the ring quickly; this is what makes it **survive a full-cluster stop**. Without
  it the ring reseeded from `MALACHI_LOG_VNODES` on restart, whose even geometry does not match a ring
  grown by splitting, so the cluster came back routing to vnodes that no longer owned the metadata.

  ## Why the whole topology, not just the ring

  The stored value is the entire `RingTopology`: ring, placements **and** the pending-split intent. A
  split records its intent before migrating so a coordinator taking over the lease can carry it to
  completion; keeping that intent here means a restart in the middle of a split does not lose it
  either.

  ## Why the commands are fenced

  Only the lease holder writes the ring, so in the happy path there is one writer. The case a lease
  does not cover is a holder that has **lost** the lease and not yet noticed. So `advance` is a
  compare-and-set on the version the writer believed it was extending, and carries the writer's lease
  `fence` (`Malachi.Cluster.Lease`'s monotonic token, which advances whenever the holder changes).
  A stale writer fails both tests and is refused by the log rather than by a timeout.

  A renewing holder keeps its fence, so consecutive splits by the same leader carry the same token:
  the fence must be non-decreasing, not strictly increasing. The version, in contrast, advances on
  every publish, so it is strictly increasing.
  """

  alias Malachi.Cluster.RingTopology

  defstruct topology: nil, fence: 0

  @type t :: %__MODULE__{topology: RingTopology.t() | nil, fence: non_neg_integer()}

  @typedoc """
  `{:init, topology}` seeds a cluster that has never had a ring; it is idempotent and **auto-fenced**,
  so every node may send it at first boot and exactly one wins. `{:advance, expected_version, fence,
  topology}` publishes a ring change, accepted only when the stored version is still
  `expected_version` and `fence` is at least the highest seen.
  """
  @type command ::
          {:init, RingTopology.t()}
          | {:advance, expected_version :: non_neg_integer(), fence :: non_neg_integer(), RingTopology.t()}

  @type reply :: :ok | {:error, {:exists, RingTopology.t()}} | {:error, {:conflict, t()}}

  @doc "A store with no ring recorded yet (the first-boot state)."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  Applies a `command`, returning `{new_state, reply}`. Deterministic: no clock, no randomness, no
  node identity, so every replica reaches the same state from the same log.

  `{:init, _}` on a store that already holds a ring answers `{:error, {:exists, current}}` rather than
  overwriting: that is what makes the concurrent first-boot seed safe. `{:advance, _, _, _}` answers
  `{:error, {:conflict, state}}` when the compare-and-set fails, handing back the whole state so the
  caller can re-read without another round trip.
  """
  @spec apply(t(), command()) :: {t(), reply()}
  def apply(%__MODULE__{topology: nil} = state, {:init, %RingTopology{} = topology}) do
    {%{state | topology: topology}, :ok}
  end

  def apply(%__MODULE__{topology: current} = state, {:init, %RingTopology{}}) do
    {state, {:error, {:exists, current}}}
  end

  # Advancing a store that was never seeded is a conflict, not an implicit init: a writer that thinks
  # it is extending a ring while none exists has read something this cluster never wrote.
  def apply(%__MODULE__{topology: nil} = state, {:advance, _expected_version, _fence, %RingTopology{}}) do
    {state, {:error, {:conflict, state}}}
  end

  def apply(%__MODULE__{} = state, {:advance, expected_version, fence, %RingTopology{} = topology}) do
    if acceptable?(state, expected_version, fence, topology) do
      {%{state | topology: topology, fence: fence}, :ok}
    else
      {state, {:error, {:conflict, state}}}
    end
  end

  @doc """
  The stored topology as a three-valued answer: `{:ok, topology}` when a ring is recorded, `:none`
  when this cluster affirms it has never had one. The distinction is the whole reason the ring lives
  in `ra`: an unreachable store answers neither, so boot can tell "genuinely fresh cluster" from "I
  cannot see the record", and only ever seeds from the environment for the former.
  """
  @spec topology(t()) :: {:ok, RingTopology.t()} | :none
  def topology(%__MODULE__{topology: nil}), do: :none
  def topology(%__MODULE__{topology: topology}), do: {:ok, topology}

  @doc "The version of the stored topology, or `nil` when none is recorded."
  @spec version(t()) :: non_neg_integer() | nil
  def version(%__MODULE__{topology: nil}), do: nil
  def version(%__MODULE__{topology: %RingTopology{version: version}}), do: version

  # The compare-and-set: the writer must have read the version it is extending, must not carry a
  # fence older than one already seen, and must actually move the version forward. The last test is
  # what keeps the stored version monotonic even if a caller hands us a topology it built wrong.
  defp acceptable?(state, expected_version, fence, topology) do
    version(state) == expected_version and fence >= state.fence and topology.version > expected_version
  end
end
