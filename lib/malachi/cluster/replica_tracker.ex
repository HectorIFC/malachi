defmodule Malachi.Cluster.ReplicaTracker do
  @moduledoc """
  Pure quorum-commit tracking for the replication of a **single segment** — the deterministic
  core of the data-plane replication mechanism, with no processes or network.

  A segment is replicated across an ordered `replica_set` (chosen by `Malachi.Cluster.Placement`).
  The first broker is the **primary**: it accepts writes and forwards records to the followers.
  Each replica reports the highest offset it has durably stored (its *match offset*); this module
  folds those reports into the segment's **commit offset** — the highest offset present on a
  **quorum** (majority) of replicas, which is the point up to which the data is durable and may be
  acknowledged to producers.

  Quorum (rather than all-replicas) means a write commits once ⌊N/2⌋+1 replicas hold it,
  tolerating ⌊(N-1)/2⌋ failures without blocking on the slowest replica. The primary is just a
  replica that is always ahead, so it needs no special-casing in the commit math.

  This is the counterpart, for segment *data*, of the deterministic `Malachi.Metadata` machine for
  *metadata*: the subtle "what is committed" logic lives here, pure and property-tested, before any
  transport (primary → followers over distributed Erlang) is wired on top. Primary failover and
  replica-set changes are a later concern; this models a fixed replica set.
  """

  alias Malachi.Metadata

  @typedoc "The highest contiguous offset a replica has durably stored."
  @type offset :: non_neg_integer()

  @type t :: %__MODULE__{
          replica_set: [Metadata.broker()],
          match: %{Metadata.broker() => offset()}
        }

  defstruct replica_set: [], match: %{}

  # A replica that has acked nothing sorts below any real (non-negative) offset.
  @none -1

  @doc """
  Builds a tracker for a segment replicated across `replica_set` (the first broker is the
  primary). Duplicates are ignored. Raises `ArgumentError` on an empty/invalid replica set —
  `Malachi.Cluster.Placement.place/3` always yields a non-empty, distinct set.
  """
  @spec new([Metadata.broker()]) :: t()
  def new(replica_set) when is_list(replica_set) do
    case Enum.uniq(replica_set) do
      [] -> raise ArgumentError, "replica_set must be a non-empty list of brokers"
      replicas -> %__MODULE__{replica_set: replicas, match: %{}}
    end
  end

  @doc "The primary broker (the first of the replica set), which accepts writes."
  @spec primary(t()) :: Metadata.broker()
  def primary(%__MODULE__{replica_set: [primary | _]}), do: primary

  @doc "The number of replicas that must hold an offset for it to commit (a strict majority)."
  @spec quorum(t()) :: pos_integer()
  def quorum(%__MODULE__{replica_set: replica_set}), do: div(length(replica_set), 2) + 1

  @doc """
  Records that `broker` has durably stored up to `offset`. Monotonic: a stale report (lower than
  what was already recorded) is ignored. `{:error, :unknown_replica}` if `broker` is not in the
  replica set.
  """
  @spec ack(t(), Metadata.broker(), offset()) :: {:ok, t()} | {:error, :unknown_replica}
  def ack(%__MODULE__{} = tracker, broker, offset) when is_integer(offset) and offset >= 0 do
    if broker in tracker.replica_set do
      match = Map.update(tracker.match, broker, offset, &max(&1, offset))
      {:ok, %{tracker | match: match}}
    else
      {:error, :unknown_replica}
    end
  end

  @doc """
  The segment's commit offset: the highest offset stored on at least `quorum/1` replicas, or
  `:none` if no offset has reached a quorum yet. This is monotonic across `ack/3`s.
  """
  @spec commit_offset(t()) :: offset() | :none
  def commit_offset(%__MODULE__{} = tracker) do
    quorum = quorum(tracker)

    # The q-th largest match offset is, by definition, the highest offset held by at least q
    # replicas. Replicas that have acked nothing contribute @none, which sorts last.
    committed =
      tracker.replica_set
      |> Enum.map(fn broker -> Map.get(tracker.match, broker, @none) end)
      |> Enum.sort(:desc)
      |> Enum.at(quorum - 1)

    if committed >= 0, do: committed, else: :none
  end

  @doc "Whether `offset` is committed (stored on a quorum of replicas)."
  @spec committed?(t(), offset()) :: boolean()
  def committed?(%__MODULE__{} = tracker, offset) when is_integer(offset) and offset >= 0 do
    case commit_offset(tracker) do
      :none -> false
      committed -> committed >= offset
    end
  end
end
