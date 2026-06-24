defmodule Malachi.Cluster.Placement do
  @moduledoc """
  Pure replica **placement and self-healing** policy for segments — the decision layer of the
  data plane. The segment is NorthGuard's unit of replication: each lives on a *replica set* of
  `replication_factor` brokers. This module decides *which* brokers, and which segments have
  lost replicas and must be re-replicated. It is a pure function of `(metadata, available
  brokers, replication factor)` — it never touches storage or the network.

  Placement uses **rendezvous (HRW) hashing**: for a segment, every available broker is scored
  by `:erlang.phash2({segment_id, broker})`, and the top `replication_factor` brokers win. Two
  properties make this the right fit for a Raft-replicated control plane:

    * **Deterministic** — the same inputs yield the same replica set on every replica, so a
      placement decision can be derived inside (or fed through) the `Malachi.Metadata` machine
      without diverging across nodes.
    * **Minimum reshuffle** — removing a broker only moves the segments that broker hosted; the
      surviving replicas keep their ranks, so `heal/3` preserves live members and only fills the
      vacated slots.

  The policy *decides*; `Malachi.Metadata` *records*. `heal/3` returns a list of
  `{:set_segment_replicas, ...}` commands to apply through the RSM (and, later, Raft) — it does
  not mutate anything itself.

  Self-healing covers **all** segments, sealed as well as active: a sealed segment is immutable
  but its data must still survive `replication_factor` failures, so a lost replica is re-placed.

  `available_brokers` is an abstract broker set supplied by the caller. Membership (which
  brokers are actually alive) is a separate concern (SWIM, later); taking it as a parameter is
  what keeps this layer pure and pins down the contract that membership will have to satisfy.
  """

  alias Malachi.Metadata

  @typedoc "The target replica count, clamped to the number of available brokers."
  @type target :: non_neg_integer()

  @doc """
  Chooses the replica set for `segment_id` from `available_brokers` via rendezvous hashing,
  returning the `min(replication_factor, length(available_brokers))` highest-scoring brokers
  (all distinct). Duplicate brokers in the input are ignored.

  Returns `{:error, :no_brokers}` if there are none, or `{:error, :invalid_replication_factor}`
  if `replication_factor < 1`.
  """
  @spec place(Metadata.segment_id(), [Metadata.broker()], pos_integer()) ::
          {:ok, [Metadata.broker()]} | {:error, :no_brokers | :invalid_replication_factor}
  def place(_segment_id, _available_brokers, replication_factor) when replication_factor < 1 do
    {:error, :invalid_replication_factor}
  end

  def place(segment_id, available_brokers, replication_factor) do
    case Enum.uniq(available_brokers) do
      [] -> {:error, :no_brokers}
      brokers -> {:ok, ranked(segment_id, brokers) |> Enum.take(replication_factor)}
    end
  end

  @doc """
  The ids of segments that are under-replicated for the given live broker set: those with fewer
  live replicas than the achievable target `min(replication_factor, length(available_brokers))`.

  A replica counts as live only if it is in `available_brokers`, so a segment whose broker has
  left is flagged. The target is clamped to what is achievable, so a cluster that is simply too
  small to reach `replication_factor` is not reported as perpetually under-replicated. Returns a
  sorted list (deterministic).
  """
  @spec under_replicated(Metadata.t(), [Metadata.broker()], pos_integer()) ::
          [Metadata.segment_id()]
  def under_replicated(%Metadata{} = metadata, available_brokers, replication_factor) do
    live = MapSet.new(available_brokers)
    target = min(replication_factor, MapSet.size(live))

    metadata.segments
    |> Enum.filter(fn {_id, segment} -> live_count(segment, live) < target end)
    |> Enum.map(fn {id, _segment} -> id end)
    |> Enum.sort()
  end

  @doc """
  A self-healing plan: a list of `{:set_segment_replicas, segment_id, replica_set}` commands
  that restore every under-replicated segment to a full replica set, to be applied through
  `Malachi.Metadata`. Re-placing over the live broker set preserves the surviving replicas (HRW
  retention) and only fills the vacated slots, so one pass reaches a fully-replicated fixpoint.
  Returns `[]` when nothing needs healing.
  """
  @spec heal(Metadata.t(), [Metadata.broker()], pos_integer()) :: [Metadata.command()]
  def heal(%Metadata{} = metadata, available_brokers, replication_factor) do
    metadata
    |> under_replicated(available_brokers, replication_factor)
    |> Enum.map(fn segment_id ->
      # under_replicated only yields ids when target > 0, which requires a non-empty broker
      # set, so place/3 cannot fail here.
      {:ok, replica_set} = place(segment_id, available_brokers, replication_factor)
      {:set_segment_replicas, segment_id, replica_set}
    end)
  end

  # --- internals ---

  # Brokers ordered by descending rendezvous score, tie-broken by the broker term for a stable,
  # input-order-independent ranking.
  defp ranked(segment_id, brokers) do
    Enum.sort_by(brokers, fn broker -> {:erlang.phash2({segment_id, broker}), broker} end, :desc)
  end

  # Count *distinct* live replicas: a replica set that happens to list a broker twice still
  # provides only one copy, so it must not be mistaken for two toward the target.
  defp live_count(segment, live) do
    segment.replica_set |> Enum.uniq() |> Enum.count(&MapSet.member?(live, &1))
  end
end
