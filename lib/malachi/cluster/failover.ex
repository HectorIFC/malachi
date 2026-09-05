defmodule Malachi.Cluster.Failover do
  @moduledoc """
  Pure primary-failover policy for **active** segments: when an active segment's primary is no longer
  alive, the segment is **sealed** and writing rolls to a fresh one, which is what NorthGuard does
  ("we just seal it, make a new one, move the producers over to that new segment").

  Sealing rather than promoting is what makes this safe. A batch is acknowledged once a majority holds
  it durably, so a replica outside that majority can be behind. Promoting such a replica would let it
  append at offsets the dead primary had already assigned and acknowledged, leaving two replicas with
  different records under the same offset, which nothing downstream detects: the `expected_first` chain
  in replication rejects a gap rather than a conflict, and the integrity scrub verifies each copy
  against itself. Sealing removes that possibility **by construction**, because an offset the sealed
  segment already assigned is never handed out again. The segment store refuses an append to a sealed
  segment, so the seal also fences a returning old primary, and the freshly sealed segment is
  picked up by `Malachi.Cluster.SelfHealing`, which already re-replicates sealed segments.

  ## The seal point, and when a range is left blocked

  The seal is placed at the **highest durable end** reported by the segment's replicas, and only when a
  **majority** of the replica set answered the probe. An acknowledged write lives on a majority, so with
  a majority reporting, at least one reporter holds every acknowledged record. Below a majority the
  committed end is unknowable, and sealing at a lone survivor's end could discard acknowledged writes:
  precisely what this policy exists to prevent. So the segment is left alone and its range stops
  accepting writes.

  That block is not a latch. The caller (`Malachi.Cluster.HealCoordinator`) is a periodic
  level-triggered loop, so the next pass re-evaluates: as soon as a majority answers again, the seal is
  emitted and writing rolls to a new segment on its own. If a majority never returns, the range stays
  blocked, which is the CP choice, and recovering it is a deliberate operator decision rather than
  something this policy takes on the operator's behalf.

  The probing itself lives in the caller: `candidates/2` names the segments to probe, and `plan/4`
  turns the probe results into commands, so the policy stays pure and testable without processes.
  """

  alias Malachi.Metadata

  @typedoc "What one replica durably holds for a segment: its end offset and byte size."
  @type probe :: {non_neg_integer(), non_neg_integer()}

  @typedoc "Probe results per segment: `%{segment_id => %{replica_ref => probe}}`."
  @type probes :: %{optional(term()) => %{optional(term()) => probe()}}

  @doc """
  The active segments whose primary is dead, each with the live replicas worth probing. The caller
  probes these and feeds the results to `plan/4`. Sorted, so a pass is deterministic.
  """
  @spec candidates(Metadata.t(), [Metadata.broker()]) :: [{term(), [Metadata.broker()]}]
  def candidates(%Metadata{} = metadata, live_brokers) do
    live = MapSet.new(live_brokers)

    metadata.segments
    |> Map.values()
    |> Enum.filter(&active_primary_dead?(&1, live))
    |> Enum.map(&{&1.id, Enum.filter(&1.replica_set, fn replica -> MapSet.member?(live, replica) end)})
    |> Enum.reject(fn {_id, replicas} -> replicas == [] end)
    |> Enum.sort()
  end

  @doc """
  The seal commands for the probed segments, at `now_ms` (passed in so every replica applies the same
  timestamp, as the produce path already does for its own seals). A segment whose probes do not reach a
  majority of its replica set is skipped: see the moduledoc on why that leaves the range blocked rather
  than risking acknowledged data. Returns a sorted (deterministic) list.
  """
  @spec plan(Metadata.t(), [Metadata.broker()], probes(), integer()) :: [Metadata.command()]
  def plan(%Metadata{} = metadata, live_brokers, probes, now_ms) do
    # `candidates/2` is already sorted, and the per-segment pair must stay in the order it is built
    # (seal, then head), so the list is not re-sorted here.
    metadata
    |> candidates(live_brokers)
    |> Enum.flat_map(&seal(&1, metadata, probes, now_ms))
  end

  @doc """
  Whether `answered` replicas are a majority of `replica_set`, the condition under which a seal point
  can be trusted. Public because the caller logs the blocked case and wants the same rule.
  """
  @spec majority?(non_neg_integer(), [Metadata.broker()]) :: boolean()
  def majority?(answered, replica_set), do: answered * 2 > length(replica_set)

  defp active_primary_dead?(%{state: :active, replica_set: [primary | _]}, live) do
    not MapSet.member?(live, primary)
  end

  defp active_primary_dead?(_segment, _live), do: false

  defp seal({segment_id, _live_replicas}, metadata, probes, now_ms) do
    segment = Map.fetch!(metadata.segments, segment_id)
    answers = Map.get(probes, segment_id, %{})

    if majority?(map_size(answers), segment.replica_set) do
      # The furthest reporter defines the seal: it is the only one that can hold every acknowledged
      # record. Its byte size travels with it rather than being maxed independently, since a length
      # from one replica and a size from another would describe a segment that never existed.
      {furthest, {end_offset, byte_size}} = Enum.max_by(answers, fn {_replica, {offset, _bytes}} -> offset end)

      [
        {:seal_segment, segment_id, end_offset - segment.start_offset, byte_size, now_ms},
        # Reads route to the head of the replica set, and the head here is the broker that just died,
        # so the sealed segment would answer `:unreachable` until re-replication got to it. Moving the
        # furthest replica to the head restores reads at once, and it holds everything the seal
        # promised. Reordering is what was unsafe on an ACTIVE segment (the new head would reissue
        # offsets); on a sealed one no append is possible at all, so it is only a routing change.
        {:set_segment_replicas, segment_id, [furthest | List.delete(segment.replica_set, furthest)]}
      ]
    else
      []
    end
  end
end
