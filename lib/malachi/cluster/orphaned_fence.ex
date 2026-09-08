defmodule Malachi.Cluster.OrphanedFence do
  @moduledoc """
  Pure policy for the state where a segment's **store is fenced** while the control plane still calls
  it **active**: the range is closed to writes and nothing reconciles the two halves.

  `Malachi.BrokerServer`'s split and merge path closes the segment on the primary and only then
  records the seal through the control plane. Those two steps cannot be made atomic, and that is not an oversight:
  a fence has no inverse, which is the argument the whole fence design rests on, so a failed metadata
  write cannot be answered by unsealing the store. On a generic error from the second step (an `ra`
  timeout is the ordinary way to get one) `Malachi.Broker.record_seal/5` returns the broker unchanged,
  so no roll is owed and nothing retries.

  What that leaves is a produce loop with no exit: `ensure_segment/2` finds no cached segment, adopts
  the one the metadata still calls active, the store refuses the batch with `{:error, {:sealed, N}}`,
  the cache is dropped, and the next produce adopts the same segment again. A successor is never
  opened. Nothing else rescues it either: `Malachi.Cluster.Failover` requires the primary to be DEAD
  and here it is alive and answering, while healing and retention only touch sealed segments.

  So this is the level-triggered half. `candidates/2` names the segments to ask about, the caller asks
  each primary which of them are already fenced, and `plan/3` turns those answers into seal commands.
  Level-triggered rather than a retry at the source, because a retry only converges while the process
  that owed the seal is alive: `Malachi.BrokerServer` seeds offsets and sequence floors on restart, not
  rolls, so a restart loses the debt for good.

  ## Why no majority rule here

  `Malachi.Cluster.Failover` seals only on a majority of answers, and it has to: its primary is dead,
  so the committed end is only knowable from the intersection of two majorities. Here the primary is
  ALIVE and its store is fenced, and the end it reports is the very number the fencing node itself
  would have recorded had its metadata write landed. The fence is a latch, so nothing can extend the
  segment past it. That is the same rule, from the same source,
  and it needs no new argument.

  ## Why the pass may not fence

  The probing this policy drives must be read-only, which is why it asks
  `Malachi.Cluster.ReplicationServer.fenced_segments/3` (which reports a latch) rather than `seal/4`
  (which sets one). `Malachi.Cluster.HealCoordinator` learned this the hard way on the failover path:
  a pass that fenced while probing closed replicas of segments it then declined to seal, and at
  `replication_factor: 2` that wedged a range permanently, because below a majority nothing was sealed
  and nothing unseals a store. This pass visits EVERY active segment, so the same mistake here would
  be that one multiplied by the whole workload.

  ## Why it acts on the first observation

  A segment can be legitimately fenced and not yet sealed for a moment, during a normal split or merge.
  Acting inside that window is harmless, and deliberately so rather than by luck. The fence is a latch,
  so the end this pass reads is the end the splitting node read; and when the splitter's own
  `record_seal/5` then lands it is answered `{:error, {:already_sealed, existing}}`, which
  `Malachi.Broker` converges on and reports as `:ok`. Both sides agree on the same length and the split
  proceeds. Waiting for a second observation would buy nothing and would cost the coordinator a piece
  of per-segment state that a leadership change resets anyway.
  """

  alias Malachi.Metadata

  @typedoc "What a fenced store reports for a segment: where it ended and its size on disk."
  @type answer :: {non_neg_integer(), non_neg_integer()}

  @typedoc "Fence answers per segment: `%{segment_id => answer}`. A segment that is not fenced is absent."
  @type answers :: %{optional(term()) => answer()}

  @doc """
  The active segments worth asking about, grouped by the primary to ask: `[{primary, [{segment_id,
  start_offset}]}]`, sorted so a pass is deterministic.

  Grouped rather than flat because the question is asked in one batched call per primary
  (`Malachi.Cluster.ReplicationServer.fenced_segments/3`): this runs over every active segment on every
  pass, so a call per segment would make the poll's cost scale with the number of ranges.

  Only segments whose primary is LIVE. A dead primary is `Malachi.Cluster.Failover`'s case and sealing
  it from here would need that module's majority rule, so the two sets are disjoint by construction and
  neither can act on the other's segments. A segment with an empty replica set has no primary to ask
  and is skipped, as `Malachi.Broker.active_roll/2` skips it for the same reason.
  """
  @spec candidates(Metadata.t(), [Metadata.broker()]) :: [{Metadata.broker(), [{term(), non_neg_integer()}]}]
  def candidates(%Metadata{} = metadata, live_brokers) do
    live = MapSet.new(live_brokers)

    metadata.segments
    |> Map.values()
    |> Enum.filter(&active_primary_live?(&1, live))
    |> Enum.group_by(fn %{replica_set: [primary | _]} -> primary end, &{&1.id, &1.start_offset})
    |> Enum.map(fn {primary, segments} -> {primary, Enum.sort(segments)} end)
    |> Enum.sort()
  end

  @doc """
  The seal commands for the segments whose store answered FENCED, at `now_ms` (passed in so every
  replica applies the same timestamp, as the produce path already does for its own seals). Returns a
  sorted, deterministic list.

  The length comes from the store's answer, never from a counter, which is the rule
  `Malachi.Broker.record_seal/5` already states: a sealed length has to be a consequence of closing the
  segment rather than a measurement racing it.

  No `{:set_segment_replicas, ...}` alongside the seal, unlike `Malachi.Cluster.Failover`: that one
  moves a live holder to the head because the head it seals is a broker that just died. Here the head
  is alive and holds everything the seal promises, so reads already route correctly.

  A segment that is no longer in `metadata` (retention dropped it between the probe and this call) is
  skipped rather than sealed at a start offset nobody can check.
  """
  @spec plan(Metadata.t(), answers(), integer()) :: [Metadata.command()]
  def plan(%Metadata{} = metadata, answers, now_ms) do
    answers
    |> Enum.sort()
    |> Enum.flat_map(&seal(&1, metadata, now_ms))
  end

  defp active_primary_live?(%{state: :active, replica_set: [primary | _]}, live) do
    MapSet.member?(live, primary)
  end

  defp active_primary_live?(_segment, _live), do: false

  defp seal({segment_id, {end_offset, byte_size}}, metadata, now_ms) do
    case Map.get(metadata.segments, segment_id) do
      %{state: :active} = segment -> [Metadata.seal_command(segment, end_offset, byte_size, now_ms)]
      _gone_or_already_sealed -> []
    end
  end
end
