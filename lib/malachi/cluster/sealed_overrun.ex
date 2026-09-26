defmodule Malachi.Cluster.SealedOverrun do
  @moduledoc """
  Pure policy for the state where a segment's **control plane says sealed at N** while a copy of it on
  disk does not agree with that length: the record of the seal is true and the bytes are not.

  The mirror of `Malachi.Cluster.OrphanedFence`, which handles the opposite half (a store that is
  fenced while the control plane still calls the segment active). Both exist for the same reason: the
  seal is two things, a latch on a store and a length in the metadata, and nothing makes the two steps
  atomic.

  ## How a copy comes to hold more than the seal says

  `Malachi.Cluster.ReplicationServer`'s write fence stops a copy growing AFTER the seal reached that
  server. Neither way into this state is covered by it:

    * a primary writes a batch to its own log durably, loses quorum or dies before the batch is
      acknowledged, and `Malachi.Cluster.Failover` seals the segment on the replicas that answered, at
      the shorter length they hold. The old primary comes back holding one record more, and the fence
      never reached it, so it never refused anything;
    * `Malachi.Cluster.Failover` seals at the largest end a majority reported but fences each replica
      at the end THAT replica holds, so a replica above the recorded length ends up fenced above it.

  Neither is visible to anything that runs today. The sealed-copy integrity probe in
  `Malachi.Cluster.SelfHealing` reports a copy that falls SHORT of the recorded byte size and treats
  one that runs past as intact, which is why only the storage chaos drill ever found this.

  ## Why the control plane's length wins

  For a segment the control plane records as sealed, `start_offset + length` is the end, always. Four
  reasons, and they are not interchangeable opinions:

    * `Malachi.Metadata` REFUSES to rewrite a length once it is recorded (`{:error, {:already_sealed,
      existing}}`), so the number is immutable by construction and there is no other candidate;
    * the offsets above it already belong to the SUCCESSOR segment. Raising the length would put one
      offset in two segments, which is the failure `Malachi.Cluster.Failover` exists to prevent;
    * `Malachi.Broker`'s read budget already clamps a sealed read to the recorded length, so a record
      past it was never served and never acknowledged to anybody. Dropping it loses nothing that was
      promised;
    * NorthGuard's segment is the unit of replication and a sealed one is immutable (meetup
      transcript, lines 436 and 437), and the coordinator of a vnode is what carries out the protocols
      attached to the metadata it owns (lines 506 to 508).

  The transcript does not say what NorthGuard does with a copy that runs past a sealed length, so
  bringing it down to the recorded one is a DEDUCTION from those two statements rather than a rule
  taken from it. The opposite deduction (keep the bytes, raise the length) is ruled out by the second
  reason above, not by the transcript.

  For a segment the control plane still calls ACTIVE the rule is the other way round: no length has
  been decided, so the disk is what there is, and choosing a safe point to seal it at is the open half
  of issue #210, which has to follow this same split.

  ## Why fencing here is safe, when fencing while probing is not

  `Malachi.Cluster.HealCoordinator` learned the hard way that a pass must never fence while it probes:
  fencing a replica of an ACTIVE segment the pass then declines to seal closes a copy nothing can
  reopen, and at `replication_factor: 2` that wedges the range permanently.

  That argument is about active segments and does not carry over. Here the control plane has already
  sealed the segment, the length is immutable, no producer will ever write to it again, and there is no
  decision left for the pass to decline. Fencing every copy costs nothing it could regret, and it is
  what lets the next pass skip a copy it has already settled: the marker is the memory.

  Repair still works on a fenced copy. `ReplicationServer.follow/4`, which `Malachi.Cluster.Catchup`
  uses to backfill a short copy, deliberately carries no fence check, so settling a copy that is short
  of the recorded length does not stand in the way of the pass that fills it.
  """

  alias Malachi.Metadata

  @typedoc """
  One copy to settle: which replica, which segment, and the offsets that bound it. `end_offset` is the
  length the control plane recorded, which is what the replica is brought down to and fenced at.
  """
  @type action ::
          {Metadata.broker(), Metadata.segment_id(), start_offset :: non_neg_integer(), end_offset :: non_neg_integer()}

  @doc """
  The `Malachi.Cluster.ReplicationServer.seal_at/5` calls for the copies in `unsettled`, at most
  `batch_size` of them, sorted so a pass is deterministic.

  `unsettled` is the `{segment_id, replica}` pairs whose stored bytes do not match their segment's
  recorded `byte_size` from above, which `Malachi.Cluster.SelfHealing`'s integrity probe already
  measures for every sealed segment on every pass. That probe is the whole reason this policy asks for
  nothing of its own: a copy that has been settled is byte-exact, so it stops appearing here, and the
  question costs the pass no call it was not already making.

  Bounded because the FIRST pass after this ships finds every sealed copy in the cluster unsettled at
  once. Each action opens a log, verifies its records and fsyncs, so an unbounded pass would put that
  on the disk the produce path is using. The bound costs only time: the pass is level-triggered, so
  what it leaves is simply picked up next tick.

  A segment that is no longer sealed in `metadata` is skipped rather than acted on: retention can drop
  a segment between the probe and this call, and a length that is not recorded is not a length to bring
  a copy down to.
  """
  @spec plan(Metadata.t(), [{Metadata.segment_id(), Metadata.broker()}], pos_integer()) :: [action()]
  def plan(%Metadata{} = metadata, unsettled, batch_size)
      when is_list(unsettled) and is_integer(batch_size) and batch_size > 0 do
    unsettled
    |> Enum.sort()
    |> Enum.uniq()
    |> Enum.flat_map(&settle(&1, metadata))
    |> Enum.take(batch_size)
  end

  defp settle({segment_id, replica}, metadata) do
    case Map.get(metadata.segments, segment_id) do
      %{state: :sealed, start_offset: start_offset, length: length} when is_integer(length) ->
        [{replica, segment_id, start_offset, start_offset + length}]

      _active_or_gone ->
        []
    end
  end
end
