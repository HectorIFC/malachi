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
  segment already assigned is never handed out again. The freshly sealed segment is then picked up by
  `Malachi.Cluster.SelfHealing`, which already re-replicates sealed segments.

  The fence is what makes the seal binding rather than advisory, and `Malachi.Cluster.HealCoordinator`
  applies it in two steps, in this order. It first MEASURES every live replica with
  `Malachi.Cluster.ReplicationServer.durable_stats/4`, which reports what a replica holds and leaves it
  writable. Only if those answers reach a majority does it FENCE them with
  `Malachi.Cluster.ReplicationServer.seal/4`, and the fence answers are what `plan/4` then seals on, so
  the point is recorded after every replica behind it has stopped accepting writes. A returning old
  primary is refused an append by each fenced replica it reaches, including by its own store after a
  restart, and so cannot close a quorum.

  Measuring before fencing is not a nicety. A fence has no inverse: nothing unseals a replica's store.
  Fencing whatever answers, before knowing whether a majority did, therefore closes replicas of a
  segment this pass may then decline to seal, and they keep refusing writes after their primary comes
  back. With `replication_factor: 2` that is terminal: one live follower is never a majority, so nothing
  is sealed, and once the primary returns the segment is no longer a candidate, so no later pass ever
  finishes while every produce fails quorum against a replica nothing can reopen. Below a majority the
  pass therefore leaves every replica untouched, and the range stays blocked until one returns.

  Note what this still does not claim: a replica the pass never reached is not fenced.

  ## The seal point, and when a range is left blocked

  The seal is placed at the **highest durable end** any replica reports, and only when a **majority** of
  the replica set answered the probe. Both halves are forced, not chosen.

  The argument, per record rather than per replica, because the difference matters: take an
  acknowledged record at offset `o`. It lives on a majority of the full replica set, the answering
  replicas are themselves a majority, and two majorities of the same set always intersect, so **some**
  answering replica holds `o`. A replica's log is contiguous (replication rejects a gap), so that
  replica's reported end is above `o`, and the highest end among the answers is at least that. The
  covering replica may be a **different one for each record**; no single answer need hold the whole
  segment, which is exactly why the seal takes the maximum instead of trusting one replica's view.

  Any lower point, including the offset a majority of the ANSWERS agree on, can sit below an
  acknowledged record that only the dead primary and one survivor ever held, and sealing there would
  discard it: exactly what this policy exists to prevent. Below a majority answering, no such
  intersection is guaranteed, the committed end is unknowable, and the segment is left alone with its
  range no longer accepting writes.

  That block is not a latch. The caller (`Malachi.Cluster.HealCoordinator`) is a periodic
  level-triggered loop, so the next pass re-evaluates: as soon as a majority answers again, the seal is
  emitted and writing rolls to a new segment on its own. If a majority never returns, the range stays
  blocked, which is the CP choice, and recovering it is a deliberate operator decision rather than
  something this policy takes on the operator's behalf.

  The probing itself lives in the caller: `candidates/3` names the segments to probe, and `plan/5`
  turns the probe results into commands, so the policy stays pure and testable without processes.

  ## A copy that failed

  A replica does not have to die for its copy to be lost to a segment. When a storage operation fails,
  `Malachi.Cluster.ReplicationServer` takes that copy out of service for good (it never writes it again,
  and answers every request for it with an error), and the segment can no longer count on it. NorthGuard
  treats that exactly like a failed broker, "we just seal it, make a new one, move the producers over",
  and so does this policy: a segment with a FAILED copy is a candidate too, whether the copy is the
  primary's or a follower's, with the caller learning which copies failed from
  `Malachi.Cluster.ReplicationServer.failed_segments/3` (`failure_probes/2` says whom to ask).

  The argument above carries over unchanged, because a failed copy is removed from the answers just as a
  dead primary is: it is never probed, never fenced, and never becomes the head. The consequence carries
  over too: without the failed copy there must still be a majority, so at `replication_factor: 3` one
  failed copy seals the segment on the other two, while at 2 and 1 the range stays blocked until the
  failed server restarts.
  """

  alias Malachi.Metadata

  @typedoc "What one replica durably holds for a segment: its end offset and byte size."
  @type probe :: {non_neg_integer(), non_neg_integer()}

  @typedoc "Probe results per segment: `%{segment_id => %{replica_ref => probe}}`."
  @type probes :: %{optional(term()) => %{optional(term()) => probe()}}

  @typedoc "Copies that failed in storage, as `{segment_id, replica}` pairs."
  @type failed :: MapSet.t()

  @doc """
  The active segments whose primary is dead or that have a failed copy, each with the live, non-failed
  replicas worth probing. The caller probes these and feeds the results to `plan/5`. Sorted, so a pass is
  deterministic.
  """
  @spec candidates(Metadata.t(), [Metadata.broker()], failed()) :: [{term(), [Metadata.broker()]}]
  def candidates(%Metadata{} = metadata, live_brokers, failed \\ MapSet.new()) do
    live = MapSet.new(live_brokers)

    metadata.segments
    |> Map.values()
    |> Enum.filter(&(active_primary_dead?(&1, live) or active_copy_failed?(&1, failed)))
    |> Enum.map(&{&1.id, probeable_replicas(&1, live, failed)})
    |> Enum.reject(fn {_id, replicas} -> replicas == [] end)
    |> Enum.sort()
  end

  @doc """
  The seal commands for the probed segments, at `now_ms` (passed in so every replica applies the same
  timestamp, as the produce path already does for its own seals). A segment whose probes do not reach a
  majority of its replica set is skipped: see the moduledoc on why that leaves the range blocked rather
  than risking acknowledged data. Returns a sorted (deterministic) list.
  """
  @spec plan(Metadata.t(), [Metadata.broker()], probes(), integer(), failed()) :: [Metadata.command()]
  def plan(%Metadata{} = metadata, live_brokers, probes, now_ms, failed \\ MapSet.new()) do
    # `candidates/3` is already sorted, and the per-segment pair must stay in the order it is built
    # (seal, then head), so the list is not re-sorted here.
    metadata
    |> candidates(live_brokers, failed)
    |> Enum.flat_map(&seal(&1, metadata, probes, now_ms, failed))
  end

  @doc """
  Whom to ask which copies failed: every live replica of a segment, active or sealed, with the segments it
  holds, as `[{replica, [segment_id]}]`, sorted. One entry per replica so the caller makes one batched call
  per broker, whatever the number of segments.

  Sealed segments are asked about too, though they are never a failover candidate: a sealed copy that
  failed takes no seal, but it is a lost replica the metadata still lists, which
  `Malachi.Cluster.SelfHealing` replaces on another broker.
  """
  @spec failure_probes(Metadata.t(), [Metadata.broker()]) :: [{Metadata.broker(), [term()]}]
  def failure_probes(%Metadata{} = metadata, live_brokers) do
    live = MapSet.new(live_brokers)

    metadata.segments
    |> Map.values()
    |> Enum.filter(&(&1.state in [:active, :sealed]))
    |> Enum.flat_map(fn segment -> for replica <- segment.replica_set, replica in live, do: {replica, segment.id} end)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.map(fn {replica, segment_ids} -> {replica, Enum.sort(segment_ids)} end)
    |> Enum.sort()
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

  defp active_copy_failed?(%{state: :active, id: id, replica_set: replica_set}, failed) do
    Enum.any?(replica_set, &MapSet.member?(failed, {id, &1}))
  end

  defp active_copy_failed?(_segment, _failed), do: false

  defp probeable_replicas(segment, live, failed) do
    Enum.filter(segment.replica_set, &(MapSet.member?(live, &1) and not MapSet.member?(failed, {segment.id, &1})))
  end

  defp seal({segment_id, _live_replicas}, metadata, probes, now_ms, failed) do
    segment = Map.fetch!(metadata.segments, segment_id)

    # Filtered here as well as in `candidates/3`, because the probes come from the caller: an answer that
    # arrived from a copy known to have failed must neither count toward the majority nor become the head.
    answers =
      probes
      |> Map.get(segment_id, %{})
      |> Map.reject(fn {replica, _probe} -> MapSet.member?(failed, {segment_id, replica}) end)

    if majority?(map_size(answers), segment.replica_set) do
      # The furthest end reported, not the one a majority of the ANSWERS agree on. Taking the
      # majority-th largest looks like Raft's commit index but is wrong here: the dead primary does not
      # answer, so a record it acknowledged together with a single survivor sits above that point and
      # would be sealed away. The intersection argument in the moduledoc is what makes the maximum both
      # safe and the lowest safe choice.
      {holder, {end_offset, byte_size}} =
        Enum.max_by(answers, fn {_replica, {offset, _bytes}} -> offset end)

      [
        Metadata.seal_command(segment, end_offset, byte_size, now_ms),
        # Reads route to the head of the replica set, and the head here is the broker that just died,
        # so the sealed segment would answer `:unreachable` until re-replication got to it. Moving a
        # replica that holds everything the seal promised to the head restores reads at once.
        # Reordering is what was unsafe on an ACTIVE segment (the new head would reissue offsets); on a
        # sealed one no append is possible at all, so it is only a routing change.
        {:set_segment_replicas, segment_id, [holder | List.delete(segment.replica_set, holder)]}
      ]
    else
      []
    end
  end
end
