defmodule Malachi.Cluster.SelfHealing do
  @moduledoc """
  Closes the self-healing loop for **sealed** segments: the decision comes from
  `Malachi.Cluster.Placement` and the execution from `Malachi.Cluster.Catchup`.

  Given the current `Malachi.Metadata`, the set of **live** brokers, and the replication factor,
  `heal_sealed/4` finds the sealed segments that are under-replicated (a replica left), picks the
  healed replica set with `Placement.place/3`, and **backfills** each newly added replica with the
  segment's records (copied from a surviving replica via `Catchup.run/6`). It returns the
  `:set_segment_replicas` commands whose backfill succeeded, for the caller to apply through the
  control plane (`Malachi.Metadata` / the replicated DS-RSM), plus any segments it could not heal.

  Only **sealed** segments are handled here: their offset range `[start_offset, start_offset +
  length)` is fixed, so a backfill is a well-defined copy. The active segment grows as it is
  written, so a follower that falls behind on it rejoins via the write-path catch-up trigger (a
  separate slice) rather than a one-shot backfill.

  Brokers are `Malachi.Cluster.ReplicationServer` references. The live-broker set is supplied by
  the caller (membership is a separate concern); a segment whose every replica is dead cannot be
  backfilled and is reported as failed rather than silently dropped.

  Besides the metadata-level pass, `heal_sealed/4` also runs a **physical integrity pass** over
  sealed segments whose replica set looks healthy: a live node can lose a sealed copy without the
  metadata noticing (a deleted or truncated file, a swapped disk), leaving the cluster silently
  below its replication factor while every replica is "alive". The pass probes each live replica's
  stored bytes (`Malachi.Cluster.ReplicationServer.stored_bytes/3`, a read-only stat that never
  opens the log, so steady state costs no descriptors); only a replica whose bytes fall short of
  the sealed `byte_size` is then opened for its exact durable end
  (`Malachi.Cluster.ReplicationServer.durable_end/4`) and re-backfilled from an intact replica via
  `Malachi.Cluster.Catchup`, resuming at the truncation point rather than recopying the segment.
  In-place corruption that keeps the byte size is out of this probe's reach by construction, since it
  compares sizes: that is what `Malachi.Cluster.Scrubber` verifies checksums for.

  ## A copy that failed in storage

  A sealed copy its replication server has latched as failed (see "Storage failures" in
  `Malachi.Cluster.ReplicationServer`, and the `:failed` option) is a lost replica on a live broker: it
  refuses every request, so it is neither a backfill source nor a placement target for that segment, and
  the segment is healed as if that broker had left it. The healed set leaves the copy out, and the caller
  deletes it once the new set is recorded (`Malachi.Cluster.HealCoordinator`).

  A failed copy is REPLACED, never merely dropped. With no spare broker to take its place it stays in the
  set, moved behind the healthy replicas so reads avoid it, and the segment is reported as failed with
  `{:no_spare_broker, copies}` on every pass. Dropping it would give up the one chance it has left: the
  latch lives in memory, and after a restart a copy whose sealed bytes were intact serves again.
  """

  alias Malachi.Cluster.Catchup
  alias Malachi.Cluster.Placement
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Metadata

  # Bounds each integrity probe call, so one hung replica cannot stall the healing pass for the
  # default five seconds per segment it holds.
  @probe_timeout 2_000

  @type result :: %{
          applied: [Metadata.command()],
          failed: [{Metadata.segment_id(), term()}],
          repaired: [{Metadata.segment_id(), Metadata.broker()}]
        }

  @doc """
  Backfills and heals every under-replicated **sealed** segment. Returns `%{applied: commands,
  failed: [{segment_id, reason}]}`, `applied` are `:set_segment_replicas` commands to apply
  through the control plane; `failed` segments could not be backfilled (e.g. `:no_live_source`).

  ## Options
    * `:batch_size` - forwarded to `Malachi.Cluster.Catchup.run/6`.
    * `:spread` - `{attribute_key, attributes}` forwarded to `Placement.place/4` so re-replication stays
      rack/DC-aware. Best-effort only: any `:min_domains`/`:policy` is intentionally *not* forwarded:
      healing prioritises durability and never fails a re-replication for domain diversity.
    * `:failed` - a `MapSet` of `{segment_id, replica}` whose copy failed in storage there (default
      empty). See "A copy that failed in storage" above.
  """
  @spec heal_sealed(Metadata.t(), [Metadata.broker()], pos_integer(), keyword()) :: result()
  def heal_sealed(%Metadata{} = metadata, live_brokers, replication_factor, opts \\ []) do
    failed = Keyword.get(opts, :failed, MapSet.new())
    under_replicated = Placement.under_replicated(metadata, live_brokers, replication_factor)
    to_heal = Enum.sort(Enum.uniq(under_replicated ++ with_failed_copy(metadata, failed)))

    healed =
      to_heal
      |> Enum.map(&Metadata.get_segment(metadata, &1))
      |> Enum.filter(&(&1.state == :sealed))
      |> Enum.reduce(%{applied: [], failed: []}, fn segment, acc ->
        heal_segment(segment, live_brokers, replication_factor, opts, failed_copies(failed, segment), acc)
      end)

    integrity = repair_lost_copies(metadata, MapSet.new(to_heal), live_brokers, opts)

    finalize(%{
      applied: healed.applied,
      failed: healed.failed ++ integrity.failed,
      repaired: integrity.repaired
    })
  end

  # Sealed segments with a copy that failed on a broker still in their replica set.
  defp with_failed_copy(metadata, failed) do
    for {segment_id, replica} <- failed,
        segment = Map.get(metadata.segments, segment_id),
        segment != nil and segment.state == :sealed and replica in segment.replica_set,
        uniq: true,
        do: segment_id
  end

  defp failed_copies(failed, segment) do
    for {segment_id, replica} <- failed, segment_id == segment.id, replica in segment.replica_set, do: replica
  end

  # `lost` are this segment's failed copies: a broker holding one is live, but not for this segment.
  defp heal_segment(segment, live_brokers, replication_factor, opts, lost, acc) do
    available = live_brokers -- lost
    sources = Enum.filter(segment.replica_set, &(&1 in available))

    # Only :spread is forwarded: heal is durability-first and must never fail on a domain guarantee, so
    # :min_domains/:policy are deliberately stripped, which leaves an empty broker list as the only error.
    case Placement.place(segment.id, available, replication_factor, Keyword.take(opts, [:spread])) do
      {:ok, new_set} -> heal_to(acc, segment, new_set, sources, lost, opts)
      # Every live broker holds a failed copy of it, so there is no broker left to hold it.
      {:error, :no_brokers} -> record_failed(acc, segment.id, :no_live_source)
    end
  end

  defp heal_to(acc, segment, new_set, sources, lost, opts) do
    to_add = new_set -- segment.replica_set

    cond do
      # Nothing to put in a failed copy's place: it stays, behind the healthy replicas (see the moduledoc).
      to_add == [] and lost != [] -> keep_failed_copies(acc, segment, new_set, lost)
      # The healed set drops/reorders replicas but adds none, so no data has to move.
      to_add == [] -> record_applied(acc, segment.id, new_set)
      sources == [] -> record_failed(acc, segment.id, :no_live_source)
      true -> backfill_and_record(acc, segment, new_set, to_add, hd(sources), opts)
    end
  end

  defp keep_failed_copies(acc, segment, new_set, lost) do
    kept = new_set ++ lost
    acc = record_failed(acc, segment.id, {:no_spare_broker, lost})

    if kept == segment.replica_set, do: acc, else: record_applied(acc, segment.id, kept)
  end

  defp backfill_and_record(acc, segment, new_set, to_add, source, opts) do
    case backfill(to_add, source, segment, opts) do
      :ok -> record_applied(acc, segment.id, new_set)
      {:error, reason} -> record_failed(acc, segment.id, reason)
    end
  end

  defp backfill(to_add, source, segment, opts) do
    from = segment.start_offset
    to = segment.start_offset + segment.length

    Enum.reduce_while(to_add, :ok, fn replica, :ok ->
      case Catchup.run(replica, source, segment.id, from, to, opts) do
        {:ok, ^to} -> {:cont, :ok}
        {:ok, reached} -> {:halt, {:error, {:incomplete_source, reached}}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  # --- physical integrity pass (lost sealed copies on live replicas) ---

  # Probes every sealed segment the metadata considers healthy (fully live replica set, not already
  # being healed by the membership pass) and re-backfills any live replica whose stored bytes fall
  # short of the sealed byte_size. The replica set is unchanged, so no metadata command results;
  # repairs are reported for observability.
  defp repair_lost_copies(metadata, under_replicated, live_brokers, opts) do
    metadata.segments
    |> Map.values()
    |> Enum.filter(fn segment ->
      segment.state == :sealed and is_integer(segment.byte_size) and
        not MapSet.member?(under_replicated, segment.id) and
        Enum.all?(segment.replica_set, &(&1 in live_brokers))
    end)
    |> Enum.reduce(%{repaired: [], failed: []}, &probe_and_repair(&1, opts, &2))
  end

  defp probe_and_repair(segment, opts, acc) do
    probes = Enum.map(segment.replica_set, &{&1, probe_stored_bytes(&1, segment.id)})
    lost = for {replica, {:ok, bytes}} <- probes, bytes < segment.byte_size, do: replica
    sources = for {replica, {:ok, bytes}} <- probes, bytes >= segment.byte_size, do: replica

    cond do
      lost == [] -> acc
      sources == [] -> %{acc | failed: [{segment.id, :no_intact_copy} | acc.failed]}
      true -> Enum.reduce(lost, acc, &repair_copy(segment, &1, hd(sources), opts, &2))
    end
  end

  defp repair_copy(segment, replica, source, opts, acc) do
    expected_end = segment.start_offset + segment.length

    with {:ok, from} <- probe_durable_end(replica, segment.id, segment.start_offset),
         true <- from < expected_end,
         {:ok, ^expected_end} <- Catchup.run(replica, source, segment.id, from, expected_end, opts) do
      %{acc | repaired: [{segment.id, replica} | acc.repaired]}
    else
      # Offsets complete but bytes short: not a truncation Catchup can mend; leave it to the scrub,
      # which verifies checksums and repairs from an intact replica, rather than recopying blindly.
      false -> %{acc | failed: [{segment.id, {:integrity_suspect, replica}} | acc.failed]}
      {:ok, reached} -> %{acc | failed: [{segment.id, {:incomplete_source, reached}} | acc.failed]}
      {:error, reason} -> %{acc | failed: [{segment.id, reason} | acc.failed]}
      :unreachable -> acc
    end
  end

  # A replica that does not answer the probe is skipped, never "repaired" on unknown state: if it is
  # truly gone the membership pass owns it; if it is merely slow, the next pass probes again.
  defp probe_stored_bytes(replica, segment_id) do
    {:ok, ReplicationServer.stored_bytes(replica, segment_id, @probe_timeout)}
  catch
    :exit, _reason -> :unreachable
  end

  # A copy that failed in storage answers an error, and it is skipped like a silent replica: it will not
  # take a repair (it refuses every write), and the failover pass is what deals with it.
  defp probe_durable_end(replica, segment_id, base_offset) do
    case ReplicationServer.durable_end(replica, segment_id, base_offset, @probe_timeout) do
      {:error, _reason} -> :unreachable
      end_offset -> {:ok, end_offset}
    end
  catch
    :exit, _reason -> :unreachable
  end

  defp record_applied(acc, segment_id, replica_set) do
    %{acc | applied: [{:set_segment_replicas, segment_id, replica_set} | acc.applied]}
  end

  defp record_failed(acc, segment_id, reason) do
    %{acc | failed: [{segment_id, reason} | acc.failed]}
  end

  defp finalize(acc) do
    %{
      applied: Enum.reverse(acc.applied),
      failed: Enum.reverse(acc.failed),
      repaired: Enum.reverse(acc.repaired)
    }
  end
end
