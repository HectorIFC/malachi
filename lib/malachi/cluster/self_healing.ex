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
  """

  alias Malachi.Cluster.Catchup
  alias Malachi.Cluster.Placement
  alias Malachi.Metadata

  @type result :: %{
          applied: [Metadata.command()],
          failed: [{Metadata.segment_id(), term()}]
        }

  @doc """
  Backfills and heals every under-replicated **sealed** segment. Returns `%{applied: commands,
  failed: [{segment_id, reason}]}` — `applied` are `:set_segment_replicas` commands to apply
  through the control plane; `failed` segments could not be backfilled (e.g. `:no_live_source`).

  ## Options
    * `:batch_size` - forwarded to `Malachi.Cluster.Catchup.run/6`.
    * `:spread` - `{attribute_key, attributes}` forwarded to `Placement.place/4` so re-replication stays
      rack/DC-aware. Best-effort only: any `:min_domains`/`:policy` is intentionally *not* forwarded —
      healing prioritises durability and never fails a re-replication for domain diversity.
  """
  @spec heal_sealed(Metadata.t(), [Metadata.broker()], pos_integer(), keyword()) :: result()
  def heal_sealed(%Metadata{} = metadata, live_brokers, replication_factor, opts \\ []) do
    metadata
    |> Placement.under_replicated(live_brokers, replication_factor)
    |> Enum.map(&Metadata.get_segment(metadata, &1))
    |> Enum.filter(&(&1.state == :sealed))
    |> Enum.reduce(%{applied: [], failed: []}, fn segment, acc ->
      heal_segment(segment, live_brokers, replication_factor, opts, acc)
    end)
    |> finalize()
  end

  defp heal_segment(segment, live_brokers, replication_factor, opts, acc) do
    # Only :spread is forwarded — heal is durability-first and must never fail on a domain guarantee, so
    # :min_domains/:policy are deliberately stripped (place then always returns {:ok, _}).
    {:ok, new_set} = Placement.place(segment.id, live_brokers, replication_factor, Keyword.take(opts, [:spread]))
    to_add = new_set -- segment.replica_set
    sources = Enum.filter(segment.replica_set, &(&1 in live_brokers))

    cond do
      # The healed set drops/reorders replicas but adds none, so no data has to move.
      to_add == [] -> record_applied(acc, segment.id, new_set)
      sources == [] -> record_failed(acc, segment.id, :no_live_source)
      true -> backfill_and_record(acc, segment, new_set, to_add, hd(sources), opts)
    end
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

  defp record_applied(acc, segment_id, replica_set) do
    %{acc | applied: [{:set_segment_replicas, segment_id, replica_set} | acc.applied]}
  end

  defp record_failed(acc, segment_id, reason) do
    %{acc | failed: [{segment_id, reason} | acc.failed]}
  end

  defp finalize(acc), do: %{applied: Enum.reverse(acc.applied), failed: Enum.reverse(acc.failed)}
end
