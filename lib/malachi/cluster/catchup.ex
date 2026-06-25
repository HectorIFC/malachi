defmodule Malachi.Cluster.Catchup do
  @moduledoc """
  Copies a segment's records from a **source** replica to a **target** replica, offset range
  `[from_offset, to_offset)`. This is the primitive behind two recoveries:

    * a follower that fell behind on the active segment catching the gap back up, and
    * a freshly placed replica (from `Malachi.Cluster.Placement.heal/3`) **backfilling** an entire
      sealed segment so it can carry its share of the replica set.

  It runs as **external orchestration** — in the caller's process, issuing ordinary
  `Malachi.Cluster.ReplicationServer` calls to both servers (`read/4` on the source, `follow/4`
  on the target). Nothing runs inside a server's call while that server is being waited on, so it
  cannot deadlock the primary↔follower path the way an in-line pull would.

  `from_offset` must be the target's current end for the segment (or the segment's base when the
  target holds none of it yet), so the appended records line up; `Malachi.Cluster.ReplicationServer.end_offset/2`
  gives that for an existing replica. The source must hold the records being copied; if it is
  itself behind and runs out before `to_offset`, the copy stops at what the source has and returns
  the offset reached — the caller can check it against the target and pick another source.
  """

  alias Malachi.Cluster.ReplicationServer

  @default_batch_size 1000

  @doc """
  Copies `segment_id` from `source` to `target` over `[from_offset, to_offset)`. Returns
  `{:ok, reached_offset}` — the target's new end, which equals `to_offset` on full catch-up or
  less if the source ran out first — or `{:error, {:source | :target, reason}}`.

  ## Options
    * `:batch_size` - records copied per round trip (default 1000).
  """
  @spec run(term(), term(), term(), non_neg_integer(), non_neg_integer(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, {:source | :target, term()}}
  def run(target, source, segment_id, from_offset, to_offset, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)
    copy(target, source, segment_id, from_offset, to_offset, batch_size)
  end

  defp copy(_target, _source, _segment_id, offset, to_offset, _batch_size) when offset >= to_offset do
    {:ok, offset}
  end

  defp copy(target, source, segment_id, offset, to_offset, batch_size) do
    limit = min(batch_size, to_offset - offset)

    case ReplicationServer.read(source, segment_id, offset, limit) do
      :eof ->
        {:ok, offset}

      {:error, reason} ->
        {:error, {:source, reason}}

      {:ok, records} ->
        case ReplicationServer.follow(target, segment_id, offset, records) do
          {:ok, last} -> copy(target, source, segment_id, last + 1, to_offset, batch_size)
          {:error, reason} -> {:error, {:target, reason}}
        end
    end
  end
end
