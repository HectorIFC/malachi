defmodule Malachi.Telemetry do
  @moduledoc """
  The telemetry events Malachi emits on its hot paths. Attach a handler with `:telemetry.attach/4` (or
  `attach_many/4`) to feed metrics, logs, or traces: Malachi ships a default handler that folds a few of
  these into the ETS `Malachi.Metrics` (which the Prometheus endpoint exposes).

  Events, `event name` - `measurements` / `metadata`:

    * `[:malachi, :produce]`: `%{count, bytes}` / `%{topic}` - records appended to a topic.
    * `[:malachi, :consume]`: `%{count}` / `%{topic}` - records read from a topic.
    * `[:malachi, :auth]`. `%{count: 1}` / `%{result: :ok | :error}` - an authentication attempt.
    * `[:malachi, :replication, :commit]`. `%{count}` / `%{result: :ok | :no_quorum}` - a quorum
      replication of a batch (`count` = records in the batch; `result` is whether a quorum stored it).
    * `[:malachi, :storage, :flush]`. `%{duration_us, bytes, records}` / `%{segment, directory}` - one
      group-commit flush reached the disk: the buffered frames were written and the segment was synced.
      That write plus sync is the durability barrier every acknowledged produce waits behind, so
      `duration_us` is the latency that bounds produce throughput on an fsync-bound disk. Only a flush
      that wrote something and succeeded emits, so the event count is the number of syncs the data
      plane paid (a failed one is `[:malachi, :storage, :failure]`). The metadata names the segment
      that paid it, so one slow range can be told apart from a node-wide slowdown; it is deliberately
      not folded into the exported metric, where it would be unbounded label cardinality.
    * `[:malachi, :storage, :integrity]`. `%{position, unreadable_bytes}` /
      `%{result, sealed, source, segment}` - a stored segment failed verification. `result` is what
      the verification found, and the list is open rather than closed, so a consumer should have a
      fallback branch: today it is `:bad_crc` or `:bad_magic` (a frame that does not decode),
      `:incomplete` (a frame cut short), `:short_copy` (every frame decodes, but there are fewer
      records or bytes than the control plane recorded at seal time), `:bad_index` (the sparse index
      sidecar does not describe the segment) or a POSIX reason when the device itself could not be
      read. `position` is the byte where the damage starts, `sealed` whether the segment was
      immutable (damage there is corruption at rest, not a crash mid-write), and `source` where the
      verdict came from (`:recover` when a segment was opened, `:scrub` from the background pass).
    * `[:malachi, :storage, :failure]`. `%{count: 1}` / `%{segment, reason}` - a storage operation on a
      segment's copy failed on this node (`reason` is the POSIX reason, `:enospc` for a full volume),
      so `Malachi.Cluster.ReplicationServer` stopped using that copy and the heal pass will seal the
      segment on its other replicas. A failed WRITE, unlike `:integrity`, which is damage found by
      reading. Alert on any: a volume that fills or a device that fails shows up here first.
    * `[:malachi, :cluster, :orphaned_fence]`. `%{count: 1}` / `%{segment, reason}` - a segment's store
      was fenced but the control-plane seal that had to follow it FAILED, so the segment is closed to
      writes while the metadata still calls it active and its range accepts nothing until a heal pass
      reconciles the two. Alert on this rising while `[:malachi, :cluster, :fence_reconciled]` stays
      flat: that pair is the difference between a divergence that healed and a range that is stuck.
    * `[:malachi, :cluster, :fence_reconciled]`. `%{count}` / `%{}` - a heal pass finished the seal for
      `count` such segments, which is what unblocks their ranges.
    * `[:malachi, :storage, :scrub]`. `%{verified, damaged, repaired, unrepairable}` / `%{}` - one
      background verification pass finished, with how many segments it covered. Steady progress
      with `damaged: 0` is what a healthy node looks like; no events at all means the scrub is not
      running.

    * `[:malachi, :retention, :skip]`. `%{count: 1, offsets}` / `%{topic, group, range, source_range,
      origin, span}` - a consumer was moved past data that is no longer stored (expired by retention,
      or removed by an operator), once per distinct skip (a re-read before the commit is not counted
      again). `group` is `nil` for a fetch outside a group. `origin` is `:start` for a reader that had
      no position (a new group, or a child range after a split) and `:cursor` for one that resumed from
      a position, which is the reader that fell behind retention. `span` says how far `offsets` can be
      trusted: `:exact`, `:upper_bound` (the data was an ancestor's, of which this range would only have
      received its key slice) or `:unknown` (the ancestor's end was not recovered after a restart;
      `offsets` is then 0). See `Malachi.Broker.Skip`.

    * `[:malachi, :retention, :expire]`. `%{count: 1, bytes}` / `%{topic, segment, result}` - the
      retention sweep tried to expire one sealed segment of `topic` (`bytes` is its size). `result` is
      `Malachi.Cluster.Retention.reply_label/1` of what the control plane answered: `:ok` (expired),
      `:no_such_segment` (already gone), `:migrating`, `:segment_active` or `:other`. Only `:ok` freed
      the bytes. The segment is metadata for a handler, not a label of the exported metric.
    * `[:malachi, :retention, :sweep]`. `%{duration_us, expired, failed}` / `%{}` - one retention sweep
      ran on this node (only the leader sweeps), with how many segments it expired and how many deletes
      were refused. No events at all means no sweep is running.

  Reserved for later retention work, not emitted yet, so that the names are chosen once:

    * `[:malachi, :retention, :orphan_removed]` - replica directories the orphan sweeper reclaimed.
    * `[:malachi, :retention, :pinned]` - segments a consumer group keeps from expiring.
    * `[:malachi, :storage, :roll]` with `reason: :size | :time` - why a segment was rolled.

  Emitting is a no-op fast path when nothing is attached, so these are safe on the hot path.
  """

  alias Malachi.Broker.Skip

  @doc "Records appended to `topic` (`count` records, `bytes` total value bytes)."
  @spec produce(String.t(), non_neg_integer(), non_neg_integer()) :: :ok
  def produce(topic, count, bytes) do
    :telemetry.execute([:malachi, :produce], %{count: count, bytes: bytes}, %{topic: topic})
  end

  @doc "Records read from `topic` (`count` records)."
  @spec consume(String.t(), non_neg_integer()) :: :ok
  def consume(topic, count) do
    :telemetry.execute([:malachi, :consume], %{count: count}, %{topic: topic})
  end

  @doc "An authentication attempt with its `result` (`:ok` or `:error`)."
  @spec auth(:ok | :error) :: :ok
  def auth(result) do
    :telemetry.execute([:malachi, :auth], %{count: 1}, %{result: result})
  end

  @doc "A quorum replication of a batch of `count` records with its `result`."
  @spec replication_commit(non_neg_integer(), :ok | :no_quorum) :: :ok
  def replication_commit(count, result) do
    :telemetry.execute([:malachi, :replication, :commit], %{count: count}, %{result: result})
  end

  @doc """
  One group-commit flush was made durable: the write plus sync took `duration_us`, over `records` records
  and `bytes` encoded bytes, on the segment `segment_id` in `directory`.
  """
  @spec storage_flush(non_neg_integer(), non_neg_integer(), non_neg_integer(), term(), Path.t()) :: :ok
  def storage_flush(duration_us, bytes, records, segment_id, directory) do
    :telemetry.execute(
      [:malachi, :storage, :flush],
      %{duration_us: duration_us, bytes: bytes, records: records},
      %{segment: segment_id, directory: directory}
    )
  end

  @doc """
  A stored segment failed verification: `verdict` is the storage layer's finding (`:reason`,
  `:position`, `:unreadable_bytes`, `:sealed?`), `segment_id` names the segment and `source` says
  whether it surfaced while opening the segment (`:recover`) or during the background scrub.
  """
  @spec storage_integrity(map(), term(), :recover | :scrub) :: :ok
  def storage_integrity(verdict, segment_id, source) do
    :telemetry.execute(
      [:malachi, :storage, :integrity],
      %{position: verdict.position, unreadable_bytes: verdict.unreadable_bytes},
      %{result: verdict.reason, sealed: verdict.sealed?, source: source, segment: segment_id}
    )
  end

  @doc """
  A storage operation on `segment_id`'s copy failed on this node with `reason` (the POSIX reason), and
  the copy was taken out of service.
  """
  @spec storage_failure(term(), term()) :: :ok
  def storage_failure(segment_id, reason) do
    :telemetry.execute([:malachi, :storage, :failure], %{count: 1}, %{segment: segment_id, reason: reason})
  end

  @doc """
  A segment's store was fenced but recording the seal in the control plane failed, leaving the segment
  closed to writes while the metadata still calls it active. `reason` is what the command answered.
  """
  @spec orphaned_fence(term(), term()) :: :ok
  def orphaned_fence(segment_id, reason) do
    :telemetry.execute([:malachi, :cluster, :orphaned_fence], %{count: 1}, %{segment: segment_id, reason: reason})
  end

  @doc "A heal pass recorded the seal for `count` segments whose store was already fenced."
  @spec fence_reconciled(non_neg_integer()) :: :ok
  def fence_reconciled(count) do
    :telemetry.execute([:malachi, :cluster, :fence_reconciled], %{count: count}, %{})
  end

  @doc """
  `group` (a consumer group, or `nil` outside one) was moved past the data `skip` describes on `topic`.
  """
  @spec retention_skip(String.t(), String.t() | nil, Skip.t()) :: :ok
  def retention_skip(topic, group, %Skip{} = skip) do
    offsets = if skip.offsets == :unknown, do: 0, else: skip.offsets

    :telemetry.execute([:malachi, :retention, :skip], %{count: 1, offsets: offsets}, %{
      topic: topic,
      group: group,
      range: skip.range_id,
      source_range: skip.source_range_id,
      origin: skip.origin,
      span: Skip.span(skip)
    })
  end

  @doc """
  The retention sweep tried to expire `segment_id` of `topic`, `bytes` long, and the control plane's answer
  was labelled `result`.
  """
  @spec retention_expire(String.t(), term(), non_neg_integer(), atom()) :: :ok
  def retention_expire(topic, segment_id, bytes, result) do
    :telemetry.execute([:malachi, :retention, :expire], %{count: 1, bytes: bytes}, %{
      topic: topic,
      segment: segment_id,
      result: result
    })
  end

  @doc "One retention sweep ran for `duration_us`, expiring `expired` segments with `failed` refusals."
  @spec retention_sweep(non_neg_integer(), non_neg_integer(), non_neg_integer()) :: :ok
  def retention_sweep(duration_us, expired, failed) do
    :telemetry.execute(
      [:malachi, :retention, :sweep],
      %{duration_us: duration_us, expired: expired, failed: failed},
      %{}
    )
  end

  @doc "One background scrub pass finished, with the segments it verified, found damaged and repaired."
  @spec scrub_pass(non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()) :: :ok
  def scrub_pass(verified, damaged, repaired, unrepairable) do
    :telemetry.execute(
      [:malachi, :storage, :scrub],
      %{verified: verified, damaged: damaged, repaired: repaired, unrepairable: unrepairable},
      %{}
    )
  end
end
