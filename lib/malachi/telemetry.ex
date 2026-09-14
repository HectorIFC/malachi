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

  Emitting is a no-op fast path when nothing is attached, so these are safe on the hot path.
  """

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
