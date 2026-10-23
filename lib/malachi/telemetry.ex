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
    * `[:malachi, :cluster, :sealed_copies_settled]`. `%{copies, records}` / `%{result}` - a heal pass
      settled `copies` copies of sealed segments against the length the control plane recorded. `result:
      :fenced` is the routine case and only writes a marker; `result: :trimmed` gave up `records`
      records a copy held past its segment's sealed end, which is a defect that reached disk, so alert
      on it outside the first pass after an upgrade; `result: :failed` is a copy still divergent from
      what the control plane promises; `result: :recorded_size_below` is a copy holding exactly the
      records its seal names that measures above the recorded `byte_size`, so the recorded size may be the
      low one. A sealed size is immutable and no pass can clear it, so the copy is reported again on later
      passes, and a full batch of such copies leaves others waiting (a known gap of issue #248).
    * `[:malachi, :cluster, :vnode_reconcile]`. `%{ring, hosted, leading, started, stopped}` /
      `%{placement: :ok | :unreadable}` - one reconcile pass of `Malachi.Cluster.VnodeCoordinatorManager`
      on a sharded control plane. `ring` is how many vnodes the live ring names, `hosted` how many of
      them this node holds an `ra` member of, and `leading` how many of those it leads and therefore
      runs coordinators for; `started` and `stopped` are what this pass changed. `placement: :unreadable`
      means the pass could not read the placement and deliberately changed nothing: `ring`, `started` and
      `stopped` are then 0, while `hosted` and `leading` describe the set still running from the last
      good read. `started` or `stopped` staying non-zero pass after pass is a placement or a leadership
      that flaps; `hosted` above `leading` is normal (a follower hosts without coordinating).
    * `[:malachi, :storage, :scrub]`. `%{verified, damaged, repaired, unrepairable}` / `%{}` - one
      background verification pass finished, with how many segments it covered. Steady progress
      with `damaged: 0` is what a healthy node looks like; no events at all means the scrub is not
      running.
    * `[:malachi, :process, :unexpected_message]`. `%{count: 1}` / `%{server, kind, shape, pid}` - a
      long-lived server received a message it has no clause for and dropped it (`kind` is `:cast` or
      `:info`) or answered it `{:error, :unknown_call}` (`kind` is `:call`) instead of crashing. See
      `Malachi.UnexpectedMessage`: `shape` identifies the sender without carrying the message's data, and
      `pid` is the server that received it. Non-zero on a cluster where every node runs the same build
      means a bug; during a rolling upgrade it means a newer node is sending a shape an older one does
      not know.

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

    * `[:malachi, :retention, :orphan_left]`. `%{count: 1}` / `%{topic}` - an expire deleted a segment
      from the control plane and one of its replicas did not answer the delete, so that replica kept a
      directory no later sweep can name. `Malachi.Retention.OrphanSweeper` is what reclaims it.

    * `[:malachi, :retention, :orphan_removed]`. `%{count}` / `%{}` - replica directories
      `Malachi.Retention.OrphanSweeper` reclaimed in one pass.
    * `[:malachi, :retention, :unresolved_policy]`. `%{count: 1}` / `%{topic}` - a sweep found `topic`
      bound to a policy name it could not resolve, and expired nothing of it. Once per sweep per topic,
      so it is a gauge of the misconfiguration rather than a count of anything.

  Reserved for later retention work, not emitted yet, so that the names are chosen once:

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
  A heal pass settled `copies` copies of sealed segments against the length the control plane recorded:
  `:fenced` wrote only a marker, `:trimmed` also gave up `records` records held past the sealed end,
  `:recorded_size_below` is a copy holding exactly the records its seal names that measures above the
  recorded `byte_size`, and `:failed` means the copy was not settled: either the seal did not complete, or
  a copy holding fewer records than its seal names was fenced at its short length and reported as
  `:failed`. A storage error latches the copy as failed in this process, and a heal pass replaces a latched
  copy when a spare broker is available. Neither a copy whose directory cannot be listed nor one that
  cannot be reached is latched. A copy that does not answer the byte-count probe is skipped by that pass
  and left to the next heal pass, which settles by default at most 64 copies longer than their seals per
  pass once the node's replication server answers; a copy past that cap waits for a later pass, and copies
  that never settle can keep their slots (a known gap of issue #248). A copy missing records whose bytes are
  below the recorded size is attempted by the heal pass when its segment has at least the replicas
  achievable (the lower of the replication factor and the live brokers), every replica is live, and an
  intact copy exists to copy from. A copy missing records whose bytes are not below the recorded size is
  not repaired by this pass; when its bytes are above it, the copy is settled at its short length and
  reported as `:failed`. A latched copy is never probed or used as a source; with a spare broker it is
  replaced, and without one it stays behind the healthy replicas. A failed attempt that returns an error is
  reported as a failure; an attempt whose copy cannot be reached is skipped without a report and tried again
  on a later pass. A copy whose intact sources do not answer is reported as having no intact copy and is
  tried again on a later pass. A copy with all records present and bytes short is listed among the heal
  pass's failures, not counted under this event's failed result, and the scrubber finds it on its own cycle,
  which verifies local copies; its repair is deferred while that server does not answer, except an index
  rebuild, which is not deferred. One that fails between the probe and the settle is counted as `:failed`,
  and a settle that times out is counted the same way even if the server trims records afterwards, so those
  records are not counted as `:trimmed`. A directory that cannot be listed reads as lost and goes to the
  repair path. Without a replacement, a copy trimmed but unfenced stays so after a restart, unless its bytes
  are still above the recorded size, in which case the next pass selects it again.
  `records` is 0 for anything but `:trimmed`.
  """
  @spec sealed_copies_settled(
          :fenced | :trimmed | :failed | :recorded_size_below,
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok
  def sealed_copies_settled(result, copies, records) do
    :telemetry.execute(
      [:malachi, :cluster, :sealed_copies_settled],
      %{copies: copies, records: records},
      %{result: result}
    )
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
  was labeled `result`.
  """
  @spec retention_expire(String.t(), term(), non_neg_integer(), atom()) :: :ok
  def retention_expire(topic, segment_id, bytes, result) do
    :telemetry.execute([:malachi, :retention, :expire], %{count: 1, bytes: bytes}, %{
      topic: topic,
      segment: segment_id,
      result: result
    })
  end

  @doc """
  An expire removed `topic`'s segment from the control plane and a replica did not answer the delete,
  leaving a directory behind on it.
  """
  @spec retention_orphan_left(String.t()) :: :ok
  def retention_orphan_left(topic) do
    :telemetry.execute([:malachi, :retention, :orphan_left], %{count: 1}, %{topic: topic})
  end

  @doc """
  A retention sweep found `topic` bound to a policy name its definitions do not resolve, so nothing of
  that topic was expired.
  """
  @spec retention_unresolved_policy(String.t()) :: :ok
  def retention_unresolved_policy(topic) do
    :telemetry.execute([:malachi, :retention, :unresolved_policy], %{count: 1}, %{topic: topic})
  end

  @doc "The orphan sweeper reclaimed `count` replica directories in one pass."
  @spec retention_orphan_removed(non_neg_integer()) :: :ok
  def retention_orphan_removed(count) do
    :telemetry.execute([:malachi, :retention, :orphan_removed], %{count: count}, %{})
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

  @doc """
  The server labeled `server` received a message of `shape` as `kind` (`:cast`, `:info` or `:call`) with
  no clause for it, and dropped it rather than crash. Emitted from the server's own process.
  """
  @spec unexpected_message(atom(), :cast | :info | :call, term()) :: :ok
  def unexpected_message(server, kind, shape) do
    :telemetry.execute(
      [:malachi, :process, :unexpected_message],
      %{count: 1},
      %{server: server, kind: kind, shape: shape, pid: self()}
    )
  end

  @doc """
  The broker's control plane reconcile did not complete this tick, for `reason`:

    * `:skipped` - the previous tick's reconcile was still running, so this one did not start one;
    * `:down` - the reconcile task crashed;
    * `:timeout` - the reconcile task overran its deadline and was killed.

  All three leave the node serving from the view it already holds, which is correct but increasingly
  stale, so a rising total is the signal that the control plane is not answering. Emitted from the
  broker's own process.
  """
  @spec reconcile_degraded(:skipped | :down | :timeout) :: :ok
  def reconcile_degraded(reason) do
    :telemetry.execute([:malachi, :cluster, :reconcile_degraded], %{count: 1}, %{reason: reason})
  end

  @doc """
  One reconcile pass of the vnode coordinator manager: the vnodes the live ring named, how many this
  node hosts and leads, and how many coordinator trees the pass started and stopped. `placement` is
  `:unreadable` when the pass kept its set because the placement could not be read.
  """
  @spec vnode_reconcile(
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          :ok | :unreadable
        ) :: :ok
  def vnode_reconcile(ring, hosted, leading, started, stopped, placement) do
    :telemetry.execute(
      [:malachi, :cluster, :vnode_reconcile],
      %{ring: ring, hosted: hosted, leading: leading, started: started, stopped: stopped},
      %{placement: placement}
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
