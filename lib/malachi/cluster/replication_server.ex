defmodule Malachi.Cluster.ReplicationServer do
  @moduledoc """
  The transport for segment replication: a `GenServer`, one per broker, that ships an active
  segment's records from the **primary** to its **followers** and acknowledges a write once a
  **quorum** has durably stored it.

  A broker is identified by this server's process reference (a registered name locally, or a
  `{name, node}` tuple across nodes: `GenServer.call/3` accepts both, so the same code path runs
  in-process for tests and over distributed Erlang in production). A segment's `replica_set` (from
  `Malachi.Cluster.Placement`) is a list of those references; the first is the primary.

  On `replicate/5` the primary appends the batch durably to its local copy, then PUSHES it to the
  followers as pipelined replica-appends (the NorthGuard replication protocol): the caller is parked
  and the pushes go out as casts from the primary's own loop, up to `:replication_window` unacked
  batches per segment, so the loop never blocks waiting on a follower (a synchronous fan-out let
  primaries on different nodes block each other's loops in a circular wait). Casting from one fixed
  process gives per-follower FIFO, so the appends arrive in offset order with no extra coordination.
  Each push carries the segment's commit progress; each follower ack carries the follower's durable
  end offset and feeds a `Malachi.Cluster.ReplicaTracker`. A segment's log opens at the segment's
  `base_offset` (its first range-relative offset), so the offsets of a range's segments are
  contiguous rather than restarting at zero per segment. The parked call is replied `{:ok, last}`
  as soon as a quorum (the primary plus enough followers) has the batch durably, tolerating up to
  ⌊(N-1)/2⌋ slow or unreachable followers, or `{:error, :no_quorum}` when the quorum does not close
  within the follow timeout. Both the primary and the followers `fsync` before counting toward the
  quorum, so "committed" means "durable on a majority".

  Scope: the active segment's happy path with quorum tolerance, plus **automatic catch-up** of a
  follower that is behind: when the primary's fan-out reaches a follower whose end is below the
  batch's offset, the follower kicks off a background pull from the primary (`Malachi.Cluster.Catchup`)
  and rejoins the quorum on a later batch. This covers both a follower that missed some batches and
  a **brand-new replica** that joins an active segment: it opens at the segment's `base`, sees the
  gap, backfills, and converges on the moving head as later fan-outs re-trigger. Sealed-segment
  re-replication and primary failover live in their own modules (`Malachi.Cluster.SelfHealing` driven
  by the heal coordinator, and `Malachi.Cluster.Failover`), not here.

  ## Closing a segment

  `seal/4` is the write fence: it seals the segment's log durably and answers where it ended. Afterwards
  every write entry point here refuses that segment with `{:error, {:sealed, end_offset}}`, across
  restarts, while reads and repair (`follow/4`) keep working. That is what makes a control-plane sealed
  length a consequence of closing the segment rather than a number measured beside a log that is still
  growing.

  `fenced_segments/3` is the read-only counterpart: it reports which segments are ALREADY fenced,
  without fencing anything, so a reconciling pass can find a fence whose control-plane seal never
  landed and finish it.

  ## Storage failures

  This one process holds every log on the node, so a storage failure in one segment must never take
  the process down: that would take every OTHER segment with it, and a condition like a full volume does
  not clear between supervisor restarts. A failed storage operation is instead answered to its caller as
  `{:error, {:storage, reason}}` (the POSIX reason, `{:storage, :enospc}`), and the segment is FAILED on
  this server:

    * its log is closed and dropped, its parked batches are answered with the same error, and deferred
      follower acks for it are discarded, so nothing acknowledges records that never became durable;
    * every later request for it, writes and reads alike, is refused with that error WITHOUT touching
      the disk, until the segment is deleted here or the server restarts;
    * it is reported by `failed_segments/3`, which is how `Malachi.Cluster.HealCoordinator` finds it and
      seals the segment on its surviving replicas so producers move to a new one, as NorthGuard does
      when a replica fails.

  The copy is never retried. This store writes through the page cache (no Direct I/O), and after a
  failed fsync the cache can claim bytes are written that never reached the device, so a retry could
  report success over lost data.
  """

  use GenServer

  require Logger
  require OpenTelemetry.Tracer, as: Tracer

  alias Malachi.Cluster.Catchup
  alias Malachi.Cluster.ReplicaTracker
  alias Malachi.I18n
  alias Malachi.Log
  alias Malachi.Storage.Layout
  alias Malachi.Telemetry
  alias OpenTelemetry.Ctx

  @follow_timeout 5_000
  # Max unacked replica-append batches in flight per segment (the NorthGuard replication window; static
  # in this slice, a follower-advertised dynamic window is a later protocol evolution).
  @default_replication_window 32
  # Group-commit time trigger under replication (NorthGuard fsyncs every 10ms / 20k records / 10MB; the
  # count and size triggers already live in the store, this is the time one).
  @default_gc_interval_ms 10

  @typep reply_target :: {:call, GenServer.from()} | {:notify, pid(), term()}
  @typep batch :: %{
           ref: reference(),
           reply: reply_target(),
           last: non_neg_integer(),
           count: pos_integer(),
           timer: reference()
         }

  @typep state :: %{
           ref: term(),
           directory: Path.t(),
           log_opts: keyword(),
           logs: %{term() => Log.t()},
           trackers: %{term() => ReplicaTracker.t()},
           catching_up: MapSet.t(),
           catchup_monitors: %{reference() => term()},
           follow_timeout: pos_integer(),
           replication_window: pos_integer(),
           inflight: %{term() => [batch()]},
           pending: %{term() => :queue.queue()},
           committed: %{term() => non_neg_integer()},
           group_commit: boolean(),
           gc_interval: pos_integer(),
           gc_timer: reference() | nil,
           pending_acks: %{{term(), term()} => non_neg_integer()},
           failed: %{term() => term()},
           lost_unflushed: %{term() => term()}
         }

  @doc """
  Starts a replication server.

  ## Options
    * `:name` (optional) - the broker reference this server is registered under and known by in
      replica sets. When omitted, the server is unregistered and its reference is its pid.
    * `:directory` (required) - where replicated segment logs are stored.
    * `:follow_timeout` - ms a parked replicate waits for its quorum before `:no_quorum` (default 5000).
    * `:replication_window` - max unacked replica-append batches in flight per segment (default 32).
    * `:group_commit` - coalesce fsyncs under replication (NorthGuard: fsync on every replica by
      time/count/size triggers, before the produce ack). Default false (fsync per batch).
    * `:group_commit_interval_ms` - the time trigger for that coalescing (default 10).
    * any remaining options are forwarded to each segment's `Malachi.Log`.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    gen_server_opts =
      case Keyword.fetch(opts, :name) do
        {:ok, name} -> [name: name]
        :error -> []
      end

    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @doc """
  Replicates `records` for `segment_id` across `replica_set`, called on the primary (the first
  broker of the set). `base_offset` is the segment's first offset; it is used only when the
  segment's log is opened for the first time, so a segment's offsets continue its range
  (`start_offset, start_offset + 1, ...`) rather than restarting at zero.

  Returns `{:ok, last_offset}` once a quorum has the batch durably, `{:error, :no_quorum}` if too
  few replicas acked, `{:error, :not_primary}` if this server is not the set's primary,
  `{:error, :empty}` for an empty batch, `{:error, {:sealed, end_offset}}` for a fenced segment, or
  `{:error, {:storage, reason}}` when this server's copy failed (see "Storage failures").
  """
  @spec replicate(term(), term(), [term()], non_neg_integer(), [Malachi.Log.Record.t()]) ::
          {:ok, non_neg_integer()}
          | {:error,
             :no_quorum
             | :not_primary
             | :empty
             | :empty_replica_set
             | :unreachable
             | {:sealed, non_neg_integer()}
             | {:storage, term()}}
  def replicate(primary, segment_id, replica_set, base_offset, records) do
    # Carry the caller's trace context (the broker produce span, possibly on another node) so the quorum
    # replication becomes a child span: distributed tracing across the produce -> replication hop.
    GenServer.call(
      primary,
      {:replicate, segment_id, replica_set, base_offset, records, Ctx.get_current()},
      @follow_timeout + 10_000
    )
  catch
    # A dead/unreachable primary must not crash the caller; surface it as an error to handle.
    :exit, _reason -> {:error, :unreachable}
  end

  @doc """
  Fire-and-forget variant of `replicate/5` for a frontend that must never block its loop on
  replication (the NorthGuard end-to-end pipelined produce): same semantics and quorum rules, but the
  result is DELIVERED as a message `{:replicate_result, tag, {:ok, last} | {:error, reason}}` to
  `notify_pid` instead of a call reply. The caller owns retry/timeout policy for a lost cast (an
  unreachable primary never answers), typically with its own safety timer.
  """
  @spec replicate_async(term(), term(), [term()], non_neg_integer(), [Malachi.Log.Record.t()], pid(), term()) :: :ok
  def replicate_async(primary, segment_id, replica_set, base_offset, records, notify_pid, tag) do
    # Same trace propagation as replicate/5: the broker produce span becomes this commit's parent.
    GenServer.cast(
      primary,
      {:replicate_async, segment_id, replica_set, base_offset, records, {notify_pid, tag}, Ctx.get_current()}
    )
  end

  @doc """
  Group-commit append: buffers `records` for `segment_id` on the primary WITHOUT fsyncing, returning
  `{:ok, last_offset}` as soon as they are in the buffer. Durability comes from a later `flush/1`,
  which coalesces the fsyncs of many appends into one. Single-broker (rf=1) only: it does no follower
  fan-out, so the caller must use `replicate/5` when the replica set has followers. Same offset and
  return contract as `replicate/5`, so the two are interchangeable as the broker's write function.
  """
  @spec append(term(), term(), [term()], non_neg_integer(), [Malachi.Log.Record.t()]) ::
          {:ok, non_neg_integer()} | {:error, :not_primary | :empty | :empty_replica_set | term()}
  def append(primary, segment_id, replica_set, base_offset, records) do
    GenServer.call(primary, {:append, segment_id, replica_set, base_offset, records})
  end

  @doc """
  Fsyncs every segment on this server that has buffered (un-synced) records, making all prior
  `append/5`s durable in one pass. This is the flush half of group commit.

  Returns `:ok` only when every record appended since the previous flush is durable. Otherwise it
  returns `{:error, [{segment_id, reason}]}` naming each segment whose buffered records were lost to a
  storage failure, whether that failure happened in this flush or earlier, between the append and it:
  either way the append was already answered `{:ok, last}`, and a caller that acks on this reply must
  not ack those records.
  """
  @spec flush(term()) :: :ok | {:error, [{term(), term()}]}
  def flush(ref) do
    GenServer.call(ref, :flush)
  end

  @doc """
  Reads up to `max_records` records of `segment_id` stored on this server, from `offset`.

  A server that is down or on an unreachable node answers `{:error, :unreachable}` rather than exiting
  the caller. The caller is the broker loop serving a consumer, and letting it exit would take the
  whole node's reads down with the one segment whose primary went away.
  """
  @spec read(term(), term(), non_neg_integer(), pos_integer()) ::
          {:ok, [Malachi.Log.Record.t()]} | :eof | {:error, term()}
  def read(ref, segment_id, offset, max_records) do
    GenServer.call(ref, {:read, segment_id, offset, max_records})
  catch
    :exit, _reason -> {:error, :unreachable}
  end

  @doc """
  Deletes `segment_id`'s stored data from this server (used by retention once the control plane has
  dropped the segment). Idempotent. Deleting an unknown or already-removed segment is `:ok`, and it
  also clears any on-disk files left after a restart when the log was not reopened.
  """
  @spec delete(term(), term()) :: :ok
  def delete(ref, segment_id) do
    GenServer.call(ref, {:delete, segment_id})
  catch
    # An unreachable replica must not crash the caller (e.g. retention's periodic sweep): the files
    # are left in place, harmless without control-plane metadata, and cleaned up on a later sweep.
    :exit, _reason -> :ok
  end

  @doc """
  Appends a replicated batch of `segment_id` to this server (the follower side). `expected_first`
  is the offset the batch must start at: it must equal this server's current end for the segment
  (or the segment's base when it is opened here for the first time). Returns `{:ok, last_offset}`
  or `{:error, :out_of_sync}` if this server is behind.

  This is the **directed** append, used by `Malachi.Cluster.Catchup` to copy a span into a target
  replica. The primary's own fan-out does not come through here: it pushes `:replica_append` casts
  from its loop, which is what keeps the pipeline per-pair FIFO. A batch that arrives here never
  triggers a catch-up, since the caller is already driving one.
  """
  @spec follow(term(), term(), non_neg_integer(), [Malachi.Log.Record.t()]) ::
          {:ok, non_neg_integer()} | {:error, :out_of_sync | {:storage, term()}}
  def follow(ref, segment_id, expected_first, records) do
    GenServer.call(ref, {:follow, segment_id, expected_first, records})
  end

  @doc """
  This server's next offset for `segment_id`, or `:empty` if it stores none of it yet. `timeout`
  bounds the call: pollers (the broker's periodic range-state refresh) pass a short one so an
  unreachable replica cannot block their loop for the default five seconds.
  """
  @spec end_offset(term(), term(), timeout()) :: non_neg_integer() | :empty
  def end_offset(ref, segment_id, timeout \\ 5_000) do
    GenServer.call(ref, {:end_offset, segment_id}, timeout)
  end

  @doc """
  The on-disk byte size this server stores for `segment_id`: the sum of its segment files' sizes,
  read without opening the log (no descriptors, no state change), so it is cheap enough to poll.
  0 when nothing is stored. The first stage of the sealed-copy integrity probe
  (`Malachi.Cluster.SelfHealing`): a **sealed** segment whose stored bytes fall short of the
  metadata's sealed `byte_size` has lost data on this replica. Only meaningful for sealed segments;
  an active segment's file legitimately trails its in-memory log by the unflushed buffer.
  """
  @spec stored_bytes(term(), term(), timeout()) :: non_neg_integer()
  def stored_bytes(ref, segment_id, timeout \\ 5_000) do
    GenServer.call(ref, {:stored_bytes, segment_id}, timeout)
  end

  @doc """
  The durable end offset this server holds for `segment_id`, recovering the log from disk when it
  is not open yet. Unlike `end_offset/3` (which answers `:empty` for a segment that exists on disk
  but has not been touched since this server booted), this gives the true resume point after a
  restart, which is what a repair needs as its copy start. `base_offset` seats a missing or empty
  log at the segment's base. A copy that failed here answers `{:error, {:storage, reason}}`.
  """
  @spec durable_end(term(), term(), non_neg_integer(), timeout()) :: non_neg_integer() | {:error, term()}
  def durable_end(ref, segment_id, base_offset, timeout \\ 5_000) do
    GenServer.call(ref, {:durable_end, segment_id, base_offset}, timeout)
  end

  @doc """
  What this server durably holds for `segment_id`, as `{end_offset, byte_size}`, WITHOUT fencing it.

  The measuring half of `seal/4`, kept separate because the two answer different questions and only one
  of them has consequences. A failover pass has to learn what its replicas hold before it knows whether
  a majority answered, and fencing to find that out would close replicas of a segment the pass then
  declines to seal: a fence has no inverse, so those replicas keep refusing writes after their primary
  returns, and the range never recovers. So the pass measures first with this, and fences with `seal/4`
  only once it knows it is going to seal.

  Flushes before answering, for the same reason `seal/4` does: a log's next offset counts buffered
  records while the store serves only committed ones, so an unflushed answer describes records a read
  cannot return. `base_offset` seats a missing or empty log at the segment's base.
  """
  @spec durable_stats(term(), term(), non_neg_integer(), timeout()) ::
          {:ok, non_neg_integer(), non_neg_integer()} | {:error, term()}
  def durable_stats(ref, segment_id, base_offset, timeout \\ 5_000) do
    GenServer.call(ref, {:durable_stats, segment_id, base_offset}, timeout)
  catch
    :exit, _reason -> {:error, :unreachable}
  end

  @doc """
  Seals `segment_id` on this server and reports what it durably holds: `{:ok, end_offset, byte_size}`.

  The write fence. After it returns, `replicate/5`, `replicate_async/7`, `append/5` and the primary's
  `:replica_append` fan-out are all refused here with `{:error, {:sealed, end_offset}}`, and again after
  a restart, so the segment's log can never grow past the offset reported. That is what lets a caller
  RECORD the returned end as the sealed length instead of measuring one beside it: the length becomes a
  consequence of closing the segment rather than a number racing it.

  Idempotent and cheap on an already-fenced segment (no fsync, the same pair). `base_offset` seats a
  missing or empty log at the segment's base, so fencing a segment this server never stored succeeds at
  `{:ok, base_offset, 0}` and refuses a later stray append rather than silently accepting it. Reads
  (`read/4`) and repair (`follow/4`) still work on a fenced segment.

  A dead or unreachable server answers `{:error, :unreachable}` rather than exiting the caller, as
  `read/4` and `delete/2` already do: the caller is the broker loop, and letting it exit would take a
  node's writes down with one segment.
  """
  @spec seal(term(), term(), non_neg_integer(), timeout()) ::
          {:ok, non_neg_integer(), non_neg_integer()} | {:error, term()}
  def seal(ref, segment_id, base_offset, timeout \\ 5_000) do
    GenServer.call(ref, {:seal, segment_id, base_offset}, timeout)
  catch
    :exit, _reason -> {:error, :unreachable}
  end

  @doc """
  The same fence as `seal/4`, without waiting for it: the answer is DELIVERED to `notify_pid` as
  `{:seal_result, tag, {:ok, end_offset, byte_size} | {:error, reason}}`.

  For a caller that must not block its loop on a network call, which is the produce roll in
  `Malachi.BrokerServer`. A cast to a dead or unreachable server is simply never answered, so the caller
  owns the retry: sending the fence again is safe, since it is idempotent and answers the same numbers.
  """
  @spec seal_async(term(), term(), non_neg_integer(), pid(), term()) :: :ok
  def seal_async(ref, segment_id, base_offset, notify_pid, tag) do
    GenServer.cast(ref, {:seal_async, segment_id, base_offset, {notify_pid, tag}})
  end

  @doc """
  Which of `segments` this server has already FENCED, as `%{segment_id => {end_offset, byte_size}}`.
  Never fences anything: it reports a latch somebody else already closed.

  `segments` is a list of `{segment_id, base_offset}` pairs, batched into one call on purpose. The
  caller (`Malachi.Cluster.HealCoordinator`) asks about EVERY active segment on every pass, which is a
  different shape of question from `durable_stats/4`'s: that one is asked about a handful of failover
  candidates and pays an open plus a flush per segment, and paying that for every active segment of a
  cluster would put descriptors and I/O on a poll where there is none today.

  A segment that is not fenced is simply absent from the answer and costs a map lookup, or a `File.exists?`
  of the seal marker when this server has not opened it since booting (the same test `replicate/5` itself
  applies). Only the fenced ones, which is the rare case this exists to find, pay an open.

  A dead or unreachable server answers `{:error, :unreachable}` rather than exiting the caller, as
  `durable_stats/4` and `seal/4` do: the caller is a coordinator loop that must survive a replica it
  cannot reach.
  """
  @spec fenced_segments(term(), [{term(), non_neg_integer()}], timeout()) ::
          {:ok, %{optional(term()) => {non_neg_integer(), non_neg_integer()}}} | {:error, term()}
  def fenced_segments(ref, segments, timeout \\ 5_000) do
    GenServer.call(ref, {:fenced_segments, segments}, timeout)
  catch
    :exit, _reason -> {:error, :unreachable}
  end

  @doc """
  Which of `segment_ids` have FAILED on this server (see "Storage failures"), as a `MapSet`.

  Read-only and free of disk access: it is a lookup in the failure latch, so a heal pass can ask every
  replica about every active segment it holds on each pass. A failed copy is one this server will never
  write again, which is what `Malachi.Cluster.Failover` needs to know to seal the segment elsewhere.

  A dead or unreachable server answers `{:error, :unreachable}` rather than exiting the caller.
  """
  @spec failed_segments(term(), [term()], timeout()) :: {:ok, MapSet.t()} | {:error, term()}
  def failed_segments(ref, segment_ids, timeout \\ 5_000) do
    GenServer.call(ref, {:failed_segments, segment_ids}, timeout)
  catch
    :exit, _reason -> {:error, :unreachable}
  end

  # --- GenServer ---

  @impl true
  def init(opts) do
    # When unregistered, the server's reference is its own pid (set in replica sets by the caller). A
    # registered server's ref is `{name, node()}`: replica sets built for a cluster carry `{name, node}`
    # tuples (see `Malachi.Application.broker_refs/1`), and a bare-atom ref would never equal them, so
    # the primary check would reject every clustered produce with `:not_primary`.
    ref =
      case Keyword.get(opts, :name) do
        nil -> self()
        name -> {name, node()}
      end

    directory = Keyword.fetch!(opts, :directory)
    File.mkdir_p!(directory)

    state = %{
      ref: ref,
      directory: directory,
      log_opts:
        Keyword.drop(opts, [
          :name,
          :directory,
          :follow_timeout,
          :replication_window,
          :group_commit,
          :group_commit_interval_ms
        ]),
      logs: %{},
      trackers: %{},
      catching_up: MapSet.new(),
      catchup_monitors: %{},
      # Pipelined replication (NorthGuard style). `inflight` holds the pushed-but-unresolved batches per
      # segment in offset order, each with its parked caller; `pending` queues batches past the window;
      # `committed` is the per-segment commit progress pushed to followers with each replica append.
      follow_timeout: Keyword.get(opts, :follow_timeout, @follow_timeout),
      replication_window: Keyword.get(opts, :replication_window, @default_replication_window),
      inflight: %{},
      pending: %{},
      committed: %{},
      # Group commit under replication (NorthGuard: fsync on every replica, coalesced by time/count/size,
      # before the produce ack). With it on, appends buffer, the primary defers its own tracker ack and
      # each follower defers its cumulative durable ack to the next `:gc_flush` tick (`gc_interval`),
      # so many batches share one fsync per replica while the reply still waits for a durable quorum.
      group_commit: Keyword.get(opts, :group_commit, false),
      gc_interval: Keyword.get(opts, :group_commit_interval_ms, @default_gc_interval_ms),
      gc_timer: nil,
      pending_acks: %{},
      # The failure latch (see "Storage failures"): segment => the reason its copy failed here.
      failed: %{},
      # Segments that failed while holding buffered records, which `:flush` has to report because those
      # appends were already answered `{:ok, last}`. One entry per segment, cleared by the flush.
      lost_unflushed: %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:replicate, _segment_id, _replica_set, _base_offset, [], _ctx}, _from, state) do
    {:reply, {:error, :empty}, state}
  end

  def handle_call({:replicate, _segment_id, [], _base_offset, _records, _ctx}, _from, state) do
    {:reply, {:error, :empty_replica_set}, state}
  end

  def handle_call({:replicate, segment_id, replica_set, base_offset, records, ctx}, from, state) do
    token = Ctx.attach(ctx)
    replica_set = Enum.map(replica_set, &canonical_ref/1)

    try do
      # The span covers the primary's local durable append; the follower fan-out completes
      # asynchronously (see the :replica_ack handler), so it is not inside this span.
      Tracer.with_span "malachi.replication.commit" do
        case write_refusal(state, segment_id, base_offset) do
          {:refuse, reply, state} ->
            {:reply, reply, state}

          :proceed when hd(replica_set) == state.ref ->
            case do_replicate(state, {:call, from}, segment_id, replica_set, base_offset, records) do
              {:done, result, state} -> {:reply, result, state}
              {:parked, state} -> {:noreply, state}
            end

          :proceed ->
            {:reply, {:error, :not_primary}, state}
        end
      end
    after
      Ctx.detach(token)
    end
  end

  def handle_call({:append, _segment_id, _replica_set, _base_offset, []}, _from, state) do
    {:reply, {:error, :empty}, state}
  end

  def handle_call({:append, _segment_id, [], _base_offset, _records}, _from, state) do
    {:reply, {:error, :empty_replica_set}, state}
  end

  def handle_call({:append, segment_id, replica_set, base_offset, records}, _from, state) do
    # Buffer only, no fsync (that is `:flush`). Single-broker: the primary is the sole replica, so there
    # is no follower fan-out here; the broker routes multi-replica sets through `:replicate` instead.
    replica_set = Enum.map(replica_set, &canonical_ref/1)

    case write_refusal(state, segment_id, base_offset) do
      {:refuse, reply, state} ->
        {:reply, reply, state}

      :proceed when hd(replica_set) == state.ref ->
        with {:ok, state, log} <- open_segment(state, segment_id, base_offset),
             {:ok, state, _first, last} <- write_records(state, segment_id, log, records, :buffered) do
          {:reply, {:ok, last}, state}
        else
          {:error, reason, state} -> {:reply, {:error, reason}, state}
        end

      :proceed ->
        {:reply, {:error, :not_primary}, state}
    end
  end

  def handle_call(:flush, _from, state) do
    # `sync_pending/1` fails every segment whose sync fails, and failing a segment that held buffered
    # records records the loss, so after it the losses are exactly what this flush must not call durable.
    state = sync_pending(state)

    reply = if state.lost_unflushed == %{}, do: :ok, else: {:error, Enum.sort(state.lost_unflushed)}
    {:reply, reply, %{state | lost_unflushed: %{}}}
  end

  def handle_call({:follow, segment_id, expected_first, records}, _from, state) do
    # A fresh log opens exactly where this batch starts: the caller (Catchup) copies a span it chose,
    # so there is no earlier gap for this replica to backfill. The primary's fan-out, which does have
    # to seat a new replica at the segment's base, comes in through :replica_append instead.
    case open_segment(state, segment_id, expected_first) do
      {:ok, state, %Log{next_offset: ^expected_first} = log} ->
        case write_records(state, segment_id, log, records, :durable) do
          {:ok, state, _first, last} -> {:reply, {:ok, last}, state}
          {:error, reason, state} -> {:reply, {:error, reason}, state}
        end

      {:ok, state, _log_at_another_offset} ->
        {:reply, {:error, :out_of_sync}, state}

      {:error, reason, state} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:read, segment_id, offset, max_records}, _from, state) do
    cond do
      Map.has_key?(state.failed, segment_id) ->
        {:reply, storage_error(state, segment_id), state}

      Map.has_key?(state.logs, segment_id) ->
        read_open(state, segment_id, offset, max_records)

      # Cold read: a restarted server holds durable segments nothing has opened yet, and only the
      # append path used to open them, so every pre-restart record answered :eof until some write
      # happened to touch its segment (the storage-chaos harness read 0 of 4592 acked records off
      # a fully healthy cluster this way). Recover from disk when files exist; reading a segment
      # this server never stored stays :eof and must not create an empty log as a side effect.
      Path.wildcard(Path.join(segment_directory(state.directory, segment_id), "*.log")) == [] ->
        {:reply, :eof, state}

      true ->
        # The base offset opt only seats an EMPTY log; with files present recover derives the
        # true offsets from them, so 0 here is inert.
        case open_segment(state, segment_id, 0) do
          {:ok, state, _log} -> read_open(state, segment_id, offset, max_records)
          {:error, reason, state} -> {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call({:delete, segment_id}, _from, state) do
    # Deleting clears the failure latch too: the copy is gone, and a segment that comes back under this
    # id later is a new copy with nothing wrong with it.
    state = %{
      state
      | failed: Map.delete(state.failed, segment_id),
        lost_unflushed: Map.delete(state.lost_unflushed, segment_id)
    }

    case Map.pop(state.logs, segment_id) do
      # Open here: close and drop its whole directory.
      {%Log{} = log, logs} ->
        :ok = Log.delete(log)
        {:reply, :ok, %{state | logs: logs}}

      # Not open (never replicated here, or not reopened after a restart, or failed): clear any files.
      {nil, logs} ->
        _ = File.rm_rf(segment_directory(state.directory, segment_id))
        {:reply, :ok, %{state | logs: logs}}
    end
  end

  def handle_call({:end_offset, segment_id}, _from, state) do
    reply =
      case Map.fetch(state.logs, segment_id) do
        {:ok, log} -> log.next_offset
        :error -> :empty
      end

    {:reply, reply, state}
  end

  def handle_call({:stored_bytes, segment_id}, _from, state) do
    {:reply, bytes_on_disk(state, segment_id), state}
  end

  def handle_call({:durable_end, segment_id, base_offset}, _from, state) do
    case open_segment(state, segment_id, base_offset) do
      {:ok, state, log} -> {:reply, log.next_offset, state}
      {:error, reason, state} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:seal, segment_id, base_offset}, _from, state) do
    # `Log.seal/1` fsyncs before it writes its marker, so the numbers below describe records that can
    # actually be READ back. A log's next offset counts buffered records too, and the store serves only
    # committed ones, so answering unflushed would seal a segment at a length its own replicas cannot
    # serve: the metadata would promise records that read as :eof and the range's reads would stop dead.
    #
    # The sealed log goes back into the state (inside `run_log/4`) BEFORE the byte count is taken: the
    # count comes from the open log rather than from stat, and the copy that was in the state is the
    # pre-seal one, whose committed byte count predates the flush that sealing just did.
    {reply, state} = seal_segment(state, segment_id, base_offset)
    {:reply, reply, state}
  end

  # The batched fence report. Read-only by construction: `fenced?/2` is the same test the write paths
  # use, and only a segment it answers TRUE for is opened (to report where it ended). A segment that is
  # merely active is never touched, which is the property that keeps this safe to run over every active
  # segment: a pass that fenced while probing is what wedged a range at `replication_factor: 2` before.
  def handle_call({:fenced_segments, segments}, _from, state) do
    # A FAILED copy is left out even when it carries a fence marker: its end cannot be trusted, and a
    # failed segment is `failed_segments/3`'s to report, where the heal pass seals it on a majority.
    {state, fenced} =
      Enum.reduce(segments, {state, %{}}, fn {segment_id, base_offset}, {acc_state, acc} ->
        if not Map.has_key?(acc_state.failed, segment_id) and fenced?(acc_state, segment_id) do
          case fenced_end(acc_state, segment_id, base_offset) do
            {:ok, acc_state, end_offset} ->
              {acc_state, Map.put(acc, segment_id, {end_offset, bytes_on_disk(acc_state, segment_id)})}

            {:error, _reason, acc_state} ->
              {acc_state, acc}
          end
        else
          {acc_state, acc}
        end
      end)

    {:reply, {:ok, fenced}, state}
  end

  def handle_call({:failed_segments, segment_ids}, _from, state) do
    failed = for segment_id <- segment_ids, Map.has_key?(state.failed, segment_id), into: MapSet.new(), do: segment_id
    {:reply, {:ok, failed}, state}
  end

  def handle_call({:durable_stats, segment_id, base_offset}, _from, state) do
    # The same measurement as the seal above, without the latch: flush so the answer describes readable
    # records, then report. Sharing the flush matters more than sharing the code, since a probe that
    # answered unflushed would hand a failover pass a seal point its own replicas could not serve.
    with {:ok, state, log} <- open_segment(state, segment_id, base_offset),
         {:ok, state, log} <- flush_log(state, segment_id, log) do
      {:reply, {:ok, log.next_offset, bytes_on_disk(state, segment_id)}, state}
    else
      {:error, reason, state} -> {:reply, {:error, reason}, state}
    end
  end

  # The asynchronous fence (`seal_async/5`): the same seal as the call, answered as a message.
  @impl true
  def handle_cast({:seal_async, segment_id, base_offset, notify}, state) do
    {reply, state} = seal_segment(state, segment_id, base_offset)
    notify_seal(notify, reply)
    {:noreply, state}
  end

  # The fire-and-forget produce path (a frontend that must not block its loop): same flow as the
  # :replicate call, but completions are DELIVERED as messages to the notify target.
  def handle_cast({:replicate_async, _segment_id, _replica_set, _base_offset, records, notify, _ctx}, state)
      when records == [] do
    notify_result(notify, {:error, :empty})
    {:noreply, state}
  end

  def handle_cast({:replicate_async, _segment_id, [], _base_offset, _records, notify, _ctx}, state) do
    notify_result(notify, {:error, :empty_replica_set})
    {:noreply, state}
  end

  def handle_cast({:replicate_async, segment_id, replica_set, base_offset, records, {pid, tag} = notify, ctx}, state) do
    token = Ctx.attach(ctx)
    replica_set = Enum.map(replica_set, &canonical_ref/1)

    try do
      # As in the call path, the span covers the primary's local durable append; the fan-out completes
      # asynchronously.
      Tracer.with_span "malachi.replication.commit" do
        case write_refusal(state, segment_id, base_offset) do
          {:refuse, reply, state} ->
            notify_result(notify, reply)
            {:noreply, state}

          :proceed when hd(replica_set) == state.ref ->
            case do_replicate(state, {:notify, pid, tag}, segment_id, replica_set, base_offset, records) do
              {:done, result, state} ->
                notify_result(notify, result)
                {:noreply, state}

              {:parked, state} ->
                {:noreply, state}
            end

          :proceed ->
            notify_result(notify, {:error, :not_primary})
            {:noreply, state}
        end
      end
    after
      Ctx.detach(token)
    end
  end

  # Follower side of the pipelined push: append durably and ack the primary with the durable end
  # offset (the NorthGuard replica ack). Pushes from one primary arrive in offset order (per-pair
  # FIFO), so a mismatch means this replica is genuinely behind (or ahead via catch-up), not reordered.
  def handle_cast({:replica_append, segment_id, base, expected_first, records, _committed, source}, state) do
    case open_segment(state, segment_id, base) do
      # A failed copy acks the error like a fenced one does below: silence would have the primary wait out
      # its follow timeout for an ack that can never come, while an error just drops this replica from
      # the quorum count.
      {:error, reason, state} ->
        GenServer.cast(source, {:replica_ack, segment_id, state.ref, {:error, reason}})
        {:noreply, state}

      {:ok, state, log} ->
        follow_push(state, log, {segment_id, base, expected_first, records, source})
    end
  end

  # Primary side: fold a follower's durable-offset ack into the tracker and complete every parked
  # batch the quorum now covers. Errors (out_of_sync, and stale acks from a replica no longer in the
  # set) do not count toward the quorum, mirroring the old synchronous gather.
  def handle_cast({:replica_ack, segment_id, follower, {:ok, offset}}, state) do
    with tracker when tracker != nil <- Map.get(state.trackers, segment_id),
         {:ok, tracker} <- ReplicaTracker.ack(tracker, follower, offset) do
      state = put_in(state.trackers[segment_id], tracker)
      {:noreply, resolve_batches(state, segment_id)}
    else
      _stale -> {:noreply, state}
    end
  end

  def handle_cast({:replica_ack, _segment_id, _follower, {:error, _reason}}, state) do
    {:noreply, state}
  end

  # The follower side of one pipelined push, once its log is open.
  defp follow_push(state, log, {segment_id, base, expected_first, records, source}) do
    cond do
      # Fenced here: ack an ERROR rather than stay silent, so the primary simply does not count this
      # replica toward the quorum instead of waiting out the follow timeout for an ack that can never come.
      Log.sealed?(log) ->
        GenServer.cast(source, {:replica_ack, segment_id, state.ref, {:error, :sealed}})
        {:noreply, state}

      log.next_offset == expected_first and state.group_commit ->
        # Group commit on the follower: buffer the append and defer the durable ack to the next flush
        # tick, so one fsync (and one cumulative ack per primary) covers every batch since the last.
        case write_records(state, segment_id, log, records, :buffered) do
          {:ok, state, _first, last} ->
            state = %{state | pending_acks: Map.put(state.pending_acks, {segment_id, source}, last)}
            {:noreply, ensure_gc_timer(state)}

          {:error, reason, state} ->
            GenServer.cast(source, {:replica_ack, segment_id, state.ref, {:error, reason}})
            {:noreply, state}
        end

      log.next_offset == expected_first ->
        case write_records(state, segment_id, log, records, :durable) do
          {:ok, state, _first, last} ->
            GenServer.cast(source, {:replica_ack, segment_id, state.ref, {:ok, last}})
            {:noreply, state}

          {:error, reason, state} ->
            GenServer.cast(source, {:replica_ack, segment_id, state.ref, {:error, reason}})
            {:noreply, state}
        end

      log.next_offset > expected_first ->
        # Already have this batch (a background catch-up overtook the push stream). Under group commit
        # recent buffered records may not be durable yet, so defer the cumulative ack to the flush;
        # otherwise the durable end is an honest immediate ack.
        if state.group_commit do
          state = %{state | pending_acks: Map.put(state.pending_acks, {segment_id, source}, log.next_offset - 1)}
          {:noreply, ensure_gc_timer(state)}
        else
          GenServer.cast(source, {:replica_ack, segment_id, state.ref, {:ok, log.next_offset - 1}})
          {:noreply, state}
        end

      true ->
        # Behind (a new replica from base, or missed batches): nack and pull from the primary in the
        # background; this batch commits via the up-to-date replicas and we rejoin on a later one.
        GenServer.cast(source, {:replica_ack, segment_id, state.ref, {:error, :out_of_sync}})
        {:noreply, trigger_catchup(state, segment_id, base, source)}
    end
  end

  # The group-commit flush tick: one fsync per replica covers every batch buffered since the last tick.
  # As the PRIMARY, self-ack the now-durable end of every segment with parked batches and resolve them;
  # as a FOLLOWER, send one cumulative durable ack per (segment, primary). Both roles run here because a
  # server is usually primary for some segments and follower for others at once.
  @impl true
  def handle_info(:gc_flush, state) do
    # A segment whose sync fails is failed on the spot (`fail_segment/3`): its parked batches get the
    # error and its deferred acks are dropped, so nothing below can release an ack, or self-ack a durable
    # end, for records that did not become durable.
    state = sync_pending(%{state | gc_timer: nil})

    # Follower role: everything appended is durable now, so release the deferred cumulative acks.
    Enum.each(state.pending_acks, fn {{segment_id, source}, last} ->
      GenServer.cast(source, {:replica_ack, segment_id, state.ref, {:ok, last}})
    end)

    state = %{state | pending_acks: %{}}

    # Primary role: fold the local durable end into each parked segment's tracker and resolve.
    state =
      state.inflight
      |> Map.keys()
      |> Enum.concat(Map.keys(state.pending))
      |> Enum.uniq()
      |> Enum.reduce(state, &self_ack_durable_end/2)

    {:noreply, state}
  end

  # A parked batch whose quorum never closed within the follow timeout: give its caller the same
  # no_quorum the synchronous path returned. A batch already resolved by acks is simply gone.
  def handle_info({:replicate_timeout, segment_id, batch_ref}, state) do
    inflight = Map.get(state.inflight, segment_id, [])
    queue = Map.get(state.pending, segment_id, :queue.new())

    case Enum.split_with(inflight, &(&1.ref == batch_ref)) do
      {[batch], rest} ->
        reply_batch(batch, {:error, :no_quorum})
        Telemetry.replication_commit(batch.count, :no_quorum)
        state = put_in(state.inflight[segment_id], rest)
        {:noreply, drain_pending(state, segment_id)}

      {[], _} ->
        queued = :queue.to_list(queue)

        case Enum.split_with(queued, fn {batch, _push} -> batch.ref == batch_ref end) do
          {[{batch, _push}], rest} ->
            reply_batch(batch, {:error, :no_quorum})
            Telemetry.replication_commit(batch.count, :no_quorum)
            {:noreply, put_in(state.pending[segment_id], :queue.from_list(rest))}

          {[], _} ->
            {:noreply, state}
        end
    end
  end

  def handle_info({:DOWN, monitor_ref, :process, _pid, _reason}, state) do
    case Map.pop(state.catchup_monitors, monitor_ref) do
      {nil, _monitors} ->
        {:noreply, state}

      {segment_id, monitors} ->
        {:noreply, %{state | catching_up: MapSet.delete(state.catching_up, segment_id), catchup_monitors: monitors}}
    end
  end

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.logs, fn {_segment_id, log} -> Log.close(log) end)
    :ok
  end

  # --- internals ---

  # Pipelined replication (NorthGuard style). The primary appends durably, acks itself in the tracker,
  # and, with followers, PARKS the caller and pushes the batch to every follower as a cast from this
  # loop, without ever blocking on them. Waiting synchronously here (the previous design) let primaries
  # on different nodes block each other's loops in a circular wait: with several cross-node primaries
  # even light traffic deadlocked until every call timed out.
  @spec do_replicate(state(), reply_target(), term(), [term()], non_neg_integer(), [Malachi.Log.Record.t()]) ::
          {:done, {:ok, non_neg_integer()} | {:error, term()}, state()} | {:parked, state()}
  defp do_replicate(state, reply_target, segment_id, replica_set, base_offset, records) do
    # Distinct replicas, and never the primary itself among the followers. The ack math also assumes
    # distinct replicas.
    replica_set = Enum.uniq(replica_set)
    followers = replica_set -- [state.ref]

    if state.group_commit do
      do_replicate_grouped(state, reply_target, segment_id, replica_set, followers, base_offset, records)
    else
      do_replicate_durable(state, reply_target, segment_id, replica_set, followers, base_offset, records)
    end
  end

  # Per-batch durability: append + fsync locally, self-ack, push. The original semantics.
  defp do_replicate_durable(state, reply_target, segment_id, replica_set, followers, base_offset, records) do
    with {:ok, state, log} <- open_segment(state, segment_id, base_offset),
         {:ok, state, first, last} <- write_records(state, segment_id, log, records, :durable) do
      commit_local(state, reply_target, {segment_id, replica_set, followers, base_offset, records}, first, last)
    else
      {:error, reason, state} -> {:done, {:error, reason}, state}
    end
  end

  # The primary's own copy of the batch is durable: self-ack it and either reply (no followers) or park
  # the caller and push to the followers.
  defp commit_local(state, reply_target, {segment_id, replica_set, followers, base_offset, records}, first, last) do
    tracker = tracker_for(state, segment_id, replica_set)
    {:ok, tracker} = ReplicaTracker.ack(tracker, state.ref, last)
    state = put_in(state.trackers[segment_id], tracker)

    if followers == [] do
      reply = if ReplicaTracker.committed?(tracker, last), do: {:ok, last}, else: {:error, :no_quorum}
      Telemetry.replication_commit(length(records), if(match?({:ok, _}, reply), do: :ok, else: :no_quorum))
      {:done, reply, state}
    else
      {:parked, park_and_push(state, reply_target, segment_id, base_offset, first, last, followers, records)}
    end
  end

  # Group commit (NorthGuard: fsync on every replica coalesced by time/count/size, before the produce
  # ack): append WITHOUT fsync (the store's count/size triggers may still fire) and push right away, but
  # defer the primary's own tracker ack to the next :gc_flush tick, when one fsync covers every batch
  # buffered since the last. The caller parks even with no followers: a reply must never precede local
  # durability.
  defp do_replicate_grouped(state, reply_target, segment_id, replica_set, followers, base_offset, records) do
    with {:ok, state, log} <- open_segment(state, segment_id, base_offset),
         {:ok, state, first, last} <- write_records(state, segment_id, log, records, :buffered) do
      # Ensure the tracker exists now, so a follower ack arriving before our first flush still lands.
      state = put_in(state.trackers[segment_id], tracker_for(state, segment_id, replica_set))

      state
      |> park_and_push(reply_target, segment_id, base_offset, first, last, followers, records)
      |> ensure_gc_timer()
      |> then(&{:parked, &1})
    else
      {:error, reason, state} -> {:done, {:error, reason}, state}
    end
  end

  # Parks the caller's batch and pushes it to the followers (with none it still parks; the batch then
  # resolves once the deferred local durable ack covers it, on the next flush).
  defp park_and_push(state, reply_target, segment_id, base_offset, first, last, followers, records) do
    ref = make_ref()
    # The no-quorum timer arms at park time (not push time), so a caller queued behind a full window
    # still gets its reply well before its own call timeout. Its reference travels in the batch so a
    # normal resolution cancels it; otherwise every committed batch would still fire a stale timeout
    # message follow_timeout later, each one walking the inflight list and the pending queue for nothing.
    timer = Process.send_after(self(), {:replicate_timeout, segment_id, ref}, state.follow_timeout)
    batch = %{ref: ref, reply: reply_target, last: last, count: length(records), timer: timer}

    push =
      {followers,
       {:replica_append, segment_id, base_offset, first, records, committed_of(state, segment_id), state.ref}}

    park_batch(state, segment_id, batch, push)
  end

  # Completes a parked batch toward whoever is waiting: a synchronous caller (GenServer.reply) or an
  # async notify target (a plain message, the fire-and-forget produce path).
  defp reply_batch(%{reply: {:call, from}}, result), do: GenServer.reply(from, result)
  defp reply_batch(%{reply: {:notify, pid, tag}}, result), do: send(pid, {:replicate_result, tag, result})

  defp notify_result({pid, tag}, result), do: send(pid, {:replicate_result, tag, result})

  # Parks a batch: pushes it right away when the segment's window has room, otherwise queues it FIFO.
  # The local append already assigned its offsets under this serial loop, so draining in FIFO order
  # preserves the follower-side expected_first chain.
  defp park_batch(state, segment_id, batch, push) do
    if length(Map.get(state.inflight, segment_id, [])) < state.replication_window do
      do_push(push)
      update_in(state.inflight[segment_id], &((&1 || []) ++ [batch]))
    else
      update_in(state.pending[segment_id], &:queue.in({batch, push}, &1 || :queue.new()))
    end
  end

  # Casting from the primary's own loop (never from a helper process) is what guarantees per-follower
  # FIFO: Erlang orders messages between a fixed pair of processes, so the appends arrive in offset
  # order and pipelining needs no per-batch synchronization.
  defp do_push({followers, message}), do: Enum.each(followers, &GenServer.cast(&1, message))

  # Replies (in offset order) every inflight batch whose last offset the quorum now covers, then
  # refills the window from the pending queue. committed?/2 is monotone in the offset, so the walk can
  # stop at the first uncovered batch.
  defp resolve_batches(state, segment_id) do
    tracker = Map.fetch!(state.trackers, segment_id)
    inflight = Map.get(state.inflight, segment_id, [])
    {done, still} = Enum.split_while(inflight, &ReplicaTracker.committed?(tracker, &1.last))

    state =
      Enum.reduce(done, state, fn batch, acc ->
        Process.cancel_timer(batch.timer)
        reply_batch(batch, {:ok, batch.last})
        Telemetry.replication_commit(batch.count, :ok)
        %{acc | committed: Map.put(acc.committed, segment_id, batch.last)}
      end)

    state = put_in(state.inflight[segment_id], still)
    drain_pending(state, segment_id)
  end

  # Pushes queued batches while the window has room.
  defp drain_pending(state, segment_id) do
    inflight = Map.get(state.inflight, segment_id, [])
    queue = Map.get(state.pending, segment_id, :queue.new())

    if length(inflight) < state.replication_window do
      case :queue.out(queue) do
        {{:value, {batch, push}}, rest} ->
          do_push(push)

          state
          |> put_in([Access.key(:inflight), segment_id], inflight ++ [batch])
          |> put_in([Access.key(:pending), segment_id], rest)
          |> drain_pending(segment_id)

        {:empty, _} ->
          state
      end
    else
      state
    end
  end

  defp committed_of(state, segment_id), do: Map.get(state.committed, segment_id, -1)

  # Schedules the next group-commit flush only when none is pending, so a burst shares one timer.
  defp ensure_gc_timer(%{gc_timer: nil} = state) do
    %{state | gc_timer: Process.send_after(self(), :gc_flush, state.gc_interval)}
  end

  defp ensure_gc_timer(state), do: state

  # After a flush, acks the local (now durable) end of `segment_id` into its tracker and resolves the
  # parked batches it covers. Skips segments whose log is gone (deleted) or empty.
  defp self_ack_durable_end(segment_id, state) do
    with {:ok, log} <- Map.fetch(state.logs, segment_id),
         durable_end = log.next_offset - 1,
         true <- durable_end >= 0,
         tracker when tracker != nil <- Map.get(state.trackers, segment_id),
         {:ok, tracker} <- ReplicaTracker.ack(tracker, state.ref, durable_end) do
      state
      |> put_in([Access.key(:trackers), segment_id], tracker)
      |> resolve_batches(segment_id)
    else
      _skip -> state
    end
  end

  # Reuses the segment's tracker (so acks accumulate across batches), rebuilding it if the replica
  # set changed.
  defp tracker_for(state, segment_id, replica_set) do
    case Map.get(state.trackers, segment_id) do
      %ReplicaTracker{replica_set: ^replica_set} = tracker -> tracker
      _ -> ReplicaTracker.new(replica_set)
    end
  end

  # Starts one background catch-up per segment (deduped via `catching_up`): it pulls everything the
  # source has past our current end. Monitored so the in-progress flag is cleared on completion or
  # crash; the offset check in `follow` keeps concurrent appends safe, so a racing produce just
  # makes the catch-up abort and the next gap re-trigger.
  # Normalizes a replica ref for comparison with `state.ref`: a bare atom names a locally registered
  # server (single-node deployments and tests), so it is equivalent to `{name, node()}`; pids and
  # `{name, node}` tuples (cluster replica sets) pass through unchanged.
  defp canonical_ref(ref) when is_atom(ref), do: {ref, node()}
  defp canonical_ref(ref), do: ref

  defp trigger_catchup(state, segment_id, base, source) do
    if MapSet.member?(state.catching_up, segment_id) do
      state
    else
      target = state.ref
      {_pid, monitor_ref} = spawn_monitor(fn -> run_catchup(target, source, segment_id, base) end)

      %{
        state
        | catching_up: MapSet.put(state.catching_up, segment_id),
          catchup_monitors: Map.put(state.catchup_monitors, monitor_ref, segment_id)
      }
    end
  end

  defp run_catchup(target, source, segment_id, base) do
    from = current_end(target, segment_id, base)
    to = current_end(source, segment_id, base)

    if to > from do
      case Catchup.run(target, source, segment_id, from, to) do
        {:ok, _offset} ->
          :ok

        {:error, reason} ->
          # A failed catch-up leaves the replica behind; the offset check in `follow` re-triggers it on
          # the next fan-out. Log it so a persistently failing catch-up is visible rather than silent.
          Logger.warning(
            I18n.t(:replication_catchup_failed,
              segment_id: inspect(segment_id),
              from: from,
              to: to,
              reason: inspect(reason)
            )
          )
      end
    end
  end

  # `base` is the segment's range-relative first offset, used as the fallback when `ref` holds none of the
  # segment yet (a brand-new or far-behind replica). Falling back to 0 here would make Catchup.run read the
  # source below its real base and fail with :out_of_range for any non-zero-base segment.
  defp current_end(ref, segment_id, base) do
    case end_offset(ref, segment_id) do
      :empty -> base
      offset -> offset
    end
  end

  # `base_offset` is used only when the segment's log does not exist yet, so a fresh segment starts
  # at its range-relative first offset.
  # Every `*.log` file this server holds for the segment, sealed ones included, which is what makes it
  # the whole logical segment rather than whichever piece is open right now.
  #
  # An OPEN log is asked rather than stat'd, because a store may size the active segment's file
  # ahead of what it has written (`Malachi.Storage.Preallocation`) and stat would then report the
  # room instead of the records. It matters well past this function: this number is recorded as the
  # segment's `byte_size` when it is sealed or fenced, and from there it drives size-based retention
  # and the lost-copy probe in `Malachi.Cluster.SelfHealing`, which re-backfills any replica whose
  # bytes fall short of it. A preallocated file counted as content would inflate that number on one
  # replica and have healthy peers re-copied as if they were truncated.
  #
  # A segment with no open log is stat'd as before. Every caller that needs the number to be exact
  # (`:seal`, `:durable_stats`, `:fenced_segments`) opens the log first; `:stored_bytes` deliberately
  # does not, and it probes SEALED segments, whose files carry no preallocated tail.
  defp bytes_on_disk(state, segment_id) do
    case Map.fetch(state.logs, segment_id) do
      {:ok, log} -> Log.bytes_on_disk(log)
      :error -> stat_bytes_on_disk(state, segment_id)
    end
  end

  defp stat_bytes_on_disk(state, segment_id) do
    state.directory
    |> segment_directory(segment_id)
    |> Path.join("*.log")
    |> Path.wildcard()
    |> Enum.reduce(0, fn path, sum ->
      case File.stat(path) do
        {:ok, %{size: size}} -> sum + size
        {:error, _reason} -> sum
      end
    end)
  end

  # Only `open_segment/3` calls this: it is what applies the failure latch before and after.
  defp fetch_or_open(state, segment_id, base_offset) do
    case Map.fetch(state.logs, segment_id) do
      {:ok, log} ->
        {:ok, state, log}

      :error ->
        # recover, not open: after a restart the segment's files may already exist on disk (a durable
        # replica), and a blind open would crash the first append with :already_exists. recover resumes
        # at the true durable end (a push past it nacks out_of_sync and catch-up backfills the gap) and
        # falls back to a fresh open when the directory is empty.
        opts = [base_offset: base_offset] ++ state.log_opts

        with {:ok, log} <- Log.recover(segment_directory(state.directory, segment_id), opts) do
          report_integrity(log.integrity, segment_id)
          {:ok, put_log(state, segment_id, log), log}
        end
    end
  end

  # Recovery already scanned the segment, so it knows whether the bytes on disk are intact. Reporting
  # that here, at the one place a segment is opened, is what makes damage visible the moment a node
  # touches it: the background scrub verifies every segment eventually, but on its own slow cadence,
  # so without this a node could serve a damaged copy for days without a word. Fires once per segment
  # (the log is cached afterwards), so it cannot spam.
  defp report_integrity(:ok, _segment_id), do: :ok

  defp report_integrity(verdict, segment_id) do
    Telemetry.storage_integrity(verdict, segment_id, :recover)

    # Three separate keys rather than one shared stem with an appended clause: a translated sentence
    # cannot be assembled by concatenating a fragment onto a stem and stay grammatical.
    bindings = [
      segment_id: inspect(segment_id),
      position: verdict.position,
      reason: verdict.reason,
      bytes: verdict.unreadable_bytes
    ]

    cond do
      # Immutable and fully durable when it was sealed, so a short scan is corruption at rest. The
      # copy now serves only its valid prefix and needs repair from a peer.
      verdict.sealed? ->
        Logger.warning(I18n.t(:replication_sealed_segment_damaged, bindings))

      # A torn frame at the end of an active segment is ordinary crash recovery: those bytes were
      # never acked. Worth a line because it quantifies what the crash cost, not an alarm.
      verdict.reason == :incomplete ->
        Logger.info(
          I18n.t(:replication_partial_write_dropped,
            segment_id: inspect(segment_id),
            bytes: verdict.unreadable_bytes
          )
        )

      # A full frame that fails its checksum was written completely and is wrong: rot or a bug, not
      # a torn write, even though the segment is still active.
      true ->
        Logger.warning(I18n.t(:replication_active_segment_damaged, bindings))
    end
  end

  # Cheap when the log is open, which is the hot case. The marker check is the cold one (a restarted
  # server that has not touched this segment yet) and is what makes the fence survive a restart. The
  # guard sits here, not in `Log.append/2`, because repair must still be able to write into a fenced
  # segment: `follow/4` appends records that already carry their offsets, bounded above by the source's
  # end, and a fenced source's end IS the sealed edge, so a repair cannot overshoot it.
  defp fenced?(state, segment_id) do
    case Map.fetch(state.logs, segment_id) do
      {:ok, log} -> Log.sealed?(log)
      :error -> File.exists?(Log.seal_marker_path(segment_directory(state.directory, segment_id)))
    end
  end

  # The write fence behind both `seal/4` and `seal_async/5`, answering what the sealed log holds.
  defp seal_segment(state, segment_id, base_offset) do
    with {:ok, state, log} <- open_segment(state, segment_id, base_offset),
         {:ok, state, log} <- run_log(state, segment_id, log, &Log.seal/1) do
      {{:ok, log.next_offset, bytes_on_disk(state, segment_id)}, state}
    else
      {:error, reason, state} -> {{:error, reason}, state}
    end
  end

  defp notify_seal({pid, tag}, reply), do: send(pid, {:seal_result, tag, reply})

  # The end a refused writer should seat itself at, so its retry opens the successor where this segment
  # closed instead of racing the control plane's tiling rule.
  defp fenced_end(state, segment_id, base_offset) do
    with {:ok, state, log} <- open_segment(state, segment_id, base_offset) do
      {:ok, state, log.next_offset}
    end
  end

  # Why a write to `segment_id` must not proceed here, asked the same way by every write entry point
  # (`replicate/5`, `replicate_async/7`, `append/5`). The failure latch comes first because it answers
  # without touching the disk, which the fence check may not.
  defp write_refusal(state, segment_id, base_offset) do
    cond do
      Map.has_key?(state.failed, segment_id) ->
        {:refuse, storage_error(state, segment_id), state}

      fenced?(state, segment_id) ->
        case fenced_end(state, segment_id, base_offset) do
          {:ok, state, end_offset} -> {:refuse, {:error, {:sealed, end_offset}}, state}
          {:error, reason, state} -> {:refuse, {:error, reason}, state}
        end

      true ->
        :proceed
    end
  end

  # --- storage failures (see the moduledoc) ---

  # The one way a handler reaches a segment's log. A segment that already failed here is refused without
  # touching the disk; one whose log fails to open now is failed on the spot.
  @spec open_segment(state(), term(), non_neg_integer()) :: {:ok, state(), Log.t()} | {:error, term(), state()}
  defp open_segment(state, segment_id, base_offset) do
    if Map.has_key?(state.failed, segment_id) do
      {:error, {:storage, Map.fetch!(state.failed, segment_id)}, state}
    else
      case fetch_or_open(state, segment_id, base_offset) do
        {:ok, state, log} -> {:ok, state, log}
        {:error, reason} -> {:error, {:storage, reason}, fail_segment(state, segment_id, reason)}
      end
    end
  end

  defp storage_error(state, segment_id), do: {:error, {:storage, Map.fetch!(state.failed, segment_id)}}

  # Appends `records` to `segment_id`'s open `log`, either `:durable` (appended and fsynced) or `:buffered`
  # (appended only, for group commit to make durable later), and records the result in the state.
  @spec write_records(state(), term(), Log.t(), [Malachi.Log.Record.t()], :durable | :buffered) ::
          {:ok, state(), non_neg_integer(), non_neg_integer()} | {:error, term(), state()}
  defp write_records(state, segment_id, log, records, durability) do
    case write(log, records, durability) do
      {:ok, log, first, last} ->
        {:ok, put_log(state, segment_id, log), first, last}

      {:error, reason, latest_log} ->
        {:error, {:storage, reason}, fail_segment(put_log(state, segment_id, latest_log), segment_id, reason)}
    end
  end

  # A failure answers the NEWEST log along with the reason, because that is the one `fail_segment/3` has
  # to close: an append that reaches a segment's next file opens it, and only the appended log holds that
  # descriptor. `Log.append/2` closes a file it opened and then failed to write, so after a failed append
  # the newest log is the one we started with.
  defp write(log, records, :buffered) do
    case Log.append(log, records) do
      {:ok, _log, _first, _last} = appended -> appended
      {:error, reason} -> {:error, reason, log}
    end
  end

  defp write(log, records, :durable) do
    case Log.append(log, records) do
      {:ok, appended, first, last} ->
        case Log.sync(appended) do
          {:ok, synced} -> {:ok, synced, first, last}
          {:error, reason} -> {:error, reason, appended}
        end

      {:error, reason} ->
        {:error, reason, log}
    end
  end

  # Runs one whole-log `operation` (`Log.sync/1`, `Log.seal/1`) on `segment_id`'s open log and records the
  # result, failing the segment when it fails.
  defp run_log(state, segment_id, log, operation) do
    case operation.(log) do
      {:ok, log} -> {:ok, put_log(state, segment_id, log), log}
      {:error, reason} -> {:error, {:storage, reason}, fail_segment(state, segment_id, reason)}
    end
  end

  defp flush_log(state, segment_id, log) do
    if Log.pending?(log), do: run_log(state, segment_id, log, &Log.sync/1), else: {:ok, state, log}
  end

  # Syncs every log holding buffered records, failing each one whose sync fails.
  defp sync_pending(state) do
    Enum.reduce(state.logs, state, fn {segment_id, log}, acc ->
      case flush_log(acc, segment_id, log) do
        {:ok, acc, _log} -> acc
        {:error, _reason, acc} -> acc
      end
    end)
  end

  # Latches `segment_id` as failed on this server. It decides nothing about the segment beyond this copy:
  # sealing it where it can still be served is the heal pass's job, which finds it via
  # `failed_segments/3`.
  @spec fail_segment(state(), term(), term()) :: state()
  defp fail_segment(state, segment_id, reason) do
    {log, logs} = Map.pop(state.logs, segment_id)
    lost_buffer? = log != nil and Log.pending?(log)
    if log, do: :ok = Log.close(log)

    # Every caller parked on this segment is answered now. Their records either never became durable
    # here or cannot be vouched for, and leaving them parked would only turn this failure into a
    # misleading `:no_quorum` when their timers fire.
    {inflight, inflight_by_segment} = Map.pop(state.inflight, segment_id, [])
    {queued, pending_by_segment} = Map.pop(state.pending, segment_id, :queue.new())
    reply = {:error, {:storage, reason}}

    for batch <- inflight ++ Enum.map(:queue.to_list(queued), &elem(&1, 0)) do
      Process.cancel_timer(batch.timer)
      reply_batch(batch, reply)
    end

    Logger.error(I18n.t(:replication_segment_storage_failed, segment_id: inspect(segment_id), reason: inspect(reason)))
    Telemetry.storage_failure(segment_id, reason)

    %{
      state
      | logs: logs,
        trackers: Map.delete(state.trackers, segment_id),
        committed: Map.delete(state.committed, segment_id),
        inflight: inflight_by_segment,
        pending: pending_by_segment,
        # Deferred follower acks for records that will now never be synced here.
        pending_acks: Map.reject(state.pending_acks, fn {{acked, _source}, _last} -> acked == segment_id end),
        failed: Map.put(state.failed, segment_id, reason),
        lost_unflushed:
          if(lost_buffer?, do: Map.put(state.lost_unflushed, segment_id, reason), else: state.lost_unflushed)
    }
  end

  # Reads from a segment whose log is open. `:out_of_range` is the log's own answer for an offset below
  # its start (`Malachi.Log.read/3`), not a failure; any other error is the copy failing to be read.
  defp read_open(state, segment_id, offset, max_records) do
    case Log.read(Map.fetch!(state.logs, segment_id), offset, max_records) do
      {:error, :out_of_range} = out_of_range ->
        {:reply, out_of_range, state}

      {:error, reason} ->
        {:reply, {:error, {:storage, reason}}, fail_segment(state, segment_id, reason)}

      records_or_eof ->
        {:reply, records_or_eof, state}
    end
  end

  defp put_log(state, segment_id, log), do: put_in(state.logs[segment_id], log)

  # The on-disk mapping lives in Malachi.Storage.Layout: the scrubber reads the same directories to
  # verify their checksums, and a second copy of this rule could drift from the writer's.
  defp segment_directory(base, segment_id), do: Layout.segment_directory(base, segment_id)
end
