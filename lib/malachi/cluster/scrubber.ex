defmodule Malachi.Cluster.Scrubber do
  @moduledoc """
  Background verification of data at rest (a "scrub"), and repair of what it finds.

  A sealed segment is immutable, and nothing re-reads it: `Malachi.Log.recover/2` trusts sealed
  files by name, so a checksum is only ever confirmed when a consumer happens to read that exact
  frame. Bit rot there is therefore silent, and worse than an error: the read bound comes from the
  control plane's record count rather than from the file, so a read past the damaged frame returns
  no records, which the broker reads as "this source is drained". The consumer then stalls at that
  offset or skips the rest of the range, with nothing logged anywhere.

  This worker closes that by walking the node's own sealed segments on a slow cadence, verifying
  every frame's checksum (`Malachi.Log.verify/2`), and repairing a damaged copy from an intact
  replica. NorthGuard's rule that you are on the clock to re-replicate a lost replica applies here
  too: a copy that will not decode is a lost replica, whatever the metadata says about it.

  ## Why it runs on every node, not just the leader

  Unlike the healing and retention coordinators, this one is deliberately **not** leader-gated:
  each node is the only one that can read its own disk, and corruption is a per-copy fact, not a
  cluster-wide one. Every node scrubs what it stores.

  ## Repair, and its one safety rule

  Damage is repaired by fetching the segment again from a replica that still has it intact, which
  means deleting the local copy first. The rule is that **the local copy is never destroyed before
  a peer has confirmed it holds the whole segment, intact**: swapping a partially readable copy for
  a shorter one, or for no copy at all, would be worse than the damage. Both halves of that
  sentence carry weight, and the second is the one easy to lose: a peer whose checksums all pass can
  still be missing whole frames at the end, and repairing from it deletes the local copy and then
  catches up to an offset the source cannot reach. So a peer qualifies only when its scan matches
  what the control plane recorded at seal time, which is the same test `cross_check/4` applies to
  this node's own copy. Peers are verified through their own `Scrubber` (never the replication server),
  so the verification scan stays off the replication hot loop. The confirmation that precedes a repair
  reads the peer's replication server directly, in bounded batches under a time budget, and the peer is read
  through to the end before any delete. Each batch holds that server for its duration, so a confirmation competes
  with the peer's writes and fsyncs for up to the budget; the per-read bound keeps any one batch short.

  During a rolling upgrade, a repair whose damaged copy is the head is refused while the effective machine
  version is below the one that introduces the compare-and-swap demote. That repair reports `{:demote, ...}` as
  unrepairable and deletes nothing. The refusal lifts when the effective version reaches that version, which needs
  every member on the newer code and no operator pin below it (`MALACHI_RA_MACHINE_VERSION`): a pin keeps the
  effective version down, so the refusal lasts until the pin is removed.

  ## A copy that failed in storage

  A copy this node's replication server has latched as failed (see "Storage failures" in
  `Malachi.Cluster.ReplicationServer`) is skipped, reported under `:skipped`, and never repaired here. The
  repair deletes the copy and refetches it onto the disk that just failed: the delete clears the latch, a
  disk still failing latches it again during the refetch, and the next pass does it all over, reporting
  the same damage every time. `Malachi.Cluster.SelfHealing` replaces such a copy on another broker instead.
  For the same reason a peer asked about a latched copy answers `{:error, :failed_copy}`, so it is never
  chosen as a repair source: this node would delete its own copy and then be refused by the peer.

  A latch that cannot be read (the replication server is slow, restarting or gone) is UNKNOWN, never
  absent, because the repair is the one step here that destroys something. The pass still verifies the
  copies and reports their damage, but repairs nothing from a peer (`:latch_unknown` under
  `:unrepairable`), and a peer in that state answers `{:error, :latch_unknown}` and is not a source.

  When the damaged copy is the segment's primary, the repair first submits a
  `:set_segment_replicas` that moves this node to the end of the replica set. Reads follow the head
  of that set, so they move to an intact replica immediately instead of hitting the hole (or the
  gap left while the copy is being refetched).

  Every pass returns its outcome; logging is only the default `:on_result` callback, so tests
  assert on data rather than on log lines.

  ## Options

    * `:metadata_source` - `(-> Malachi.Metadata.t())`, the current metadata (required);
    * `:local_ref` - this node's replication server reference, or a `(-> ref)` resolved per pass
      (required). A single-node broker owns an unnamed replication server, so its reference is a pid
      that a broker restart replaces: passing a function keeps the scrub from silently matching
      nothing after such a restart;
    * `:directory` - the data directory whose segments are scrubbed (required);
    * `:apply_command` - `(Metadata.command() -> any)`, applies the demote command (required);
    * `:peer_scrubber` - `(node() -> GenServer.server())`, how to reach a peer's scrubber. Defaults
      to this process's own registered name on that node, since every node runs the same worker
      under the same name;
    * `:peer_scrubber_for` - `(replica -> GenServer.server())`, the scrubber of one replica. It defaults to
      `:peer_scrubber` on the replica's node, and exists so a test can tell two replicas on one node apart;
    * `:interval` - ms between ticks (default 60s). A value that is not a positive integer is
      refused with a warning and the default used instead, since it arrives from the environment
      (`Malachi.Cluster.PeriodicWorker`);
    * `:segments_per_tick` - segments verified per tick (default 1). With the default 64MB segment
      size a full cycle takes `segments * interval / segments_per_tick`, so a node holding 10k
      sealed segments revisits each one about weekly;
    * `:on_result` - `(result -> any)` for each pass (default: log damage and repairs);
    * `:confirm_deadline_ms` - the budget, in ms, of one source confirmation (default 10s). A confirmation that runs
      out of it defers the repair and resumes on a later tick;
    * `:call_timeout` - how long the calls that decide the seal and the latch of a rebuilt copy wait for their
      answer, in ms (default 1s). The demotion, the metadata read and the latch lookup keep the fixed one-second bound,
      and the peer verification keeps its own longer timeout;
    * `:confirm_clock` - `(-> integer)`, the clock the confirmation budget is measured by (default: monotonic ms);
    * `:fail_copy` - `(ref, segment_id, reason -> any)`, the latch call of a rebuilt copy (default:
      `ReplicationServer.fail_copy/3`). It is a seam for tests, which make a latch fail on demand;
    * `:delete` - `(ref, segment_id -> any)`, the delete that takes a rebuilt copy away when its latch does not
      answer (default: `ReplicationServer.delete/2`). A seam for tests, as `:fail_copy` is;
  """

  use GenServer

  require Logger
  require OpenTelemetry.Tracer, as: Tracer

  alias Malachi.Cluster.Catchup
  alias Malachi.Cluster.PeriodicWorker
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.I18n
  alias Malachi.Log
  alias Malachi.Metadata
  alias Malachi.Storage.Layout
  alias Malachi.Telemetry

  @default_interval 60_000
  @default_segments_per_tick 1
  # How long one segment's source confirmation may run on the scrub loop before it is deferred to a later tick.
  @default_confirm_deadline_ms 10_000

  # How long the calls that decide the seal and the latch of a rebuilt copy may wait for their answer (see
  # `bounded/2`). The demotion and the metadata read keep this same fixed bound.
  @scrub_call_timeout 1_000

  # A repair that defers this many visits in a row leaves the rotation until the cycle refills, so it cannot keep
  # the segments behind it out of the rotation for as long as its deferral lasts.
  @max_consecutive_deferrals 3
  # Bounds a peer's verification scan. Generous next to the other cross-node probes because the peer
  # is reading a whole segment from disk (24 to 86ms for 64MB warm, more from cold storage), but
  # still bounded so one stuck peer cannot hold up the pass.
  @peer_timeout 30_000

  @type verdict :: :ok | map()
  @type result :: %{
          verified: [Metadata.segment_id()],
          damaged: [{Metadata.segment_id(), verdict()}],
          repaired: [Metadata.segment_id()],
          unrepairable: [{Metadata.segment_id(), term()}],
          skipped: [Metadata.segment_id()]
        }

  @doc "Starts the scrubber. See the module doc for options."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_server_opts, opts} = Keyword.split(opts, [:name])
    # The registered name is ALSO handed to init: a peer's scrubber is addressed by the same name on
    # its node, so deriving that from the name this node registered under keeps the two from
    # drifting. Hardcoding the module name here instead is what made the first cluster run fail to
    # repair: the supervisor registers it as Malachi.LogScrubber, every peer lookup hit a dead name,
    # and every repair concluded there was no intact copy anywhere.
    GenServer.start_link(__MODULE__, Keyword.merge(opts, gen_server_opts), gen_server_opts)
  end

  @doc "Runs one pass synchronously and returns its result, for tests and manual triggers."
  @spec scrub_now(GenServer.server()) :: result()
  def scrub_now(server), do: GenServer.call(server, :scrub_now, :infinity)

  @doc """
  Verifies `segment_id` on this node's disk, without repairing anything. This is what a peer calls
  during another node's repair, and the reason peers are asked through the scrubber rather than the
  replication server: the scan reads a whole segment, which must not run inside the replication
  loop.
  """
  @spec verify_segment(GenServer.server(), Metadata.segment_id(), timeout()) ::
          {:ok, map()} | {:error, term()}
  def verify_segment(server, segment_id, timeout \\ @peer_timeout) do
    GenServer.call(server, {:verify_segment, segment_id}, timeout)
  end

  @doc "The segments this node currently knows to be damaged (a gauge for operators)."
  @spec damaged(GenServer.server()) :: [Metadata.segment_id()]
  def damaged(server), do: GenServer.call(server, :damaged)

  # --- server ---

  @impl true
  def init(opts) do
    registered_name = Keyword.get(opts, :name, __MODULE__)

    peer_scrubber = Keyword.get(opts, :peer_scrubber, &{registered_name, &1})

    state =
      Map.merge(PeriodicWorker.new(opts, :scrubber, @default_interval, :scrub_interval_ms), %{
        metadata_source: Keyword.fetch!(opts, :metadata_source),
        local_ref: Keyword.fetch!(opts, :local_ref),
        directory: Keyword.fetch!(opts, :directory),
        apply_command: Keyword.fetch!(opts, :apply_command),
        peer_scrubber: peer_scrubber,
        # The scrubber of one peer replica. By default it is the node's scrubber (`:peer_scrubber`); a test can tell
        # two replicas on the same node apart.
        peer_scrubber_for: Keyword.get(opts, :peer_scrubber_for, fn {_name, node} -> peer_scrubber.(node) end),
        segments_per_tick: max(1, Keyword.get(opts, :segments_per_tick, @default_segments_per_tick)),
        confirm_deadline_ms: Keyword.get(opts, :confirm_deadline_ms, @default_confirm_deadline_ms),
        # The clock the confirmation budget is measured with (milliseconds). Injectable so a test can spend the budget
        # between two reads deterministically.
        confirm_clock: Keyword.get(opts, :confirm_clock, &monotonic_ms/0),
        on_result: Keyword.get(opts, :on_result, &log_result/1),
        # Segments still to visit in this cycle; refilled from the metadata when it empties, so every
        # sealed copy is revisited on a fixed period instead of the walk restarting from the top.
        pending: [],
        damaged: MapSet.new(),
        # How far each damaged copy's source confirmation got on earlier ticks, by segment, so a confirmation that ran
        # out of its budget resumes where it stopped instead of starting over. Dropped once the copy is clean,
        # repaired, or unrepairable.
        confirm_progress: %{},
        # Rebuilt copies that were not sealed and whose latch did not land (the call did not answer in time). Each
        # pass tries the latch again before it scans, so such a copy is not left in service without a fence.
        unlatched: %{},
        # Consecutive deferrals of each repair, by segment. Cleared when the copy is clean, repaired or unrepairable.
        deferrals: %{},
        # How long the calls that decide the seal and the latch of a rebuilt copy wait for their answer
        # (see `bounded/2`).
        call_timeout: Keyword.get(opts, :call_timeout, @scrub_call_timeout),
        # The latch call of a rebuilt copy (see `latch_rebuilt/3`). A test replaces it to make a latch fail on demand.
        fail_copy: Keyword.get(opts, :fail_copy, &ReplicationServer.fail_copy/3),
        # The delete that takes a rebuilt copy away when its latch does not answer (see `latch_rebuilt/3`).
        delete: Keyword.get(opts, :delete, &ReplicationServer.delete/2),
        # The reference resolved for the pass currently running (see `:local_ref`).
        resolved_ref: nil,
        # Whether the pass currently running could not learn which of its copies are latched (see `repair/4`).
        latches_unknown?: false
      })

    PeriodicWorker.schedule(state)
    {:ok, state}
  end

  @impl true
  def handle_call(:scrub_now, _from, state) do
    {result, state} = run(state)
    {:reply, result, state}
  end

  def handle_call({:verify_segment, segment_id}, _from, state) do
    case latched(resolve_ref(state.local_ref), [segment_id]) do
      {:ok, failed} ->
        if segment_id in failed,
          do: {:reply, {:error, :failed_copy}, state},
          else: {:reply, verify(state, segment_id), state}

      # The asker deletes its own copy once this answers `{:ok, _}`, so an answer about a copy whose latch
      # nobody could read must not be one.
      :unknown ->
        {:reply, {:error, :latch_unknown}, state}
    end
  end

  def handle_call(:damaged, _from, state), do: {:reply, MapSet.to_list(state.damaged), state}

  def handle_call(message, _from, state), do: PeriodicWorker.unknown_call(state, message)

  # Nothing casts to this server; without this clause, `use GenServer` would stop it on the first cast.
  @impl true
  def handle_cast(message, state), do: PeriodicWorker.unknown_cast(state, message)

  @impl true
  def handle_info(:tick, state), do: PeriodicWorker.tick(state, fn state -> state |> run() |> elem(1) end)

  # A worker meant to run for the lifetime of the node must not die on a message it did not plan for:
  # the crash costs the cycle position and the damaged set, so the restarted walk begins again at the
  # first segment and the tail of the rotation is never reached on a busy node. Nothing here is known
  # to send one, and that is the point: an unexpected message is a fact worth surfacing, not a reason to
  # take the process down (see `Malachi.UnexpectedMessage`).
  def handle_info(message, state), do: PeriodicWorker.unknown_info(state, message)

  # --- internals ---

  defp resolve_ref(local_ref) when is_function(local_ref, 0), do: local_ref.()
  defp resolve_ref(local_ref), do: local_ref

  defp run(state) do
    state = %{state | resolved_ref: resolve_ref(state.local_ref)}

    case take_segments(state) do
      {:error, reason} -> {skipped_result(reason), retry_unlatched(state)}
      {segments, state} -> scrub_batch(segments, state)
    end
  end

  # A pass that could not read the metadata is a pass that did not happen: reported, not crashed. The
  # cycle position and the damaged set are worth more than the tick (see `PeriodicWorker.ask/1`).
  defp skipped_result(reason) do
    Logger.warning(I18n.t(:scrub_metadata_unavailable, reason: inspect(reason)))
    %{empty_result() | unrepairable: [{:metadata, reason}]}
  end

  defp scrub_batch(segments, state) do
    state = retry_unlatched(state)

    {failed, state} =
      case latched(state.resolved_ref, Enum.map(segments, & &1.id)) do
        {:ok, failed} -> {failed, %{state | latches_unknown?: false}}
        :unknown -> {MapSet.new(), %{state | latches_unknown?: true}}
      end

    {result, state} =
      Enum.reduce(segments, {empty_result(), state}, fn segment, {acc, acc_state} ->
        if segment.id in failed do
          {%{acc | skipped: [segment.id | acc.skipped]}, acc_state}
        else
          scrub_segment(segment, acc, acc_state)
        end
      end)

    result = finalize(result)
    emit_progress(result)
    state.on_result.(result)
    {result, state}
  end

  # Pops the next batch off the cycle, refilling from the current metadata when the cycle is done.
  # Refilling reads the metadata fresh, so segments sealed since the last cycle join the rotation
  # and deleted ones drop out of it.
  defp take_segments(%{pending: []} = state) do
    case sealed_segments_here(state) do
      {:error, reason} -> {:error, reason}
      {:ok, []} -> {[], state}
      {:ok, segments} -> take_segments(%{state | pending: segments})
    end
  end

  defp take_segments(state) do
    {batch, rest} = Enum.split(state.pending, state.segments_per_tick)
    {batch, %{state | pending: rest}}
  end

  defp sealed_segments_here(state) do
    with {:ok, metadata} <- PeriodicWorker.ask(state.metadata_source) do
      sealed =
        metadata.segments
        |> Map.values()
        |> Enum.filter(&(&1.state == :sealed and state.resolved_ref in &1.replica_set))
        |> Enum.sort_by(& &1.id)

      {:ok, sealed}
    end
  end

  defp scrub_segment(segment, acc, state) do
    case verify(state, segment.id) do
      {:ok, counts} ->
        cross_check(segment, counts, acc, state)

      # Retention removed the segment mid-cycle, or this node never stored it: not damage.
      {:error, :enoent} ->
        {acc, forget_damage(state, segment.id)}

      {:error, verdict} ->
        report_damage(segment, verdict, acc, state)
    end
  end

  # The checksums pass, but the control plane recorded what the segment held when it was sealed, so
  # a copy with fewer records (or bytes) than that is damaged in a way no checksum can see: whole
  # frames are missing from the end.
  defp cross_check(segment, counts, acc, state) do
    if short_copy?(segment, counts) do
      verdict = %{reason: :short_copy, position: counts.bytes, unreadable_bytes: 0, sealed?: true}
      report_damage(segment, verdict, acc, state)
    else
      {%{acc | verified: [segment.id | acc.verified]}, forget_damage(state, segment.id)}
    end
  end

  # Applied to this node's copy to classify it, and to a peer's copy to decide whether it can be a
  # repair source. One predicate for both on purpose: a peer we would call damaged if it were ours is
  # not a copy worth deleting ours for. `length` and `byte_size` are only set at seal time, so an
  # unset one simply does not constrain (`register_segment` leaves them nil until the seal lands).
  defp short_copy?(segment, counts) do
    (is_integer(segment.length) and counts.records < segment.length) or
      (is_integer(segment.byte_size) and counts.bytes < segment.byte_size)
  end

  defp report_damage(segment, verdict, acc, state) do
    # The scrub only ever walks segments the control plane sealed, so damage found here is always
    # damage to an immutable copy, whatever the local file's seal marker says (a replica's marker is
    # written only when its own log rolls, so it is usually absent).
    verdict = Map.put(verdict, :sealed?, true)
    Telemetry.storage_integrity(verdict, segment.id, :scrub)
    state = %{state | damaged: MapSet.put(state.damaged, segment.id)}
    acc = %{acc | damaged: [{segment.id, verdict} | acc.damaged]}

    repair(segment, verdict, acc, state)
  end

  # A damaged sparse index is repaired without leaving the node and without touching the segment: the
  # index is DERIVED from the records, so nothing original was lost and no replica can offer anything
  # this node does not already hold. That makes it the one damage shape with no peer, no demotion and
  # no deletion, which is also why it must never be confused with damage to the segment itself.
  defp repair(segment, %{reason: :bad_index}, acc, state), do: rebuild_index(segment, acc, state)

  # A repair from a peer deletes this copy before refetching it, which is only safe on a copy known not to
  # be latched: a latched one is SelfHealing's to replace, and refetching it onto the disk that failed is the
  # loop the latch check exists to stop. Unknown is not "not latched", so the damage stays reported and the
  # copy stays on disk until a pass that can read the latches.
  defp repair(segment, _segment_damage, acc, %{latches_unknown?: true} = state) do
    {%{acc | unrepairable: [{segment.id, :latch_unknown} | acc.unrepairable]}, state}
  end

  defp repair(segment, _segment_damage, acc, state), do: repair_from_peer(segment, acc, state)

  defp rebuild_index(segment, acc, state) do
    directory = Layout.segment_directory(state.directory, segment.id)

    case Log.rebuild_index(directory) do
      :ok ->
        {%{acc | repaired: [segment.id | acc.repaired]}, forget_damage(state, segment.id)}

      {:error, reason} ->
        {%{acc | unrepairable: [{segment.id, {:rebuild_index, reason}} | acc.unrepairable]}, state}
    end
  end

  # The repair is a distributed operation (a peer scan, a control-plane command and a cross-node
  # copy), which is why it gets a span while the local scan does not: this is where the latency and
  # the failure modes are worth tracing.
  defp repair_from_peer(segment, acc, state) do
    Tracer.with_span "malachi.scrub.repair" do
      Tracer.set_attributes(%{
        "malachi.segment" => inspect(segment.id),
        "malachi.replicas" => length(segment.replica_set)
      })

      case intact_peer(segment, state) do
        nil ->
          Tracer.set_attributes(%{"malachi.repair" => "no_intact_copy"})
          {%{acc | unrepairable: [{segment.id, :no_intact_copy} | acc.unrepairable]}, state}

        peer ->
          run_repair(segment, peer, acc, state)
      end
    end
  end

  # The safety rule: a peer must confirm it holds the WHOLE segment, intact, BEFORE the local copy is
  # deleted. An unreachable, damaged or short peer is simply not a source.
  defp intact_peer(segment, state) do
    segment.replica_set
    |> Enum.reject(&(&1 == state.resolved_ref))
    |> Enum.find(fn replica ->
      case peer_verify(state, replica, segment.id) do
        {:ok, counts} -> not short_copy?(segment, counts)
        {:error, _reason} -> false
      end
    end)
  end

  defp peer_verify(state, {_name, _node} = replica, segment_id) do
    verify_segment(state.peer_scrubber_for.(replica), segment_id)
  catch
    :exit, _reason -> {:error, :unreachable}
  end

  defp peer_verify(_state, _replica, _segment_id), do: {:error, :not_addressable}

  defp run_repair(segment, peer, acc, state) do
    if (segment.length || 0) == 0 do
      # Nothing to rebuild from, and the refetch's verify would find no directory once the copy is deleted, so
      # the copy is kept as it is and reported.
      unrepairable(acc, state, segment.id, :empty_segment)
    else
      case confirm_source(segment, peer, state) do
        :ok ->
          repair_from(segment, peer, acc, drop_progress(state, segment.id))

        # A peer that did not answer in time, or a confirmation that ran out of its budget, says nothing about
        # the peer's bytes. The copy stays as it is, still damaged, and the confirmation resumes on a later tick
        # from the offset it reached, against the same peer (progress is keyed by peer, not only by segment).
        {:deferred, offset} ->
          deferred(
            acc,
            %{state | confirm_progress: Map.put(state.confirm_progress, {segment.id, peer}, offset)},
            segment
          )

        {:error, reason} ->
          unrepairable(acc, state, segment.id, {:source_read, reason})
      end
    end
  end

  # A deferred repair goes to the back of the cycle, so the segments behind it are visited first. Its next visit
  # compares against the replica set that holds then, re-read from the metadata when that read answers; when it does
  # not, the segment goes back as it was. A segment the metadata no longer has drops out of the rotation. After
  # `@max_consecutive_deferrals` deferrals in a row it leaves the rotation until the cycle refills.
  defp deferred(acc, state, segment) do
    Tracer.set_attributes(%{"malachi.repair" => "deferred"})
    count = Map.get(state.deferrals, segment.id, 0) + 1

    if count >= @max_consecutive_deferrals do
      {acc, %{state | deferrals: Map.delete(state.deferrals, segment.id)}}
    else
      {acc, requeue(%{state | deferrals: Map.put(state.deferrals, segment.id, count)}, segment)}
    end
  end

  defp requeue(state, segment) do
    case bounded(state.metadata_source) do
      {:ok, metadata} ->
        case Metadata.get_segment(metadata, segment.id) do
          nil -> forget_segment(state, segment.id)
          fresh -> readd(state, fresh)
        end

      _timed_out_or_exited ->
        readd(state, segment)
    end
  end

  # A segment this node is no longer in is not put back: its copy here is no longer this node's to repair.
  defp readd(state, segment) do
    if state.resolved_ref in segment.replica_set,
      do: %{state | pending: state.pending ++ [segment]},
      else: forget_segment(state, segment.id)
  end

  defp unrepairable(acc, state, segment_id, reason) do
    Tracer.set_attributes(%{"malachi.repair" => "failed"})
    state = drop_progress(%{state | deferrals: Map.delete(state.deferrals, segment_id)}, segment_id)
    {%{acc | unrepairable: [{segment_id, reason} | acc.unrepairable]}, state}
  end

  defp monotonic_ms, do: System.monotonic_time(:millisecond)

  # A segment the metadata no longer has is out of the cycle for good: nothing else may keep it as damaged or deferred.
  defp forget_segment(state, segment_id) do
    drop_progress(
      %{state | damaged: MapSet.delete(state.damaged, segment_id), deferrals: Map.delete(state.deferrals, segment_id)},
      segment_id
    )
  end

  # Progress is kept per {segment, peer}: a confirmation resumes only against the peer it was earned against.
  defp drop_progress(state, segment_id) do
    %{state | confirm_progress: Map.reject(state.confirm_progress, fn {{id, _peer}, _offset} -> id == segment_id end)}
  end

  # `intact_peer/2` checks checksums and counts only, and never opens the peer's log. So the copy is deleted
  # only once the peer has been read through, batch by batch, across the whole range the refetch will rebuild:
  # that read goes down the same path the refetch takes. Each read is bounded by one second and by the time left in
  # the whole confirmation's budget (`:confirm_deadline_ms`), since this runs on the scrub loop.
  @confirm_batch 256
  @confirm_timeout 1_000

  # Resumes from the offset an earlier tick confirmed up to. The peer is still verified whole on every tick
  # (`intact_peer/2`), so the progress only skips reads, not the checks that make the copy trustworthy.
  defp confirm_source(segment, peer, state) do
    to_offset = segment.start_offset + segment.length
    from = Map.get(state.confirm_progress, {segment.id, peer}, segment.start_offset)
    deadline = state.confirm_clock.() + state.confirm_deadline_ms
    confirm_range(peer, segment.id, from, to_offset, deadline, state.confirm_clock)
  end

  defp confirm_range(_peer, _segment_id, offset, to_offset, _deadline, _clock) when offset >= to_offset, do: :ok

  defp confirm_range(peer, segment_id, offset, to_offset, deadline, clock) do
    now = clock.()

    if now >= deadline do
      {:deferred, offset}
    else
      # Each read gets the time left in the budget, capped at one read's bound, so a read started just before the
      # deadline cannot run past it by a whole bound.
      timeout = min(@confirm_timeout, deadline - now)

      case ReplicationServer.read(peer, segment_id, offset, @confirm_batch, timeout) do
        {:ok, [_record | _] = records} ->
          confirm_range(peer, segment_id, offset + length(records), to_offset, deadline, clock)

        {:error, :unreachable} ->
          {:deferred, offset}

        other ->
          {:error, other}
      end
    end
  end

  defp repair_from(segment, peer, acc, state) do
    case demote_if_primary(segment, state) do
      {:ok, state} -> delete_and_refetch(segment, peer, acc, state)
      :deferred -> deferred(acc, state, segment)
      # The control plane no longer has the segment, so there is nothing to repair and nothing to report.
      :gone -> {acc, forget_segment(state, segment.id)}
      {:error, reason} -> unrepairable(acc, state, segment.id, reason)
    end
  end

  # The latch is read again here, not only at the start of the batch: the confirmation before this can run for up to
  # its deadline, and a copy latched during that time must not be deleted, since the delete clears the latch and the
  # refetch would then write the copy back onto the disk that failed.
  # The check and the delete are two calls to the same server, so a latch set between them is still cleared by the
  # delete. That window is one mailbox hop wide, and the refetch rebuilds from a peer that was just confirmed, so it
  # is accepted rather than closed: a single conditional delete would close it at the cost of a new server call shape.
  defp delete_and_refetch(segment, peer, acc, state) do
    answer =
      case bounded(fn -> latched(state.resolved_ref, [segment.id]) end) do
        {:ok, answer} -> answer
        _no_answer -> :unknown
      end

    case answer do
      {:ok, failed} ->
        if MapSet.member?(failed, segment.id) do
          unrepairable(acc, state, segment.id, :latched_before_delete)
        else
          delete_then_refetch(segment, peer, acc, state)
        end

      :unknown ->
        unrepairable(acc, state, segment.id, :latch_unknown)
    end
  end

  defp delete_then_refetch(segment, peer, acc, state) do
    to_offset = segment.start_offset + (segment.length || 0)

    # The point of no return: past this line the damaged bytes are gone and only the peer can put them
    # back. That is what makes `intact_peer/2` strict about what counts as a source, and why a failure
    # after it is reported apart from one decided before it, which left the copy untouched.
    #
    # The one answer that is not past that line is `{:error, :unreachable}`: this node's own
    # replication server is down or restarting, so nothing was deleted and the damaged copy is still
    # whole. Matching `:ok` here would turn that into a `MatchError` that takes the whole pass with it,
    # losing the cycle position and the damaged set over a process that will be back on the next tick.
    case ReplicationServer.delete(state.resolved_ref, segment.id) do
      :ok ->
        # The copy this pending latch was for is gone, and the copy rebuilt in its place is decided on its own: a
        # latch left over from the old copy must not be applied to the new one.
        refetch(segment, peer, to_offset, acc, %{state | unlatched: Map.delete(state.unlatched, segment.id)})

      {:error, reason} ->
        Tracer.set_attributes(%{"malachi.repair" => "failed"})
        {%{acc | unrepairable: [{segment.id, {:delete, reason}} | acc.unrepairable]}, state}
    end
  end

  defp refetch(segment, peer, to_offset, acc, state) do
    # Reaching `to_offset` is itself the completeness check: the catchup copied every offset the seal
    # recorded, so the verify only has to confirm the frames landed readable.
    # A rebuilt copy comes back without its seal marker, so it is sealed again here, at the length the control
    # plane recorded: the marker is what fences the copy, and a copy that is not fenced takes writes it must refuse.
    with {:ok, ^to_offset} <- Catchup.run(state.resolved_ref, peer, segment.id, segment.start_offset, to_offset),
         {:ok, _counts} <- verify(state, segment.id) do
      case seal_rebuilt(state, segment, to_offset) do
        :ok ->
          Tracer.set_attributes(%{"malachi.repair" => "ok"})
          {%{acc | repaired: [segment.id | acc.repaired]}, forget_damage(state, segment.id)}

        {:error, reason, state} ->
          Tracer.set_attributes(%{"malachi.repair" => "failed"})
          {%{acc | unrepairable: [{segment.id, {:refetch, {:error, reason}}} | acc.unrepairable]}, state}
      end
    else
      other ->
        # The local copy was deleted before this point, so whatever is left of it is not fenced. It is latched here
        # too, so healing replaces it. A latch that does not answer in time takes the copy away instead (see
        # `latch_rebuilt/3`), so nothing unfenced stays in service here; only a delete that does not answer either
        # leaves the latch pending.
        Tracer.set_attributes(%{"malachi.repair" => "failed"})

        {%{acc | unrepairable: [{segment.id, {:refetch, other}} | acc.unrepairable]},
         latch_rebuilt(state, segment.id, {:rebuilt_unsealed, other})}
    end
  end

  # A copy that cannot be sealed is latched on its replica server, so healing replaces it. The latch is attempted
  # whatever the failure, so the seal is not retried. Both calls run through `bounded/2`, since this runs on the scrub
  # loop. A latch that does not answer in time takes the copy away instead (see `latch_rebuilt/3`), and only a
  # delete that does not answer either leaves the latch pending, to be tried again on each later pass (see
  # `retry_unlatched/1`). A seal that times out can still land on the replica server after this call has given up,
  # and the latch that follows then latches a sealed copy too. That is a safe over-latch: the copy is replaced and
  # re-replicated, and no data is lost.
  defp seal_rebuilt(state, segment, to_offset) do
    answer =
      case bounded(
             fn -> ReplicationServer.seal(state.resolved_ref, segment.id, segment.start_offset) end,
             state.call_timeout
           ) do
        {:ok, reply} -> reply
        no_answer -> no_answer
      end

    case answer do
      {:ok, ^to_offset, _bytes} ->
        :ok

      other ->
        {:error, {:seal, other}, latch_rebuilt(state, segment.id, {:rebuilt_unsealed, other})}
    end
  end

  # A latch that does not answer leaves the rebuilt copy in service unfenced, and a restart would lose the pending
  # entry. So the copy is taken away instead: nothing unfenced stays here, and the next pass repairs the segment again.
  # Only when that delete does not answer either is the latch kept pending.
  defp latch_rebuilt(state, segment_id, reason) do
    case bounded(fn -> state.fail_copy.(state.resolved_ref, segment_id, reason) end, state.call_timeout) do
      {:ok, :ok} ->
        %{state | unlatched: Map.delete(state.unlatched, segment_id)}

      _no_answer ->
        case bounded(fn -> state.delete.(state.resolved_ref, segment_id) end, state.call_timeout) do
          {:ok, :ok} -> %{state | unlatched: Map.delete(state.unlatched, segment_id)}
          _no_answer -> %{state | unlatched: Map.put(state.unlatched, segment_id, reason)}
        end
    end
  end

  # Tries the latch of every rebuilt copy that did not get it, before the pass scans anything. The reason is the one
  # the copy was rebuilt under, so the retry records the same cause. A copy this node no longer holds (retention or
  # healing removed it) has nothing to latch, so its entry is dropped.
  defp retry_unlatched(state) do
    Enum.reduce(state.unlatched, state, fn {segment_id, reason}, acc ->
      case File.stat(Layout.segment_directory(acc.directory, segment_id)) do
        {:error, reason} when reason in [:enoent, :enotdir] -> %{acc | unlatched: Map.delete(acc.unlatched, segment_id)}
        _present_or_unknown -> latch_pending(acc, segment_id, reason)
      end
    end)
  end

  # The retry of a pending latch only latches: it never deletes, since the entry does not say which copy it was for. The
  # entry is dropped once the latch lands.
  defp latch_pending(state, segment_id, reason) do
    case bounded(fn -> state.fail_copy.(state.resolved_ref, segment_id, reason) end, state.call_timeout) do
      {:ok, :ok} -> %{state | unlatched: Map.delete(state.unlatched, segment_id)}
      _no_answer -> state
    end
  end

  # Reads follow the head of the replica set, so moving this node to the end takes the damaged copy
  # out of the read path before it is deleted and refetched. The set itself is unchanged, so the
  # segment is never under-replicated by the repair. An error answer stops the repair before the delete:
  # while this node may still be the head, deleting its copy would leave reads answering end-of-log.
  # The demote is a compare-and-swap: the command names the set it was decided against, and the control plane
  # applies it only while that is still the set it holds. A set change committed since the pass took its snapshot
  # (a heal, a failover) therefore refuses the demote, which defers the repair to a later pass that sees the new set.
  defp demote_if_primary(segment, state) do
    case current_replica_set(segment, state) do
      {:ok, replica_set} when replica_set == segment.replica_set -> demote_from(segment, state)
      _moved_or_unknown -> :deferred
    end
  end

  defp current_replica_set(segment, state) do
    case bounded(state.metadata_source) do
      {:ok, metadata} ->
        case Metadata.get_segment(metadata, segment.id) do
          %{replica_set: replica_set} -> {:ok, replica_set}
          nil -> :unknown
        end

      _timed_out_or_exited ->
        :unknown
    end
  end

  # The scrub loop bounds the calls that decide a repair (REVIEW.md): the call runs in a task, and an answer that does
  # not come within the timeout is given up on. That covers the seal and the latch of a rebuilt copy, the demote and
  # the metadata read, and the latch lookup. Three calls keep their own longer timeouts on the loop, so the loop can
  # still wait on them that long: the peer's verification scan (`@peer_timeout`), and the copy catch-up (`Catchup.run`)
  # with the delete, which use ReplicationServer's five-second default. Bounding them is a follow-up.
  # The task answers exits itself. A raise inside the bounded function is not caught here and would take the loop
  # down with the task (the task is linked to it): the functions passed in today are GenServer calls, which raise
  # nothing, so no raise is reachable through this wrapper now.
  defp bounded(fun, timeout \\ @scrub_call_timeout) do
    task =
      Task.async(fn ->
        try do
          {:ok, fun.()}
        catch
          :exit, reason -> {:exit, reason}
        end
      end)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, {:ok, result}} -> {:ok, result}
      {:ok, {:exit, reason}} -> {:exit, reason}
      _no_answer -> :timeout
    end
  end

  defp demote_from(segment, state) do
    others = Enum.reject(segment.replica_set, &(&1 == state.resolved_ref))

    case segment.replica_set do
      # Only worth a control-plane command when there is somewhere else for reads to go: at rf=1 the
      # demoted set would be the same single-element list.
      [primary | _rest] when primary == state.resolved_ref and others != [] ->
        # `segment.replica_set` is the set this demote is decided against: the compare-and-swap's expected value.
        command = {:set_segment_replicas, segment.id, others ++ [state.resolved_ref], segment.replica_set}

        # Only `:ok` is a demote that took. A set that moved since the snapshot, or a call that exits (the control plane
        # did not answer in time), defers the repair. A command that timed out may still commit, so the head may or may
        # not have moved; a later pass decides again against the current replica set. Any other answer stops the repair
        # the same way an error does.
        case demote_answer(state, command) do
          :ok -> {:ok, state}
          :deferred -> :deferred
          {:error, :replica_set_changed} -> :deferred
          {:error, :no_such_segment} -> :gone
          {:error, reason} -> {:error, {:demote, reason}}
          other -> {:error, {:demote, other}}
        end

      _not_primary_or_alone ->
        {:ok, state}
    end
  end

  defp demote_answer(state, command) do
    case bounded(fn -> state.apply_command.(command) end) do
      {:ok, answer} -> answer
      _timed_out_or_exited -> :deferred
    end
  end

  defp verify(state, segment_id) do
    state.directory
    |> Layout.segment_directory(segment_id)
    |> Log.verify()
  end

  # A copy that is clean or repaired forgets its confirmation progress too: progress is only valid while the same
  # copy stays damaged, since a later damage must be confirmed from the start.
  defp forget_damage(state, segment_id) do
    drop_progress(
      %{state | damaged: MapSet.delete(state.damaged, segment_id), deferrals: Map.delete(state.deferrals, segment_id)},
      segment_id
    )
  end

  # Which of `segment_ids` this node's replication server has latched as failed, or `:unknown` when it cannot
  # say. Never an empty set on an error: an empty set reads as "none latched", which is what lets a latched
  # copy be deleted by a repair or serve as another node's repair source (see the moduledoc).
  defp latched(_ref, []), do: {:ok, MapSet.new()}

  defp latched(ref, segment_ids) do
    case bounded(fn -> ReplicationServer.failed_segments(ref, segment_ids) end) do
      {:ok, {:ok, failed}} -> {:ok, failed}
      _no_answer_or_error -> :unknown
    end
  end

  defp empty_result, do: %{verified: [], damaged: [], repaired: [], unrepairable: [], skipped: []}

  defp finalize(result) do
    %{
      verified: Enum.reverse(result.verified),
      damaged: Enum.reverse(result.damaged),
      repaired: Enum.reverse(result.repaired),
      unrepairable: Enum.reverse(result.unrepairable),
      skipped: Enum.reverse(result.skipped)
    }
  end

  defp emit_progress(result) do
    Telemetry.scrub_pass(
      length(result.verified),
      length(result.damaged),
      length(result.repaired),
      length(result.unrepairable)
    )
  end

  defp log_result(%{damaged: [], unrepairable: []}), do: :ok

  defp log_result(result) do
    for {segment_id, verdict} <- result.damaged do
      Logger.warning(
        I18n.t(:scrub_segment_damaged,
          segment_id: inspect(segment_id),
          reason: verdict.reason,
          position: verdict.position,
          outcome: repair_outcome(result, segment_id)
        )
      )
    end

    :ok
  end

  defp repair_outcome(result, segment_id) do
    cond do
      segment_id in result.repaired ->
        I18n.t(:scrub_repair_succeeded)

      entry = List.keyfind(result.unrepairable, segment_id, 0) ->
        not_repaired(elem(entry, 1))

      true ->
        ""
    end
  end

  # The two failures need different operator action, so they do not share a message: everything up to
  # the refetch leaves the damaged bytes on disk to salvage by hand, while a failure during it leaves
  # a copy that is merely incomplete, which a later pass finds as short and repairs again on its own.
  defp not_repaired({:refetch, reason}) do
    I18n.t(:scrub_repair_failed_refetch, reason: inspect(reason))
  end

  defp not_repaired(reason) do
    I18n.t(:scrub_repair_not_done, reason: inspect(reason))
  end
end
