defmodule Malachi.Cluster.HealCoordinator do
  @moduledoc """
  Drives reactive self-healing: periodically (and on demand) it re-replicates under-replicated
  **sealed** segments to the currently live brokers, closing the loop *broker dies → membership
  marks it gone → its segments are healed*.

  It is decoupled from where membership and metadata actually live, via injected seams, so it can
  be tested in-process and wired to the real `Malachi.Cluster.MembershipServer` and control plane
  later without change:

    * `:live_brokers` - `(-> [broker])`, the currently alive broker set (e.g. from
      `Malachi.Cluster.MembershipServer.alive_members/1`);
    * `:metadata_source` - `(-> Malachi.Metadata.t())`, the current metadata;
    * `:apply_command` - `(Malachi.Metadata.command() -> any)`, applies a `:set_segment_replicas`
      command to the control plane;
    * `:replication_factor` - the target replica count;
    * `:interval` - the healing period in ms (default 5000). No environment variable sets it, which is
      why the warning about a value that cannot be a period names the option rather than a setting an
      operator could look for;
    * `:leader?` - `(-> boolean())`, whether this node should heal this pass (default always). Only the
      cluster's membership leader heals, so N nodes do not redo the same work (1C); a non-leader still
      ticks but skips the pass. `heal_now/1` is a manual trigger and always runs;
    * `:heal_opts` - forwarded to `Malachi.Cluster.SelfHealing.heal_sealed/4` (e.g. `:batch_size`).

  Each pass **reconciles** against the live set: it runs `Malachi.Cluster.SelfHealing.heal_sealed/4`
  (re-replicating under-replicated sealed segments, backfilling via `Malachi.Cluster.Catchup`) and
  `Malachi.Cluster.Failover.plan/5` (sealing active segments whose primary died, that have a copy that
  failed in storage, or whose live replicas fall below the acknowledgement quorum, so writing rolls to a
  fresh segment), and hands the resulting commands to the control
  plane, seals first, stopping at the first one the broker does not answer. The rest wait for the next pass,
  which plans them again, except a failover seal whose reason is gone by then (see `apply_commands/2`'s
  comment, and #269). Membership is a view, so before fencing for failover the pass asks, read-only, each
  broker the view calls gone that a candidate needs, and leaves alone a segment that is no candidate once
  those that answered are counted: a suspicion is not a failure. The exception is a segment an earlier
  pass already fenced a follower of for a seal that did not land, which is kept so the seal is retried.
  `heal_now/1` runs one pass
  synchronously and returns the combined result, for tests and manual triggers; its `applied` is what the pass
  planned, as in `Malachi.Cluster.SelfHealing`, not what reached the control plane.

  Failover needs to know what each surviving replica holds, which no pure function can answer, so this
  pass does the probing, in two steps whose order carries the safety. `Failover.candidates/3` names the
  segments; each live replica is MEASURED (`:probe`), which leaves it writable; and only once those
  answers reach the seal quorum (`Failover.seal_quorum/1`) is each answering replica FENCED (`:fence`),
  which is what makes its answer final. The fence answers go to `Failover.plan/5`, which applies the
  same rule again to them, so a fence that fails on enough replicas still declines rather than sealing on
  too few.

  Fencing before knowing whether enough answered would close replicas of a segment the pass then
  declines to seal, and nothing unseals a store: see `Malachi.Cluster.Failover`'s moduledoc. A replica
  that does not answer in time simply does not count, which is what leaves a segment below the seal
  quorum unsealed and its range blocked; that case is logged every pass, because a blocked range that
  says nothing is the failure mode worth avoiding.

    * `:probe` - `((replica, segment_id, base_offset) -> {end_offset, byte_size} | :error)`, how a
      replica is MEASURED (default `Malachi.Cluster.ReplicationServer.durable_stats/4` with a short
      timeout, so an unreachable replica cannot stall the pass);
    * `:fence` - the same shape, how a replica is CLOSED once the seal quorum has answered (default
      `Malachi.Cluster.ReplicationServer.seal/4`). Separate from `:probe` so a test can watch a pass
      measure without fencing, which is the property that must hold below the seal quorum;
    * `:seal_state` - `((replica, [{segment_id, base_offset}]) -> %{segment_id => {end_offset, byte_size}})`,
      which of a replica's segments are ALREADY fenced (default
      `Malachi.Cluster.ReplicationServer.fenced_segments/3`, answering `%{}` on any error). See the
      orphaned-fence pass below; the failover pass also asks it, batched per follower, about the few
      candidates a suspected broker's answer would otherwise drop;
    * `:failed_state` - `((replica, [segment_id]) -> MapSet.t(segment_id))`, which of a replica's
      segments have a copy that FAILED there in storage (default
      `Malachi.Cluster.ReplicationServer.failed_segments/3`, answering an empty set on any error). An
      active one becomes a failover candidate (see "A copy that failed" in `Malachi.Cluster.Failover`); a
      sealed one is a lost replica `Malachi.Cluster.SelfHealing` replaces on another broker;
    * `:discard_copy` - `((replica, segment_id) -> any)`, how a failed copy that was replaced is removed
      from the broker it failed on (default `Malachi.Cluster.ReplicationServer.delete/2`, which also clears
      the latch there). Called only once the control plane shows the copy gone from the replica set: a
      copy deleted while still listed would answer a read with nothing rather than an error, and a read
      that finds nothing takes the range as drained;
    * `:probe_timeout` - ms for the four probing defaults (default 1000).

  ## The orphaned-fence pass

  Alongside failover, each pass reconciles the state where a segment's store is FENCED while the
  control plane still calls it active, which stops its range from taking any write and which nothing
  else converges (`Malachi.Cluster.OrphanedFence` documents how it arises and why it is terminal).
  `OrphanedFence.candidates/2` names the segments, `:seal_state` asks each primary which of them are
  fenced, and `OrphanedFence.plan/3` turns the answers into seal commands applied like every other.

  Three seams rather than two, because this one differs from `:probe` on both axes that matter. It is
  asked about EVERY active segment on every pass rather than a handful of failover candidates, so it is
  batched per primary and its default answers from a marker check rather than opening and flushing each
  log. And like `:probe` it must never fence: this pass visits the whole workload, so a probe that
  fenced here would close a replica of every range it asked about, and nothing unseals a store.

  ## The settling pass

  The other half of the same split, on SEALED segments: the control plane records a length and a copy
  on disk does not agree with it (`Malachi.Cluster.SealedOverrun` documents how a copy comes to hold
  more than the seal says, and why the recorded length is the one that wins). The copies to settle come
  from the integrity probe `Malachi.Cluster.SelfHealing` already runs over every sealed segment, as its
  `:unsettled` list, so this pass asks nothing of its own; `SealedOverrun.plan/3` turns that list into
  `seal_at` calls and the `:settle_copy` seam makes them.

    * `:settle_copy` - `((replica, segment_id, base_offset, end_offset) -> {:ok, dropped} | :error)`,
      how one copy is brought down to the recorded length and fenced there (default
      `Malachi.Cluster.ReplicationServer.seal_at/5`). Unlike `:fence` it takes the length, because the
      whole point is that the number is the control plane's and not the replica's;
    * `:settle_timeout` - ms for that call (default 5000, not `:probe_timeout`). It is the one seam
      here that does real work: it opens the log, verifies the records it keeps and fsyncs. A probe's
      one second would time out on a large segment and have the pass retry work it had already done;
    * `:settle_batch_size` - copies settled per pass (default 64). The FIRST pass after this ships
      finds every sealed copy in the cluster unsettled at once, and an unbounded pass would put that
      whole scan and fsync load on the disk the produce path is using. Separate from `:heal_opts`'
      `:batch_size`, which bounds how much a backfill copies, because the two answer different
      questions and tying them would make one of them unexplainable.

  Fencing here is safe, unlike fencing while probing an active segment: the control plane has already
  sealed the segment, so the pass has no decision left to decline and there is no copy it can close
  that it might have wanted open. `SealedOverrun`'s moduledoc carries the full argument.
  """

  use GenServer

  require Logger

  alias Malachi.Cluster.Failover
  alias Malachi.Cluster.OrphanedFence
  alias Malachi.Cluster.PeriodicWorker
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.SealedOverrun
  alias Malachi.Cluster.SelfHealing
  alias Malachi.I18n
  alias Malachi.Telemetry

  @default_interval 5_000

  # Copies settled per pass. Small on purpose: each one opens a log, verifies every record it keeps and
  # fsyncs, and the first pass after this ships finds every sealed copy in the cluster at once.
  @default_settle_batch_size 64

  @doc "Starts the coordinator. See the module doc for required options."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_server_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @doc "Runs one healing pass synchronously and returns the `SelfHealing.heal_sealed/4` result."
  @spec heal_now(GenServer.server()) :: SelfHealing.result()
  def heal_now(server), do: GenServer.call(server, :heal_now)

  # --- server ---

  @impl true
  def init(opts) do
    state =
      Map.merge(PeriodicWorker.new(opts, :heal, @default_interval, :heal_coordinator_interval), %{
        live_brokers: Keyword.fetch!(opts, :live_brokers),
        metadata_source: Keyword.fetch!(opts, :metadata_source),
        apply_command: Keyword.fetch!(opts, :apply_command),
        replication_factor: Keyword.fetch!(opts, :replication_factor),
        leader?: Keyword.get(opts, :leader?, fn -> true end),
        heal_opts: Keyword.get(opts, :heal_opts, []),
        # `(-> {attribute_key, attributes} | nil)`: the current spread for rack/DC-aware re-replication,
        # resolved per pass so it tracks live membership. Default: no spread.
        spread: Keyword.get(opts, :spread, fn -> nil end),
        # Two seams, not one, because the two calls differ in consequence: `:probe` measures and leaves
        # the replica writable, `:fence` closes it. Injectable separately so a test can watch a pass
        # measure without fencing, which is exactly the case that must hold below the seal quorum.
        probe: Keyword.get(opts, :probe, default_probe(Keyword.get(opts, :probe_timeout, 1_000))),
        fence: Keyword.get(opts, :fence, default_fence(Keyword.get(opts, :probe_timeout, 1_000))),
        # The third seam. Read-only like `:probe`, batched per replica unlike either, and asked about every
        # active segment's primary (and a rescued failover candidate's followers). See the moduledoc.
        seal_state: Keyword.get(opts, :seal_state, default_seal_state(Keyword.get(opts, :probe_timeout, 1_000))),
        # The fourth seam, as cheap as `:seal_state` (a lookup, no disk) and batched per replica, because it is
        # asked of every live replica of every active segment.
        failed_state: Keyword.get(opts, :failed_state, default_failed_state(Keyword.get(opts, :probe_timeout, 1_000))),
        discard_copy: Keyword.get(opts, :discard_copy, &ReplicationServer.delete/2),
        # The fifth seam, and the only one here that WRITES. Its timeout and its batch are its own for
        # the reasons in the moduledoc: it opens and fsyncs where the others stat or look up a marker.
        settle_copy: Keyword.get(opts, :settle_copy, default_settle_copy(Keyword.get(opts, :settle_timeout, 5_000))),
        settle_batch_size: Keyword.get(opts, :settle_batch_size, @default_settle_batch_size)
      })

    PeriodicWorker.schedule(state)
    {:ok, state}
  end

  @impl true
  def handle_call(:heal_now, _from, state) do
    {result, state} = run(state)
    {:reply, result, state}
  end

  def handle_call(message, _from, state), do: PeriodicWorker.unknown_call(state, message)

  # Nothing casts to this server; without this clause, `use GenServer` would stop it on the first cast.
  @impl true
  def handle_cast(message, state), do: PeriodicWorker.unknown_cast(state, message)

  @impl true
  def handle_info(:tick, state), do: PeriodicWorker.tick(state, &heal_if_leader/1)

  def handle_info(message, state), do: PeriodicWorker.unknown_info(state, message)

  # The gate belongs to the tick alone: `heal_now/1` is a manual trigger and ignores it.
  defp heal_if_leader(state) do
    if state.leader?.(), do: state |> run() |> elem(1), else: state
  end

  # --- internals ---

  # A pass that could not read the metadata is a pass that did not happen: reported once per cause
  # (`PeriodicWorker.skip/3`), not crashed, as the scrubber does (see `PeriodicWorker.ask/1`). A broker busy
  # for longer than the call's timeout while other nodes restart is the ordinary case, and a crash would say
  # nothing an operator could act on. Only the node-wide coordinator's source can exit (a call to the
  # broker): a vnode's answers an empty metadata when its group cannot be read
  # (`Malachi.Application.vnode_metadata_source/1`), and that pass finds nothing to do.
  defp run(state) do
    case PeriodicWorker.ask(state.metadata_source) do
      {:ok, metadata} ->
        run(state, metadata)

      {:error, reason} ->
        {%{applied: [], failed: [], repaired: []}, PeriodicWorker.skip(state, :heal_metadata_unavailable, reason)}
    end
  end

  defp run(state, metadata) do
    live = state.live_brokers.()
    now_ms = System.system_time(:millisecond)

    # Asked first, because both halves below act on it: a failed copy of an active segment is a failover
    # candidate, and one of a sealed segment is a lost replica to replace.
    failed = probe_failures(state, metadata, live)
    heal_opts = state.heal_opts |> put_spread(state.spread.()) |> Keyword.put(:failed, failed)
    healed = SelfHealing.heal_sealed(metadata, live, state.replication_factor, heal_opts)
    seals = Failover.plan(metadata, live, probe_candidates(state, metadata, live, failed), now_ms, failed)
    # A segment this pass seals for failover may have a primary it just fenced, which the orphaned-fence
    # probe below would find fenced and seal a second time, counting a divergence that never existed.
    sealed_here = MapSet.new(for {:seal_segment, segment_id, _length, _bytes, _at} <- seals, do: segment_id)

    orphans =
      metadata
      |> OrphanedFence.plan(probe_fences(state, metadata, live), now_ms)
      |> Enum.reject(fn {:seal_segment, segment_id, _length, _bytes, _at} -> segment_id in sealed_here end)

    applied = healed.applied ++ seals ++ orphans
    state = apply_commands(state, seals ++ orphans ++ healed.applied)

    discard_replaced_copies(state, replaced_copies(healed.applied, failed))
    report_orphans(orphans, state)
    settle_sealed_copies(state, metadata, healed.unsettled)

    # A heal that cannot complete leaves the cluster under-replicated; the periodic tick used to
    # discard the result, making persistent failures invisible until something else broke.
    if healed.failed != [] do
      Logger.warning(I18n.t(:heal_repair_failed, count: length(healed.failed), failures: inspect(healed.failed)))
    end

    {%{applied: applied, failed: healed.failed, repaired: healed.repaired}, state}
  end

  # Handed to the broker one at a time, and stopped at the first that does not answer: a broker busy past the
  # call's timeout is the ordinary case `run/1` describes, and every command after it would wait on the same
  # broker. A command that timed out may still have landed, which is why what follows re-reads the control
  # plane rather than trusting this list.
  #
  # Seals are handed over first (`run/2`), because this pass has already fenced their replicas. A heal command
  # or an orphan seal that did not land is planned again by the next pass, but a failover seal is planned only
  # while its reason lasts: its primary dead, a copy latched as failed in storage, or its live replicas below
  # the acknowledgement quorum. One that did not land, whose reason is gone by the next pass (the primary
  # back in the view, the primary's failed copy unlatched by a restart of its replication server, or enough
  # replicas back), leaves the segment active with fenced followers and an unfenced primary, which neither
  # `Malachi.Cluster.Failover` nor `Malachi.Cluster.OrphanedFence` (it asks only the primary) plans again.
  # The one way back this pass itself takes: a primary the view still calls gone but that answers does not
  # drop a segment an earlier pass already fenced a follower of (`fenced_followers/3`). Going first keeps a slow heal
  # command from being what holds one back, and nothing more: the first command that does not answer stops
  # every command after it, so a failover seal can still be left behind by its own call, by an earlier seal,
  # or by the head move `Malachi.Cluster.Failover.plan/5` pairs with each seal of a segment before it (#269).
  defp apply_commands(state, commands) do
    unanswered =
      Enum.find_value(commands, fn command ->
        case PeriodicWorker.ask(fn -> state.apply_command.(command) end) do
          {:ok, _reply} -> nil
          {:error, reason} -> {:error, reason}
        end
      end)

    case unanswered do
      nil -> PeriodicWorker.resume(state)
      {:error, reason} -> PeriodicWorker.skip(state, :heal_commands_unapplied, reason)
    end
  end

  # Adds the resolved spread to the heal opts for this pass (nil = leave them unchanged).
  defp put_spread(opts, nil), do: opts
  defp put_spread(opts, spread), do: Keyword.put(opts, :spread, spread)

  # Asks each live primary which of its active segments are already fenced. The impure half of the
  # orphaned-fence decision, and the mirror of `probe_candidates/4` for failover: one batched call per
  # primary, and `state.fence` is deliberately not reachable from here.
  defp probe_fences(state, metadata, live) do
    metadata
    |> OrphanedFence.candidates(live)
    |> Enum.reduce(%{}, fn {primary, segments}, acc ->
      Map.merge(acc, state.seal_state.(primary, segments))
    end)
  end

  # Loud on both halves: the seal that never landed is reported where it failed
  # (`Malachi.BrokerServer.fence_and_seal/2`), and this is the other end of that pair, so an operator
  # can tell a divergence that healed from a range that is still refusing writes.
  #
  # Counted from what LANDED, re-read from the control plane, not from what was planned. A seal applied
  # here can fail on the very timeout that created the divergence, and counting a planned seal as a
  # reconciled one would put `fence_reconciled` above `orphaned_fence` while the range was still
  # refusing every write, inverting the one signal this pair exists to give. The extra read costs a
  # round trip only on the passes that found something, which is the rare case.
  defp report_orphans([], _state), do: :ok

  # A re-read that exits (only the node-wide coordinator's source can, see `run/1`) reports nothing: a seal
  # that did not land is found again by the next pass, and one that did is left uncounted, which undercounts
  # `fence_reconciled` rather than inverting it. A vnode's source answers an empty metadata instead, so there
  # every orphan of that pass is reported as unrecorded, landed or not.
  defp report_orphans(orphans, state) do
    case PeriodicWorker.ask(state.metadata_source) do
      {:ok, metadata} -> report_orphans_landed(orphans, metadata)
      {:error, _reason} -> :ok
    end
  end

  defp report_orphans_landed(orphans, metadata) do
    {landed, pending} = Enum.split_with(orphans, &sealed_now?(metadata, &1))

    if landed != [] do
      Telemetry.fence_reconciled(length(landed))

      Logger.warning(
        I18n.t(:heal_orphaned_fence_reconciled,
          count: length(landed),
          segments: inspect(Enum.map(landed, &elem(&1, 1)))
        )
      )
    end

    # Retried next pass (level-triggered), but silence here is what let the original divergence go
    # unnoticed, so a seal this pass could not land says so.
    if pending != [] do
      Logger.error(
        I18n.t(:heal_orphaned_fence_unrecorded,
          count: length(pending),
          segments: inspect(Enum.map(pending, &elem(&1, 1)))
        )
      )
    end
  end

  # Brings the copies of sealed segments that do not match their recorded length back to it. Level
  # triggered like everything else here: a copy this pass could not settle, or one the batch left out,
  # is still reported by the integrity probe next tick and simply settled then.
  defp settle_sealed_copies(state, metadata, unsettled) do
    metadata
    |> SealedOverrun.plan(unsettled, state.settle_batch_size)
    |> Enum.map(fn {replica, segment_id, base_offset, end_offset} ->
      {segment_id, replica, state.settle_copy.(replica, segment_id, base_offset, end_offset)}
    end)
    |> report_settled()
  end

  # Counted and logged apart, because the two outcomes are not the same event wearing different
  # numbers. A copy that only gained a marker is routine and happens in the thousands on the first
  # pass; a copy that gave up records was holding data its segment's seal excludes, which is the defect
  # this pass exists for, happening in production. Sharing a counter would bury the second in the first
  # on the one pass that touches everything.
  defp report_settled([]), do: :ok

  defp report_settled(results) do
    trimmed = for {segment_id, replica, {:ok, dropped}} <- results, dropped > 0, do: {segment_id, replica, dropped}
    fenced = Enum.count(results, &match?({_segment_id, _replica, {:ok, 0}}, &1))
    unsettled = for {segment_id, replica, :error} <- results, do: {segment_id, replica}

    if fenced > 0, do: Telemetry.sealed_copies_settled(:fenced, fenced, 0)

    if trimmed != [] do
      records = Enum.reduce(trimmed, 0, fn {_segment_id, _replica, dropped}, sum -> sum + dropped end)
      Telemetry.sealed_copies_settled(:trimmed, length(trimmed), records)

      Logger.warning(
        I18n.t(:heal_sealed_copy_trimmed,
          count: length(trimmed),
          records: records,
          copies: inspect(Enum.map(trimmed, fn {segment_id, replica, _dropped} -> {segment_id, replica} end))
        )
      )
    end

    # Retried next pass, but a copy that keeps refusing to settle stays divergent from what the control
    # plane promises, and a pass that said nothing about it is how this defect went unseen for so long.
    if unsettled != [] do
      Telemetry.sealed_copies_settled(:failed, length(unsettled), 0)
      Logger.warning(I18n.t(:heal_sealed_copy_not_settled, count: length(unsettled), copies: inspect(unsettled)))
    end
  end

  defp sealed_now?(metadata, {:seal_segment, segment_id, _length, _bytes, _at}) do
    match?(%{state: :sealed}, Map.get(metadata.segments, segment_id))
  end

  # The failed copies a heal command left out of its segment's new replica set: `SelfHealing` never places
  # a segment on a broker whose copy of it failed, so every one of them was replaced.
  defp replaced_copies(heal_commands, failed) do
    for {:set_segment_replicas, segment_id, new_set} <- heal_commands,
        {^segment_id, replica} <- failed,
        replica not in new_set,
        do: {segment_id, replica}
  end

  defp discard_replaced_copies(_state, []), do: :ok

  # Re-read like `report_orphans/2`, and for a sharper reason: a copy deleted while the control plane still
  # lists it answers reads with nothing, where the latched copy answered with an error. A re-read that exits
  # discards nothing, and neither does a vnode's empty answer (`left_the_set?/3` finds no segment). Such a
  # copy is not found again: once its heal command landed the replica is out of the segment's set, and no
  # later pass probes it, so it stays latched on its broker until retention deletes the segment.
  defp discard_replaced_copies(state, replaced) do
    case PeriodicWorker.ask(state.metadata_source) do
      {:ok, metadata} -> discard_replaced_copies(state, replaced, metadata)
      {:error, _reason} -> :ok
    end
  end

  defp discard_replaced_copies(state, replaced, metadata) do
    discarded = Enum.filter(replaced, fn {segment_id, replica} -> left_the_set?(metadata, segment_id, replica) end)

    Enum.each(discarded, fn {segment_id, replica} -> state.discard_copy.(replica, segment_id) end)

    if discarded != [] do
      Logger.warning(I18n.t(:heal_failed_copy_replaced, count: length(discarded), copies: inspect(discarded)))
    end
  end

  defp left_the_set?(metadata, segment_id, replica) do
    case Map.get(metadata.segments, segment_id) do
      %{replica_set: replica_set} -> replica not in replica_set
      nil -> false
    end
  end

  # Asks every live replica which of its segments have a copy that failed there, as the
  # `{segment_id, replica}` pairs `Failover` and `SelfHealing` take. Read-only and batched per replica, like
  # `probe_fences/3`.
  defp probe_failures(state, metadata, live) do
    metadata
    |> Failover.failure_probes(live)
    |> Enum.reduce(MapSet.new(), fn {replica, segment_ids}, acc ->
      state.failed_state.(replica, segment_ids)
      |> Enum.reduce(acc, &MapSet.put(&2, {&1, replica}))
    end)
  end

  # Asks every live replica of every failover candidate what it holds. The impure half of the
  # failover decision: `Failover` stays a pure function of these answers. A failed copy is not asked,
  # since `Failover.candidates/3` already leaves it out.
  #
  # Membership is a view, and a broker it calls gone may only be suspected: one missed ack is enough for
  # SWIM, well before it confirms a failure. Fencing on that view would seal a segment that can still take
  # writes. So the replicas the view leaves out are asked too, read-only, and one that answers counts as
  # live: a segment that is no candidate once they are counted is left alone this pass, unfenced, unless an
  # earlier pass already fenced one of its followers (`fenced_followers/3`).
  #
  # Liveness is a broker's, not a segment's, so each broker the view leaves out is asked once per pass,
  # about one of its candidate segments: a dead broker costs one probe timeout per pass, not one per
  # segment it held.
  #
  # A segment kept is probed over every replica that answers, the view's and the ones it left out alike,
  # except a copy latched as failed.
  defp probe_candidates(state, metadata, live, failed) do
    live_set = MapSet.new(live)
    candidates = Enum.map(Failover.candidates(metadata, live, failed), &Map.fetch!(metadata.segments, elem(&1, 0)))
    reachable = MapSet.union(live_set, answering_absent(state, candidates, live_set))
    {still, rescued} = Enum.split_with(candidates, &Failover.candidate?(&1, reachable, failed))
    fenced = fenced_followers(state, rescued, reachable)

    (still ++ Enum.filter(rescued, &MapSet.member?(fenced, &1.id)))
    |> Map.new(fn segment ->
      answers =
        for r <- segment.replica_set,
            MapSet.member?(reachable, r),
            not MapSet.member?(failed, {segment.id, r}),
            stats = probe(state, r, segment.id, segment.start_offset),
            into: %{},
            do: {r, stats}

      warn_if_blocked(segment.id, answers, segment.replica_set)
      {segment.id, fence_answered(state, segment, answers)}
    end)
  end

  # The brokers the view leaves out of some candidate's replica set that answer a read-only probe, each
  # asked once, about the first candidate segment it holds. An answer that is an error counts as no answer:
  # a broker whose copy of that one segment failed in storage is taken as gone for the pass, which costs its
  # other segments a seal they did not need, never a write they acknowledged.
  defp answering_absent(state, candidates, live_set) do
    candidates
    |> Enum.flat_map(fn segment ->
      for replica <- segment.replica_set, not MapSet.member?(live_set, replica), do: {replica, segment}
    end)
    |> Enum.uniq_by(&elem(&1, 0))
    |> Enum.filter(fn {replica, segment} -> probe(state, replica, segment.id, segment.start_offset) != nil end)
    |> MapSet.new(&elem(&1, 0))
  end

  # The `rescued` segments (no candidate once the brokers that answered are counted) that an earlier pass
  # already fenced a follower of. Only a failover fences an active segment's follower, so that pass
  # committed to a seal that did not land; a primary that answers now does not undo it (a fenced follower
  # refuses its appends, so it cannot close a quorum), so the segment is kept and the seal retried, rather
  # than left with its followers closed and nothing to finish it (#269). Followers only: a fenced primary is
  # a roll, a split or an orphaned failover, which `Malachi.Cluster.OrphanedFence` finishes and reports.
  # Asked like `probe_fences/3`, one batched call per replica, whatever the number of segments.
  defp fenced_followers(_state, [], _reachable), do: MapSet.new()

  defp fenced_followers(state, rescued, reachable) do
    rescued
    |> Enum.flat_map(fn segment ->
      for follower <- Enum.drop(segment.replica_set, 1),
          MapSet.member?(reachable, follower),
          do: {follower, {segment.id, segment.start_offset}}
    end)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.reduce(MapSet.new(), fn {follower, segments}, acc ->
      state.seal_state.(follower, segments) |> Map.keys() |> MapSet.new() |> MapSet.union(acc)
    end)
  end

  # Measure first, fence second, and only once the measurement has reached the seal quorum
  # (`Failover.seal_quorum/1`).
  #
  # The fence is what makes a seal point final, so it has to happen before the point is recorded. But it
  # has no inverse: nothing in the system unseals a replica's store. Fencing every replica that answers,
  # before knowing whether enough did, therefore closes replicas of a segment this pass may then decline
  # to seal, and those replicas keep refusing writes after their primary comes back, shrinking every later
  # write quorum for nothing.
  #
  # Below the seal quorum the pass leaves the replicas untouched and the range simply stays blocked until
  # enough return, which is the CP choice `Malachi.Cluster.Failover` already documents. At or above it, the
  # fence answers are what `Failover.plan/5` seals on, and it applies the same rule again to them, so a
  # fence that fails on enough replicas still declines rather than sealing on too few.
  defp fence_answered(state, segment, answers) do
    if Failover.seal_quorum?(map_size(answers), segment.replica_set) do
      for {replica, _measured} <- answers,
          fenced = probe_with(state.fence, replica, segment.id, segment.start_offset),
          into: %{},
          do: {replica, fenced}
    else
      answers
    end
  end

  # `nil` (rather than an error tuple) so the comprehension above filters a silent replica out: a
  # replica that cannot answer tells us nothing about what it holds, and counting it would be the same
  # mistake as sealing on a guess.
  defp probe(state, replica, segment_id, base_offset), do: probe_with(state.probe, replica, segment_id, base_offset)

  defp probe_with(fun, replica, segment_id, base_offset) do
    case fun.(replica, segment_id, base_offset) do
      {end_offset, byte_size} when is_integer(end_offset) and is_integer(byte_size) -> {end_offset, byte_size}
      _other -> nil
    end
  end

  defp warn_if_blocked(segment_id, answers, replica_set) do
    unless Failover.seal_quorum?(map_size(answers), replica_set) do
      Logger.warning(
        I18n.t(:heal_seal_no_quorum,
          segment_id: inspect(segment_id),
          answered: map_size(answers),
          replicas: length(replica_set),
          needed: Failover.seal_quorum(replica_set)
        )
      )
    end
  end

  # Read-only: this is the measurement that decides whether the seal quorum is even present, and whether a
  # broker the membership view calls gone still answers. It flushes
  # before answering (see `ReplicationServer.durable_stats/4`), so what it reports is what a read can
  # serve, but it leaves the replica writable. `fence` below is the half with consequences.
  defp default_probe(timeout), do: answer_fun(&ReplicationServer.durable_stats/4, timeout)

  # The fence, applied only to segments the pass has already decided to seal (see `fence_answered/3`).
  # After it returns, that replica refuses every append to the segment, so the end it reports cannot
  # move afterwards: the seal point becomes a consequence of closing the segment rather than a number
  # racing it, which is what the `Malachi.Cluster.Failover` moduledoc claims.
  defp default_fence(timeout), do: answer_fun(&ReplicationServer.seal/4, timeout)

  # Read-only too, and batched: one call per primary, whatever the number of segments. `%{}` on any
  # error (including an unreachable replica) is the same rule the other two defaults follow, and it is
  # what a segment nothing could be learned about deserves: no answer means no seal.
  defp default_seal_state(timeout) do
    fn replica, segments ->
      case ReplicationServer.fenced_segments(replica, segments, timeout) do
        {:ok, fenced} -> fenced
        {:error, _reason} -> %{}
      end
    end
  end

  # Same rule as `default_seal_state/1`: no answer means nothing learned, so an unreachable replica reports
  # no failed copy rather than stalling or crashing the pass.
  defp default_failed_state(timeout) do
    fn replica, segment_ids ->
      case ReplicationServer.failed_segments(replica, segment_ids, timeout) do
        {:ok, failed} -> failed
        {:error, _reason} -> MapSet.new()
      end
    end
  end

  # The only default here that changes a replica. It answers the DROPPED count rather than the pair the
  # other seams answer: what the pass has to tell apart is a copy that merely gained a marker from one
  # that gave up records, and the end offset it lands on is the same number either way.
  defp default_settle_copy(timeout) do
    fn replica, segment_id, base_offset, end_offset ->
      case ReplicationServer.seal_at(replica, segment_id, base_offset, end_offset, timeout) do
        {:ok, _end_offset, _byte_size, dropped} -> {:ok, dropped}
        {:error, _reason} -> :error
      end
    end
  end

  # Both calls answer `{:ok, end_offset, byte_size}` or an error, and both catch an unreachable replica
  # themselves, so a silent one costs the pass a timeout rather than a crash.
  defp answer_fun(call, timeout) do
    fn replica, segment_id, base_offset ->
      case call.(replica, segment_id, base_offset, timeout) do
        {:ok, end_offset, byte_size} -> {end_offset, byte_size}
        {:error, _reason} -> :error
      end
    end
  end
end
