defmodule Malachi.Retention.OrphanSweeper do
  @moduledoc """
  Reclaims the replica directories retention could not delete, on a slow cadence and behind explicit
  guards.

  ## The leak it closes

  `Malachi.Retention.Expirer` deletes a segment from the control plane and then deletes its bytes on
  each replica. A replica that did not answer keeps its directory, and no later sweep can ask for it
  again: a segment gone from the control plane never comes back from
  `Malachi.Cluster.Retention.expired/3`. Without this worker the disk a node outage cost is never
  returned, whatever retention is configured to do. Other paths leak the same way, a catch-up that
  failed after creating the directory or a copy healing moved to another broker, and this reclaims those
  too, because it works from what is on disk rather than from what went wrong.

  ## Why it runs per node, and asks the owning vnode

  The data directory is a fact about the **node**: `Malachi.Storage.Layout` puts the segments of every
  vnode in one root, so a per-vnode worker would see its neighbours' directories as unexplained. It is
  not leader gated for the same reason `Malachi.Cluster.Scrubber` is not: only a node can read its own
  disk.

  What it does NOT read is this broker's cached copy of the metadata. That cache is refreshed by a
  reconcile that can skip ticks, drop a stale result or fail, and a view that aged without anyone
  noticing is exactly a view missing segments whose replicas are already on this disk (#249). So before
  a directory can be removed, the vnode that OWNS its segment is asked, linearizably, whether it lists
  it (`Malachi.Retention.Orphans.explain/2`, `Malachi.Cluster.ReplicatedDSRSM.known_segments/3`). That
  is NorthGuard's shape: the only global state is which vnodes exist, and a vnode's leader answers for
  the metadata it owns (the meetup transcript, 502-508 and 609-613). The owner is the vnode every
  segment command is routed to (`Malachi.Metadata.segment_routing_topic/1`). A segment's registration
  commits on its owner before any replica creates its directory, so the answer has no registration lag.

  ## The guards

  Deleting data is the one thing here that cannot be undone, so the pass gives up rather than guesses:

    * **The owners answer, in time.** The question runs in a task under one deadline
      (`:authority_deadline_ms`), and a pass whose owners did not all answer, whose topology changed
      while it asked, that had no ring to route by, or that ran out of time is skipped whole. A partial
      answer is not an answer. A directory its owner does not list is also asked of every other vnode
      before it can go, so while one vnode anywhere is silent, a pass with a real orphan to decide is
      skipped too. On a sharded control plane the
      topology is the one of record in the durable ring store, not this node's gossiped copy
      (`Malachi.Application.durable_orphan_authority/1` says why).
    * **An undecided directory is kept.** One whose segment id carries no topic has no owner to ask;
      one whose topic a pending split is moving may be listed by neither owner at the instant each is
      read; and one a vnode that does not own it lists was written there by a broker routing by a
      stale ring. All are reported as held, without a sighting, never removed on that answer, and
      named in the log when that set changes.
    * **Age, repeated sightings and a cap per pass**, which `Malachi.Retention.Orphans` owns and
      explains. Only the candidates old enough to matter are asked about, oldest first and at most
      `:max_tracked` of them, which bounds this node's work and how many owners one pass asks (each
      owner still answers with its whole segment map). The overflow waits for a later pass, and the
      pass says so in the log.
    * **Removal through the replication server.** A directory that looks orphaned may still have an open
      log here, so `Malachi.Cluster.ReplicationServer.delete_directory/3` closes it first. Nothing here
      touches the filesystem directly.

  Out of scope, stated so it is not mistaken for covered: a vnode split whose rollback failed can leave
  a topic on a vnode no topology lists. Nothing can ask that vnode, this sweep included, and the broker's
  cache never read it either.

  ## Modes

  `:delete` reclaims, `:report` does everything except the removal (for a first run on a cluster whose
  operator wants to see the list first), and `:off` does not even list. A value that cannot be a mode
  is refused at boot by `Malachi.Config.retention_orphan_sweep/1` rather than defaulted, because the
  default is the mode that removes and a typo would have selected it in silence. The bounds come from
  the environment too, and each is checked rather than trusted (`Malachi.Config.checked/4`).

  ## Options

    * `:authority` - `([name] -> {:ok, %{known: MapSet.t(), undecided: MapSet.t()}} | {:error, term()})`,
      which of the candidate directory names the control plane accounts for (required). Built on
      `Malachi.Retention.Orphans.explain/2` by `Malachi.Application` for each kind of control plane;
    * `:local_ref` - this node's replication server reference, or a `(-> ref)` resolved per pass, as in
      `Malachi.Cluster.Scrubber` (required);
    * `:directory` - the data directory to sweep (required);
    * `:mode` - `:delete` (default), `:report` or `:off`. A value outside those three falls back to
      `:report`, not to the default, since the default removes;
    * `:interval` - ms between passes (default 300_000: a leak accumulates slowly and a pass reads the
      whole directory, so this is deliberately slower than the other workers);
    * `:min_age_ms` - default 600_000;
    * `:sightings` - default 2;
    * `:max_per_pass` - default 50;
    * `:max_tracked` - default 10_000;
    * `:authority_deadline_ms` - how long one pass waits for the `:authority`, every read included
      (default 15_000). Past it, the pass is skipped as `{:unreachable, :deadline}`;
    * `:clock` - `(-> non_neg_integer())` epoch ms, for the directory ages;
    * `:on_result` - `(result -> any)` for each pass (default: logs what was removed and what failed;
      the directories held as undecided are logged by the sweeper itself, when that set changes).

  Every pass returns its outcome, so tests assert on data rather than on log lines.
  """

  use GenServer

  require Logger

  alias Malachi.Cluster.PeriodicWorker
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Config
  alias Malachi.I18n
  alias Malachi.Retention.Orphans
  alias Malachi.Telemetry

  @default_interval 300_000
  @default_min_age_ms 600_000
  @default_sightings 2
  @default_max_per_pass 50
  @default_max_tracked 10_000
  # How long one pass may wait for the control plane's answer, all reads included. ra follows a
  # leader redirect with a fresh timeout, so a per-read bound is not a bound on the pass during an
  # election; this is. Far above a healthy pass, far below the interval.
  @default_authority_deadline_ms 15_000
  @modes [:delete, :report, :off]

  @typedoc """
  One pass: how many directories it looked at, the ones it removed, the ones still short of a guard,
  the ones it could not remove, and the reason it did nothing at all when that is the case.
  """
  @type result :: %{
          scanned: non_neg_integer(),
          removed: [String.t()],
          held: [String.t()],
          failed: [{String.t(), term()}],
          skipped:
            nil
            | :off
            | :no_topology
            | {:vnodes_unreachable, [term()]}
            | {:topology_changed, non_neg_integer() | nil, non_neg_integer() | nil}
            | {:unreadable, term()}
            | {:unreachable, term()}
        }

  @doc "Starts the sweeper. See the module doc for options."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_server_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @doc "Runs one pass synchronously and returns its result, for tests and manual triggers."
  @spec sweep_now(GenServer.server()) :: result()
  def sweep_now(server), do: GenServer.call(server, :sweep_now, :infinity)

  @doc "The mode this sweeper is running in."
  @spec mode(GenServer.server()) :: :delete | :report | :off
  def mode(server), do: GenServer.call(server, :mode)

  # --- server ---

  @impl true
  def init(opts) do
    state =
      Map.merge(PeriodicWorker.new(opts, :orphan_sweeper, @default_interval, :retention_orphan_interval_ms), %{
        authority: Keyword.fetch!(opts, :authority),
        local_ref: Keyword.fetch!(opts, :local_ref),
        directory: Keyword.fetch!(opts, :directory),
        # `:report` rather than `:delete` when the value cannot be a mode. The environment is already
        # refused at boot by `Malachi.Config.retention_orphan_sweep/1`, so this is the last line for a
        # caller that built its options by hand, and the safe side of a mode is the one that does not
        # remove anything.
        mode: checked_mode(opts),
        min_age_ms: checked(opts, :min_age_ms, @default_min_age_ms, &non_neg_integer?/1),
        sightings: checked(opts, :sightings, @default_sightings, &positive_integer?/1),
        max_per_pass: checked(opts, :max_per_pass, @default_max_per_pass, &positive_integer?/1),
        max_tracked: checked(opts, :max_tracked, @default_max_tracked, &positive_integer?/1),
        authority_deadline_ms:
          checked(opts, :authority_deadline_ms, @default_authority_deadline_ms, &positive_integer?/1),
        clock: Keyword.get(opts, :clock, fn -> System.system_time(:millisecond) end),
        on_result: Keyword.get(opts, :on_result, &log_result/1),
        # How many consecutive passes each candidate has been unexplained for (see `Orphans`).
        seen: %{},
        # Whether the last pass was held for want of an answer, so the line is logged on the transition only.
        waiting?: false,
        # The directories the last pass held as undecided, so the line naming them is logged when the
        # set changes rather than every pass.
        undecided: MapSet.new()
      })

    PeriodicWorker.schedule(state)
    {:ok, state}
  end

  @impl true
  def handle_call(:sweep_now, _from, state) do
    {result, state} = run(state)
    {:reply, result, state}
  end

  def handle_call(:mode, _from, state), do: {:reply, state.mode, state}

  def handle_call(message, _from, state), do: PeriodicWorker.unknown_call(state, message)

  # Nothing casts to this server; without this clause, `use GenServer` would stop it on the first cast.
  @impl true
  def handle_cast(message, state), do: PeriodicWorker.unknown_cast(state, message)

  @impl true
  def handle_info(:tick, state), do: PeriodicWorker.tick(state, fn state -> state |> run() |> elem(1) end)

  def handle_info(message, state), do: PeriodicWorker.unknown_info(state, message)

  # --- internals ---

  defp run(%{mode: :off} = state), do: report(skipped(:off), state)

  defp run(state) do
    case entries(state) do
      {:ok, entries} -> ask(state, entries)
      {:error, reason} -> report(skipped({:unreadable, reason}), state)
    end
  end

  # A disk with nothing old enough to matter asks nobody: the owners are only bothered about names
  # that could actually be removed.
  defp ask(state, entries) do
    {names, left_out} = Orphans.candidates(entries, min_age_ms: state.min_age_ms, max_tracked: state.max_tracked)

    # The overflow waits for a later pass, which only delays a removal; the line is what says it waits.
    if left_out > 0, do: Logger.warning(I18n.t(:retention_orphan_tracking_capped, limit: state.max_tracked))

    case names do
      [] -> act(%{state | waiting?: false}, entries, [], MapSet.new())
      names -> ask(state, entries, names)
    end
  end

  defp ask(state, entries, names) do
    case ask_authority(state, names) do
      {:ok, {:ok, %{known: known, undecided: undecided}}} ->
        unexplained = Enum.reject(names, &(MapSet.member?(known, &1) or MapSet.member?(undecided, &1)))
        act(%{state | waiting?: false}, entries, unexplained, undecided)

      {:ok, {:error, reason}} ->
        report(skipped(skip_reason(reason)), announce_waiting(state, reason))

      {:error, reason} ->
        report(skipped({:unreachable, reason}), announce_waiting(state, reason))
    end
  end

  # The whole question under one deadline, in a task this server is not linked to: a read that keeps
  # being redirected during an election, an exit, or a raise, each ends the pass as a skipped one
  # rather than holding this server or taking it down. The task is killed at the deadline, so nothing
  # it would have answered can land after the pass moved on.
  defp ask_authority(state, names) do
    task = Task.Supervisor.async_nolink(Malachi.TaskSupervisor, fn -> state.authority.(names) end)

    case Task.yield(task, state.authority_deadline_ms) || Task.shutdown(task, :brutal_kill) do
      {:ok, answer} -> {:ok, answer}
      {:exit, reason} -> {:error, reason}
      nil -> {:error, :deadline}
    end
  end

  defp skip_reason({:vnodes_unreachable, _vnodes} = reason), do: reason
  defp skip_reason({:topology_changed, _before, _after} = reason), do: reason
  defp skip_reason(:no_topology), do: :no_topology
  defp skip_reason(reason), do: {:unreachable, reason}

  defp act(state, entries, unexplained, undecided) do
    review =
      Orphans.review(unexplained, state.seen,
        sightings: state.sightings,
        max_per_pass: state.max_per_pass,
        max_tracked: state.max_tracked
      )

    {removed, failed} = remove(state, review.ready)
    if removed != [], do: Telemetry.retention_orphan_removed(length(removed))

    result = %{
      scanned: length(entries),
      removed: Enum.sort(removed),
      # In `:report` mode nothing was removed, so every directory that was ready is held back too: the
      # result then says exactly what a `:delete` pass would have taken. An undecided name (no owner
      # to ask, or a split still moving it) is held without a sighting, and listed so it is not a leak
      # nobody can see.
      held:
        Enum.sort(
          review.held ++ ((review.ready -- removed) -- Enum.map(failed, &elem(&1, 0))) ++ MapSet.to_list(undecided)
        ),
      failed: Enum.sort(failed),
      skipped: nil
    }

    report(result, announce_undecided(%{state | seen: review.sightings}, undecided))
  end

  # An undecided directory can be held for a long time (one whose segment id carries no topic, for
  # good), and a held directory the default handler never names is a leak nobody sees. Named when the
  # set changes, not every pass.
  defp announce_undecided(%{undecided: same} = state, same), do: state

  defp announce_undecided(state, undecided) do
    if MapSet.size(undecided) > 0 do
      Logger.info(
        I18n.t(:retention_orphan_undecided,
          count: MapSet.size(undecided),
          directories: inspect(Enum.sort(undecided))
        )
      )
    end

    %{state | undecided: undecided}
  end

  # `:report` is the mode for a first run on a cluster whose operator wants the list before the removal:
  # everything up to here is identical, so what it reports is exactly what `:delete` would have taken.
  defp remove(%{mode: :report}, _ready), do: {[], []}

  defp remove(state, ready) do
    ref = resolve_ref(state.local_ref)

    Enum.reduce(ready, {[], []}, fn name, {removed, failed} ->
      case ReplicationServer.delete_directory(ref, name) do
        :ok -> {[name | removed], failed}
        {:error, reason} -> {removed, [{name, reason} | failed]}
      end
    end)
  end

  # Directories only, each with its age. A node whose data directory does not exist yet has swept
  # nothing rather than failed, which is what a fresh node looks like before its first segment.
  defp entries(state) do
    now_ms = state.clock.()

    case File.ls(state.directory) do
      {:ok, names} -> {:ok, for(name <- names, entry = entry(state, name, now_ms), do: entry)}
      {:error, :enoent} -> {:ok, []}
      {:error, reason} -> {:error, reason}
    end
  end

  defp entry(state, name, now_ms) do
    path = Path.join(state.directory, name)

    case File.stat(path, time: :posix) do
      {:ok, %File.Stat{type: :directory, ctime: ctime}} -> {name, max(now_ms - ctime * 1000, 0)}
      _not_a_directory_or_gone -> nil
    end
  end

  defp resolve_ref(local_ref) when is_function(local_ref, 0), do: local_ref.()
  defp resolve_ref(local_ref), do: local_ref

  defp skipped(reason), do: %{scanned: 0, removed: [], held: [], failed: [], skipped: reason}

  defp report(result, state) do
    state.on_result.(result)
    {result, state}
  end

  # Logged on the transition only: a node whose owners stay silent would otherwise say the same thing
  # every interval for as long as they do.
  defp announce_waiting(%{waiting?: true} = state, _reason), do: state

  defp announce_waiting(state, reason) do
    Logger.info(I18n.t(:retention_orphan_authority_unavailable, directory: state.directory, reason: inspect(reason)))
    %{state | waiting?: true}
  end

  defp log_result(%{removed: [], failed: []}), do: :ok

  defp log_result(result) do
    if result.removed != [] do
      Logger.info(
        I18n.t(:retention_orphan_removed,
          count: length(result.removed),
          directories: inspect(result.removed)
        )
      )
    end

    if result.failed != [] do
      Logger.warning(I18n.t(:retention_orphan_remove_failed, failures: inspect(result.failed)))
    end

    :ok
  end

  defp checked(opts, key, default, valid?) do
    opts |> Keyword.get(key, default) |> Config.checked(:"retention_orphan_#{key}", default, valid?)
  end

  # The absent default and the invalid fallback differ here, and only here. Absent is `:delete`, the
  # documented default. Invalid falls back to `:report`, because falling back to a mode that removes
  # is how a value nobody could read would still delete directories.
  defp checked_mode(opts) do
    opts
    |> Keyword.get(:mode, :delete)
    |> Config.checked(:retention_orphan_sweep, :report, &(&1 in @modes))
  end

  defp non_neg_integer?(value), do: is_integer(value) and value >= 0
  defp positive_integer?(value), do: is_integer(value) and value > 0
end
