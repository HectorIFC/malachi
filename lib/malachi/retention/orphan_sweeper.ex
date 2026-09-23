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

  ## Why it runs per node and reads every vnode

  The data directory is a fact about the **node**: `Malachi.Storage.Layout` puts the segments of every
  vnode in one root, so a per-vnode worker would see its neighbours' directories as unexplained. It is
  not leader gated for the same reason `Malachi.Cluster.Scrubber` is not: only a node can read its own
  disk.

  Reading the whole metadata does not make this a global-state consumer: `Malachi.BrokerServer.metadata/1`
  answers from the cache the broker already keeps to serve reads, refreshed by its own reconcile, so the
  sweep adds no cross-node traffic.

  ## The guards

  Deleting data is the one thing here that cannot be undone, so the pass gives up rather than guesses:

    * **A broker that answers.** Both questions this pass asks the broker go through
      `Malachi.Cluster.PeriodicWorker.ask/1`: one that cannot answer skips the pass rather than taking
      the worker down with it, which is the same rule as the guards below, applied to the act of
      asking. A sharded control plane coming back from a full-cluster restart is the measured case.
    * **Metadata ready.** `Malachi.BrokerServer.metadata_ready?/2` says every vnode has been read at
      least once since boot. Until then a silent vnode is an empty placeholder in the cache and every
      directory it owns looks unexplained.
    * **Every vnode answering.** `Malachi.BrokerServer.unreachable_vnodes/2` says whether the view
      being served right now is complete. Readiness alone is not enough, and the difference is where a
      live replica would have been lost: a vnode that goes silent AFTER being read keeps the view it
      had (`Malachi.Cluster.DSRSM.retain_vnodes/3` retains rather than blanks), so its old segments
      stay explained while segments registered on it since are missing from the merge. Their replicas
      still land on this node's disk over the data plane, which is a different channel from the ra
      query this node cannot make, so a silence longer than the minimum age plus a sighting interval
      would have made this sweep delete a live copy. The minimum age bounds the registration lag, not
      an outage.
    * **Age, repeated sightings and a cap per pass**, which `Malachi.Retention.Orphans` owns and
      explains.
    * **Removal through the replication server.** A directory that looks orphaned may still have an open
      log here, so `Malachi.Cluster.ReplicationServer.delete_directory/3` closes it first. Nothing here
      touches the filesystem directly.

  ## Modes

  `:delete` reclaims, `:report` does everything except the removal (for a first run on a cluster whose
  operator wants to see the list first), and `:off` does not even list. The mode and every bound come
  from the environment, so each is checked rather than trusted (`Malachi.Config.checked/4`).

  ## Options

    * `:metadata_source` - `(-> Malachi.Metadata.t())` (required);
    * `:metadata_ready?` - `(-> boolean())` (required);
    * `:unreachable_vnodes` - `(-> [vnode_id])`, the vnodes the last metadata refresh could not read
      (required). Always empty where the metadata is local;
    * `:local_ref` - this node's replication server reference, or a `(-> ref)` resolved per pass, as in
      `Malachi.Cluster.Scrubber` (required);
    * `:directory` - the data directory to sweep (required);
    * `:mode` - `:delete` (default), `:report` or `:off`;
    * `:interval` - ms between passes (default 300_000: a leak accumulates slowly and a pass reads the
      whole directory, so this is deliberately slower than the other workers);
    * `:min_age_ms` - default 600_000;
    * `:sightings` - default 2;
    * `:max_per_pass` - default 50;
    * `:max_tracked` - default 10_000;
    * `:clock` - `(-> non_neg_integer())` epoch ms, for the directory ages;
    * `:on_result` - `(result -> any)` for each pass (default: logs what was removed or held).

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
            | :metadata_not_ready
            | {:vnodes_unreachable, [term()]}
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
        metadata_source: Keyword.fetch!(opts, :metadata_source),
        metadata_ready?: Keyword.fetch!(opts, :metadata_ready?),
        unreachable_vnodes: Keyword.fetch!(opts, :unreachable_vnodes),
        local_ref: Keyword.fetch!(opts, :local_ref),
        directory: Keyword.fetch!(opts, :directory),
        # `:report` rather than `:delete` when the value cannot be a mode. The environment is already
        # refused at boot by `Malachi.Config.retention_orphan_sweep/1`, so this is the last line for a
        # caller that built its options by hand, and the safe side of a mode is the one that does not
        # remove anything.
        mode: checked(opts, :mode, :delete, &(&1 in @modes)),
        min_age_ms: checked(opts, :min_age_ms, @default_min_age_ms, &non_neg_integer?/1),
        sightings: checked(opts, :sightings, @default_sightings, &positive_integer?/1),
        max_per_pass: checked(opts, :max_per_pass, @default_max_per_pass, &positive_integer?/1),
        max_tracked: checked(opts, :max_tracked, @default_max_tracked, &positive_integer?/1),
        clock: Keyword.get(opts, :clock, fn -> System.system_time(:millisecond) end),
        on_result: Keyword.get(opts, :on_result, &log_result/1),
        # How many consecutive passes each candidate has been unexplained for (see `Orphans`).
        seen: %{},
        # Whether the last pass found metadata unready, so the line is logged on the transition only.
        waiting?: false
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
    case PeriodicWorker.ask(state.metadata_ready?) do
      {:ok, true} -> run_when_complete(state)
      {:ok, false} -> report(skipped(:metadata_not_ready), announce_waiting(state))
      {:error, reason} -> report(skipped({:unreachable, reason}), state)
    end
  end

  # Ready says every vnode has been read once; this says the view in hand is the current one. Both, or
  # the pass acts on an absence it cannot account for.
  defp run_when_complete(state) do
    case PeriodicWorker.ask(state.unreachable_vnodes) do
      {:ok, []} -> sweep(%{state | waiting?: false})
      {:ok, vnodes} -> report(skipped({:vnodes_unreachable, vnodes}), announce_waiting(state))
      {:error, reason} -> report(skipped({:unreachable, reason}), state)
    end
  end

  defp sweep(state) do
    case entries(state) do
      {:ok, entries} -> act(state, entries)
      {:error, reason} -> report(skipped({:unreadable, reason}), state)
    end
  end

  defp act(state, entries) do
    case PeriodicWorker.ask(state.metadata_source) do
      {:ok, metadata} -> act(state, entries, Orphans.expected(metadata, state.directory))
      {:error, reason} -> report(skipped({:unreachable, reason}), state)
    end
  end

  defp act(state, entries, expected) do
    review =
      Orphans.review(expected, entries, state.seen,
        min_age_ms: state.min_age_ms,
        sightings: state.sightings,
        max_per_pass: state.max_per_pass,
        max_tracked: state.max_tracked
      )

    if review.capped?, do: Logger.warning(I18n.t(:retention_orphan_tracking_capped, limit: state.max_tracked))

    {removed, failed} = remove(state, review.ready)
    if removed != [], do: Telemetry.retention_orphan_removed(length(removed))

    result = %{
      scanned: length(entries),
      removed: Enum.sort(removed),
      # In `:report` mode nothing was removed, so every directory that was ready is held back too: the
      # result then says exactly what a `:delete` pass would have taken.
      held: Enum.sort(review.held ++ ((review.ready -- removed) -- Enum.map(failed, &elem(&1, 0)))),
      failed: Enum.sort(failed),
      skipped: nil
    }

    report(result, %{state | seen: review.sightings})
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

  # Logged on the transition only: a node that boots with a silent vnode would otherwise say the same
  # thing every interval for as long as that vnode stays silent.
  defp announce_waiting(%{waiting?: true} = state), do: state

  defp announce_waiting(state) do
    Logger.info(I18n.t(:retention_orphan_waiting_for_metadata, directory: state.directory))
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


  defp non_neg_integer?(value), do: is_integer(value) and value >= 0
  defp positive_integer?(value), do: is_integer(value) and value > 0
end
