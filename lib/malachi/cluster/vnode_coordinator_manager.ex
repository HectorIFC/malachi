defmodule Malachi.Cluster.VnodeCoordinatorManager do
  @moduledoc """
  Keeps this node's per-vnode coordinators in sync with the vnodes it currently leads (1C-b-ii). On a
  **level-triggered** reconcile. Right after start (`handle_continue`), every `:interval` ms, and on a
  `:topology_changed` nudge - it reads the vnode placement, compares the vnodes this node leads now with
  the ones it already runs coordinators for, then **starts** coordinators for newly-led vnodes and
  **stops** them for vnodes it no longer leads.
  Generic and testable via seams:

    * `:placement` - `(-> {:ok, placement} | {:error, reason})`, the vnodes the cluster has **right now**
      (e.g. the live ring this node tracks). Read once per reconcile, so the two derivations below can
      never disagree about which vnodes exist within a pass;
    * `:leading` - `(placement -> [vnode_id])`, the vnodes this node currently leads (e.g.
      `leading_vnodes/4` over live `ra` membership and leadership);
    * `:spawn` - `(vnode_id -> pid)`, starts that vnode's coordinators (e.g. a `Supervisor` under a
      `DynamicSupervisor`) and returns the pid of that (sub)tree, which the manager monitors and later
      stops by;
    * `:stop` - `(pid -> any)`, stops a vnode's coordinators;
    * `:version_servers` - optional `(placement -> [{machine, server_id}])`, the local vnode members whose
      machine version to watch (default none);
    * `:interval` - reconcile period in ms (default 5_000);
    * `:name` - optional registered name.

  ## Why the placement is a seam and not a list

  Every other input here is level-triggered, and this one used to be the exception: the placement was
  resolved once at boot and captured. A vnode this node gains afterwards, through a rebalance that adds
  it as an `ra` member or through a split that creates a new vnode, was then invisible, so none of its
  per-vnode work ran until the node restarted for some other reason: its segments never expired, its
  under-replicated ones were never repaired, its consumer-group coordinator never started, and its member
  was never watched for a machine version it had stopped applying.

  ## An unreadable placement is inert

  A read that fails leaves the running set, and the last version statuses, exactly as they are. The
  alternative, treating a failed read as "this node leads nothing", would stop **every** coordinator on
  the node over a transient read error, which is worse than the gap above. The state is logged on the
  transition into and out of that condition (not on every pass) and reported on every pass through the
  `[:malachi, :cluster, :vnode_reconcile]` telemetry event.

  An empty placement is **not** a failed read: a cluster genuinely has no vnodes before its ring is
  published, and a node that hosts none of them correctly runs no coordinators.

  Idempotent: a transient leadership flap just starts/stops coordinators; the underlying work is
  idempotent and routed through `ra`, so a brief double-run only redoes work (the same reasoning as
  1C-a, hence no lease). `reconcile_now/1` reconciles synchronously and returns the running vnode ids
  (a manual trigger, e.g. for tests).

  Self-healing on death: the manager monitors each spawned pid. If a vnode's coordinator tree dies on
  its own (e.g. its supervisor exhausts its restart intensity), the manager drops it so the next
  reconcile starts it again while this node still leads it, rather than leaving it silently down until
  leadership changes. The interval paces the retry, so a coordinator that keeps dying respawns at most
  once per reconcile with a log each time instead of spinning. A deliberate stop demonitors first, so
  it is never mistaken for a death.

  Version watch: on the same tick and over the same placement snapshot, every member returned by
  `:version_servers` is checked with `Malachi.Cluster.MachineVersion.check/3`, whether this node leads it
  or not, since a follower is just as able to stop applying its log. The last status per member is kept
  here, so a member that stays stuck is logged once, and a member no longer listed is forgotten.
  """

  use GenServer

  require Logger

  alias Malachi.Cluster.MachineVersion
  alias Malachi.I18n
  alias Malachi.Telemetry
  alias Malachi.UnexpectedMessage

  @default_interval 5_000

  @typedoc "What `:placement` answers: the vnodes that exist now, or why they could not be read."
  @type placement_read :: {:ok, [term()]} | {:error, term()}

  @doc "Starts the manager. See the module doc for required options."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_server_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @doc "Reconciles once synchronously; returns the vnode ids currently running coordinators."
  @spec reconcile_now(GenServer.server()) :: [term()]
  def reconcile_now(server), do: GenServer.call(server, :reconcile_now)

  @doc """
  Tells the manager the cluster topology changed, so it reconciles now instead of at the next tick.

  A nudge, not the mechanism: the periodic reconcile is what makes the manager correct, and this only
  makes a ring change visible sooner. A change that publishes no topology event, such as a rebalance
  adding this node to a vnode's `ra` cluster, is still caught by the tick. Asynchronous and safe to send
  to a node that runs no manager.
  """
  @spec topology_changed(GenServer.server()) :: :ok
  def topology_changed(server), do: GenServer.cast(server, :topology_changed)

  @doc "The last machine version status per watched member, keyed by server id."
  @spec version_status(GenServer.server()) :: %{term() => MachineVersion.status()}
  def version_status(server), do: GenServer.call(server, :version_status)

  @doc "Whether the last reconcile could read the placement."
  @spec placement_status(GenServer.server()) :: :ok | :unreadable
  def placement_status(server), do: GenServer.call(server, :placement_status)

  @impl true
  def init(opts) do
    state = %{
      placement: Keyword.fetch!(opts, :placement),
      leading: Keyword.fetch!(opts, :leading),
      spawn: Keyword.fetch!(opts, :spawn),
      stop: Keyword.fetch!(opts, :stop),
      version_servers: Keyword.get(opts, :version_servers, fn _placement -> [] end),
      version_status: %{},
      # `:ok` until a read fails, so the first failure is a transition and gets logged.
      placement_status: :ok,
      interval: Keyword.get(opts, :interval, @default_interval),
      running: %{},
      # The unknown message shapes already logged (see `Malachi.UnexpectedMessage`).
      unexpected_shapes: MapSet.new()
    }

    {:ok, state, {:continue, :reconcile}}
  end

  @impl true
  def handle_continue(:reconcile, state), do: {:noreply, reconcile_and_schedule(state)}

  @impl true
  def handle_info(:reconcile, state), do: {:noreply, reconcile_and_schedule(state)}

  # A coordinator tree died on its own (not via stop_vnodes, which demonitors first). Drop it and let
  # the next reconcile start it again while this node still leads it. Respawning here instead of on the
  # tick would turn a crash-looping coordinator into a tight respawn loop; the reconcile interval paces
  # the retry and keeps each one visible in the log.
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Enum.find(state.running, fn {_vnode_id, {_pid, monitor_ref}} -> monitor_ref == ref end) do
      {vnode_id, _handle} ->
        Logger.warning(I18n.t(:vnode_coordinators_down, vnode: inspect(vnode_id), reason: inspect(reason)))

        {:noreply, %{state | running: Map.delete(state.running, vnode_id)}}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info(message, state), do: {:noreply, drop_unexpected(state, :info, message)}

  # Reconciles without rescheduling: the periodic timer keeps its own cadence, so a burst of nudges
  # costs extra passes (idempotent) and never shifts or multiplies the tick.
  @impl true
  def handle_cast(:topology_changed, state), do: {:noreply, reconcile(state)}

  def handle_cast(message, state), do: {:noreply, drop_unexpected(state, :cast, message)}

  @impl true
  def handle_call(:reconcile_now, _from, state) do
    state = reconcile(state)
    {:reply, Map.keys(state.running), state}
  end

  def handle_call(:version_status, _from, state), do: {:reply, state.version_status, state}

  def handle_call(:placement_status, _from, state), do: {:reply, state.placement_status, state}

  def handle_call(message, _from, state) do
    {:reply, UnexpectedMessage.unknown_call_reply(), drop_unexpected(state, :call, message)}
  end

  defp drop_unexpected(state, kind, message) do
    shapes = UnexpectedMessage.drop(state.unexpected_shapes, :vnode_coordinator, kind, message)
    %{state | unexpected_shapes: shapes}
  end

  defp reconcile_and_schedule(state) do
    schedule(state)
    reconcile(state)
  end

  defp schedule(state), do: Process.send_after(self(), :reconcile, state.interval)

  defp reconcile(state) do
    case read_placement(state.placement) do
      {:ok, placement} -> reconcile_placement(state, placement)
      {:error, reason} -> placement_unreadable(state, reason)
    end
  end

  # Total over whatever the seam answers: a raising seam and one that answers a shape we do not know
  # are both "the placement could not be read", which is inert, rather than a crash that would take the
  # whole coordinator tree down with this process.
  @spec read_placement((-> placement_read())) :: placement_read()
  defp read_placement(placement) do
    case placement.() do
      {:ok, vnodes} when is_list(vnodes) -> {:ok, vnodes}
      {:error, reason} -> {:error, reason}
      other -> {:error, {:unexpected_placement, other}}
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  # Starts coordinators for newly-led vnodes and stops them for no-longer-led ones (stop first, so a
  # vnode that changed hands frees its coordinators before the new set spins up).
  defp reconcile_placement(state, placement) do
    desired = MapSet.new(state.leading.(placement))
    running = MapSet.new(Map.keys(state.running))
    stopping = MapSet.difference(running, desired)
    starting = MapSet.difference(desired, running)
    servers = state.version_servers.(placement)

    state =
      state
      |> stop_vnodes(stopping)
      |> start_vnodes(starting)
      |> check_versions(servers)
      |> placement_readable()

    Telemetry.vnode_reconcile(
      length(placement),
      length(servers),
      MapSet.size(desired),
      MapSet.size(starting),
      MapSet.size(stopping),
      :ok
    )

    state
  end

  # Nothing starts, nothing stops, no version status is recomputed: the set stays exactly as the last
  # good read left it. Reported every pass, logged only on the way in.
  defp placement_unreadable(state, reason) do
    Telemetry.vnode_reconcile(0, map_size(state.version_status), map_size(state.running), 0, 0, :unreadable)

    if state.placement_status == :ok do
      Logger.warning(I18n.t(:vnode_placement_unreadable, reason: inspect(reason)))
    end

    %{state | placement_status: :unreadable}
  end

  defp placement_readable(%{placement_status: :unreadable} = state) do
    Logger.info(I18n.t(:vnode_placement_recovered))
    %{state | placement_status: :ok}
  end

  defp placement_readable(state), do: state

  defp check_versions(state, servers) do
    version_status =
      Map.new(servers, fn {machine, server_id} ->
        last_status = Map.get(state.version_status, server_id, :ok)
        {status, _transition} = MachineVersion.check(machine, server_id, last_status)
        {server_id, status}
      end)

    %{state | version_status: version_status}
  end

  defp start_vnodes(state, vnode_ids) do
    running =
      Enum.reduce(vnode_ids, state.running, fn vnode_id, acc ->
        pid = state.spawn.(vnode_id)
        ref = Process.monitor(pid)
        Map.put(acc, vnode_id, {pid, ref})
      end)

    %{state | running: running}
  end

  defp stop_vnodes(state, vnode_ids) do
    running =
      Enum.reduce(vnode_ids, state.running, fn vnode_id, acc ->
        {{pid, ref}, acc} = Map.pop(acc, vnode_id)
        # Demonitor before stopping so the stop we are about to cause is not delivered back as a :DOWN
        # and mistaken for an unbidden death; [:flush] also drops a :DOWN already in the mailbox.
        Process.demonitor(ref, [:flush])
        state.stop.(pid)
        acc
      end)

    %{state | running: running}
  end
end
