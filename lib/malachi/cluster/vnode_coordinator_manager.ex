defmodule Malachi.Cluster.VnodeCoordinatorManager do
  @moduledoc """
  Keeps this node's per-vnode coordinators in sync with the vnodes it currently leads (1C-b-ii). On a
  **level-triggered** reconcile. Right after start (`handle_continue`) and every `:interval` ms - it
  compares the vnodes this node leads now (`:leading`) with the ones it already runs coordinators for,
  then **starts** coordinators for newly-led vnodes and **stops** them for vnodes it no longer leads.
  Generic and testable via seams:

    * `:leading` - `(-> [vnode_id])`, the vnodes this node currently leads (e.g. `leading_vnodes/3`
      over live Raft leadership);
    * `:spawn` - `(vnode_id -> pid)`, starts that vnode's coordinators (e.g. a `Supervisor` under a
      `DynamicSupervisor`) and returns the pid of that (sub)tree, which the manager monitors and later
      stops by;
    * `:stop` - `(pid -> any)`, stops a vnode's coordinators;
    * `:interval` - reconcile period in ms (default 5_000);
    * `:name` - optional registered name.

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
  """

  use GenServer

  require Logger

  @default_interval 5_000

  @doc "Starts the manager. See the module doc for required options."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_server_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @doc "Reconciles once synchronously; returns the vnode ids currently running coordinators."
  @spec reconcile_now(GenServer.server()) :: [term()]
  def reconcile_now(server), do: GenServer.call(server, :reconcile_now)

  @impl true
  def init(opts) do
    state = %{
      leading: Keyword.fetch!(opts, :leading),
      spawn: Keyword.fetch!(opts, :spawn),
      stop: Keyword.fetch!(opts, :stop),
      interval: Keyword.get(opts, :interval, @default_interval),
      running: %{}
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
        Logger.warning(
          "vnode #{inspect(vnode_id)} coordinators went down (#{inspect(reason)}); " <>
            "restarting on the next reconcile"
        )

        {:noreply, %{state | running: Map.delete(state.running, vnode_id)}}

      nil ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_call(:reconcile_now, _from, state) do
    state = reconcile(state)
    {:reply, Map.keys(state.running), state}
  end

  defp reconcile_and_schedule(state) do
    schedule(state)
    reconcile(state)
  end

  defp schedule(state), do: Process.send_after(self(), :reconcile, state.interval)

  # Starts coordinators for newly-led vnodes and stops them for no-longer-led ones (stop first, so a
  # vnode that changed hands frees its coordinators before the new set spins up).
  defp reconcile(state) do
    desired = MapSet.new(state.leading.())
    running = MapSet.new(Map.keys(state.running))

    state
    |> stop_vnodes(MapSet.difference(running, desired))
    |> start_vnodes(MapSet.difference(desired, running))
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
