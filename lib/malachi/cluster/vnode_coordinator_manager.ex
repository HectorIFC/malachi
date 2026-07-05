defmodule Malachi.Cluster.VnodeCoordinatorManager do
  @moduledoc """
  Keeps this node's per-vnode coordinators in sync with the vnodes it currently leads (1C-b-ii). On a
  **level-triggered** reconcile — right after start (`handle_continue`) and every `:interval` ms — it
  compares the vnodes this node leads now (`:leading`) with the ones it already runs coordinators for,
  then **starts** coordinators for newly-led vnodes and **stops** them for vnodes it no longer leads.
  Generic and testable via seams:

    * `:leading` - `(-> [vnode_id])`, the vnodes this node currently leads (e.g. `leading_vnodes/3`
      over live Raft leadership);
    * `:spawn` - `(vnode_id -> handle)`, starts that vnode's coordinators (e.g. under a
      `DynamicSupervisor`) and returns an opaque handle to later stop them by;
    * `:stop` - `(handle -> any)`, stops a vnode's coordinators;
    * `:interval` - reconcile period in ms (default 5_000);
    * `:name` - optional registered name.

  Idempotent: a transient leadership flap just starts/stops coordinators; the underlying work is
  idempotent and routed through `ra`, so a brief double-run only redoes work (the same reasoning as
  1C-a, hence no lease). `reconcile_now/1` reconciles synchronously and returns the running vnode ids
  (a manual trigger, e.g. for tests).
  """

  use GenServer

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
        Map.put(acc, vnode_id, state.spawn.(vnode_id))
      end)

    %{state | running: running}
  end

  defp stop_vnodes(state, vnode_ids) do
    running =
      Enum.reduce(vnode_ids, state.running, fn vnode_id, acc ->
        {handle, acc} = Map.pop(acc, vnode_id)
        state.stop.(handle)
        acc
      end)

    %{state | running: running}
  end
end
