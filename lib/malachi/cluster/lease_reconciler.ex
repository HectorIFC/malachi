defmodule Malachi.Cluster.LeaseReconciler do
  @moduledoc """
  Periodically reconciles this node into the lease cluster (R3-b-iii hardening). On start and every
  `:interval` ms it calls the injected `:reconcile` seam, wired to `LeaseServer.reconcile/2` - which
  bootstraps the lease cluster if unformed and starts the local server so a node that was down when the
  cluster first formed rejoins. Level-triggered and idempotent (a joined node's reconcile is a no-op), so
  it just keeps ticking. Keeps the `LeaseHolder` free of ra/membership concerns. Every replicated store
  (lease, ring, users, lockouts, ACLs) runs one of these.

  With `:version_check` it also asks `Malachi.Cluster.MachineVersion.check/3` on each tick whether the
  local member stopped applying its log because the group moved to a machine version this node does not
  support. The last answer is this process's own state, so a member that stays stuck is logged once,
  when it gets stuck, and again only when it recovers.

  Seams:
    * `:reconcile` - `(-> any)`, one reconcile pass;
    * `:version_check` - optional `{machine, server_id}`, the local member to watch;
    * `:interval` - reconcile period in ms (default 30_000);
    * `:name` - optional registered name.
  """

  use GenServer

  alias Malachi.Cluster.MachineVersion

  @default_interval 30_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_server_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @doc "Runs one reconcile pass synchronously (a manual trigger, e.g. for tests)."
  @spec reconcile_now(GenServer.server()) :: :ok
  def reconcile_now(server), do: GenServer.call(server, :reconcile_now)

  @doc "The last machine version status seen for the watched member (`:ok` when nothing is watched)."
  @spec version_status(GenServer.server()) :: MachineVersion.status()
  def version_status(server), do: GenServer.call(server, :version_status)

  @impl true
  def init(opts) do
    state = %{
      reconcile: Keyword.fetch!(opts, :reconcile),
      version_check: Keyword.get(opts, :version_check),
      version_status: :ok,
      interval: Keyword.get(opts, :interval, @default_interval)
    }

    {:ok, state, {:continue, :reconcile}}
  end

  @impl true
  def handle_continue(:reconcile, state), do: {:noreply, reconcile_and_schedule(state)}

  @impl true
  def handle_info(:reconcile, state), do: {:noreply, reconcile_and_schedule(state)}

  @impl true
  def handle_call(:reconcile_now, _from, state), do: {:reply, :ok, reconcile(state)}

  def handle_call(:version_status, _from, state), do: {:reply, state.version_status, state}

  defp reconcile_and_schedule(state) do
    Process.send_after(self(), :reconcile, state.interval)
    reconcile(state)
  end

  defp reconcile(state) do
    state.reconcile.()
    check_version(state)
  end

  defp check_version(%{version_check: nil} = state), do: state

  defp check_version(%{version_check: {machine, server_id}} = state) do
    {status, _transition} = MachineVersion.check(machine, server_id, state.version_status)
    %{state | version_status: status}
  end
end
