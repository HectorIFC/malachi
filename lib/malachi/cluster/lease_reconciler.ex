defmodule Malachi.Cluster.LeaseReconciler do
  @moduledoc """
  Periodically reconciles this node into the lease cluster (R3-b-iii hardening). On start and every
  `:interval` ms it calls the injected `:reconcile` seam, wired to `LeaseServer.reconcile/2` - which
  bootstraps the lease cluster if unformed and starts the local server so a node that was down when the
  cluster first formed rejoins. Level-triggered and idempotent (a joined node's reconcile is a no-op), so
  it just keeps ticking; it does not track state. Keeps the `LeaseHolder` free of ra/membership concerns.

  Seams:
    * `:reconcile` - `(-> any)`, one reconcile pass;
    * `:interval` - reconcile period in ms (default 30_000);
    * `:name` - optional registered name.
  """

  use GenServer

  @default_interval 30_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_server_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @doc "Runs one reconcile pass synchronously (a manual trigger, e.g. for tests)."
  @spec reconcile_now(GenServer.server()) :: :ok
  def reconcile_now(server), do: GenServer.call(server, :reconcile_now)

  @impl true
  def init(opts) do
    state = %{
      reconcile: Keyword.fetch!(opts, :reconcile),
      interval: Keyword.get(opts, :interval, @default_interval)
    }

    {:ok, state, {:continue, :reconcile}}
  end

  @impl true
  def handle_continue(:reconcile, state) do
    reconcile_and_schedule(state)
    {:noreply, state}
  end

  @impl true
  def handle_info(:reconcile, state) do
    reconcile_and_schedule(state)
    {:noreply, state}
  end

  @impl true
  def handle_call(:reconcile_now, _from, state) do
    state.reconcile.()
    {:reply, :ok, state}
  end

  defp reconcile_and_schedule(state) do
    Process.send_after(self(), :reconcile, state.interval)
    state.reconcile.()
  end
end
