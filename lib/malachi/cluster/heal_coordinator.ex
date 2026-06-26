defmodule Malachi.Cluster.HealCoordinator do
  @moduledoc """
  Drives reactive self-healing: periodically (and on demand) it re-replicates under-replicated
  **sealed** segments to the currently live brokers, closing the loop *broker dies → membership
  marks it gone → its segments are healed*.

  It is decoupled from where membership and metadata actually live, via injected seams — so it can
  be tested in-process and wired to the real `Malachi.Cluster.MembershipServer` and control plane
  later without change:

    * `:live_brokers` - `(-> [broker])`, the currently alive broker set (e.g. from
      `Malachi.Cluster.MembershipServer.alive_members/1`);
    * `:metadata_source` - `(-> Malachi.Metadata.t())`, the current metadata;
    * `:apply_command` - `(Malachi.Metadata.command() -> any)`, applies a `:set_segment_replicas`
      command to the control plane;
    * `:replication_factor` - the target replica count;
    * `:interval` - the healing period in ms (default 5000);
    * `:heal_opts` - forwarded to `Malachi.Cluster.SelfHealing.heal_sealed/4` (e.g. `:batch_size`).

  Each pass **reconciles** against the live set: it runs `Malachi.Cluster.SelfHealing.heal_sealed/4`
  (re-replicating under-replicated sealed segments, backfilling via `Malachi.Cluster.Catchup`) and
  `Malachi.Cluster.Failover.plan/2` (promoting a live replica to primary for active segments whose
  primary died), and applies all resulting commands. `heal_now/1` runs one pass synchronously and
  returns the combined result, for tests and manual triggers.
  """

  use GenServer

  alias Malachi.Cluster.Failover
  alias Malachi.Cluster.SelfHealing

  @default_interval 5_000

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
    state = %{
      live_brokers: Keyword.fetch!(opts, :live_brokers),
      metadata_source: Keyword.fetch!(opts, :metadata_source),
      apply_command: Keyword.fetch!(opts, :apply_command),
      replication_factor: Keyword.fetch!(opts, :replication_factor),
      interval: Keyword.get(opts, :interval, @default_interval),
      heal_opts: Keyword.get(opts, :heal_opts, [])
    }

    schedule(state)
    {:ok, state}
  end

  @impl true
  def handle_call(:heal_now, _from, state), do: {:reply, run(state), state}

  @impl true
  def handle_info(:tick, state) do
    run(state)
    schedule(state)
    {:noreply, state}
  end

  # --- internals ---

  defp run(state) do
    live = state.live_brokers.()
    metadata = state.metadata_source.()

    healed = SelfHealing.heal_sealed(metadata, live, state.replication_factor, state.heal_opts)
    promotions = Failover.plan(metadata, live)

    applied = healed.applied ++ promotions
    Enum.each(applied, state.apply_command)
    %{applied: applied, failed: healed.failed}
  end

  defp schedule(state), do: Process.send_after(self(), :tick, state.interval)
end
