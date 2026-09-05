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
    * `:interval` - the healing period in ms (default 5000);
    * `:leader?` - `(-> boolean())`, whether this node should heal this pass (default always). Only the
      cluster's membership leader heals, so N nodes do not redo the same work (1C); a non-leader still
      ticks but skips the pass. `heal_now/1` is a manual trigger and always runs;
    * `:heal_opts` - forwarded to `Malachi.Cluster.SelfHealing.heal_sealed/4` (e.g. `:batch_size`).

  Each pass **reconciles** against the live set: it runs `Malachi.Cluster.SelfHealing.heal_sealed/4`
  (re-replicating under-replicated sealed segments, backfilling via `Malachi.Cluster.Catchup`) and
  `Malachi.Cluster.Failover.plan/4` (sealing active segments whose primary died, so writing rolls to a
  fresh segment), and applies all resulting commands. `heal_now/1` runs one pass synchronously and
  returns the combined result, for tests and manual triggers.

  Failover needs to know what each surviving replica holds, which no pure function can answer, so this
  pass does the probing: `Failover.candidates/2` names the segments, each live replica is asked for its
  `durable_stats`, and the answers go to `Failover.plan/4`. A replica that does not answer in time
  simply does not count, which is what leaves a segment below a majority unsealed and its range
  blocked; that case is logged every pass, because a blocked range that says nothing is the failure
  mode worth avoiding.

    * `:probe` - `((replica, segment_id, base_offset) -> {end_offset, byte_size} | :error)`, how a
      replica is asked (default `Malachi.Cluster.ReplicationServer.durable_stats/4` with a short
      timeout, so an unreachable replica cannot stall the pass);
    * `:probe_timeout` - ms for that default probe (default 1000).
  """

  use GenServer

  require Logger

  alias Malachi.Cluster.Failover
  alias Malachi.Cluster.ReplicationServer
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
      leader?: Keyword.get(opts, :leader?, fn -> true end),
      heal_opts: Keyword.get(opts, :heal_opts, []),
      # `(-> {attribute_key, attributes} | nil)`: the current spread for rack/DC-aware re-replication,
      # resolved per pass so it tracks live membership. Default: no spread.
      spread: Keyword.get(opts, :spread, fn -> nil end),
      probe: Keyword.get(opts, :probe, default_probe(Keyword.get(opts, :probe_timeout, 1_000)))
    }

    schedule(state)
    {:ok, state}
  end

  @impl true
  def handle_call(:heal_now, _from, state), do: {:reply, run(state), state}

  @impl true
  def handle_info(:tick, state) do
    if state.leader?.(), do: run(state)
    schedule(state)
    {:noreply, state}
  end

  # --- internals ---

  defp run(state) do
    live = state.live_brokers.()
    metadata = state.metadata_source.()

    heal_opts = put_spread(state.heal_opts, state.spread.())
    healed = SelfHealing.heal_sealed(metadata, live, state.replication_factor, heal_opts)
    seals = Failover.plan(metadata, live, probe_candidates(state, metadata, live), System.system_time(:millisecond))

    applied = healed.applied ++ seals
    Enum.each(applied, state.apply_command)

    # A heal that cannot complete leaves the cluster under-replicated; the periodic tick used to
    # discard the result, making persistent failures invisible until something else broke.
    if healed.failed != [] do
      Logger.warning("healing pass could not repair #{length(healed.failed)}: #{inspect(healed.failed)}")
    end

    %{applied: applied, failed: healed.failed, repaired: healed.repaired}
  end

  # Adds the resolved spread to the heal opts for this pass (nil = leave them unchanged).
  defp put_spread(opts, nil), do: opts
  defp put_spread(opts, spread), do: Keyword.put(opts, :spread, spread)

  # Asks every live replica of every failover candidate what it holds. The impure half of the
  # failover decision: `Failover` stays a pure function of these answers.
  defp probe_candidates(state, metadata, live) do
    metadata
    |> Failover.candidates(live)
    |> Map.new(fn {segment_id, replicas} ->
      segment = Map.fetch!(metadata.segments, segment_id)
      answers = for r <- replicas, stats = probe(state, r, segment_id, segment.start_offset), into: %{}, do: {r, stats}
      warn_if_blocked(segment_id, answers, segment.replica_set)
      {segment_id, answers}
    end)
  end

  # `nil` (rather than an error tuple) so the comprehension above filters a silent replica out: a
  # replica that cannot answer tells us nothing about what it holds, and counting it would be the same
  # mistake as sealing on a guess.
  defp probe(state, replica, segment_id, base_offset) do
    case state.probe.(replica, segment_id, base_offset) do
      {end_offset, byte_size} when is_integer(end_offset) and is_integer(byte_size) -> {end_offset, byte_size}
      _other -> nil
    end
  end

  defp warn_if_blocked(segment_id, answers, replica_set) do
    unless Failover.majority?(map_size(answers), replica_set) do
      Logger.warning(
        "segment #{inspect(segment_id)} cannot be sealed for failover: #{map_size(answers)} of " <>
          "#{length(replica_set)} replicas answered, no majority. Its range is blocked for writes " <>
          "until a majority returns, because sealing on a minority could discard acknowledged writes"
      )
    end
  end

  # ReplicationServer.durable_stats/4 with a short timeout, wrapped so an unreachable replica is a
  # silent one rather than a crashed healing pass.
  defp default_probe(timeout) do
    fn replica, segment_id, base_offset ->
      try do
        ReplicationServer.durable_stats(replica, segment_id, base_offset, timeout)
      catch
        :exit, _reason -> :error
      end
    end
  end

  defp schedule(state), do: Process.send_after(self(), :tick, state.interval)
end
