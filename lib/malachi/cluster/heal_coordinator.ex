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
  pass does the probing, in two steps whose order carries the safety. `Failover.candidates/2` names the
  segments; each live replica is MEASURED (`:probe`), which leaves it writable; and only once those
  answers reach a majority is each answering replica FENCED (`:fence`), which is what makes its answer
  final. The fence answers go to `Failover.plan/4`, which applies the majority rule again to them, so a
  fence that fails on enough replicas still declines rather than sealing on a minority.

  Fencing before knowing whether a majority answered would close replicas of a segment the pass then
  declines to seal, and nothing unseals a store: see `Malachi.Cluster.Failover`'s moduledoc for why that
  is terminal at `replication_factor: 2`. A replica that does not answer in time simply does not count,
  which is what leaves a segment below a majority unsealed and its range blocked; that case is logged
  every pass, because a blocked range that says nothing is the failure mode worth avoiding.

    * `:probe` - `((replica, segment_id, base_offset) -> {end_offset, byte_size} | :error)`, how a
      replica is MEASURED (default `Malachi.Cluster.ReplicationServer.durable_stats/4` with a short
      timeout, so an unreachable replica cannot stall the pass);
    * `:fence` - the same shape, how a replica is CLOSED once a majority has answered (default
      `Malachi.Cluster.ReplicationServer.seal/4`). Separate from `:probe` so a test can watch a pass
      measure without fencing, which is the property that must hold below a majority;
    * `:probe_timeout` - ms for both defaults (default 1000).
  """

  use GenServer

  require Logger

  alias Malachi.Cluster.Failover
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.SelfHealing
  alias Malachi.I18n

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
      # Two seams, not one, because the two calls differ in consequence: `:probe` measures and leaves
      # the replica writable, `:fence` closes it. Injectable separately so a test can watch a pass
      # measure without fencing, which is exactly the case that must hold below a majority.
      probe: Keyword.get(opts, :probe, default_probe(Keyword.get(opts, :probe_timeout, 1_000))),
      fence: Keyword.get(opts, :fence, default_fence(Keyword.get(opts, :probe_timeout, 1_000)))
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
      Logger.warning(I18n.t(:heal_repair_failed, count: length(healed.failed), failures: inspect(healed.failed)))
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
      {segment_id, fence_answered(state, segment, answers)}
    end)
  end

  # Measure first, fence second, and only once the measurement has shown a majority.
  #
  # The fence is what makes a seal point final, so it has to happen before the point is recorded. But it
  # has no inverse: nothing in the system unseals a replica's store. Fencing every replica that answers,
  # before knowing whether a majority did, therefore closes replicas of a segment this pass may then
  # decline to seal, and those replicas keep refusing writes after their primary comes back. With
  # `replication_factor: 2` that is terminal: one live follower is not a majority, so nothing is sealed,
  # and when the primary returns the segment is no longer a failover candidate, so no later pass ever
  # finishes the seal, while every produce fails quorum against the follower that stayed closed.
  #
  # Below a majority the pass leaves the replicas untouched and the range simply stays blocked until one
  # returns, which is the CP choice `Malachi.Cluster.Failover` already documents. At or above it, the
  # fence answers are what `Failover.plan/4` seals on, and it applies the majority rule again to them, so
  # a fence that fails on enough replicas still declines rather than sealing on a minority.
  defp fence_answered(state, segment, answers) do
    if Failover.majority?(map_size(answers), segment.replica_set) do
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
    unless Failover.majority?(map_size(answers), replica_set) do
      Logger.warning(
        I18n.t(:heal_seal_no_majority,
          segment_id: inspect(segment_id),
          answered: map_size(answers),
          replicas: length(replica_set)
        )
      )
    end
  end

  # Read-only: this is the measurement that decides whether a majority is even present. It flushes
  # before answering (see `ReplicationServer.durable_stats/4`), so what it reports is what a read can
  # serve, but it leaves the replica writable. `fence` below is the half with consequences.
  defp default_probe(timeout), do: answer_fun(&ReplicationServer.durable_stats/4, timeout)

  # The fence, applied only to segments the pass has already decided to seal (see `fence_answered/3`).
  # After it returns, that replica refuses every append to the segment, so the end it reports cannot
  # move afterwards: the seal point becomes a consequence of closing the segment rather than a number
  # racing it, which is what the `Malachi.Cluster.Failover` moduledoc claims.
  defp default_fence(timeout), do: answer_fun(&ReplicationServer.seal/4, timeout)

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

  defp schedule(state), do: Process.send_after(self(), :tick, state.interval)
end
