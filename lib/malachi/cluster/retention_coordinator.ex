defmodule Malachi.Cluster.RetentionCoordinator do
  @moduledoc """
  Periodically expires sealed segments that exceed the retention policy, using the pure
  `Malachi.Cluster.Retention` decision and executing it through injected seams, so it is testable
  in-process and wired to the real broker/replication later without change:

    * `:metadata_source` - `(-> Malachi.Metadata.t())`, the current control-plane metadata;
    * `:expire_segment` - `(Malachi.Metadata.segment_meta() -> :ok | {:error, term()})`, removes one
      expired segment from the control plane **and** deletes its stored data on the replicas, and
      answers what the control plane answered (labeled by `Malachi.Cluster.Retention.reply_label/1`);
    * `:policy` - a `Malachi.Cluster.Retention.policy()` (`:max_age_ms` / `:max_bytes`; `nil` = off);
    * `:policies` - `(-> {:ok, %{name => Malachi.Cluster.Policy.t()}} | {:error, term()})`, the
      cluster's policy definitions, resolved ONCE per sweep (default
      `Malachi.Cluster.PolicyStore.fetch_all/0`). A topic's own metadata says which name it points at;
      the definitions are an administrative object of the cluster. An error skips the sweep entirely,
      for the reason `Malachi.Cluster.PolicyStore` gives: expiring under the global limits because the
      policy could not be read deletes exactly what the policy existed to keep;
    * `:unresolved_policy_max_age_ms` - the backstop for a topic pointing at a name that does not
      resolve (default none, meaning nothing expires for it). Off by default because a bound nobody
      stated is not one this code gets to invent, and the counter below is what makes the case visible;
    * `:clock` - `(-> non_neg_integer())` epoch ms (default `System.system_time/1`);
    * `:interval` - the sweep period in ms (default 60_000);
    * `:leader?` - `(-> boolean())`, whether this node should sweep (default always). Only the cluster's
      membership leader sweeps, so N nodes do not redo the same work (1C); a non-leader still ticks but
      skips the sweep.

  Each sweep asks `Retention.expired/3` which sealed segments to drop, resolves each to its metadata
  (for its replica set), and calls `expire_segment` on it. `run_now/1` runs one sweep synchronously,
  ignoring `:leader?` (it is a manual trigger).

  Every segment it tries emits `[:malachi, :retention, :expire]` and every sweep that runs emits
  `[:malachi, :retention, :sweep]` (see `Malachi.Telemetry`), so an operator can tell whether the sweep
  runs, how much it deleted and whether the control plane refused any of it.
  """

  use GenServer

  require Logger

  alias Malachi.Cluster.PeriodicWorker
  alias Malachi.Cluster.PolicyStore
  alias Malachi.Cluster.Retention
  alias Malachi.I18n
  alias Malachi.Metadata
  alias Malachi.Telemetry

  @default_interval 60_000

  @doc "Starts the coordinator. See the module doc for required options."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_server_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @doc "Runs one retention sweep synchronously; returns the list of expired segment ids."
  @spec run_now(GenServer.server()) :: [Metadata.segment_id()]
  def run_now(server), do: GenServer.call(server, :run_now)

  @impl true
  def init(opts) do
    state =
      Map.merge(PeriodicWorker.new(opts, :retention, @default_interval, :retention_interval_ms), %{
        metadata_source: Keyword.fetch!(opts, :metadata_source),
        expire_segment: Keyword.fetch!(opts, :expire_segment),
        policy: Keyword.fetch!(opts, :policy),
        policies: Keyword.get(opts, :policies, &PolicyStore.fetch_all/0),
        unresolved_policy_max_age_ms: Keyword.get(opts, :unresolved_policy_max_age_ms),
        clock: Keyword.get(opts, :clock, fn -> System.system_time(:millisecond) end),
        leader?: Keyword.get(opts, :leader?, fn -> true end),
        # Whether the last pass already said the policies could not be read, so a store that stays
        # down says it once rather than every interval.
        skipping?: false
      })

    PeriodicWorker.schedule(state)
    {:ok, state}
  end

  @impl true
  def handle_call(:run_now, _from, state) do
    {expired_ids, state} = run(state)
    {:reply, expired_ids, state}
  end

  def handle_call(message, _from, state), do: PeriodicWorker.unknown_call(state, message)

  # Nothing casts to this server; without this clause, `use GenServer` would stop it on the first cast.
  @impl true
  def handle_cast(message, state), do: PeriodicWorker.unknown_cast(state, message)

  @impl true
  def handle_info(:tick, state), do: PeriodicWorker.tick(state, &sweep_if_leader/1)

  def handle_info(message, state), do: PeriodicWorker.unknown_info(state, message)

  # The gate belongs to the tick alone: `run_now/1` is a manual trigger and ignores it.
  defp sweep_if_leader(state) do
    if state.leader?.(), do: state |> run() |> elem(1), else: state
  end

  defp run(state) do
    case state.policies.() do
      {:ok, policies} -> sweep(state, policies)
      {:error, reason} -> skip(state, reason)
    end
  end

  # A sweep that could not read the policies is a sweep that does not happen. It emits no sweep event
  # on purpose: `malachi_retention_sweep_duration_seconds`'s count is what an operator alerts on to
  # know a sweep is running at all, and a skipped sweep is exactly the thing that alert exists to
  # surface. Logged on the transition, so a store that stays down says it once rather than every minute.
  defp skip(state, reason) do
    unless state.skipping? do
      Logger.warning(I18n.t(:retention_policies_unreadable, reason: inspect(reason)))
    end

    {[], %{state | skipping?: true}}
  end

  defp sweep(state, policies) do
    started = System.monotonic_time()
    metadata = state.metadata_source.()
    now_ms = state.clock.()
    expired_ids = Retention.expired(metadata, now_ms, state.policy, policies, state.unresolved_policy_max_age_ms)

    for topic <- Retention.unresolved_policies(metadata, policies), do: Telemetry.retention_unresolved_policy(topic)

    labels = for id <- expired_ids, segment = Metadata.get_segment(metadata, id), do: expire(state, segment)

    duration_us = System.convert_time_unit(System.monotonic_time() - started, :native, :microsecond)
    Telemetry.retention_sweep(duration_us, Enum.count(labels, &(&1 == :ok)), Enum.count(labels, &failed?/1))
    {expired_ids, %{state | skipping?: false}}
  end

  defp expire(state, segment) do
    label = segment |> state.expire_segment.() |> Retention.reply_label()
    Telemetry.retention_expire(elem(segment.range_id, 0), segment.id, segment.byte_size || 0, label)
    label
  end

  # A segment already gone was deleted by an earlier sweep: not this sweep's expiry, and not a failure.
  defp failed?(label), do: label not in [:ok, :no_such_segment]
end
