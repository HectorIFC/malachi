defmodule Malachi.Cluster.RetentionCoordinator do
  @moduledoc """
  Periodically expires sealed segments that exceed the retention policy, using the pure
  `Malachi.Cluster.Retention` decision and executing it through injected seams, so it is testable
  in-process and wired to the real broker/replication later without change:

    * `:metadata_source` - `(-> Malachi.Metadata.t())`, the current control-plane metadata;
    * `:expire_segment` - `(Malachi.Metadata.segment_meta() -> any)`, removes one expired segment
      from the control plane **and** deletes its stored data on the replicas;
    * `:policy` - a `Malachi.Cluster.Retention.policy()` (`:max_age_ms` / `:max_bytes`; `nil` = off);
    * `:clock` - `(-> non_neg_integer())` epoch ms (default `System.system_time/1`);
    * `:interval` - the sweep period in ms (default 60_000);
    * `:leader?` - `(-> boolean())`, whether this node should sweep (default always). Only the cluster's
      membership leader sweeps, so N nodes do not redo the same work (1C); a non-leader still ticks but
      skips the sweep.

  Each sweep asks `Retention.expired/3` which sealed segments to drop, resolves each to its metadata
  (for its replica set), and calls `expire_segment` on it. `run_now/1` runs one sweep synchronously,
  ignoring `:leader?` (it is a manual trigger).
  """

  use GenServer

  alias Malachi.Cluster.Retention
  alias Malachi.Metadata

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
    state = %{
      metadata_source: Keyword.fetch!(opts, :metadata_source),
      expire_segment: Keyword.fetch!(opts, :expire_segment),
      policy: Keyword.fetch!(opts, :policy),
      clock: Keyword.get(opts, :clock, fn -> System.system_time(:millisecond) end),
      interval: Keyword.get(opts, :interval, @default_interval),
      leader?: Keyword.get(opts, :leader?, fn -> true end)
    }

    schedule(state)
    {:ok, state}
  end

  @impl true
  def handle_call(:run_now, _from, state), do: {:reply, run(state), state}

  @impl true
  def handle_info(:tick, state) do
    if state.leader?.(), do: run(state)
    schedule(state)
    {:noreply, state}
  end

  defp run(state) do
    metadata = state.metadata_source.()
    now_ms = state.clock.()
    expired_ids = Retention.expired(metadata, now_ms, state.policy)

    for id <- expired_ids, segment = Metadata.get_segment(metadata, id), do: state.expire_segment.(segment)
    expired_ids
  end

  defp schedule(state), do: Process.send_after(self(), :tick, state.interval)
end
