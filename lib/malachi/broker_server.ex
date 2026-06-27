defmodule Malachi.BrokerServer do
  @moduledoc """
  A `GenServer` that owns a `Malachi.Broker` (the control-plane router) and a local
  `Malachi.Cluster.ReplicationServer` (segment storage/replication), serializing concurrent
  access. It wires the broker's injected effect functions to the replication server:
  `produce` replicates through it and `read`/`stream_history` read segments from it.

  Writes are durable on return: each batch is fsynced on a quorum by the replication server
  before it commits, so there is no buffering and no time-based flush — `sync/1` is a no-op kept
  for API compatibility.

  The `Broker` (and the layers it composes) are pure immutable values; routing all mutations
  through this single process is what makes concurrent producers/consumers safe.

  Supersedes `Malachi.TopicServer`.
  """

  use GenServer

  alias Malachi.Broker
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.ReplicatedMetadata
  alias Malachi.Cluster.ReplicationServer

  @default_brokers_refresh_interval 1_000

  # --- client API ---

  @doc """
  Starts a server whose segment storage is rooted at `directory`.

  ## Options
    * `:brokers` - references of the `Malachi.Cluster.ReplicationServer`s that segments are placed
      on. When given, this server uses them and does not start its own; when omitted, it starts a
      single local replication server rooted at `directory` (single-node default).
    * `:live_brokers` - a `(-> [broker])` (e.g. from membership); when given, the placement broker
      set is refreshed from it every `:brokers_refresh_interval` ms, so new segments land on
      currently-alive brokers. An empty result is ignored (the last non-empty set is kept).
    * `:brokers_refresh_interval` - refresh period in ms (default 1000).
    * `:metadata_cluster` - a Raft cluster name (atom). When given, the metadata is made
      authoritative via that `ra` cluster (mutations go through the log; reads come from a local
      cache); `ra` must already be running. When omitted, metadata is in-memory (single node).
    * `:metadata_nodes` - the nodes the metadata Raft cluster spans (default `[node()]`); several
      nodes make the control plane HA (the metadata survives losing a member).
    * `:replication_factor` - replicas per segment (default 1; clamped to the broker count).
    * `:segment_max_bytes` - byte threshold at which the active segment seals and rolls.
    * remaining options are forwarded to a started `Malachi.Cluster.ReplicationServer` (segment log
      options such as `:max_bytes`, `:flush_bytes`, `:index_interval`); ignored with `:brokers`.
    * standard `GenServer` options (`:name`, etc.) are honored.
  """
  @spec start_link(Path.t(), keyword()) :: GenServer.on_start()
  def start_link(directory, opts \\ []) do
    {gen_server_opts, broker_opts} =
      Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after])

    GenServer.start_link(__MODULE__, {directory, broker_opts}, gen_server_opts)
  end

  @doc "Creates a topic (and its root range); returns `{:ok, root_range_id}` or an error."
  @spec create_topic(GenServer.server(), Malachi.Metadata.topic_name(), pos_integer()) :: term()
  def create_topic(server, name, keyspace_bits),
    do: GenServer.call(server, {:create_topic, name, keyspace_bits})

  @doc "Routes, replicates and commits records; returns `{:ok, placements}` or an error."
  @spec produce(GenServer.server(), Malachi.Metadata.topic_name(), [Malachi.Log.Record.t()]) ::
          {:ok, %{Malachi.Metadata.range_id() => {non_neg_integer(), non_neg_integer()}}}
          | {:error, term()}
  def produce(server, topic, records), do: GenServer.call(server, {:produce, topic, records})

  @doc "Reads up to `max_records` committed records from a range, starting at `offset`."
  @spec read(GenServer.server(), Malachi.Metadata.range_id(), non_neg_integer(), pos_integer()) ::
          {:ok, [Malachi.Log.Record.t()]} | :eof | {:error, term()}
  def read(server, range_id, offset, max_records),
    do: GenServer.call(server, {:read, range_id, offset, max_records})

  @doc "Streams one bounded page of a range's cross-epoch history (see `Malachi.Broker.stream_history/5`)."
  @spec stream_history(GenServer.server(), Malachi.Metadata.range_id(), Broker.history_cursor(), pos_integer()) ::
          {:ok, [Malachi.Log.Record.t()], Broker.history_cursor()} | {:error, term()}
  def stream_history(server, range_id, cursor \\ :start, max_records \\ 1000),
    do: GenServer.call(server, {:stream_history, range_id, cursor, max_records})

  @doc "No-op: writes are already durable on return. Kept for API compatibility."
  @spec sync(GenServer.server()) :: :ok
  def sync(server), do: GenServer.call(server, :sync)

  @doc "Splits a range; returns `{:ok, left_id, right_id}` or an error."
  @spec split_range(GenServer.server(), Malachi.Metadata.range_id()) ::
          {:ok, Malachi.Metadata.range_id(), Malachi.Metadata.range_id()} | {:error, term()}
  def split_range(server, range_id), do: GenServer.call(server, {:split_range, range_id})

  @doc "Merges two buddy ranges; returns `{:ok, child_id}` or an error."
  @spec merge_ranges(GenServer.server(), Malachi.Metadata.range_id(), Malachi.Metadata.range_id()) ::
          {:ok, Malachi.Metadata.range_id()} | {:error, term()}
  def merge_ranges(server, range_id_a, range_id_b),
    do: GenServer.call(server, {:merge_ranges, range_id_a, range_id_b})

  @doc "The ids of a topic's active ranges."
  @spec active_range_ids(GenServer.server(), Malachi.Metadata.topic_name()) :: [Malachi.Metadata.range_id()]
  def active_range_ids(server, topic), do: GenServer.call(server, {:active_range_ids, topic})

  @doc "The current control-plane metadata (e.g. for a healing coordinator to inspect)."
  @spec metadata(GenServer.server()) :: Malachi.Metadata.t()
  def metadata(server), do: GenServer.call(server, :metadata)

  @doc "Applies `:set_segment_replicas` healing commands to the control plane."
  @spec apply_heal(GenServer.server(), [Malachi.Metadata.command()]) :: :ok
  def apply_heal(server, commands), do: GenServer.call(server, {:apply_heal, commands})

  @doc "Stops the server (and its replication storage)."
  @spec stop(GenServer.server()) :: :ok
  def stop(server), do: GenServer.stop(server)

  # --- server callbacks ---

  @impl true
  def init({directory, opts}) do
    {segment_max_bytes, opts} = Keyword.pop(opts, :segment_max_bytes)
    {replication_factor, opts} = Keyword.pop(opts, :replication_factor, 1)
    {live_brokers, opts} = Keyword.pop(opts, :live_brokers)
    {refresh_interval, opts} = Keyword.pop(opts, :brokers_refresh_interval, @default_brokers_refresh_interval)
    {metadata_cluster, opts} = Keyword.pop(opts, :metadata_cluster)
    {metadata_nodes, opts} = Keyword.pop(opts, :metadata_nodes, [node()])
    {external_brokers, log_opts} = Keyword.pop(opts, :brokers)

    # With an external broker set we use it as-is; otherwise we own a single local store.
    {owned_replication, brokers} =
      case external_brokers do
        nil ->
          {:ok, replication} = ReplicationServer.start_link([directory: directory] ++ log_opts)
          {replication, [replication]}

        list ->
          {nil, list}
      end

    broker_opts =
      [brokers: brokers, replication_factor: replication_factor]
      |> maybe_put(:segment_max_bytes, segment_max_bytes)
      |> with_metadata_authority(metadata_cluster, metadata_nodes)

    {:ok, broker} = Broker.open(broker_opts)

    state = %{
      broker: broker,
      replication: owned_replication,
      live_brokers: live_brokers,
      refresh_interval: refresh_interval
    }

    if live_brokers, do: schedule_refresh(state)
    {:ok, state}
  end

  @impl true
  def handle_call({:create_topic, name, keyspace_bits}, _from, state) do
    {broker, reply} = Broker.create_topic(state.broker, name, keyspace_bits)
    {:reply, reply, %{state | broker: broker}}
  end

  def handle_call({:produce, topic, records}, _from, state) do
    {broker, reply} = Broker.produce(state.broker, topic, records, &ReplicationServer.replicate/5)
    {:reply, reply, %{state | broker: broker}}
  end

  def handle_call({:read, range_id, offset, max_records}, _from, state) do
    {:reply, Broker.read(state.broker, range_id, offset, max_records, &ReplicationServer.read/4), state}
  end

  def handle_call({:stream_history, range_id, cursor, max_records}, _from, state) do
    reply = Broker.stream_history(state.broker, range_id, cursor, max_records, &ReplicationServer.read/4)
    {:reply, reply, state}
  end

  def handle_call(:sync, _from, state) do
    {:reply, :ok, state}
  end

  def handle_call({:split_range, range_id}, _from, state) do
    {broker, reply} = Broker.split_range(state.broker, range_id)
    {:reply, reply, %{state | broker: broker}}
  end

  def handle_call({:merge_ranges, range_id_a, range_id_b}, _from, state) do
    {broker, reply} = Broker.merge_ranges(state.broker, range_id_a, range_id_b)
    {:reply, reply, %{state | broker: broker}}
  end

  def handle_call({:active_range_ids, topic}, _from, state) do
    {:reply, Broker.active_range_ids(state.broker, topic), state}
  end

  def handle_call(:metadata, _from, state) do
    {:reply, Broker.metadata(state.broker), state}
  end

  def handle_call({:apply_heal, commands}, _from, state) do
    {:reply, :ok, %{state | broker: Broker.apply_heal(state.broker, commands)}}
  end

  @impl true
  def handle_info(:refresh_brokers, state) do
    broker =
      case state.live_brokers.() do
        [] -> state.broker
        live -> Broker.set_brokers(state.broker, live)
      end

    schedule_refresh(state)
    {:noreply, %{state | broker: broker}}
  end

  @impl true
  def terminate(_reason, state) do
    # Only stop the replication server we started ourselves; an external broker set is not ours.
    if state.replication && Process.alive?(state.replication), do: GenServer.stop(state.replication)
    :ok
  end

  # --- internals ---

  defp schedule_refresh(state), do: Process.send_after(self(), :refresh_brokers, state.refresh_interval)

  # In-memory metadata by default; with a cluster name, route mutations through that ra cluster and
  # seed the broker's cache from the replicated state (so it recovers prior metadata on start).
  defp with_metadata_authority(opts, nil, _nodes), do: opts

  defp with_metadata_authority(opts, cluster_name, nodes) do
    {:ok, server_id} = MetadataServer.start(cluster_name, nodes)
    {:ok, seed} = MetadataServer.query(server_id, & &1)

    opts
    |> Keyword.put(:metadata, seed)
    |> Keyword.put(:command_fun, &ReplicatedMetadata.apply_command(server_id, &1, &2))
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)
end
