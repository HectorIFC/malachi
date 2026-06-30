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

  @doc "Reads one cross-epoch consume page of a range, tailing the active range (see `Malachi.Broker.read_consume/5`)."
  @spec read_consume(GenServer.server(), Malachi.Metadata.range_id(), Broker.consume_cursor(), pos_integer()) ::
          {:ok, [Malachi.Log.Record.t()], Broker.consume_cursor()} | {:error, term()}
  def read_consume(server, range_id, cursor, max_records),
    do: GenServer.call(server, {:read_consume, range_id, cursor, max_records})

  @doc """
  Consumes a topic's current ranges from `positions`, returning `{records, next_positions}`. When
  `wait_ms > 0` and nothing is available yet, the call blocks (long-poll) until a produce to the
  topic delivers data or `wait_ms` elapses (then `records` is `[]`). With `wait_ms == 0` it returns
  immediately. `positions`/`next_positions` map each range id to its `Broker.consume_cursor`.
  """
  @spec consume(GenServer.server(), Malachi.Metadata.topic_name(), map(), pos_integer(), non_neg_integer()) ::
          {[Malachi.Log.Record.t()], map()}
  def consume(server, topic, positions, max_records, wait_ms) do
    # The call may block up to wait_ms server-side; give it headroom over the default 5s call timeout.
    GenServer.call(server, {:consume, topic, positions, max_records, wait_ms}, wait_ms + 5_000)
  end

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

  @doc "Durably commits a consumer group's position for a topic; returns the control-plane reply."
  @spec commit_offset(
          GenServer.server(),
          Malachi.Metadata.group(),
          Malachi.Metadata.topic_name(),
          Malachi.Metadata.offsets()
        ) ::
          term()
  def commit_offset(server, group, topic, offsets),
    do: GenServer.call(server, {:commit_offset, group, topic, offsets})

  @doc "A consumer group's committed offsets for a topic (empty if it never committed)."
  @spec committed_offsets(GenServer.server(), Malachi.Metadata.group(), Malachi.Metadata.topic_name()) ::
          Malachi.Metadata.offsets()
  def committed_offsets(server, group, topic), do: GenServer.call(server, {:committed_offsets, group, topic})

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
      refresh_interval: refresh_interval,
      # Long-poll: fetches that found nothing and are willing to wait, parked here until a produce to
      # their topic wakes them (with data) or their timer fires (empty). See `handle_call({:consume,…})`.
      waiters: []
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
    state = wake_waiters(%{state | broker: broker}, topic, reply)
    {:reply, reply, state}
  end

  def handle_call({:consume, topic, positions, max_records, wait_ms}, from, state) do
    case consume_ranges(state.broker, topic, positions, max_records) do
      # Caught up and willing to wait: park the request; a later produce to this topic (or the
      # timeout) replies. Hold the original positions so the wake re-consumes from the same place.
      {[], _positions} when wait_ms > 0 ->
        ref = make_ref()
        timer = Process.send_after(self(), {:longpoll_timeout, ref}, wait_ms)
        waiter = %{ref: ref, timer: timer, from: from, topic: topic, positions: positions, max: max_records}
        {:noreply, %{state | waiters: [waiter | state.waiters]}}

      {records, next_positions} ->
        {:reply, {records, next_positions}, state}
    end
  end

  def handle_call({:read, range_id, offset, max_records}, _from, state) do
    {:reply, Broker.read(state.broker, range_id, offset, max_records, &ReplicationServer.read/4), state}
  end

  def handle_call({:read_consume, range_id, cursor, max_records}, _from, state) do
    {:reply, Broker.read_consume(state.broker, range_id, cursor, max_records, &ReplicationServer.read/4), state}
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

  def handle_call({:commit_offset, group, topic, offsets}, _from, state) do
    {broker, reply} = Broker.commit_offset(state.broker, group, topic, offsets)
    {:reply, reply, %{state | broker: broker}}
  end

  def handle_call({:committed_offsets, group, topic}, _from, state) do
    {:reply, Broker.committed_offsets(state.broker, group, topic), state}
  end

  @impl true
  def handle_info({:longpoll_timeout, ref}, state) do
    case Enum.split_with(state.waiters, &(&1.ref == ref)) do
      {[waiter], rest} ->
        GenServer.reply(waiter.from, {[], waiter.positions})
        {:noreply, %{state | waiters: rest}}

      # Already woken by a produce (timer raced the reply); nothing to do.
      {[], _rest} ->
        {:noreply, state}
    end
  end

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

  # Reads a topic's current ranges from `positions`, cross-epoch (see `Broker.read_consume/5`), and
  # returns {records, next_positions}. This is the read orchestration the LogApi used to do client-side;
  # holding it here lets a single call serve a fetch and lets produce re-run it to wake long-pollers.
  defp consume_ranges(broker, topic, positions, max_records) do
    broker
    |> Broker.active_range_ids(topic)
    |> Enum.reduce({[], positions}, fn range_id, {acc, positions} ->
      cursor = Map.get(positions, range_id, :start)

      case Broker.read_consume(broker, range_id, cursor, max_records, &ReplicationServer.read/4) do
        {:ok, records, next_cursor} -> {acc ++ records, Map.put(positions, range_id, next_cursor)}
        {:error, _reason} -> {acc, positions}
      end
    end)
  end

  # After a successful produce to `topic`, re-consume each parked waiter on that topic; reply (and
  # drop) the ones that now have data, leaving the rest parked until their timeout.
  defp wake_waiters(state, _topic, {:error, _reason}), do: state

  defp wake_waiters(state, topic, {:ok, _placements}) do
    {on_topic, others} = Enum.split_with(state.waiters, &(&1.topic == topic))

    still_waiting =
      Enum.reduce(on_topic, [], fn waiter, keep ->
        case consume_ranges(state.broker, waiter.topic, waiter.positions, waiter.max) do
          {[], _positions} ->
            [waiter | keep]

          {records, next_positions} ->
            Process.cancel_timer(waiter.timer)
            GenServer.reply(waiter.from, {records, next_positions})
            keep
        end
      end)

    %{state | waiters: others ++ still_waiting}
  end

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
