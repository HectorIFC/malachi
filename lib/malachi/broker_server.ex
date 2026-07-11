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

  require OpenTelemetry.Tracer, as: Tracer

  alias Malachi.Broker
  alias Malachi.Cluster.DSRSM
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.ReplicatedDSRSM
  alias Malachi.Cluster.ReplicatedMetadata
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Metadata
  alias OpenTelemetry.Ctx

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
  def produce(server, topic, records) do
    # Carry the caller's trace context (the LogApi produce span) into the broker process so the
    # server-side work becomes a child span (cross-process propagation, O5b).
    GenServer.call(server, {:produce, topic, records, Ctx.get_current()})
  end

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

  @doc "Removes a sealed segment from the control plane (retention); returns the control-plane reply."
  @spec delete_segment(GenServer.server(), Malachi.Metadata.segment_id()) :: term()
  def delete_segment(server, segment_id), do: GenServer.call(server, {:delete_segment, segment_id})

  @doc "The current control-plane metadata (e.g. for a healing coordinator to inspect)."
  @spec metadata(GenServer.server()) :: Malachi.Metadata.t()
  def metadata(server), do: GenServer.call(server, :metadata)

  @doc """
  The per-topic overview (`Malachi.Metadata.overview/1`) annotated with each topic's failure-domain
  violation count (`Malachi.Broker.domain_violations/2`), computed from a single merged-metadata view.
  """
  def topics_overview(server), do: GenServer.call(server, :topics_overview)

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

  @doc """
  Subscribes the calling process as a streaming consumer of `topic` for consumer `group` (B2): records
  are pushed to it as `{:log_records, topic, records, next_positions}` as they are produced, resuming
  from the group's committed position and bounded by a credit `window` (at most `window` records in
  flight, at most `max` per push). Ack with `stream_ack/5` to return credit and commit progress. `:ok`.
  """
  @spec subscribe(
          GenServer.server(),
          Malachi.Metadata.topic_name(),
          Malachi.Metadata.group(),
          pos_integer(),
          pos_integer()
        ) ::
          :ok
  def subscribe(server, topic, group, window, max) do
    GenServer.call(server, {:subscribe, topic, group, window, max, self()})
  end

  @doc """
  Acks `count` streamed records of `topic`/`group` at `positions` (a decoded cursor): returns that much
  window credit (unblocking further pushes) and durably commits the group's position. Returns `:ok`.
  """
  @spec stream_ack(
          GenServer.server(),
          Malachi.Metadata.topic_name(),
          Malachi.Metadata.group(),
          Malachi.Metadata.offsets(),
          non_neg_integer()
        ) ::
          :ok
  def stream_ack(server, topic, group, positions, count) do
    GenServer.call(server, {:stream_ack, topic, group, positions, count, self()})
  end

  @doc "Removes the calling process's streaming subscription to `topic`. Returns `:ok`."
  @spec unsubscribe(GenServer.server(), Malachi.Metadata.topic_name()) :: :ok
  def unsubscribe(server, topic), do: GenServer.call(server, {:unsubscribe, topic, self()})

  @doc "Stops the server (and its replication storage)."
  @spec stop(GenServer.server()) :: :ok
  def stop(server), do: GenServer.stop(server)

  # --- server callbacks ---

  @impl true
  def init({directory, opts}) do
    {segment_max_bytes, opts} = Keyword.pop(opts, :segment_max_bytes)
    {replication_factor, opts} = Keyword.pop(opts, :replication_factor, 1)
    {live_brokers, opts} = Keyword.pop(opts, :live_brokers)
    {broker_attributes, opts} = Keyword.pop(opts, :broker_attributes)
    {spread_by, opts} = Keyword.pop(opts, :spread_by)
    {min_domains, opts} = Keyword.pop(opts, :min_domains)
    {placement_policy, opts} = Keyword.pop(opts, :placement_policy)
    {refresh_interval, opts} = Keyword.pop(opts, :brokers_refresh_interval, @default_brokers_refresh_interval)
    {metadata_cluster, opts} = Keyword.pop(opts, :metadata_cluster)
    {metadata_nodes, opts} = Keyword.pop(opts, :metadata_nodes, [node()])
    {metadata_vnodes, opts} = Keyword.pop(opts, :metadata_vnodes)
    {bootstrap_orchestrator, opts} = Keyword.pop(opts, :bootstrap_orchestrator, fn -> true end)
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

    {broker_opts, metadata_refresh, bootstrap} =
      [brokers: brokers, replication_factor: replication_factor]
      |> maybe_put(:segment_max_bytes, segment_max_bytes)
      |> maybe_put(:spread_by, spread_by)
      |> maybe_put(:min_domains, min_domains)
      |> maybe_put(:placement_policy, placement_policy)
      |> with_metadata_authority(metadata_cluster, metadata_nodes, metadata_vnodes, bootstrap_orchestrator)

    {:ok, broker} = Broker.open(broker_opts)

    state = %{
      broker: broker,
      replication: owned_replication,
      live_brokers: live_brokers,
      broker_attributes: broker_attributes,
      refresh_interval: refresh_interval,
      # Re-seeds the local metadata cache from the authoritative ra clusters (fills vnodes not yet ready
      # at boot; picks up writes made through other nodes). `nil` for in-memory metadata. See
      # `reconcile_metadata/1`.
      metadata_refresh: metadata_refresh,
      # Sharded control plane only: `%{orchestrator?: (-> boolean), vnodes: [{id, token, nodes}],
      # replicated: ReplicatedDSRSM.t()}`. The reconcile loop uses it to bootstrap missing vnodes while
      # this node is the leader (see `bootstrap_missing_vnodes/1`). `nil` otherwise.
      bootstrap: bootstrap,
      # Long-poll: fetches that found nothing and are willing to wait, parked here until a produce to
      # their topic wakes them (with data) or their timer fires (empty). See `handle_call({:consume,…})`.
      waiters: [],
      # Streaming subscribers (B2): `%{topic => [subscriber]}`. A subscriber is pushed records as they
      # are produced, bounded by a credit window (in_flight < window); acks return credit and durably
      # commit the group's position. See `wake_subscribers/2` / `push_subscriber/2`.
      subscribers: %{}
    }

    # Refresh the placement inputs (live broker set and their attributes) from the given sources.
    if live_brokers || broker_attributes, do: schedule_refresh(state)

    # A replicated control plane reconciles right after boot (bootstrap missing vnodes if leader; seed
    # the cache once the clusters are ready) and then periodically; in-memory metadata needs neither.
    if metadata_refresh do
      {:ok, state, {:continue, :reconcile}}
    else
      {:ok, state}
    end
  end

  @impl true
  def handle_call({:create_topic, name, keyspace_bits}, _from, state) do
    {broker, reply} = Broker.create_topic(state.broker, name, keyspace_bits)
    {:reply, reply, %{state | broker: broker}}
  end

  def handle_call({:produce, topic, records, ctx}, _from, state) do
    # Attach the caller's context so the broker span is a child of the LogApi produce span, and the
    # downstream replication span (started inside Broker.produce) is a grandchild. Detach after.
    token = Ctx.attach(ctx)

    try do
      Tracer.with_span "malachi.broker.produce" do
        {broker, reply} = Broker.produce(state.broker, topic, records, &ReplicationServer.replicate/5)
        Tracer.set_attributes(%{"malachi.topic" => topic, "malachi.records" => length(records)})

        state =
          %{state | broker: broker}
          |> wake_waiters(topic, reply)
          |> wake_subscribers(topic, reply)

        {:reply, reply, state}
      end
    after
      Ctx.detach(token)
    end
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

  def handle_call(:topics_overview, _from, state) do
    metadata = Broker.metadata(state.broker)
    violations = Broker.domain_violations(state.broker, metadata)

    overview =
      metadata
      |> Metadata.overview()
      |> Enum.map(fn topic -> Map.put(topic, :domain_violations, Map.get(violations, topic.name, 0)) end)

    {:reply, overview, state}
  end

  def handle_call({:apply_heal, commands}, _from, state) do
    {:reply, :ok, %{state | broker: Broker.apply_heal(state.broker, commands)}}
  end

  def handle_call({:delete_segment, segment_id}, _from, state) do
    {broker, reply} = Broker.delete_segment(state.broker, segment_id)
    {:reply, reply, %{state | broker: broker}}
  end

  def handle_call({:commit_offset, group, topic, offsets}, _from, state) do
    {broker, reply} = Broker.commit_offset(state.broker, group, topic, offsets)
    {:reply, reply, %{state | broker: broker}}
  end

  def handle_call({:committed_offsets, group, topic}, _from, state) do
    {:reply, Broker.committed_offsets(state.broker, group, topic), state}
  end

  def handle_call({:subscribe, topic, group, window, max, pid}, _from, state) do
    ref = Process.monitor(pid)
    positions = Broker.committed_offsets(state.broker, group, topic)

    subscriber =
      push_subscriber(state.broker, %{
        pid: pid,
        ref: ref,
        topic: topic,
        group: group,
        positions: positions,
        window: window,
        in_flight: 0,
        max: max
      })

    subscribers = Map.update(state.subscribers, topic, [subscriber], &[subscriber | &1])
    {:reply, :ok, %{state | subscribers: subscribers}}
  end

  def handle_call({:stream_ack, topic, group, positions, count, pid}, _from, state) do
    # commit the group's position durably, then return `count` credit to this subscriber and push more
    {broker, _reply} = Broker.commit_offset(state.broker, group, topic, positions)

    subs =
      state.subscribers
      |> Map.get(topic, [])
      |> Enum.map(fn sub ->
        if sub.pid == pid, do: push_subscriber(broker, %{sub | in_flight: max(sub.in_flight - count, 0)}), else: sub
      end)

    {:reply, :ok, %{state | broker: broker, subscribers: Map.put(state.subscribers, topic, subs)}}
  end

  def handle_call({:unsubscribe, topic, pid}, _from, state) do
    {:reply, :ok, drop_subscriber(state, topic, pid)}
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

  # A streaming subscriber's process died: drop it from every topic it was subscribed to.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    subscribers =
      Map.new(state.subscribers, fn {topic, subs} -> {topic, Enum.reject(subs, &(&1.ref == ref))} end)

    {:noreply, %{state | subscribers: subscribers}}
  end

  def handle_info(:refresh_brokers, state) do
    broker =
      state.broker
      |> refresh_broker_set(state.live_brokers)
      |> refresh_broker_attributes(state.broker_attributes)

    schedule_refresh(state)
    {:noreply, %{state | broker: broker}}
  end

  def handle_info(:reconcile, state) do
    {:noreply, reconcile_metadata(state)}
  end

  @impl true
  def handle_continue(:reconcile, state) do
    {:noreply, reconcile_metadata(state)}
  end

  @impl true
  def terminate(_reason, state) do
    # Only stop the replication server we started ourselves; an external broker set is not ours.
    if state.replication && Process.alive?(state.replication), do: GenServer.stop(state.replication)
    :ok
  end

  # --- internals ---

  defp schedule_refresh(state), do: Process.send_after(self(), :refresh_brokers, state.refresh_interval)

  defp schedule_reconcile(state), do: Process.send_after(self(), :reconcile, state.refresh_interval)

  # The reconcile loop (level-triggered, controller-style, D-c-1d): while this node is the leader,
  # bootstrap any vnode whose ra cluster is not up yet; then re-seed the local cache from the clusters.
  # Reschedules itself. Idempotent: bootstrapping an already-formed vnode is a no-op, and the cache
  # re-seed only moves forward (the ra log is the source of truth).
  defp reconcile_metadata(state) do
    schedule_reconcile(state)
    bootstrap_missing_vnodes(state.bootstrap)

    case state.metadata_refresh.() do
      nil -> state
      dsrsm -> %{state | broker: Broker.put_cache(state.broker, dsrsm)}
    end
  end

  # Bootstrap step: only the leader acts, and only on vnodes whose cluster is not yet ready. Starting a
  # vnode whose cluster already exists returns an error and is ignored (the name fences a double start).
  defp bootstrap_missing_vnodes(nil), do: :ok

  defp bootstrap_missing_vnodes(%{orchestrator?: orchestrator?, vnodes: vnodes, replicated: replicated}) do
    if orchestrator?.() do
      Enum.each(vnodes, fn {vnode_id, _token, nodes} ->
        unless MetadataServer.ready?(ReplicatedDSRSM.server_for(replicated, vnode_id)) do
          _ = MetadataServer.start(vnode_id, nodes)
        end
      end)
    end

    :ok
  end

  # An empty live set is ignored (keep the last non-empty one); no source leaves the broker as-is.
  defp refresh_broker_set(broker, nil), do: broker

  defp refresh_broker_set(broker, live_brokers) do
    case live_brokers.() do
      [] -> broker
      live -> Broker.set_brokers(broker, live)
    end
  end

  defp refresh_broker_attributes(broker, nil), do: broker
  defp refresh_broker_attributes(broker, attributes_fun), do: Broker.set_broker_attributes(broker, attributes_fun.())

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

  # After a successful produce to `topic`, push newly-available records to each of its subscribers, each
  # bounded by its own window credit.
  defp wake_subscribers(state, _topic, {:error, _reason}), do: state

  defp wake_subscribers(state, topic, {:ok, _placements}) do
    case Map.get(state.subscribers, topic) do
      nil ->
        state

      subs ->
        %{state | subscribers: Map.put(state.subscribers, topic, Enum.map(subs, &push_subscriber(state.broker, &1)))}
    end
  end

  # Pushes up to the subscriber's remaining window credit (min with `max`) of records from its current
  # position, advancing the position and the in-flight count. A no-op when out of credit or caught up.
  defp push_subscriber(broker, subscriber) do
    budget = min(subscriber.max, subscriber.window - subscriber.in_flight)

    if budget <= 0 do
      subscriber
    else
      case consume_ranges(broker, subscriber.topic, subscriber.positions, budget) do
        {[], _positions} ->
          subscriber

        {records, next_positions} ->
          send(subscriber.pid, {:log_records, subscriber.topic, records, next_positions})
          %{subscriber | positions: next_positions, in_flight: subscriber.in_flight + length(records)}
      end
    end
  end

  # Removes `pid`'s subscription to `topic` (and stops monitoring it).
  defp drop_subscriber(state, topic, pid) do
    subs = Map.get(state.subscribers, topic, [])
    {removed, kept} = Enum.split_with(subs, &(&1.pid == pid))
    Enum.each(removed, &Process.demonitor(&1.ref, [:flush]))
    %{state | subscribers: Map.put(state.subscribers, topic, kept)}
  end

  # Control-plane authority, most specific first, returning `{broker_opts, metadata_refresh, bootstrap}`
  # where `metadata_refresh` re-seeds the local cache from the ra clusters (`nil` for in-memory) and
  # `bootstrap` drives the leader's reconcile loop (`nil` unless sharded):
  #   * `:metadata_vnodes` — a sharded control plane: one ra cluster per vnode, each placed on its own
  #     `nodes`, routed by topic. Every node only *routes* at boot; the reconcile loop bootstraps the
  #     clusters on whichever node is currently the leader. `metadata_vnodes` is `[{vnode_id, token,
  #     nodes}]` (D-c).
  #   * `:metadata_cluster` — a single ra cluster, the whole metadata in one Raft group (D-a/D1 HA).
  #   * neither — in-memory metadata (single node).
  defp with_metadata_authority(opts, _cluster, _nodes, [_ | _] = vnodes, orchestrator?) do
    replicated = build_replicated(vnodes)
    {:ok, cache} = ReplicatedDSRSM.snapshot(replicated)

    new_opts =
      opts
      |> Keyword.put(:dsrsm, cache)
      |> Keyword.put(:command_fun, sharded_command_fun(replicated))

    refresh = fn -> elem(ReplicatedDSRSM.snapshot(replicated), 1) end
    bootstrap = %{orchestrator?: orchestrator?, vnodes: vnodes, replicated: replicated}
    {new_opts, refresh, bootstrap}
  end

  defp with_metadata_authority(opts, nil, _nodes, _no_vnodes, _orchestrator?), do: {opts, nil, nil}

  defp with_metadata_authority(opts, cluster_name, nodes, _no_vnodes, _orchestrator?) do
    {:ok, server_id} = MetadataServer.start(cluster_name, nodes)
    {:ok, seed} = MetadataServer.query(server_id, &Function.identity/1)

    new_opts =
      opts
      |> Keyword.put(:dsrsm, DSRSM.single(seed))
      |> Keyword.put(:command_fun, raft_command_fun(server_id))

    {new_opts, single_cluster_refresh(server_id), nil}
  end

  # Builds the sharded control plane's ReplicatedDSRSM as routing-only: every vnode points at a real
  # placement member (the first) without starting its cluster. The reconcile loop starts the clusters
  # on the current leader (see `bootstrap_missing_vnodes/1`), so exactly one node bootstraps each vnode
  # and the role fails over with leadership.
  defp build_replicated(vnodes) do
    Enum.reduce(vnodes, ReplicatedDSRSM.new(), fn {vnode_id, token, nodes}, replicated ->
      {:ok, replicated} = ReplicatedDSRSM.route_vnode(replicated, vnode_id, token, {vnode_id, hd(nodes)})
      replicated
    end)
  end

  # A refresh that re-reads the single ra cluster into a one-vnode DSRSM; nil if the cluster is
  # momentarily unreachable (a leader election), so the cache is simply left as-is that tick.
  defp single_cluster_refresh(server_id) do
    fn ->
      case MetadataServer.query(server_id, &Function.identity/1) do
        {:ok, metadata} -> DSRSM.single(metadata)
        {:error, _reason} -> nil
      end
    end
  end

  # A command function over the sharded control plane: route the command's topic (via the cache's
  # ring, shared with `replicated`) to its vnode and apply it through that vnode's ra cluster, keeping
  # the local cache in step (deterministic apply).
  defp sharded_command_fun(replicated) do
    fn dsrsm, topic, command ->
      {:ok, vnode_id} = DSRSM.vnode_for(dsrsm, topic)
      server_id = ReplicatedDSRSM.server_for(replicated, vnode_id)
      DSRSM.update_vnode(dsrsm, topic, &ReplicatedMetadata.apply_command(server_id, &1, command))
    end
  end

  # A command function over a single-vnode DSRSM whose lone vnode is an authoritative ra cluster:
  # route by topic to that vnode and apply the command through the Raft log (the deterministic apply
  # keeps the local cache in step).
  defp raft_command_fun(server_id) do
    fn dsrsm, topic, command ->
      DSRSM.update_vnode(dsrsm, topic, &ReplicatedMetadata.apply_command(server_id, &1, command))
    end
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)
end
