defmodule Malachi.BrokerServer do
  @moduledoc """
  A `GenServer` that owns a `Malachi.Broker` (the control-plane router) and a local
  `Malachi.Cluster.ReplicationServer` (segment storage/replication), serializing concurrent
  access. It wires the broker's injected effect functions to the replication server:
  `produce` replicates through it and `read`/`stream_history` read segments from it.

  Writes are durable on return: by default each batch is fsynced on a quorum by the replication
  server before it commits, so there is no buffering. With group commit enabled (`:group_commit`,
  single-node rf=1), a produce instead buffers its batch and the client reply is deferred until the
  next time-based flush (~`:group_commit_interval_ms`), so many concurrent producers coalesce into
  one fsync; the reply is still returned only once the batch is durable. Under group commit the flush
  is also triggered early once `:group_commit_flush_max_records` are parked (so each fsync, and so each
  reply, stays bounded and a produce never waits long enough to time out), and beyond
  `:group_commit_max_inflight` parked records new produces are shed with `{:error, :overloaded}` rather
  than dropped. Either way `sync/1` is a no-op kept for API compatibility.

  On the replicated (non-group-commit) path the produce is NON-BLOCKING for this server's loop (the
  NorthGuard end-to-end pipelined shape): the loop plans the produce (routing, segment opening,
  optimistic offset commit), fires the replication dispatches as casts, parks the caller, and replies
  from the replication results, waking consumers only after every dispatch is quorum-durable. So the
  node accepts the next produce while earlier ones replicate, instead of one produce per replication
  round trip.

  The `Broker` (and the layers it composes) are pure immutable values; routing all mutations
  through this single process is what makes concurrent producers/consumers safe.

  Supersedes `Malachi.TopicServer`.
  """

  use GenServer

  require OpenTelemetry.Tracer, as: Tracer

  alias Malachi.Broker
  alias Malachi.Cluster.DSRSM
  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.ReplicatedDSRSM
  alias Malachi.Cluster.ReplicatedMetadata
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.RingTopology
  alias Malachi.Consumer.CoordinatorRouter
  alias Malachi.Consumer.GroupCoordinator
  alias Malachi.Metadata
  alias OpenTelemetry.Ctx

  @default_brokers_refresh_interval 1_000
  # Produce call timeout: must exceed every server-side completion path (replication no_quorum ~5s,
  # the async-produce safety timer at 6s), so callers get a real error reply, never a call exit.
  @produce_call_timeout 10_000
  # Safety net for an async produce whose replication result never arrives (e.g. the cast to a dead
  # primary was silently dropped): reply an error instead of leaving the caller to time out.
  @async_produce_timeout 6_000

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
    * `:group_commit` - when true, produce buffers its batch and defers the client reply to the next
      flush so concurrent producers coalesce into one fsync (default from app env; only active at
      rf=1). See the moduledoc.
    * `:group_commit_interval_ms` - group-commit flush period in ms (default 5, or app env).
    * `:group_commit_flush_max_records` - flush eagerly once this many records are parked, bounding the
      per-flush fsync (default 8000, or app env).
    * `:group_commit_max_inflight` - past this many parked records, shed produces with `:overloaded`
      instead of dropping the connection (default 200000, or app env).
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
    # The timeout leaves room for the server-side completion paths (replication no_quorum at ~5s and
    # the async-produce safety timer) to reply with a real error before the call ever exits.
    GenServer.call(server, {:produce, topic, records, Ctx.get_current()}, @produce_call_timeout)
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
  @spec consume(
          GenServer.server(),
          Malachi.Metadata.topic_name(),
          map(),
          pos_integer(),
          non_neg_integer(),
          [term()] | nil
        ) ::
          {[Malachi.Log.Record.t()], map()}
  def consume(server, topic, positions, max_records, wait_ms, ranges \\ nil) do
    # The call may block up to wait_ms server-side; give it headroom over the default 5s call timeout.
    GenServer.call(server, {:consume, topic, positions, max_records, wait_ms, ranges}, wait_ms + 5_000)
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
          pos_integer(),
          keyword()
        ) ::
          :ok
  def subscribe(server, topic, group, window, max, group_opts \\ []) do
    GenServer.call(server, {:subscribe, topic, group, window, max, self(), group_opts})
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
          non_neg_integer(),
          [term()] | nil,
          GenServer.server() | nil
        ) ::
          :ok
  def stream_ack(server, topic, group, positions, count, ranges \\ nil, coordinator \\ nil) do
    GenServer.call(server, {:stream_ack, topic, group, positions, count, self(), ranges, coordinator})
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
    {group_commit_flag, opts} = Keyword.pop(opts, :group_commit, Application.get_env(:malachi, :group_commit, false))

    {gc_interval, opts} =
      Keyword.pop(opts, :group_commit_interval_ms, Application.get_env(:malachi, :group_commit_interval_ms, 5))

    {flush_max_records, opts} =
      Keyword.pop(
        opts,
        :group_commit_flush_max_records,
        Application.get_env(:malachi, :group_commit_flush_max_records, 8_000)
      )

    {max_inflight_records, opts} =
      Keyword.pop(opts, :group_commit_max_inflight, Application.get_env(:malachi, :group_commit_max_inflight, 200_000))

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
      subscribers: %{},
      # Group commit (NorthGuard fps-store style): when on, produce buffers the batch and defers the
      # client reply until the next flush (~`gc_interval` ms), so many concurrent producers coalesce into
      # one fsync. Gated on rf=1 (the append path does no follower fan-out). `pending_produce` holds the
      # parked callers; `gc_timer` is the scheduled flush (nil when none is pending).
      group_commit: group_commit_flag and replication_factor == 1,
      gc_interval: gc_interval,
      pending_produce: [],
      # sum of the record counts of the parked (not-yet-flushed) produces; drives the eager flush and the
      # overload valve, reset on every flush.
      pending_records: 0,
      # eager-flush threshold: flush as soon as this many records are parked, so a single fsync (and so the
      # reply latency) stays bounded and a produce never waits long enough to time out, even on a slow disk.
      flush_max_records: flush_max_records,
      # backpressure valve: past this many parked records the broker sheds new produces with `:overloaded`
      # (graceful) instead of letting them queue until the caller's call times out and drops the connection.
      max_inflight_records: max_inflight_records,
      gc_timer: nil,
      # In-flight async produces (the non-group-commit path): ref => the parked caller, its computed
      # placements, how many replication dispatches are still owed, and the safety timer.
      async_produces: %{}
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

  def handle_call({:produce, topic, records, ctx}, from, state) do
    # Attach the caller's context so the broker span is a child of the LogApi produce span, and the
    # downstream replication span (started inside Broker.produce) is a grandchild. Detach after.
    token = Ctx.attach(ctx)

    try do
      Tracer.with_span "malachi.broker.produce" do
        Tracer.set_attributes(%{"malachi.topic" => topic, "malachi.records" => length(records)})

        if state.group_commit do
          produce_grouped(from, topic, records, state)
        else
          produce_async(from, topic, records, state)
        end
      end
    after
      Ctx.detach(token)
    end
  end

  def handle_call({:consume, topic, positions, max_records, wait_ms, ranges}, from, state) do
    case consume_ranges(state.broker, topic, positions, max_records, ranges) do
      # Caught up and willing to wait: park the request; a later produce to this topic (or the
      # timeout) replies. Hold the original positions (and range scope) so the wake re-consumes the same.
      {[], _positions} when wait_ms > 0 ->
        ref = make_ref()
        timer = Process.send_after(self(), {:longpoll_timeout, ref}, wait_ms)

        waiter = %{
          ref: ref,
          timer: timer,
          from: from,
          topic: topic,
          positions: positions,
          max: max_records,
          ranges: ranges
        }

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

  def handle_call({:subscribe, topic, group, window, max, pid, group_opts}, _from, state) do
    ref = Process.monitor(pid)
    ranges = Keyword.get(group_opts, :ranges)
    positions = Broker.committed_offsets(state.broker, group, topic)
    # scope a member's start positions to its ranges (a whole-group subscriber keeps them all)
    positions = if ranges, do: Map.take(positions, ranges), else: positions

    subscriber =
      push_subscriber(state.broker, %{
        pid: pid,
        ref: ref,
        topic: topic,
        group: group,
        positions: positions,
        window: window,
        in_flight: 0,
        max: max,
        # consumer-group member scoping: `member`/`ranges` scope the push to the member's ranges (nil =
        # whole group); `coordinator` lets the :DOWN handler leave the group on disconnect. The LogApi
        # layer supplies these (the broker must never call the coordinator itself, deadlock).
        member: Keyword.get(group_opts, :member),
        ranges: ranges,
        coordinator: Keyword.get(group_opts, :coordinator)
      })

    subscribers = Map.update(state.subscribers, topic, [subscriber], &[subscriber | &1])
    {:reply, :ok, %{state | subscribers: subscribers}}
  end

  def handle_call({:stream_ack, topic, group, positions, count, pid, ranges, coordinator}, _from, state) do
    # commit the group's position durably, then return `count` credit to this subscriber and push more.
    # `ranges` (from the LogApi member poll) refreshes this member's assignment, so a rebalance is picked
    # up on the ack (nil keeps the current scope: a whole-group or unchanged member subscription).
    # `coordinator` refreshes the member's resolved coordinator ref, so after a vnode leadership change
    # the :DOWN leave targets the current owner (nil keeps the ref captured at subscribe).
    {broker, _reply} = Broker.commit_offset(state.broker, group, topic, positions)

    subs =
      state.subscribers
      |> Map.get(topic, [])
      |> Enum.map(fn sub ->
        if sub.pid == pid do
          push_subscriber(broker, %{
            sub
            | in_flight: max(sub.in_flight - count, 0),
              ranges: ranges || sub.ranges,
              coordinator: coordinator || sub.coordinator
          })
        else
          sub
        end
      end)

    {:reply, :ok, %{state | broker: broker, subscribers: Map.put(state.subscribers, topic, subs)}}
  end

  def handle_call({:unsubscribe, topic, pid}, _from, state) do
    {:reply, :ok, drop_subscriber(state, topic, pid)}
  end

  @impl true
  # Adopt a ring change (a vnode split) gossiped in via the membership hook: rebuild the metadata routing
  # (cache ring + write router) and the refresh source, so the periodic reconcile re-seeds against the new
  # topology instead of reverting to the boot ring. Fired async (a cast), so it never blocks membership.
  def handle_cast({:adopt_topology, %RingTopology{} = topology}, state) do
    replicated = replicated_of(topology)
    broker = adopt_topology(state.broker, topology)
    metadata_refresh = fn -> elem(ReplicatedDSRSM.snapshot(replicated), 1) end
    bootstrap = if state.bootstrap, do: %{state.bootstrap | replicated: replicated}, else: state.bootstrap

    {:noreply, %{state | broker: broker, metadata_refresh: metadata_refresh, bootstrap: bootstrap}}
  end

  # Completes one dispatch of an async produce. The tag carries the expected last offset, so a primary
  # whose log disagrees with our bookkeeping surfaces as the same offset_mismatch the executing path
  # reported. Consumers are only woken once every dispatch committed (they must not observe data that
  # is not yet quorum-durable).
  @impl true
  def handle_info({:replicate_result, {ref, dispatch}, result}, state) do
    case Map.get(state.async_produces, ref) do
      # Already completed (a failure replied early, or the safety timer fired): ignore the straggler.
      nil ->
        {:noreply, state}

      pending ->
        case result do
          {:ok, actual} ->
            # The range's primary serializes appends and assigns the REAL offsets (the NorthGuard
            # invariant). When several broker frontends interleave on one range, the primary-assigned
            # end can differ from this frontend's precomputed one; the frontend ADOPTS the primary's
            # truth (placements and local counter follow it) instead of failing the produce. That is
            # what lets any node accept a produce for any range with no client-side routing.
            {state, pending} = adopt_result(state, pending, dispatch, actual)

            if pending.remaining == 1 do
              {:noreply, finish_async_produce(state, ref, pending, {:ok, pending.placements})}
            else
              pending = %{pending | remaining: pending.remaining - 1}
              {:noreply, %{state | async_produces: Map.put(state.async_produces, ref, pending)}}
            end

          {:error, reason} ->
            {:noreply, finish_async_produce(state, ref, pending, {:error, reason})}
        end
    end
  end

  def handle_info({:produce_timeout, ref}, state) do
    case Map.get(state.async_produces, ref) do
      nil -> {:noreply, state}
      pending -> {:noreply, finish_async_produce(state, ref, pending, {:error, :replication_timeout})}
    end
  end

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
    # a departing group member leaves its group for a fast rebalance: done in an unlinked task, since the
    # coordinator's leave calls back into this broker and a synchronous call from here would deadlock.
    for {_topic, subs} <- state.subscribers,
        sub <- subs,
        sub.ref == ref,
        sub.member != nil,
        sub.coordinator != nil do
      Task.start(fn -> GroupCoordinator.leave(sub.coordinator, sub.group, sub.topic, sub.member) end)
    end

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

  # Group-commit flush: one fsync per pipeline covers every parked producer. Fsync first (durable), then
  # reply to each caller and wake its topic's long-poll consumers and subscribers, so consumers only ever
  # observe durable data. `broker.brokers` is the set of local pipelines a segment can live on (one for
  # rf=1, or several when striping), so flushing all of them covers wherever the buffered batches landed.
  def handle_info(:group_flush, state) do
    {:noreply, do_flush(state)}
  end

  # Fsyncs every pipeline, replies to the parked producers (now durable), wakes each topic's consumers, and
  # resets the group-commit accumulators. Used by both the interval timer and the eager (size-triggered)
  # path; cancels any pending timer so an eager flush leaves no stale one queued (a stale `:group_flush`
  # that still arrives just re-runs this on empty pending, a no-op).
  defp do_flush(state) do
    Enum.each(state.broker.brokers, &ReplicationServer.flush/1)
    if state.gc_timer, do: Process.cancel_timer(state.gc_timer)

    pending = Enum.reverse(state.pending_produce)
    base = %{state | pending_produce: [], pending_records: 0, gc_timer: nil}

    Enum.reduce(pending, base, fn waiter, st ->
      GenServer.reply(waiter.from, waiter.reply)

      st
      |> wake_waiters(waiter.topic, waiter.reply)
      |> wake_subscribers(waiter.topic, waiter.reply)
    end)
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
  # `ranges` nil consumes every active range of the topic (whole-group / single consumer); a range list
  # (a group member's assignment) consumes only those, intersected with the active set, so a stale
  # assigned range that has since split is skipped, and the client never sees ranges either way.
  defp consume_ranges(broker, topic, positions, max_records, ranges) do
    broker
    |> selected_ranges(topic, ranges)
    |> Enum.reduce({[], positions}, fn range_id, {acc, positions} ->
      cursor = Map.get(positions, range_id, :start)

      case Broker.read_consume(broker, range_id, cursor, max_records, &ReplicationServer.read/4) do
        {:ok, records, next_cursor} -> {acc ++ records, Map.put(positions, range_id, next_cursor)}
        {:error, _reason} -> {acc, positions}
      end
    end)
  end

  defp selected_ranges(broker, topic, nil), do: Broker.active_range_ids(broker, topic)

  defp selected_ranges(broker, topic, ranges) do
    assigned = MapSet.new(ranges)
    broker |> Broker.active_range_ids(topic) |> Enum.filter(&MapSet.member?(assigned, &1))
  end

  # After a successful produce to `topic`, re-consume each parked waiter on that topic; reply (and
  # drop) the ones that now have data, leaving the rest parked until their timeout.
  # Synchronous produce (group commit off): replicate the batch through `replicate/5`, which fsyncs on a
  # quorum before returning, then reply and wake consumers. The historical default; unchanged behaviour.
  # Non-blocking produce (the NorthGuard end-to-end pipelined shape): plan the produce in this loop
  # (routing, segment opening, optimistic offset commit), fire the replication dispatches as casts, park
  # the caller, and reply from the `:replicate_result` messages. The loop is free for the next produce
  # while replication runs, so a node's throughput is no longer one produce per replication round trip.
  defp produce_async(from, topic, records, state) do
    case Broker.produce_plan(state.broker, topic, records) do
      {broker, {:ok, placements, []}} ->
        # Nothing to replicate (an empty batch): complete immediately.
        {:reply, {:ok, placements}, %{state | broker: broker}}

      {broker, {:ok, placements, dispatches}} ->
        ref = make_ref()

        Enum.each(dispatches, fn d ->
          tag = {ref, %{range_id: d.range_id, last: d.last, count: d.count}}

          ReplicationServer.replicate_async(
            d.primary,
            d.segment_id,
            d.replica_set,
            d.base_offset,
            d.records,
            self(),
            tag
          )
        end)

        timer = Process.send_after(self(), {:produce_timeout, ref}, @async_produce_timeout)

        pending = %{from: from, topic: topic, placements: placements, remaining: length(dispatches), timer: timer}
        {:noreply, %{state | broker: broker, async_produces: Map.put(state.async_produces, ref, pending)}}

      {broker, {:error, _reason} = error} ->
        {:reply, error, %{state | broker: broker}}
    end
  end

  # Folds one dispatch's primary-assigned end offset into the produce: when it matches the plan this
  # is a no-op; when frontends interleaved, the batch's placement becomes the actual contiguous span
  # `[actual - count + 1, actual]` and the local counter jumps forward to the primary's end.
  defp adopt_result(state, pending, %{range_id: range_id, last: expected, count: count}, actual) do
    if actual == expected do
      {state, pending}
    else
      placements = Map.put(pending.placements, range_id, {actual - count + 1, actual})
      state = %{state | broker: Broker.adopt_offsets(state.broker, range_id, actual)}
      {state, %{pending | placements: placements}}
    end
  end

  defp finish_async_produce(state, ref, pending, reply) do
    Process.cancel_timer(pending.timer)
    GenServer.reply(pending.from, reply)

    %{state | async_produces: Map.delete(state.async_produces, ref)}
    |> wake_waiters(pending.topic, reply)
    |> wake_subscribers(pending.topic, reply)
  end

  # Group-commit produce: `append/5` buffers the batch (no fsync) and reserves offsets exactly as the sync
  # path does, so the routing/offset code is shared; the client reply is parked until the next
  # `:group_flush` makes it durable. A routing/append error (bad topic, unroutable key) is returned now,
  # not parked, since nothing was buffered.
  defp produce_grouped(from, topic, records, state) do
    if state.pending_records >= state.max_inflight_records do
      # Backpressure: shed load gracefully. Replying now (fast) keeps the caller's produce call from timing
      # out and crashing its connection; the client sees an `:overloaded` error and backs off.
      {:reply, {:error, :overloaded}, state}
    else
      case Broker.produce(state.broker, topic, records, &ReplicationServer.append/5) do
        {broker, {:ok, _placements} = reply} ->
          waiter = %{from: from, reply: reply, topic: topic}
          pending_records = state.pending_records + length(records)

          state = %{
            state
            | broker: broker,
              pending_produce: [waiter | state.pending_produce],
              pending_records: pending_records
          }

          # Flush eagerly once enough is parked so each fsync (and so each reply) stays bounded; otherwise
          # let the interval timer fire.
          if pending_records >= state.flush_max_records do
            {:noreply, do_flush(state)}
          else
            {:noreply, ensure_flush_timer(state)}
          end

        {broker, {:error, _reason} = error} ->
          {:reply, error, %{state | broker: broker}}
      end
    end
  end

  # Schedules the next flush only when none is already pending, so a burst of produces shares one timer.
  defp ensure_flush_timer(%{gc_timer: nil} = state) do
    %{state | gc_timer: Process.send_after(self(), :group_flush, state.gc_interval)}
  end

  defp ensure_flush_timer(state), do: state

  defp wake_waiters(state, _topic, {:error, _reason}), do: state

  defp wake_waiters(state, topic, {:ok, _placements}) do
    {on_topic, others} = Enum.split_with(state.waiters, &(&1.topic == topic))

    still_waiting =
      Enum.reduce(on_topic, [], fn waiter, keep ->
        case consume_ranges(state.broker, waiter.topic, waiter.positions, waiter.max, waiter.ranges) do
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
      # `subscriber.ranges` scopes the push to a group member's assigned ranges (nil = the whole group).
      case consume_ranges(broker, subscriber.topic, subscriber.positions, budget, subscriber.ranges) do
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
  #   * `:metadata_vnodes`. A sharded control plane: one ra cluster per vnode, each placed on its own
  #     `nodes`, routed by topic. Every node only *routes* at boot; the reconcile loop bootstraps the
  #     clusters on whichever node is currently the leader. `metadata_vnodes` is `[{vnode_id, token,
  #     nodes}]` (D-c).
  #   * `:metadata_cluster`: a single ra cluster, the whole metadata in one Raft group (D-a/D1 HA).
  #   * neither, in-memory metadata (single node).
  defp with_metadata_authority(opts, _cluster, _nodes, [_ | _] = vnodes, orchestrator?) do
    replicated = build_replicated(vnodes)
    # Publish the topic→vnode routing so consumer-group coordination is forwarded to the owning node
    # (the same HashRing the metadata is sharded by). Absent in single-node/in-memory → coordination
    # stays local.
    CoordinatorRouter.put_topology(replicated.ring, replicated.vnodes)
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

  @doc """
  Rebuilds a sharded broker's metadata routing for a new ring `topology` (a vnode split adopted via
  gossip): the local read cache takes the new ring: existing vnodes keep their cached `Metadata`, a
  newly-added vnode starts **empty** until the next refresh from `ra`, and the write path is re-routed
  over the topology's `%{vnode_id => nodes}` placements (server id = `{vnode_id, a_member}`). Pure: the
  metadata catch-up for a new/changed vnode is the separate refresh side effect. Returns the new broker.
  """
  @spec adopt_topology(Broker.t(), RingTopology.t()) :: Broker.t()
  def adopt_topology(%Broker{} = broker, %RingTopology{ring: ring} = topology) do
    metadata_by_vnode =
      Map.new(HashRing.vnode_ids(ring), fn vnode_id ->
        {vnode_id, Map.get(broker.dsrsm.vnodes, vnode_id, Metadata.new())}
      end)

    %{
      broker
      | dsrsm: DSRSM.seed(ring, metadata_by_vnode),
        command_fun: sharded_command_fun(replicated_of(topology))
    }
  end

  # The ReplicatedDSRSM (routing view) for a topology: its ring plus the vnode→server map.
  defp replicated_of(%RingTopology{ring: ring} = topology) do
    %ReplicatedDSRSM{ring: ring, vnodes: RingTopology.servers(topology)}
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
