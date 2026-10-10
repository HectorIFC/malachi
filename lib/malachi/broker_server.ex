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

  require Logger
  require OpenTelemetry.Tracer, as: Tracer

  alias Malachi.Broker
  alias Malachi.Broker.ReadView
  alias Malachi.Broker.Skip
  alias Malachi.BrokerServer.ConsumeIndex
  alias Malachi.BrokerServer.Subscribers
  alias Malachi.Cluster.BoundedFanout
  alias Malachi.Cluster.DSRSM
  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.ReplicatedDSRSM
  alias Malachi.Cluster.ReplicatedMetadata
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.RingTopology
  alias Malachi.Consumer.CoordinatorRouter
  alias Malachi.Consumer.GroupCoordinator
  alias Malachi.I18n
  alias Malachi.Metadata
  alias Malachi.Retention.SkipReporter
  alias Malachi.StreamWindow
  alias Malachi.Telemetry
  alias Malachi.UnexpectedMessage
  alias OpenTelemetry.Ctx

  @default_brokers_refresh_interval 1_000

  # Bounds the split/merge fence, a call that is waited on. The produce roll's fence is sent as a cast
  # instead: see `fence_and_seal/2` for why a waited-on network call belongs on one and not the other.
  @default_fence_timeout 1_000
  # Produce call timeout: must exceed every server-side completion path (replication no_quorum ~5s,
  # the async-produce safety timer at 6s), so callers get a real error reply, never a call exit.
  @produce_call_timeout 10_000
  # Safety net for an async produce whose replication result never arrives (e.g. the cast to a dead
  # primary was silently dropped): reply an error instead of leaving the caller to time out.
  @async_produce_timeout 6_000
  # How long a roll's fence may go unanswered before it is sent again, matching the heal coordinator's probe
  # timeout. A spurious resend is safe: the roll stays owed and a fence is idempotent, answering the same
  # numbers.
  @roll_fence_retry_ms 1_000

  # A binding waits for its commit for at most 2 s inside this loop (`Malachi.Cluster.ReplicatedMetadata`),
  # so the caller waits a little longer than that plus a busy mailbox, and never less: a caller that gave
  # up first would exit while the answer it was waiting for is still on its way.
  @bind_call_timeout_ms 4_000
  # What one control plane read may cost the reconcile. The rule is the one already written beside
  # `safe_durable_end/3`: a remote call the reconcile makes must cost milliseconds, not ra's default
  # five seconds. It bounds the boot reconcile (which runs on this loop, before the first client call)
  # and each read the periodic reconcile task makes.
  @default_reconcile_read_timeout 1_000
  # How long the reconcile TASK may run before it is killed. Its reads are bounded by the above, but
  # bootstrapping a vnode reaches `:ra.start_cluster`, which does an `rpc:call/4` with no timeout at
  # all. Only one reconcile runs at a time, so a task wedged there would mean no reconcile ever runs
  # again: this deadline is what keeps that from happening silently.
  @default_reconcile_deadline 30_000

  # --- client API ---

  @doc """
  Starts a server whose segment storage is rooted at `directory`.

  ## Options
    * `:brokers` - references of the `Malachi.Cluster.ReplicationServer`s that segments are placed
      on. When given, this server uses them and does not start its own; when omitted, it starts a
      single local replication server rooted at `directory` (single-node default).
    * `:live_brokers` - a `(-> [broker])` (e.g. from membership); when given, the placement broker
      set is refreshed from it every `:brokers_refresh_interval` ms, so new segments land on
      currently-alive brokers. An empty result is ignored (the last non-empty set is kept), and so is
      a call that exits (the membership server is down); `:broker_attributes` likewise.
    * `:brokers_refresh_interval` - refresh period in ms (default 1000).
    * `:reconcile_read_timeout` - ms one control plane read made by the reconcile may take before the
      vnode counts as unreachable for that pass (default 1000). Deliberately far below ra's 5s.
    * `:reconcile_deadline_ms` - ms the periodic reconcile task may run before it is killed and the
      tick counted as degraded (default 30000). See `reconcile_now/2`.
    * `:fence_timeout` - ms a split's or a merge's store fence may take before the operation is
      refused (default 1000). The produce roll's fence is asynchronous and not bounded by this: see
      `fence_and_seal/2`.
    * `:metadata_cluster` - a Raft cluster name (atom). When given, the metadata is made
      authoritative via that `ra` cluster (mutations go through the log; reads come from a local
      cache); `ra` must already be running. When omitted, metadata is in memory and gone on a restart:
      only the data-plane sharding measurement mode (`MALACHI_DATA_SHARDS` > 1) and direct callers
      such as benchmarks run that way. A single node runs a one-member cluster.
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
    * `:segment_max_bytes` - byte threshold at which the active segment asks to roll.
    * `:skip_reporter` - the `Malachi.Retention.SkipReporter` that push subscriptions report skipped
      data to. Defaults to `Malachi.Retention.SkipReporter.name_for/1` of `:name` (nil, so no reporting,
      for an unnamed server).
    * remaining options are forwarded to a started `Malachi.Cluster.ReplicationServer` (segment log
      options such as `:max_bytes`, `:flush_bytes`, `:index_interval`); ignored with `:brokers`.
    * standard `GenServer` options (`:name`, etc.) are honored.
  """
  @spec start_link(Path.t(), keyword()) :: GenServer.on_start()
  def start_link(directory, opts \\ []) do
    {gen_server_opts, broker_opts} =
      Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after])

    broker_opts = Keyword.put_new(broker_opts, :skip_reporter, SkipReporter.name_for(gen_server_opts[:name]))
    GenServer.start_link(__MODULE__, {directory, broker_opts}, gen_server_opts)
  end

  @doc "Creates a topic (and its root range); returns `{:ok, root_range_id}` or an error."
  @spec create_topic(GenServer.server(), Malachi.Metadata.topic_name(), pos_integer()) :: term()
  def create_topic(server, name, keyspace_bits),
    do: GenServer.call(server, {:create_topic, name, keyspace_bits})

  @doc "Binds `topic` to a policy name, or detaches it with `nil` (`Malachi.Broker.bind_topic_policy/3`)."
  @spec bind_topic_policy(GenServer.server(), Malachi.Metadata.topic_name(), Malachi.Metadata.policy_name() | nil) ::
          :ok | {:error, term()}
  def bind_topic_policy(server, topic, policy_name),
    do: GenServer.call(server, {:bind_topic_policy, topic, policy_name}, @bind_call_timeout_ms)

  @doc """
  The policy name `topic` is bound to (`nil` for none), `{:error, :no_such_topic}`, or
  `{:error, :metadata_unavailable}` while this node has not yet heard from the topic's vnode.
  """
  @spec topic_policy_name(GenServer.server(), Malachi.Metadata.topic_name()) ::
          {:ok, Malachi.Metadata.policy_name() | nil} | {:error, :no_such_topic | :metadata_unavailable}
  def topic_policy_name(server, topic), do: GenServer.call(server, {:topic_policy_name, topic})

  @doc "The topics bound to the policy `name`, sorted."
  @spec topics_bound_to(GenServer.server(), Malachi.Metadata.policy_name()) :: [Malachi.Metadata.topic_name()]
  def topics_bound_to(server, name), do: GenServer.call(server, {:topics_bound_to, name})

  # How much longer than its own wait a `fetch_range/4` call waits for the broker to answer it.
  @fetch_range_margin_ms 5_000

  @doc """
  Opens a producer stream on `range_id` for `pid` (`Malachi.ProducerStreams`): `{:ok, token, segment_id,
  broker}` when this node leads the range's active segment (opened now if the range has none), `broker`
  being this server's pid, the process whose index holds the stream; `{:moved, reason,
  targets}` when the stream belongs elsewhere (the segment is led by another node, or the range was
  retired), or `{:error, reason}`. The token closes the stream and names it in `{:stream_moved, token,
  reason, targets}`, which the broker sends `pid` when the range moves on.
  """
  @spec open_stream(GenServer.server(), Metadata.range_id(), pid()) ::
          {:ok, reference(), Metadata.segment_id(), pid()} | {:moved, atom(), list()} | {:error, term()}
  def open_stream(server, range_id, pid \\ self()), do: GenServer.call(server, {:open_stream, range_id, pid})

  @doc "Closes the producer or consume stream `token` names. Idempotent."
  @spec close_stream(GenServer.server(), reference()) :: :ok
  def close_stream(server, token), do: GenServer.call(server, {:close_stream, token})

  @doc """
  Opens a consume stream on `range_id` for `pid` (`Malachi.ConsumeStreams`), from `start` (resolved by
  `Malachi.Broker.consume_start/3`). Served where the range is read: where its active segment is led, or
  on any node while it has none (opening never places one): `{:ok, token, position, broker}` here,
  `{:moved, reason, targets}` when it belongs elsewhere, or `{:error, reason}`.

  The broker reads nothing for the stream: it hands the connection a `Malachi.Broker.ReadView` of the
  range in `{:consume_wake, token, view, skip_reporter}`, at once and then each time the range grows past
  the end the connection said it had read to (`arm_consume/3`), and the connection reads and pushes. When
  the range moves on it is sent `{:stream_moved, token, reason, targets}`, as a producer stream is.
  """
  @spec open_consume(GenServer.server(), Metadata.range_id(), term(), pid()) ::
          {:ok, reference(), {non_neg_integer(), non_neg_integer()}, pid()}
          | {:moved, atom(), list()}
          | {:error, term()}
  def open_consume(server, range_id, start, pid \\ self()),
    do: GenServer.call(server, {:open_consume, range_id, start, pid})

  @doc """
  The consume stream `token` read its range up to `seen_end` (the durable end of the last view it was
  handed) and waits for more: it is woken once the range's durable records end past that.
  """
  @spec arm_consume(GenServer.server(), reference(), non_neg_integer()) :: :ok
  def arm_consume(server, token, seen_end), do: GenServer.cast(server, {:arm_consume, token, seen_end})

  @doc "Hands the consume stream `token` a fresh view of its range at once (after a read that failed)."
  @spec refresh_consume(GenServer.server(), reference()) :: :ok
  def refresh_consume(server, token), do: GenServer.cast(server, {:refresh_consume, token})

  @doc """
  The read of one page of `range_id` from `start` (`fetch_range`, wire key 31), served where the range is
  read, as `open_consume/4` is: `{:ok, position, view, skip_reporter}` once the range holds records past
  `position`, or after `wait_ms` without any (the read then finds none), for the caller to read through
  `view`. `{:moved, reason, targets}` or `{:error, reason}` as `open_consume/4` answers.
  """
  @spec fetch_range(GenServer.server(), Metadata.range_id(), term(), non_neg_integer()) ::
          {:ok, {non_neg_integer(), non_neg_integer()}, ReadView.t(), atom() | pid() | nil}
          | {:moved, atom(), list()}
          | {:error, term()}
  def fetch_range(server, range_id, start, wait_ms),
    do: GenServer.call(server, {:fetch_range, range_id, start, wait_ms}, wait_ms + @fetch_range_margin_ms)

  @doc """
  Sends an append of a producer stream without waiting for it, under `label`, adding the request to
  `reqids` (`:gen_server.send_request/4`). Its answer, `{produce_reply, scale}` with `scale` the share of
  the stream's window the range's load leaves, arrives as a message `:gen_server.check_response/3` reads.
  """
  @spec send_stream_produce(
          GenServer.server(),
          Malachi.Metadata.range_id(),
          [Malachi.Log.Record.t()],
          term(),
          :gen_server.request_id_collection()
        ) ::
          :gen_server.request_id_collection()
  def send_stream_produce(server, range_id, records, label, reqids),
    do: :gen_server.send_request(server, {:stream_produce, range_id, records, Ctx.get_current()}, label, reqids)

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
          {:ok, [Malachi.Log.Record.t()], Broker.consume_cursor(), [Skip.t()]} | {:error, term()}
  def read_consume(server, range_id, cursor, max_records),
    do: GenServer.call(server, {:read_consume, range_id, cursor, max_records})

  @doc """
  Consumes a topic's current ranges from `positions`, returning `{records, next_positions, skips}`.
  When `wait_ms > 0` and nothing is available yet, the call blocks (long-poll) until a produce to the
  topic delivers data or `wait_ms` elapses (then `records` and `skips` are `[]`). With `wait_ms == 0`
  it returns immediately. `positions`/`next_positions` map each range id to its
  `Broker.consume_cursor`.

  `skips` are the stretches of history the returned positions moved past because they were no longer
  stored (`Malachi.Broker.Skip`), for the caller to attribute: this server does not know which consumer
  group a fetch belongs to, so it neither counts nor logs them here.
  """
  @spec consume(
          GenServer.server(),
          Malachi.Metadata.topic_name(),
          map(),
          pos_integer(),
          non_neg_integer(),
          [term()] | nil
        ) ::
          {[Malachi.Log.Record.t()], map(), [Skip.t()]} | {:error, term()}
  def consume(server, topic, positions, max_records, wait_ms, ranges \\ nil) do
    # The call may block up to wait_ms server-side; give it headroom over the default 5s call timeout.
    GenServer.call(server, {:consume, topic, positions, max_records, wait_ms, ranges}, wait_ms + 5_000)
  end

  @doc "Streams one bounded page of a range's cross-epoch history (see `Malachi.Broker.stream_history/5`)."
  @spec stream_history(GenServer.server(), Malachi.Metadata.range_id(), Broker.history_cursor(), pos_integer()) ::
          {:ok, [Malachi.Log.Record.t()], Broker.history_cursor()} | {:error, term()}
  def stream_history(server, range_id, cursor \\ :start, max_records \\ 1000),
    do: GenServer.call(server, {:stream_history, range_id, cursor, max_records})

  @doc """
  Whether this broker has read every metadata vnode at least once since boot. Until it has, a topic
  whose vnode is still silent is represented by an empty placeholder in the local cache, and every read
  of it answers a page with no records: durable data reported as a drained topic. A node in that state
  can accept connections and pass `/health` while being unable to answer a single read honestly.
  """
  @spec metadata_ready?(GenServer.server(), timeout()) :: boolean()
  def metadata_ready?(server, timeout \\ 1_000), do: GenServer.call(server, :metadata_ready?, timeout)

  @doc """
  Runs one control plane reconcile **synchronously** and returns once its result has been applied.

  The periodic reconcile runs off this server's loop (see the `:reconcile` clauses), so sending
  `:reconcile` and waiting for the server to answer anything no longer proves the pass landed. This
  is the barrier that does. It is bounded the same way the boot reconcile is, by
  `:reconcile_read_timeout` per read, so it cannot hold the loop for ra's five seconds either.

  Same shape as `Malachi.Cluster.LeaseReconciler.reconcile_now/1` and
  `Malachi.Cluster.AutoRebalancer.reconcile_now/1`. A server with in-memory metadata has nothing to
  reconcile and answers `:ok` at once.
  """
  @spec reconcile_now(GenServer.server(), timeout()) :: :ok
  def reconcile_now(server, timeout \\ 10_000), do: GenServer.call(server, :reconcile_now, timeout)

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
  The `Malachi.Cluster.ReplicationServer` this broker owns, or `nil` when it was given an external
  broker set (the clustered shape, where the replication server is supervised and named separately).

  A single-node broker starts its own, unnamed, so this is the only way to name it as a replica: the
  integrity scrub needs it to tell which stored segments are its own. Ask per use rather than caching
  it, since a broker restart replaces the process.
  """
  @spec replication_ref(GenServer.server()) :: pid() | nil
  def replication_ref(server), do: GenServer.call(server, :replication_ref)

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
  Subscribes the calling process as a streaming consumer of `topic` for consumer `group`, resuming from
  the group's committed position and bounded by a credit `window` (at most `window` records in flight,
  at most `max` per push). Whenever it is owed records the process receives `{:log_read, plan}` and runs
  the plan with `execute_push/1`, which reads in the calling process and returns the records to deliver.
  Ack with `stream_ack/5` to return credit and commit progress. `:ok`.
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

  @doc """
  Runs, in the calling process, a read this broker handed to it as `{:log_read, plan}`: a streaming
  subscriber receives one whenever it is owed records, and runs it with this function rather than the
  broker running it in the loop that serializes appends. Reads the plan's ranges through its view, tells
  the broker how the read ended (what it pushed and where it left the positions, or that it failed, in
  which case nothing moves and the next produce or ack reads again), and returns the records to deliver
  as `{:ok, topic, records, next_positions}`, or `:nothing` when there are none to deliver.

  The caller delivers the records it gets back (a connection writes them to its socket) before it runs
  the next plan. The broker hands a subscriber one plan at a time, so its pushes stay in order.
  """
  @spec execute_push(map()) ::
          {:ok, Malachi.Metadata.topic_name(), [Malachi.Log.Record.t()], Malachi.Metadata.offsets()} | :nothing
  def execute_push(%{broker: broker, ref: ref, topic: topic, group: group} = plan) do
    case Broker.consume_shared(plan.view, plan.ranges, plan.positions, plan.budget, &ReplicationServer.read/4) do
      {:ok, {records, next_positions, skips}} ->
        GenServer.cast(broker, {:read_done, ref, topic, group, {:ok, length(records), next_positions, skips}})
        if records == [], do: :nothing, else: {:ok, topic, records, next_positions}

      {:error, _reason} ->
        GenServer.cast(broker, {:read_done, ref, topic, group, :error})
        :nothing
    end
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

    {stream_inflight_soft, opts} =
      Keyword.pop(opts, :stream_inflight_soft, StreamWindow.inflight_soft())

    {stream_inflight_hard, opts} =
      Keyword.pop(opts, :stream_inflight_hard, StreamWindow.inflight_hard())

    {live_brokers, opts} = Keyword.pop(opts, :live_brokers)
    {broker_attributes, opts} = Keyword.pop(opts, :broker_attributes)
    {spread_by, opts} = Keyword.pop(opts, :spread_by)
    {min_domains, opts} = Keyword.pop(opts, :min_domains)
    {placement_policy, opts} = Keyword.pop(opts, :placement_policy)
    {refresh_interval, opts} = Keyword.pop(opts, :brokers_refresh_interval, @default_brokers_refresh_interval)
    {fence_timeout, opts} = Keyword.pop(opts, :fence_timeout, @default_fence_timeout)
    {read_timeout, opts} = Keyword.pop(opts, :reconcile_read_timeout, @default_reconcile_read_timeout)
    {reconcile_deadline, opts} = Keyword.pop(opts, :reconcile_deadline_ms, @default_reconcile_deadline)
    {metadata_cluster, opts} = Keyword.pop(opts, :metadata_cluster)
    {metadata_nodes, opts} = Keyword.pop(opts, :metadata_nodes, [node()])
    {metadata_vnodes, opts} = Keyword.pop(opts, :metadata_vnodes)
    {bootstrap_orchestrator, opts} = Keyword.pop(opts, :bootstrap_orchestrator, fn -> true end)
    {skip_reporter, opts} = Keyword.pop(opts, :skip_reporter)
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
      |> with_metadata_authority(
        metadata_cluster,
        metadata_nodes,
        metadata_vnodes,
        bootstrap_orchestrator,
        read_timeout
      )

    {:ok, broker} = Broker.open(broker_opts)

    # Only a replicated control plane re-seeds this cache from a read, so only it can undo a local
    # write by installing one; in-memory metadata never replaces itself.
    broker = if metadata_refresh, do: Broker.journal(broker), else: broker

    # With authoritative (ra-backed) metadata, the topics/ranges/segments survive a restart, but the
    # in-memory offsets and segment_seq maps do not: without recovery, every read of pre-restart data
    # clamps to :eof at offset 0 (durable data unreadable, the failure the chaos harness caught).
    # An unreachable primary leaves its range unrecovered, refusing reads until a later pass or a
    # produce learns the end (`Malachi.Broker.seed_unrecovered_range/3`). In-memory metadata (nil
    # refresh) has nothing to recover from because it does not survive the restart at all, which is why
    # only the data-plane sharding measurement mode still runs that way: a single node runs a
    # one-member cluster (`Malachi.Config.log_cluster/2`).
    broker = if metadata_refresh, do: recover_range_state(broker), else: broker

    state = %{
      broker: broker,
      replication: owned_replication,
      live_brokers: live_brokers,
      broker_attributes: broker_attributes,
      refresh_interval: refresh_interval,
      fence_timeout: fence_timeout,
      # What one control plane read may cost the reconcile, and how long the reconcile task may run
      # before it is killed. See the attributes they default from.
      reconcile_read_timeout: read_timeout,
      reconcile_deadline: reconcile_deadline,
      # The reconcile task currently in flight (`%{ref, pid, generation, timer}`), or nil. At most one
      # runs at a time: a tick that finds one still running counts itself degraded and starts none, so
      # a slow control plane cannot make the tasks pile up.
      reconcile_task: nil,
      # Bumped by `:adopt_topology`. A task carries the generation it was started with, and a result
      # from an older one is dropped: applying it would reinstate the ring from before the split and
      # keep it until the next tick managed to read.
      reconcile_generation: 0,
      # One slot for the ref of the last task a deadline gave up on. The task is killed, but its reply
      # may already have been in the mailbox, and a reply with no clause is a counted drop rather than
      # a crash (`Malachi.UnexpectedMessage`). One slot suffices: only one task is ever in flight.
      abandoned_ref: nil,
      # Re-seeds the local metadata cache from the authoritative ra clusters (fills vnodes not yet ready
      # at boot; picks up writes made through other nodes). `nil` for in-memory metadata. Run by
      # `run_reconcile/3` and applied by `apply_reconcile/2`.
      metadata_refresh: metadata_refresh,
      # The metadata vnodes this broker has read at least once since boot (see `seen_vnodes/3`). Empty
      # at boot even when the boot snapshot succeeded: the reconcile that follows `init` fills it, and
      # treating the boot snapshot as authoritative would mean trusting a snapshot taken before the ra
      # clusters were necessarily up. Meaningless without `metadata_refresh` (in-memory metadata is
      # already local truth), which is why `metadata_ready?/2` checks that first.
      seen_vnodes: MapSet.new(),
      # Sharded control plane only: `%{orchestrator?: (-> boolean), vnodes: [{id, token, nodes}],
      # replicated: ReplicatedDSRSM.t()}`. The reconcile loop uses it to bootstrap missing vnodes while
      # this node is the leader (see `bootstrap_missing_vnodes/2`). `nil` otherwise. Both `vnodes` and
      # `replicated` follow the ring this node adopted last (see `adopt_bootstrap/3`), not the one it
      # booted with.
      bootstrap: bootstrap,
      # Long-poll: fetches that found nothing and are willing to wait, parked here until a produce to
      # their topic wakes them (with data) or their timer fires (empty). See `handle_call({:consume,…})`.
      waiters: [],
      # Streaming subscribers (`Malachi.BrokerServer.Subscribers`). A subscriber is pushed records as
      # they are produced, bounded by a credit window (in_flight <= window); acks return credit and
      # durably commit the group's position. This loop only decides who is read for: the read and the
      # socket write run in the subscriber's own process. See `dispatch_reads/2` and `execute_push/1`.
      subscribers: Subscribers.new(),
      # Where a push reports the data it moved a subscriber past (see `handle_cast({:read_done, ...})`);
      # nil = nowhere.
      skip_reporter: skip_reporter,
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
      # Records dispatched for each range and not yet answered (the async path), which scale a producer
      # stream's window between these two limits (`Malachi.StreamWindow`).
      range_inflight: %{},
      stream_inflight_soft: stream_inflight_soft,
      stream_inflight_hard: stream_inflight_hard,
      # Open producer streams, by the monitor ref of the connection that holds them: the range and the
      # segment and primary each was opened on. A stream whose range moves on is sent `{:stream_moved, ...}`
      # and dropped (`sweep_streams/1`).
      streams: %{},
      # Open consume streams and waiting `fetch_range` calls, by range (`ConsumeIndex`).
      consume: ConsumeIndex.new(),
      # In-flight async produces (the non-group-commit path): ref => the parked caller, its computed
      # placements, how many replication dispatches are still owed, the records of the dispatches that went
      # out behind this frontend's own fence (`parts`, so they can be planned again), and the safety timer.
      async_produces: %{},
      # Produces held behind a fence this frontend sent and has not seen answered, by range: each a refused
      # async dispatch or a whole group-commit call. Released when the answer lands, or when the retry
      # window runs out. See `park_on_fence/3`.
      fence_parked: %{},
      # The unknown message shapes already logged (see `Malachi.UnexpectedMessage`).
      unexpected_shapes: MapSet.new()
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

  def handle_call({:bind_topic_policy, topic, policy_name}, _from, state) do
    {broker, reply} = Broker.bind_topic_policy(state.broker, topic, policy_name)
    {:reply, reply, %{state | broker: broker}}
  end

  # Gated like consume: before the topic's vnode has answered, the cache holds an empty placeholder for
  # it, which would report an existing topic as missing, or bound to nothing.
  def handle_call({:topic_policy_name, topic}, _from, state) do
    if topic_metadata_ready?(state, topic) do
      {:reply, Broker.topic_policy_name(state.broker, topic), state}
    else
      {:reply, {:error, :metadata_unavailable}, state}
    end
  end

  def handle_call({:topics_bound_to, name}, _from, state) do
    {:reply, Broker.topics_bound_to(state.broker, name), state}
  end

  # A producer stream on one range (`Malachi.ProducerStreams`), opened where the range's active segment is
  # led: a node that does not lead it answers where it is, so the client talks to the primary directly.
  def handle_call({:open_stream, range_id, pid}, _from, state), do: do_open_stream(range_id, pid, state)

  def handle_call({:close_stream, token}, _from, state) do
    if is_map_key(state.streams, token) or ConsumeIndex.consumer?(state.consume, token) do
      Process.demonitor(token, [:flush])
      {:reply, :ok, drop_stream(state, token)}
    else
      {:reply, :ok, state}
    end
  end

  # A consume stream on one range (`Malachi.ConsumeStreams`), served where the range is served
  # (`with_consume_range/3`): its records are read through views of the range, up to where they are
  # durable, wherever its sealed segments live.
  def handle_call({:open_consume, range_id, start, pid}, _from, state) do
    with_consume_range(range_id, state, fn state ->
      case Broker.consume_start(state.broker, range_id, start) do
        {:ok, position} ->
          token = Process.monitor(pid)

          state =
            wake_ranges(%{state | consume: ConsumeIndex.put_consumer(state.consume, token, pid, range_id)}, [range_id])

          {:reply, {:ok, token, position, self()}, state}

        {:error, _reason} = error ->
          {:reply, error, state}
      end
    end)
  end

  def handle_call({:fetch_range, range_id, start, wait_ms}, from, state) do
    with_consume_range(range_id, state, fn state ->
      case Broker.consume_start(state.broker, range_id, start) do
        {:ok, position} ->
          if wait_ms == 0 or Broker.consume_ready?(state.broker, range_id, position) do
            {:reply, fetch_reply(state, %{range_id: range_id, position: position}), state}
          else
            ref = make_ref()
            timer = Process.send_after(self(), {:fetch_range_timeout, ref}, wait_ms)
            waiter = %{from: from, range_id: range_id, position: position, timer: timer}
            {:noreply, %{state | consume: ConsumeIndex.put_waiter(state.consume, ref, waiter)}}
          end

        {:error, _reason} = error ->
          {:reply, error, state}
      end
    end)
  end

  # An append on a producer stream: the produce of one range, refused whole when a key falls outside it,
  # answered with the produce's own reply and how much of its window the range's load leaves.
  def handle_call({:stream_produce, range_id, records, ctx}, from, state) do
    {topic, _seq} = range_id

    if Broker.keys_in_range?(state.broker, range_id, records) do
      case handle_call({:produce, topic, records, ctx}, {:stream, from, range_id}, state) do
        {:reply, reply, state} ->
          reply_produce({:stream, from, range_id}, reply, state)
          {:noreply, state}

        noreply ->
          noreply
      end
    else
      {:reply, {{:error, :key_outside_range}, stream_load(state, range_id)}, state}
    end
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
    # The vnode owning this topic may never have answered, in which case this broker knows of no ranges
    # for it and every read would report the topic drained. Saying so is the whole point: an empty page
    # here is a successful wrong answer, and a client cannot tell it from having caught up.
    if topic_metadata_ready?(state, topic) do
      consume_or_park(topic, positions, max_records, wait_ms, ranges, from, state)
    else
      {:reply, {:error, :metadata_unavailable}, state}
    end
  end

  def handle_call(:metadata_ready?, _from, state) do
    {:reply, all_vnodes_seen?(state), state}
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

  # Fence the parent's segment BEFORE the metadata split, which is the order the seal rule requires (the
  # length must be what closing the segment answered) and which also closes the write half of the split
  # gap: a node that has not seen the split can no longer get a record into the parent, because the
  # parent's store is physically fenced before either child exists. The head to fence comes from the
  # CONTROL PLANE (`Broker.active_roll/2`), not from this node's cache, so the fence does not depend on
  # the splitting node having produced to the range: an operator can split from anywhere. Its READS
  # still go to the parent, which is correct cross-epoch behavior.
  def handle_call({:split_range, range_id}, _from, state) do
    case fence_parent(state, range_id) do
      {:ok, state} ->
        {broker, reply} = Broker.split_range(state.broker, range_id)
        {:reply, reply, sweep_streams(%{state | broker: broker})}

      {:error, {^range_id, reason}, state} ->
        {:reply, {:error, {:fence_failed, range_id, reason}}, state}
    end
  end

  # Both parents are fenced first, and the merge is applied only when both succeeded. A fence that
  # succeeds for A while B's fails leaves A with a short sealed segment and the merge refused; A's next
  # produce opens a fresh segment at the sealed end, which is harmless.
  def handle_call({:merge_ranges, range_id_a, range_id_b}, _from, state) do
    with {:ok, state} <- fence_parent(state, range_id_a),
         {:ok, state} <- fence_parent(state, range_id_b) do
      {broker, reply} = Broker.merge_ranges(state.broker, range_id_a, range_id_b)
      {:reply, reply, sweep_streams(%{state | broker: broker})}
    else
      {:error, {range_id, reason}, state} -> {:reply, {:error, {:fence_failed, range_id, reason}}, state}
    end
  end

  def handle_call({:active_range_ids, topic}, _from, state) do
    {:reply, Broker.active_range_ids(state.broker, topic), state}
  end

  def handle_call(:metadata, _from, state) do
    {:reply, Broker.metadata(state.broker), state}
  end

  def handle_call(:replication_ref, _from, state) do
    {:reply, state.replication, state}
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
    {:reply, :ok, sweep_streams(%{state | broker: Broker.apply_heal(state.broker, commands)})}
  end

  def handle_call({:delete_segment, segment_id}, _from, state) do
    {broker, reply} = Broker.delete_segment(state.broker, segment_id)
    # A read already handed out may list this segment in its view; once its copies are deleted that read
    # fails (`Malachi.Broker` will not take a deleted segment for drained data) and nothing else would ask
    # for another until a produce, an ack or a reconcile tick. Waking the topic's subscribers here reads
    # them again at once with a view that no longer lists it (one mid-read is read again when its read
    # ends).
    state = %{state | broker: broker}

    case Metadata.segment_routing_topic(segment_id) do
      nil -> {:reply, reply, state}
      topic -> {:reply, reply, wake_subscribers(state, topic, {:ok, []})}
    end
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

    {reads, subscribers} =
      Subscribers.add(state.subscribers, %{
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

    {:reply, :ok, dispatch_reads(%{state | subscribers: subscribers}, reads)}
  end

  def handle_call({:stream_ack, topic, group, positions, count, pid, ranges, coordinator}, _from, state) do
    # commit the group's position durably, then return `count` credit to this subscriber and push more.
    # `ranges` (from the LogApi member poll) refreshes this member's assignment, so a rebalance is picked
    # up on the ack (nil keeps the current scope: a whole-group or unchanged member subscription).
    # `coordinator` refreshes the member's resolved coordinator ref, so after a vnode leadership change
    # the :DOWN leave targets the current owner (nil keeps the ref captured at subscribe).
    {broker, _reply} = Broker.commit_offset(state.broker, group, topic, positions)
    {reads, subscribers} = Subscribers.ack(state.subscribers, topic, pid, count, ranges, coordinator)
    {:reply, :ok, dispatch_reads(%{state | broker: broker, subscribers: subscribers}, reads)}
  end

  def handle_call({:unsubscribe, topic, pid}, _from, state) do
    {:reply, :ok, drop_subscriber(state, topic, pid)}
  end

  def handle_call(:reconcile_now, _from, state) do
    {:reply, :ok, reconcile_synchronously(state)}
  end

  def handle_call(message, _from, state) do
    {:reply, UnexpectedMessage.unknown_call_reply(), drop_unexpected(state, :call, message)}
  end

  @impl true
  # A subscriber's process finished the read `dispatch_reads/2` handed it (`execute_push/1`): its
  # position and credit move by what was pushed, the data it was moved past is reported, and it is read
  # for again at once if a wake arrived meanwhile.
  def handle_cast({:arm_consume, token, seen_end}, state), do: {:noreply, arm(state, token, seen_end)}

  # -1: any durable end wakes it, so it is handed a fresh view now
  def handle_cast({:refresh_consume, token}, state), do: {:noreply, arm(state, token, -1)}

  def handle_cast({:read_done, ref, topic, group, outcome}, state) do
    {result, skips} =
      case outcome do
        {:ok, pushed, positions, skips} -> {{:ok, pushed, positions}, skips}
        :error -> {:error, []}
      end

    SkipReporter.report(state.skip_reporter, topic, group, skips)
    {reads, subscribers} = Subscribers.read_done(state.subscribers, ref, result)
    {:noreply, dispatch_reads(%{state | subscribers: subscribers}, reads)}
  end

  # Adopt a ring change (a vnode split) gossiped in via the membership hook: rebuild the metadata routing
  # (cache ring + write router), the refresh source and the bootstrap pass, so the periodic reconcile
  # re-seeds against the new topology instead of reverting to the boot ring, and bootstraps the vnodes
  # of the new ring instead of the boot list. Fired async (a cast), so it never blocks membership.
  def handle_cast({:adopt_topology, %RingTopology{} = topology}, state) do
    replicated = replicated_of(topology)
    broker = adopt_topology(state.broker, topology)
    metadata_refresh = sharded_refresh(replicated, state.reconcile_read_timeout)
    bootstrap = adopt_bootstrap(state.bootstrap, topology, replicated)

    # A reconcile started before this cast read the PREVIOUS ring. Bumping the generation is what makes
    # its result arrive stale and be dropped, instead of overwriting the ring this cast just installed
    # and reverting the split for a tick.
    {:noreply,
     %{
       state
       | broker: broker,
         metadata_refresh: metadata_refresh,
         bootstrap: bootstrap,
         reconcile_generation: state.reconcile_generation + 1
     }}
  end

  def handle_cast(message, state), do: {:noreply, drop_unexpected(state, :cast, message)}

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
        {state, pending} = settle_dispatch(state, pending, dispatch)

        case result do
          {:ok, actual} ->
            # The range's primary serializes appends and assigns the REAL offsets (the NorthGuard
            # invariant). When several broker frontends interleave on one range, the primary-assigned
            # end can differ from this frontend's precomputed one; the frontend ADOPTS the primary's
            # truth (placements and local counter follow it) instead of failing the produce. That is
            # what lets any node accept a produce for any range with no client-side routing.
            {state, pending} = adopt_result(state, pending, dispatch, actual)

            if pending.remaining == 1 do
              # Every dispatch of this produce has answered, so a roll it tripped is sent its fence now.
              # The cast leaves this loop after the batch's own, so it reaches the primary behind them and
              # the end the fence answers includes the batch that crossed the threshold.
              state = send_roll_fences(state)
              {:noreply, finish_async_produce(state, ref, pending, {:ok, pending.placements})}
            else
              pending = %{pending | remaining: pending.remaining - 1}
              {:noreply, %{state | async_produces: Map.put(state.async_produces, ref, pending)}}
            end

          # The segment was fenced between the plan and the push: seat this frontend at the fenced end so
          # the client's retry opens or adopts the successor instead of racing :segment_overlap.
          {:error, {:sealed, end_offset}} ->
            broker = Broker.forget_sealed(state.broker, dispatch.range_id, dispatch.segment_id, end_offset)
            # the settled pending goes back first: a dispatch parked behind the fence is finished later from
            # what `async_produces` holds, and must not be released from the in-flight count twice
            async_produces = Map.put(state.async_produces, ref, pending)
            state = sweep_streams(%{state | broker: broker, async_produces: async_produces})
            {:noreply, sealed_dispatch(state, ref, pending, dispatch, end_offset)}

          {:error, reason} ->
            {:noreply, finish_async_produce(state, ref, pending, {:error, reason})}
        end
    end
  end

  # The answer to a produce roll's fence (`send_roll_fences/1`). Whatever it says, the produces held behind
  # that fence go again: on success the successor is now open to them, and on failure they learn so from
  # the primary rather than from a wait.
  def handle_info({:seal_result, {:roll_fence, roll}, reply}, state) do
    state = state |> record_roll_fence(roll, reply) |> sweep_streams()
    {:noreply, release_fence_parked(state, roll.range_id, :replan)}
  end

  # The fence's answer never came within the retry window (`park_on_fence/3`). A later park on the same range
  # carries a new token, so a timer that lost the race to an answer releases nothing.
  def handle_info({:fence_park_timeout, range_id, token}, state) do
    case Map.get(state.fence_parked, range_id) do
      %{token: ^token} -> {:noreply, release_fence_parked(state, range_id, :refuse)}
      _released_or_newer -> {:noreply, state}
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
        GenServer.reply(waiter.from, {[], waiter.positions, waiter.skips})
        {:noreply, %{state | waiters: rest}}

      # Already woken by a produce (timer raced the reply); nothing to do.
      {[], _rest} ->
        {:noreply, state}
    end
  end

  # --- the reconcile task's replies ---
  #
  # All four sit ABOVE the generic `:DOWN` clause below, which matches ANY monitor ref and reads it as
  # a subscriber's: a reconcile task's DOWN landing there would be absorbed without a trace. They are
  # also above the `handle_info/2` catch-all, which counts an unmatched message instead of crashing,
  # so a mistyped pattern here shows up as a drop rather than as a failure. Both are why each of these
  # shapes has a test of its own.
  #
  # `ref` repeated between the message and the state is the match: Elixir unifies a variable that
  # appears twice in one pattern, so these only fire for the task this server is currently waiting on.

  # The task answered. Apply it only if the ring it read is still the current one: an `:adopt_topology`
  # that landed while it ran bumped the generation, and installing what it read would revert the split.
  def handle_info({ref, {:reconciled, generation, result}}, %{reconcile_task: %{ref: ref}} = state) do
    state = finish_reconcile_task(state)

    if generation == state.reconcile_generation,
      do: {:noreply, apply_reconcile(state, result)},
      else: {:noreply, state}
  end

  # A reply from the task a deadline already gave up on: it was killed, but its answer could have been
  # in the mailbox when it went. Dropped on purpose, and named so it is not counted as a bug.
  def handle_info({ref, _result}, %{abandoned_ref: ref} = state) do
    {:noreply, %{state | abandoned_ref: nil}}
  end

  # The task crashed. The next tick is already scheduled (the tick schedules before it starts one), so
  # there is nothing to retry here; the node keeps serving the view it holds until a later pass reads.
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{reconcile_task: %{ref: ref}} = state) do
    Logger.warning(I18n.t(:broker_reconcile_task_down, reason: inspect(reason)))
    Telemetry.reconcile_degraded(:down)
    {:noreply, finish_reconcile_task(state)}
  end

  # The task overran. Its reads are bounded, but bootstrapping a vnode reaches `:ra.start_cluster`,
  # which does an `rpc:call/4` with no timeout, so a task can wedge there for good. Killing it is what
  # keeps the mutual exclusion from freezing every later reconcile, silently.
  def handle_info({:reconcile_deadline, ref}, %{reconcile_task: %{ref: ref, pid: pid}} = state) do
    Logger.warning(I18n.t(:broker_reconcile_task_timeout, timeout_ms: state.reconcile_deadline))
    Telemetry.reconcile_degraded(:timeout)
    Process.demonitor(ref, [:flush])
    _ = Task.Supervisor.terminate_child(Malachi.TaskSupervisor, pid)
    {:noreply, %{state | reconcile_task: nil, abandoned_ref: ref}}
  end

  # A deadline whose task already finished: the timer was cancelled, but it may have fired first.
  def handle_info({:reconcile_deadline, _ref}, state), do: {:noreply, state}

  # --- end of the reconcile task's replies ---

  # A streaming subscriber's process died: drop that subscription. Each subscribe monitors anew, so the
  # ref names exactly one subscription and its topic is one index lookup away; a process subscribed to
  # several topics sends one :DOWN per subscription.
  # A producer or consume stream's connection went away: drop the stream (the subscriber clause below
  # handles the rest).
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{streams: streams} = state)
      when is_map_key(streams, ref) do
    {:noreply, drop_stream(state, ref)}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{consume: %ConsumeIndex{consumers: consumers}} = state)
      when is_map_key(consumers, ref) do
    {:noreply, drop_stream(state, ref)}
  end

  # A `fetch_range` that waited `wait_ms` for records and got none: answered with a view to read nothing
  # from, unless the range grew meanwhile and it was answered then.
  def handle_info({:fetch_range_timeout, ref}, state) do
    case ConsumeIndex.pop_waiter(state.consume, ref) do
      :error ->
        {:noreply, state}

      {waiter, consume} ->
        GenServer.reply(waiter.from, fetch_reply(state, waiter))
        {:noreply, %{state | consume: consume}}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    {removed, subscribers} = Subscribers.remove_ref(state.subscribers, ref)

    # a departing group member leaves its group for a fast rebalance: done in an unlinked task, since the
    # coordinator's leave calls back into this broker and a synchronous call from here would deadlock.
    case removed do
      %{member: member, coordinator: coordinator} = sub when member != nil and coordinator != nil ->
        Task.start(fn -> GroupCoordinator.leave(coordinator, sub.group, sub.topic, member) end)

      _not_a_member ->
        :ok
    end

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

  # Also where a roll whose fence went unanswered is sent again: a range that stops producing would
  # otherwise keep its roll owed, and its segment unsealed, for as long as nothing wrote to it.
  def handle_info(:reconcile, state) do
    schedule_reconcile(state)
    {:noreply, state |> start_reconcile_task() |> send_roll_fences()}
  end

  # Group-commit flush: one fsync per pipeline covers every parked producer. Fsync first (durable), then
  # reply to each caller and wake its topic's long-poll consumers and subscribers, so consumers only ever
  # observe durable data. `broker.brokers` is the set of local pipelines a segment can live on (one for
  # rf=1, or several when striping), so flushing all of them covers wherever the buffered batches landed.
  def handle_info(:group_flush, state) do
    {:noreply, do_flush(state)}
  end

  # Stays the LAST info clause: a clause added below it would never be reached, and one added above it
  # with a pattern that does not match what is sent is counted here instead of crashing, which the
  # counter and the test suite's guard are there to catch (see `Malachi.UnexpectedMessage`).
  def handle_info(message, state), do: {:noreply, drop_unexpected(state, :info, message)}

  # Fsyncs every pipeline, replies to the parked producers (now durable), wakes each topic's consumers, and
  # resets the group-commit accumulators. Used by both the interval timer and the eager (size-triggered)
  # path; cancels any pending timer so an eager flush leaves no stale one queued (a stale `:group_flush`
  # that still arrives just re-runs this on empty pending, a no-op).
  defp do_flush(state) do
    # A dead or stuck pipeline must not crash this server: flush/1 is a call with a 5s timeout, so a
    # single bad pipeline would otherwise take down the broker and every parked producer with it.
    # Every pipeline is still attempted (the healthy ones make their buffers durable), but waiters are
    # only acked when ALL of them flushed: an ack must never precede a confirmed fsync. On any failure
    # the whole parked cycle is replied an error and the clients retry.
    #
    # A pipeline that is alive can fail too: `flush/1` answers `{:error, failures}` when a segment's
    # storage failed, including a failure between an append it already answered and this flush. That
    # has to fail the cycle exactly like a dead pipeline does, or those appends would be acked unsynced.
    flushed_ok? =
      Enum.reduce(state.broker.brokers, true, fn pipeline, ok? ->
        try do
          case ReplicationServer.flush(pipeline) do
            :ok ->
              ok?

            {:error, failures} ->
              Logger.warning(I18n.t(:group_flush_failed, pipeline: inspect(pipeline), reason: inspect(failures)))
              false
          end
        catch
          :exit, reason ->
            Logger.warning(I18n.t(:group_flush_failed, pipeline: inspect(pipeline), reason: inspect(reason)))
            false
        end
      end)

    if state.gc_timer, do: Process.cancel_timer(state.gc_timer)

    pending = Enum.reverse(state.pending_produce)
    base = %{state | pending_produce: [], pending_records: 0, gc_timer: nil}

    if flushed_ok? do
      Enum.reduce(pending, base, fn waiter, st ->
        # the load the reply reports is the one this flush is relieving
        reply_produce(waiter.from, waiter.reply, state)

        st
        |> wake_waiters(waiter.topic, waiter.reply)
        |> wake_subscribers(waiter.topic, waiter.reply)
      end)
    else
      Enum.each(pending, &reply_produce(&1.from, {:error, :flush_failed}, state))
      base
    end
  end

  @impl true
  # Boot reconciles on the loop, before the first client call is served, so `metadata_ready?/2` and the
  # per-topic gate are already answerable when it is. That is only affordable because the reads are
  # bounded: unbounded, a node restarting next to one silent vnode sat here for ra's five seconds.
  def handle_continue(:reconcile, state) do
    schedule_reconcile(state)
    {:noreply, reconcile_synchronously(state)}
  end

  @impl true
  def terminate(_reason, state) do
    # Only stop the replication server we started ourselves; an external broker set is not ours.
    if state.replication && Process.alive?(state.replication), do: GenServer.stop(state.replication)
    :ok
  end

  # --- internals ---

  defp drop_unexpected(state, kind, message) do
    %{state | unexpected_shapes: UnexpectedMessage.drop(state.unexpected_shapes, :broker, kind, message)}
  end

  defp schedule_refresh(state), do: Process.send_after(self(), :refresh_brokers, state.refresh_interval)

  defp schedule_reconcile(state), do: Process.send_after(self(), :reconcile, state.refresh_interval)

  # The reconcile loop (level-triggered, controller-style, D-c-1d): while this node is the leader,
  # bootstrap any vnode whose ra cluster is not up yet; then re-seed the local cache from the clusters.
  # Reschedules itself. Idempotent: bootstrapping an already-formed vnode is a no-op, and the cache
  # re-seed only moves forward (the ra log is the source of truth).
  # Recovers each range's end offset and segment sequence floor from the authoritative metadata plus
  # the primaries' logs, so a restarted broker can serve reads of pre-restart data. Active segments
  # ask their primary's replication server for the true end; a range whose primary does not answer is
  # left unrecovered, refusing consumes until a later pass or a produce learns the end
  # (`Malachi.Broker.seed_unrecovered_range/3`). Sealed-only ranges compute the end from the sealed
  # metadata (start_offset + length).
  defp recover_range_state(broker) do
    metadata = DSRSM.merged_metadata(broker.dsrsm)

    metadata.topics
    |> Map.keys()
    |> Enum.flat_map(fn topic -> Enum.map(DSRSM.ranges_of_topic(broker.dsrsm, topic), &{topic, &1.id}) end)
    |> Enum.reduce(broker, &recover_one_range/2)
  end

  defp recover_one_range({topic, range_id}, broker) do
    case DSRSM.segments_of_range(broker.dsrsm, topic, range_id) do
      [] ->
        broker

      segments ->
        min_seq = segments |> Enum.map(fn %{id: {_range, seq}} -> seq end) |> Enum.max() |> Kernel.+(1)

        case recovered_next_offset(segments) do
          next when is_integer(next) -> Broker.seed_range_state(broker, range_id, next, min_seq)
          # The primary did not answer, so the end is unknown. Any horizon guessed here is wrong in one
          # direction: the segment's start would serve the range's earlier segments and hide the active
          # one, zero would hide everything, and either page comes back empty and successful. So the
          # range refuses its reads until a later pass (every reconcile runs this again) learns the end.
          :unreachable -> Broker.seed_unrecovered_range(broker, range_id, min_seq)
        end
    end
  end

  defp recovered_next_offset(segments) do
    case Enum.find(segments, &(&1.state == :active)) do
      %{id: segment_id, start_offset: start_offset, replica_set: [primary | _]} ->
        safe_durable_end(primary, segment_id, start_offset)

      nil ->
        # Sealed-only range: the seal command recorded each segment's length deterministically.
        %{start_offset: start_offset, length: length} = Enum.max_by(segments, & &1.start_offset)
        start_offset + (length || 0)
    end
  end

  # `durable_end` rather than `end_offset`, which is the whole of the cold-segment fix. `end_offset` is
  # deliberately cheap and never opens anything, so it answers :empty for a segment this server holds
  # on disk but has not touched since it booted. Recovery took that for a real end, set the read
  # horizon to the segment's base, and every read then clamped to :eof before it could reach the read
  # path's own cold-segment recovery: no read, so no open segment, so no horizon, so no read. Only a
  # produce broke the loop. `durable_end` recovers the log from disk and answers the true resume point,
  # which is exactly what this caller means.
  #
  # Short timeout: this runs inside the broker loop (init and the periodic reconcile), and an
  # unreachable primary must cost milliseconds, not the default five seconds per range. A legitimate
  # open that overruns it is retried on the next tick, by which point the server has finished opening
  # and answers from memory.
  #
  # A primary whose copy failed answers `{:error, _}`, and it is treated like one that does not answer:
  # it has no trustworthy end to seat a horizon at, and the heal pass is what seals that segment.
  defp safe_durable_end(primary, segment_id, base_offset) do
    case ReplicationServer.durable_end(primary, segment_id, base_offset, 250) do
      {:error, _reason} -> :unreachable
      end_offset -> end_offset
    end
  catch
    :exit, _reason -> :unreachable
  end

  # Fences and seals a range's write head on demand, for a split or a merge retiring it. A range with no
  # head THE CONTROL PLANE KNOWS OF has nothing to fence, which is a success: there is no length to get
  # wrong. That is a stronger statement than the one this used to make, which was about this frontend's
  # own cache and so held on the very node where it did not matter (issue #41).
  defp fence_parent(state, range_id) do
    case Broker.active_roll(state.broker, range_id) do
      :none ->
        {:ok, state}

      roll ->
        # The range travels with the failure. A merge fences TWO parents, and an operator told only
        # that "the fence failed" cannot tell which one to look at.
        case fence_and_seal(state, roll) do
          {:ok, state} -> {:ok, state}
          {:error, reason, state} -> {:error, {range_id, reason}, state}
        end
    end
  end

  # This fence is WAITED ON, and the produce roll's is not, and what separates them is the caller's rhythm
  # rather than the correctness of the fence. A split or a merge retires the range: it happens once, it is
  # already a control-plane round trip, and nothing may write to the parent afterwards, so waiting on a
  # network call to make that true is exactly the trade. A produce roll is the opposite on every count, and
  # a waited-on fence there put a synchronous call to a possibly mute primary inside the loop that
  # serializes every client of this node. So the roll sends its fence as a cast and records the answer
  # when it arrives (`send_roll_fences/1`). What it must not do is skip the fence, which it used to: sealing
  # from its own counter let a frontend that had not yet seen the seal keep appending to the segment, and
  # the primary acknowledged records above the recorded edge that no read could reach.
  defp fence_and_seal(state, roll) do
    case ReplicationServer.seal(roll.primary, roll.segment_id, roll.start_offset, state.fence_timeout) do
      {:ok, end_offset, byte_size} ->
        case Broker.record_seal(state.broker, roll, end_offset, byte_size, System.system_time(:millisecond)) do
          {broker, :ok} ->
            {:ok, %{state | broker: broker}}

          # The store is fenced but the metadata is not: the parent is closed to writes and the split
          # must not proceed on a range whose end the control plane does not know.
          #
          # Loud, because the two halves have come apart and the damage outlives this call. A fence has
          # no inverse, so the segment cannot be reopened, and `Broker.record_seal/5` returns the broker
          # UNCHANGED on this branch, so no roll is owed and nothing here retries. The range accepts no
          # write at all until `Malachi.Cluster.OrphanedFence` reconciles it, and reporting only
          # "the split failed" left an operator with no way to tell that apart from a split that
          # changed nothing.
          {broker, {:error, reason}} ->
            Logger.error(I18n.t(:seal_record_failed, segment_id: inspect(roll.segment_id), reason: inspect(reason)))

            Telemetry.orphaned_fence(roll.segment_id, reason)
            {:error, reason, %{state | broker: broker}}
        end

      {:error, reason} ->
        Logger.warning(I18n.t(:fence_failed, segment_id: inspect(roll.segment_id), reason: inspect(reason)))
        {:error, reason, state}
    end
  end

  # Sends the fence of every owed roll that is due (`Broker.fences_to_send/3`): the first send, or a resend
  # once one has gone unanswered for `@roll_fence_retry_ms`. Never waits: the answer comes back as a
  # `{:seal_result, ...}` message, and until it does the segment stays active and keeps taking writes, each
  # of which the fence's answer then covers.
  defp send_roll_fences(state) do
    {broker, rolls} = Broker.fences_to_send(state.broker, now_ms(), @roll_fence_retry_ms)

    for roll <- rolls do
      ReplicationServer.seal_async(roll.primary, roll.segment_id, roll.start_offset, self(), {:roll_fence, roll})
    end

    %{state | broker: broker}
  end

  defp record_roll_fence(state, roll, {:ok, end_offset, byte_size}) do
    case Broker.record_fence(state.broker, roll, end_offset, byte_size, System.system_time(:millisecond)) do
      {broker, :ok} ->
        %{state | broker: broker}

      # The store is fenced and the metadata is not, as in `fence_and_seal/2`, and just as loud. Unlike a
      # split, the roll stays owed: its fence is resent once the retry window passes, and answers the same
      # numbers, so the seal is recorded again rather than left to a heal pass. The answer did arrive, so the
      # fence is no longer awaited: a produce refused by it fails at once instead of being held for nothing.
      {broker, {:error, reason}} ->
        Logger.error(I18n.t(:seal_record_failed, segment_id: inspect(roll.segment_id), reason: inspect(reason)))
        Telemetry.orphaned_fence(roll.segment_id, reason)
        %{state | broker: Broker.forget_fence(broker, roll)}
    end
  end

  # A failed fence changes nothing: the segment stays open for writes and the roll stays owed, so the fence
  # is resent after the retry window. A primary whose copy failed in storage answers here too, and the
  # failover pass seals that segment, which clears the roll. The fence is no longer awaited, though, so no
  # produce is held behind it.
  defp record_roll_fence(state, roll, {:error, reason}) do
    Logger.warning(I18n.t(:roll_fence_failed, segment_id: inspect(roll.segment_id), reason: inspect(reason)))
    %{state | broker: Broker.forget_fence(state.broker, roll)}
  end

  # A dispatch refused by a sealed segment. One that went out behind this frontend's own fence is this
  # frontend's roll overtaking its own produce, not a writer racing a seal it has not seen, so it is not
  # failed back to the client: it waits for the fence's answer if that is still on its way, and is planned
  # again at once if the answer already landed (the primary sends the answer before the refusal, so that
  # is the common order). Only once: a dispatch that is itself a second attempt fails as before.
  defp sealed_dispatch(state, ref, pending, %{behind_fence: true} = dispatch, end_offset) do
    %{range_id: range_id, segment_id: segment_id} = dispatch

    if Broker.awaiting_fence?(state.broker, range_id, segment_id, now_ms(), @roll_fence_retry_ms) do
      park_on_fence(state, range_id, {:async, ref, dispatch, end_offset})
    else
      replan_dispatch(state, ref, pending, dispatch)
    end
  end

  defp sealed_dispatch(state, ref, pending, _dispatch, end_offset) do
    finish_async_produce(state, ref, pending, {:error, {:sealed, end_offset}})
  end

  # Holds `entry` until the fence of `range_id` answers (`handle_info/2` for `:seal_result`) or the retry
  # window runs out, whichever comes first. The window is the one after which the fence itself is resent, so
  # a produce never waits longer for an answer than the frontend does. Entries keep their arrival order.
  defp park_on_fence(state, range_id, entry) do
    parked =
      case Map.get(state.fence_parked, range_id) do
        nil ->
          token = make_ref()
          timer = Process.send_after(self(), {:fence_park_timeout, range_id, token}, @roll_fence_retry_ms)
          %{token: token, timer: timer, entries: [entry]}

        parked ->
          %{parked | entries: [entry | parked.entries]}
      end

    %{state | fence_parked: Map.put(state.fence_parked, range_id, parked)}
  end

  # `:replan` sends every held produce again, `:refuse` fails the refused dispatches with the refusal they
  # got. A held group-commit call was never refused (it was held before it was tried), so both modes simply
  # try it now, with no second hold.
  defp release_fence_parked(state, range_id, mode) do
    case Map.pop(state.fence_parked, range_id) do
      {nil, _fence_parked} ->
        state

      {parked, fence_parked} ->
        Process.cancel_timer(parked.timer)

        parked.entries
        |> Enum.reverse()
        |> Enum.reduce(%{state | fence_parked: fence_parked}, &release_parked_entry(&2, &1, mode))
    end
  end

  defp release_parked_entry(state, {:async, ref, dispatch, end_offset}, mode) do
    case {Map.get(state.async_produces, ref), mode} do
      # Already finished: another of its dispatches failed, or its safety timer fired.
      {nil, _mode} -> state
      {pending, :replan} -> replan_dispatch(state, ref, pending, dispatch)
      {pending, :refuse} -> finish_async_produce(state, ref, pending, {:error, {:sealed, end_offset}})
    end
  end

  defp release_parked_entry(state, {:grouped, from, topic, records}, _mode) do
    case grouped_append(from, topic, records, state) do
      {:reply, reply, state} ->
        reply_produce(from, reply, state)
        state

      {:noreply, state} ->
        state
    end
  end

  # Plans one refused dispatch's records again, as a produce of their own folded into the one they belong to:
  # the refused dispatch is replaced by whatever the new plan owes (one dispatch into the successor, or more
  # if the range split meanwhile), and its placement by the new ones.
  defp replan_dispatch(state, ref, pending, dispatch) do
    {records, parts} = Map.pop!(pending.parts, dispatch)
    pending = %{pending | parts: parts}

    case Broker.produce_plan(state.broker, pending.topic, records) do
      {broker, {:ok, placements, dispatches}} ->
        state = %{state | broker: broker}
        {_parts, tags} = dispatch_async(state, ref, dispatches, false)
        {state, pending} = owe_dispatches(state, pending, tags)

        pending = %{
          pending
          | placements: pending.placements |> Map.delete(dispatch.range_id) |> Map.merge(placements),
            remaining: pending.remaining - 1 + length(dispatches)
        }

        %{state | async_produces: Map.put(state.async_produces, ref, pending)}

      {broker, {:error, _reason} = error} ->
        finish_async_produce(%{state | broker: broker}, ref, pending, error)
    end
  end

  defp do_open_stream(range_id, pid, state) do
    with_local_range(range_id, state, fn state, segment_id, primary ->
      token = Process.monitor(pid)
      stream = %{pid: pid, range_id: range_id, segment_id: segment_id, primary: primary}
      {:reply, {:ok, token, segment_id, self()}, %{state | streams: Map.put(state.streams, token, stream)}}
    end)
  end

  # Runs `serve` with that segment and its primary when this node leads `range_id`'s active segment
  # (placing the range's first segment when it has none), the condition a producer stream is served under;
  # otherwise answers where the range is served: the node that leads it (`:elsewhere`), or the ranges a
  # split or merge left (`:retired`). Reading has its own condition (`with_consume_range/3`).
  defp with_local_range({topic, _seq} = range_id, state, serve) do
    if topic_metadata_ready?(state, topic) do
      case Broker.stream_segment(state.broker, range_id) do
        {broker, {:ok, segment_id, primary}} ->
          state = %{state | broker: broker}

          if local_ref?(primary),
            do: serve.(state, segment_id, primary),
            else: {:reply, {:moved, :elsewhere, [{range_id, {segment_id, primary}}]}, state}

        {broker, {:error, :range_sealed}} ->
          {:reply, {:moved, :retired, targets(broker, Broker.stream_target(broker, range_id))},
           %{state | broker: broker}}

        {broker, {:error, _reason} = error} ->
          {:reply, error, %{state | broker: broker}}
      end
    else
      {:reply, {:error, :metadata_unavailable}, state}
    end
  end

  # Runs `serve` when `range_id` is read here: its active segment is led here, or it has none right now (a
  # segment just sealed by a roll or a failover, or a range nothing has written to), when its history is
  # readable from any node and no record can land until a segment opens; a consume stream then moves when
  # that segment opens on another node (`sweep_consumers/1`). Otherwise answers where the range is read,
  # as `with_local_range/3` does. Reading never places a segment.
  defp with_consume_range({topic, _seq} = range_id, state, serve) do
    cond do
      not topic_metadata_ready?(state, topic) -> {:reply, {:error, :metadata_unavailable}, state}
      not Broker.range_known?(state.broker, range_id) -> {:reply, {:error, :no_such_range}, state}
      true -> consume_target(state, range_id, serve)
    end
  end

  defp consume_target(state, range_id, serve) do
    case Broker.stream_target(state.broker, range_id) do
      {:retired, _ranges} = target -> {:reply, {:moved, :retired, targets(state.broker, target)}, state}
      {:active, segment_id, primary} -> consume_here(state, range_id, segment_id, primary, serve)
      :none -> serve.(state)
    end
  end

  defp consume_here(state, range_id, segment_id, primary, serve) do
    if local_ref?(primary),
      do: serve.(state),
      else: {:reply, {:moved, :elsewhere, [{range_id, {segment_id, primary}}]}, state}
  end

  # A broker reference that lives on this node: `{name, node}` on this node, a local pid, or a bare name.
  defp local_ref?({_name, ref_node}) when is_atom(ref_node), do: ref_node == node()
  defp local_ref?(pid) when is_pid(pid), do: node(pid) == node()
  defp local_ref?(name) when is_atom(name), do: true
  defp local_ref?(_other), do: false

  defp reply_produce({:stream, from, range_id}, reply, state),
    do: GenServer.reply(from, {reply, stream_load(state, range_id)})

  defp reply_produce(from, reply, _state), do: GenServer.reply(from, reply)

  # How much of a stream's window the range's load leaves (`Malachi.StreamWindow.scale/3`): with group
  # commit, the node's parked records from half of the most it parks; otherwise the range's records in
  # flight between the stream limits.
  defp stream_load(%{group_commit: true} = state, _range_id),
    do: StreamWindow.scale(state.pending_records, div(state.max_inflight_records, 2), state.max_inflight_records)

  defp stream_load(state, range_id),
    do:
      StreamWindow.scale(
        Map.get(state.range_inflight, range_id, 0),
        state.stream_inflight_soft,
        state.stream_inflight_hard
      )

  # Tells every open stream whose range has moved on where it belongs now, and drops it: its segment
  # sealed (a roll, or a seal found on a refusal or by the reconcile), its primary changed (a failover), or
  # its range was retired by a split or merge. Run after each of those.
  defp sweep_streams(%{streams: streams, consume: %ConsumeIndex{consumers: consumers, waiters: waiters}} = state)
       when map_size(streams) == 0 and map_size(consumers) == 0 and map_size(waiters) == 0,
       do: state

  defp sweep_streams(state) do
    state = sweep_consumers(state)

    streams =
      Map.filter(state.streams, fn {token, stream} ->
        case Broker.stream_target(state.broker, stream.range_id) do
          {:active, segment_id, primary} when segment_id == stream.segment_id and primary == stream.primary ->
            true

          target ->
            Process.demonitor(token, [:flush])

            send(
              stream.pid,
              {:stream_moved, token, moved_reason(target), targets(state.broker, target, stream.range_id)}
            )

            false
        end
      end)

    %{state | streams: streams}
  end

  # Tells every consume stream and `fetch_range` waiter whose range is no longer read here where it is read
  # now, and drops it: its range was retired by a split or merge (a position read in a split's parent goes
  # on in each child, see `Malachi.Wire`), or its active segment opened on another node. A range with no
  # active segment keeps its readers (`with_consume_range/3`).
  defp sweep_consumers(state) do
    {consumers, waiters, consume} = ConsumeIndex.take_unless(state.consume, &read_here?(state.broker, &1))

    Enum.each(consumers, fn {token, consumer} ->
      Process.demonitor(token, [:flush])
      target = Broker.stream_target(state.broker, consumer.range_id)
      send(consumer.pid, {:stream_moved, token, moved_reason(target), targets(state.broker, target, consumer.range_id)})
    end)

    Enum.each(waiters, fn {_ref, waiter} ->
      Process.cancel_timer(waiter.timer)
      target = Broker.stream_target(state.broker, waiter.range_id)
      GenServer.reply(waiter.from, {:moved, moved_reason(target), targets(state.broker, target, waiter.range_id)})
    end)

    %{state | consume: consume}
  end

  defp read_here?(broker, range_id) do
    case Broker.stream_target(broker, range_id) do
      {:active, _segment_id, primary} -> local_ref?(primary)
      {:retired, _ranges} -> false
      :none -> true
    end
  end

  # Drops the producer or consume stream `token`.
  defp drop_stream(state, token),
    do: %{state | streams: Map.delete(state.streams, token), consume: ConsumeIndex.drop_consumer(state.consume, token)}

  # A failover seals the segment before it moves the replicas (`Malachi.Cluster.Failover`), so it reaches a
  # stream as `:sealed`, as a roll does: either way the range goes on in another segment.
  defp moved_reason({:retired, _ranges}), do: :retired
  defp moved_reason(_sealed_or_replaced), do: :sealed

  # Where a moved stream goes: each range with its active segment and primary, when it has one.
  defp targets(broker, target, range_id \\ nil)
  defp targets(broker, {:retired, ranges}, _range_id), do: Enum.map(ranges, &{&1, active_of(broker, &1)})
  defp targets(_broker, {:active, segment_id, primary}, range_id), do: [{range_id, {segment_id, primary}}]
  defp targets(_broker, :none, range_id), do: [{range_id, nil}]

  defp active_of(broker, range_id) do
    case Broker.stream_target(broker, range_id) do
      {:active, segment_id, primary} -> {segment_id, primary}
      _none -> nil
    end
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  # The remote half of a reconcile, and the ONLY half that may run off this server's loop. It takes and
  # returns plain data: handing it `state.broker` and applying what it gave back would discard every
  # produce, seal and fence the loop handled while it ran.
  defp run_reconcile(bootstrap, metadata_refresh, read_timeout) do
    bootstrap_missing_vnodes(bootstrap, read_timeout)
    metadata_refresh.()
  end

  # Starts the tick's reconcile off the loop. At most one at a time: a control plane slow enough that a
  # pass outlives the tick would otherwise have its passes pile up, each with its own vnode bootstrap.
  # The skipped tick is counted, because the cost of skipping it is a view that keeps ageing.
  defp start_reconcile_task(%{metadata_refresh: nil} = state), do: state

  defp start_reconcile_task(%{reconcile_task: running} = state) when not is_nil(running) do
    Telemetry.reconcile_degraded(:skipped)
    state
  end

  defp start_reconcile_task(state) do
    generation = state.reconcile_generation
    {bootstrap, refresh, read_timeout} = {state.bootstrap, state.metadata_refresh, state.reconcile_read_timeout}

    task =
      Task.Supervisor.async_nolink(Malachi.TaskSupervisor, fn ->
        {:reconciled, generation, run_reconcile(bootstrap, refresh, read_timeout)}
      end)

    timer = Process.send_after(self(), {:reconcile_deadline, task.ref}, state.reconcile_deadline)
    %{state | reconcile_task: %{ref: task.ref, pid: task.pid, generation: generation, timer: timer}}
  end

  defp finish_reconcile_task(%{reconcile_task: %{ref: ref, timer: timer}} = state) do
    Process.demonitor(ref, [:flush])
    Process.cancel_timer(timer)
    %{state | reconcile_task: nil}
  end

  # Boot and `reconcile_now/2`: the same pass, run on the loop and waited on. In-memory metadata has no
  # control plane to read, so there is nothing to do and nothing to wait for.
  defp reconcile_synchronously(%{metadata_refresh: nil} = state), do: state

  defp reconcile_synchronously(state) do
    result = run_reconcile(state.bootstrap, state.metadata_refresh, state.reconcile_read_timeout)

    # A task already in flight read the control plane BEFORE this pass did, so its result would install
    # that older read over the one this pass is about to install. The journal cannot repair it either:
    # this pass drains the journal, so by the time the task's result lands there is nothing left to put
    # the difference back, and a topic created in between disappears. Bumping the generation is what
    # makes that task arrive stale and be dropped, which is the right answer: this pass read later.
    state
    |> Map.update!(:reconcile_generation, &(&1 + 1))
    |> apply_reconcile(result)
  end

  # The local half: pure with respect to the network, and applied to the CURRENT broker, whichever pass
  # produced the data.
  defp apply_reconcile(state, result) do
    case result do
      nil ->
        # The single ra cluster answered nothing this tick, so the cache keeps the view it holds. The
        # sharded refresh reports its silent vnodes by id and never answers nil.
        state

      {dsrsm, unreachable} ->
        # Refresh the range end offsets along with the metadata: a frontend's read horizon is its
        # local offsets map, which only advances for produces IT handles, so writes flowing through
        # OTHER frontends (or landed while this node was down) would otherwise stay invisible to its
        # reads forever. The seeding is monotone (max), so refreshing never rewinds live state.
        #
        # `unreachable` keeps this tick from erasing the topics of a vnode that did not answer, which
        # would make every read of them succeed with zero records for as long as it stayed silent.
        # `drop_stale_active_segments/1` right after the cache swap, so a segment sealed on ANOTHER node
        # stops being routed at here within one reconcile instead of only when the store refuses a batch.
        # The read this installs was taken BEFORE the commands applied on this loop since the last
        # pass, and a re-seed REPLACES a reachable vnode's metadata. Without the replay, a topic
        # created, a range split or a group position committed while the task was reading is undone
        # here and stays undone until a later pass happens to read it back, during which a produce is
        # refused as :no_such_topic and a consumer is handed a position it already passed.
        {journaled, broker} = Broker.take_journal(state.broker)

        broker =
          broker
          |> Broker.put_cache(dsrsm, unreachable)
          |> Broker.replay_journal(journaled)
          |> Broker.drop_stale_active_segments()

        sweep_streams(%{
          state
          | broker: recover_range_state(broker),
            seen_vnodes: seen_vnodes(state, dsrsm, unreachable)
        })
        |> wake_all_subscribers()
    end
  end

  # A streaming subscriber is pushed to on subscribe, on its ack, and on a produce to its topic, and
  # nowhere else, which leaves it stranded in two ways. One that subscribed while its topic's vnode
  # was still blank was pushed nothing and nothing pushes it again: an ack cannot, having no records
  # to acknowledge, so a produce THROUGH THIS BROKER is the only way out. And a produce through a
  # different frontend is exactly what never arrives here, since it wakes that broker's subscribers
  # and not this one's. A topic written through one node and streamed from another delivered nothing.
  #
  # This is the same gap the offsets refresh above exists to close, described in its own comment: a
  # frontend only learns about writes it did not handle on this tick. Subscribers belong on the same
  # tick for the same reason.
  #
  # Every subscriber with credit and no read out is handed one on every tick, caught up or not (one with a
  # read out is read for again when it ends): the loop pays for a
  # view of its ranges (local metadata lookups, shared by the subscribers reading the same ranges) and a
  # message each way, and the read itself runs in the subscriber's process, where a caught-up one answers
  # `:eof` from the view's end before any read function is called. The subscriber that does have a
  # backlog pays for records it was owed anyway.
  defp wake_all_subscribers(state) do
    state = Enum.reduce(Subscribers.topics(state.subscribers), state, &wake_subscribers(&2, &1, {:ok, []}))
    # a seal of another frontend's writes makes them durable here, and reaches consume streams on this tick
    wake_ranges(state, ConsumeIndex.ranges(state.consume))
  end

  # The vnodes this broker has read at least once since boot. A vnode that has never answered has no
  # view to fall back on, so its topics cannot be served at all, and that is the difference between a
  # cache that is stale and one that is blank. Monotone: once seen, a later outage does not unsee it.
  defp seen_vnodes(state, dsrsm, unreachable) do
    read = DSRSM.vnode_ids(dsrsm) -- unreachable
    MapSet.union(state.seen_vnodes, MapSet.new(read))
  end

  # Bootstrap step: only the leader acts, and only on vnodes whose cluster is not yet ready. Starting a
  # vnode whose cluster already exists returns an error and is ignored (the name fences a double start).
  #
  # `ready?/2` is bounded like every other read the reconcile makes. `MetadataServer.start/2` is NOT:
  # it reaches `:ra.start_cluster`, whose `rpc:call/4` has no timeout at all. That is why the periodic
  # pass runs off the loop behind a deadline, and why the boot pass, which does run on the loop, is the
  # one place this can still cost more than its bound.
  defp bootstrap_missing_vnodes(nil, _read_timeout), do: :ok

  defp bootstrap_missing_vnodes(%{orchestrator?: orchestrator?, vnodes: vnodes, replicated: replicated}, read_timeout) do
    if orchestrator?.() do
      vnodes
      |> not_ready(replicated, read_timeout)
      |> Enum.each(fn {vnode_id, _token, nodes} -> _ = MetadataServer.start(vnode_id, nodes) end)
    end

    :ok
  end

  # The readiness checks are independent and each can cost the whole `read_timeout`, so they run
  # concurrently. In sequence, a pass over n silent vnodes cost n times that before the metadata read
  # even began, and this pass runs ON THIS LOOP at boot and in `reconcile_now/2`.
  #
  # The starts that follow stay sequential on purpose: each reaches `:ra.start_cluster`, whose
  # `rpc:call/4` has no timeout, and firing n of those at once would multiply what a single wedged one
  # already costs rather than bound it.
  defp not_ready(vnodes, replicated, read_timeout) do
    vnodes
    |> BoundedFanout.map(
      read_timeout,
      fn {vnode_id, _token, _nodes} = vnode ->
        if MetadataServer.ready?(ReplicatedDSRSM.server_for(replicated, vnode_id), read_timeout), do: nil, else: vnode
      end,
      # A check that overran its own bound says nothing about the cluster, and reading it as not ready
      # is the harmless half: starting one that is already formed is refused by the name and ignored.
      fn vnode -> vnode end
    )
    |> Enum.reject(&is_nil/1)
  end

  # An empty live set is ignored (keep the last non-empty one); no source leaves the broker as-is, and so
  # does a source that did not answer. The source is the membership server, a sibling the supervisor
  # restarts on its own; a call into it that exits while it is down says nothing about which brokers are
  # alive, and letting that exit take this broker down would cost every client its connection over a
  # refresh that can simply wait for the next tick. A single node runs that server too (it is a
  # one-member cluster), so this is the path every deployment takes.
  defp refresh_broker_set(broker, nil), do: broker

  defp refresh_broker_set(broker, live_brokers) do
    case ask_membership(live_brokers, []) do
      [] -> broker
      live -> Broker.set_brokers(broker, live)
    end
  end

  defp refresh_broker_attributes(broker, nil), do: broker

  defp refresh_broker_attributes(broker, attributes_fun) do
    case ask_membership(attributes_fun, :unanswered) do
      :unanswered -> broker
      attributes -> Broker.set_broker_attributes(broker, attributes)
    end
  end

  defp ask_membership(fun, unanswered) do
    fun.()
  catch
    :exit, _reason -> unanswered
  end

  # Reads a topic's current ranges from `positions`, cross-epoch (see `Broker.read_consume/5`), and
  # returns {records, next_positions, skips}. This is the read orchestration the LogApi used to do client-side;
  # holding it here lets a single call serve a fetch and lets produce re-run it to wake long-pollers.
  # `ranges` nil consumes every active range of the topic (whole-group / single consumer); a range list
  # (a group member's assignment) consumes only those, intersected with the active set, so a stale
  # assigned range that has since split is skipped, and the client never sees ranges either way.
  defp consume_or_park(topic, positions, max_records, wait_ms, ranges, from, state) do
    case consume_ranges(state.broker, topic, positions, max_records, ranges) do
      # A read that failed is not a read that found nothing: waiting would only park the client on a
      # broken range until its long poll expires and then hand it the same empty page.
      {:error, _reason} = error ->
        {:reply, error, state}

      # Caught up and willing to wait: park the request; a later produce to this topic (or the
      # timeout) replies. The page found no records, but it can still have made progress: a source whose
      # data is gone advances the cursor and reports what it skipped. Parking on the ORIGINAL position
      # would rescan that dead source on every wake and hold the skip back until the range gets traffic
      # again, so the waiter parks on what the page reached and carries its skips to whichever of the
      # wake or the timeout answers it.
      {:ok, {[], next_positions, skips}} when wait_ms > 0 ->
        ref = make_ref()
        timer = Process.send_after(self(), {:longpoll_timeout, ref}, wait_ms)

        waiter = %{
          ref: ref,
          timer: timer,
          from: from,
          topic: topic,
          positions: next_positions,
          skips: skips,
          max: max_records,
          ranges: ranges
        }

        {:noreply, %{state | waiters: [waiter | state.waiters]}}

      {:ok, {records, next_positions, skips}} ->
        {:reply, {records, next_positions, skips}, state}
    end
  end

  # What readiness reports: every vnode on the ring has been read at least once, so this node holds a
  # view of the whole keyspace and can answer for any topic routed to it. The same question the read
  # path asks per topic in `topic_metadata_ready?/2`, asked about the node, because a load balancer
  # routes topics it cannot enumerate.
  #
  # Deliberately NOT "is the control plane answering right now". A refresh that fails leaves the last
  # view in place and this node keeps serving from it correctly, so reporting it unready would take a
  # working node out of rotation. Worse, every node sees the same control-plane outage at the same
  # moment, so that answer empties the load balancer precisely when all of its backends still work.
  # The view goes stale during such an outage, which is a real cost, and the honest place to surface it
  # is a signal an operator watches rather than one a router acts on.
  defp all_vnodes_seen?(%{metadata_refresh: nil}), do: true

  defp all_vnodes_seen?(state) do
    state.broker.dsrsm |> DSRSM.vnode_ids() |> Enum.all?(&MapSet.member?(state.seen_vnodes, &1))
  end

  # Whether this broker holds a view of `topic`'s metadata that it is entitled to answer from. Without a
  # metadata authority (the in-memory measurement mode) the local metadata is the only copy there is, so
  # there is nothing to wait for. With one, the topic's vnode must have answered at least once: until
  # then the cache holds an empty placeholder for it, which reads exactly like a topic that exists and is
  # drained.
  defp topic_metadata_ready?(%{metadata_refresh: nil}, _topic), do: true

  defp topic_metadata_ready?(state, topic) do
    case DSRSM.vnode_for(state.broker.dsrsm, topic) do
      {:ok, vnode_id} -> MapSet.member?(state.seen_vnodes, vnode_id)
      # An empty ring routes nowhere; there is no vnode to have heard from, so nothing to gate on.
      {:error, :empty} -> true
    end
  end

  # A consume page for a fetch or a parked waiter, read here, through the same view a push hands out.
  # The skips of every range read ride along, in range order, for whoever delivers the page to attribute.
  defp consume_ranges(broker, topic, positions, max_records, ranges) do
    range_ids = selected_ranges(broker, topic, ranges)
    Broker.consume_ranges(ReadView.new(broker, range_ids), range_ids, positions, max_records, &ReplicationServer.read/4)
  end

  defp selected_ranges(broker, topic, nil), do: Broker.active_range_ids(broker, topic)

  defp selected_ranges(broker, topic, ranges) do
    assigned = MapSet.new(ranges)
    broker |> Broker.active_range_ids(topic) |> Enum.filter(&MapSet.member?(assigned, &1))
  end

  # After a successful produce to `topic`, re-consume each parked waiter on that topic; reply (and
  # drop) the ones that now have data, leaving the rest parked until their timeout.
  # Synchronous produce (group commit off): replicate the batch through `replicate/5`, which fsyncs on a
  # quorum before returning, then reply and wake consumers. The historical default; unchanged behavior.
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
        # The plan's counter is a RESERVATION, so no roll may settle yet: only the primary's answers,
        # folded in by `adopt_result/4` as each dispatch lands, say where the segment really ends.
        state = %{state | broker: broker}
        ref = make_ref()
        {parts, tags} = dispatch_async(state, ref, dispatches, true)

        timer = Process.send_after(self(), {:produce_timeout, ref}, @async_produce_timeout)

        pending = %{
          from: from,
          topic: topic,
          placements: placements,
          remaining: length(dispatches),
          parts: parts,
          timer: timer,
          owed: %{}
        }

        {state, pending} = owe_dispatches(state, pending, tags)
        {:noreply, %{state | async_produces: Map.put(state.async_produces, ref, pending)}}

      {broker, {:error, _reason} = error} ->
        {:reply, error, %{state | broker: broker}}
    end
  end

  # Casts each dispatch to its primary, tagged with what its answer needs. A dispatch into a segment whose
  # roll fence this frontend already sent is marked `behind_fence`: the primary takes the fence first, so it
  # will refuse the dispatch, and `sealed_dispatch/5` plans it again instead of failing it. Only those keep
  # their records (the returned `parts`), so the common produce holds nothing extra. `first_attempt?` false
  # marks none, which is what bounds a produce to one replan.
  defp dispatch_async(state, ref, dispatches, first_attempt?) do
    now = now_ms()

    Enum.reduce(dispatches, {%{}, []}, fn d, {parts, tags} ->
      behind_fence? =
        first_attempt? and Broker.awaiting_fence?(state.broker, d.range_id, d.segment_id, now, @roll_fence_retry_ms)

      tag = %{range_id: d.range_id, segment_id: d.segment_id, last: d.last, count: d.count, behind_fence: behind_fence?}

      ReplicationServer.replicate_async(
        d.primary,
        d.segment_id,
        d.replica_set,
        d.base_offset,
        d.records,
        self(),
        {ref, tag}
      )

      parts = if behind_fence?, do: Map.put(parts, tag, d.records), else: parts
      {parts, [tag | tags]}
    end)
  end

  # The records each range has in flight (dispatched, no answer yet), which a producer stream's window
  # scales by (`Malachi.StreamWindow`). A produce owes the counter its dispatches until each answers or
  # the produce finishes, whichever comes first: a straggler answering after that changes nothing.
  defp owe_dispatches(state, pending, tags) do
    range_inflight =
      Enum.reduce(tags, state.range_inflight, fn tag, acc ->
        Map.update(acc, tag.range_id, tag.count, &(&1 + tag.count))
      end)

    {%{state | range_inflight: range_inflight},
     %{pending | owed: Enum.reduce(tags, pending.owed, &Map.put(&2, &1, true))}}
  end

  defp settle_dispatch(state, pending, tag) do
    if Map.has_key?(pending.owed, tag),
      do: {release_inflight(state, [tag]), %{pending | owed: Map.delete(pending.owed, tag)}},
      else: {state, pending}
  end

  defp release_inflight(state, tags) do
    range_inflight =
      Enum.reduce(tags, state.range_inflight, fn tag, acc ->
        case Map.get(acc, tag.range_id, 0) - tag.count do
          left when left > 0 -> Map.put(acc, tag.range_id, left)
          _none -> Map.delete(acc, tag.range_id)
        end
      end)

    %{state | range_inflight: range_inflight}
  end

  # Folds one dispatch's primary-assigned end offset into the produce: when it matches the plan this
  # is a no-op; when frontends interleaved, the batch's placement becomes the actual contiguous span
  # `[actual - count + 1, actual]` and the local counter jumps forward to the primary's end.
  defp adopt_result(state, pending, dispatch, actual) do
    %{range_id: range_id, segment_id: segment_id, last: expected, count: count} = dispatch

    if actual == expected do
      # Nothing to move, but the primary's answer is still the range's end, which is what an unrecovered
      # range was waiting for: adopting it is a no-op on the counter and ends the wait.
      {%{state | broker: Broker.adopt_offsets(state.broker, range_id, segment_id, actual)}, pending}
    else
      # The placements always follow the primary's truth, even when the counter no longer does: the
      # client must be told where its records actually landed. The counter, in contrast, is only moved
      # when the dispatch still names the range's write head (see `Broker.adopt_offsets/4`).
      placements = Map.put(pending.placements, range_id, {actual - count + 1, actual})
      state = %{state | broker: Broker.adopt_offsets(state.broker, range_id, segment_id, actual)}
      {state, %{pending | placements: placements}}
    end
  end

  defp finish_async_produce(state, ref, pending, reply) do
    Process.cancel_timer(pending.timer)
    state = release_inflight(state, Map.keys(pending.owed))
    reply_produce(pending.from, reply, state)

    %{state | async_produces: Map.delete(state.async_produces, ref)}
    |> wake_waiters(pending.topic, reply)
    |> wake_subscribers(pending.topic, reply)
  end

  # Group-commit produce: `append/5` buffers the batch (no fsync) and reserves offsets exactly as the sync
  # path does, so the routing/offset code is shared; the client reply is parked until the next
  # `:group_flush` makes it durable. A routing/append error (bad topic, unroutable key) is returned now,
  # not parked, since nothing was buffered.
  #
  # A call for a topic with a roll fence still awaited is held until that fence answers (`park_on_fence/3`)
  # rather than tried and refused. Unlike an async dispatch it cannot be replanned after a refusal: the
  # executing produce may already have appended the batch's other ranges, and trying it again would store
  # those twice. Held BEFORE it is tried, nothing has been stored yet.
  defp produce_grouped(from, topic, records, state) do
    case Broker.range_awaiting_fence(state.broker, topic, now_ms(), @roll_fence_retry_ms) do
      nil -> grouped_append(from, topic, records, state)
      range_id -> {:noreply, park_on_fence(state, range_id, {:grouped, from, topic, records})}
    end
  end

  defp grouped_append(from, topic, records, state) do
    if state.pending_records >= state.max_inflight_records do
      # Backpressure: shed load gracefully. Replying now (fast) keeps the caller's produce call from timing
      # out and crashing its connection; the client sees an `:overloaded` error and backs off.
      {:reply, {:error, :overloaded}, state}
    else
      case Broker.produce(state.broker, topic, records, &ReplicationServer.append/5) do
        {broker, {:ok, _placements} = reply} ->
          waiter = %{from: from, reply: reply, topic: topic}
          pending_records = state.pending_records + length(records)

          # A roll this batch tripped is sent its fence here, after the append it follows, so the end the
          # fence answers includes this batch.
          state =
            send_roll_fences(%{
              state
              | broker: broker,
                pending_produce: [waiter | state.pending_produce],
                pending_records: pending_records
            })

          # Flush eagerly once enough is parked so each fsync (and so each reply) stays bounded; otherwise
          # let the interval timer fire.
          if pending_records >= state.flush_max_records do
            {:noreply, do_flush(state)}
          else
            {:noreply, ensure_flush_timer(state)}
          end

        {broker, {:error, _reason} = error} ->
          # a sealed refusal dropped the segment from the cache: a stream bound to it is moved now
          {:reply, error, sweep_streams(%{state | broker: broker})}
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
          # A failed re-read keeps the waiter parked: its own timeout is the right place to give up,
          # and replying an empty page here would be the silent short read the failure is hiding.
          {:error, _reason} ->
            [waiter | keep]

          # Still nothing to deliver, but the re-read can have moved past a source whose data is gone:
          # keep the waiter parked on that progress, with the skips it has gathered so far.
          {:ok, {[], next_positions, skips}} ->
            [%{waiter | positions: next_positions, skips: waiter.skips ++ skips} | keep]

          {:ok, {records, next_positions, skips}} ->
            Process.cancel_timer(waiter.timer)
            GenServer.reply(waiter.from, {records, next_positions, waiter.skips ++ skips})
            keep
        end
      end)

    %{state | waiters: others ++ still_waiting}
  end

  # After a successful produce to `topic`, hand a read to each of its subscribers that has credit, each
  # bounded by its own window. One already reading is read for again when that read ends.
  defp wake_subscribers(state, _topic, {:error, _reason}), do: state

  defp wake_subscribers(state, topic, {:ok, placements}) do
    {reads, subscribers} = Subscribers.wake(state.subscribers, topic)
    # an acknowledged produce's records are durable, which is what consume streams read up to
    placements = Map.new(placements)
    broker = Broker.mark_durable(state.broker, placements)

    %{state | subscribers: subscribers, broker: broker}
    |> dispatch_reads(reads)
    # the ranges the produce wrote to; a reconcile tick wakes every range (`wake_all_subscribers/1`)
    |> wake_ranges(Map.keys(placements))
  end

  # Hands the readers of each of `range_ids` whose range's durable records now end past what they read a
  # view of the range to read from, one view per range: a waiting consume stream (`armed`) is sent
  # `{:consume_wake, ...}` and reads until it asks again, a waiting `fetch_range` is answered.
  defp wake_ranges(state, range_ids) do
    range_ids
    |> Enum.filter(&ConsumeIndex.readers?(state.consume, &1))
    |> Enum.reduce(state, fn range_id, state ->
      durable_end = Broker.durable_end(state.broker, range_id)
      own = Broker.own_source(state.broker, range_id)
      ready? = &(own != nil and Broker.ready_at?(&1.position, own, durable_end))
      {consumers, waiters, consume} = ConsumeIndex.take_ready(state.consume, range_id, durable_end, ready?)

      if consumers != [] or waiters != [] do
        view = Broker.consume_view(state.broker, range_id)

        Enum.each(consumers, fn {token, consumer} ->
          send(consumer.pid, {:consume_wake, token, view, state.skip_reporter})
        end)

        Enum.each(waiters, fn {_ref, waiter} ->
          Process.cancel_timer(waiter.timer)
          GenServer.reply(waiter.from, {:ok, waiter.position, view, state.skip_reporter})
        end)
      end

      %{state | consume: consume}
    end)
  end

  defp arm(state, token, seen_end) do
    case ConsumeIndex.fetch_consumer(state.consume, token) do
      {:ok, consumer} ->
        wake_ranges(%{state | consume: ConsumeIndex.arm(state.consume, token, seen_end)}, [consumer.range_id])

      :error ->
        state
    end
  end

  defp fetch_reply(state, waiter),
    do: {:ok, waiter.position, Broker.consume_view(state.broker, waiter.range_id), state.skip_reporter}

  # Hands each read to its subscriber's own process as `{:log_read, plan}`: the ranges to read, the
  # positions to read them from, the budget, and a `ReadView` of just those ranges, built once per set
  # of ranges for the whole batch. Nothing is read here; see `execute_push/1`.
  defp dispatch_reads(state, []), do: state

  defp dispatch_reads(state, reads) do
    _views =
      Enum.reduce(reads, %{}, fn {sub, budget}, views ->
        range_ids = selected_ranges(state.broker, sub.topic, sub.ranges)
        {view, views} = view_for(views, state.broker, range_ids)
        # `sub` is the subscriber as it was when the read was handed out, before `turn` moved past it.
        range_ids = Subscribers.rotate(range_ids, sub)

        send(
          sub.pid,
          {:log_read,
           %{
             broker: self(),
             ref: sub.ref,
             topic: sub.topic,
             group: sub.group,
             ranges: range_ids,
             positions: sub.positions,
             budget: budget,
             view: view
           }}
        )

        views
      end)

    state
  end

  defp view_for(views, broker, range_ids) do
    case Map.fetch(views, range_ids) do
      {:ok, view} ->
        {view, views}

      :error ->
        view = ReadView.new(broker, range_ids)
        {view, Map.put(views, range_ids, view)}
    end
  end

  # Removes `pid`'s subscription to `topic` (and stops monitoring it).
  defp drop_subscriber(state, topic, pid) do
    {removed, subscribers} = Subscribers.remove_pid(state.subscribers, topic, pid)
    Enum.each(removed, &Process.demonitor(&1.ref, [:flush]))
    %{state | subscribers: subscribers}
  end

  # Control-plane authority, most specific first, returning `{broker_opts, metadata_refresh, bootstrap}`
  # where `metadata_refresh` re-seeds the local cache from the ra clusters (`nil` for in-memory) and
  # `bootstrap` drives the leader's reconcile loop (`nil` unless sharded):
  #   * `:metadata_vnodes`. A sharded control plane: one ra cluster per vnode, each placed on its own
  #     `nodes`, routed by topic. Every node only *routes* at boot; the reconcile loop bootstraps the
  #     clusters on whichever node is currently the leader. `metadata_vnodes` is `[{vnode_id, token,
  #     nodes}]`.
  #   * `:metadata_cluster`: a single ra cluster, the whole metadata in one Raft group (D-a/D1 HA).
  #   * neither, in-memory metadata (single node).
  defp with_metadata_authority(opts, _cluster, _nodes, [_ | _] = vnodes, orchestrator?, read_timeout) do
    replicated = build_replicated(vnodes)
    # Publish the topic→vnode routing so consumer-group coordination is forwarded to the owning node
    # (the same HashRing the metadata is sharded by). Absent in single-node/in-memory → coordination
    # stays local.
    CoordinatorRouter.put_topology(replicated.ring, replicated.vnodes)
    # Bounded like every other read the reconcile makes: this one runs inside `init/1`, where an
    # unbounded read of n silent vnodes used to cost 5s each before the supervisor saw the child start.
    {:ok, cache, _unreachable} = ReplicatedDSRSM.snapshot(replicated, timeout: read_timeout)

    new_opts =
      opts
      |> Keyword.put(:dsrsm, cache)
      |> Keyword.put(:command_fun, sharded_command_fun(replicated))

    refresh = sharded_refresh(replicated, read_timeout)
    bootstrap = %{orchestrator?: orchestrator?, vnodes: vnodes, replicated: replicated}
    {new_opts, refresh, bootstrap}
  end

  defp with_metadata_authority(opts, nil, _nodes, _no_vnodes, _orchestrator?, _read_timeout), do: {opts, nil, nil}

  defp with_metadata_authority(opts, cluster_name, nodes, _no_vnodes, _orchestrator?, read_timeout) do
    {:ok, server_id} = MetadataServer.start(cluster_name, nodes)
    # Left at ra's default on purpose, unlike every read the reconcile makes. This one is matched on
    # `{:ok, _}`, so bounding it would turn a control plane that is merely slow to elect at boot into a
    # broker that refuses to start. The single cluster is also all or nothing: there is no partial view
    # to fall back on here, which is exactly what makes waiting the right answer.
    {:ok, seed} = MetadataServer.query(server_id, &Function.identity/1)

    new_opts =
      opts
      |> Keyword.put(:dsrsm, DSRSM.single(seed))
      |> Keyword.put(:command_fun, raft_command_fun(server_id))

    {new_opts, single_cluster_refresh(server_id, read_timeout), nil}
  end

  # Builds the sharded control plane's ReplicatedDSRSM as routing-only: every vnode points at a real
  # placement member (the first) without starting its cluster. The reconcile loop starts the clusters
  # on the current leader (see `bootstrap_missing_vnodes/2`), so exactly one node bootstraps each vnode
  # and the role fails over with leadership.
  defp build_replicated(vnodes) do
    Enum.reduce(vnodes, ReplicatedDSRSM.new(), fn {vnode_id, token, nodes}, replicated ->
      {:ok, replicated} = ReplicatedDSRSM.route_vnode(replicated, vnode_id, token, {vnode_id, hd(nodes)})
      replicated
    end)
  end

  # A refresh that re-reads the single ra cluster into a one-vnode DSRSM; nil if the cluster is
  # momentarily unreachable (a leader election), so the cache is simply left as-is that tick. The one
  # vnode is either read or not read, so the unreachable list this returns is always empty: a failure
  # is the `nil`, on which `apply_reconcile/2` keeps the view it holds.
  defp single_cluster_refresh(server_id, read_timeout) do
    fn ->
      case MetadataServer.query(server_id, &Function.identity/1, read_timeout) do
        {:ok, metadata} -> {DSRSM.single(metadata), []}
        {:error, _reason} -> nil
      end
    end
  end

  # The sharded equivalent. Unlike the single cluster it can be PARTLY readable, so it never answers
  # nil: it answers the vnodes it read plus the ids of the ones it did not, and the caller keeps its
  # own view of those rather than accepting the empty placeholder.
  defp sharded_refresh(replicated, read_timeout) do
    fn ->
      {:ok, dsrsm, unreachable} = ReplicatedDSRSM.snapshot(replicated, timeout: read_timeout)
      {dsrsm, unreachable}
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

  # The bootstrap pass for an adopted `topology`. Every field of it that comes from the ring is rebuilt
  # here, together, and nowhere else. A pass that kept the boot list while the routing view moved on
  # never bootstrapped a vnode a split added, and for a vnode that had left the ring its readiness check
  # asked the new routing view for a server it no longer has: a KeyError that killed the pass, on this
  # loop at boot and in `reconcile_now/2`, and in the task on every tick (#242). `orchestrator?` is the
  # one field that does not come from the ring, so it is kept.
  #
  # The `nodes` of each vnode are the ring's recorded placement, and the pass starts a cluster over them
  # for ANY vnode whose first recorded member did not answer in time, not only for one that was never
  # formed. For a vnode a split added they are the nodes the split formed it on, but they are not the
  # truth about membership once a rebalance has moved members (#243). And `MetadataServer.start/2`
  # resumes only this node's own member: a remote member that is registered but stopped is started by
  # `:ra.start_cluster` under a fresh uid, the amnesia `Malachi.Cluster.RaResume` describes. That
  # exposure is the same for every vnode on this list, boot and split alike; adopting the ring only
  # stops a split vnode from waiting for this node to restart before it is on the list.
  defp adopt_bootstrap(nil, _topology, _replicated), do: nil

  defp adopt_bootstrap(bootstrap, topology, replicated) do
    %{bootstrap | vnodes: RingTopology.vnode_placement(topology), replicated: replicated}
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
