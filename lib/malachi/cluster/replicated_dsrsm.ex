defmodule Malachi.Cluster.ReplicatedDSRSM do
  @moduledoc """
  The DS-RSM backed by real Raft: a `Malachi.Cluster.HashRing` plus **one `ra` cluster per
  vnode** (each running `Malachi.Cluster.MetadataMachine`). Commands and queries are routed
  by consistent hashing (topic name) to the owning vnode and submitted to that vnode's Raft
  cluster, so the cluster's metadata is sharded across vnodes *and* durably replicated within
  each one. Leadership of a vnode's cluster is that vnode's coordinator.

  This is the production counterpart of the pure `Malachi.Cluster.DSRSM` (which holds the
  per-vnode `Metadata` in memory and is what the property tests exercise). Here each vnode's
  `Metadata` lives in a Raft log instead.

  The value threaded through calls holds only the ring and a `vnode_id => server_id` map
  (both immutable); the metadata itself lives in the ra processes, so `command/3`/`query/3`
  do not change it: only `add_vnode/3` does (and starts the vnode's cluster as a side
  effect). `ra` must already be running (e.g. `:ra.start_in/1`), as with
  `Malachi.Cluster.MetadataServer`.

  Each vnode's cluster can span several nodes (`add_vnode/4`), so a vnode survives losing a member:
  HA per vnode. `split_vnode/4` grows the ring at runtime, migrating the displaced topics' metadata
  between the source and new Raft groups (the dynamically-sharded part); fencing concurrent writes to a
  migrating topic (zero-window cutover) is a later step.
  """

  alias Malachi.Cluster.BoundedFanout
  alias Malachi.Cluster.DSRSM
  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.RingTopology
  alias Malachi.Metadata

  @type vnode_id :: atom()

  @typedoc """
  What `known_segments/3` found: the ids an owner lists, the ids that carry no topic to route by, the ids
  a pending split is moving that neither owner listed, and the ids no owner lists but another vnode does.
  """
  @type found :: %{
          known: MapSet.t(),
          unroutable: [Metadata.segment_id()],
          migrating: [Metadata.segment_id()],
          misplaced: [Metadata.segment_id()]
        }

  @type t :: %__MODULE__{
          ring: HashRing.t(),
          vnodes: %{vnode_id() => MetadataServer.server_id()}
        }

  defstruct ring: nil, vnodes: %{}

  # ra's own default, so `snapshot/2` with no `:timeout` reads exactly as `snapshot/1` used to.
  @default_timeout 5_000

  @doc "Builds an empty replicated DS-RSM. Options are forwarded to `HashRing.new/1`."
  @spec new(keyword()) :: t()
  def new(opts \\ []), do: %__MODULE__{ring: HashRing.new(opts), vnodes: %{}}

  @doc """
  Adds a vnode at `token` and starts its Raft cluster (named `vnode_id`) across `nodes` (default the
  local node). With several nodes the vnode is replicated and survives losing a member, HA per
  vnode. The stored server id addresses a **real member** (see `MetadataServer.start/2`), so a vnode
  placed on a subset of nodes is reachable even from a node that hosts no replica of it. Propagates
  ring placement errors and `ra` start errors.
  """
  @spec add_vnode(t(), vnode_id(), HashRing.token(), [node()]) :: {:ok, t()} | {:error, term()}
  def add_vnode(%__MODULE__{} = state, vnode_id, token, nodes \\ [node()]) do
    with {:ok, ring} <- HashRing.add_vnode(state.ring, vnode_id, token),
         {:ok, server_id} <- MetadataServer.start(vnode_id, nodes) do
      {:ok, %{state | ring: ring, vnodes: Map.put(state.vnodes, vnode_id, server_id)}}
    end
  end

  @doc """
  Places `vnode_id` at `token` on the ring pointing at `server_id`, **without** starting its ra cluster:
  the routing-only counterpart of `add_vnode/4` for a node that is not the bootstrap orchestrator: the
  orchestrator started the cluster (across the placement nodes), and this node only routes to it.
  `server_id` must address a real member of the vnode's placement. Propagates ring placement errors.
  """
  @spec route_vnode(t(), vnode_id(), HashRing.token(), MetadataServer.server_id()) ::
          {:ok, t()} | {:error, term()}
  def route_vnode(%__MODULE__{} = state, vnode_id, token, server_id) do
    with {:ok, ring} <- HashRing.add_vnode(state.ring, vnode_id, token) do
      {:ok, %{state | ring: ring, vnodes: Map.put(state.vnodes, vnode_id, server_id)}}
    end
  end

  @doc """
  Splits the ring by adding a vnode at `token` (a new ra cluster on `nodes`) and **migrating** every topic
  that now routes to it out of its current vnode. Vnode split over real Raft (the NorthGuard model: spawn
  a new group and break off that half of the state). Each displaced topic is **fenced** on the source first
  (`:begin_migration`, so a concurrent write is rejected and cannot race the copy, seal-first), then
  **copy-first**: `insert_topic` into the new vnode, then `extract_topic` from the source (which lifts the
  fence), so no single failure loses a topic (a crash after the insert leaves a harmless duplicate the new
  ring routes past). Returns the grown state on full success; propagates a ring/start error, or
  `{:error, {:fence | :migrate, topic, reason}}` on failure: a partial split leaves its remaining fences up
  (writes to those topics stay blocked) for the caller/coordinator to reconcile. A topic *created* mid-split
  that routes to the new vnode is not caught here (create is not fenced); today's caller quiesces the split.
  """
  @spec split_vnode(t(), vnode_id(), HashRing.token(), [node()]) :: {:ok, t()} | {:error, term()}
  def split_vnode(%__MODULE__{} = state, new_vnode_id, token, nodes \\ [node()]) do
    with {:ok, new_ring} <- HashRing.add_vnode(state.ring, new_vnode_id, token),
         {:ok, new_server_id} <- MetadataServer.start(new_vnode_id, nodes),
         :ok <- migrate_displaced(state, new_ring, new_vnode_id, new_server_id) do
      {:ok, %{state | ring: new_ring, vnodes: Map.put(state.vnodes, new_vnode_id, new_server_id)}}
    end
  end

  @doc """
  Resumes and **completes** a split whose coordinator crashed mid-way: the complete-forward counterpart of
  `abort_split/3` (the NorthGuard "carrying it out to the end"). Re-drives the same migration as
  `split_vnode/4`, but idempotently and **without rolling back** on failure: `ensure_started/2` reuses the
  new vnode's cluster if it is already up (a crash may have started it), and the migration re-drives only
  what is left: a topic already moved off its source is skipped, and re-fencing / re-inserting are no-ops
  (see `Malachi.Metadata.insert_topic/2`). `state` is the pre-split topology (a pending split never advanced
  the ring). On success returns the grown state; on failure returns the error **leaving the partial state in
  place** for the next resume to finish (keep-trying, so a transient outage does not undo progress).
  """
  @spec complete_split(t(), vnode_id(), HashRing.token(), [node()]) :: {:ok, t()} | {:error, term()}
  def complete_split(%__MODULE__{} = state, new_vnode_id, token, nodes \\ [node()]) do
    with {:ok, new_ring} <- HashRing.add_vnode(state.ring, new_vnode_id, token),
         {:ok, new_server_id} <- MetadataServer.ensure_started(new_vnode_id, nodes),
         :ok <- do_migrate(state, new_ring, new_vnode_id, new_server_id) do
      {:ok, %{state | ring: new_ring, vnodes: Map.put(state.vnodes, new_vnode_id, new_server_id)}}
    end
  end

  @doc """
  Aborts a split that a crashed coordinator left in flight, rolling it back to the pre-split state: moves
  every topic that reached the new vnode back to its owner under `state`'s (unchanged) ring and lifts any
  migration fence left on a source: the same derived, best-effort rollback an in-call failure runs.
  `state` is the pre-split topology (a pending split never advanced the ring); `new_server_id` addresses the
  new vnode's (possibly unreachable) cluster.

  Returns `:ok` only when the rollback is **complete**: the new vnode is confirmed **empty** (every topic
  moved back), so its orphan ra cluster is **deleted** (letting a later retry recreate it). Returns
  `{:error, :incomplete}` when the new vnode still holds topics or is unreachable: the cluster is **left
  intact** (deleting it would lose those topics) for the caller to retry: the new vnode's data is safe
  there, just not yet moved back. Idempotent: safe to re-run.
  """
  @spec abort_split(t(), vnode_id(), MetadataServer.server_id()) :: :ok | {:error, :incomplete}
  def abort_split(%__MODULE__{} = state, _new_vnode_id, new_server_id) do
    roll_back(state, new_server_id)

    # delete the orphan new vnode only once it is confirmed empty, deleting one that still holds topics
    # (a move-back that failed, or an unreachable vnode) would lose them. Query with a named stdlib capture
    # (loadable on a possibly-remote leader) and test emptiness locally.
    case MetadataServer.query(new_server_id, &Function.identity/1) do
      {:ok, meta} when map_size(meta.topics) == 0 ->
        # Best-effort cleanup of the now-empty orphan cluster; the rollback itself already succeeded.
        _ = MetadataServer.delete(new_server_id)
        :ok

      _still_populated_or_unreachable ->
        {:error, :incomplete}
    end
  end

  @doc """
  Routes a `Malachi.Metadata` command to the vnode owning `topic_name` and submits it through
  that vnode's Raft log. Returns the machine reply (e.g. `{:ok, root_id}` or
  `{:error, :already_exists}`), `{:error, :no_vnode}` if the ring is empty, or
  `{:error, {:raft, reason}}` on a transport failure.
  """
  @spec command(t(), Metadata.topic_name(), Metadata.command()) :: term()
  def command(%__MODULE__{} = state, topic_name, command) do
    if Metadata.routed_to_foreign_topic?(command, topic_name) do
      # Rejected here, before the Raft submit, so a command that targets another topic's range never
      # enters the log. Same guard as `Malachi.Cluster.DSRSM.command/3`, at the replicated boundary.
      {:error, :range_topic_mismatch}
    else
      with_vnode(state, topic_name, fn server_id ->
        case MetadataServer.command(server_id, command) do
          {:ok, reply} -> reply
          {:error, reason} -> {:error, {:raft, reason}}
        end
      end)
    end
  end

  @doc """
  Routes a linearizable query to the vnode owning `topic_name`. `query_fun` receives that
  vnode's `Metadata` state. `{:error, :no_vnode}` if the ring is empty.
  """
  @spec query(t(), Metadata.topic_name(), (Metadata.t() -> result)) ::
          {:ok, result} | {:error, term()}
        when result: term()
  def query(%__MODULE__{} = state, topic_name, query_fun) do
    with_vnode(state, topic_name, fn server_id -> MetadataServer.query(server_id, query_fun) end)
  end

  @doc "The vnode id owning `topic_name`, or `{:error, :empty}` if there are no vnodes."
  @spec vnode_for(t(), Metadata.topic_name()) :: {:ok, vnode_id()} | {:error, :empty}
  def vnode_for(%__MODULE__{} = state, topic_name), do: HashRing.route(state.ring, topic_name)

  @doc "The ra server id of `vnode_id`: for routing a write to that vnode's cluster."
  @spec server_for(t(), vnode_id()) :: MetadataServer.server_id()
  def server_for(%__MODULE__{} = state, vnode_id), do: Map.fetch!(state.vnodes, vnode_id)

  @doc """
  Reads every vnode's replicated `Metadata` into a local `Malachi.Cluster.DSRSM` cache sharing this
  ring: the read-side mirror a broker threads (reads served locally; writes routed back through the
  vnodes' ra clusters via `server_for/2`).

  A vnode whose cluster is not ready yet (still electing, or the orchestrator has not bootstrapped it)
  contributes an **empty** `Metadata` and its id is returned in the second list, so a caller can tell
  "this vnode holds no topics" from "this vnode did not answer". Collapsing those two is what made a
  restarted broker serve an empty page for a topic with durable records on disk: the unreachable vnode
  contributed nothing, the cache replaced the real topics with that nothing, and every read of them
  succeeded with zero records. Re-snapshotting later fills the vnode in (the ra log is authoritative,
  so a refresh only ever moves the cache forward).

  The vnodes are read **concurrently**, and `:timeout` (in ms, default ra's own 5s) bounds each read. Both
  matter to a caller on a latency-sensitive path: read sequentially with the default, a snapshot of
  `n` silent vnodes costs `5s x n`, which is what used to be paid inside the broker's own loop while
  it was supposed to be serving clients (#178). A read that times out is just an unreachable vnode,
  the case this function already reports, so bounding it adds no outcome a caller must learn.
  """
  @spec snapshot(t(), keyword()) :: {:ok, DSRSM.t(), [vnode_id()]}
  def snapshot(%__MODULE__{} = state, opts \\ []) do
    read = Map.new(read_vnodes(state.vnodes, Keyword.get(opts, :timeout, @default_timeout)))

    metadata_by_vnode = Map.new(read, fn {vnode_id, result} -> {vnode_id, metadata_or_empty(result)} end)
    unreachable = for {vnode_id, :unreachable} <- read, do: vnode_id

    {:ok, DSRSM.seed(state.ring, metadata_by_vnode), unreachable}
  end

  @doc """
  Which of `segment_ids` the vnodes that OWN them list, with every other vnode asked only about the ids
  their owner did not list.

  The question a caller about to act on an ABSENCE has, answered where the answer lives. Each id is
  routed by `Malachi.Metadata.segment_routing_topic/1`, the function every segment command is routed
  by, so the sweep asks exactly the vnode the segment was written to; and each owning vnode is read
  linearizably (`MetadataServer.segments/2`), so a segment registered a moment ago is already there:
  registration commits before any replica creates its directory. Asking the owner rather than a copy
  of everyone's metadata is NorthGuard's shape: the only global state is which vnodes exist, and a
  vnode's leader answers for the metadata it owns (the meetup transcript, 502-508 and 609-613). Only the
  owners can make an id known; the wider question below can only keep an id, and it departs from that
  shape for a reason `docs/ARCHITECTURE.md` records ("a vnode accepts metadata outside its arc").

  Malachi co-locates a topic's ranges and segments on the topic's vnode, while NorthGuard routes a
  range by the hash of the range itself (520-522); that difference predates this function and lives in
  `Malachi.Metadata.segment_routing_topic/1` alone, so the sweep follows it if it ever changes.

  While a split is pending, a topic's metadata can already sit on the new vnode while the ring still
  routes it to the old one, so each id is also routed under the ring the split will install, and asked
  of that owner too. An id counts as known when either owner lists it. One that the split is moving and
  that NEITHER owner lists is returned as `migrating`, not as unknown: the two owners are read at two
  moments, and a topic copied to the new vnode and then extracted from the old one between those two
  reads is listed by neither, although it never stopped existing.

  An id its owner does not list, and that no split is moving, is then asked of every vnode on the ring
  before it can count as unknown. A vnode only stores what was routed to it, but a broker routing
  by a ring gossip had not yet updated can write a new topic to a vnode the recorded ring no longer
  sends it to, and nothing moves it afterwards. Its segments are real, and one that another vnode lists
  is returned as `misplaced`, never as unknown. The wider question costs only the ids that were about
  to be declared absent, which on a healthy cluster are the real orphans alone.

  An id that does not carry a topic cannot be routed and is returned as `unroutable`, never as unknown:
  the caller must treat it as explained. A vnode that does not answer within `timeout` on any node of
  its placement fails the whole call, because a vnode that did not answer knows nothing the caller can
  act on. Returns `{:error, :no_topology}` when there is no ring to route by.
  """
  @spec known_segments(RingTopology.t() | nil, [Metadata.segment_id()], pos_integer()) ::
          {:ok, found()} | {:error, :no_topology | {:vnodes_unreachable, [vnode_id()]}}
  def known_segments(%RingTopology{ring: %HashRing{sorted: [_ | _]}} = topology, segment_ids, timeout) do
    rings = owner_rings(topology)
    placements = owner_placements(topology)
    {routable, unroutable} = Enum.split_with(segment_ids, &is_binary(Metadata.segment_routing_topic(&1)))

    by_owner =
      for id <- routable,
          ring <- rings,
          {:ok, vnode_id} <- [HashRing.route(ring, Metadata.segment_routing_topic(id))],
          reduce: %{} do
        acc -> Map.update(acc, vnode_id, [id], &[id | &1])
      end

    with {:ok, known} <- ask_vnodes(by_owner, placements, timeout) do
      migrating = for id <- routable, moving?(topology, id), not MapSet.member?(known, id), do: id
      absent = for id <- routable, not MapSet.member?(known, id), not moving?(topology, id), do: id

      with {:ok, misplaced} <- ask_everyone(absent, placements, timeout) do
        {:ok, %{known: known, unroutable: unroutable, migrating: migrating, misplaced: MapSet.to_list(misplaced)}}
      end
    end
  end

  def known_segments(_no_ring, _segment_ids, _timeout), do: {:error, :no_topology}

  @doc """
  `known_segments/3` over the topology `read_topology` returns, read again once the owners answered.

  The answers are only worth something if they came from the owners the ring still routes to. A
  topology that moved while the owners were being asked (a split advanced, a pending one appeared) is
  refused as `{:topology_changed, before, after}` rather than trusted, and the caller asks again on its
  next pass. `read_topology` answers `{:ok, topology_or_nil}` or `{:error, reason}`, which is returned.
  """
  @spec known_segments_stable(
          (-> {:ok, RingTopology.t() | nil} | {:error, term()}),
          [Metadata.segment_id()],
          pos_integer()
        ) ::
          {:ok, found()} | {:error, term()}
  def known_segments_stable(read_topology, segment_ids, timeout) when is_function(read_topology, 0) do
    with {:ok, before} <- read_topology.(),
         {:ok, found} <- known_segments(before, segment_ids, timeout),
         {:ok, now} <- read_topology.() do
      if version(before) == version(now),
        do: {:ok, found},
        else: {:error, {:topology_changed, version(before), version(now)}}
    end
  end

  @doc "The ids of the vnodes."
  @spec vnode_ids(t()) :: [vnode_id()]
  def vnode_ids(%__MODULE__{} = state), do: HashRing.vnode_ids(state.ring)

  @doc "Stops and deletes every vnode's Raft cluster (removing on-disk state)."
  @spec delete(t()) :: :ok
  def delete(%__MODULE__{} = state) do
    # Best-effort teardown: delete every vnode through its real member, ignoring individual failures.
    Enum.each(state.vnodes, fn {_vnode_id, server_id} -> MetadataServer.delete(server_id) end)
    :ok
  end

  # --- internals ---

  # The vnode's replicated Metadata, or `:unreachable` when its cluster does not answer (still
  # electing, not bootstrapped yet, or partitioned). A linearizable query runs on the (possibly remote)
  # leader, so use a named stdlib function rather than a module-local closure, which the leader node
  # may not have loaded.
  defp vnode_metadata(server_id, timeout) do
    case MetadataServer.query(server_id, &Function.identity/1, timeout) do
      {:ok, metadata} -> {:ok, metadata}
      {:error, _reason} -> :unreachable
    end
  end

  # One bounded read per vnode, concurrently: the reads are independent, and a vnode that is going to
  # cost the whole timeout must not hold the others behind it. A read that overran is reported as the
  # unreachable vnode it is, which is a case this module already answers for.
  defp read_vnodes(vnodes, timeout) do
    vnodes
    |> Map.to_list()
    |> BoundedFanout.map(
      timeout,
      fn {vnode_id, server_id} -> {vnode_id, vnode_metadata(server_id, timeout)} end,
      fn {vnode_id, _server_id} -> {vnode_id, :unreachable} end
    )
  end

  # The ring routing uses now, plus the one a pending split will install. `HashRing.add_vnode/3` refuses a
  # token already on the ring, which is what a split that already advanced looks like, and then the
  # current ring is the only one.
  defp owner_rings(%RingTopology{ring: ring, pending: %{new_vnode: new_vnode, token: token}}) do
    case HashRing.add_vnode(ring, new_vnode, token) do
      {:ok, advanced} -> [ring, advanced]
      {:error, _already_placed} -> [ring]
    end
  end

  defp owner_rings(%RingTopology{ring: ring}), do: [ring]

  # One bounded read per vnode, concurrently, of which of `ids` each lists. A vnode that does not answer
  # fails the whole question: what it would have said is exactly what the caller cannot do without.
  defp ask_vnodes(by_vnode, _placements, _timeout) when map_size(by_vnode) == 0, do: {:ok, MapSet.new()}

  defp ask_vnodes(by_vnode, placements, timeout) do
    answers =
      by_vnode
      |> Map.to_list()
      |> BoundedFanout.map(
        timeout * max_placement(placements),
        fn {vnode_id, ids} -> {vnode_id, known_on(Map.get(placements, vnode_id, []), vnode_id, ids, timeout)} end,
        fn {vnode_id, _ids} -> {vnode_id, :unreachable} end
      )

    case for({vnode_id, :unreachable} <- answers, do: vnode_id) do
      [] -> {:ok, for({_vnode_id, {:ok, known}} <- answers, id <- known, into: MapSet.new(), do: id)}
      silent -> {:error, {:vnodes_unreachable, Enum.sort(silent)}}
    end
  end

  # The ids no owner listed, asked of every vnode the topology places. Nothing to ask costs nothing.
  defp ask_everyone([], _placements, _timeout), do: {:ok, MapSet.new()}

  defp ask_everyone(ids, placements, timeout),
    do: ask_vnodes(Map.new(Map.keys(placements), &{&1, ids}), placements, timeout)

  # Whether a pending split is moving this id: the ring it will install sends it to the new vnode.
  defp moving?(%RingTopology{pending: %{new_vnode: new_vnode}} = topology, id) do
    case owner_rings(topology) do
      [_current, advanced] -> HashRing.route(advanced, Metadata.segment_routing_topic(id)) == {:ok, new_vnode}
      [_current] -> false
    end
  end

  defp moving?(%RingTopology{}, _id), do: false

  defp owner_placements(%RingTopology{placements: placements, pending: %{new_vnode: new_vnode, nodes: nodes}}),
    do: Map.put_new(placements, new_vnode, nodes)

  defp owner_placements(%RingTopology{placements: placements}), do: placements

  defp version(%RingTopology{version: version}), do: version
  defp version(nil), do: nil

  # Every node of a placement may have to be tried, so the fan-out's bound covers all of them.
  defp max_placement(placements),
    do: placements |> Map.values() |> Enum.map(&length/1) |> Enum.max(fn -> 1 end) |> max(1)

  # A placement is where the vnode was put, not necessarily where its members are now (a rebalance moves
  # them), so each node is tried in turn and the first answer wins. A vnode with no placement at all has
  # nowhere to be asked and is as silent as one that does not answer.
  defp known_on(nodes, vnode_id, ids, timeout) do
    Enum.reduce_while(nodes, :unreachable, fn node, :unreachable ->
      case MetadataServer.segments({vnode_id, node}, timeout) do
        {:ok, segments} -> {:halt, {:ok, for(id <- ids, Map.has_key?(segments, id), into: MapSet.new(), do: id)}}
        {:error, _reason} -> {:cont, :unreachable}
      end
    end)
  end

  # The placeholder keeps the cache shape total (every vnode on the ring has an entry). It is only a
  # placeholder: the caller is told which entries it stands for, and decides whether to keep its own
  # previous view instead.
  defp metadata_or_empty({:ok, metadata}), do: metadata
  defp metadata_or_empty(:unreachable), do: Metadata.new()

  defp with_vnode(state, topic_name, fun) do
    case HashRing.route(state.ring, topic_name) do
      {:error, :empty} -> {:error, :no_vnode}
      {:ok, vnode_id} -> fun.(Map.fetch!(state.vnodes, vnode_id))
    end
  end

  # The migration loop shared by a fresh split (`migrate_displaced`, which rolls back on failure) and a
  # resumed one (`complete_split`, which does not). For each source vnode, migrate its topics that now route
  # to the new vnode under `new_ring`; halt on the first failure. Walks the sources in a deterministic
  # (id-sorted) order so a split, and any partial state a failure leaves - is reproducible rather than
  # dependent on map iteration order.
  defp do_migrate(state, new_ring, new_vnode_id, new_server_id) do
    Enum.reduce_while(Enum.sort(state.vnodes), :ok, fn {_source_id, source_server}, :ok ->
      case migrate_from(source_server, new_server_id, new_ring, new_vnode_id) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  # `do_migrate` for a **fresh** split: **all-or-nothing**: on any failure it best-effort **rolls back**
  # (moves anything that reached the new vnode back to its old-ring owner and lifts any fence left on a
  # source), so a failed split leaves no orphaned topic and no stuck fence.
  defp migrate_displaced(state, new_ring, new_vnode_id, new_server_id) do
    case do_migrate(state, new_ring, new_vnode_id, new_server_id) do
      :ok ->
        :ok

      {:error, _reason} = error ->
        roll_back(state, new_server_id)
        error
    end
  end

  # Find the source's topics that now route to the new vnode, **fence** them (seal-first, so no write can
  # race the copy), then re-snapshot the now-stable source and migrate each from that snapshot. The re-read
  # after fencing captures any write that landed before the fence. The read is linearizable, and the
  # whole state comes back: `MetadataServer.query/3` applies its function here, in the caller.
  defp migrate_from(source_server, new_server, new_ring, new_vnode_id) do
    with {:ok, metadata} <- MetadataServer.query(source_server, &Function.identity/1) do
      displaced =
        for name <- Map.keys(metadata.topics),
            HashRing.route(new_ring, name) == {:ok, new_vnode_id},
            do: name

      with :ok <- fence_topics(source_server, displaced),
           {:ok, snapshot} <- MetadataServer.query(source_server, &Function.identity/1) do
        migrate_topics(source_server, new_server, snapshot, displaced)
      end
    end
  end

  # Fence each displaced topic on the source so concurrent writes to it are rejected during the copy. A
  # failed migration leaves the remaining fences up (writes stay blocked) for the coordinator to reconcile.
  defp fence_topics(source_server, names) do
    Enum.reduce_while(names, :ok, fn name, :ok ->
      case MetadataServer.command(source_server, {:begin_migration, name}) do
        {:ok, :ok} -> {:cont, :ok}
        other -> {:halt, {:error, {:fence, name, other}}}
      end
    end)
  end

  defp migrate_topics(source_server, new_server, snapshot, names) do
    Enum.reduce_while(names, :ok, fn name, :ok ->
      case move_topic(source_server, new_server, Metadata.export_topic(snapshot, name), name) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  # Move a topic between vnodes, copy-first: insert its `export` into `to_server`'s log, then extract it
  # from `from_server`'s: a failure before the extract leaves the source intact (no loss); after it, a
  # harmless duplicate. Used to migrate (source→new) and to roll a failed split back (new→source).
  #
  # Reaching consensus is not the same as succeeding: a machine can commit a command and still refuse it
  # (the destination's group may not understand the export yet, see `Malachi.Cluster.MachineVersion`).
  # Only an insert the destination answered `:ok` may be followed by the extract, or a refused insert
  # would delete the topic from both vnodes. A refused extract leaves the source's fence up, so it is a
  # failure too rather than a split that reports success with writes still blocked.
  defp move_topic(from_server, to_server, export, name) do
    with {:ok, :ok} <- MetadataServer.command(to_server, {:insert_topic, export}),
         {:ok, extracted} when is_map(extracted) or is_nil(extracted) <-
           MetadataServer.command(from_server, {:extract_topic, name}) do
      :ok
    else
      {:ok, {:error, reason}} -> {:error, {:migrate, name, {:refused, reason}}}
      error -> {:error, {:migrate, name, error}}
    end
  end

  # Best-effort rollback of a failed split, **derived from the current state** (no per-step tracking): move
  # every topic that reached the new vnode back to the source it owns under the (unchanged) ring, then lift
  # any migration fence left on a source. Idempotent; a failed rollback step is swallowed (left for
  # manual/coordinator recovery): the point is that a mid-split failure never orphans a topic or sticks a
  # fence in the common case.
  defp roll_back(state, new_server_id) do
    case MetadataServer.query(new_server_id, &Function.identity/1) do
      {:ok, new_meta} ->
        Enum.each(Map.keys(new_meta.topics), fn name ->
          case HashRing.route(state.ring, name) do
            {:ok, source_vnode} ->
              _ =
                move_topic(
                  new_server_id,
                  Map.fetch!(state.vnodes, source_vnode),
                  Metadata.export_topic(new_meta, name),
                  name
                )

            _unrouted ->
              :ok
          end
        end)

      _unreachable ->
        :ok
    end

    for {_vnode_id, source_server} <- state.vnodes do
      case MetadataServer.query(source_server, &Function.identity/1) do
        {:ok, meta} -> Enum.each(Map.keys(meta.migrating), &MetadataServer.command(source_server, {:end_migration, &1}))
        _unreachable -> :ok
      end
    end

    :ok
  end
end
