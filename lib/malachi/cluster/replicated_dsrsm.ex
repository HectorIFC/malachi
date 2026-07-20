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

  alias Malachi.Cluster.DSRSM
  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Metadata

  @type vnode_id :: atom()

  @type t :: %__MODULE__{
          ring: HashRing.t(),
          vnodes: %{vnode_id() => MetadataServer.server_id()}
        }

  defstruct ring: nil, vnodes: %{}

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
  migration fence left on a source: the same derived, best-effort rollback an in-call failure runs (B1).
  `state` is the pre-split topology (a pending split never advanced the ring); `new_server_id` addresses the
  new vnode's (possibly unreachable) cluster.

  Returns `:ok` only when the rollback is **complete**: the new vnode is confirmed **empty** (every topic
  moved back), so its orphan ra cluster is **deleted** (letting a later retry recreate it). Returns
  `{:error, :incomplete}` when the new vnode still holds topics or is unreachable: the cluster is **left
  intact** (deleting it would lose those topics) for the caller to retry: the new vnode's data is safe
  there, just not yet moved back. Idempotent: safe to re-run.
  """
  @spec abort_split(t(), vnode_id(), MetadataServer.server_id()) :: :ok | {:error, :incomplete}
  def abort_split(%__MODULE__{} = state, new_vnode_id, new_server_id) do
    roll_back(state, new_server_id)

    # delete the orphan new vnode only once it is confirmed empty, deleting one that still holds topics
    # (a move-back that failed, or an unreachable vnode) would lose them. Query with a named stdlib capture
    # (loadable on a possibly-remote leader) and test emptiness locally.
    case MetadataServer.query(new_server_id, &Function.identity/1) do
      {:ok, meta} when map_size(meta.topics) == 0 ->
        MetadataServer.delete(new_vnode_id)
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
    with_vnode(state, topic_name, fn server_id ->
      case MetadataServer.command(server_id, command) do
        {:ok, reply} -> reply
        {:error, reason} -> {:error, {:raft, reason}}
      end
    end)
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
  contributes an **empty** `Metadata`, so this never fails on a not-ready vnode; re-snapshotting later
  fills it in (the ra log is authoritative, so a refresh only ever moves the cache forward).
  """
  @spec snapshot(t()) :: {:ok, DSRSM.t()}
  def snapshot(%__MODULE__{} = state) do
    metadata_by_vnode = Map.new(state.vnodes, fn {vnode_id, server_id} -> {vnode_id, vnode_metadata(server_id)} end)
    {:ok, DSRSM.seed(state.ring, metadata_by_vnode)}
  end

  @doc "The ids of the vnodes."
  @spec vnode_ids(t()) :: [vnode_id()]
  def vnode_ids(%__MODULE__{} = state), do: HashRing.vnode_ids(state.ring)

  @doc "Stops and deletes every vnode's Raft cluster (removing on-disk state)."
  @spec delete(t()) :: :ok
  def delete(%__MODULE__{} = state) do
    Enum.each(state.vnodes, fn {vnode_id, _server_id} -> MetadataServer.delete(vnode_id) end)
    :ok
  end

  # --- internals ---

  # The vnode's replicated Metadata, or an empty one when its cluster is unreachable / not ready yet.
  # A linearizable query runs on the (possibly remote) leader, so use a named stdlib function rather
  # than a module-local closure, which the leader node may not have loaded.
  defp vnode_metadata(server_id) do
    case MetadataServer.query(server_id, &Function.identity/1) do
      {:ok, metadata} -> metadata
      {:error, _reason} -> Metadata.new()
    end
  end

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
  # after fencing captures any write that landed before the fence. `&Function.identity/1` (not a closure)
  # so the query runs on the leader.
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
  defp move_topic(from_server, to_server, export, name) do
    with {:ok, _ok} <- MetadataServer.command(to_server, {:insert_topic, export}),
         {:ok, _export} <- MetadataServer.command(from_server, {:extract_topic, name}) do
      :ok
    else
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
