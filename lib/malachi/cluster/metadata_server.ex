defmodule Malachi.Cluster.MetadataServer do
  @moduledoc """
  A thin wrapper around `ra` for running the `Malachi.Cluster.MetadataMachine` of a single
  DS-RSM vnode: start the Raft cluster, submit metadata commands through the log, and run
  consistent (linearizable) queries over the replicated state.

  `ra` itself must already be running (e.g. `:ra.start_in/1` with a data directory, done by
  the application or test setup). This module does not own ra's lifecycle, only the vnode's
  cluster.
  """

  alias Malachi.Cluster.MetadataMachine
  alias Malachi.Metadata

  @system :default

  @type cluster_name :: atom()
  @type server_id :: {cluster_name(), node()}

  @doc """
  Starts a Raft cluster named `cluster_name` running the metadata machine across `nodes` (default
  the local node), and returns a `server_id` for a **real member**: the local node when it is one,
  otherwise the first of `nodes`. (The starter need not be a member: a sharded control plane places a
  vnode on a subset of nodes, so the node bootstrapping it may not host a replica; `ra` still routes
  commands/queries from that member to the leader.) With several nodes the metadata is replicated and
  survives the loss of a member. `ra` must be running on every node (`:ra.start_in/1`).
  """
  @spec start(cluster_name(), [node()]) :: {:ok, server_id()} | {:error, term()}
  def start(cluster_name, nodes \\ [node()]) do
    server_ids = Enum.map(nodes, &{cluster_name, &1})
    machine = {:module, MetadataMachine, %{}}

    case :ra.start_cluster(@system, cluster_name, machine, server_ids) do
      {:ok, _started, _not_started} ->
        {:ok, {cluster_name, member_node(nodes)}}

      {:error, _reason} ->
        # The cluster already exists: either its members are running (a broker restart within a live
        # node, e.g. supervision), or this node restarted and its member has PERSISTED ra state that
        # must be resumed, not re-formed (start_cluster refuses both shapes). Bring the local member
        # back and reuse the cluster; without this, a node restarted after a power loss crash-looped
        # at boot forever, which is exactly what the chaos harness caught.
        local = {cluster_name, node()}

        case :ra.restart_server(@system, local) do
          :ok -> {:ok, {cluster_name, member_node(nodes)}}
          {:error, {:already_started, _pid}} -> {:ok, {cluster_name, member_node(nodes)}}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @doc """
  Like `start/2`, but **idempotent**: if the cluster is already running (reachable via `:ra.members`),
  returns its `server_id` without restarting; otherwise starts it. This is what *resuming* a split needs:
  a coordinator that crashed after starting the new vnode's cluster must be able to re-drive the split
  without `start/2` failing on an already-formed cluster. Returns `{:error, reason}` only when the cluster
  is neither running nor startable (e.g. its placement nodes are unreachable), so the caller can retry.
  """
  @spec ensure_started(cluster_name(), [node()]) :: {:ok, server_id()} | {:error, term()}
  def ensure_started(cluster_name, nodes \\ [node()]) do
    server_id = {cluster_name, member_node(nodes)}

    case :ra.members(server_id) do
      {:ok, _members, _leader} -> {:ok, server_id}
      _not_running -> start(cluster_name, nodes)
    end
  end

  # A node that actually hosts a replica, to address the cluster through: the local node when it is a
  # member (no network hop for reads), otherwise the first placement node.
  defp member_node(nodes) do
    if node() in nodes, do: node(), else: hd(nodes)
  end

  @doc "Submits a `Malachi.Metadata` command through the Raft log; returns the machine reply."
  @spec command(server_id(), Metadata.command()) :: {:ok, term()} | {:error, term()}
  def command(server_id, command) do
    case :ra.process_command(server_id, command) do
      {:ok, reply, _leader} -> {:ok, reply}
      {:error, reason} -> {:error, reason}
      {:timeout, _server} -> {:error, :timeout}
    end
  end

  @doc """
  Runs `query_fun` over the replicated `Metadata` state with a linearizable (consistent)
  read. `query_fun` receives the `Metadata` state (e.g. `&Malachi.Metadata.get_topic(&1, name)`).
  """
  @spec query(server_id(), (Metadata.t() -> result)) :: {:ok, result} | {:error, term()}
        when result: term()
  def query(server_id, query_fun) do
    case :ra.consistent_query(server_id, query_fun) do
      {:ok, result, _leader} -> {:ok, result}
      {:error, reason} -> {:error, reason}
      {:timeout, _server} -> {:error, :timeout}
    end
  end

  @doc """
  Whether the cluster addressed by `server_id` is formed and reachable (a member answers `:ra.members`).
  Used by the reconcile loop to decide if a vnode still needs bootstrapping.
  """
  @spec ready?(server_id()) :: boolean()
  def ready?(server_id) do
    match?({:ok, _members, _leader}, :ra.members(server_id))
  end

  @doc """
  Whether `server_id` is currently the **leader** of its Raft cluster. `:ra.members` (answered by any
  reachable member) reports the leader as a `server_id`; this returns true iff it equals `server_id`
  itself, so pass the **local** server id (`{cluster_name, node()}`) to ask "does this node lead this
  vnode?". Unreachable/unformed clusters answer false (never assume leadership). Used by 1C-b to run a
  vnode's coordinators only on the node that leads its Raft group (the NorthGuard-faithful placement).
  """
  @spec leader?(server_id()) :: boolean()
  def leader?(server_id) do
    match?({:ok, _members, ^server_id}, :ra.members(server_id))
  end

  @doc """
  Stops and deletes the vnode's Raft cluster (removing its on-disk state). Prefer passing a `server_id`
  addressing a **real member** (as `start/2` returns and the cluster callers already hold), so a vnode
  placed on a subset of nodes is deleted through a node that actually hosts it: `:ra` finds the leader from
  there and propagates the deletion to every member. A bare `cluster_name` is accepted as
  `{cluster_name, node()}` for the single-node case (tests, local setups). Returns `{:error, reason}` when
  the deletion cannot be committed, instead of reporting `:ok` regardless.
  """
  @spec delete(server_id() | cluster_name()) :: :ok | {:error, term()}
  def delete({_cluster_name, _node} = server_id) do
    case :ra.delete_cluster([server_id]) do
      {:ok, _leader} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  def delete(cluster_name) when is_atom(cluster_name) do
    delete({cluster_name, node()})
  end
end
