defmodule Malachi.Cluster.RaCluster do
  @moduledoc """
  The shared `ra` lifecycle every one of this repo's Raft-backed stores needs: form or resume the
  cluster, keep this node joined, submit commands through the log, query the replicated state, and
  tear the cluster down.

  `Malachi.Cluster.MetadataServer`, `Malachi.Cluster.LeaseServer`, `Malachi.Auth.UserServer`,
  `Malachi.Auth.LockoutServer` and `Malachi.Auth.AclServer` each grew their own copy of this
  skeleton, differing only in the machine module and the cluster name. That repetition is how
  `LeaseServer` ended up as the one store that forms without resuming first, which is precisely the
  amnesia path `Malachi.Cluster.RaResume` exists to close. Holding the lifecycle in one place means a
  fix like that lands once.

  Every function takes the machine module and cluster name explicitly, so a store is a thin named
  facade over this module rather than a subclass of it. `ra` itself must already be running
  (`:ra.start_in/1`); this module owns a cluster, never ra's lifecycle.
  """

  alias Malachi.Cluster.RaResume

  @system :default

  @type cluster_name :: atom()
  @type server_id :: {cluster_name(), node()}

  @doc """
  Starts `cluster_name` running `machine` across `nodes`, returning a `server_id` for a **real
  member**: the local node when it is one (no network hop for reads), otherwise the first of `nodes`.

  Resume-first (see `Malachi.Cluster.RaResume`): forming over a member this node has ever started
  would register a fresh empty uid and resurrect an amnesiac member, orphaning the persisted log.
  """
  @spec start(module(), cluster_name(), [node()]) :: {:ok, server_id()} | {:error, term()}
  def start(machine, cluster_name, nodes \\ [node()]) do
    case RaResume.resume_or(@system, {cluster_name, node()}, fn -> form(machine, cluster_name, nodes) end) do
      :ok -> {:ok, {cluster_name, member_node(nodes)}}
      other -> other
    end
  end

  defp form(machine, cluster_name, nodes) do
    server_ids = Enum.map(nodes, &{cluster_name, &1})

    case :ra.start_cluster(@system, cluster_name, {:module, machine, %{}}, server_ids) do
      {:ok, _started, _not_started} -> {:ok, {cluster_name, member_node(nodes)}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Ensures this node participates in `cluster_name` (self-join), so a **staggered boot** converges to a
  fully-replicated cluster. Idempotent and best-effort: it forms the cluster if it is not yet formed
  (auto-fenced, every node may call) and starts the local server if it is not running. A no-op once
  the local server is up, so a periodic caller stops paying after the node has joined.
  """
  @spec reconcile(module(), cluster_name(), [node()]) :: :ok
  def reconcile(machine, cluster_name, nodes) do
    # Skip when the local server already runs (the common case): re-issuing start_cluster on a formed
    # cluster makes ra log a harmless but noisy "failed to form" and churns the shared ra system.
    case :ra.members({cluster_name, node()}) do
      {:ok, _members, _leader} ->
        :ok

      _not_running ->
        _ = start(machine, cluster_name, nodes)
        ensure_local_server(machine, cluster_name, nodes)
    end
  end

  # Best-effort self-join. Resume-first here too: :ra.start_server registers a fresh empty uid just
  # like start_cluster, so a self-join over a member this node once hosted must restart it, never
  # re-create it. Any error (already started, cluster not yet formed) is ignored; reconcile retries.
  defp ensure_local_server(machine, cluster_name, nodes) do
    server_ids = Enum.map(nodes, &{cluster_name, &1})

    _ =
      RaResume.resume_or(@system, {cluster_name, node()}, fn ->
        :ra.start_server(@system, cluster_name, {cluster_name, node()}, {:module, machine, %{}}, server_ids)
      end)

    :ok
  end

  @doc "A node that actually hosts a replica: the local node when it is a member, else the first."
  @spec member_node([node()]) :: node()
  def member_node(nodes) do
    if node() in nodes, do: node(), else: hd(nodes)
  end

  @doc """
  Submits `command` through the Raft log and returns `{:ok, machine_reply}`, or `{:error, reason}`
  when the cluster is unreachable. The caller distinguishes a refusal (a machine reply that is itself
  an error) from a failure to reach consensus.
  """
  @spec command(server_id(), term()) :: {:ok, term()} | {:error, term()}
  def command(server_id, command) do
    case :ra.process_command(server_id, command) do
      {:ok, reply, _leader} -> {:ok, reply}
      {:error, reason} -> {:error, reason}
      {:timeout, _server} -> {:error, :timeout}
    end
  end

  @doc """
  Reads the replicated machine state with a **linearizable** (consistent) query.

  The `{Function, :identity, []}` is ra's required shape: it only accepts an `{M, F, A}` and applies
  it as `apply(M, F, A ++ [State])`, so asking for the state itself and projecting in the calling
  process is both the simplest translation and the one that keeps a raising projection from taking
  the replicated server down with it.
  """
  @spec query(server_id()) :: {:ok, term()} | {:error, term()}
  def query(server_id) do
    case :ra.consistent_query(server_id, {Function, :identity, []}) do
      {:ok, state, _leader} -> {:ok, state}
      {:error, reason} -> {:error, reason}
      {:timeout, _server} -> {:error, :timeout}
    end
  end

  @doc """
  Reads the **local** replica's state through `query_fun` (no consensus round-trip). Eventually
  consistent: a just-written value propagates within replication lag. For hot paths that tolerate
  that; use `query/1` when the read must be linearizable.
  """
  @spec local_query(server_id(), (term() -> result)) :: {:ok, result} | {:error, term()} when result: term()
  def local_query(server_id, query_fun) do
    case :ra.local_query(server_id, query_fun) do
      {:ok, {_idx_term, result}, _leader} -> {:ok, result}
      {:error, reason} -> {:error, reason}
      {:timeout, _server} -> {:error, :timeout}
    end
  end

  @doc "Whether the cluster is formed and reachable (a member answers `:ra.members`)."
  @spec ready?(server_id()) :: boolean()
  def ready?(server_id), do: match?({:ok, _members, _leader}, :ra.members(server_id))

  @doc """
  Whether `server_id` is currently its cluster's **leader**. Pass the local server id
  (`{cluster_name, node()}`) to ask "does this node lead?". An unreachable or unformed cluster
  answers false: never assume leadership.
  """
  @spec leader?(server_id()) :: boolean()
  def leader?(server_id), do: match?({:ok, _members, ^server_id}, :ra.members(server_id))

  @doc """
  Stops and deletes the cluster, removing its on-disk state. Prefer a `server_id` addressing a real
  member, so a cluster placed on a subset of nodes is deleted through a node that hosts it: `ra`
  finds the leader from there and propagates the deletion. A bare `cluster_name` means the local node.
  """
  @spec delete(server_id() | cluster_name()) :: :ok | {:error, term()}
  def delete({_cluster_name, _node} = server_id) do
    case :ra.delete_cluster([server_id]) do
      {:ok, _leader} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  def delete(cluster_name) when is_atom(cluster_name), do: delete({cluster_name, node()})
end
