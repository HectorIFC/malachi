defmodule Malachi.Auth.AclServer do
  @moduledoc """
  A thin wrapper around `ra` for the cluster's per-topic ACL store (`Malachi.Auth.AclMachine`): start the
  dedicated Raft cluster, submit ACL commands through the log, and read the replicated grants. Mirrors
  `Malachi.Auth.UserServer`; `ra` must already be running. This module owns only the ACL cluster, not ra's
  lifecycle.

  **Writes** (`grant`/`revoke`/`revoke_user`) go through the log, replicated by consensus, so every node
  enforces the same ACLs. **Reads** (`authorized?`/`list_grants`/`list_all`) use `:ra.local_query` against
  the **local** replica: fast (no consensus round-trip) and adequate for the produce/consume hot path, which
  authorizes every request. They are eventually consistent: a just-granted ACL propagates within
  replication lag, acceptable for authorization.
  """

  alias Malachi.Auth.AclMachine
  alias Malachi.Auth.AclRegistry
  alias Malachi.Cluster.RaResume

  @system :default

  @type cluster_name :: atom()
  @type server_id :: {cluster_name(), node()}

  @doc """
  Starts the ACL store's Raft cluster named `cluster_name` across `nodes` (default the local node) and
  returns a `server_id` addressing a real member (the local node when it is one, else the first).
  """
  @spec start(cluster_name(), [node()]) :: {:ok, server_id()} | {:error, term()}
  def start(cluster_name, nodes \\ [node()]) do
    # Resume-first (see Malachi.Cluster.RaResume): forming over a member this node has ever started
    # would register a fresh empty uid and resurrect an amnesiac member, losing the replicated
    # auth state the same way the storage-chaos harness caught the metadata control plane wiped.
    case RaResume.resume_or(@system, {cluster_name, node()}, fn -> form(cluster_name, nodes) end) do
      :ok -> {:ok, {cluster_name, member_node(nodes)}}
      other -> other
    end
  end

  defp form(cluster_name, nodes) do
    server_ids = Enum.map(nodes, &{cluster_name, &1})
    machine = {:module, AclMachine, %{}}

    case :ra.start_cluster(@system, cluster_name, machine, server_ids) do
      {:ok, _started, _not_started} -> {:ok, {cluster_name, member_node(nodes)}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp member_node(nodes) do
    if node() in nodes, do: node(), else: hd(nodes)
  end

  @doc """
  Ensures this node participates in the ACL cluster (self-join), so a **staggered boot** converges to a
  fully-replicated ACL store. Idempotent and best-effort. Mirrors `UserServer.reconcile/2`.
  """
  @spec reconcile(cluster_name(), [node()]) :: :ok
  def reconcile(cluster_name, nodes) do
    # Skip if the local server is already running (the common case), avoids re-issuing start_cluster on a
    # formed cluster, which ra logs as an error. Only a node that has not yet joined tries to form/join.
    case :ra.members({cluster_name, node()}) do
      {:ok, _members, _leader} ->
        :ok

      _not_running ->
        _ = start(cluster_name, nodes)
        ensure_local_server(cluster_name, nodes)
    end
  end

  # Best-effort: starts the local ACL server so it (re)joins the cluster. Any error (already started, or the
  # cluster not yet formed) is ignored: reconcile is idempotent and the caller retries.
  defp ensure_local_server(cluster_name, nodes) do
    # Resume-first here too: :ra.start_server registers a fresh empty uid just like start_cluster,
    # so a self-join over a member this node once hosted must restart it, never re-create it.
    _ =
      RaResume.resume_or(@system, {cluster_name, node()}, fn ->
        server_ids = Enum.map(nodes, &{cluster_name, &1})
        machine = {:module, AclMachine, %{}}
        :ra.start_server(@system, cluster_name, {cluster_name, node()}, machine, server_ids)
      end)

    :ok
  end

  @doc "Grants `username` an `operation` on `resource` (`{:literal, topic}` / `{:prefix, prefix}`). Reply `:ok`."
  @spec grant(server_id(), String.t(), AclRegistry.operation(), AclRegistry.resource()) ::
          {:ok, :ok} | {:error, term()}
  def grant(server_id, username, operation, resource) do
    command(server_id, {:grant, username, operation, resource})
  end

  @doc "Revokes a single grant (idempotent). Reply `:ok`."
  @spec revoke(server_id(), String.t(), AclRegistry.operation(), AclRegistry.resource()) ::
          {:ok, :ok} | {:error, term()}
  def revoke(server_id, username, operation, resource) do
    command(server_id, {:revoke, username, operation, resource})
  end

  @doc "Revokes every grant for `username` (e.g. when the user is deleted). Reply `:ok`."
  @spec revoke_user(server_id(), String.t()) :: {:ok, :ok} | {:error, term()}
  def revoke_user(server_id, username), do: command(server_id, {:revoke_user, username})

  @doc "Whether `username` has a grant for `operation` on `topic`, read from the local replica."
  @spec authorized?(server_id(), String.t(), AclRegistry.operation(), String.t()) ::
          {:ok, boolean()} | {:error, term()}
  def authorized?(server_id, username, operation, topic) do
    local_query(server_id, &AclRegistry.authorized?(&1, username, operation, topic))
  end

  @doc "The grants for `username` as `{operation, resource}`, from the local replica."
  @spec list_grants(server_id(), String.t()) ::
          {:ok, [{AclRegistry.operation(), AclRegistry.resource()}]} | {:error, term()}
  def list_grants(server_id, username), do: local_query(server_id, &AclRegistry.list_grants(&1, username))

  @doc "Every grant across all users, from the local replica."
  @spec list_all(server_id()) :: {:ok, [AclRegistry.grant()]} | {:error, term()}
  def list_all(server_id), do: local_query(server_id, &AclRegistry.list_all/1)

  @doc "Stops and deletes the ACL store's Raft cluster (removing its on-disk state)."
  @spec delete(cluster_name()) :: :ok
  def delete(cluster_name) do
    :ra.delete_cluster([{cluster_name, node()}])
    :ok
  end

  defp command(server_id, command) do
    case :ra.process_command(server_id, command) do
      {:ok, reply, _leader} -> {:ok, reply}
      {:error, reason} -> {:error, reason}
      {:timeout, _server} -> {:error, :timeout}
    end
  end

  # Reads the local replica's state (no consensus round-trip). Eventually consistent; fine for authorization.
  defp local_query(server_id, query_fun) do
    case :ra.local_query(server_id, query_fun) do
      {:ok, {_idx_term, result}, _leader} -> {:ok, result}
      {:error, reason} -> {:error, reason}
      {:timeout, _server} -> {:error, :timeout}
    end
  end
end
