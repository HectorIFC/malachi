defmodule Malachi.Auth.AclServer do
  @moduledoc """
  A thin named facade over `Malachi.Cluster.RaCluster` for the cluster's per-topic ACL store (`Malachi.Auth.AclMachine`): start the
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
  alias Malachi.Cluster.RaCluster

  @type cluster_name :: atom()
  @type server_id :: {cluster_name(), node()}

  @doc """
  Starts the ACL store's Raft cluster named `cluster_name` across `nodes` (default the local node) and
  returns a `server_id` addressing a real member (the local node when it is one, else the first).
  """
  @spec start(cluster_name(), [node()]) :: {:ok, server_id()} | {:error, term()}
  def start(cluster_name, nodes \\ [node()]) do
    RaCluster.start(AclMachine, cluster_name, nodes)
  end

  @doc """
  Ensures this node participates in the ACL cluster (self-join), so a **staggered boot** converges to a
  fully-replicated ACL store. Idempotent and best-effort. Mirrors `UserServer.reconcile/2`.
  """
  @spec reconcile(cluster_name(), [node()]) :: :ok
  def reconcile(cluster_name, nodes), do: RaCluster.reconcile(AclMachine, cluster_name, nodes)

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
    _ = RaCluster.delete(cluster_name)
    :ok
  end

  defp command(server_id, command), do: RaCluster.command(server_id, command)

  # Reads the local replica's state (no consensus round-trip). Eventually consistent; fine for authorization.
  defp local_query(server_id, query_fun), do: RaCluster.local_query(server_id, query_fun)
end
