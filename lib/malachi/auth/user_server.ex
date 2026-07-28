defmodule Malachi.Auth.UserServer do
  @moduledoc """
  A thin wrapper around `ra` for the cluster's user store (`Malachi.Auth.UserMachine`): start the dedicated
  Raft cluster, submit user commands through the log, and read the replicated user set. Mirrors
  `Malachi.Cluster.LeaseServer`; `ra` must already be running (`:ra.start_in/1`). This module owns only the
  user cluster, not ra's lifecycle.

  **Writes** (`put_user`/`delete_user`/`update_password`/`import_users`) go through the log, replicated by
  consensus, so every node converges on the same users (unlike the old node-local Mnesia store). They return
  `{:ok, machine_reply}` (e.g. `{:ok, :ok}` or `{:ok, {:error, :user_exists}}`) or `{:error, reason}` when
  the cluster is unreachable.

  **Reads** (`get_user`/`list_users`/`export_users`) use `:ra.local_query` against the **local** replica:
  fast (no consensus round-trip) and adequate for the auth hot path, which runs once per connection. They
  are eventually consistent: a just-written user propagates within replication lag - which is acceptable
  for auth; a caller needing linearizable reads can query the leader instead.
  """

  alias Malachi.Auth.UserMachine
  alias Malachi.Auth.UserRegistry

  @system :default

  @type cluster_name :: atom()
  @type server_id :: {cluster_name(), node()}
  @type user_entry :: {UserRegistry.username(), UserRegistry.password_hash(), UserRegistry.permissions()}

  @doc """
  Starts the user store's Raft cluster named `cluster_name` across `nodes` (default the local node) and
  returns a `server_id` addressing a real member (the local node when it is one, else the first).
  """
  @spec start(cluster_name(), [node()]) :: {:ok, server_id()} | {:error, term()}
  def start(cluster_name, nodes \\ [node()]) do
    server_ids = Enum.map(nodes, &{cluster_name, &1})
    machine = {:module, UserMachine, %{}}

    case :ra.start_cluster(@system, cluster_name, machine, server_ids) do
      {:ok, _started, _not_started} -> {:ok, {cluster_name, member_node(nodes)}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp member_node(nodes) do
    if node() in nodes, do: node(), else: hd(nodes)
  end

  @doc """
  Ensures this node participates in the user cluster (self-join), so a **staggered boot** converges to a
  fully-replicated user store. Idempotent and best-effort: forms the cluster if not yet formed (`start/2`,
  auto-fenced) and starts the local server if it is not running (`:ra.start_server`). Meant to be called
  periodically until the node has joined. Mirrors `LeaseServer.reconcile/2`.
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

  # Best-effort: starts the local user server so it (re)joins the cluster. Any error (already started, or the
  # cluster not yet formed) is ignored: reconcile is idempotent and the caller retries.
  defp ensure_local_server(cluster_name, nodes) do
    server_ids = Enum.map(nodes, &{cluster_name, &1})
    machine = {:module, UserMachine, %{}}
    _ = :ra.start_server(@system, cluster_name, {cluster_name, node()}, machine, server_ids)
    :ok
  end

  @doc "Inserts a user. Machine reply is `:ok` or `{:error, :user_exists}`."
  @spec put_user(server_id(), UserRegistry.username(), UserRegistry.password_hash(), UserRegistry.permissions()) ::
          {:ok, UserRegistry.reply()} | {:error, term()}
  def put_user(server_id, username, hash, permissions) do
    command(server_id, {:put_user, username, hash, permissions})
  end

  @doc "Deletes a user (idempotent). Machine reply is `:ok`."
  @spec delete_user(server_id(), UserRegistry.username()) :: {:ok, UserRegistry.reply()} | {:error, term()}
  def delete_user(server_id, username), do: command(server_id, {:delete_user, username})

  @doc "Updates a user's password hash. Machine reply is `:ok` or `{:error, :user_not_found}`."
  @spec update_password(server_id(), UserRegistry.username(), UserRegistry.password_hash()) ::
          {:ok, UserRegistry.reply()} | {:error, term()}
  def update_password(server_id, username, new_hash) do
    command(server_id, {:update_password, username, new_hash})
  end

  @doc "Bulk-imports users (skips existing). Machine reply is `{:ok, %{imported: n, skipped: n}}`."
  @spec import_users(server_id(), [user_entry()]) :: {:ok, UserRegistry.reply()} | {:error, term()}
  def import_users(server_id, users), do: command(server_id, {:import_users, users})

  @doc "Reads a user as `{username, hash, permissions}` from the local replica, or `{:error, :user_not_found}`."
  @spec get_user(server_id(), UserRegistry.username()) ::
          {:ok, {UserRegistry.username(), UserRegistry.password_hash(), UserRegistry.permissions()}}
          | {:error, term()}
  def get_user(server_id, username) do
    case local_query(server_id, &UserRegistry.get_user(&1, username)) do
      {:ok, {:ok, user}} -> {:ok, user}
      {:ok, {:error, :user_not_found}} -> {:error, :user_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Lists all users (no hashes) from the local replica."
  @spec list_users(server_id()) :: {:ok, [map()]} | {:error, term()}
  def list_users(server_id), do: local_query(server_id, &UserRegistry.list_users/1)

  @doc "Exports all users (no hashes) from the local replica."
  @spec export_users(server_id()) :: {:ok, [map()]} | {:error, term()}
  def export_users(server_id), do: local_query(server_id, &UserRegistry.export_users/1)

  @doc "Stops and deletes the user store's Raft cluster (removing its on-disk state)."
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

  # Reads the local replica's state (no consensus round-trip). Eventually consistent; fine for auth.
  defp local_query(server_id, query_fun) do
    case :ra.local_query(server_id, query_fun) do
      {:ok, {_idx_term, result}, _leader} -> {:ok, result}
      {:error, reason} -> {:error, reason}
      {:timeout, _server} -> {:error, :timeout}
    end
  end
end
