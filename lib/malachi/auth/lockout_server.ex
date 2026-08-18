defmodule Malachi.Auth.LockoutServer do
  @moduledoc """
  A thin wrapper around `ra` for the cluster's account-lockout store (`Malachi.Auth.LockoutMachine`): start
  the dedicated Raft cluster, submit lockout commands through the log, and read the replicated lockout state.
  Mirrors `Malachi.Auth.UserServer`; `ra` must already be running. This module owns only the lockout cluster,
  not ra's lifecycle.

  **Writes** (`record_failed_attempt`/`record_successful_auth`/`unlock_user`/`unlock_key`/`cleanup`) go
  through the log: replicated by consensus, so brute-force protection is **cluster-wide** (attempts spread
  across nodes still count against one limit) and **survives a restart**. `record_failed_attempt` returns the
  machine reply `%{count, locked}` so the caller can drive metrics/logging/audit for a newly applied lockout.

  **Reads** (`locked?`/`failed_attempts`/`list_locked`) use `:ra.local_query` against the **local** replica:
  fast (no consensus round-trip) and adequate for the auth hot path, which checks the lock once per attempt.
  Expiry is evaluated against a caller-supplied `now` (the local node's clock), seconds of clock skew are
  irrelevant to minutes-long lockouts, and reads are eventually consistent (a just-written lockout propagates
  within replication lag), which is acceptable for brute-force defense.
  """

  alias Malachi.Auth.LockoutMachine
  alias Malachi.Auth.LockoutRegistry
  alias Malachi.Cluster.RaResume

  @system :default

  @type cluster_name :: atom()
  @type server_id :: {cluster_name(), node()}

  @doc """
  Starts the lockout store's Raft cluster named `cluster_name` across `nodes` (default the local node) and
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
    machine = {:module, LockoutMachine, %{}}

    case :ra.start_cluster(@system, cluster_name, machine, server_ids) do
      {:ok, _started, _not_started} -> {:ok, {cluster_name, member_node(nodes)}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp member_node(nodes) do
    if node() in nodes, do: node(), else: hd(nodes)
  end

  @doc """
  Ensures this node participates in the lockout cluster (self-join), so a **staggered boot** converges to a
  fully-replicated lockout store. Idempotent and best-effort. Mirrors `UserServer.reconcile/2`.
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

  # Best-effort: starts the local lockout server so it (re)joins the cluster. Any error (already started, or
  # the cluster not yet formed) is ignored: reconcile is idempotent and the caller retries.
  defp ensure_local_server(cluster_name, nodes) do
    # Resume-first here too: :ra.start_server registers a fresh empty uid just like start_cluster,
    # so a self-join over a member this node once hosted must restart it, never re-create it.
    _ =
      RaResume.resume_or(@system, {cluster_name, node()}, fn ->
        server_ids = Enum.map(nodes, &{cluster_name, &1})
        machine = {:module, LockoutMachine, %{}}
        :ra.start_server(@system, cluster_name, {cluster_name, node()}, machine, server_ids)
      end)

    :ok
  end

  @doc """
  Records a failed authentication attempt for `key` (`{username, ip}`), applying `config`
  (`%{max_attempts, base_duration_ms, progressive}`). Machine reply is `%{count, locked}` where `locked` is
  `nil` or `%{duration_ms, locked_until}` when this attempt (re)applied a lockout.
  """
  @spec record_failed_attempt(server_id(), LockoutRegistry.key(), LockoutRegistry.config()) ::
          {:ok, map()} | {:error, term()}
  def record_failed_attempt(server_id, key, config) do
    command(server_id, {:failed_attempt, key, config})
  end

  @doc "Clears failed attempts and any lockout for `key` after a successful login. Machine reply is `:ok`."
  @spec record_successful_auth(server_id(), LockoutRegistry.key()) :: {:ok, :ok} | {:error, term()}
  def record_successful_auth(server_id, key), do: command(server_id, {:successful_auth, key})

  @doc "Unlocks every IP for `username` (admin action). Machine reply is `{:ok, cleared_count}`."
  @spec unlock_user(server_id(), String.t()) :: {:ok, {:ok, non_neg_integer()}} | {:error, term()}
  def unlock_user(server_id, username), do: command(server_id, {:unlock_user, username})

  @doc "Unlocks a single `{username, ip}` key (admin action). Machine reply is `:ok`."
  @spec unlock_key(server_id(), LockoutRegistry.key()) :: {:ok, :ok} | {:error, term()}
  def unlock_key(server_id, key), do: command(server_id, {:unlock_key, key})

  @doc "Compacts expired lockouts and attempts older than `attempt_ttl_ms`. Machine reply is `:ok`."
  @spec cleanup(server_id(), non_neg_integer()) :: {:ok, :ok} | {:error, term()}
  def cleanup(server_id, attempt_ttl_ms), do: command(server_id, {:cleanup, attempt_ttl_ms})

  @doc "Whether `key` is locked at `now` (ms): `:not_locked` or `{:locked, time_remaining_ms}`."
  @spec locked?(server_id(), LockoutRegistry.key(), integer()) ::
          {:ok, :not_locked | {:locked, non_neg_integer()}} | {:error, term()}
  def locked?(server_id, key, now), do: local_query(server_id, &LockoutRegistry.locked?(&1, key, now))

  @doc "The number of failed attempts recorded for `key`."
  @spec failed_attempts(server_id(), LockoutRegistry.key()) :: {:ok, non_neg_integer()} | {:error, term()}
  def failed_attempts(server_id, key), do: local_query(server_id, &LockoutRegistry.failed_attempts(&1, key))

  @doc "Every account currently locked at `now`, as info maps."
  @spec list_locked(server_id(), integer()) :: {:ok, [map()]} | {:error, term()}
  def list_locked(server_id, now), do: local_query(server_id, &LockoutRegistry.list_locked(&1, now))

  @doc "Stops and deletes the lockout store's Raft cluster (removing its on-disk state)."
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

  # Reads the local replica's state (no consensus round-trip). Eventually consistent; fine for lockouts.
  defp local_query(server_id, query_fun) do
    case :ra.local_query(server_id, query_fun) do
      {:ok, {_idx_term, result}, _leader} -> {:ok, result}
      {:error, reason} -> {:error, reason}
      {:timeout, _server} -> {:error, :timeout}
    end
  end
end
