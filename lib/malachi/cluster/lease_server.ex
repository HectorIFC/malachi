defmodule Malachi.Cluster.LeaseServer do
  @moduledoc """
  A thin wrapper around `ra` for the cluster's lease (`Malachi.Cluster.LeaseMachine`): start the
  dedicated Raft cluster, submit `acquire_or_renew`/`release` commands through the log, and run a
  consistent (linearizable) query of the lease state. Mirrors `MetadataServer`; `ra` must already be
  running (`:ra.start_in/1`). This module owns only the lease cluster, not ra's lifecycle.

  Commands return `{:ok, machine_reply}` (the machine reply is `{:ok, fence}` on grant or
  `{:error, {:held, holder}}` on refusal) or `{:error, reason}` when the cluster is unreachable, the
  caller (`LeaseHolder`) distinguishes "refused" from "could not reach", treating the latter as
  not-renewed.
  """

  alias Malachi.Cluster.Lease
  alias Malachi.Cluster.LeaseMachine

  @system :default

  @type cluster_name :: atom()
  @type server_id :: {cluster_name(), node()}

  @doc """
  Starts the lease's Raft cluster named `cluster_name` across `nodes` (default the local node) and
  returns a `server_id` addressing a real member (the local node when it is one, else the first).
  """
  @spec start(cluster_name(), [node()]) :: {:ok, server_id()} | {:error, term()}
  def start(cluster_name, nodes \\ [node()]) do
    server_ids = Enum.map(nodes, &{cluster_name, &1})
    machine = {:module, LeaseMachine, %{}}

    case :ra.start_cluster(@system, cluster_name, machine, server_ids) do
      {:ok, _started, _not_started} -> {:ok, {cluster_name, member_node(nodes)}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp member_node(nodes) do
    if node() in nodes, do: node(), else: hd(nodes)
  end

  @doc """
  Ensures this node participates in the lease cluster (self-join), so a **staggered boot** converges to a
  fully-replicated lease. Idempotent and best-effort: it tries to form the cluster if it is not yet
  formed (`start/2`, auto-fenced) **and** to start the local server if it is not running
  (`:ra.start_server`). The local node is already a config member (the initial `start_cluster` lists every
  node), so starting its server rejoins the existing cluster and `ra` replicates the lease state to it:
  recovering a node that was down when the cluster first formed. A no-op once the local server is up.
  Meant to be called periodically by `Malachi.Cluster.LeaseReconciler` until the node has joined.
  """
  @spec reconcile(cluster_name(), [node()]) :: :ok
  def reconcile(cluster_name, nodes) do
    # Skip if the local server is already running (the common case), avoids re-issuing start_cluster on a
    # formed cluster, which ra logs as a (harmless but noisy) "failed to form" error and needlessly churns
    # the shared ra system. Only a node that has not yet joined tries to form/join. Mirrors
    # `UserServer.reconcile/2` and `LockoutServer.reconcile/2`.
    case :ra.members({cluster_name, node()}) do
      {:ok, _members, _leader} ->
        :ok

      _not_running ->
        _ = start(cluster_name, nodes)
        ensure_local_server(cluster_name, nodes)
    end
  end

  # Best-effort: starts the local lease server so it (re)joins the cluster. Any error (already started, or
  # the cluster not yet formed) is ignored, reconcile is idempotent and LeaseReconciler retries.
  defp ensure_local_server(cluster_name, nodes) do
    server_ids = Enum.map(nodes, &{cluster_name, &1})
    machine = {:module, LeaseMachine, %{}}
    _ = :ra.start_server(@system, cluster_name, {cluster_name, node()}, machine, server_ids)
    :ok
  end

  @doc """
  Acquires the lease for `candidate` (or renews it if already held), for `duration_ms`. Returns
  `{:ok, {:ok, fence}}` on grant, `{:ok, {:error, {:held, holder}}}` when held by another, or
  `{:error, reason}` when the cluster is unreachable.
  """
  @spec acquire_or_renew(server_id(), term(), pos_integer()) :: {:ok, Lease.reply()} | {:error, term()}
  def acquire_or_renew(server_id, candidate, duration_ms) do
    command(server_id, {:acquire_or_renew, candidate, duration_ms})
  end

  @doc "Releases the lease if `candidate` still holds it at `fence` (idempotent). Returns `{:ok, :ok}`."
  @spec release(server_id(), term(), non_neg_integer()) :: {:ok, Lease.reply()} | {:error, term()}
  def release(server_id, candidate, fence) do
    command(server_id, {:release, candidate, fence})
  end

  @doc """
  The current lease state via a consistent query. The `{Function, :identity, []}` is `ra`'s required
  shape for a consistent query (a plain fun is rejected); it returns the machine state unchanged.
  """
  @spec get(server_id()) :: {:ok, Lease.t()} | {:error, term()}
  def get(server_id) do
    case :ra.consistent_query(server_id, {Function, :identity, []}) do
      {:ok, %Lease{} = lease, _leader} -> {:ok, lease}
      {:error, reason} -> {:error, reason}
      {:timeout, _server} -> {:error, :timeout}
    end
  end

  @doc "Stops and deletes the lease's Raft cluster (removing its on-disk state)."
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
end
