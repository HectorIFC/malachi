defmodule Malachi.Cluster.LeaseServer do
  @moduledoc """
  A thin named facade over `Malachi.Cluster.RaCluster` for the cluster's lease (`Malachi.Cluster.LeaseMachine`): start the
  dedicated Raft cluster, submit `acquire_or_renew`/`release` commands through the log, and run a
  consistent (linearizable) query of the lease state. Mirrors `MetadataServer`; `ra` must already be
  running (`:ra.start_in/1`). This module owns only the lease cluster, not ra's lifecycle.

  Until issue #32 this was the one store that formed without resuming first: `start/2` called
  `:ra.start_cluster` directly, so a returning member could be re-created over its registered uid and
  come back amnesiac. Going through `RaCluster` fixes that here by construction.

  Commands return `{:ok, machine_reply}` (the machine reply is `{:ok, fence}` on grant or
  `{:error, {:held, holder}}` on refusal) or `{:error, reason}` when the cluster is unreachable, the
  caller (`LeaseHolder`) distinguishes "refused" from "could not reach", treating the latter as
  not-renewed.
  """

  alias Malachi.Cluster.Lease
  alias Malachi.Cluster.LeaseMachine
  alias Malachi.Cluster.RaCluster

  @type cluster_name :: atom()
  @type server_id :: {cluster_name(), node()}

  @doc """
  Starts the lease's Raft cluster named `cluster_name` across `nodes` (default the local node) and
  returns a `server_id` addressing a real member (the local node when it is one, else the first).
  """
  @spec start(cluster_name(), [node()]) :: {:ok, server_id()} | {:error, term()}
  def start(cluster_name, nodes \\ [node()]) do
    RaCluster.start(LeaseMachine, cluster_name, nodes)
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
  def reconcile(cluster_name, nodes), do: RaCluster.reconcile(LeaseMachine, cluster_name, nodes)

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
    case RaCluster.query(server_id) do
      {:ok, %Lease{} = lease} -> {:ok, lease}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Stops and deletes the lease's Raft cluster (removing its on-disk state)."
  @spec delete(cluster_name()) :: :ok
  def delete(cluster_name) do
    _ = RaCluster.delete(cluster_name)
    :ok
  end

  defp command(server_id, command), do: RaCluster.command(server_id, command)
end
