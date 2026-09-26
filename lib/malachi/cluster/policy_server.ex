defmodule Malachi.Cluster.PolicyServer do
  @moduledoc """
  A thin named facade over `Malachi.Cluster.RaCluster` for the cluster's storage policy store
  (`Malachi.Cluster.PolicyMachine`): start the dedicated Raft cluster, submit policy commands through the
  log, and read the replicated definitions. Mirrors `Malachi.Auth.AclServer`; `ra` must already be
  running. This module owns only the policy cluster, not ra's lifecycle.

  **Writes** (`define/3`, `delete/2`) go through the log, replicated by consensus, so every node reads
  the same definition. **Reads** (`get/2`, `all/1`) use `:ra.local_query` against the **local** replica:
  no consensus round trip, which is what lets the placement path resolve a topic's spread attribute on
  every segment creation without a cross-node hop. They are eventually consistent, and a policy edit
  reaching a node within replication lag is the same guarantee ACL grants get.
  """

  alias Malachi.Cluster.Policy
  alias Malachi.Cluster.PolicyMachine
  alias Malachi.Cluster.PolicyRegistry
  alias Malachi.Cluster.RaCluster

  @type cluster_name :: atom()
  @type server_id :: {cluster_name(), node()}

  @doc """
  Starts the policy store's Raft cluster named `cluster_name` across `nodes` (default the local node) and
  returns a `server_id` addressing a real member (the local node when it is one, else the first).
  """
  @spec start(cluster_name(), [node()]) :: {:ok, server_id()} | {:error, term()}
  def start(cluster_name, nodes \\ [node()]) do
    RaCluster.start(PolicyMachine, cluster_name, nodes)
  end

  @doc """
  Ensures this node participates in the policy cluster (self-join), so a **staggered boot** converges to
  a fully-replicated store. Idempotent and best-effort. Mirrors `Malachi.Auth.AclServer.reconcile/2`.
  """
  @spec reconcile(cluster_name(), [node()]) :: :ok
  def reconcile(cluster_name, nodes), do: RaCluster.reconcile(PolicyMachine, cluster_name, nodes)

  @doc "Defines (or replaces) the policy named `name`. Reply `:ok`, or `{:error, :invalid_policy}`."
  @spec define(server_id(), Policy.name(), Policy.t()) :: {:ok, term()} | {:error, term()}
  def define(server_id, name, policy), do: RaCluster.command(server_id, {:define_policy, name, policy})

  @doc "Removes the policy named `name` (idempotent). Reply `:ok`."
  @spec delete(server_id(), Policy.name()) :: {:ok, term()} | {:error, term()}
  def delete(server_id, name), do: RaCluster.command(server_id, {:delete_policy, name})

  @doc "The policy named `name`, read from the local replica (`nil` when undefined)."
  @spec get(server_id(), Policy.name()) :: {:ok, Policy.t() | nil} | {:error, term()}
  def get(server_id, name), do: RaCluster.local_query(server_id, &PolicyRegistry.get(&1, name))

  @doc "Every definition as a map from name to policy, read from the local replica."
  @spec all(server_id()) :: {:ok, %{Policy.name() => Policy.t()}} | {:error, term()}
  def all(server_id), do: RaCluster.local_query(server_id, &PolicyRegistry.all/1)

  @doc "Stops and deletes the policy store's Raft cluster (removing its on-disk state)."
  @spec delete_cluster(cluster_name()) :: :ok
  def delete_cluster(cluster_name) do
    _ = RaCluster.delete(cluster_name)
    :ok
  end
end
