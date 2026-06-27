defmodule Malachi.Cluster.MetadataServer do
  @moduledoc """
  A thin wrapper around `ra` for running the `Malachi.Cluster.MetadataMachine` of a single
  DS-RSM vnode: start the Raft cluster, submit metadata commands through the log, and run
  consistent (linearizable) queries over the replicated state.

  `ra` itself must already be running (e.g. `:ra.start_in/1` with a data directory, done by
  the application or test setup). This module does not own ra's lifecycle — only the vnode's
  cluster.
  """

  alias Malachi.Cluster.MetadataMachine
  alias Malachi.Metadata

  @system :default

  @type cluster_name :: atom()
  @type server_id :: {cluster_name(), node()}

  @doc """
  Starts a Raft cluster named `cluster_name` running the metadata machine across `nodes` (default
  the local node), and returns the **local** `server_id`. With several nodes the metadata is
  replicated and survives the loss of a member: a follower is elected leader, so the control plane
  has no single point of failure. `ra` must be running on every node (`:ra.start_in/1`).
  """
  @spec start(cluster_name(), [node()]) :: {:ok, server_id()} | {:error, term()}
  def start(cluster_name, nodes \\ [node()]) do
    server_ids = Enum.map(nodes, &{cluster_name, &1})
    machine = {:module, MetadataMachine, %{}}

    case :ra.start_cluster(@system, cluster_name, machine, server_ids) do
      {:ok, _started, _not_started} -> {:ok, {cluster_name, node()}}
      {:error, reason} -> {:error, reason}
    end
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

  @doc "Stops and deletes the vnode's Raft cluster (removing its on-disk state)."
  @spec delete(cluster_name()) :: :ok
  def delete(cluster_name) do
    :ra.delete_cluster([{cluster_name, node()}])
    :ok
  end
end
