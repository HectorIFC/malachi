defmodule Malachi.Cluster.ReplicatedMetadata do
  @moduledoc """
  The control plane's metadata made **authoritative via Raft**, with a local read cache.

  It pairs a `Malachi.Cluster.MetadataServer` (one `ra` cluster running `Malachi.Metadata.apply/2`)
  with a local `Malachi.Metadata` materialized view. Mutations go through the Raft log (durable and
  replicated); on commit, the very same command is applied to the local cache. Because
  `Malachi.Metadata.apply/2` is deterministic, the cache always equals the replicated state, so
  reads are served locally from the cache (no Raft round-trip on the hot path) with read-your-writes
  consistency.

  The cache is correct without refreshing as long as this process is the only writer (the
  single-control-node topology). `refresh/1` re-reads the replicated state for the multi-writer case
  or after recovery.

  `ra` must already be running (e.g. `:ra.start_in/1`), as with `Malachi.Cluster.MetadataServer`.
  """

  alias Malachi.Cluster.MetadataServer
  alias Malachi.Metadata

  @type t :: %__MODULE__{server_id: MetadataServer.server_id(), cache: Metadata.t()}

  defstruct server_id: nil, cache: nil

  @doc "Starts (or joins) the Raft cluster `cluster_name` and seeds the local cache from it."
  @spec start(MetadataServer.cluster_name()) :: {:ok, t()} | {:error, term()}
  def start(cluster_name) do
    with {:ok, server_id} <- MetadataServer.start(cluster_name),
         {:ok, cache} <- MetadataServer.query(server_id, & &1) do
      {:ok, %__MODULE__{server_id: server_id, cache: cache}}
    end
  end

  @doc """
  Submits `command` through the Raft log and, on commit, applies it to the local cache too.
  Returns `{reply, replicated_metadata}` — `reply` is the machine reply (e.g. `{:ok, root_id}` or
  `{:error, :already_exists}`), or `{:error, reason}` on a transport failure (cache unchanged).
  """
  @spec command(t(), Metadata.command()) :: {term(), t()}
  def command(%__MODULE__{} = replicated, command) do
    case MetadataServer.command(replicated.server_id, command) do
      {:ok, reply} ->
        # Deterministic apply ⇒ the cache tracks the replicated state (a rejected command leaves
        # both unchanged).
        {cache, _local_reply} = Metadata.apply(replicated.cache, command)
        {reply, %{replicated | cache: cache}}

      {:error, reason} ->
        {{:error, reason}, replicated}
    end
  end

  @doc "The local metadata view, for reads (routing, segment lookup)."
  @spec metadata(t()) :: Metadata.t()
  def metadata(%__MODULE__{cache: cache}), do: cache

  @doc "Re-reads the replicated state into the cache (multi-writer or post-recovery)."
  @spec refresh(t()) :: {:ok, t()} | {:error, term()}
  def refresh(%__MODULE__{} = replicated) do
    case MetadataServer.query(replicated.server_id, & &1) do
      {:ok, cache} -> {:ok, %{replicated | cache: cache}}
      {:error, _reason} = error -> error
    end
  end

  @doc "Stops and deletes the underlying Raft cluster (removing its on-disk state)."
  @spec delete(t()) :: :ok
  def delete(%__MODULE__{server_id: {cluster_name, _node}}), do: MetadataServer.delete(cluster_name)
end
