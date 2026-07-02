defmodule Malachi.Cluster.ReplicatedDSRSM do
  @moduledoc """
  The DS-RSM backed by real Raft: a `Malachi.Cluster.HashRing` plus **one `ra` cluster per
  vnode** (each running `Malachi.Cluster.MetadataMachine`). Commands and queries are routed
  by consistent hashing (topic name) to the owning vnode and submitted to that vnode's Raft
  cluster, so the cluster's metadata is sharded across vnodes *and* durably replicated within
  each one. Leadership of a vnode's cluster is that vnode's coordinator.

  This is the production counterpart of the pure `Malachi.Cluster.DSRSM` (which holds the
  per-vnode `Metadata` in memory and is what the property tests exercise). Here each vnode's
  `Metadata` lives in a Raft log instead.

  The value threaded through calls holds only the ring and a `vnode_id => server_id` map
  (both immutable); the metadata itself lives in the ra processes, so `command/3`/`query/3`
  do not change it — only `add_vnode/3` does (and starts the vnode's cluster as a side
  effect). `ra` must already be running (e.g. `:ra.start_in/1`), as with
  `Malachi.Cluster.MetadataServer`.

  Phase 1b scope: **static vnodes** (no vnode split yet — migrating committed metadata
  between Raft groups is a separate step) and single-node clusters (multi-node membership
  comes with SWIM).
  """

  alias Malachi.Cluster.DSRSM
  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Metadata

  @type vnode_id :: atom()

  @type t :: %__MODULE__{
          ring: HashRing.t(),
          vnodes: %{vnode_id() => MetadataServer.server_id()}
        }

  defstruct ring: nil, vnodes: %{}

  @doc "Builds an empty replicated DS-RSM. Options are forwarded to `HashRing.new/1`."
  @spec new(keyword()) :: t()
  def new(opts \\ []), do: %__MODULE__{ring: HashRing.new(opts), vnodes: %{}}

  @doc """
  Adds a vnode at `token` and starts its Raft cluster (named `vnode_id`). Propagates ring
  placement errors and `ra` start errors.
  """
  @spec add_vnode(t(), vnode_id(), HashRing.token()) :: {:ok, t()} | {:error, term()}
  def add_vnode(%__MODULE__{} = state, vnode_id, token) do
    with {:ok, ring} <- HashRing.add_vnode(state.ring, vnode_id, token),
         {:ok, server_id} <- MetadataServer.start(vnode_id) do
      {:ok, %{state | ring: ring, vnodes: Map.put(state.vnodes, vnode_id, server_id)}}
    end
  end

  @doc """
  Routes a `Malachi.Metadata` command to the vnode owning `topic_name` and submits it through
  that vnode's Raft log. Returns the machine reply (e.g. `{:ok, root_id}` or
  `{:error, :already_exists}`), `{:error, :no_vnode}` if the ring is empty, or
  `{:error, {:raft, reason}}` on a transport failure.
  """
  @spec command(t(), Metadata.topic_name(), Metadata.command()) :: term()
  def command(%__MODULE__{} = state, topic_name, command) do
    with_vnode(state, topic_name, fn server_id ->
      case MetadataServer.command(server_id, command) do
        {:ok, reply} -> reply
        {:error, reason} -> {:error, {:raft, reason}}
      end
    end)
  end

  @doc """
  Routes a linearizable query to the vnode owning `topic_name`. `query_fun` receives that
  vnode's `Metadata` state. `{:error, :no_vnode}` if the ring is empty.
  """
  @spec query(t(), Metadata.topic_name(), (Metadata.t() -> result)) ::
          {:ok, result} | {:error, term()}
        when result: term()
  def query(%__MODULE__{} = state, topic_name, query_fun) do
    with_vnode(state, topic_name, fn server_id -> MetadataServer.query(server_id, query_fun) end)
  end

  @doc "The vnode id owning `topic_name`, or `{:error, :empty}` if there are no vnodes."
  @spec vnode_for(t(), Metadata.topic_name()) :: {:ok, vnode_id()} | {:error, :empty}
  def vnode_for(%__MODULE__{} = state, topic_name), do: HashRing.route(state.ring, topic_name)

  @doc "The ra server id of `vnode_id` — for routing a write to that vnode's cluster."
  @spec server_for(t(), vnode_id()) :: MetadataServer.server_id()
  def server_for(%__MODULE__{} = state, vnode_id), do: Map.fetch!(state.vnodes, vnode_id)

  @doc """
  Reads every vnode's replicated `Metadata` into a local `Malachi.Cluster.DSRSM` cache sharing this
  ring — the read-side mirror a broker threads (reads served locally; writes routed back through the
  vnodes' ra clusters via `server_for/2`). Propagates a query error from any vnode.
  """
  @spec snapshot(t()) :: {:ok, DSRSM.t()} | {:error, term()}
  def snapshot(%__MODULE__{} = state) do
    result =
      Enum.reduce_while(state.vnodes, {:ok, %{}}, fn {vnode_id, server_id}, {:ok, acc} ->
        case MetadataServer.query(server_id, & &1) do
          {:ok, metadata} -> {:cont, {:ok, Map.put(acc, vnode_id, metadata)}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    with {:ok, metadata_by_vnode} <- result, do: {:ok, DSRSM.seed(state.ring, metadata_by_vnode)}
  end

  @doc "The ids of the vnodes."
  @spec vnode_ids(t()) :: [vnode_id()]
  def vnode_ids(%__MODULE__{} = state), do: HashRing.vnode_ids(state.ring)

  @doc "Stops and deletes every vnode's Raft cluster (removing on-disk state)."
  @spec delete(t()) :: :ok
  def delete(%__MODULE__{} = state) do
    Enum.each(state.vnodes, fn {vnode_id, _server_id} -> MetadataServer.delete(vnode_id) end)
    :ok
  end

  # --- internals ---

  defp with_vnode(state, topic_name, fun) do
    case HashRing.route(state.ring, topic_name) do
      {:error, :empty} -> {:error, :no_vnode}
      {:ok, vnode_id} -> fun.(Map.fetch!(state.vnodes, vnode_id))
    end
  end
end
