defmodule Malachi.Cluster.RingServer do
  @moduledoc """
  The cluster's **durable ring**: a thin named facade over `Malachi.Cluster.RaCluster` running
  `Malachi.Cluster.RingMachine`. Start the dedicated Raft cluster, seed it at first boot, publish ring
  changes under the lease fence, and read the topology of record.

  ## Why this cluster has no bootstrap circularity

  The ring says where the **metadata** vnodes live, so storing it inside those vnodes would be
  circular. It is not: this is a separate Raft group whose membership comes from the static
  `MALACHI_LOG_NODES`, the same list the lease cluster forms over. The layering is

    * `MALACHI_LOG_NODES` (static environment) fixes this cluster's membership, depending on nothing;
    * this cluster answers with the topology;
    * the metadata vnodes are started from that topology.

  The only genuine bootstrap is the very first boot, when nothing is recorded. `init/2` handles it
  with an idempotent compare-and-set, so **every** node may seed concurrently and exactly one wins,
  the same auto-fenced move `RaCluster.start/3` already makes when forming a cluster. The seeds are
  identical anyway: placement is deterministic rendezvous hashing over the same static node list.

  ## Cost

  Read once per boot; written twice per vnode split (the intent, then the completed ring). Routing on
  the hot path never comes here: it reads the gossiped topology held in memory. So this store adds a
  boot-time dependency, not a request-path one.
  """

  alias Malachi.Cluster.RaCluster
  alias Malachi.Cluster.Ring
  alias Malachi.Cluster.RingMachine
  alias Malachi.Cluster.RingTopology

  @type cluster_name :: atom()
  @type server_id :: {cluster_name(), node()}

  @doc """
  Starts the ring's Raft cluster named `cluster_name` across `nodes`, returning a `server_id`
  addressing a real member. Resume-first, so a node that already hosted this cluster comes back with
  its history rather than as an empty member.
  """
  @spec start(cluster_name(), [node()]) :: {:ok, server_id()} | {:error, term()}
  def start(cluster_name, nodes \\ [node()]) do
    RaCluster.start(RingMachine, cluster_name, nodes)
  end

  @doc """
  Keeps this node joined to the ring cluster, so a staggered boot converges to a fully-replicated
  ring. Idempotent and best-effort; meant to be called periodically until the node has joined.
  """
  @spec reconcile(cluster_name(), [node()]) :: :ok
  def reconcile(cluster_name, nodes), do: RaCluster.reconcile(RingMachine, cluster_name, nodes)

  @doc """
  Seeds a cluster that has never had a ring. Returns `:ok` when this call planted the topology, or
  `{:error, {:exists, current}}` when one was already recorded, which at first boot simply means
  another node won the race and `current` is what to adopt. `{:error, reason}` when the cluster is
  unreachable.
  """
  @spec init(server_id(), RingTopology.t()) :: :ok | {:error, term()}
  def init(server_id, %RingTopology{} = topology) do
    unwrap(RaCluster.command(server_id, {:init, topology}))
  end

  @doc """
  Publishes a ring change under the writer's lease `fence`, extending the topology it read at
  `expected_version`. Returns `:ok`, `{:error, {:conflict, ring}}` when another writer moved the ring
  first (or this writer's fence is stale), or `{:error, reason}` when the cluster is unreachable.

  The conflict is the guard a lease cannot give on its own: a holder that lost the lease without
  noticing still believes it leads, and is stopped here by the log rather than by a timeout.
  """
  @spec advance(server_id(), non_neg_integer(), non_neg_integer(), RingTopology.t()) :: :ok | {:error, term()}
  def advance(server_id, expected_version, fence, %RingTopology{} = topology) do
    unwrap(RaCluster.command(server_id, {:advance, expected_version, fence, topology}))
  end

  @doc """
  The topology of record, as the three-valued read boot depends on: `{:ok, topology}` when a ring is
  recorded, `{:ok, :none}` when the cluster **affirms** it has never had one, and `{:error, reason}`
  when it cannot answer (no quorum yet, typically the first node of a full-cluster restart).

  Boot must keep those three apart: only the middle one licenses seeding from `MALACHI_LOG_VNODES`.
  Treating the third as the second is exactly the bug this store exists to fix.
  """
  @spec topology(server_id()) :: {:ok, RingTopology.t()} | {:ok, :none} | {:error, term()}
  def topology(server_id) do
    case RaCluster.query(server_id) do
      {:ok, %Ring{} = ring} ->
        case Ring.topology(ring) do
          {:ok, topology} -> {:ok, topology}
          :none -> {:ok, :none}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "The full replicated store (topology plus the highest fence seen), for diagnostics."
  @spec get(server_id()) :: {:ok, Ring.t()} | {:error, term()}
  def get(server_id) do
    case RaCluster.query(server_id) do
      {:ok, %Ring{} = ring} -> {:ok, ring}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Stops and deletes the ring's Raft cluster, removing its on-disk state."
  @spec delete(server_id() | cluster_name()) :: :ok | {:error, term()}
  def delete(target), do: RaCluster.delete(target)

  # A machine reply of :ok is success; anything else is the machine refusing, which the caller must
  # see as an error rather than as a value. An unreachable cluster is already {:error, reason}.
  defp unwrap({:ok, :ok}), do: :ok
  defp unwrap({:ok, {:error, reason}}), do: {:error, reason}
  defp unwrap({:error, reason}), do: {:error, reason}
end
