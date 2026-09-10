defmodule Malachi.Cluster.TopologyPublisher do
  @moduledoc """
  The one place a ring change becomes visible: **persist first, disseminate second**.

  Every publication writes the new `Malachi.Cluster.RingTopology` to the durable store
  (`Malachi.Cluster.RingServer`) and only then hands it to the membership, from where gossip carries it
  cluster-wide.

  ## Why the order is not interchangeable

  Gossiping first and persisting second leaves a window in which a crash strands the cluster believing
  a ring that was never recorded, which is the very failure the durable store exists to prevent, just
  with extra steps. In this order a crash between the two leaves the ring durable and undisseminated,
  and gossip re-derives it from the next boot or read. So the failure mode is a slow convergence rather
  than a lost ring.

  ## Why there is one function and not a pair of calls at each site

  A vnode split publishes at four points: the intent, the completed ring, the abort, and the completion
  driven by a reconciler after a crash. Persisting beside each `set_topology` would be four pairs to
  keep in step, and the fifth publication someone adds later is the one that forgets. Routing every
  publication through here makes forgetting impossible.

  A refused write (`{:error, {:conflict, _}}`, the store rejecting a stale writer) is returned **without
  gossiping**: a ring this cluster did not accept must not be disseminated as though it had been.
  """

  alias Malachi.Cluster.MembershipServer
  alias Malachi.Cluster.RingServer
  alias Malachi.Cluster.RingTopology

  @typedoc """
  The seam a caller holds: `(topology, expected_version, fence -> :ok | {:error, reason})`.
  `expected_version` is the version the writer read and believes it is extending, and `fence` its lease
  token; together they are the compare-and-set the store applies.
  """
  @type publish :: (RingTopology.t(), non_neg_integer(), non_neg_integer() -> :ok | {:error, term()})

  @doc """
  Persists `topology` to the ring store and, only on success, publishes it to `membership`.

  Returns `:ok`, or the store's error: `{:error, {:conflict, ring}}` when another writer moved the ring
  first or this writer's fence is stale, and `{:error, reason}` when the store is unreachable.
  """
  @spec publish(RingServer.server_id(), GenServer.server(), RingTopology.t(), non_neg_integer(), non_neg_integer()) ::
          :ok | {:error, term()}
  def publish(ring_server_id, membership, %RingTopology{} = topology, expected_version, fence) do
    case RingServer.advance(ring_server_id, expected_version, fence, topology) do
      :ok -> MembershipServer.set_topology(membership, topology)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  A `t:publish/0` bound to a ring store and a membership, the shape `Malachi.Cluster.VnodeSplit` takes.
  """
  @spec seam(RingServer.server_id(), GenServer.server()) :: publish()
  def seam(ring_server_id, membership) do
    fn topology, expected_version, fence ->
      publish(ring_server_id, membership, topology, expected_version, fence)
    end
  end

  @doc """
  A `t:publish/0` that only gossips, ignoring the fence and the expected version.

  For contexts with no durable store to write to: unit tests of the split machinery itself, which are
  about the migration rather than about durability. Never wired in production, where a publication that
  skipped the store would recreate the bug the store exists to fix.
  """
  @spec gossip_only(GenServer.server()) :: publish()
  def gossip_only(membership) do
    fn topology, _expected_version, _fence -> MembershipServer.set_topology(membership, topology) end
  end
end
