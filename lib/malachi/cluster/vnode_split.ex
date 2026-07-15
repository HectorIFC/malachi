defmodule Malachi.Cluster.VnodeSplit do
  @moduledoc """
  Runs a vnode split end to end, under the lease. Only the lease leader acts (one writer of the ring
  version). It reads the cluster's current `Malachi.Cluster.RingTopology` from the membership, splits the
  ring over real Raft — starting the new vnode's cluster and migrating the displaced topics' metadata,
  fenced and copy-first (`Malachi.Cluster.ReplicatedDSRSM.split_vnode/4`) — then **advances** the topology
  a version and **publishes** it back to the membership (`set_topology`). From there gossip disseminates
  it and every node adopts the new ring for both metadata and consumer-group routing (Int-1 / VS-2b).

  This ties the split slices together: the migration (VS-2a/VS-2c), the versioned disseminable ring
  (VS-2b), and the runtime adoption (Int-1). The caller (or the app's lease-gated wiring) supplies the
  `leader?` seam; `split/5` refuses with `{:error, :not_leader}` unless it holds the lease.
  """

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MembershipServer
  alias Malachi.Cluster.ReplicatedDSRSM
  alias Malachi.Cluster.RingTopology

  @doc """
  Splits the vnode owning `token`'s region by adding `new_vnode_id` at `token` (a new ra cluster on
  `nodes`), migrating the displaced topics, and publishing the advanced topology to `membership`.
  Returns `:ok`, `{:error, :not_leader}` if this node does not hold the lease, `{:error, :no_topology}`
  if the cluster has no ring yet, or the `split_vnode/4` error (a ring/start/migration failure).
  """
  @spec split(GenServer.server(), HashRing.vnode_id(), HashRing.token(), [node()], (-> boolean())) ::
          :ok | {:error, term()}
  def split(membership, new_vnode_id, token, nodes, leader? \\ fn -> true end) do
    if leader?.() do
      case MembershipServer.topology(membership) do
        %RingTopology{} = current -> do_split(membership, current, new_vnode_id, token, nodes)
        nil -> {:error, :no_topology}
      end
    else
      {:error, :not_leader}
    end
  end

  defp do_split(membership, current, new_vnode_id, token, nodes) do
    replicated = %ReplicatedDSRSM{ring: current.ring, vnodes: RingTopology.servers(current)}

    case ReplicatedDSRSM.split_vnode(replicated, new_vnode_id, token, nodes) do
      {:ok, grown} ->
        placements = Map.put(current.placements, new_vnode_id, nodes)
        MembershipServer.set_topology(membership, RingTopology.advance(current, grown.ring, placements))

      {:error, _reason} = error ->
        error
    end
  end
end
