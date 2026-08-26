defmodule Malachi.Cluster.VnodeSplit do
  @moduledoc """
  Runs a vnode split end to end, under the lease. Only the lease leader acts (one writer of the ring
  version). It reads the cluster's current `Malachi.Cluster.RingTopology` from the membership, **records the
  split intent** (`begin_split`, gossiped before any migration so a coordinator that takes over the lease
  mid-split can reconcile it: B2), splits the ring over real Raft - starting the new vnode's cluster and
  migrating the displaced topics' metadata, fenced and copy-first
  (`Malachi.Cluster.ReplicatedDSRSM.split_vnode/4`), then **advances** the topology (which moves the ring
  forward and clears the intent) and **publishes** it back to the membership (`set_topology`). On a logical
  migration failure, `split_vnode` has already rolled back in-process, so it just clears the now-stale
  intent (`clear_pending`) and returns the error. From there gossip disseminates the topology and every node
  adopts the new ring for both metadata and consumer-group routing (Int-1 / VS-2b).

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

  @doc """
  Reconciles an **interrupted** split on lease takeover (or coordinator restart): if the topology carries a
  pending-split intent: a split whose coordinator crashed after recording it but before completing:
  **completes it forward** (`ReplicatedDSRSM.complete_split/4`: resume the migration idempotently from
  wherever it stopped) and publish the finished topology (`advance`, which moves the ring forward and clears
  the intent). This is NorthGuard's *"carrying it out to the end"*: the coordinator that takes over drives
  the split to completion rather than undoing it. If it cannot complete now (a vnode is unreachable), the
  intent is **kept pending** so a later takeover/restart retries: the partial state is left intact, never
  cleared, so no progress is lost (an explicit abort is `ReplicatedDSRSM.abort_split/3`). A no-op when
  nothing is pending. Only the lease leader acts (`{:error, :not_leader}` otherwise). Idempotent.
  """
  @spec reconcile(GenServer.server(), (-> boolean())) :: :ok | {:error, :not_leader}
  def reconcile(membership, leader? \\ fn -> true end) do
    if leader?.() do
      case MembershipServer.topology(membership) do
        %RingTopology{pending: %{new_vnode: new_vnode, token: token, nodes: [_ | _] = nodes}} = topology ->
          state = %ReplicatedDSRSM{ring: topology.ring, vnodes: RingTopology.servers(topology)}

          case ReplicatedDSRSM.complete_split(state, new_vnode, token, nodes) do
            {:ok, grown} ->
              # the split finished: publish the completed ring + placement (advance clears the intent)
              placements = Map.put(topology.placements, new_vnode, nodes)
              MembershipServer.set_topology(membership, RingTopology.advance(topology, grown.ring, placements))

            {:error, _reason} ->
              # a vnode was unreachable, so the split could not complete now; keep the intent pending (the
              # partial state is intact) so a later takeover/restart retries, do not clear it away
              :ok
          end

          :ok

        _no_pending ->
          :ok
      end
    else
      {:error, :not_leader}
    end
  end

  defp do_split(membership, current, new_vnode_id, token, nodes) do
    replicated = %ReplicatedDSRSM{ring: current.ring, vnodes: RingTopology.servers(current)}

    # Record the split intent (gossiped) *before* migrating: if this coordinator crashes mid-split, the node
    # that takes over the lease finds the pending intent and reconciles the interrupted split (B2-3). The
    # ring is unchanged at this point: routing still uses the pre-split placement until the split completes.
    pending = RingTopology.begin_split(current, new_vnode_id, token, nodes)
    MembershipServer.set_topology(membership, pending)

    case ReplicatedDSRSM.split_vnode(replicated, new_vnode_id, token, nodes) do
      {:ok, grown} ->
        placements = Map.put(current.placements, new_vnode_id, nodes)
        MembershipServer.set_topology(membership, RingTopology.advance(pending, grown.ring, placements))

      {:error, _reason} = error ->
        # split_vnode already rolled the migration back in-process, so the ring is back to pre-split;
        # drop the now-stale intent (`clear_pending` bumps the version forward, keeping the old ring) so no
        # failover reconciler acts on an already-undone split. Only a *crash* before here leaves it pending.
        MembershipServer.set_topology(membership, RingTopology.clear_pending(pending))
        error
    end
  end
end
