defmodule Malachi.Cluster.VnodeSplit do
  @moduledoc """
  Runs a vnode split end to end, under the lease. Only the lease leader acts (one writer of the ring
  version). It reads the cluster's current `Malachi.Cluster.RingTopology` from the membership, **records the
  split intent** (`begin_split`, published before any migration so a coordinator that takes over the lease
  mid-split can reconcile it: B2), splits the ring over real Raft - starting the new vnode's cluster and
  migrating the displaced topics' metadata, fenced and copy-first
  (`Malachi.Cluster.ReplicatedDSRSM.split_vnode/4`), then **advances** the topology (which moves the ring
  forward and clears the intent) and **publishes** it. On a logical migration failure, `split_vnode` has
  already rolled back in-process, so it just clears the now-stale intent (`clear_pending`) and returns the
  error. From there gossip disseminates the topology and every node adopts the new ring for both metadata
  and consumer-group routing (Int-1 / VS-2b).

  Every publication goes through the `:publish` seam
  (`Malachi.Cluster.TopologyPublisher`), which writes the durable ring **before** gossiping it, so a
  reshard survives a full-cluster restart. A publication the store refuses stops the split where it
  stands rather than disseminating a ring the cluster did not accept.

  ## The lease seam carries a token, not a boolean

  `:lease` answers `{:ok, fence} | :error`. The fence rides into every durable write, so a coordinator
  that lost the lease without noticing is refused by the store's compare-and-set rather than left to
  discover it on the next renew. That is the case a lease cannot cover on its own.

  This ties the split slices together: the migration (VS-2a/VS-2c), the versioned disseminable ring
  (VS-2b), the durable ring (issue #32), and the runtime adoption (Int-1).
  """

  require Logger

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MembershipServer
  alias Malachi.Cluster.ReplicatedDSRSM
  alias Malachi.Cluster.RingTopology
  alias Malachi.Cluster.TopologyPublisher
  alias Malachi.I18n

  @typedoc "The lease with its fencing token: `{:ok, fence}` while this node leads, `:error` otherwise."
  @type lease :: (-> {:ok, non_neg_integer()} | :error)

  @typedoc """
  Required `:publish` (a `t:Malachi.Cluster.TopologyPublisher.publish/0`) and `:lease`. `:lease`
  defaults to a permanently-held lease at fence 0, which is what unit tests of the split machinery want
  and what the application never uses: it always wires the real holder.
  """
  @type option :: {:publish, TopologyPublisher.publish()} | {:lease, lease()}

  @doc """
  Splits the vnode owning `token`'s region by adding `new_vnode_id` at `token` (a new ra cluster on
  `nodes`), migrating the displaced topics, and publishing the advanced topology. Returns `:ok`,
  `{:error, :not_leader}` if this node does not hold the lease, `{:error, :no_topology}` if the cluster
  has no ring yet, `{:error, {:conflict, ring}}` if the durable store refused the publication, or the
  `split_vnode/4` error (a ring/start/migration failure).
  """
  @spec split(GenServer.server(), HashRing.vnode_id(), HashRing.token(), [node()], [option()]) ::
          :ok | {:error, term()}
  def split(membership, new_vnode_id, token, nodes, opts) do
    with {:ok, fence} <- held_lease(opts) do
      case MembershipServer.topology(membership) do
        %RingTopology{} = current -> do_split(current, new_vnode_id, token, nodes, publish(opts), fence)
        nil -> {:error, :no_topology}
      end
    end
  end

  @doc """
  Reconciles an **interrupted** split on lease takeover (or coordinator restart): if the topology carries a
  pending-split intent: a split whose coordinator crashed after recording it but before completing:
  **completes it forward** (`ReplicatedDSRSM.complete_split/4`: resume the migration idempotently from
  wherever it stopped) and publishes the finished topology (`advance`, which moves the ring forward and
  clears the intent). This is NorthGuard's *"carrying it out to the end"*: the coordinator that takes over
  drives the split to completion rather than undoing it. If it cannot complete now (a vnode is
  unreachable), the intent is **kept pending** so a later takeover/restart retries: the partial state is
  left intact, never cleared, so no progress is lost (an explicit abort is
  `ReplicatedDSRSM.abort_split/3`). A no-op when nothing is pending. Only the lease leader acts
  (`{:error, :not_leader}` otherwise). Idempotent.

  Because the intent is part of the durable topology, it now also survives a **full-cluster restart**:
  a split interrupted by everything going down is still there to be completed when the cluster returns.
  """
  @spec reconcile(GenServer.server(), [option()]) :: :ok | {:error, :not_leader}
  def reconcile(membership, opts) do
    with {:ok, fence} <- held_lease(opts) do
      case MembershipServer.topology(membership) do
        %RingTopology{pending: %{new_vnode: new_vnode, token: token, nodes: [_ | _] = nodes}} = topology ->
          complete_pending(topology, new_vnode, token, nodes, publish(opts), fence)

        _no_pending ->
          :ok
      end
    end
  end

  defp complete_pending(topology, new_vnode, token, nodes, publish, fence) do
    state = %ReplicatedDSRSM{ring: topology.ring, vnodes: RingTopology.servers(topology)}

    case ReplicatedDSRSM.complete_split(state, new_vnode, token, nodes) do
      {:ok, grown} ->
        # the split finished: publish the completed ring + placement (advance clears the intent)
        placements = Map.put(topology.placements, new_vnode, nodes)
        completed = RingTopology.advance(topology, grown.ring, placements)
        log_refusal(publish.(completed, topology.version, fence), new_vnode, :ring_publish_refused_completing)
        :ok

      {:error, _reason} ->
        # a vnode was unreachable, so the split could not complete now; keep the intent pending (the
        # partial state is intact) so a later takeover/restart retries, do not clear it away
        :ok
    end
  end

  defp do_split(current, new_vnode_id, token, nodes, publish, fence) do
    replicated = %ReplicatedDSRSM{ring: current.ring, vnodes: RingTopology.servers(current)}

    # Record the split intent *before* migrating: if this coordinator crashes mid-split, the node that
    # takes over the lease finds the pending intent and reconciles the interrupted split (B2-3). The ring
    # is unchanged at this point: routing still uses the pre-split placement until the split completes.
    #
    # Publishing it is also the point at which this coordinator proves it still holds the ring: a refusal
    # here stops the split before anything has been migrated, which is the cheapest place to stop.
    pending = RingTopology.begin_split(current, new_vnode_id, token, nodes)

    with :ok <- publish.(pending, current.version, fence) do
      migrate(replicated, pending, current.placements, new_vnode_id, token, nodes, publish, fence)
    end
  end

  defp migrate(replicated, pending, placements, new_vnode_id, token, nodes, publish, fence) do
    case ReplicatedDSRSM.split_vnode(replicated, new_vnode_id, token, nodes) do
      {:ok, grown} ->
        grown_placements = Map.put(placements, new_vnode_id, nodes)
        publish.(RingTopology.advance(pending, grown.ring, grown_placements), pending.version, fence)

      {:error, _reason} = error ->
        # split_vnode already rolled the migration back in-process, so the ring is back to pre-split;
        # drop the now-stale intent (`clear_pending` bumps the version forward, keeping the old ring) so no
        # failover reconciler acts on an already-undone split. Only a *crash* before here leaves it pending.
        cleared = RingTopology.clear_pending(pending)
        log_refusal(publish.(cleared, pending.version, fence), new_vnode_id, :ring_publish_refused_clearing)
        error
    end
  end

  # Both of these publications are best-effort by design: the caller's contract is fixed (`:ok` for the
  # reconciler, the migration error for an abort), and the pending intent is durable, so a later lease
  # takeover or coordinator restart retries. What must not happen is the refusal passing unrecorded: the
  # metadata has moved while the published ring has not, and without a line here nothing says so.
  # Retrying from here is deliberately not done, because the topology we hold is the stale one.
  defp log_refusal(:ok, _vnode_id, _message_key), do: :ok

  defp log_refusal({:error, reason}, vnode_id, message_key) do
    Logger.warning(I18n.t(message_key, vnode: inspect(vnode_id), reason: inspect(reason)))
  end

  # `{:error, :not_leader}` keeps the historical contract: callers already treat it as "someone else
  # will do this", and a lost lease is exactly that.
  defp held_lease(opts) do
    lease = Keyword.get(opts, :lease, fn -> {:ok, 0} end)

    case lease.() do
      {:ok, fence} -> {:ok, fence}
      :error -> {:error, :not_leader}
    end
  end

  defp publish(opts), do: Keyword.fetch!(opts, :publish)
end
