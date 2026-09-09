defmodule Malachi.Cluster.SplitCoordinator do
  @moduledoc """
  Drives vnode splits under the lease. A `GenServer` so splits **serialize** (one at a time), a split
  mutates the ring, and two at once would race. Only the lease holder acts: `split/4` runs
  `Malachi.Cluster.VnodeSplit.split/5`, which reads the current topology, migrates the displaced topics
  over `ra` (fenced, copy-first), advances the version and publishes it, gossip then disseminates it and
  every node adopts the new ring.

  Operator-driven, like `Malachi.Cluster.RebalanceCoordinator`: nothing splits automatically; an operator
  (or a future policy) calls `split/4`. The seams (`:membership`, `:publish`, `:lease`) keep it testable
  without a real lease or `ra`.

  `:publish` is how a ring change reaches the world: in production
  `Malachi.Cluster.TopologyPublisher.seam/2`, which records the ring durably before gossiping it, so a
  reshard survives a full-cluster restart. `:lease` answers `{:ok, fence} | :error`; the fence rides into
  every durable write so a coordinator that has lost the lease is refused by the store.
  """

  use GenServer

  alias Malachi.Cluster.VnodeSplit

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_server_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @doc """
  Splits the vnode owning `token`'s region by adding `new_vnode_id` at `token` (a new `ra` cluster on
  `nodes`) and publishing the advanced topology. Refuses with `{:error, :not_leader}` unless this node
  holds the lease. Serialized with other splits; may take a while (it migrates metadata), so it uses an
  infinite call timeout.
  """
  @spec split(GenServer.server(), term(), non_neg_integer(), [node()]) :: :ok | {:error, term()}
  def split(server, new_vnode_id, token, nodes) do
    GenServer.call(server, {:split, new_vnode_id, token, nodes}, :infinity)
  end

  @doc """
  Reconciles a split left in flight by a coordinator that crashed mid-way (`VnodeSplit.reconcile/2`). Fired
  on lease acquisition (`LeaseHolder` `on_acquired`) and on this coordinator's own restart (`init`), so an
  interrupted split is cleaned up whether the whole node failed over or just this process restarted. A cast:
  it serializes with splits (same process) but never blocks the caller - and a no-op unless this node
  leads and a split is actually pending.
  """
  @spec reconcile(GenServer.server()) :: :ok
  def reconcile(server), do: GenServer.cast(server, :reconcile)

  @impl true
  def init(opts) do
    state = %{
      membership: Keyword.fetch!(opts, :membership),
      split_opts: [publish: Keyword.fetch!(opts, :publish), lease: Keyword.fetch!(opts, :lease)]
    }

    # a coordinator (re)starting may be resuming after a crash mid-split: reconcile any interrupted split.
    # A cast (not inline) so init does not block on ra; guarded by leader?, so it is a no-op unless we lead.
    reconcile(self())
    {:ok, state}
  end

  @impl true
  def handle_call({:split, new_vnode_id, token, nodes}, _from, state) do
    {:reply, VnodeSplit.split(state.membership, new_vnode_id, token, nodes, state.split_opts), state}
  end

  @impl true
  def handle_cast(:reconcile, state) do
    VnodeSplit.reconcile(state.membership, state.split_opts)
    {:noreply, state}
  end
end
