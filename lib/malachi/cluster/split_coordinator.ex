defmodule Malachi.Cluster.SplitCoordinator do
  @moduledoc """
  Drives vnode splits under the lease. A `GenServer` so splits **serialize** (one at a time) — a split
  mutates the ring, and two at once would race. Only the lease holder acts: `split/4` runs
  `Malachi.Cluster.VnodeSplit.split/5`, which reads the current topology, migrates the displaced topics
  over `ra` (fenced, copy-first), advances the version and publishes it — gossip then disseminates it and
  every node adopts the new ring.

  Operator-driven, like `Malachi.Cluster.RebalanceCoordinator`: nothing splits automatically; an operator
  (or a future policy) calls `split/4`. The seams (`:membership`, `:leader?`) keep it testable without a
  real lease or `ra`.
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

  @impl true
  def init(opts) do
    {:ok, %{membership: Keyword.fetch!(opts, :membership), leader?: Keyword.fetch!(opts, :leader?)}}
  end

  @impl true
  def handle_call({:split, new_vnode_id, token, nodes}, _from, state) do
    {:reply, VnodeSplit.split(state.membership, new_vnode_id, token, nodes, state.leader?), state}
  end
end
