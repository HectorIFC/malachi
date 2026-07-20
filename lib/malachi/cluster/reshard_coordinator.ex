defmodule Malachi.Cluster.ReshardCoordinator do
  @moduledoc """
  Drives a **grow re-sharding** under the lease: takes the ring from its current vnode count up to a larger
  target, one `Malachi.Cluster.SplitCoordinator` split at a time. A `GenServer` so reshards **serialize** —
  each step mutates the ring, and two reshards at once would race it (the same reason splits serialize).

  **Level-triggered and resumable.** Rather than executing a plan computed once, each iteration re-reads the
  **live** ring, asks `Malachi.Cluster.ReshardPlan` for the steps that remain, and performs only the **first**
  one. Because the plan is a pure function of `(ring, target)` and split-natural steps never move an existing
  token, an interrupted reshard converges simply by re-issuing the same target: the splits already done are
  reflected in the ring, so the remaining plan is exactly what is left. Nothing extra is persisted — the
  target is the operator's input (there is no durable reshard intent; a single split's own `pending` intent
  already makes each step crash-safe).

  Operator-driven, like `SplitCoordinator`/`RebalanceCoordinator`: nothing reshards automatically. The seams
  (`:ring`, `:split`, `:placement`, `:leader?`) keep it testable without a real lease, membership or `ra`.
  """

  use GenServer

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.ReshardPlan

  @type option ::
          {:name, GenServer.name()}
          | {:ring, (-> HashRing.t() | nil)}
          | {:split, (term(), non_neg_integer(), [node()] -> :ok | {:error, term()})}
          | {:placement, (term() -> [node()])}
          | {:leader?, (-> boolean())}
          | {:prefix, String.t()}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    {gen_server_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @doc """
  Grows the ring to `target_count` vnodes, performing the required splits one at a time. Returns `:ok` when
  the ring is at (or reaches) the target, `{:error, :not_leader}` unless this node holds the lease,
  `{:error, :no_topology}` if the cluster has no ring yet, `{:error, :cannot_shrink}` for a smaller target
  (grow-only), or `{:error, {:split_failed, vnode_id, reason}}` if a step fails — earlier steps stay applied
  and re-issuing the same target resumes from there.

  Serialized with other reshards and may take a long time (each step migrates metadata), so it uses an
  infinite call timeout.
  """
  @spec reshard(GenServer.server(), pos_integer()) :: :ok | {:error, term()}
  def reshard(server, target_count) do
    GenServer.call(server, {:reshard, target_count}, :infinity)
  end

  @impl true
  def init(opts) do
    state = %{
      ring: Keyword.fetch!(opts, :ring),
      split: Keyword.fetch!(opts, :split),
      placement: Keyword.fetch!(opts, :placement),
      leader?: Keyword.fetch!(opts, :leader?),
      prefix: Keyword.get(opts, :prefix, "vn")
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:reshard, target_count}, _from, state) do
    reply = if state.leader?.(), do: grow(state, target_count), else: {:error, :not_leader}
    {:reply, reply, state}
  end

  # One level-triggered pass: read the live ring, plan what remains, do the first step, recurse.
  #
  # Re-planning the whole remainder each pass and using only its head is deliberate — it is exactly what
  # makes an interrupted reshard resumable (the plan is re-derived from the live ring, never replayed from a
  # stale one). The discarded tail costs O(steps x vnodes) of pure ring math per pass, which is immaterial at
  # realistic vnode counts and reshards being rare operator actions.
  defp grow(state, target_count) do
    with {:ok, ring} <- current_ring(state),
         {:ok, steps} <- ReshardPlan.plan(ring, target_count, prefix: state.prefix) do
      case steps do
        [] -> :ok
        [step | _rest] -> execute(state, step, target_count, HashRing.size(ring))
      end
    end
  end

  defp execute(state, %{new_vnode_id: vnode_id, token: token}, target_count, size_before) do
    # An empty placement (no live node to host the new vnode) would otherwise fail deep inside ra when its
    # cluster is started with no members; refuse up front with a reason that names the cause.
    case state.placement.(vnode_id) do
      [] ->
        {:error, {:no_placement, vnode_id}}

      nodes ->
        case state.split.(vnode_id, token, nodes) do
          :ok -> continue(state, target_count, size_before)
          {:error, reason} -> {:error, {:split_failed, vnode_id, reason}}
        end
    end
  end

  # Guard against looping forever: a split that reports success but whose ring is not visible to us yet
  # would otherwise re-plan the same step endlessly. Progress is "the ring actually grew".
  defp continue(state, target_count, size_before) do
    with {:ok, ring} <- current_ring(state) do
      if HashRing.size(ring) > size_before do
        grow(state, target_count)
      else
        {:error, :ring_did_not_advance}
      end
    end
  end

  defp current_ring(state) do
    case state.ring.() do
      %HashRing{} = ring -> {:ok, ring}
      _absent -> {:error, :no_topology}
    end
  end
end
