defmodule Malachi.Cluster.RingTopology do
  @moduledoc """
  The cluster's routing topology as a **versioned, gossip-disseminable** value: a `HashRing` (which vnode
  owns which hash arc) plus each vnode's node placement (`%{vnode_id => [node]}`), tagged with a monotonic
  `version`.

  This is how a vnode **split** propagates. NorthGuard keeps *"very minimal global states"* and spreads
  them over its SWIM **dissemination** path (the meetup transcript: *"we also use this dissemination for
  spreading some minimal global like cluster metadata"*); the ring is exactly such minimal global state.
  So rather than a dedicated Raft cluster for the ring (strongly consistent but heavier and less faithful),
  the ring rides the existing gossip and every node converges on the latest version.

  Convergence is a CRDT join: `merge/2` keeps the **higher version** (last-version-wins), with a
  deterministic term-order tiebreak on the rare same-version clash — so `merge` is commutative,
  associative and idempotent, and every node reaches the same topology regardless of gossip order. Only
  the rebalancing leader `advance/3`s the version (one writer), so a same-version clash should not arise
  in normal operation; the tiebreak just keeps the math total.

  The topology also carries an optional **pending-split intent** (`begin_split/4`): the durable record of a
  split that is migrating but not yet complete. It rides the same gossip, so if the coordinator driving a
  split crashes, whichever node takes over the lease reads the intent and reconciles the interrupted split
  (see `t:pending/0`). `advance/3` (complete) and `clear_pending/1` (abort) both clear it.
  """

  alias Malachi.Cluster.HashRing

  @type placements :: %{term() => [node()]}

  @typedoc """
  A **split in flight**: the durable intent recorded before a split migrates, so a coordinator that takes
  over the lease after the previous one crashed mid-split can reconcile it (`nil` when none is pending).
  Carries what a reconciler needs to identify and undo the split — the new vnode, its ring `token`, and its
  placement `nodes` (the old ring/placements are the topology's own, since a pending split has *not* yet
  advanced the ring).
  """
  @type pending :: %{new_vnode: HashRing.vnode_id(), token: HashRing.token(), nodes: [node()]} | nil

  @type t :: %__MODULE__{
          version: non_neg_integer(),
          ring: HashRing.t() | nil,
          placements: placements(),
          pending: pending()
        }

  defstruct version: 0, ring: nil, placements: %{}, pending: nil

  @doc "The initial (version 0) topology for `ring` and its vnode `placements`; no split pending."
  @spec new(HashRing.t(), placements()) :: t()
  def new(%HashRing{} = ring, placements) when is_map(placements) do
    %__MODULE__{version: 0, ring: ring, placements: placements}
  end

  @doc """
  The `%{vnode_id => server_id}` routing map derived from the placements: each vnode's server id is
  `{vnode_id, a_member}` (any member of its placement — ra resolves the live leader). A vnode with an
  empty placement is skipped (it has no cluster to route to). Used to point routing at the topology.
  """
  @spec servers(t()) :: %{term() => {term(), node()}}
  def servers(%__MODULE__{placements: placements}) do
    for {vnode_id, [member | _]} <- placements, into: %{}, do: {vnode_id, {vnode_id, member}}
  end

  @doc """
  A new topology at `version + 1` with `ring`/`placements` — the single-writer bump the rebalancing leader
  applies to **complete** a split (or any ring change). The monotonic version is what makes `merge/2`
  converge. Completing a ring change also **clears** any pending-split intent: the split it recorded is now
  reflected in the ring, so there is nothing left to reconcile.
  """
  @spec advance(t(), HashRing.t(), placements()) :: t()
  def advance(%__MODULE__{version: version}, %HashRing{} = ring, placements) when is_map(placements) do
    %__MODULE__{version: version + 1, ring: ring, placements: placements, pending: nil}
  end

  @doc """
  Records a **pending split** at `version + 1` **without changing the ring**: the intent that a split of
  `new_vnode_id` at `token` onto `nodes` is now in flight. Written (and gossiped) *before* the migration so
  a coordinator that takes over the lease mid-split can find and reconcile it. The ring still routes by the
  pre-split placement until `advance/3` completes the split (or `clear_pending/1` aborts it).
  """
  @spec begin_split(t(), HashRing.vnode_id(), HashRing.token(), [node()]) :: t()
  def begin_split(%__MODULE__{} = topology, new_vnode_id, token, nodes) when is_list(nodes) do
    %{topology | version: topology.version + 1, pending: %{new_vnode: new_vnode_id, token: token, nodes: nodes}}
  end

  @doc """
  Drops a pending-split intent at `version + 1`, **keeping the ring unchanged** — how an **aborted** split
  is published (the reconciler rolled the migration back, so the ring stays as it was and the intent is
  gone). Contrast `advance/3`, which clears the intent by moving the ring *forward* to the completed split.
  """
  @spec clear_pending(t()) :: t()
  def clear_pending(%__MODULE__{} = topology) do
    %{topology | version: topology.version + 1, pending: nil}
  end

  @doc """
  CRDT join of two observations of the topology: the higher `version` wins; a same-version clash is broken
  deterministically by Erlang term order (so the result is independent of argument order). Idempotent
  (`merge(a, a) == a`), commutative and associative.
  """
  @spec merge(t(), t()) :: t()
  def merge(%__MODULE__{} = a, %__MODULE__{} = b) do
    cond do
      b.version > a.version -> b
      a.version > b.version -> a
      # same version (rare, since only the leader advances): a deterministic total order on the serialized
      # value keeps the join order-independent — comparing the binaries, not the structs (which would be a
      # meaningless structural comparison)
      :erlang.term_to_binary(b) > :erlang.term_to_binary(a) -> b
      true -> a
    end
  end
end
