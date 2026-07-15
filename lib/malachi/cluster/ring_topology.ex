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
  """

  alias Malachi.Cluster.HashRing

  @type placements :: %{term() => [node()]}
  @type t :: %__MODULE__{version: non_neg_integer(), ring: HashRing.t() | nil, placements: placements()}

  defstruct version: 0, ring: nil, placements: %{}

  @doc "The initial (version 0) topology for `ring` and its vnode `placements`."
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
  applies for a split (or any ring change). The monotonic version is what makes `merge/2` converge.
  """
  @spec advance(t(), HashRing.t(), placements()) :: t()
  def advance(%__MODULE__{version: version}, %HashRing{} = ring, placements) when is_map(placements) do
    %__MODULE__{version: version + 1, ring: ring, placements: placements}
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
