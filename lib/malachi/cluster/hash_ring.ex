defmodule Malachi.Cluster.HashRing do
  @moduledoc """
  A consistent-hashing ring that maps metadata keys to the vnode that owns them — the
  foundation of NorthGuard's DS-RSM (Dynamically-Sharded Replicated State Machine).

  Each vnode sits at a `token` (a position) on a ring `[0, ring_size)`. A key is hashed
  with `:erlang.phash2/2` and routed to the first vnode whose token is `>= hash`, wrapping
  around past the largest token back to the smallest. So a vnode at token `T` owns the arc
  `(predecessor_token, T]` (with wraparound). Topic metadata is hashed by topic name, range
  and segment metadata by range id, which is how the DS-RSM shards metadata with minimal
  movement when vnodes are added or removed.

  The ring is a pure value: a `vnode_id => token` map is the source of truth, plus a
  derived list sorted by token for routing. Membership changes (rare) rebuild the sorted
  list; routing (frequent, over a modest number of vnodes) is a linear ceiling search,
  which is plenty fast for the ~hundreds of vnodes a cluster has.
  """

  import Bitwise

  @default_ring_bits 32
  @max_ring_bits 32

  @type vnode_id :: term()
  @type token :: non_neg_integer()

  @type t :: %__MODULE__{
          ring_size: pos_integer(),
          tokens: %{vnode_id() => token()},
          sorted: [{token(), vnode_id()}]
        }

  defstruct ring_size: 1 <<< @default_ring_bits, tokens: %{}, sorted: []

  @doc """
  Builds an empty ring over `[0, 2^ring_bits)`.

  ## Options
    * `:ring_bits` - ring spans `[0, 2^ring_bits)` (default `32`, max `32` because
      `:erlang.phash2/2` supports a range up to `2^32`).
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    ring_bits = Keyword.get(opts, :ring_bits, @default_ring_bits)

    if ring_bits < 1 or ring_bits > @max_ring_bits do
      raise ArgumentError, "ring_bits must be in 1..#{@max_ring_bits} (got #{inspect(ring_bits)})"
    end

    %__MODULE__{ring_size: 1 <<< ring_bits, tokens: %{}, sorted: []}
  end

  @doc """
  Places `vnode_id` at `token` on the ring.

  Fails with `{:error, :token_out_of_range}` if `token` is outside `[0, ring_size)`,
  `{:error, :token_taken}` if another vnode already holds it, or
  `{:error, :already_present}` if the vnode is already on the ring.
  """
  @spec add_vnode(t(), vnode_id(), token()) :: {:ok, t()} | {:error, atom()}
  def add_vnode(%__MODULE__{} = ring, vnode_id, token) do
    cond do
      token < 0 or token >= ring.ring_size -> {:error, :token_out_of_range}
      Map.has_key?(ring.tokens, vnode_id) -> {:error, :already_present}
      token_taken?(ring, token) -> {:error, :token_taken}
      true -> {:ok, rebuild(%{ring | tokens: Map.put(ring.tokens, vnode_id, token)})}
    end
  end

  @doc "Removes `vnode_id` from the ring. `{:error, :not_found}` if it is not present."
  @spec remove_vnode(t(), vnode_id()) :: {:ok, t()} | {:error, :not_found}
  def remove_vnode(%__MODULE__{} = ring, vnode_id) do
    if Map.has_key?(ring.tokens, vnode_id) do
      {:ok, rebuild(%{ring | tokens: Map.delete(ring.tokens, vnode_id)})}
    else
      {:error, :not_found}
    end
  end

  @doc "Routes `key` to the vnode that owns it. `{:error, :empty}` if the ring has no vnodes."
  @spec route(t(), term()) :: {:ok, vnode_id()} | {:error, :empty}
  def route(%__MODULE__{sorted: []}, _key), do: {:error, :empty}

  def route(%__MODULE__{} = ring, key) do
    hash = :erlang.phash2(key, ring.ring_size)
    {:ok, owner_for_hash(ring.sorted, hash)}
  end

  @doc """
  The arc `{start_exclusive, end_inclusive}` that `vnode_id` owns (with wraparound). When
  `start == end` the vnode is the only one on the ring and owns the whole ring.
  `{:error, :not_found}` if the vnode is not present.
  """
  @spec boundaries(t(), vnode_id()) :: {:ok, {token(), token()}} | {:error, :not_found}
  def boundaries(%__MODULE__{} = ring, vnode_id) do
    case Map.fetch(ring.tokens, vnode_id) do
      :error -> {:error, :not_found}
      {:ok, token} -> {:ok, {predecessor_token(ring.sorted, token), token}}
    end
  end

  @doc "The ids of the vnodes on the ring."
  @spec vnode_ids(t()) :: [vnode_id()]
  def vnode_ids(%__MODULE__{} = ring), do: Map.keys(ring.tokens)

  @doc "The number of vnodes on the ring."
  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{} = ring), do: map_size(ring.tokens)

  # --- internals ---

  defp token_taken?(ring, token), do: Enum.any?(ring.tokens, fn {_id, t} -> t == token end)

  defp rebuild(ring) do
    sorted = ring.tokens |> Enum.map(fn {id, token} -> {token, id} end) |> Enum.sort()
    %{ring | sorted: sorted}
  end

  # First vnode with token >= hash; wraps to the smallest token if hash is past the last.
  defp owner_for_hash(sorted, hash) do
    case Enum.find(sorted, fn {token, _id} -> token >= hash end) do
      {_token, vnode_id} -> vnode_id
      nil -> sorted |> hd() |> elem(1)
    end
  end

  # The token immediately before `token` on the ring (the largest token < `token`); wraps
  # to the largest token overall when `token` is the smallest. Equals `token` itself when
  # it is the only vnode.
  defp predecessor_token(sorted, token) do
    tokens = Enum.map(sorted, fn {ring_token, _id} -> ring_token end)

    case Enum.filter(tokens, &(&1 < token)) do
      [] -> List.last(tokens)
      smaller -> List.last(smaller)
    end
  end
end
