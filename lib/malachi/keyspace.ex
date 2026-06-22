defmodule Malachi.Keyspace do
  @moduledoc """
  Shared keyspace arithmetic: hashing keys into a power-of-two space `[0, 2^bits)` and the
  buddy-allocator block math NorthGuard's ranges use.

  Both the data plane (`Malachi.Range`, which routes record keys to keyspace slices) and
  the control plane (`Malachi.Cluster.HashRing`/DS-RSM, which routes metadata keys to
  vnodes) hash keys the same way and share the same `1..32` bound (because
  `:erlang.phash2/2` supports a range up to `2^32`). Centralizing it here keeps that rule —
  and the buddy split/merge math — in one place.
  """

  import Bitwise

  # :erlang.phash2/2 supports a range of 1..2^32.
  @max_bits 32

  @doc """
  Validates `bits` is in `1..32` and returns the keyspace size `2^bits`. Raises
  `ArgumentError` (mentioning `label`) otherwise.
  """
  @spec size_for_bits!(integer(), String.t()) :: pos_integer()
  def size_for_bits!(bits, label \\ "bits") do
    if bits < 1 or bits > @max_bits do
      raise ArgumentError,
            "#{label} must be in 1..#{@max_bits} (got #{inspect(bits)}); " <>
              ":erlang.phash2/2 supports a range up to 2^32"
    end

    1 <<< bits
  end

  @doc "Hashes `key` into `[0, size)`."
  @spec position_of(term(), pos_integer()) :: non_neg_integer()
  def position_of(key, size), do: :erlang.phash2(key, size)

  @doc "Whether `position` falls in the block `[block_start, block_end)`."
  @spec within?(non_neg_integer(), non_neg_integer(), non_neg_integer()) :: boolean()
  def within?(position, block_start, block_end),
    do: position >= block_start and position < block_end

  @doc "Whether the block `[block_start, block_end)` is large enough to split (size >= 2)."
  @spec splittable?(non_neg_integer(), non_neg_integer()) :: boolean()
  def splittable?(block_start, block_end), do: block_end - block_start >= 2

  @doc "The midpoint that splits a power-of-two block `[block_start, block_end)` in half."
  @spec split_point(non_neg_integer(), non_neg_integer()) :: non_neg_integer()
  def split_point(block_start, block_end), do: block_start + div(block_end - block_start, 2)

  @doc """
  Whether two equal-size, power-of-two-aligned blocks are buddies — the two halves of a
  common parent block (the buddy-allocator rule `start XOR size`).
  """
  @spec buddies?(non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()) ::
          boolean()
  def buddies?(start_a, end_a, start_b, end_b) do
    size_a = end_a - start_a
    size_b = end_b - start_b
    size_a == size_b and bxor(start_a, size_a) == start_b
  end
end
