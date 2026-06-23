defmodule Malachi.Test.KeyspaceHelper do
  @moduledoc """
  Shared keyspace-coverage assertions for tests.
  """

  @doc """
  Whether `ranges` (sorted ascending by `:key_start`) tile `[position, keyspace_size)`
  contiguously, with no gaps or overlaps. Pass `position` as `0` to check full coverage.
  """
  @spec tiles?([%{key_start: non_neg_integer(), key_end: non_neg_integer()}], non_neg_integer(), non_neg_integer()) ::
          boolean()
  def tiles?([], position, keyspace_size), do: position == keyspace_size

  def tiles?([range | rest], position, keyspace_size) do
    range.key_start == position and tiles?(rest, range.key_end, keyspace_size)
  end
end
