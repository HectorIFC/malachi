defmodule Malachi.Broker.Skip do
  @moduledoc """
  One stretch of a range's history that a consume read moved a consumer past because it was no longer
  stored: expired by retention, or removed by an operator. `Malachi.Broker.read_consume/5` returns one
  per stretch it stepped over, so the layers above can tell an operator which reader lost data.

  The fields:

    * `range_id` - the range being consumed.
    * `source_range_id` - the range whose data was missing: the consumed range itself, or one of its
      sealed ancestors when the read was still draining the pre-split history.
    * `from` - the source offset the reader was positioned at when the data was found missing.
    * `offsets` - how many offsets were stepped over, or `:unknown` when an ancestor has no segment left
      and its end was not recovered after a restart, so only the fact of the skip is known.
    * `origin` - `:start` when the reader began without a position (a new group, or a child range after
      a split, which starts over its ancestors), `:cursor` when it resumed from one it held. Only the
      second is a reader that fell behind retention.
    * `source` - `:self` or `:ancestor`.

  The count comes from segment boundaries, never from the gaps between record offsets, so a segment
  whose offsets are sparse by design is not reported. It counts OFFSETS, not records lost: reading an
  ancestor delivers only the child's key slice of it, so an ancestor skip is an upper bound on what this
  child missed (`span/1` says `:upper_bound`).
  """

  @enforce_keys [:range_id, :source_range_id, :from, :offsets, :origin, :source]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          range_id: Malachi.Metadata.range_id(),
          source_range_id: Malachi.Metadata.range_id(),
          from: non_neg_integer(),
          offsets: pos_integer() | :unknown,
          origin: :start | :cursor,
          source: :self | :ancestor
        }

  @typedoc "How far `offsets` can be trusted as a count of what this reader missed."
  @type span :: :exact | :upper_bound | :unknown

  @doc """
  `:unknown` when the size of the skip is not known, `:upper_bound` for a skip over an ancestor (the
  reader would only have received its own key slice of it), `:exact` otherwise.
  """
  @spec span(t()) :: span()
  def span(%__MODULE__{offsets: :unknown}), do: :unknown
  def span(%__MODULE__{source: :ancestor}), do: :upper_bound
  def span(%__MODULE__{source: :self}), do: :exact
end
