defmodule Malachi.Broker.ReadView do
  @moduledoc """
  The slice of a `Malachi.Broker` that reading a set of ranges needs, and nothing else: each range's
  metadata, the segments of the range and of every ancestor it reads history from, where each of those
  ends on this frontend, and whether the range still waits for a recovery.

  It exists so a read can run outside the process that owns the broker. A `Malachi.Broker` carries the
  metadata cache of every vnode, all of which a read of a few ranges never looks at; a view carries
  only the ranges read, their ancestors' segments and their ends. Its size grows with the segments those
  ranges still retain, not with the cluster. `Malachi.BrokerServer` builds one for each set of ranges a
  batch of pushes reads and hands it to the subscribers' own processes, which read and write their
  sockets there, so a cold read no longer runs inside the loop that serializes appends. The broker's own
  reads take the same path through a view, so there is one read implementation whoever runs it.

  A view is a snapshot. A segment that seals, is deleted, or a range that grows after it was taken is
  not in it, and reads through it are bounded by what it holds: the end it holds for a range is the
  horizon, including for an active segment. A segment it lists that has since been deleted answers a
  read with nothing, and `Malachi.Broker` fails that read rather than taking it for the end of the data,
  so a stale view costs a retry, never a skip.
  """

  alias Malachi.Broker
  alias Malachi.Cluster.DSRSM
  alias Malachi.Metadata

  @type t :: %__MODULE__{
          ranges: %{Metadata.range_id() => Metadata.range_meta()},
          segments: %{Metadata.range_id() => [Metadata.segment_meta()]},
          ends: %{Metadata.range_id() => non_neg_integer()},
          unrecovered: MapSet.t(Metadata.range_id())
        }

  defstruct ranges: %{}, segments: %{}, ends: %{}, unrecovered: MapSet.new()

  @doc """
  The view of `range_ids` from `broker`: each range the control plane knows, plus the segments and the
  end of every range a read of them can touch (the range itself and its ancestors). A range the control
  plane does not know is left out, and reads of it answer `:no_such_range`.
  """
  @spec new(Broker.t(), [Metadata.range_id()]) :: t()
  def new(%Broker{} = broker, range_ids) do
    ranges =
      for range_id <- Enum.uniq(range_ids),
          range = DSRSM.get_range(broker.dsrsm, topic_of(range_id), range_id),
          into: %{},
          do: {range_id, range}

    touched = ranges |> Enum.flat_map(fn {range_id, range} -> [range_id | range.parents] end) |> Enum.uniq()

    %__MODULE__{
      ranges: ranges,
      segments: Map.new(touched, &{&1, DSRSM.segments_of_range(broker.dsrsm, topic_of(&1), &1)}),
      ends: Map.take(broker.offsets, touched),
      unrecovered: touched |> Enum.filter(&Broker.unrecovered?(broker, &1)) |> MapSet.new()
    }
  end

  @doc "The metadata of `range_id`, or `nil` if the view does not hold it."
  @spec range(t(), Metadata.range_id()) :: Metadata.range_meta() | nil
  def range(%__MODULE__{ranges: ranges}, range_id), do: Map.get(ranges, range_id)

  @doc "The segments of `range_id` (the range or one of its ancestors), `[]` if it has none or is unknown."
  @spec segments(t(), Metadata.range_id()) :: [Metadata.segment_meta()]
  def segments(%__MODULE__{segments: segments}, range_id), do: Map.get(segments, range_id, [])

  @doc "Where `range_id` ends on the frontend the view came from (its next offset), or `:error` if unknown."
  @spec fetch_end(t(), Metadata.range_id()) :: {:ok, non_neg_integer()} | :error
  def fetch_end(%__MODULE__{ends: ends}, range_id), do: Map.fetch(ends, range_id)

  @doc "Whether `range_id` waits for a recovery that learns its end (see `Malachi.Broker.unrecovered?/2`)."
  @spec unrecovered?(t(), Metadata.range_id()) :: boolean()
  def unrecovered?(%__MODULE__{unrecovered: unrecovered}, range_id), do: MapSet.member?(unrecovered, range_id)

  defp topic_of(range_id), do: elem(range_id, 0)
end
