defmodule Malachi.Loadtest.ReadLanes do
  @moduledoc """
  The ranges a reading client of the load generator reads, one lane each, kept in step with a topic's
  routes: the consumer (`Malachi.Loadtest.Consumer`) and the fetcher (`Malachi.Loadtest.Fetcher`) share it.

  A lane is a map with at least `:block` (its range's keyspace block, `{key_start, key_end}`) and `:start`
  (where the next read of the range starts: `:earliest`, `{:position, position}`, or a marker of the
  client's own, the consumer's `:stopped`). When the routes no longer hold a range, its lane is retired and
  kept, with its start; a range the routes name with no lane yet gets one starting where its nearest retired
  ancestor stopped, or else from the start of its history, so a record is read at least once and never lost
  (`Malachi.Wire`).

  The retired ranges of the topic whose block holds a new range's are its ancestors, and the nearest is the
  one the topic made last. An ancestor's history comes, in order, within the new range's (a split extends
  its parent's, a merge lists the first buddy's ancestors, then the second's not listed yet, then both), and
  what the new range lists before it in its own history holds no record of its block. So a position read
  in an ancestor names, in the new range, the same record or one at or before it: nothing past what was read
  is skipped, and at most some records are read again. A split's child goes on from where its parent was
  read, and so does a merge back into a block read before it was split, reading the children's records
  again; a merge of two ranges first read as they are starts from the start of its history, since neither
  buddy's block holds the merged one. A start that is a marker carries over the same way: a range made of a
  stopped one, by a split or by a merge back into exactly its block, is stopped too.
  """

  @type key :: {String.t(), non_neg_integer()}
  @type lane :: %{
          required(:block) => {non_neg_integer(), pos_integer()},
          required(:start) => term(),
          optional(atom()) => term()
        }

  @doc """
  `lanes` and `retired` (range key to retired lane) brought in step with `ranges`, `topic`'s active ranges: lanes
  of ranges they no longer hold retired when `retirable?` says so (a consumer keeps one whose stream is
  still open until it is told it moved), and a lane made with `new_lane.(block, start)` for each range with
  none.
  """
  @spec reconcile(%{key() => lane()}, map(), String.t(), [map()], (lane() -> boolean()), (tuple(), term() -> lane())) ::
          {%{key() => lane()}, map()}
  def reconcile(lanes, retired, topic, ranges, retirable?, new_lane) do
    live = MapSet.new(ranges, & &1.range)

    {gone, kept} =
      Map.split_with(lanes, fn {{lane_topic, range}, lane} ->
        lane_topic == topic and not MapSet.member?(live, range) and retirable?.(lane)
      end)

    retired = Map.merge(retired, gone)

    lanes =
      Enum.reduce(ranges, kept, fn range, lanes ->
        key = {topic, range.range}
        block = {range.key_start, range.key_end}

        if Map.has_key?(lanes, key),
          do: lanes,
          else: Map.put(lanes, key, new_lane.(block, start_for(retired, topic, block)))
      end)

    {lanes, retired}
  end

  # Where a range of `topic` whose block is `{key_start, key_end}` starts: where its nearest retired ancestor
  # stopped, or the start of its history. Of the retired ranges of the topic whose block holds its own (all
  # of them its ancestors), the nearest is the one the topic made last: a topic numbers its ranges in the
  # order it makes them (`Malachi.Metadata`), so the highest number, whatever order the map is in and
  # whenever each retired. Another topic's ranges cover the same keyspace and say nothing about this one's.
  defp start_for(retired, topic, {key_start, key_end}) do
    ancestors =
      for {{^topic, range}, %{block: {parent_start, parent_end}} = lane} <- retired,
          parent_start <= key_start and key_end <= parent_end,
          do: {range, lane.start}

    case ancestors do
      [] -> :earliest
      _ -> ancestors |> Enum.max_by(&elem(&1, 0)) |> elem(1)
    end
  end
end
