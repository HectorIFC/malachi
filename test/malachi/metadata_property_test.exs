defmodule Malachi.MetadataPropertyTest do
  @moduledoc """
  Model-based property tests for the metadata state machine: generate random sequences of
  operations, interpret them against the current state (always picking *valid* targets), and
  assert the structural invariants hold and that the machine is deterministic. This stands in
  for NorthGuard's deterministic-simulation testing of the control plane.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Keyspace
  alias Malachi.Metadata
  alias Malachi.Test.KeyspaceHelper

  @keyspace_bits 4
  @topic_pool ["a", "b", "c"]

  property "structural invariants hold after any sequence of operations" do
    check all(ops <- StreamData.list_of(op(), max_length: 30), max_runs: 200) do
      state = run(ops)
      assert topics_cover_keyspace?(state), "active ranges must tile the keyspace per active topic"
      assert ranges_reference_topics?(state), "every range must belong to an existing topic"
      assert segments_reference_ranges?(state), "every segment must belong to an existing range"
      assert range_ids_well_formed?(state), "every range id must be {topic, seq} of its topic"
    end
  end

  property "the machine is deterministic (same ops -> same state)" do
    check all(ops <- StreamData.list_of(op(), max_length: 30), max_runs: 100) do
      assert run(ops) == run(ops)
    end
  end

  # The reverse indexes (topic_ranges/range_segments) must stay exactly equal to what a scan of the
  # source-of-truth maps would produce — otherwise a future O(1) lookup over them would lie. Compares the
  # maintained index against one rebuilt from `ranges`/`segments`, which catches missing, extra, stale,
  # and lingering-empty entries at once. (V-idx-b then switches the readers to use the index.)
  property "the secondary indexes equal a scan-derived index after any sequence of operations" do
    check all(ops <- StreamData.list_of(op(), max_length: 40), max_runs: 200) do
      state = run(ops)

      expected_topic_ranges =
        state.ranges
        |> Enum.group_by(fn {_id, range} -> range.topic end, fn {id, _range} -> id end)
        |> Map.new(fn {topic, ids} -> {topic, MapSet.new(ids)} end)

      expected_range_segments =
        state.segments
        |> Enum.group_by(fn {_id, seg} -> seg.range_id end, fn {id, _seg} -> id end)
        |> Map.new(fn {range_id, ids} -> {range_id, MapSet.new(ids)} end)

      assert state.topic_ranges == expected_topic_ranges
      assert state.range_segments == expected_range_segments
    end
  end

  # --- op generator ---

  defp op do
    StreamData.one_of([
      StreamData.tuple({StreamData.constant(:create_topic), StreamData.member_of(@topic_pool)}),
      StreamData.tuple({StreamData.constant(:seal_topic), StreamData.member_of(@topic_pool)}),
      StreamData.tuple({StreamData.constant(:delete_topic), StreamData.member_of(@topic_pool)}),
      StreamData.tuple({StreamData.constant(:split), StreamData.positive_integer()}),
      StreamData.tuple({StreamData.constant(:merge), StreamData.positive_integer()}),
      StreamData.tuple({StreamData.constant(:register), StreamData.positive_integer(), StreamData.positive_integer()}),
      StreamData.tuple({StreamData.constant(:seal_seg), StreamData.positive_integer()}),
      StreamData.tuple({StreamData.constant(:delete_seg), StreamData.positive_integer()})
    ])
  end

  # --- interpreter (deterministic: targets chosen from current state) ---

  defp run(ops), do: Enum.reduce(ops, Metadata.new(), &apply_op(&2, &1))

  defp apply_op(state, {:create_topic, name}), do: command(state, {:create_topic, name, @keyspace_bits})
  defp apply_op(state, {:seal_topic, name}), do: command(state, {:seal_topic, name})
  defp apply_op(state, {:delete_topic, name}), do: command(state, {:delete_topic, name})

  defp apply_op(state, {:split, picker}) do
    case pick_active_range(state, picker) do
      nil -> state
      range_id -> command(state, {:split_range, range_id})
    end
  end

  defp apply_op(state, {:merge, picker}) do
    case pick_buddy_pair(state, picker) do
      nil -> state
      {a, b} -> command(state, {:merge_ranges, a, b})
    end
  end

  defp apply_op(state, {:register, picker, seg_seed}) do
    case pick_active_range(state, picker) do
      nil -> state
      range_id -> command(state, {:register_segment, range_id, {:seg, seg_seed}, [:b1], 0})
    end
  end

  defp apply_op(state, {:seal_seg, picker}) do
    case pick_segment(state, picker, :active) do
      nil -> state
      id -> command(state, {:seal_segment, id, 1, 100, 1_700_000_000_000})
    end
  end

  defp apply_op(state, {:delete_seg, picker}) do
    case pick_segment(state, picker, :sealed) do
      nil -> state
      id -> command(state, {:delete_segment, id})
    end
  end

  defp command(state, command), do: state |> Metadata.apply(command) |> elem(0)

  defp pick_segment(state, picker, seg_state) do
    case for {id, s} <- state.segments, s.state == seg_state, do: id do
      [] -> nil
      ids -> Enum.at(Enum.sort(ids), rem(picker, length(ids)))
    end
  end

  defp pick_active_range(state, picker) do
    case active_range_ids(state) do
      [] -> nil
      ids -> Enum.at(ids, rem(picker, length(ids)))
    end
  end

  defp pick_buddy_pair(state, picker) do
    active = state.ranges |> Map.values() |> Enum.filter(&(&1.state == :active)) |> Enum.sort_by(& &1.id)

    pairs =
      for a <- active, b <- active, a.id < b.id, buddies?(a, b), do: {a.id, b.id}

    case pairs do
      [] -> nil
      _ -> Enum.at(pairs, rem(picker, length(pairs)))
    end
  end

  defp buddies?(a, b) do
    a.topic == b.topic and a.keyspace_size == b.keyspace_size and
      Keyspace.buddies?(a.key_start, a.key_end, b.key_start, b.key_end)
  end

  defp active_range_ids(state) do
    for {id, range} <- state.ranges, range.state == :active, do: id
  end

  # --- invariants ---

  defp topics_cover_keyspace?(state) do
    Enum.all?(state.topics, fn {name, topic} ->
      topic.state == :sealed or
        state
        |> Metadata.active_ranges_of_topic(name)
        |> Enum.sort_by(& &1.key_start)
        |> KeyspaceHelper.tiles?(0, topic.keyspace_size)
    end)
  end

  defp ranges_reference_topics?(state) do
    Enum.all?(state.ranges, fn {_id, range} -> Map.has_key?(state.topics, range.topic) end)
  end

  defp segments_reference_ranges?(state) do
    Enum.all?(state.segments, fn {_id, segment} -> Map.has_key?(state.ranges, segment.range_id) end)
  end

  defp range_ids_well_formed?(state) do
    Enum.all?(state.ranges, fn {{topic, seq}, range} ->
      range.topic == topic and is_integer(seq) and seq >= 0
    end)
  end
end
