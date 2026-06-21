defmodule Malachi.TopicTest do
  use ExUnit.Case, async: true

  alias Malachi.Log.Record
  alias Malachi.Topic

  @moduletag :tmp_dir

  defp record(value, key), do: Record.new(value, key: key)

  # Small keyspace (0..15) so buddy splits are easy to reason about.
  defp create(directory), do: Topic.create(directory, "events", keyspace_bits: 4)

  defp only_active_range_id(topic) do
    [range_id] = Topic.active_range_ids(topic)
    range_id
  end

  # Reads a range fully by paging across segments.
  defp read_all(topic, range_id) do
    read_all(topic, range_id, 0, [])
  end

  defp read_all(topic, range_id, offset, accumulated) do
    case Topic.read(topic, range_id, offset, 100) do
      :eof -> accumulated |> Enum.reverse() |> List.flatten()
      {:ok, records} -> read_all(topic, range_id, offset + length(records), [records | accumulated])
    end
  end

  describe "create" do
    test "starts with one root range covering the whole keyspace", %{tmp_dir: directory} do
      {:ok, topic} = create(directory)
      assert topic.keyspace_size == 16
      assert length(Topic.active_range_ids(topic)) == 1
      assert Topic.keyspace_covered?(topic)
    end
  end

  describe "append / route / read" do
    test "appends routed records and reads them back from the owning range",
         %{tmp_dir: directory} do
      {:ok, topic} = create(directory)
      records = for index <- 0..9, do: record("v#{index}", "k#{index}")

      {:ok, topic, placements} = Topic.append(topic, records)
      {:ok, topic} = Topic.sync(topic)

      root_id = only_active_range_id(topic)
      # single range, so all records land there with contiguous offsets
      assert placements == %{root_id => {0, 9}}
      assert topic |> read_all(root_id) |> Enum.map(& &1.value) == Enum.map(records, & &1.value)
    end

    test "appending an empty list is a no-op", %{tmp_dir: directory} do
      {:ok, topic} = create(directory)
      assert {:ok, _topic, placements} = Topic.append(topic, [])
      assert placements == %{}
    end

    test "route returns the active range owning a key", %{tmp_dir: directory} do
      {:ok, topic} = create(directory)
      assert {:ok, range} = Topic.route(topic, "anything")
      assert range.key_start == 0 and range.key_end == 16
    end
  end

  describe "split_range" do
    test "keeps keyspace coverage and routes records to both halves", %{tmp_dir: directory} do
      {:ok, topic} = create(directory)
      root_id = only_active_range_id(topic)
      {:ok, topic, left_id, right_id} = Topic.split_range(topic, root_id)

      assert Topic.keyspace_covered?(topic)
      assert Enum.sort(Topic.active_range_ids(topic)) == Enum.sort([left_id, right_id])

      records = for index <- 0..29, do: record("v#{index}", "k#{index}")
      {:ok, topic, _placements} = Topic.append(topic, records)
      {:ok, topic} = Topic.sync(topic)

      left_values = topic |> read_all(left_id) |> Enum.map(& &1.value)
      right_values = topic |> read_all(right_id) |> Enum.map(& &1.value)

      # every record landed in exactly one half; together they reconstruct the input
      assert Enum.sort(left_values ++ right_values) == Enum.sort(Enum.map(records, & &1.value))
      refute left_values == []
      refute right_values == []

      # and each record routes to the range that actually stored it
      for one_record <- records do
        {:ok, range} = Topic.route(topic, one_record.key)
        assert range.id in [left_id, right_id]
      end
    end

    test "split is logical: the sealed parent keeps its records", %{tmp_dir: directory} do
      {:ok, topic} = create(directory)
      root_id = only_active_range_id(topic)
      records = for index <- 0..4, do: record("v#{index}", "k#{index}")
      {:ok, topic, _placements} = Topic.append(topic, records)
      {:ok, topic} = Topic.sync(topic)

      {:ok, topic, left_id, right_id} = Topic.split_range(topic, root_id)

      # the parent's records are still readable through its (now sealed) id
      assert topic |> read_all(root_id) |> Enum.map(& &1.value) == Enum.map(records, & &1.value)
      # the fresh children start empty
      assert Topic.read(topic, left_id, 0, 10) == :eof
      assert Topic.read(topic, right_id, 0, 10) == :eof
    end

    test "splitting an unknown range fails", %{tmp_dir: directory} do
      {:ok, topic} = create(directory)
      assert Topic.split_range(topic, "nope") == {:error, :no_such_range}
    end
  end

  describe "merge_ranges" do
    test "merges buddies back into one range covering their union", %{tmp_dir: directory} do
      {:ok, topic} = create(directory)
      root_id = only_active_range_id(topic)
      {:ok, topic, left_id, right_id} = Topic.split_range(topic, root_id)

      {:ok, topic, child_id} = Topic.merge_ranges(topic, left_id, right_id)

      assert Topic.active_range_ids(topic) == [child_id]
      assert Topic.keyspace_covered?(topic)
      {:ok, child} = Topic.route(topic, "anything")
      assert {child.key_start, child.key_end} == {0, 16}
    end

    test "merging non-buddies is rejected", %{tmp_dir: directory} do
      {:ok, topic} = create(directory)
      root_id = only_active_range_id(topic)
      {:ok, topic, left_id, right_id} = Topic.split_range(topic, root_id)
      {:ok, topic, left_left_id, _left_right_id} = Topic.split_range(topic, left_id)

      assert Topic.merge_ranges(topic, left_left_id, right_id) == {:error, :not_buddies}
    end

    test "merging an unknown range fails", %{tmp_dir: directory} do
      {:ok, topic} = create(directory)
      root_id = only_active_range_id(topic)
      assert Topic.merge_ranges(topic, root_id, "nope") == {:error, :no_such_range}
    end
  end

  describe "sealing" do
    test "seals all ranges; appends fail but reads still work", %{tmp_dir: directory} do
      {:ok, topic} = create(directory)
      records = [record("a", "k0"), record("b", "k1")]
      {:ok, topic, _placements} = Topic.append(topic, records)
      {:ok, topic} = Topic.sync(topic)
      root_id = only_active_range_id(topic)

      {:ok, topic} = Topic.seal(topic)
      assert topic.state == :sealed
      assert Topic.append(topic, [record("c", "k2")]) == {:error, :sealed}
      assert {:ok, [_, _]} = Topic.read(topic, root_id, 0, 10)
    end
  end

  describe "read errors" do
    test "reading an unknown range fails", %{tmp_dir: directory} do
      {:ok, topic} = create(directory)
      assert Topic.read(topic, "nope", 0, 10) == {:error, :no_such_range}
    end
  end

  describe "cross-epoch history (read_history)" do
    test "a child's history is the parent's slice followed by the child's own records",
         %{tmp_dir: directory} do
      {:ok, topic} = create(directory)
      root_id = only_active_range_id(topic)

      parent_records = for index <- 0..19, do: record("v#{index}", "k#{index}")
      {:ok, topic, _placements} = Topic.append(topic, parent_records)
      {:ok, topic} = Topic.sync(topic)

      {:ok, topic, left_id, right_id} = Topic.split_range(topic, root_id)

      child_records = for index <- 20..39, do: record("v#{index}", "k#{index}")
      {:ok, topic, _placements} = Topic.append(topic, child_records)
      {:ok, topic} = Topic.sync(topic)

      {:ok, left_history} = Topic.read_history(topic, left_id)
      {:ok, right_history} = Topic.read_history(topic, right_id)

      # together the two children's histories reconstruct every record exactly once
      all_values = Enum.map(left_history ++ right_history, & &1.value)
      expected_values = Enum.map(parent_records ++ child_records, & &1.value)
      assert Enum.sort(all_values) == Enum.sort(expected_values)

      # happens-before: in each history, all parent-epoch records precede all child-epoch ones
      parent_value_set = MapSet.new(Enum.map(parent_records, & &1.value))

      for history <- [left_history, right_history] do
        origins = Enum.map(history, &if(MapSet.member?(parent_value_set, &1.value), do: :parent, else: :child))
        {_parents, rest} = Enum.split_while(origins, &(&1 == :parent))
        assert Enum.all?(rest, &(&1 == :child)), "a child-epoch record appeared before a parent-epoch one"
      end
    end

    test "reading history of an unknown range fails", %{tmp_dir: directory} do
      {:ok, topic} = create(directory)
      assert Topic.read_history(topic, "nope") == {:error, :no_such_range}
    end

    test "a stray :base_offset option does not leak into range logs", %{tmp_dir: directory} do
      # base_offset is a keyspace concept, not a per-range log offset; a range's log must
      # still start at 0 so reads (and read_history) work from offset 0.
      {:ok, topic} = Topic.create(directory, "events", keyspace_bits: 4, base_offset: 100)
      root_id = only_active_range_id(topic)
      {:ok, topic, _placements} = Topic.append(topic, [record("a", "k0")])
      {:ok, topic} = Topic.sync(topic)

      assert {:ok, [stored]} = Topic.read(topic, root_id, 0, 10)
      assert stored.offset == 0
      assert {:ok, [_]} = Topic.read_history(topic, root_id)
    end

    test "stream_history pages bounded chunks to the same result as read_history",
         %{tmp_dir: directory} do
      {:ok, topic} = create(directory)
      root_id = only_active_range_id(topic)
      records = for index <- 0..9, do: record("v#{index}", "k#{index}")
      {:ok, topic, _placements} = Topic.append(topic, records)
      {:ok, topic} = Topic.sync(topic)
      {:ok, topic, left_id, _right_id} = Topic.split_range(topic, root_id)

      paged = drain_history(topic, left_id, :start, [])
      {:ok, full} = Topic.read_history(topic, left_id)
      assert Enum.map(paged, & &1.value) == Enum.map(full, & &1.value)
    end
  end

  defp drain_history(topic, range_id, cursor, accumulated) do
    case Topic.stream_history(topic, range_id, cursor, 3) do
      {:ok, records, :done} -> [records | accumulated] |> Enum.reverse() |> List.flatten()
      {:ok, records, next_cursor} -> drain_history(topic, range_id, next_cursor, [records | accumulated])
    end
  end
end
