defmodule Malachi.RangeTest do
  use ExUnit.Case, async: true

  alias Malachi.Log.Record
  alias Malachi.Range

  @moduletag :tmp_dir

  defp rec(value, opts), do: Record.new(value, opts)

  # Small keyspace (0..15) so buddy splits are easy to reason about in assertions.
  defp open(directory), do: Range.open(directory, keyspace_bits: 4, max_bytes: 120, index_interval: 32)

  # First synthetic key whose hash lands inside `range`.
  defp key_in(range) do
    0..10_000
    |> Stream.map(&"k#{&1}")
    |> Enum.find(fn key -> Range.covers?(range, key) end)
  end

  describe "open / keyspace" do
    test "root range covers the whole keyspace", %{tmp_dir: directory} do
      {:ok, root} = open(directory)
      assert root.key_start == 0
      assert root.key_end == 16
      assert root.state == :active
      assert root.parents == []

      for key <- ["a", "b", "anything"] do
        assert Range.covers?(root, key)
        assert Range.position_of(root, key) in 0..15
      end
    end

    test "append + read round-trip", %{tmp_dir: directory} do
      {:ok, root} = open(directory)
      {:ok, root, first, last} = Range.append(root, [rec("a", key: "k1"), rec("b", key: "k2")])
      assert {first, last} == {0, 1}
      {:ok, root} = Range.sync(root)

      assert {:ok, records} = Range.read(root, 0, 10)
      assert Enum.map(records, & &1.value) == ["a", "b"]
      :ok = Range.close(root)
    end

    test "appending an empty list is a no-op (not a crash)", %{tmp_dir: directory} do
      {:ok, root} = open(directory)
      assert {:ok, _root, 0, -1} = Range.append(root, [])
    end

    test "rejects an invalid keyspace_bits", %{tmp_dir: directory} do
      assert_raise ArgumentError, fn -> Range.open(directory, keyspace_bits: 33) end
      assert_raise ArgumentError, fn -> Range.open(directory, keyspace_bits: 0) end
    end
  end

  describe "split" do
    test "halves the keyspace, seals the parent, and records lineage", %{tmp_dir: directory} do
      {:ok, root} = open(directory)
      {:ok, sealed, left, right} = Range.split(root)

      assert sealed.state == :sealed
      assert {left.key_start, left.key_end} == {0, 8}
      assert {right.key_start, right.key_end} == {8, 16}
      assert left.state == :active and right.state == :active
      assert left.parents == [root.id]
      assert right.parents == [root.id]
      refute left.id == right.id
    end

    test "is logical: the parent keeps its records, children start empty", %{tmp_dir: directory} do
      {:ok, root} = open(directory)
      {:ok, root, _, _} = Range.append(root, [rec("x", key: "a"), rec("y", key: "b")])
      {:ok, root} = Range.sync(root)

      {:ok, sealed, left, right} = Range.split(root)

      assert {:ok, records} = Range.read(sealed, 0, 10)
      assert Enum.map(records, & &1.value) == ["x", "y"]
      assert Range.read(left, 0, 10) == :eof
      assert Range.read(right, 0, 10) == :eof
    end

    test "rejects appends whose key hashes outside the range", %{tmp_dir: directory} do
      {:ok, root} = open(directory)
      {:ok, _sealed, left, right} = Range.split(root)

      misrouted_key = key_in(right)

      assert Range.append(left, [rec("v", key: misrouted_key)]) ==
               {:error, {:key_out_of_range, misrouted_key}}

      well_routed_key = key_in(left)
      assert {:ok, _left, 0, 0} = Range.append(left, [rec("v", key: well_routed_key)])
    end

    test "cannot split a size-1 range", %{tmp_dir: directory} do
      {:ok, root} = open(directory)
      {:ok, _s, half, _r} = Range.split(root)
      {:ok, _s, quarter, _r} = Range.split(half)
      {:ok, _s, eighth, _r} = Range.split(quarter)
      {:ok, _s, unit, _r} = Range.split(eighth)

      assert unit.key_end - unit.key_start == 1
      assert Range.split(unit) == {:error, :cannot_split}
    end

    test "cannot split or append to a sealed range", %{tmp_dir: directory} do
      {:ok, root} = open(directory)
      {:ok, sealed, _left, _right} = Range.split(root)

      assert Range.split(sealed) == {:error, :sealed}
      assert Range.append(sealed, [rec("v", key: "a")]) == {:error, :sealed}
    end
  end

  describe "buddy / merge" do
    test "the two halves of a split are buddies", %{tmp_dir: directory} do
      {:ok, root} = open(directory)
      {:ok, _s, left, right} = Range.split(root)
      assert Range.buddy?(left, right)
      assert Range.buddy?(right, left)
    end

    test "non-adjacent or mismatched-size ranges are not buddies", %{tmp_dir: directory} do
      {:ok, root} = open(directory)
      {:ok, _s, left, right} = Range.split(root)
      {:ok, _s, left_left, _left_right} = Range.split(left)

      # different size (quarter vs half)
      refute Range.buddy?(left_left, right)
    end

    test "merging buddies seals both and creates a child over their union", %{tmp_dir: directory} do
      {:ok, root} = open(directory)
      {:ok, _s, left, right} = Range.split(root)

      {:ok, sealed_left, sealed_right, child} = Range.merge(left, right)

      assert sealed_left.state == :sealed
      assert sealed_right.state == :sealed
      assert {child.key_start, child.key_end} == {0, 16}
      assert child.state == :active
      assert left.id in child.parents
      assert right.id in child.parents
    end

    test "merging non-buddies is rejected", %{tmp_dir: directory} do
      {:ok, root} = open(directory)
      {:ok, _s, left, right} = Range.split(root)
      {:ok, _s, left_left, _left_right} = Range.split(left)

      assert Range.merge(left_left, right) == {:error, :not_buddies}
    end

    test "refuses to merge geometrically-matching ranges from different topics", %{tmp_dir: directory} do
      {:ok, topic_a} = Range.open(Path.join(directory, "a"), keyspace_bits: 4)
      {:ok, topic_b} = Range.open(Path.join(directory, "b"), keyspace_bits: 4)
      {:ok, _s, left_a, _right_a} = Range.split(topic_a)
      {:ok, _s, _left_b, right_b} = Range.split(topic_b)

      # left_a = [0,8) and right_b = [8,16) would be buddies geometrically, but they
      # belong to different topics, so the merge must be rejected.
      assert Range.buddy?(left_a, right_b)
      assert Range.merge(left_a, right_b) == {:error, :different_topic}
    end
  end

  describe "ordering (happens-before)" do
    test "a parent happens-before its children, not vice versa", %{tmp_dir: directory} do
      {:ok, root} = open(directory)
      {:ok, sealed_root, left, _right} = Range.split(root)

      assert Range.happens_before?(sealed_root, left)
      refute Range.happens_before?(left, sealed_root)
    end

    test "lineage is transitive across multiple splits", %{tmp_dir: directory} do
      {:ok, root} = open(directory)
      {:ok, sealed_root, left, _right} = Range.split(root)
      {:ok, _sealed_left, left_left, _left_right} = Range.split(left)

      assert Range.happens_before?(sealed_root, left_left)
      assert Range.happens_before?(left, left_left)
    end
  end
end
