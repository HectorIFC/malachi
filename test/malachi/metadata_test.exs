defmodule Malachi.MetadataTest do
  use ExUnit.Case, async: true

  alias Malachi.Metadata

  defp apply!(state, command) do
    {state, reply} = Metadata.apply(state, command)
    {state, reply}
  end

  defp create_topic(state \\ Metadata.new(), name \\ "events", bits \\ 4) do
    {state, {:ok, root_id}} = Metadata.apply(state, {:create_topic, name, bits})
    {state, root_id}
  end

  describe "create_topic" do
    test "creates the topic and a root range covering the keyspace" do
      {state, root_id} = create_topic()

      assert %{name: "events", keyspace_size: 16, state: :active} = Metadata.get_topic(state, "events")
      root = Metadata.get_range(state, root_id)
      assert {root.key_start, root.key_end} == {0, 16}
      assert root.state == :active
      assert root.parents == []
      assert root.topic == "events"
    end

    test "rejects a duplicate topic" do
      {state, _root_id} = create_topic()
      assert {_state, {:error, :already_exists}} = Metadata.apply(state, {:create_topic, "events", 4})
    end

    test "rejects invalid keyspace bits without raising" do
      assert {_state, {:error, :invalid_keyspace_bits}} =
               Metadata.apply(Metadata.new(), {:create_topic, "t", 33})
    end

    test "rejects unsafe/invalid topic names (path-traversal hardening)" do
      for bad <- ["../evil", "a/b", "..", ".", "", "with space", "tab\t"] do
        assert {_state, {:error, :invalid_topic_name}} =
                 Metadata.apply(Metadata.new(), {:create_topic, bad, 4})
      end

      # safe names are accepted
      for good <- ["events", "page_view.v2", "orders-2024"] do
        assert {_state, {:ok, _root}} = Metadata.apply(Metadata.new(), {:create_topic, good, 4})
      end
    end
  end

  describe "split_range" do
    test "seals the parent and creates two buddy children" do
      {state, root_id} = create_topic()
      {state, {:ok, left_id, right_id}} = apply!(state, {:split_range, root_id})

      assert Metadata.get_range(state, root_id).state == :sealed
      left = Metadata.get_range(state, left_id)
      right = Metadata.get_range(state, right_id)
      assert {left.key_start, left.key_end} == {0, 8}
      assert {right.key_start, right.key_end} == {8, 16}
      assert left.parents == [root_id]
      assert left.state == :active and right.state == :active

      active = Metadata.active_ranges_of_topic(state, "events")
      assert Enum.sort(Enum.map(active, & &1.id)) == Enum.sort([left_id, right_id])
    end

    test "errors on unknown, sealed, or unsplittable ranges" do
      {state, root_id} = create_topic()
      assert {^state, {:error, :no_such_range}} = Metadata.apply(state, {:split_range, 999})

      {state, {:ok, left_id, _right_id}} = apply!(state, {:split_range, root_id})
      assert {_state, {:error, :sealed}} = Metadata.apply(state, {:split_range, root_id})

      # split down to a size-1 range, then it can't split further: [0,8)->[0,4)->[0,2)->[0,1)
      {state, {:ok, quarter, _}} = apply!(state, {:split_range, left_id})
      {state, {:ok, eighth, _}} = apply!(state, {:split_range, quarter})
      {state, {:ok, unit, _}} = apply!(state, {:split_range, eighth})
      assert {_state, {:error, :cannot_split}} = Metadata.apply(state, {:split_range, unit})
    end
  end

  describe "merge_ranges" do
    test "merges buddies into a child covering their union" do
      {state, root_id} = create_topic()
      {state, {:ok, left_id, right_id}} = apply!(state, {:split_range, root_id})
      {state, {:ok, child_id}} = apply!(state, {:merge_ranges, left_id, right_id})

      child = Metadata.get_range(state, child_id)
      assert {child.key_start, child.key_end} == {0, 16}
      assert left_id in child.parents and right_id in child.parents
      assert Metadata.get_range(state, left_id).state == :sealed
      assert Metadata.get_range(state, right_id).state == :sealed
    end

    test "rejects non-buddies and unknown ranges" do
      {state, root_id} = create_topic()
      {state, {:ok, left_id, right_id}} = apply!(state, {:split_range, root_id})
      {state, {:ok, ll, _lr}} = apply!(state, {:split_range, left_id})

      assert {_state, {:error, :not_buddies}} = Metadata.apply(state, {:merge_ranges, ll, right_id})
      # right_id is still active, so the unknown second range is what fails
      assert {_state, {:error, :no_such_range}} = Metadata.apply(state, {:merge_ranges, right_id, 999})
    end
  end

  describe "seal_topic / delete_topic" do
    test "seal_topic seals the topic and all its ranges" do
      {state, root_id} = create_topic()
      {state, {:ok, left_id, right_id}} = apply!(state, {:split_range, root_id})
      {state, :ok} = apply!(state, {:seal_topic, "events"})

      assert Metadata.get_topic(state, "events").state == :sealed
      assert Metadata.get_range(state, left_id).state == :sealed
      assert Metadata.get_range(state, right_id).state == :sealed
    end

    test "delete_topic removes the topic, its ranges and their segments" do
      {state, root_id} = create_topic()
      {state, :ok} = apply!(state, {:register_segment, root_id, "seg1", [:b1, :b2], 0})
      {state, :ok} = apply!(state, {:delete_topic, "events"})

      assert Metadata.get_topic(state, "events") == nil
      assert Metadata.get_range(state, root_id) == nil
      assert Metadata.get_segment(state, "seg1") == nil
    end

    test "errors on unknown topics" do
      assert {_state, {:error, :no_such_topic}} = Metadata.apply(Metadata.new(), {:seal_topic, "nope"})
      assert {_state, {:error, :no_such_topic}} = Metadata.apply(Metadata.new(), {:delete_topic, "nope"})
    end
  end

  describe "segments" do
    test "register, seal, and reassign a segment" do
      {state, root_id} = create_topic()
      {state, :ok} = apply!(state, {:register_segment, root_id, "seg1", [:b1, :b2, :b3], 0})

      segment = Metadata.get_segment(state, "seg1")
      assert segment.state == :active
      assert segment.replica_set == [:b1, :b2, :b3]
      assert segment.length == nil
      assert Metadata.segments_of_range(state, root_id) |> length() == 1

      {state, :ok} = apply!(state, {:seal_segment, "seg1", 1024})
      assert %{state: :sealed, length: 1024} = Metadata.get_segment(state, "seg1")

      {state, :ok} = apply!(state, {:set_segment_replicas, "seg1", [:b1, :b4]})
      assert Metadata.get_segment(state, "seg1").replica_set == [:b1, :b4]
    end

    test "errors registering a duplicate segment or on a sealed/unknown range" do
      {state, root_id} = create_topic()
      {state, :ok} = apply!(state, {:register_segment, root_id, "seg1", [:b1], 0})

      assert {_state, {:error, :segment_exists}} =
               Metadata.apply(state, {:register_segment, root_id, "seg1", [:b1], 0})

      assert {_state, {:error, :no_such_range}} =
               Metadata.apply(state, {:register_segment, 999, "seg2", [:b1], 0})

      {state, {:ok, _l, _r}} = apply!(state, {:split_range, root_id})

      assert {_state, {:error, :sealed}} =
               Metadata.apply(state, {:register_segment, root_id, "seg3", [:b1], 0})
    end

    test "errors sealing/reassigning an unknown segment" do
      {state, _root_id} = create_topic()
      assert {_state, {:error, :no_such_segment}} = Metadata.apply(state, {:seal_segment, "nope", 1})

      assert {_state, {:error, :no_such_segment}} =
               Metadata.apply(state, {:set_segment_replicas, "nope", [:b1]})
    end
  end

  describe "unknown commands" do
    test "an unrecognized command is rejected, not crashed (replica safety)" do
      {state, _root_id} = create_topic()
      assert {^state, {:error, :unknown_command}} = Metadata.apply(state, {:bogus, 1, 2})
    end
  end

  describe "determinism (replication safety)" do
    test "replaying the same command log yields identical state" do
      commands = [
        {:create_topic, "events", 4},
        {:create_topic, "logs", 8},
        {:split_range, {"events", 0}},
        {:split_range, {"logs", 0}},
        {:merge_ranges, {"events", 1}, {"events", 2}},
        {:register_segment, {"events", 3}, "s1", [:b1, :b2], 0},
        {:seal_segment, "s1", 512},
        {:seal_topic, "logs"}
      ]

      replay = fn -> Enum.reduce(commands, Metadata.new(), fn cmd, st -> elem(Metadata.apply(st, cmd), 0) end) end

      assert replay.() == replay.()
    end
  end
end
