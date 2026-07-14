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

      {state, :ok} = apply!(state, {:seal_segment, "seg1", 1024, 2048, 1_700_000_000_000})

      assert %{state: :sealed, length: 1024, byte_size: 2048, sealed_at: 1_700_000_000_000} =
               Metadata.get_segment(state, "seg1")

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
      assert {_state, {:error, :no_such_segment}} = Metadata.apply(state, {:seal_segment, "nope", 1, 0, 0})

      assert {_state, {:error, :no_such_segment}} =
               Metadata.apply(state, {:set_segment_replicas, "nope", [:b1]})
    end
  end

  describe "delete_segment (retention)" do
    test "deletes a sealed segment from the control plane" do
      {state, root_id} = create_topic()
      {state, :ok} = apply!(state, {:register_segment, root_id, "seg1", [:b1], 0})
      {state, :ok} = apply!(state, {:seal_segment, "seg1", 10, 0, 0})

      {state, :ok} = apply!(state, {:delete_segment, "seg1"})
      assert Metadata.get_segment(state, "seg1") == nil
      assert Metadata.segments_of_range(state, root_id) == []
    end

    test "refuses to delete the active segment (still being written)" do
      {state, root_id} = create_topic()
      {state, :ok} = apply!(state, {:register_segment, root_id, "seg1", [:b1], 0})

      assert {^state, {:error, :segment_active}} = Metadata.apply(state, {:delete_segment, "seg1"})
      assert Metadata.get_segment(state, "seg1").state == :active
    end

    test "errors deleting an unknown segment" do
      {state, _root_id} = create_topic()
      assert {^state, {:error, :no_such_segment}} = Metadata.apply(state, {:delete_segment, "nope"})
    end
  end

  describe "storage policies" do
    test "define_policy stores a named policy; get_policy reads it (last write wins)" do
      state = Metadata.new()
      assert Metadata.get_policy(state, "durable") == nil

      {state, :ok} =
        Metadata.apply(state, {:define_policy, "durable", %{retention: %{max_age_ms: 1_000}, spread_by: "rack"}})

      assert Metadata.get_policy(state, "durable") == %{retention: %{max_age_ms: 1_000}, spread_by: "rack"}

      {state, :ok} = Metadata.apply(state, {:define_policy, "durable", %{spread_by: "dc"}})
      assert Metadata.get_policy(state, "durable") == %{spread_by: "dc"}
    end

    test "define_policy rejects an invalid name or policy" do
      state = Metadata.new()
      assert {^state, {:error, :invalid_policy}} = Metadata.apply(state, {:define_policy, "", %{}})
      assert {^state, {:error, :invalid_policy}} = Metadata.apply(state, {:define_policy, 123, %{}})
      assert {^state, {:error, :invalid_policy}} = Metadata.apply(state, {:define_policy, "p", :not_a_map})
    end

    test "set_topic_policy associates a policy with a topic; topic_policy resolves it; nil detaches" do
      {state, _root} = create_topic()
      {state, :ok} = apply!(state, {:define_policy, "durable", %{retention: %{max_bytes: 500}}})
      assert Metadata.topic_policy(state, "events") == nil

      {state, :ok} = apply!(state, {:set_topic_policy, "events", "durable"})
      assert Metadata.topic_policy(state, "events") == %{retention: %{max_bytes: 500}}

      {state, :ok} = apply!(state, {:set_topic_policy, "events", nil})
      assert Metadata.topic_policy(state, "events") == nil
    end

    test "set_topic_policy errors on an unknown topic or unknown policy" do
      {state, _root} = create_topic()
      assert {^state, {:error, :no_such_topic}} = Metadata.apply(state, {:set_topic_policy, "nope", "durable"})
      assert {^state, {:error, :no_such_policy}} = Metadata.apply(state, {:set_topic_policy, "events", "ghost"})
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
        {:seal_segment, "s1", 512, 0, 0},
        {:seal_topic, "logs"}
      ]

      replay = fn -> Enum.reduce(commands, Metadata.new(), fn cmd, st -> elem(Metadata.apply(st, cmd), 0) end) end

      assert replay.() == replay.()
    end
  end

  describe "consumer offsets" do
    test "commit_offset stores a group's position; committed_offsets reads it (last commit wins)" do
      metadata = Metadata.new()
      assert Metadata.committed_offsets(metadata, "g", "events") == %{}

      {metadata, :ok} = Metadata.apply(metadata, {:commit_offset, "g", "events", %{{"events", 0} => 5}})
      assert Metadata.committed_offsets(metadata, "g", "events") == %{{"events", 0} => 5}

      # last commit wins; groups and topics are independent
      {metadata, :ok} = Metadata.apply(metadata, {:commit_offset, "g", "events", %{{"events", 0} => 9}})
      {metadata, :ok} = Metadata.apply(metadata, {:commit_offset, "other", "events", %{{"events", 0} => 1}})
      assert Metadata.committed_offsets(metadata, "g", "events") == %{{"events", 0} => 9}
      assert Metadata.committed_offsets(metadata, "other", "events") == %{{"events", 0} => 1}
    end

    test "commit_offset merges per range so a member does not clobber other members' ranges" do
      metadata = Metadata.new()

      # a two-range group position
      {metadata, :ok} =
        Metadata.apply(metadata, {:commit_offset, "g", "events", %{{"events", 0} => 5, {"events", 1} => 6}})

      # a member owning only range 1 commits just its range; range 0 (another member's) is preserved
      {metadata, :ok} = Metadata.apply(metadata, {:commit_offset, "g", "events", %{{"events", 1} => 9}})

      assert Metadata.committed_offsets(metadata, "g", "events") == %{{"events", 0} => 5, {"events", 1} => 9}
    end

    test "commit_offset prunes the offset of a range that is no longer active (a split parent)" do
      {state, root} = create_topic(Metadata.new(), "events", 4)
      {state, :ok} = apply!(state, {:commit_offset, "g", "events", %{root => 5}})
      assert Metadata.committed_offsets(state, "g", "events") == %{root => 5}

      # the split seals the root (now inactive); its committed offset is dead — the active children resume
      # from :start — so the next commit prunes it instead of letting the map grow one dead key per split
      {state, {:ok, left, _right}} = apply!(state, {:split_range, root})
      {state, :ok} = apply!(state, {:commit_offset, "g", "events", %{left => 3}})

      assert Metadata.committed_offsets(state, "g", "events") == %{left => 3}
    end
  end

  # Builds "events": root split into two children, two segments (one sealed) on the left child, and a
  # committed consumer group.
  defp events_with_segments do
    {state, root_id} = create_topic(Metadata.new(), "events", 4)
    {state, {:ok, left_id, _right_id}} = apply!(state, {:split_range, root_id})
    {state, :ok} = apply!(state, {:register_segment, left_id, "seg-a", [:b1], 0})
    {state, :ok} = apply!(state, {:register_segment, left_id, "seg-b", [:b1], 100})
    {state, :ok} = apply!(state, {:seal_segment, "seg-a", 100, 4096, 1_700_000_000_000})
    {state, :ok} = apply!(state, {:commit_offset, "group-1", "events", %{left_id => {0, 50}}})
    {state, left_id}
  end

  describe "overview/1" do
    test "summarizes each topic with counts, bytes, and consumer groups (no per-range detail)" do
      {state, _left_id} = events_with_segments()

      assert [topic] = Metadata.overview(state)
      assert topic.name == "events"
      assert topic.state == :active
      assert topic.keyspace_size == 16
      # root range (sealed by the split) + two active children
      assert topic.range_count == 3
      assert topic.active_range_count == 2
      assert topic.segment_count == 2
      assert topic.active_segment_count == 1
      # only the sealed segment has a byte_size; the active one contributes 0
      assert topic.total_bytes == 4096
      assert topic.groups == ["group-1"]

      # the summary is light: the per-range/segment drill-down lives in topic_detail/2, not here
      refute Map.has_key?(topic, :ranges)
    end

    test "sorts topics by name and returns [] for empty metadata" do
      assert Metadata.overview(Metadata.new()) == []

      {state, _} = create_topic(Metadata.new(), "zeta", 4)
      {state, _} = create_topic(state, "alpha", 4)
      assert ["alpha", "zeta"] == Enum.map(Metadata.overview(state), & &1.name)
    end
  end

  describe "topic_detail/2" do
    test "returns the topic's ranges (by seq), each with its segments (by start_offset)" do
      {state, left_id} = events_with_segments()

      assert %{name: "events", ranges: ranges} = Metadata.topic_detail(state, "events")
      # root (#0) + two children, sorted by seq
      assert Enum.map(ranges, & &1.seq) == Enum.sort(Enum.map(ranges, & &1.seq))
      assert length(ranges) == 3

      range_with_segs = Enum.find(ranges, &(&1.segments != []))
      assert range_with_segs.seq == elem(left_id, 1)

      assert [
               %{start_offset: 0, state: :sealed, byte_size: 4096, sealed_at: 1_700_000_000_000},
               %{start_offset: 100, state: :active, byte_size: nil}
             ] = range_with_segs.segments
    end

    test "returns nil for an unknown topic" do
      {state, _} = create_topic(Metadata.new(), "events", 4)
      assert Metadata.topic_detail(state, "nope") == nil
    end
  end

  describe "secondary index under migration (extract/insert)" do
    test "extract scopes the removal to the topic; insert rebuilds its index entries" do
      {state, root} = create_topic(Metadata.new(), "events", 4)
      {state, {:ok, left, _right}} = apply!(state, {:split_range, root})
      {state, :ok} = apply!(state, {:register_segment, left, "s1", [:b1], 0})
      {state, _} = create_topic(state, "other", 4)

      assert Map.has_key?(state.topic_ranges, "events")
      assert Map.has_key?(state.range_segments, left)

      {without, export} = Metadata.extract_topic(state, "events")
      refute Map.has_key?(without.topic_ranges, "events")
      refute Map.has_key?(without.range_segments, left)
      # a different topic's index is untouched by the extraction
      assert Map.has_key?(without.topic_ranges, "other")
      assert index_matches_scan?(without)

      reinserted = Metadata.insert_topic(without, export)
      assert index_matches_scan?(reinserted)
      assert reinserted.range_segments[left] == MapSet.new(["s1"])
    end
  end

  describe "topic migration carries committed offsets (vnode split)" do
    test "extract carries the topic's offsets (keyed by group) and leaves a co-located topic's alone" do
      {state, root} = create_topic(Metadata.new(), "events", 4)
      {state, other_root} = create_topic(state, "other", 4)
      {state, :ok} = apply!(state, {:commit_offset, "g1", "events", %{root => 42}})
      {state, :ok} = apply!(state, {:commit_offset, "g2", "events", %{root => 7}})
      {state, :ok} = apply!(state, {:commit_offset, "g1", "other", %{other_root => 99}})

      {without, export} = Metadata.extract_topic(state, "events")

      # the export carries events' offsets, re-keyed by group (the topic is implied)
      assert export.offsets == %{"g1" => %{root => 42}, "g2" => %{root => 7}}
      # events' offsets are gone from the source; the co-located topic's remain
      assert Metadata.committed_offsets(without, "g1", "events") == %{}
      assert Metadata.committed_offsets(without, "g2", "events") == %{}
      assert Metadata.committed_offsets(without, "g1", "other") == %{other_root => 99}
    end

    test "a group's committed position survives a full extract/insert migration to a fresh vnode" do
      {state, root} = create_topic(Metadata.new(), "events", 4)
      {state, :ok} = apply!(state, {:commit_offset, "workers", "events", %{root => 500}})

      {_source, export} = Metadata.extract_topic(state, "events")
      dest = Metadata.insert_topic(Metadata.new(), export)

      assert Metadata.get_topic(dest, "events").name == "events"
      assert Metadata.committed_offsets(dest, "workers", "events") == %{root => 500}
    end

    test ":extract_topic / :insert_topic commands relocate a topic (with offsets) through the log" do
      {source, root} = create_topic(Metadata.new(), "events", 4)
      {source, :ok} = apply!(source, {:commit_offset, "g", "events", %{root => 3}})

      {source_after, export} = apply!(source, {:extract_topic, "events"})
      assert Metadata.get_topic(source_after, "events") == nil

      {dest, :ok} = apply!(Metadata.new(), {:insert_topic, export})
      assert Metadata.get_topic(dest, "events").name == "events"
      assert Metadata.committed_offsets(dest, "g", "events") == %{root => 3}
    end

    test ":extract_topic on an absent topic is a no-op returning nil" do
      {state, reply} = apply!(Metadata.new(), {:extract_topic, "ghost"})
      assert reply == nil
      assert state == Metadata.new()
    end
  end

  # The maintained index must equal one derived by scanning the source-of-truth maps.
  defp index_matches_scan?(state) do
    expected_tr =
      state.ranges
      |> Enum.group_by(fn {_id, r} -> r.topic end, fn {id, _r} -> id end)
      |> Map.new(fn {t, ids} -> {t, MapSet.new(ids)} end)

    expected_rs =
      state.segments
      |> Enum.group_by(fn {_id, s} -> s.range_id end, fn {id, _s} -> id end)
      |> Map.new(fn {r, ids} -> {r, MapSet.new(ids)} end)

    state.topic_ranges == expected_tr and state.range_segments == expected_rs
  end
end
