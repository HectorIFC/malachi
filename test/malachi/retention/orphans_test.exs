defmodule Malachi.Retention.OrphansTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Metadata
  alias Malachi.Retention.Orphans
  alias Malachi.Storage.Layout

  @directory "/data"
  @topic_pool ["a", "b", "c"]
  @keyspace_bits 4
  @opts [min_age_ms: 0, sightings: 1, max_per_pass: 100, max_tracked: 1_000]

  describe "expected/2" do
    test "names the directory of every segment, whatever this node's replica set says" do
      metadata = with_segments([{1, [:a]}, {2, [:b]}])

      assert Orphans.expected(metadata, @directory) ==
               MapSet.new(["t-r0-s1", "t-r0-s2"])
    end

    test "is empty for metadata with no segments" do
      assert Orphans.expected(Metadata.new(), @directory) == MapSet.new()
    end
  end

  describe "review/4" do
    test "an unexplained directory is a candidate and a listed one is not" do
      review = Orphans.review(MapSet.new(["kept-r0-s0"]), [{"kept-r0-s0", 10}, {"gone-r0-s1", 10}], %{}, @opts)

      assert review.ready == ["gone-r0-s1"]
      assert review.held == []
    end

    test "a directory younger than the minimum age is not even a candidate" do
      opts = Keyword.put(@opts, :min_age_ms, 1_000)
      review = Orphans.review(MapSet.new(), [{"new-r0-s0", 999}, {"old-r0-s1", 1_000}], %{}, opts)

      assert review.ready == ["old-r0-s1"]
      assert Map.keys(review.sightings) == ["old-r0-s1"]
    end

    test "a candidate is held until it has been unexplained for the required passes" do
      opts = Keyword.put(@opts, :sightings, 3)
      entries = [{"gone-r0-s1", 10}]

      first = Orphans.review(MapSet.new(), entries, %{}, opts)
      assert first.ready == [] and first.held == ["gone-r0-s1"]

      second = Orphans.review(MapSet.new(), entries, first.sightings, opts)
      assert second.ready == [] and second.held == ["gone-r0-s1"]

      third = Orphans.review(MapSet.new(), entries, second.sightings, opts)
      assert third.ready == ["gone-r0-s1"]
    end

    test "a directory the metadata explains again forgets its sightings" do
      opts = Keyword.put(@opts, :sightings, 2)

      counted = Orphans.review(MapSet.new(), [{"gone-r0-s1", 10}], %{}, opts)
      assert counted.sightings == %{"gone-r0-s1" => 1}

      # The next pass sees it listed: nothing is carried, so a later disappearance starts from one.
      explained = Orphans.review(MapSet.new(["gone-r0-s1"]), [{"gone-r0-s1", 10}], counted.sightings, opts)
      assert explained.sightings == %{}

      again = Orphans.review(MapSet.new(), [{"gone-r0-s1", 10}], explained.sightings, opts)
      assert again.ready == [] and again.sightings == %{"gone-r0-s1" => 1}
    end

    test "at most max_per_pass are ready at once, deterministically" do
      entries = for n <- 1..10, do: {"d#{n}-r0-s0", 10}
      review = Orphans.review(MapSet.new(), entries, %{}, Keyword.put(@opts, :max_per_pass, 3))

      assert review.ready == ["d1-r0-s0", "d10-r0-s0", "d2-r0-s0"]
      # The rest are still reported: a candidate that vanished from the report would be a leak nobody sees.
      assert length(review.held) == 7
    end

    test "tracking past the cap is reported, and only delays a removal" do
      entries = for n <- 1..5, do: {"d#{n}-r0-s0", 10}
      review = Orphans.review(MapSet.new(), entries, %{}, Keyword.put(@opts, :max_tracked, 2))

      assert review.capped?
      assert map_size(review.sightings) == 2
    end

    test "the data directory's own files are never candidates" do
      entries = [{"malachi.format", 10}, {"malachi.format.tmp", 10}, {"shard_0", 10}, {"shard_12", 10}]
      review = Orphans.review(MapSet.new(), entries, %{}, @opts)

      assert review.ready == []
      assert review.held == []
    end

    test "a name that only looks like a shard is still a candidate" do
      review = Orphans.review(MapSet.new(), [{"shard_x-r0-s0", 10}, {"shard_0-r0-s1", 10}], %{}, @opts)

      assert review.ready == ["shard_0-r0-s1", "shard_x-r0-s0"]
    end

    test "a name the layout could not have written is never a candidate" do
      # What an operator can leave under a data directory the sweep does not own. Some of these are not
      # valid Base64 at all and some are, decoding to bytes that are no term: both mean the same thing
      # here. The removal is an rm_rf, so a name no segment could carry is refused before any guard.
      names = ["ra", "lost+found", "backup", ".snapshots", "segments.old", "gone"]
      review = Orphans.review(MapSet.new(), for(name <- names, do: {name, 10_000}), %{}, @opts)

      assert review.ready == []
      assert review.held == []
      assert review.sightings == %{}
    end

    test "the encoded form the layout falls back to is still a candidate" do
      # A segment id whose topic is not path-safe never gets the readable name, and such an id arrives
      # over replication from another node, so its directory has to stay sweepable.
      name = Path.basename(Layout.segment_directory(@directory, {{"a/b", 0}, 3}))

      refute Regex.match?(~r/-r\d+-s\d+\z/, name)
      assert Orphans.review(MapSet.new(), [{name, 10_000}], %{}, @opts).ready == [name]
    end

    test "the Base64 spelling of an id the layout writes readably is not a candidate" do
      # What makes the round trip exact rather than a plausibility check: this decodes to a real segment
      # id, but the layout would have written that id as t-r0-s1, so this name it never wrote.
      encoded = Base.url_encode64(:erlang.term_to_binary({{"t", 0}, 1}), padding: false)

      assert Path.basename(Layout.segment_directory(@directory, {{"t", 0}, 1})) == "t-r0-s1"
      assert Orphans.review(MapSet.new(), [{encoded, 10_000}], %{}, @opts).ready == []
    end
  end

  # The one rule this module exists for, stated over any metadata a sequence of control-plane
  # operations can produce: a directory of a segment the control plane still lists is never selected.
  property "no directory of a segment present in the metadata is ever selected" do
    check all(
            ops <- StreamData.list_of(op(), max_length: 30),
            extra <- StreamData.list_of(orphan_name(), max_length: 5),
            max_runs: 200
          ) do
      metadata = run(ops)
      expected = Orphans.expected(metadata, @directory)
      entries = for name <- Enum.uniq(MapSet.to_list(expected) ++ extra), do: {name, 10_000}

      review = Orphans.review(expected, entries, %{}, @opts)

      for selected <- review.ready ++ review.held do
        refute MapSet.member?(expected, selected)
      end
    end
  end

  # An extra directory is generated in the readable form, so the property keeps exercising names that CAN
  # be selected. A name the layout could not have written is refused before the guards, which is its own
  # test above rather than a case worth spending the property's runs on.
  defp orphan_name do
    StreamData.map(StreamData.string(:alphanumeric, min_length: 1), &"#{&1}-r0-s9")
  end

  # --- op generator, in the shape of Malachi.MetadataPropertyTest ---

  defp op do
    StreamData.one_of([
      StreamData.tuple({StreamData.constant(:create_topic), StreamData.member_of(@topic_pool)}),
      StreamData.tuple({StreamData.constant(:delete_topic), StreamData.member_of(@topic_pool)}),
      StreamData.tuple({StreamData.constant(:split), StreamData.positive_integer()}),
      StreamData.tuple({StreamData.constant(:register), StreamData.positive_integer(), StreamData.positive_integer()}),
      StreamData.tuple({StreamData.constant(:delete_seg), StreamData.positive_integer()})
    ])
  end

  defp run(ops), do: Enum.reduce(ops, Metadata.new(), &apply_op(&2, &1))

  defp apply_op(state, {:create_topic, name}), do: command(state, {:create_topic, name, @keyspace_bits})
  defp apply_op(state, {:delete_topic, name}), do: command(state, {:delete_topic, name})

  defp apply_op(state, {:split, picker}) do
    case pick(Map.keys(state.ranges), picker) do
      nil -> state
      range_id -> command(state, {:split_range, range_id})
    end
  end

  defp apply_op(state, {:register, range_picker, seq}) do
    case pick(Map.keys(state.ranges), range_picker) do
      nil -> state
      range_id -> command(state, {:register_segment, range_id, {range_id, seq}, [:b1], 0})
    end
  end

  defp apply_op(state, {:delete_seg, picker}) do
    case pick(Map.keys(state.segments), picker) do
      nil -> state
      segment_id -> command(state, {:delete_segment, segment_id})
    end
  end

  defp command(state, command), do: state |> Metadata.apply(command) |> elem(0)

  defp pick([], _picker), do: nil
  defp pick(ids, picker), do: Enum.at(Enum.sort(ids), rem(picker, length(ids)))

  # A range holds one active segment at a time, so each one is sealed before the next is registered.
  defp with_segments(segments) do
    base = elem(Metadata.apply(Metadata.new(), {:create_topic, "t", @keyspace_bits}), 0)

    segments
    |> Enum.with_index()
    |> Enum.reduce(base, fn {{seq, replicas}, index}, metadata ->
      segment_id = {{"t", 0}, seq}

      metadata
      |> command({:register_segment, {"t", 0}, segment_id, replicas, index})
      |> command({:seal_segment, segment_id, 1, 10, 1_000})
    end)
  end

  # Guards the fixture above against a silent change in the naming: the expected set is only meaningful
  # if it is the same mapping the writer uses.
  test "the expected names are the ones the layout writes" do
    assert Layout.segment_directory(@directory, {{"t", 0}, 1}) == "/data/t-r0-s1"
  end
end
