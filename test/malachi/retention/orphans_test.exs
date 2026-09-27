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

  describe "candidate_ids/1" do
    test "reads a readable name back into its segment id" do
      assert Orphans.candidate_ids("t-r0-s1") == [{{"t", 0}, 1}]
    end

    test "gives every reading of a topic that itself looks like a suffix" do
      # `a-r0-s0-r1-s2` is segment 2 of range 1 of topic `a-r0-s0`. `a` with range 0 and segment
      # `0-r1-s2` is not a reading: the segment is not a number. Both readings that ARE numbers are kept.
      assert Orphans.candidate_ids("a-r0-s0-r1-s2") == [{{"a-r0-s0", 1}, 2}]
      assert Orphans.candidate_ids("x-r1-s2-r3-s4") == [{{"x-r1-s2", 3}, 4}]
    end

    test "drops a reading the layout would have spelled differently" do
      # A leading zero parses, but the layout writes `t-r0-s1`, so `t-r0-s01` is no name it wrote.
      assert Orphans.candidate_ids("t-r0-s01") == []
    end

    test "decodes the Base64 form of an id whose topic is not path-safe" do
      id = {{"a/b", 0}, 3}
      assert Orphans.candidate_ids(Path.basename(Layout.segment_directory(@directory, id))) == [id]
    end

    test "decodes an id that is not a broker id at all" do
      id = {:segment, 7}
      assert Orphans.candidate_ids(Path.basename(Layout.segment_directory(@directory, id))) == [id]
    end

    test "is empty for what an operator leaves in a data directory" do
      for name <- ["ra", "lost+found", "backup", ".snapshots", "segments.old", "gone", "-r0-s1"] do
        assert Orphans.candidate_ids(name) == [], name
      end
    end

    test "the Base64 spelling of an id the layout writes readably has no reading" do
      # What makes the round trip exact rather than a plausibility check: this decodes to a real segment
      # id, but the layout would have written that id as t-r0-s1, so this name it never wrote.
      encoded = Base.url_encode64(:erlang.term_to_binary({{"t", 0}, 1}), padding: false)
      assert Orphans.candidate_ids(encoded) == []
    end
  end

  describe "candidates/2" do
    test "keeps only names old enough, with a reading, and not reserved" do
      entries = [
        {"old-r0-s1", 1_000},
        {"new-r0-s2", 999},
        {"malachi.format", 5_000},
        {"shard_0", 5_000},
        {"lost+found", 5_000},
        {"shard_0-r0-s1", 5_000}
      ]

      assert {names, 0} = Orphans.candidates(entries, min_age_ms: 1_000, max_tracked: 10)
      assert Enum.sort(names) == ["old-r0-s1", "shard_0-r0-s1"]
    end

    test "the oldest come first and the rest wait for a later pass, counted" do
      entries = [{"a-r0-s1", 10}, {"b-r0-s1", 30}, {"c-r0-s1", 20}, {"lost+found", 99}]

      # The count is of ELIGIBLE names left out, so the operator is told only about real candidates.
      assert Orphans.candidates(entries, min_age_ms: 0, max_tracked: 2) == {["b-r0-s1", "c-r0-s1"], 1}
    end
  end

  describe "explain/2" do
    test "a name is known when any of its readings is" do
      lookup = fn ids ->
        {:ok,
         %{known: MapSet.new(Enum.filter(ids, &(&1 == {{"t", 0}, 1}))), unroutable: [], migrating: [], misplaced: []}}
      end

      assert {:ok, %{known: known, undecided: undecided}} = Orphans.explain(["t-r0-s1", "t-r0-s2"], lookup)
      assert known == MapSet.new(["t-r0-s1"])
      assert undecided == MapSet.new()
    end

    test "a name no owner could be asked about is undecided, not unknown" do
      name = Path.basename(Layout.segment_directory(@directory, {:segment, 7}))
      lookup = fn ids -> {:ok, %{known: MapSet.new(), unroutable: ids, migrating: [], misplaced: []}} end

      assert {:ok, %{known: known, undecided: undecided}} = Orphans.explain([name], lookup)
      assert known == MapSet.new()
      assert undecided == MapSet.new([name])
    end

    test "a name a pending split is moving, listed by neither owner, is undecided, not unknown" do
      lookup = fn ids -> {:ok, %{known: MapSet.new(), unroutable: [], migrating: ids, misplaced: []}} end

      assert {:ok, %{known: known, undecided: undecided}} = Orphans.explain(["t-r0-s1"], lookup)
      assert known == MapSet.new()
      assert undecided == MapSet.new(["t-r0-s1"])
    end

    test "a name listed only by a vnode that does not own it is undecided, not unknown" do
      lookup = fn ids -> {:ok, %{known: MapSet.new(), unroutable: [], migrating: [], misplaced: ids}} end

      assert {:ok, %{known: known, undecided: undecided}} = Orphans.explain(["t-r0-s1"], lookup)
      assert known == MapSet.new()
      assert undecided == MapSet.new(["t-r0-s1"])
    end

    test "a known reading wins over an undecided one" do
      lookup = fn ids -> {:ok, %{known: MapSet.new(ids), unroutable: ids, migrating: ids, misplaced: []}} end

      assert {:ok, %{known: known, undecided: undecided}} = Orphans.explain(["t-r0-s1"], lookup)
      assert known == MapSet.new(["t-r0-s1"])
      assert undecided == MapSet.new()
    end

    test "every reading of every name is asked about, once" do
      test_pid = self()

      lookup = fn ids ->
        send(test_pid, {:ids, ids})
        {:ok, %{known: MapSet.new(), unroutable: [], migrating: [], misplaced: []}}
      end

      {:ok, _explained} = Orphans.explain(["t-r0-s1", "t-r0-s1", "u-r2-s3"], lookup)
      assert_received {:ids, ids}
      assert Enum.sort(ids) == [{{"t", 0}, 1}, {{"u", 2}, 3}]
    end

    test "an error from the lookup is returned as-is" do
      assert Orphans.explain(["t-r0-s1"], fn _ids -> {:error, :no_topology} end) == {:error, :no_topology}
    end
  end

  test "known_among/2 keeps the ids the segment map lists" do
    segments = %{{{"t", 0}, 1} => :meta}
    assert Orphans.known_among(segments, [{{"t", 0}, 1}, {{"t", 0}, 2}]) == MapSet.new([{{"t", 0}, 1}])
  end

  describe "review/3" do
    test "every unexplained name is a candidate" do
      review = Orphans.review(["gone-r0-s1"], %{}, @opts)

      assert review.ready == ["gone-r0-s1"]
      assert review.held == []
    end

    test "a candidate is held until it has been unexplained for the required passes" do
      opts = Keyword.put(@opts, :sightings, 3)

      first = Orphans.review(["gone-r0-s1"], %{}, opts)
      assert first.ready == [] and first.held == ["gone-r0-s1"]

      second = Orphans.review(["gone-r0-s1"], first.sightings, opts)
      assert second.ready == [] and second.held == ["gone-r0-s1"]

      third = Orphans.review(["gone-r0-s1"], second.sightings, opts)
      assert third.ready == ["gone-r0-s1"]
    end

    test "a directory explained again forgets its sightings" do
      opts = Keyword.put(@opts, :sightings, 2)

      counted = Orphans.review(["gone-r0-s1"], %{}, opts)
      assert counted.sightings == %{"gone-r0-s1" => 1}

      # The next pass finds it explained: nothing is carried, so a later disappearance starts from one.
      explained = Orphans.review([], counted.sightings, opts)
      assert explained.sightings == %{}

      again = Orphans.review(["gone-r0-s1"], explained.sightings, opts)
      assert again.ready == [] and again.sightings == %{"gone-r0-s1" => 1}
    end

    test "at most max_per_pass are ready at once, deterministically" do
      names = for n <- 1..10, do: "d#{n}-r0-s0"
      review = Orphans.review(names, %{}, Keyword.put(@opts, :max_per_pass, 3))

      assert review.ready == ["d1-r0-s0", "d10-r0-s0", "d2-r0-s0"]
      # The rest are still reported: a candidate that vanished from the report would be a leak nobody sees.
      assert length(review.held) == 7
    end

    test "tracking past the cap is reported, and only delays a removal" do
      names = for n <- 1..5, do: "d#{n}-r0-s0"
      review = Orphans.review(names, %{}, Keyword.put(@opts, :max_tracked, 2))

      assert review.capped?
      assert map_size(review.sightings) == 2
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
      listed = for {id, _segment} <- metadata.segments, into: MapSet.new(), do: layout_name(id)
      entries = for name <- Enum.uniq(MapSet.to_list(listed) ++ extra), do: {name, 10_000}

      {names, 0} = Orphans.candidates(entries, min_age_ms: 0, max_tracked: 1_000)

      lookup = fn ids ->
        {:ok, %{known: Orphans.known_among(metadata.segments, ids), unroutable: [], migrating: [], misplaced: []}}
      end

      {:ok, %{known: known, undecided: undecided}} = Orphans.explain(names, lookup)
      unexplained = Enum.reject(names, &(MapSet.member?(known, &1) or MapSet.member?(undecided, &1)))

      review = Orphans.review(unexplained, %{}, @opts)

      for selected <- review.ready ++ review.held do
        refute MapSet.member?(listed, selected)
      end
    end
  end

  # What the rule above rests on: whatever id a segment has, its own directory name reads back to it.
  # If a reading were ever missing, the owner would be asked about the wrong id and a live directory
  # would look orphaned. Topics are drawn to include `-r<n>-s<m>` inside them and characters outside the
  # layout's allowlist, and ids that are not broker ids at all.
  property "every segment's directory name reads back to that segment's id" do
    check all(id <- segment_id(), max_runs: 500) do
      assert id in Orphans.candidate_ids(layout_name(id))
    end
  end

  defp segment_id do
    topic =
      StreamData.one_of([
        StreamData.string(:alphanumeric, min_length: 1),
        StreamData.map(
          StreamData.tuple(
            {StreamData.string(:alphanumeric, min_length: 1), StreamData.integer(0..99), StreamData.integer(0..99)}
          ),
          fn {prefix, r, s} -> "#{prefix}-r#{r}-s#{s}" end
        ),
        StreamData.string(:printable, min_length: 1)
      ])

    StreamData.one_of([
      StreamData.map(
        StreamData.tuple({topic, StreamData.non_negative_integer(), StreamData.non_negative_integer()}),
        fn {t, r, s} -> {{t, r}, s} end
      ),
      StreamData.map(StreamData.integer(), &{:segment, &1})
    ])
  end

  defp layout_name(id), do: Path.basename(Layout.segment_directory(@directory, id))

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

  # Guards the readings above against a silent change in the naming: they are only meaningful if they
  # are the same mapping the writer uses.
  test "the names read here are the ones the layout writes" do
    assert Layout.segment_directory(@directory, {{"t", 0}, 1}) == "/data/t-r0-s1"
  end
end
