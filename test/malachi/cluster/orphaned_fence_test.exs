defmodule Malachi.Cluster.OrphanedFenceTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.Failover
  alias Malachi.Cluster.OrphanedFence
  alias Malachi.Metadata

  # Epoch milliseconds, the shape the coordinator passes as `now_ms`, fixed so the command a test
  # asserts on is byte-identical to the one every replica applies. Thirteen digits on purpose: a 0
  # would read as a valid 1970 timestamp and let an age calculation pass by accident.
  @now 1_700_000_000_000

  # `topics` is `[{topic_name, [{seq, replica_set, start_offset, seal_length}]}]`, a nil length leaving
  # the segment active. Returns the metadata and every segment id in the order given.
  #
  # One topic per range on purpose: a range has exactly one write head, so two ACTIVE segments only
  # exist across ranges, and that is the shape a grouping or ordering assertion needs.
  defp metadata_with(topics) do
    Enum.reduce(topics, {Metadata.new(), []}, fn {name, segments}, {meta, ids} ->
      {meta, {:ok, root}} = Metadata.apply(meta, {:create_topic, name, 8})
      Enum.reduce(segments, {meta, ids}, &register(&1, &2, root))
    end)
  end

  defp register({seq, replica_set, start_offset, seal_length}, {meta, ids}, root) do
    id = {root, seq}
    {meta, :ok} = Metadata.apply(meta, {:register_segment, root, id, replica_set, start_offset})

    meta =
      if seal_length,
        do: elem(Metadata.apply(meta, {:seal_segment, id, seal_length, 0, 0}), 0),
        else: meta

    {meta, ids ++ [id]}
  end

  defp topic_with(segments) do
    metadata_with([{"events", segments}])
  end

  defp active_segment(replica_set) do
    {metadata, [id]} = topic_with([{0, replica_set, 0, nil}])
    {metadata, id}
  end

  describe "candidates/2 (what the coordinator must ask about)" do
    test "names the active segments whose primary is LIVE, grouped by that primary" do
      {metadata, id} = active_segment([:a, :b, :c])

      assert OrphanedFence.candidates(metadata, [:a, :b, :c]) == [{:a, [{id, 0}]}]
    end

    test "groups a primary's segments into one entry, so the pass makes one call per primary" do
      {metadata, [sealed, events_head, orders_head]} =
        metadata_with([
          {"events", [{0, [:a, :b], 0, 5}, {1, [:a, :b], 5, nil}]},
          {"orders", [{0, [:a, :c], 0, nil}]}
        ])

      # :a is the primary of two active segments across two ranges, and they arrive as ONE entry: the
      # pass makes a call per primary, not per segment. Each carries its START OFFSET, which is what
      # seats a log the probed server has not opened. The sealed segment is not asked about at all.
      assert OrphanedFence.candidates(metadata, [:a, :b, :c]) ==
               [{:a, [{events_head, 5}, {orders_head, 0}]}]

      refute Enum.any?(OrphanedFence.candidates(metadata, [:a, :b, :c]), fn {_p, segments} ->
               Enum.any?(segments, &match?({^sealed, _offset}, &1))
             end)
    end

    test "ignores a sealed segment, and one whose primary is dead" do
      {sealed, _} = topic_with([{0, [:a, :b], 0, 3}])
      assert OrphanedFence.candidates(sealed, [:a, :b]) == []

      {metadata, _} = active_segment([:a, :b, :c])
      assert OrphanedFence.candidates(metadata, [:b, :c]) == []
    end

    test "is disjoint from Failover.candidates/2, so neither pass can act on the other's segments" do
      # The same metadata, the same live set: a segment belongs to exactly one of the two policies,
      # decided by whether its primary answers. Sealing an orphaned fence uses the primary's own
      # answer and needs no majority; failover cannot, which is why the split must be total.
      {metadata, id} = active_segment([:a, :b, :c])

      assert OrphanedFence.candidates(metadata, [:a, :b]) == [{:a, [{id, 0}]}]
      assert Failover.candidates(metadata, [:a, :b]) == []

      assert OrphanedFence.candidates(metadata, [:b, :c]) == []
      assert Failover.candidates(metadata, [:b, :c]) == [{id, [:b, :c]}]
    end

    test "skips a segment with no replica set at all, which has no primary to ask" do
      {metadata, _id} = active_segment([])

      assert OrphanedFence.candidates(metadata, [:a, :b]) == []
    end

    test "answers nothing on empty metadata" do
      assert OrphanedFence.candidates(Metadata.new(), [:a]) == []
    end
  end

  describe "plan/3 (recording the seal the fence already made binding)" do
    test "seals at the length the STORE reported, measured from the control plane's start offset" do
      # The segment starts at 5 and the fenced store ended at 9, so the sealed LENGTH is 4. Taking the
      # end offset for the length is the mistake this asserts against: it would claim records 0..8 on a
      # segment that only ever held 5..8, and a read would stop dead in the middle of the range.
      {metadata, [_sealed, active]} = topic_with([{0, [:a], 0, 5}, {1, [:a], 5, nil}])

      assert OrphanedFence.plan(metadata, %{active => {9, 640}}, @now) ==
               [{:seal_segment, active, 4, 640, @now}]
    end

    test "seals a fenced segment that never took a record at zero length" do
      # The ordinary shape of a split fencing a range whose head was registered and never written.
      {metadata, id} = active_segment([:a, :b])

      assert OrphanedFence.plan(metadata, %{id => {0, 0}}, @now) == [{:seal_segment, id, 0, 0, @now}]
    end

    test "emits nothing for a segment that answered nothing: no answer, no seal" do
      {metadata, _id} = active_segment([:a, :b])

      assert OrphanedFence.plan(metadata, %{}, @now) == []
    end

    test "does not re-seal a segment the metadata already sealed" do
      # The race with a normal split, from the losing side: the splitter's own `record_seal/5` landed
      # between this pass probing and planning. A second seal command would be refused anyway, but
      # emitting it would make every pass report work it did not do.
      {metadata, [id]} = topic_with([{0, [:a], 0, 3}])

      assert OrphanedFence.plan(metadata, %{id => {3, 90}}, @now) == []
    end

    test "skips a segment retention dropped between the probe and the plan" do
      {metadata, [id]} = topic_with([{0, [:a], 0, 4}])
      {metadata, :ok} = Metadata.apply(metadata, {:delete_segment, id})

      assert OrphanedFence.plan(metadata, %{id => {4, 40}}, @now) == []
    end

    test "is deterministic: the same answers plan the same commands in the same order" do
      {metadata, [first, second]} =
        metadata_with([{"events", [{0, [:a], 0, nil}]}, {"orders", [{0, [:b], 0, nil}]}])

      answers = %{second => {2, 20}, first => {1, 10}}

      planned = OrphanedFence.plan(metadata, answers, @now)

      assert planned == [{:seal_segment, first, 1, 10, @now}, {:seal_segment, second, 2, 20, @now}]
      assert planned == OrphanedFence.plan(metadata, answers, @now)
    end

    test "never moves the replica set, unlike failover: the primary is alive and holds everything" do
      {metadata, id} = active_segment([:a, :b, :c])

      assert [{:seal_segment, ^id, _length, _bytes, @now}] = OrphanedFence.plan(metadata, %{id => {1, 10}}, @now)
    end
  end
end
