defmodule Malachi.Cluster.FailoverTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.Failover
  alias Malachi.Metadata

  # Epoch milliseconds, the shape `System.system_time(:millisecond)` produces, which is what the caller
  # passes as `now_ms` and what `sealed_at` carries into retention. Fixed rather than read from the
  # clock because the seal command has to be byte-identical on every replica that applies it, and the
  # tests assert the value travels through untouched. Thirteen digits on purpose: a small stand-in like
  # 0 would read as a valid timestamp from 1970 and let an age calculation pass by accident.
  @now 1_700_000_000_000

  defp segment(replica_set, seal?) do
    {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 8})
    segment_id = {root, 0}
    {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, segment_id, replica_set, 0})

    metadata =
      if seal?, do: elem(Metadata.apply(metadata, {:seal_segment, segment_id, 1, 0, 0}), 0), else: metadata

    {metadata, segment_id}
  end

  # The probe result the coordinator gathers: per segment, what each live replica durably holds.
  defp probes(segment_id, ends), do: %{segment_id => ends}

  describe "candidates/2 (what the coordinator must probe)" do
    test "names the active segments whose primary is dead, with their live replicas" do
      {metadata, segment_id} = segment([:a, :b, :c], false)

      assert Failover.candidates(metadata, [:b, :c, :d]) == [{segment_id, [:b, :c]}]
    end

    test "ignores a segment whose primary is alive, a sealed one, and one with no live replica" do
      {alive, _} = segment([:a, :b, :c], false)
      assert Failover.candidates(alive, [:a, :b, :c]) == []

      {sealed, _} = segment([:a, :b, :c], true)
      assert Failover.candidates(sealed, [:b, :c]) == []

      {metadata, _} = segment([:a, :b, :c], false)
      assert Failover.candidates(metadata, [:d]) == []
    end
  end

  describe "a copy that failed in storage (candidates/3, plan/5, failure_probes/2)" do
    test "a LIVE primary whose copy failed makes the segment a candidate, and that copy is not probed" do
      {metadata, segment_id} = segment([:a, :b, :c], false)
      failed = MapSet.new([{segment_id, :a}])

      assert Failover.candidates(metadata, [:a, :b, :c], failed) == [{segment_id, [:b, :c]}]
    end

    test "a follower whose copy failed makes the segment a candidate too, as any failed replica does in NorthGuard" do
      {metadata, segment_id} = segment([:a, :b, :c], false)
      failed = MapSet.new([{segment_id, :c}])

      assert Failover.candidates(metadata, [:a, :b, :c], failed) == [{segment_id, [:a, :b]}]
    end

    test "a failed copy of another segment, or of a sealed one, makes nothing a candidate" do
      {metadata, _segment_id} = segment([:a, :b, :c], false)
      assert Failover.candidates(metadata, [:a, :b, :c], MapSet.new([{{:another, 0}, :a}])) == []

      {sealed, sealed_id} = segment([:a, :b, :c], true)
      assert Failover.candidates(sealed, [:a, :b, :c], MapSet.new([{sealed_id, :a}])) == []
    end

    test "rf=3, primary's copy failed: seals on the other two at the furthest end and moves the holder to the head" do
      {metadata, segment_id} = segment([:a, :b, :c], false)
      failed = MapSet.new([{segment_id, :a}])

      assert Failover.plan(metadata, [:a, :b, :c], probes(segment_id, %{b: {2, 200}, c: {5, 500}}), @now, failed) ==
               [
                 {:seal_segment, segment_id, 5, 500, @now},
                 {:set_segment_replicas, segment_id, [:c, :a, :b]}
               ]
    end

    test "rf=3, a follower's copy failed: seals on the primary and the healthy follower" do
      {metadata, segment_id} = segment([:a, :b, :c], false)
      failed = MapSet.new([{segment_id, :c}])

      assert [{:seal_segment, ^segment_id, 4, 400, @now}, {:set_segment_replicas, ^segment_id, [head | _]}] =
               Failover.plan(metadata, [:a, :b, :c], probes(segment_id, %{a: {4, 400}, b: {4, 400}}), @now, failed)

      assert head in [:a, :b]
    end

    test "an answer that came from a failed copy neither counts toward the majority nor becomes the head" do
      {metadata, segment_id} = segment([:a, :b, :c], false)

      # :a failed yet its stale answer claims the furthest end. It must not be the seal point or the head.
      ends = %{a: {9, 900}, b: {2, 200}, c: {5, 500}}

      assert Failover.plan(metadata, [:a, :b, :c], probes(segment_id, ends), @now, MapSet.new([{segment_id, :a}])) ==
               [
                 {:seal_segment, segment_id, 5, 500, @now},
                 {:set_segment_replicas, segment_id, [:c, :a, :b]}
               ]

      # With two copies failed only one answer is left, which is no majority, however far it reaches.
      two_failed = MapSet.new([{segment_id, :a}, {segment_id, :b}])
      assert Failover.plan(metadata, [:a, :b, :c], probes(segment_id, ends), @now, two_failed) == []
    end

    test "rf=2 and rf=1 with a failed copy stay blocked: without it there is no majority" do
      {rf2, rf2_id} = segment([:a, :b], false)
      assert Failover.plan(rf2, [:a, :b], probes(rf2_id, %{b: {3, 300}}), @now, MapSet.new([{rf2_id, :a}])) == []

      {rf1, rf1_id} = segment([:a], false)
      assert Failover.candidates(rf1, [:a], MapSet.new([{rf1_id, :a}])) == []
      assert Failover.plan(rf1, [:a], %{}, @now, MapSet.new([{rf1_id, :a}])) == []
    end

    test "failure_probes/2 asks each live replica once, about every segment it holds" do
      {metadata, events} = segment([:a, :b, :c], false)
      {metadata, {:ok, orders_root}} = Metadata.apply(metadata, {:create_topic, "orders", 8})
      orders = {orders_root, 0}
      {metadata, :ok} = Metadata.apply(metadata, {:register_segment, orders_root, orders, [:b, :d], 0})

      # :d is not live, so it is not asked; :b holds both segments and is asked about both in one entry.
      assert Failover.failure_probes(metadata, [:a, :b, :c]) ==
               [{:a, [events]}, {:b, Enum.sort([events, orders])}, {:c, [events]}]
    end

    test "failure_probes/2 asks about sealed segments too: a failed sealed copy is a lost replica to replace" do
      # Never a failover candidate (see the candidates/3 test above), but SelfHealing needs to know.
      {sealed, segment_id} = segment([:a, :b, :c], true)

      assert Failover.failure_probes(sealed, [:a, :b]) == [{:a, [segment_id]}, {:b, [segment_id]}]
    end
  end

  describe "plan/4 (seal-and-roll)" do
    test "seals at the highest durable end reported, not at the first live replica's" do
      # :a (primary) is dead. :b is first in replica-set order but holds only 2 records; :c holds 5.
      # Promoting :b was the bug: it would reopen offsets 2..4, which were already acknowledged on :c.
      # Sealing at 5 keeps every acknowledged offset assigned exactly once, forever.
      {metadata, segment_id} = segment([:a, :b, :c], false)
      ends = %{b: {2, 200}, c: {5, 500}}

      # Two commands, in this order: seal at the furthest end, then move that same replica to the head
      # so reads of the sealed segment do not keep routing at the dead broker.
      assert Failover.plan(metadata, [:b, :c], probes(segment_id, ends), @now) ==
               [
                 {:seal_segment, segment_id, 5, 500, @now},
                 {:set_segment_replicas, segment_id, [:c, :a, :b]}
               ]
    end

    test "the majority-th largest end is NOT the seal point, because the dead primary cannot answer" do
      # The rule that looks right and is not: with :a dead, a record :a acknowledged together with :c
      # alone sits above what a majority of the ANSWERS ({:b, :c}) agree on. Sealing at :b's end would
      # discard an acknowledged write, so the maximum is both safe and the lowest safe choice.
      {metadata, segment_id} = segment([:a, :b, :c], false)
      ends = %{b: {2, 200}, c: {5, 500}}
      majority_of_answers = 2

      assert [{:seal_segment, ^segment_id, length, _bytes, @now} | _] =
               Failover.plan(metadata, [:b, :c], probes(segment_id, ends), @now)

      assert length > majority_of_answers
    end

    test "the sealed length counts records from the segment's start offset, not from zero" do
      # Sealed first, because a range has one write head: the roll that opens the segment under test is
      # exactly what seals the one before it.
      {metadata, {root, _} = first_id} = segment([:a, :b, :c], false)
      {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, first_id, 10, 1_000, 0})
      segment_id = {root, 1}
      {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, segment_id, [:a, :b, :c], 10})

      # A segment based at 10 whose furthest replica ends at 14 holds 4 records, not 14.
      ends = %{b: {14, 400}, c: {12, 200}}
      commands = Failover.plan(metadata, [:b, :c], probes(segment_id, ends), @now)

      assert {:seal_segment, ^segment_id, 4, 400, @now} = List.keyfind(commands, segment_id, 1)
    end

    test "emits nothing when fewer than a majority of the replica set answered" do
      # Only one of three replicas reporting cannot establish the committed end: an acknowledged write
      # lives on a majority, so a lone survivor may be the replica that missed it. Sealing at its end
      # would discard exactly what this mechanism exists to protect, so the range stays blocked.
      {metadata, segment_id} = segment([:a, :b, :c], false)

      assert Failover.plan(metadata, [:b], probes(segment_id, %{b: {2, 200}}), @now) == []
    end

    test "a replica that is live but did not answer the probe does not count toward the majority" do
      # Live is not the same as reachable in time: the probe has a timeout, and a silent replica tells
      # us nothing about what it holds.
      {metadata, segment_id} = segment([:a, :b, :c], false)

      assert Failover.plan(metadata, [:b, :c], probes(segment_id, %{b: {2, 200}}), @now) == []
    end

    test "seals with the majority present even when one replica is silent" do
      {metadata, segment_id} = segment([:a, :b, :c], false)
      ends = %{b: {2, 200}, c: {5, 500}}

      assert [{:seal_segment, ^segment_id, 5, 500, @now}, {:set_segment_replicas, ^segment_id, [:c | _]}] =
               Failover.plan(metadata, [:b, :c, :d], probes(segment_id, ends), @now)
    end

    test "an empty segment seals at zero length rather than being left blocked" do
      # Nothing was acknowledged yet, so there is nothing to lose; rolling to a fresh segment restores
      # writes immediately.
      {metadata, segment_id} = segment([:a, :b, :c], false)
      ends = %{b: {0, 0}, c: {0, 0}}

      assert [{:seal_segment, ^segment_id, 0, 0, @now}, {:set_segment_replicas, ^segment_id, _set}] =
               Failover.plan(metadata, [:b, :c], probes(segment_id, ends), @now)
    end

    test "no candidates means no commands" do
      {metadata, _segment_id} = segment([:a, :b, :c], false)
      assert Failover.plan(metadata, [:a, :b, :c], %{}, @now) == []
    end

    test "sealed segments are left to self-healing" do
      {metadata, segment_id} = segment([:a, :b, :c], true)
      assert Failover.plan(metadata, [:b, :c], probes(segment_id, %{b: {1, 100}, c: {1, 100}}), @now) == []
    end
  end
end
