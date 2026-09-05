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

    test "the sealed length counts records from the segment's start offset, not from zero" do
      {metadata, {root, _} = _sid} = segment([:a, :b, :c], false)
      segment_id = {root, 1}
      {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, segment_id, [:a, :b, :c], 10})

      # A segment based at 10 whose replicas end at 14 holds 4 records, not 14.
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
