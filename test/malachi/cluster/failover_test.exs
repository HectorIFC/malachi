defmodule Malachi.Cluster.FailoverTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Cluster.Failover
  alias Malachi.Cluster.ReplicaTracker
  alias Malachi.Metadata

  doctest Malachi.Cluster.Failover

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

  describe "a segment placed below the replication factor (#270)" do
    # A roll while a broker is down places the next segment on the brokers left: two, at replication
    # factor 3. Its acknowledgements need both, so either one alone holds every acknowledged write.
    test "two replicas with the primary dead: sealed on the survivor, which becomes the head" do
      {metadata, segment_id} = segment([:a, :b], false)

      assert Failover.candidates(metadata, [:b, :c]) == [{segment_id, [:b]}]

      assert Failover.plan(metadata, [:b, :c], probes(segment_id, %{b: {7, 700}}), @now) == [
               {:seal_segment, segment_id, 7, 700, @now},
               {:set_segment_replicas, segment_id, [:b, :a]}
             ]
    end

    test "two replicas with the follower dead: a candidate although its primary lives, sealed on the primary" do
      # The live primary alone is below the acknowledgement quorum of two, so the range takes no write.
      {metadata, segment_id} = segment([:a, :b], false)

      assert Failover.candidates(metadata, [:a, :c]) == [{segment_id, [:a]}]

      assert Failover.plan(metadata, [:a, :c], probes(segment_id, %{a: {4, 400}}), @now) == [
               {:seal_segment, segment_id, 4, 400, @now},
               {:set_segment_replicas, segment_id, [:a, :b]}
             ]
    end

    test "three replicas with one follower dead still take writes, so the segment is left alone" do
      {metadata, _segment_id} = segment([:a, :b, :c], false)
      assert Failover.candidates(metadata, [:a, :b]) == []
    end

    test "four replicas with the primary and one follower dead: sealed on the two left, not a majority" do
      # Any acknowledgement needed three of four, so two answers share a replica with every one of them.
      {metadata, segment_id} = segment([:a, :b, :c, :d], false)

      assert Failover.plan(metadata, [:c, :d], probes(segment_id, %{c: {5, 500}, d: {6, 600}}), @now) == [
               {:seal_segment, segment_id, 6, 600, @now},
               {:set_segment_replicas, segment_id, [:d, :a, :b, :c]}
             ]
    end

    test "an active segment with an empty replica set is no candidate, and does not break the pass" do
      {metadata, segment_id} = segment([:a], false)
      metadata = put_in(metadata.segments[segment_id].replica_set, [])
      assert Failover.candidates(metadata, [:a]) == []
    end

    test "on a tie for the furthest end, the head is a replica the view counts live" do
      # :c was heard from but the view has it gone, and a plain maximum would pick it (the last of equals);
      # :b is live and holds the same records, so it is the one reads should route to.
      {metadata, segment_id} = segment([:a, :b, :c], false)

      assert [{:seal_segment, ^segment_id, 5, 500, @now}, {:set_segment_replicas, ^segment_id, [:b | _]}] =
               Failover.plan(metadata, [:b], probes(segment_id, %{b: {5, 500}, c: {5, 500}}), @now)
    end

    test "three replicas with both followers dead: a candidate, blocked until one returns" do
      {metadata, segment_id} = segment([:a, :b, :c], false)

      assert Failover.candidates(metadata, [:a]) == [{segment_id, [:a]}]
      assert Failover.plan(metadata, [:a], probes(segment_id, %{a: {4, 400}}), @now) == []
    end
  end

  describe "the seal quorum" do
    # Each replica's log is contiguous, so a replica's end says which offsets it holds. The acknowledged
    # end is the highest offset at least `quorum_size(n)` replicas hold: exactly what the tracker commits.
    property "any answers reaching the seal quorum reach the acknowledged end" do
      check all(
              ends <- list_of(integer(0..20), min_length: 1, max_length: 7),
              answering <- answering_subset(length(ends))
            ) do
        acknowledged = ends |> Enum.sort(:desc) |> Enum.at(ReplicaTracker.quorum_size(length(ends)) - 1)
        answers = Enum.map(answering, &Enum.at(ends, &1))
        replica_set = Enum.to_list(1..length(ends))

        if Failover.seal_quorum?(length(answers), replica_set) do
          assert Enum.max(answers) >= acknowledged
        end
      end
    end

    property "one answer fewer is not enough: some such answers miss an acknowledged write" do
      check all(n <- integer(2..7), ahead <- integer(1..20)) do
        quorum = ReplicaTracker.quorum_size(n)
        # Exactly `quorum` replicas reach `ahead`, which is therefore acknowledged; the rest hold nothing.
        ends = List.duplicate(ahead, quorum) ++ List.duplicate(0, n - quorum)
        acknowledged = ends |> Enum.sort(:desc) |> Enum.at(quorum - 1)
        lagging = Enum.drop(ends, quorum)
        replica_set = Enum.to_list(1..n)

        assert acknowledged == ahead
        # Those answers are one short of the seal quorum, and they would indeed miss the acknowledged end.
        assert length(lagging) == Failover.seal_quorum(replica_set) - 1
        assert Enum.all?(lagging, &(&1 < acknowledged))

        # And the policy itself declines them: the replicas that hold `ahead` are gone (the primary among
        # them), the lagging ones are all that answer, and nothing is sealed on them.
        {metadata, segment_id} = segment(replica_set, false)
        live = Enum.drop(replica_set, quorum)
        answers = Map.new(live, &{&1, {0, 0}})

        # With no one left to answer (two replicas, both of them ahead) there is nothing to probe at all.
        assert Failover.candidates(metadata, live) == if(live == [], do: [], else: [{segment_id, live}])
        assert Failover.plan(metadata, live, probes(segment_id, answers), @now) == []
      end
    end
  end

  defp answering_subset(n) do
    gen all(picks <- list_of(boolean(), length: n)) do
      for {true, index} <- Enum.with_index(picks), do: index
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

    test "rf=2 with a failed copy seals on the other one, which held every acknowledged write" do
      # An acknowledgement at two replicas needed both, so the healthy copy alone covers all of them.
      {rf2, rf2_id} = segment([:a, :b], false)

      assert Failover.plan(rf2, [:a, :b], probes(rf2_id, %{b: {3, 300}}), @now, MapSet.new([{rf2_id, :a}])) == [
               {:seal_segment, rf2_id, 3, 300, @now},
               {:set_segment_replicas, rf2_id, [:b, :a]}
             ]
    end

    test "rf=1 with a failed copy stays blocked: nothing else holds its writes" do
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
