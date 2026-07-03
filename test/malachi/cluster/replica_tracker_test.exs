defmodule Malachi.Cluster.ReplicaTrackerTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Cluster.ReplicaTracker

  describe "construction" do
    test "the primary is the first broker; quorum is a strict majority" do
      tracker = ReplicaTracker.new([:a, :b, :c])
      assert ReplicaTracker.primary(tracker) == :a
      assert ReplicaTracker.quorum(tracker) == 2

      assert ReplicaTracker.quorum(ReplicaTracker.new([:a])) == 1
      assert ReplicaTracker.quorum(ReplicaTracker.new([:a, :b, :c, :d, :e])) == 3
    end

    test "duplicates are ignored; an empty replica set is rejected" do
      assert ReplicaTracker.new([:a, :a, :b]).replica_set == [:a, :b]
      assert_raise ArgumentError, fn -> ReplicaTracker.new([]) end
    end
  end

  describe "ack/3 and commit_offset/1" do
    test "nothing commits until a quorum holds an offset" do
      tracker = ReplicaTracker.new([:a, :b, :c])
      assert ReplicaTracker.commit_offset(tracker) == :none

      # only the primary has offset 5 → one replica, below quorum of 2
      {:ok, tracker} = ReplicaTracker.ack(tracker, :a, 5)
      assert ReplicaTracker.commit_offset(tracker) == :none

      # a follower catches up to 3 → two replicas hold 3, so 3 commits
      {:ok, tracker} = ReplicaTracker.ack(tracker, :b, 3)
      assert ReplicaTracker.commit_offset(tracker) == 3

      # the last follower reaches 5 → two replicas hold 5, so 5 commits
      {:ok, tracker} = ReplicaTracker.ack(tracker, :c, 5)
      assert ReplicaTracker.commit_offset(tracker) == 5
      assert ReplicaTracker.committed?(tracker, 5)
      refute ReplicaTracker.committed?(tracker, 6)
    end

    test "a single-replica segment commits on the primary's ack alone" do
      tracker = ReplicaTracker.new([:a])
      {:ok, tracker} = ReplicaTracker.ack(tracker, :a, 7)
      assert ReplicaTracker.commit_offset(tracker) == 7
    end

    test "a stale ack does not lower a replica's progress" do
      tracker = ReplicaTracker.new([:a, :b])
      {:ok, tracker} = ReplicaTracker.ack(tracker, :a, 5)
      {:ok, tracker} = ReplicaTracker.ack(tracker, :a, 2)
      assert tracker.match[:a] == 5
    end

    test "an ack from a non-replica is rejected" do
      tracker = ReplicaTracker.new([:a, :b])
      assert ReplicaTracker.ack(tracker, :z, 1) == {:error, :unknown_replica}
    end
  end

  describe "properties" do
    property "commit offset is the highest offset held by a quorum of replicas" do
      check all(
              replicas <- uniq_list_of(broker(), min_length: 1, max_length: 7),
              assignment <- match_assignment(replicas)
            ) do
        tracker = build(replicas, assignment)
        quorum = ReplicaTracker.quorum(tracker)
        values = Enum.map(replicas, fn broker -> Map.get(assignment, broker, -1) end)

        # independent definition: the largest offset stored on >= quorum replicas, else :none
        expected =
          values
          |> Enum.filter(&(&1 >= 0))
          |> Enum.uniq()
          |> Enum.filter(fn offset -> Enum.count(values, &(&1 >= offset)) >= quorum end)
          |> Enum.max(fn -> :none end)

        assert ReplicaTracker.commit_offset(tracker) == expected
      end
    end

    property "commit offset never decreases as acks are applied" do
      check all(
              replicas <- uniq_list_of(broker(), min_length: 1, max_length: 7),
              acks <- list_of({member_of(replicas), offset()}, max_length: 40)
            ) do
        {_tracker, history} =
          Enum.reduce(acks, {ReplicaTracker.new(replicas), [ReplicaTracker.new(replicas)]}, fn
            {broker, offset}, {tracker, history} ->
              {:ok, tracker} = ReplicaTracker.ack(tracker, broker, offset)
              {tracker, [tracker | history]}
          end)

        commits = history |> Enum.reverse() |> Enum.map(&as_int(ReplicaTracker.commit_offset(&1)))
        assert commits == Enum.sort(commits), "commit offset went backwards: #{inspect(commits)}"
      end
    end

    property "an offset is committed iff a quorum of replicas hold at least it" do
      check all(
              replicas <- uniq_list_of(broker(), min_length: 1, max_length: 7),
              assignment <- match_assignment(replicas),
              probe <- offset()
            ) do
        tracker = build(replicas, assignment)
        quorum = ReplicaTracker.quorum(tracker)
        holders = Enum.count(replicas, fn broker -> Map.get(assignment, broker, -1) >= probe end)

        assert ReplicaTracker.committed?(tracker, probe) == holders >= quorum
      end
    end
  end

  # --- helpers ---

  defp broker, do: integer(1..100)
  defp offset, do: integer(0..50)

  # A partial map of broker => acked offset (some replicas may have acked nothing).
  defp match_assignment(replicas) do
    bind(list_of(offset(), length: length(replicas)), fn offsets ->
      bind(list_of(boolean(), length: length(replicas)), fn present ->
        constant(present_offsets(replicas, offsets, present))
      end)
    end)
  end

  # Zips replicas with their offsets, keeping only those flagged present, as a broker => offset map.
  defp present_offsets(replicas, offsets, present) do
    [replicas, offsets, present]
    |> Enum.zip()
    |> Enum.flat_map(fn {broker, offset, present?} -> if present?, do: [{broker, offset}], else: [] end)
    |> Map.new()
  end

  defp build(replicas, assignment) do
    Enum.reduce(assignment, ReplicaTracker.new(replicas), fn {broker, offset}, tracker ->
      {:ok, tracker} = ReplicaTracker.ack(tracker, broker, offset)
      tracker
    end)
  end

  defp as_int(:none), do: -1
  defp as_int(offset), do: offset
end
