defmodule Malachi.Consumer.AssignmentTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Consumer.Assignment

  # Flattens %{member => [range]} back to the set of assigned ranges and the per-member counts.
  defp assigned_ranges(assignment), do: assignment |> Map.values() |> List.flatten()
  defp counts(assignment), do: assignment |> Map.values() |> Enum.map(&length/1)

  # which member owns each range, as %{range => member}
  defp owners(assignment) do
    for {member, ranges} <- assignment, range <- ranges, into: %{}, do: {range, member}
  end

  describe "edge cases" do
    test "no members yields an empty assignment" do
      assert Assignment.assign([:r1, :r2], []) == %{}
    end

    test "no ranges yields every member mapped to an empty list (idle, not unknown)" do
      assert Assignment.assign([], [:a, :b]) == %{a: [], b: []}
    end

    test "a member that gets no ranges still appears" do
      # one range, two members: one owns it, the other is present but idle
      assignment = Assignment.assign([:r1], [:a, :b])
      assert Enum.sort(Map.keys(assignment)) == [:a, :b]
      assert assigned_ranges(assignment) == [:r1]
      assert Enum.sort(counts(assignment)) == [0, 1]
    end

    test "duplicate ranges and members are ignored" do
      assignment = Assignment.assign([:r1, :r1, :r2], [:a, :a, :b])
      assert Enum.sort(assigned_ranges(assignment)) == [:r1, :r2]
      assert Enum.sort(Map.keys(assignment)) == [:a, :b]
    end
  end

  describe "properties" do
    defp ranges, do: uniq_list_of(integer(1..1000), min_length: 0, max_length: 30)
    defp members, do: uniq_list_of(atom(:alphanumeric), min_length: 1, max_length: 6)

    property "every range is assigned to exactly one member (a partition of the ranges)" do
      check all(rs <- ranges(), ms <- members()) do
        assignment = Assignment.assign(rs, ms)
        assigned = assigned_ranges(assignment)
        assert Enum.sort(assigned) == Enum.sort(Enum.uniq(rs))
        assert length(assigned) == length(Enum.uniq(assigned))
        assert Enum.all?(Map.keys(assignment), &(&1 in ms))
      end
    end

    property "deterministic: independent of the order of ranges and members" do
      check all(rs <- ranges(), ms <- members()) do
        assert Assignment.assign(rs, ms) == Assignment.assign(Enum.shuffle(rs), Enum.shuffle(ms))
      end
    end

    property "sticky on leave: every surviving member keeps all of its ranges (only the leaver's move)" do
      check all(
              rs <- uniq_list_of(integer(1..1000), min_length: 1, max_length: 30),
              ms <- uniq_list_of(atom(:alphanumeric), min_length: 2, max_length: 6)
            ) do
        before = owners(Assignment.assign(rs, ms))
        leaver = hd(ms)
        after_leave = owners(Assignment.assign(rs, ms -- [leaver]))

        for r <- Enum.uniq(rs), before[r] != leaver do
          assert after_leave[r] == before[r], "range #{r} moved off a surviving member"
        end
      end
    end

    property "sticky on join: every range is unchanged or moves to the new member (never traded)" do
      check all(rs <- ranges(), ms <- members()) do
        # a fresh member guaranteed not already in ms (ms are atoms)
        new = {:new_member, System.unique_integer([:positive])}
        before = owners(Assignment.assign(rs, ms))
        after_join = owners(Assignment.assign(rs, ms ++ [new]))

        for r <- Enum.uniq(rs) do
          assert after_join[r] == before[r] or after_join[r] == new
        end
      end
    end
  end
end
