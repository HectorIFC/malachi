Code.require_file("../../../benchmark/support/scale_sweep.exs", __DIR__)

defmodule Malachi.Bench.ScaleSweepTest do
  use ExUnit.Case, async: true

  alias Malachi.Bench.ScaleSweep

  defp cell(n, agg), do: %{n: n, agg: agg, per: round(agg / n), eff: nil}

  describe "ladder/3" do
    test "an unset variable takes the default" do
      assert ScaleSweep.ladder("SCALE_NS", nil, [1, 2, 4, 8]) == {:ok, [1, 2, 4, 8]}
    end

    test "a set variable is taken in the order given, with any whitespace between values" do
      assert ScaleSweep.ladder("SCALE_NS", "8 1", [1]) == {:ok, [8, 1]}
      assert ScaleSweep.ladder("SCALE_BATCHES", " 10\t100\n1000 ", [1]) == {:ok, [10, 100, 1000]}
      # An explicit sign is still a positive integer.
      assert ScaleSweep.ladder("SCALE_NS", "+3", [1]) == {:ok, [3]}
    end

    for {value, reason} <- [
          {"", "is empty"},
          {"   ", "is empty"},
          {"1 x", ~s(has "x", not a positive integer)},
          {"0", ~s(has "0", not a positive integer)},
          {"-2", ~s(has "-2", not a positive integer)},
          {"1.5", ~s(has "1.5", not a positive integer)},
          {"2 1 2", "repeats a value"}
        ] do
      test "#{inspect(value)} is refused, naming the variable" do
        assert ScaleSweep.ladder("SCALE_NS", unquote(value), [1]) ==
                 {:error, "SCALE_NS=#{inspect(unquote(value))} #{unquote(reason)}; expected distinct positive integers"}
      end
    end
  end

  describe "with_efficiency/1" do
    test "is measured against the smallest N, even when it ran last" do
      assert [%{n: 4, eff: 50.0}, %{n: 2, eff: 75.0}, %{n: 1, eff: 100.0}] =
               ScaleSweep.with_efficiency([cell(4, 200), cell(2, 150), cell(1, 100)])
    end

    test "the smallest N need not be 1" do
      assert [%{n: 2, eff: 100.0}, %{n: 8, eff: 50.0}] = ScaleSweep.with_efficiency([cell(2, 200), cell(8, 400)])
    end

    test "an empty block is not a block" do
      assert_raise FunctionClauseError, fn -> ScaleSweep.with_efficiency([]) end
    end
  end

  describe "crossing/2" do
    test "is the smallest N that reached the target, whatever order the ladder was given in" do
      cells = [cell(8, 1_500_000), cell(1, 1_000_000), cell(2, 1_200_000)]

      assert %{n: 1} = ScaleSweep.crossing(cells, 1_000_000)
    end

    test "skips a smaller N that fell short" do
      cells = [cell(1, 999_999), cell(4, 1_000_001), cell(2, 900_000), cell(8, 2_000_000)]

      assert %{n: 4, agg: 1_000_001} = ScaleSweep.crossing(cells, 1_000_000)
    end

    test "a curve that bends back down still reports where it first crossed" do
      cells = [cell(1, 400_000), cell(2, 1_100_000), cell(4, 900_000), cell(8, 1_050_000)]

      assert %{n: 2} = ScaleSweep.crossing(cells, 1_000_000)
    end

    test "is nil when no N reached it" do
      assert ScaleSweep.crossing([cell(1, 10), cell(2, 20)], 1_000_000) == nil
      assert ScaleSweep.crossing([], 1_000_000) == nil
    end
  end
end
