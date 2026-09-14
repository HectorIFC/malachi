Code.require_file("../../../benchmark/support/paired_stats.exs", __DIR__)

defmodule Malachi.Bench.PairedStatsTest do
  use ExUnit.Case, async: true

  alias Malachi.Bench.PairedStats

  # Fifteen repetitions whose conditions differ a lot from one another, as every arm shares its repetition's
  # page cache and neighbours, and a treatment exactly `shift` microseconds slower than the baseline inside
  # each repetition. The control arm repeats the baseline, so the noise floor is zero.
  defp paired_samples(shift) do
    baseline = for rep <- 1..15, do: %{p50: 100 * rep, p99: 1_000 * rep}
    treatment = Enum.map(baseline, &%{p50: &1.p50 + shift, p99: &1.p99 + shift})
    %{baseline: baseline, control: baseline, treatment: treatment}
  end

  test "the interval is taken over repetitions, so a shift every repetition shows is found" do
    # Resampling each arm on its own mixes the repetitions up, and with this much spread between them the
    # interval spans hundreds of microseconds either side of zero: a consistent 5us slowdown reads as noise.
    # Resampling whole repetitions keeps every draw's difference at exactly the shift.
    %{stats: %{p50: p50, p99: p99}} =
      PairedStats.verdict({"treatment vs baseline", :treatment, :baseline}, paired_samples(5), {:baseline, :control})

    for stat <- [p50, p99] do
      assert stat.delta_us == 5.0
      assert {stat.ci95_low_us, stat.ci95_high_us} == {5.0, 5.0}
      assert stat.excludes_zero
      assert stat.signal
    end
  end

  test "the interval keeps the reported orientation: a faster treatment is below zero" do
    %{stats: %{p50: p50}} =
      PairedStats.verdict({"treatment vs baseline", :treatment, :baseline}, paired_samples(-7), {:baseline, :control})

    assert {p50.delta_us, p50.ci95_low_us, p50.ci95_high_us} == {-7.0, -7.0, -7.0}
  end

  describe "outcome/2 (the verdict of a whole run)" do
    # One evaluated case, shaped like what the A/B analysis builds: its treatment comparison first.
    defp evaluated(name, opts \\ []) do
      stat = %{signal: Keyword.get(opts, :signal, false), delta_us: Keyword.get(opts, :delta_us, 1.0)}

      %{
        case: name,
        sufficient: Keyword.get(opts, :sufficient, true),
        comparisons: [%{stats: %{p50: stat, p99: %{signal: false, delta_us: 0.0}}}, %{stats: %{}}]
      }
    end

    test "a run that evaluated nothing has no verdict, it does not pass" do
      # THE FINDING (CodeRabbit on #153), reproduced first: an empty results directory, or one holding only
      # warmups, used to print no regression by the fixed rule.
      assert PairedStats.outcome([], ["store"]) ==
               %{verdict: :no_verdict, regressions: [], missing: ["store"], insufficient: []}
    end

    test "a case that was asked for and left no samples leaves the run without a verdict" do
      assert %{verdict: :no_verdict, missing: ["e2e"]} = PairedStats.outcome([evaluated("store")], ["store", "e2e"])
    end

    test "a case that was not asked for may be absent" do
      assert %{verdict: :pass, missing: []} = PairedStats.outcome([evaluated("store")], ["store"])
    end

    test "too few repetitions leave the run without a verdict" do
      assert %{verdict: :no_verdict, insufficient: ["store"]} =
               PairedStats.outcome([evaluated("store", sufficient: false)], ["store"])
    end

    test "a regression is reported even when another case is missing, and a faster SIGNAL is not one" do
      slower = evaluated("store", signal: true, delta_us: 12.0)
      assert %{verdict: :regression, regressions: ["store p50"]} = PairedStats.outcome([slower], ["store", "e2e"])

      faster = evaluated("store", signal: true, delta_us: -12.0)
      assert %{verdict: :pass, regressions: []} = PairedStats.outcome([faster], ["store"])
    end
  end

  test "arms that did not run the same number of repetitions are refused, not truncated" do
    samples = %{paired_samples(5) | treatment: Enum.drop(paired_samples(5).treatment, 1)}

    assert_raise ArgumentError, ~r/one observation per repetition/, fn ->
      PairedStats.verdict({"treatment vs baseline", :treatment, :baseline}, samples, {:baseline, :control})
    end
  end
end
