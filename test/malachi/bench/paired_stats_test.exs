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

  test "arms that did not run the same number of repetitions are refused, not truncated" do
    samples = %{paired_samples(5) | treatment: Enum.drop(paired_samples(5).treatment, 1)}

    assert_raise ArgumentError, ~r/one observation per repetition/, fn ->
      PairedStats.verdict({"treatment vs baseline", :treatment, :baseline}, samples, {:baseline, :control})
    end
  end
end
