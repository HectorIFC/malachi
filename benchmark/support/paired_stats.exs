# The statistics behind every paired A/B in this directory, in one place so two experiments cannot
# disagree about what counts as a result. It was born inside PreallocAB (storage_viability.exs, issue
# #83) and moved here when the storage error-path experiment (issue #147) needed the same verdict.
#
# The shape of an experiment it expects: several ARMS, each measured over interleaved repetitions,
# every repetition summarized to its p50/p99, and two of the arms IDENTICAL (the A-A control). The
# spread those two report is the harness's own noise floor, measured rather than assumed, and the
# verdict rule is fixed here, before any number exists: a comparison is SIGNAL only when its delta
# exceeds the control delta AND the bootstrapped 95% interval of the difference excludes zero.
#
#   Code.require_file("support/paired_stats.exs", __DIR__)

defmodule Malachi.Bench.PairedStats do
  @bootstrap_iterations 10_000

  @doc "The `p`th percentile of an already sorted list."
  def pctl(sorted, p) do
    idx = max(0, round(p / 100 * (length(sorted) - 1)))
    Enum.at(sorted, idx)
  end

  def median([]), do: 0.0

  def median(values) do
    sorted = Enum.sort(values)
    count = length(sorted)
    middle = div(count, 2)

    if rem(count, 2) == 1 do
      Enum.at(sorted, middle) * 1.0
    else
      (Enum.at(sorted, middle - 1) + Enum.at(sorted, middle)) / 2
    end
  end

  @doc "One repetition's latencies, reduced to the p50/p99 the statistics run on."
  def summarize(latencies) do
    sorted = Enum.sort(latencies)
    %{p50: pctl(sorted, 50), p99: pctl(sorted, 99)}
  end

  @doc "An arm's repetitions, reduced to the median, min and max of each statistic."
  def summary(values) do
    Map.new([:p50, :p99], fn stat ->
      of_stat = Enum.map(values, & &1[stat])
      {stat, %{median_us: median(of_stat), min_us: Enum.min(of_stat), max_us: Enum.max(of_stat)}}
    end)
  end

  @doc """
  Compares `b_key` (treatment) against `a_key` (baseline) over `samples` (`%{arm => [%{p50, p99}]}`),
  gated by the two identical arms named in `{control_a1, control_a2}`.
  """
  def verdict({label, b_key, a_key}, samples, {control_a1, control_a2} \\ {:control_a1, :control_a2}) do
    stats =
      Map.new([:p50, :p99], fn stat ->
        a = Enum.map(samples[a_key], & &1[stat])
        b = Enum.map(samples[b_key], & &1[stat])
        control_delta = control_delta(samples, stat, control_a1, control_a2)

        delta = median(b) - median(a)
        {low, high} = bootstrap_ci(a, b)

        # Both conditions must hold. The control gate alone would call a tiny but consistent shift
        # signal on a very quiet machine; the interval alone would call a large but erratic one
        # signal on a noisy one. Requiring both is what keeps the answer honest either way.
        beats_control = abs(delta) > control_delta
        excludes_zero = (low > 0 and high > 0) or (low < 0 and high < 0)

        {stat,
         %{
           baseline_median_us: median(a),
           treatment_median_us: median(b),
           delta_us: delta,
           delta_pct: percent(delta, median(a)),
           control_delta_us: control_delta,
           ci95_low_us: low,
           ci95_high_us: high,
           beats_control: beats_control,
           excludes_zero: excludes_zero,
           signal: beats_control and excludes_zero
         }}
      end)

    %{comparison: label, baseline: a_key, treatment: b_key, stats: stats}
  end

  @doc """
  The verdict of a whole run, from the cases it evaluated (`%{case: name, sufficient: boolean, comparisons:
  [treatment_verdict | _]}`) and the case names the run was expected to evaluate.

  A regression is a sufficient case whose treatment comparison is SIGNAL with the treatment slower, and it
  is reported whatever else is wrong with the run. Otherwise the run has NO verdict when an expected case
  produced no samples or too few repetitions: an experiment that did not measure the path it exists for
  must not read as "no regression", which an empty list of findings otherwise would.
  """
  def outcome(cases, expected) do
    present = Enum.map(cases, & &1.case)

    regressions =
      for %{case: name, sufficient: true, comparisons: [treatment | _control]} <- cases,
          {stat, v} <- Enum.sort(treatment.stats),
          v.signal and v.delta_us > 0,
          do: "#{name} #{stat}"

    missing = Enum.reject(expected, &(&1 in present))
    insufficient = for %{case: name, sufficient: false} <- cases, do: name

    verdict =
      cond do
        regressions != [] -> :regression
        missing != [] or insufficient != [] -> :no_verdict
        true -> :pass
      end

    %{verdict: verdict, regressions: regressions, missing: missing, insufficient: insufficient}
  end

  defp control_delta(samples, stat, control_a1, control_a2) do
    a = Enum.map(samples[control_a1], & &1[stat])
    b = Enum.map(samples[control_a2], & &1[stat])
    abs(median(b) - median(a))
  end

  # Percentile bootstrap of the difference of medians, resampling REPETITIONS: each draw takes a whole
  # repetition, both arms' observations of it together, because the design is paired. Every arm runs in
  # every repetition, under that repetition's conditions (page cache, writeback, neighbours), so the two
  # observations of one repetition are not independent. Resampling each arm on its own mixed repetitions
  # up, which throws the pairing away: the interval came out wider than the data supports, and a wider
  # interval is one that includes zero more often, which reads as "no difference" when there is one.
  # Positions must therefore line up across arms (see the callers), and arms of different lengths are
  # refused rather than silently truncated by the zip.
  defp bootstrap_ci(a, b) when length(a) == length(b) do
    pairs = Enum.zip(a, b)

    diffs =
      for _ <- 1..@bootstrap_iterations do
        {sample_a, sample_b} = pairs |> resample() |> Enum.unzip()
        median(sample_b) - median(sample_a)
      end
      |> Enum.sort()

    {pctl(diffs, 2.5), pctl(diffs, 97.5)}
  end

  defp bootstrap_ci(a, b) do
    raise ArgumentError,
          "a paired bootstrap needs one observation per repetition in each arm, got #{length(a)} and #{length(b)}"
  end

  defp resample(samples) do
    count = length(samples)
    for _ <- 1..count, do: Enum.at(samples, :rand.uniform(count) - 1)
  end

  defp percent(_delta, +0.0), do: 0.0
  defp percent(delta, base), do: Float.round(delta / base * 100, 2)

  @doc "Prints each arm's median p50 and every comparison with its verdict."
  def report(samples, verdicts) do
    IO.puts("    -- per-arm medians --")

    for {arm_key, values} <- Enum.sort_by(samples, fn {_key, values} -> median(Enum.map(values, & &1.p50)) end) do
      of_p50 = Enum.map(values, & &1.p50)

      IO.puts(
        "    #{pad(arm_key)} p50 #{us(median(of_p50))}  (spread #{us(Enum.min(of_p50))} to #{us(Enum.max(of_p50))})"
      )
    end

    IO.puts("    -- comparisons --")

    for %{comparison: label, stats: stats} <- verdicts, stat <- [:p50, :p99] do
      v = stats[stat]

      IO.puts(
        "    #{stat} #{label}: #{us(v.baseline_median_us)} -> #{us(v.treatment_median_us)}  " <>
          "delta #{us(v.delta_us)} (#{v.delta_pct}%)  ci95 [#{us(v.ci95_low_us)}, #{us(v.ci95_high_us)}]  " <>
          "noise floor #{us(v.control_delta_us)}  => #{if v.signal, do: "SIGNAL", else: "noise"}"
      )
    end
  end

  def pad(value), do: String.pad_trailing("#{value}", 22)

  defp us(value), do: "#{Float.round(value / 1000, 3)}ms"
end
