# The analysis half of benchmark/loadtest_ab.sh (issue #192): two paired A/Bs of the load generators
# themselves, with the arms interleaved by run in a shuffled order and an A-A control, judged by the fixed
# rule of `support/paired_stats.exs`.
#
#   node-runtime  main_a1, main_a2 = Node 20; branch = Node 22. Constant bytes, the published regime.
#   payload       main_a1, main_a2 = constant bytes; branch = json. Per generator and batch size.
#
# One sample is one generator run's JSON report. The statistics run on its request latency (p50 and p99,
# lower is better), which in a closed loop at a fixed connection count is the other face of its
# throughput; the median records per second and every backpressure counter of every arm are printed and
# written beside the verdicts. A case where any sample recorded an error, a drop or a shed is not judged
# at all: a refused produce answers fast, so it shortens the latency of whichever arm hit it. The first
# run of this harness found exactly that, the server's tmpfs filling (`{:storage, :enospc}`) in both
# constant arms at batch 100, which read as json costing 30% more.
#
#   AB_MODE=analyze AB_RESULTS=dir AB_OUT=file mix run --no-start benchmark/loadtest_ab.exs

Code.require_file("support/ab_run.exs", __DIR__)

defmodule LoadtestAB do
  alias Malachi.Bench.ABRun
  alias Malachi.Bench.PairedStats

  @backpressure ~w(errors dropped overloaded rate_limited reconnects)

  def analyze(results, out) do
    dirs = results |> File.ls!() |> Enum.filter(&File.dir?(Path.join(results, &1))) |> Enum.sort()
    throughput = Map.new(dirs, &{&1, throughput(results, &1)})
    {clean, unclean} = Enum.split_with(dirs, fn dir -> Enum.all?(throughput[dir], &clean?/1) end)

    for dir <- unclean do
      IO.puts("\n  NOT JUDGED: #{dir} has samples with errors, drops or sheds; rerun it in a window the server holds")
    end

    # Every case stays expected, so one that is not judged counts as missing: the run ends with no verdict
    # (status 2) rather than reading as no regression.
    cases = for dir <- clean, do: {dir, dir, &parse/1}
    ABRun.analyze(results, out, dirs, cases, %{throughput: throughput, not_judged: unclean})
  end

  defp clean?({_arm, counters}), do: Enum.all?(@backpressure, &(counters[&1] == 0))

  # The Node generator prints its report as indented JSON and nothing else; the Elixir one prints it on one
  # line, after whatever `compose run` printed first.
  defp report(output) do
    case Jason.decode(String.trim(output)) do
      {:ok, report} -> report
      {:error, _not_whole} -> output |> String.split("\n") |> Enum.filter(&String.starts_with?(&1, "{\"")) |> last!()
    end
  end

  defp last!([]), do: raise("sample without a JSON report")
  defp last!(lines), do: lines |> List.last() |> Jason.decode!()

  # PairedStats works in microseconds; the generators report milliseconds.
  defp parse(output) do
    %{"latency_ms" => %{"p50" => p50, "p99" => p99}} = report(output)
    %{p50: p50 * 1000, p99: p99 * 1000}
  end

  defp throughput(results, dir) do
    results
    |> Path.join(dir)
    |> Path.join("*.out")
    |> Path.wildcard()
    |> Enum.reject(&String.contains?(Path.basename(&1), "warm"))
    |> Enum.group_by(&(&1 |> Path.basename(".out") |> String.split("-", parts: 2) |> List.last()))
    |> Map.new(fn {arm, paths} ->
      reports = Enum.map(paths, &(&1 |> File.read!() |> report()))
      rates = Enum.map(reports, & &1["records_per_s"])
      counters = Map.new(@backpressure, fn key -> {key, reports |> Enum.map(&(&1[key] || 0)) |> Enum.sum()} end)
      line = "    #{dir} #{arm}: median #{round(PairedStats.median(rates))} rec/s, #{inspect(counters)}"
      IO.puts(line)
      {arm, Map.put(counters, "median_records_per_s", PairedStats.median(rates))}
    end)
  end
end

case System.get_env("AB_MODE") do
  "analyze" -> LoadtestAB.analyze(System.fetch_env!("AB_RESULTS"), System.get_env("AB_OUT"))
  other -> raise "AB_MODE must be analyze, got #{inspect(other)}"
end
