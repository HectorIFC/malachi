# The analysis half of every A/B that interleaves by RUN: a baseline tree and a branch tree, built apart
# and launched one sample at a time in a shuffled arm order by a shell driver (`support/ab_lib.sh`),
# each sample written to `<results>/<dir>/<rep>-<arm>.out`. Two of the three arms are the baseline run
# twice under different labels, the A-A control. The verdict rule itself lives in `paired_stats.exs`.
#
# It was born inside store_error_path_ab.exs (issue #147) and moved here when the rate limiter A/B
# (issue #151) needed the same loading, report and exit codes.
#
#   Code.require_file("support/ab_run.exs", __DIR__)

Code.require_file("paired_stats.exs", __DIR__)

defmodule Malachi.Bench.ABRun do
  alias Malachi.Bench.PairedStats

  # How a sample finds its way out of `mix run` output, which also carries compiler and app noise.
  @marker "AB_SAMPLE "
  @arms [:main_a1, :main_a2, :branch]
  @control {:main_a1, :main_a2}
  # Below this many repetitions per arm there is no verdict at all. The #147 harness's own smoke run showed
  # why: with one repetition the bootstrap resamples a single value, its interval collapses to a point,
  # "excludes zero" becomes true for any nonzero difference, and one sample was reported as SIGNAL.
  @min_reps 5

  @doc "Prints `payload` as the one marked line a sample leaves in its output."
  def emit(payload), do: IO.puts(@marker <> Jason.encode!(payload))

  @doc "The decoded payload of the marked line in `output`; raises when there is none."
  def marked(output) do
    line = output |> String.split("\n") |> Enum.find(&String.starts_with?(&1, @marker))
    unless line, do: raise("sample without an #{String.trim(@marker)} line:\n#{output}")
    line |> String.replace_prefix(@marker, "") |> Jason.decode!()
  end

  @doc """
  Reads every sample under `results` for `cases` (`[{name, dir, parse}]`, where `parse` turns one sample's
  output into `%{p50, p99}`), prints the verdicts and writes them with `extra` to `out`. Halts with status
  1 when the branch regressed, and with status 2 when there is no verdict: a case in `expected` produced
  no samples, or too few repetitions. Either way a CI step fails.
  """
  def analyze(results, out, expected, cases, extra \\ %{}) do
    evaluated =
      for {name, dir, parse} <- cases,
          samples = load(results, dir, parse),
          samples != :missing do
        IO.puts("\n  case: #{name}")

        verdicts = [
          PairedStats.verdict({"branch vs main", :branch, :main_a1}, samples, @control),
          PairedStats.verdict({"A-A control (main vs main)", :main_a2, :main_a1}, samples, @control)
        ]

        PairedStats.report(samples, verdicts)
        reps = samples |> Map.values() |> Enum.map(&length/1) |> Enum.min()

        if reps < @min_reps do
          IO.puts("    INSUFFICIENT: #{reps} repetitions per arm, a verdict needs at least #{@min_reps}")
        end

        %{
          case: name,
          reps: reps,
          sufficient: reps >= @min_reps,
          arms: Map.new(samples, fn {arm, values} -> {arm, PairedStats.summary(values)} end),
          comparisons: verdicts
        }
      end

    %{verdict: verdict, regressions: regressions, missing: missing, insufficient: insufficient} =
      PairedStats.outcome(evaluated, expected)

    report =
      Map.merge(
        %{
          schema: 1,
          generated_at: DateTime.utc_now() |> DateTime.to_iso8601(),
          otp: :erlang.system_info(:otp_release) |> to_string(),
          schedulers_online: :erlang.system_info(:schedulers_online),
          os: :os.type() |> Tuple.to_list() |> Enum.map_join("/", &to_string/1),
          interleaving: "by run, arm order shuffled per repetition",
          min_reps: @min_reps,
          cases: evaluated,
          expected: expected,
          missing: missing,
          insufficient: insufficient,
          regressions: regressions
        },
        extra
      )

    if out, do: File.write!(out, Jason.encode_to_iodata!(report, pretty: true))

    case verdict do
      :regression ->
        IO.puts("\n  REGRESSION: #{Enum.join(regressions, ", ")}")
        System.halt(1)

      # Not a pass: a run that did not measure a case it was asked to, or measured it too few times, must
      # not read as "no regression" in a CI log.
      :no_verdict ->
        if missing != [], do: IO.puts("\n  NO VERDICT for #{Enum.join(missing, ", ")}: no samples")
        if insufficient != [], do: IO.puts("\n  NO VERDICT for #{Enum.join(insufficient, ", ")}: too few repetitions")
        System.halt(2)

      :pass ->
        IO.puts("\n  no regression by the fixed rule")
    end
  end

  # `:missing` when a case was not run at all, so a partial run still reports the cases it did run. A case
  # that WAS run but is missing an arm is an error: comparing whatever arrived would be a verdict on a
  # different experiment than the one declared.
  defp load(results, dir, parse) do
    files = Path.wildcard(Path.join([results, dir, "*.out"])) |> Enum.reject(&warmup?/1)

    if files == [] do
      :missing
    else
      by_arm = Enum.group_by(files, &arm_of/1)

      for arm <- @arms, not Map.has_key?(by_arm, arm) do
        raise "#{dir} has no samples for arm #{arm} under #{results}"
      end

      # The bootstrap pairs observations by POSITION (`Malachi.Bench.PairedStats`), so position n must be the
      # same repetition in every arm. Sorted by the repetition's number, not by file name, where rep 10 sorts
      # before rep 2; and an arm missing a repetition another arm has is an error, not a shorter list.
      reps = Map.new(by_arm, fn {arm, paths} -> {arm, paths |> Enum.map(&rep_of/1) |> Enum.sort()} end)

      if reps |> Map.values() |> Enum.uniq() |> length() > 1 do
        raise "#{dir} has arms that ran different repetitions under #{results}: #{inspect(reps)}"
      end

      Map.new(by_arm, fn {arm, paths} ->
        {arm, paths |> Enum.sort_by(&rep_of/1) |> Enum.map(&(&1 |> File.read!() |> parse.()))}
      end)
    end
  end

  defp rep_of(path) do
    [rep, _arm] = path |> Path.basename(".out") |> String.split("-", parts: 2)
    String.to_integer(rep)
  end

  defp warmup?(path), do: path |> Path.basename() |> String.starts_with?("warm")

  defp arm_of(path) do
    [_rep, arm] = path |> Path.basename(".out") |> String.split("-", parts: 2)
    String.to_existing_atom(arm)
  end
end
