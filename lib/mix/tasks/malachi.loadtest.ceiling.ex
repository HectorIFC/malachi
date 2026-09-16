defmodule Mix.Tasks.Malachi.Loadtest.Ceiling do
  @shortdoc "Plans the ceiling sweep and elects its peaks from the recorded runs"

  @moduledoc """
  #{@shortdoc}.

  The decisions behind `scripts/loadtest-ceiling.sh`, as three subcommands the script calls. The rules
  themselves are in `Malachi.Loadtest.Ceiling`; this task only reads and writes files.

      mix malachi.loadtest.ceiling plan --batch-ladder "10 100" --conns-ladder "10=32 64" \\
        --conns-ladder "100=16 32" --headline-batch 10 --reps 1 --record-size 256 \\
        --group-commit false --segment-prealloc-bytes 67108864 --out sweep.json

  Validates the sweep, writes it to `--out`, and prints the planned points one per line as
  `<batch> <connections> <repetition>`, in the order they are to run. An invalid sweep prints the
  reason and exits with status 2, which the script passes through.

      mix malachi.loadtest.ceiling peak --run-dir DIR --sweep sweep.json

  Prints the connection count of the headline batch size's peak, so the script can repeat it as the
  A-A control. Exits with status 1 when that batch size has no clean rung.

      mix malachi.loadtest.ceiling summarize --run-dir DIR --sweep sweep.json --out result.json

  Writes the published result to `--out` and prints one line per batch size. The file is written even
  when the headline batch size has no clean rung, and the task then exits with status 1, which is what
  keeps such a run from being published by CI.

  Run files are named by `Malachi.Loadtest.Ceiling.run_file/3` and `aa_file/2`. A file that is absent is
  a repetition that produced nothing; a file that does not parse fails the task, since something wrote
  garbage where a result belongs.
  """

  use Mix.Task

  alias Malachi.Loadtest.Ceiling

  @plan_switches [
    batch_ladder: :string,
    conns_ladder: :keep,
    headline_batch: :string,
    reps: :string,
    record_size: :string,
    group_commit: :string,
    segment_prealloc_bytes: :string,
    out: :string
  ]

  @peak_switches [run_dir: :string, sweep: :string]
  @summarize_switches [run_dir: :string, sweep: :string, out: :string]

  @impl Mix.Task
  def run(["plan" | argv]), do: plan(argv)
  def run(["peak" | argv]), do: peak(argv)
  def run(["summarize" | argv]), do: summarize(argv)
  def run(_argv), do: Mix.raise("usage: mix malachi.loadtest.ceiling plan|peak|summarize [options]")

  defp plan(argv) do
    {opts, _rest} = OptionParser.parse!(argv, strict: @plan_switches)
    out = required!(opts, :out)

    params = %{
      batch_ladder: opts[:batch_ladder],
      conns_ladders: Keyword.get_values(opts, :conns_ladder),
      headline_batch: opts[:headline_batch],
      repetitions: opts[:reps],
      record_size: opts[:record_size],
      group_commit: opts[:group_commit],
      segment_prealloc_bytes: opts[:segment_prealloc_bytes]
    }

    case Ceiling.plan(params) do
      {:ok, sweep} ->
        File.write!(out, Jason.encode!(Ceiling.encode_sweep(sweep), pretty: true))

        Enum.each(Ceiling.run_order(sweep), fn {batch, connections, rep} ->
          Mix.shell().info("#{batch} #{connections} #{rep}")
        end)

      {:error, message} ->
        Mix.shell().error(message)
        exit({:shutdown, 2})
    end
  end

  defp peak(argv) do
    {opts, _rest} = OptionParser.parse!(argv, strict: @peak_switches)
    {run_dir, sweep} = load!(opts)

    case Ceiling.headline_peak(sweep, read_runs!(run_dir, sweep)) do
      {:ok, connections} ->
        Mix.shell().info(Integer.to_string(connections))

      :none ->
        Mix.shell().error("no clean peak at the headline batch size #{sweep.headline_batch}")
        exit({:shutdown, 1})

      {:error, message} ->
        Mix.raise(message)
    end
  end

  defp summarize(argv) do
    {opts, _rest} = OptionParser.parse!(argv, strict: @summarize_switches)
    out = required!(opts, :out)
    {run_dir, sweep} = load!(opts)
    runs = read_runs!(run_dir, sweep)

    aa_result =
      case Ceiling.headline_peak(sweep, runs) do
        {:ok, connections} -> read_result!(Path.join(run_dir, Ceiling.aa_file(sweep.headline_batch, connections)))
        _none_or_error -> nil
      end

    case Ceiling.summarize(sweep, runs, aa_result) do
      {:error, message} ->
        Mix.raise(message)

      {outcome, result} ->
        File.write!(out, Jason.encode!(result, pretty: true))
        Enum.each(Ceiling.summary_lines(result), fn line -> Mix.shell().info(line) end)

        if outcome == :no_headline_peak do
          Mix.shell().error(
            "no clean peak at the headline batch size #{sweep.headline_batch}; #{out} was written but is not a publishable result"
          )

          exit({:shutdown, 1})
        end
    end
  end

  defp required!(opts, key) do
    case opts[key] do
      value when is_binary(value) and value != "" -> value
      _missing -> Mix.raise("--#{key |> Atom.to_string() |> String.replace("_", "-")} is required")
    end
  end

  defp load!(opts) do
    run_dir = required!(opts, :run_dir)
    sweep_path = required!(opts, :sweep)

    unless File.dir?(run_dir), do: Mix.raise("run directory #{run_dir} does not exist")

    json =
      case read_result!(sweep_path) do
        nil -> Mix.raise("cannot read the sweep at #{sweep_path}")
        json -> json
      end

    case Ceiling.decode_sweep(json) do
      {:ok, sweep} -> {run_dir, sweep}
      {:error, message} -> Mix.raise("#{sweep_path} is not a valid sweep: #{message}")
    end
  end

  defp read_runs!(run_dir, sweep) do
    for {batch, connections, rep} <- Ceiling.run_order(sweep) do
      path = Path.join(run_dir, Ceiling.run_file(batch, connections, rep))
      %{batch: batch, connections: connections, rep: rep, result: read_result!(path)}
    end
  end

  # nil for a file that is absent or empty: both are a generator that produced nothing (the harness
  # removes the output of a failed run, and a generator killed before printing leaves an empty one).
  defp read_result!(path) do
    case File.read(path) do
      {:ok, body} -> decode!(path, body)
      {:error, :enoent} -> nil
      {:error, reason} -> Mix.raise("cannot read #{path}: #{inspect(reason)}")
    end
  end

  defp decode!(path, body) do
    if String.trim(body) == "" do
      nil
    else
      case Jason.decode(body) do
        {:ok, %{} = json} -> json
        {:ok, _other} -> Mix.raise("#{path} is not a JSON object")
        {:error, error} -> Mix.raise("#{path} is not valid JSON: #{Exception.message(error)}")
      end
    end
  end
end
