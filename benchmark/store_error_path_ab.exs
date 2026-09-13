# Does returning storage I/O failures cost the hot path anything? (issue #147)
#
# #147 replaces the store's hard matches (`:ok = :file.pwrite(...)`) with matches that hand the error
# back, on the exact path every produce pays: `ElixirStore.append/2` and `sync/1`. The expected cost is
# a few nanoseconds against a sync floor measured in tens of microseconds, and "expected" is not a
# measurement, so this compares the code before and after.
#
# Two versions of one module cannot be loaded into one BEAM, so unlike PreallocAB the arms cannot
# interleave inside a process. They interleave by RUN instead: `store_error_path_ab.sh` builds the
# baseline tree and the branch tree, then launches one sample per arm per repetition, in an order
# SHUFFLED per repetition (a rotation keeps every arm behind the same neighbour, which PreallocAB
# caught biasing an identical arm by 850us). Two of the three arms are the baseline tree run twice
# under different labels, the A-A control, and the verdict rule lives in
# `support/paired_stats.exs`, fixed before any number exists: a REGRESSION is a comparison that is
# SIGNAL (delta above the A-A delta AND a 95% CI excluding zero) with the branch slower.
#
# Two cases:
#   * store - `ElixirStore.append + sync` at batch 10 x 256B on a 64MB preallocated segment, the pinned
#     ceiling regime and the production segment shape, one sync per batch.
#   * e2e   - `benchmark/throughput_1m.exs`, 1M records through BrokerServer and ReplicationServer,
#     compared on its produce batch latency.
#
# Modes (normally driven by store_error_path_ab.sh, not by hand):
#   AB_MODE=sample  AB_DIR=/scratch            mix run --no-start benchmark/store_error_path_ab.exs
#   AB_MODE=analyze AB_RESULTS=dir AB_OUT=file mix run --no-start benchmark/store_error_path_ab.exs

Code.require_file("support/paired_stats.exs", __DIR__)

defmodule StoreErrorPathAB do
  alias Malachi.Bench.PairedStats
  alias Malachi.Log.Record
  alias Malachi.Storage.ElixirStore

  @record_bytes 256
  @records_per_batch 10
  @batches 500
  @prealloc_bytes 64 * 1024 * 1024
  # How a sample finds its way out of `mix run` output, which also carries compiler and app noise.
  @marker "AB_SAMPLE "
  @arms [:main_a1, :main_a2, :branch]
  @control {:main_a1, :main_a2}
  # Below this many repetitions per arm there is no verdict at all. The harness's own smoke run showed
  # why: with one repetition the bootstrap resamples a single value, its interval collapses to a point,
  # "excludes zero" becomes true for any nonzero difference, and one sample was reported as SIGNAL.
  @min_reps 5

  @doc "One repetition of the store case, printed as a single marked JSON line."
  def sample(directory) do
    File.rm_rf!(directory)
    File.mkdir_p!(directory)

    records =
      for i <- 1..@records_per_batch, do: Record.new(:crypto.strong_rand_bytes(@record_bytes), key: "k#{i}")

    {:ok, store} =
      ElixirStore.open(directory, "00000000000000000000", base_offset: 0, prealloc_bytes: @prealloc_bytes)

    {latencies, store} =
      Enum.map_reduce(1..@batches, store, fn _batch, store ->
        started = System.monotonic_time(:microsecond)
        {:ok, store, _first, _last} = ElixirStore.append(store, records)
        {:ok, store} = ElixirStore.sync(store)
        {System.monotonic_time(:microsecond) - started, store}
      end)

    :ok = ElixirStore.close(store)
    File.rm_rf!(directory)
    IO.puts(@marker <> Jason.encode!(PairedStats.summarize(latencies)))
  end

  @doc """
  Reads every sample under `results` (`<case>/<rep>-<arm>.out`, warmups excluded), prints the verdicts
  and writes them to `out`. Halts with status 1 when the branch regressed, so a CI step fails on it.
  """
  def analyze(results, out) do
    cases =
      for {name, parse} <- [{"store", &parse_store/1}, {"e2e", &parse_e2e/1}],
          samples = load(results, name, parse),
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

    regressions =
      for %{case: name, sufficient: true, comparisons: [branch | _control]} <- cases,
          {stat, v} <- branch.stats,
          v.signal and v.delta_us > 0,
          do: "#{name} #{stat}"

    insufficient = for %{case: name, sufficient: false} <- cases, do: name

    report = %{
      schema: 1,
      generated_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      otp: :erlang.system_info(:otp_release) |> to_string(),
      schedulers_online: :erlang.system_info(:schedulers_online),
      os: :os.type() |> Tuple.to_list() |> Enum.map_join("/", &to_string/1),
      interleaving: "by run, arm order shuffled per repetition",
      min_reps: @min_reps,
      cases: cases,
      insufficient: insufficient,
      regressions: regressions
    }

    if out, do: File.write!(out, Jason.encode_to_iodata!(report, pretty: true))

    cond do
      regressions != [] ->
        IO.puts("\n  REGRESSION: #{Enum.join(regressions, ", ")}")
        System.halt(1)

      # Not a pass: a run too short to judge must not read as "no regression" in a CI log.
      insufficient != [] ->
        IO.puts("\n  NO VERDICT for #{Enum.join(insufficient, ", ")}: too few repetitions")
        System.halt(2)

      true ->
        IO.puts("\n  no regression by the fixed rule")
    end
  end

  # `:missing` when a case was not run at all, so an e2e-less run still reports the store case. A case
  # that WAS run but is missing an arm is an error: comparing whatever arrived would be a verdict on a
  # different experiment than the one declared.
  defp load(results, name, parse) do
    files = Path.wildcard(Path.join([results, name, "*.out"])) |> Enum.reject(&warmup?/1)

    if files == [] do
      :missing
    else
      samples =
        Enum.group_by(files, &arm_of/1, fn path -> path |> File.read!() |> parse.() end)

      for arm <- @arms, not Map.has_key?(samples, arm) do
        raise "case #{name} has no samples for arm #{arm} under #{results}"
      end

      samples
    end
  end

  defp warmup?(path), do: path |> Path.basename() |> String.starts_with?("warm")

  defp arm_of(path) do
    [_rep, arm] = path |> Path.basename(".out") |> String.split("-", parts: 2)
    String.to_existing_atom(arm)
  end

  defp parse_store(output) do
    line = output |> String.split("\n") |> Enum.find(&String.starts_with?(&1, @marker))
    unless line, do: raise("store sample without an #{String.trim(@marker)} line:\n#{output}")
    decoded = line |> String.replace_prefix(@marker, "") |> Jason.decode!()
    %{p50: decoded["p50"], p99: decoded["p99"]}
  end

  # The produce line of throughput_1m.exs, which is the half #147 touches.
  defp parse_e2e(output) do
    case Regex.run(~r/batch latency .*?p50=(\d+)\s+p99=(\d+)/, output) do
      [_line, p50, p99] -> %{p50: String.to_integer(p50), p99: String.to_integer(p99)}
      nil -> raise "e2e sample without a produce latency line:\n#{output}"
    end
  end
end

case System.get_env("AB_MODE") do
  "sample" ->
    StoreErrorPathAB.sample(System.get_env("AB_DIR") || Path.join(System.tmp_dir!(), "store_error_path_ab"))

  "analyze" ->
    StoreErrorPathAB.analyze(System.fetch_env!("AB_RESULTS"), System.get_env("AB_OUT"))

  other ->
    raise ArgumentError, "AB_MODE must be sample or analyze, got: #{inspect(other)}"
end
