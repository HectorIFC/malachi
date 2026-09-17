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
#   AB_MODE=analyze AB_RESULTS=dir AB_OUT=file AB_EXPECTED="store e2e" \
#     mix run --no-start benchmark/store_error_path_ab.exs
#
# AB_EXPECTED names the cases the run must have evaluated (default: store). One with no samples is a run
# with no verdict, never a pass.

Code.require_file("support/ab_run.exs", __DIR__)
Code.require_file("support/e2e_sample.exs", __DIR__)

defmodule StoreErrorPathAB do
  alias Malachi.Bench.ABRun
  alias Malachi.Bench.E2ESample
  alias Malachi.Bench.PairedStats
  alias Malachi.Log.Record
  alias Malachi.Storage.ElixirStore

  @record_bytes 256
  @records_per_batch 10
  @batches 500
  @prealloc_bytes 64 * 1024 * 1024

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
    ABRun.emit(PairedStats.summarize(latencies))
  end

  @doc "Reads the samples under `results` and judges them (see `Malachi.Bench.ABRun.analyze/5`)."
  def analyze(results, out, expected) do
    ABRun.analyze(results, out, expected, [
      {"store", "store", &parse_store/1},
      {"e2e", "e2e", &parse_e2e/1}
    ])
  end

  defp parse_store(output) do
    decoded = ABRun.marked(output)
    %{p50: decoded["p50"], p99: decoded["p99"]}
  end

  # The produce line of throughput_1m.exs, which is the half #147 touches.
  defp parse_e2e(output), do: E2ESample.parse(output)
end

case System.get_env("AB_MODE") do
  "sample" ->
    StoreErrorPathAB.sample(System.get_env("AB_DIR") || Path.join(System.tmp_dir!(), "store_error_path_ab"))

  "analyze" ->
    # The cases the run was asked for, space separated. The store case is the one this experiment exists to
    # measure, so it is expected unless the caller says otherwise; store_error_path_ab.sh passes what it ran.
    expected = System.get_env("AB_EXPECTED", "store") |> String.split()
    StoreErrorPathAB.analyze(System.fetch_env!("AB_RESULTS"), System.get_env("AB_OUT"), expected)

  other ->
    raise ArgumentError, "AB_MODE must be sample or analyze, got: #{inspect(other)}"
end
