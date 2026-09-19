# Paired A/B of the consume path (issue #191): does counting skipped data cost an ordinary fetch anything?
#
# #191 makes `Malachi.Broker.read_consume/5` return the stretches of history a page stepped over, threads
# them through `Malachi.BrokerServer.consume/6` and `Malachi.LogApi`, and hands any to a skip reporter. On
# an ordinary page the list is empty and nothing is reported, so the expected cost is an extra tuple
# element and an empty list per range. "Expected" is not a measurement, so this compares the code before
# and after on the path every fetch pays, where no data was skipped.
#
# The arms interleave by RUN, as in `store_error_path_ab.exs`: `consume_skip_ab.sh` builds the baseline
# tree and the branch tree and launches one sample per arm per repetition in a shuffled order, two of the
# three arms being the baseline run twice (the A-A control). The verdict rule is fixed before any number
# exists, in `support/paired_stats.exs`: a REGRESSION is a comparison that is SIGNAL (delta above the A-A
# delta AND a 95% CI excluding zero) with the branch slower.
#
# Two cases, both through `Malachi.LogApi.fetch/5`, the one consume API both trees share:
#   * self     - one range of 4000 records in 64KB segments, drained from :start in pages of 100.
#   * ancestor - the same records, then a split and 4000 more: a child drains its ancestor's key slice
#                before its own records, the path that reports ancestor skips.
#
# Modes (normally driven by consume_skip_ab.sh, not by hand):
#   AB_MODE=sample AB_CASE=self|ancestor AB_DIR=/scratch  mix run --no-start benchmark/consume_skip_ab.exs
#   AB_MODE=analyze AB_RESULTS=dir AB_OUT=file            mix run --no-start benchmark/consume_skip_ab.exs

Code.require_file("support/ab_run.exs", __DIR__)

defmodule ConsumeSkipAB do
  alias Malachi.Bench.ABRun
  alias Malachi.Bench.PairedStats
  alias Malachi.BrokerServer
  alias Malachi.LogApi

  # Named like a production broker, so the branch derives its reporter's name exactly as it would there.
  @broker ConsumeSkipAB.LogBroker
  @topic "bench"
  @records 4_000
  @per_produce 100
  @page 100
  @passes 20

  @doc "One repetition of `kase`, printed as a single marked JSON line of per-fetch latencies."
  def sample(kase, directory) do
    File.rm_rf!(directory)
    File.mkdir_p!(directory)
    {:ok, _apps} = Application.ensure_all_started(:telemetry)
    {:ok, _pid} = BrokerServer.start_link(directory, name: @broker, segment_max_bytes: 64 * 1024)
    :ok = LogApi.create_topic(@broker, @topic)
    produce(0)
    if kase == "ancestor", do: split_and_produce()

    latencies = Enum.flat_map(1..@passes, fn _pass -> drain(:start, []) end)

    BrokerServer.stop(@broker)
    File.rm_rf!(directory)
    ABRun.emit(PairedStats.summarize(latencies))
  end

  defp produce(first) do
    for batch <- 0..(div(@records, @per_produce) - 1) do
      records =
        for i <- 1..@per_produce do
          n = first + batch * @per_produce + i
          %{"key" => "k#{n}", "value" => :crypto.strong_rand_bytes(200) |> Base.encode64()}
        end

      {:ok, @per_produce} = LogApi.produce(@broker, @topic, records)
    end
  end

  # Splits the topic's only range and produces as much again, so each child has an ancestor to drain first.
  defp split_and_produce do
    [root] = BrokerServer.active_range_ids(@broker, @topic)
    {:ok, _left, _right} = BrokerServer.split_range(@broker, root)
    produce(@records)
  end

  # Fetches from `cursor` in pages until a page comes back empty, timing each fetch in microseconds.
  defp drain(cursor, latencies) do
    started = System.monotonic_time(:microsecond)
    {:ok, records, next} = LogApi.fetch(@broker, @topic, cursor, @page)
    latencies = [System.monotonic_time(:microsecond) - started | latencies]

    case records do
      [] -> latencies
      _records -> drain(next, latencies)
    end
  end

  @doc "Reads the samples under `results` and judges them (see `Malachi.Bench.ABRun.analyze/5`)."
  def analyze(results, out) do
    ABRun.analyze(results, out, ["self", "ancestor"], [
      {"self", "self", &parse/1},
      {"ancestor", "ancestor", &parse/1}
    ])
  end

  defp parse(output) do
    decoded = ABRun.marked(output)
    %{p50: decoded["p50"], p99: decoded["p99"]}
  end
end

case System.get_env("AB_MODE") do
  "sample" ->
    ConsumeSkipAB.sample(
      System.fetch_env!("AB_CASE"),
      System.get_env("AB_DIR") || Path.join(System.tmp_dir!(), "consume_skip_ab")
    )

  "analyze" ->
    ConsumeSkipAB.analyze(System.fetch_env!("AB_RESULTS"), System.get_env("AB_OUT"))

  other ->
    raise ArgumentError, "AB_MODE must be sample or analyze, got: #{inspect(other)}"
end
