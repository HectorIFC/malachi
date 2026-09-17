# Does naming the sharded window by the monotonic clock, and reading the shard count once, cost the
# publish check anything? (issue #151)
#
# `RateLimiter.check_limit_in_caller/3` runs on every produce once a quota is configured. #151 changes
# which clock names its window and where its shard count comes from. Both are expected to cost the same as
# before, and "expected" is not a measurement, so this compares the code before and after with the same
# paired protocol as store_error_path_ab.exs: arms interleaved by run in a shuffled order, an A-A control,
# and the fixed verdict rule of `support/paired_stats.exs`.
#
# One sample starts the limiter on its own (no broker, no sockets), keeps a pool of worker processes per
# concurrency level, and times @batches rounds of @checks_per_round checks split across the pool, all on
# ONE identifier: the quota is keyed by user, so a single hot key is the case that matters. The limit is
# far above the offered load, so the rejection path is never what is timed.
#
# Two cases, from the same sample: c1 (one caller) and c64 (64 concurrent callers).
#
# Modes (normally driven by rate_limit_ab.sh, not by hand):
#   AB_MODE=sample                          mix run --no-start benchmark/rate_limit_ab.exs
#   AB_MODE=analyze AB_RESULTS=dir AB_OUT=file mix run --no-start benchmark/rate_limit_ab.exs

Code.require_file("support/ab_run.exs", __DIR__)

defmodule RateLimitAB do
  alias Malachi.Bench.ABRun
  alias Malachi.Bench.PairedStats
  alias Malachi.RateLimiter

  @concurrency [1, 64]
  @batches 300
  @checks_per_round 6_400
  @config %{limit: 1_000_000_000, window_ms: 1_000}

  @doc "One repetition of every case, printed as a single marked JSON line."
  def sample do
    Application.put_env(:malachi, :rate_limit_enabled, true)
    {:ok, _limiter} = RateLimiter.start_link([])

    @concurrency
    |> Map.new(fn procs -> {"c#{procs}", PairedStats.summarize(rounds(procs))} end)
    |> ABRun.emit()
  end

  # Round latencies in microseconds for `procs` long-lived workers. Spawning per round would time the
  # spawns, which at 64 processes rival the checks themselves.
  defp rounds(procs) do
    identifier = "ab_#{procs}"
    per_worker = div(@checks_per_round, procs)
    parent = self()

    workers =
      for _ <- 1..procs do
        spawn_link(fn -> worker(parent, identifier, per_worker) end)
      end

    latencies =
      for _ <- 1..@batches do
        started = System.monotonic_time(:microsecond)
        Enum.each(workers, &send(&1, :go))
        for _ <- workers, do: receive(do: (:done -> :ok))
        System.monotonic_time(:microsecond) - started
      end

    Enum.each(workers, &send(&1, :stop))
    latencies
  end

  defp worker(parent, identifier, checks) do
    receive do
      :go ->
        for _ <- 1..checks, do: :ok = RateLimiter.check_limit_in_caller(identifier, :publish, @config)
        send(parent, :done)
        worker(parent, identifier, checks)

      :stop ->
        :ok
    end
  end

  @doc "Reads the samples under `results` and judges them (see `Malachi.Bench.ABRun.analyze/5`)."
  def analyze(results, out) do
    cases = for procs <- @concurrency, do: {"c#{procs}", "check", &parse(&1, "c#{procs}")}
    expected = Enum.map(cases, &elem(&1, 0))
    ABRun.analyze(results, out, expected, cases, %{checks_per_round: @checks_per_round, batches: @batches})
  end

  defp parse(output, name) do
    %{"p50" => p50, "p99" => p99} = output |> ABRun.marked() |> Map.fetch!(name)
    %{p50: p50, p99: p99}
  end
end

case System.get_env("AB_MODE") do
  "sample" -> RateLimitAB.sample()
  "analyze" -> RateLimitAB.analyze(System.fetch_env!("AB_RESULTS"), System.get_env("AB_OUT"))
  other -> raise ArgumentError, "AB_MODE must be sample or analyze, got: #{inspect(other)}"
end
