# Rate-limit check cost: what does calling the limiter on every produce actually cost?
#
# Produce is the hottest path in the system, so "an ETS token bucket is free" is a claim to measure. This
# benchmark isolates the check itself, with no sockets and no broker, and answers four questions:
#
#   1. unconfigured  - what every deployment pays by default on a produce: both publish quotas read as
#                      unlimited, so the charge touches no table
#   2. caller-side   - the one-token check, `RateLimiter.check_limit_in_caller/3`, the door the subscribe
#                      quota uses (timed here on a publish-keyed counter; the action is only part of the
#                      counter's key)
#   3. charge        - what a produce pays with quotas set, `RateLimiter.charge_in_caller/2`: a batch of
#                      100 records against the records quota, then against both records and bytes
#   4. serialized    - the one-token check through the limiter GenServer, which is the door the auth paths
#                      use
#
# Every charge here fits in the caller's own shard. A cost bigger than one shard's slice of the quota
# sweeps the other shards, an update or two per shard it takes from; that path is not timed here.
#
# The concurrent rows are the ones that matter: the quota is keyed by AUTHENTICATED USER, so every
# connection belonging to one user contends on ONE ETS bucket. That single hot bucket is the worst case
# for lock contention and the only case worth reporting.
#
# The end-to-end counterpart (this cost as a share of real produce throughput) is
# benchmark/docker-ratelimit.sh; this one says where the cost comes from.
#
# Run: MIX_ENV=test mix run benchmark/rate_limit_bench.exs

defmodule RateLimitBench do
  alias Malachi.RateLimiter

  @iterations 200_000
  @concurrency [1, 4, 16, 64]

  # Far above the offered load: the bucket must never empty, or we would be timing the rejection path.
  @config %{limit: 1_000_000_000, window_ms: 1_000}

  defp measure(iterations, fun) do
    t0 = System.monotonic_time(:microsecond)
    fun.()
    wall = System.monotonic_time(:microsecond) - t0
    {Float.round(wall / iterations * 1_000, 0), round(iterations / (wall / 1_000_000))}
  end

  # `procs` processes each running `per_proc` iterations of `fun`, all on the same bucket.
  defp concurrent(procs, per_proc, fun) do
    measure(procs * per_proc, fn ->
      1..procs
      |> Enum.map(fn _ -> Task.async(fn -> for _ <- 1..per_proc, do: fun.() end) end)
      |> Task.await_many(:infinity)
    end)
  end

  def run do
    Application.put_env(:malachi, :rate_limit_enabled, true)
    Application.put_env(:malachi, :publish_rate_limit, 0)
    Application.put_env(:malachi, :publish_bytes_rate_limit, 0)

    IO.puts("\n============ Rate-limit check cost (#{@iterations} checks, one shared bucket) ============")
    IO.puts("                          procs      ns/check      checks/s")

    # 1. The shipped default: both publish quotas are unconfigured, so the charge a produce makes reads them
    # as nil and touches no table. This is what every deployment that never configures a quota pays on
    # every produce.
    for procs <- @concurrency do
      {ns, thr} = concurrent(procs, div(@iterations, procs), fn -> produce_charge("bench_default") end)
      row("unconfigured (default)", procs, ns, thr)
    end

    # 2. The check itself, in the calling process: what a deployment that configures a quota pays.
    Application.put_env(:malachi, :publish_rate_limit, 1_000_000_000)
    Application.put_env(:malachi, :publish_rate_window_ms, 1_000)

    for procs <- @concurrency do
      id = "bench_caller_#{procs}"

      {ns, thr} =
        concurrent(procs, div(@iterations, procs), fn -> RateLimiter.check_limit_in_caller(id, :publish, @config) end)

      row("caller-side check", procs, ns, thr)
    end

    # 3. The charge a produce makes with the records quota set, then with both quotas set. The limits are
    # far above what the run spends, so no charge is ever refused.
    Application.put_env(:malachi, :publish_bytes_rate_limit, 0)

    for procs <- @concurrency do
      {ns, thr} = concurrent(procs, div(@iterations, procs), fn -> produce_charge("bench_records_#{procs}") end)
      row("charge, records", procs, ns, thr)
    end

    Application.put_env(:malachi, :publish_bytes_rate_limit, 1_000_000_000_000_000)
    Application.put_env(:malachi, :publish_bytes_rate_window_ms, 1_000)

    for procs <- @concurrency do
      {ns, thr} = concurrent(procs, div(@iterations, procs), fn -> produce_charge("bench_both_#{procs}") end)
      row("charge, records + bytes", procs, ns, thr)
    end

    # 4. The one-token check through the GenServer, for scale: the door the auth paths use, and the gap between
    # rows 2 and 4 is the whole reason the caller-side door exists.
    for procs <- @concurrency do
      id = "bench_serialized_#{procs}"
      {ns, thr} = concurrent(procs, div(@iterations, procs), fn -> RateLimiter.check_limit(id, :publish, @config) end)
      row("serialized (GenServer)", procs, ns, thr)
    end

    IO.puts("=========================================================================\n")
  end

  # What `Malachi.TCPProtocol` charges for a produce of 100 records of 256 bytes, config reads included.
  defp produce_charge(user) do
    RateLimiter.charge_in_caller(user, [
      {:publish, RateLimiter.action_config(:publish), 100},
      {:publish_bytes, RateLimiter.action_config(:publish_bytes), 25_600}
    ])
  end

  defp row(label, procs, ns, thr) do
    IO.puts(
      "#{String.pad_trailing(label, 25)} #{String.pad_leading("#{procs}", 5)}   #{String.pad_leading("#{round(ns)}", 11)}   #{String.pad_leading("#{thr}", 11)}"
    )
  end
end

RateLimitBench.run()
