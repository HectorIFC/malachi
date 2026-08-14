# Single-node horizontal-scaling proof: run N independent produce pipelines (each a BrokerServer +
# ReplicationServer + topic) concurrently on ONE node and measure AGGREGATE throughput. This answers
# whether a sharded data plane can push a single node past 1M rec/s on this hardware, and the minimal N
# to do it, before committing to the production data-plane sharding refactor.
#
# It reuses the exact per-pipeline stack the throughput_1m benchmark measures, so N=1 here reproduces that
# ~668k rec/s number and anchors the sweep. Standalone; does not touch lib/.
#
# Run: mix run benchmark/single_node_scale.exs

defmodule ScaleBench do
  alias Malachi.BrokerServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record

  @per_pipeline 1_000_000
  @batch 1_000
  @value_bytes 100
  @ns [1, 2, 4, 8]

  defp mb(bytes), do: Float.round(bytes / 1_048_576, 1)

  defp dir_bytes(dir) do
    dir |> Path.join("**/*") |> Path.wildcard() |> Enum.map(&File.stat!(&1).size) |> Enum.sum()
  end

  defp pctl(sorted, p),
    do: Enum.at(sorted, max(0, min(length(sorted) - 1, round(p / 100 * (length(sorted) - 1)))))

  defp median(list) do
    sorted = Enum.sort(list)
    pctl(sorted, 50)
  end

  # One independent pipeline: its own ReplicationServer (named, own dir), its own BrokerServer (by pid, so
  # no name collision), and its own topic. Each broker keeps in-memory metadata, so the shards share
  # nothing but the disk and the schedulers, which is exactly what production shards would contend on.
  defp start_pipeline(base, run_id, i) do
    # Name is unique per run (run_id), so a run never collides with an earlier run's still-registered
    # server even if a stop is missed. With an external `brokers:` set the broker does not own this
    # ReplicationServer, so the cleanup below must stop it explicitly.
    repl = :"scale_repl_#{run_id}_#{i}"
    repl_dir = Path.join(base, "repl_#{i}")
    {:ok, repl_pid} = ReplicationServer.start_link(name: repl, directory: repl_dir)

    {:ok, broker} =
      BrokerServer.start_link(Path.join(base, "broker_#{i}"),
        brokers: [repl],
        segment_max_bytes: 64 * 1024 * 1024
      )

    topic = "bench_#{i}"
    {:ok, _} = BrokerServer.create_topic(broker, topic, 8)
    %{broker: broker, repl: repl_pid, topic: topic, dir: repl_dir}
  end

  defp produce_loop(pipe, batch, batches) do
    lats =
      for _ <- 1..batches do
        b0 = System.monotonic_time(:microsecond)
        {:ok, _} = BrokerServer.produce(pipe.broker, pipe.topic, batch)
        System.monotonic_time(:microsecond) - b0
      end

    Enum.sort(lats)
  end

  defp run_n(n) do
    run_id = System.unique_integer([:positive])
    base = Path.join(System.tmp_dir!(), "malachi_scale_#{n}_#{run_id}")
    File.rm_rf!(base)

    pipes = for i <- 1..n, do: start_pipeline(base, run_id, i)
    value = :binary.copy("x", @value_bytes)
    batch = for i <- 1..@batch, do: Record.new(value, key: "k#{rem(i, 1000)}")
    batches = div(@per_pipeline, @batch)

    # Warm each pipeline (opens the segment, registers it) outside the timed window.
    Enum.each(pipes, fn p -> {:ok, _} = BrokerServer.produce(p.broker, p.topic, batch) end)

    t0 = System.monotonic_time(:microsecond)
    tasks = Enum.map(pipes, fn p -> Task.async(fn -> produce_loop(p, batch, batches) end) end)
    all_lats = Enum.map(tasks, &Task.await(&1, :infinity))
    wall_us = System.monotonic_time(:microsecond) - t0

    total = n * @per_pipeline
    wall_s = wall_us / 1_000_000
    agg = round(total / wall_s)
    disk = pipes |> Enum.map(&dir_bytes(&1.dir)) |> Enum.sum()
    p50s = Enum.map(all_lats, &pctl(&1, 50))
    p99s = Enum.map(all_lats, &pctl(&1, 99))

    # Stop the broker first (it holds the ReplicationServer ref), then the ReplicationServer, which the
    # broker did not own because we passed an external `brokers:` set.
    Enum.each(pipes, fn p ->
      GenServer.stop(p.broker)
      GenServer.stop(p.repl)
    end)

    File.rm_rf!(base)

    %{
      n: n,
      agg: agg,
      per: round(agg / n),
      eff: nil,
      wall_s: Float.round(wall_s, 2),
      p50_ms: Float.round(median(p50s) / 1000, 2),
      p99_ms: Float.round(Enum.max(p99s) / 1000, 2),
      mb_s: Float.round(mb(disk) / wall_s, 1)
    }
  end

  def run do
    IO.puts("""
    BEAM: schedulers_online=#{:erlang.system_info(:schedulers_online)}  \
    dirty_cpu=#{:erlang.system_info(:dirty_cpu_schedulers)}  \
    dirty_io=#{:erlang.system_info(:dirty_io_schedulers)}  \
    async_threads=#{:erlang.system_info(:thread_pool_size)}
    Each pipeline produces #{@per_pipeline} records (#{@value_bytes}B value, batches of #{@batch}).
    """)

    results = Enum.map(@ns, &run_n/1)
    base = hd(results).per
    results = Enum.map(results, fn r -> %{r | eff: Float.round(r.per / base * 100, 0)} end)

    IO.puts("\n============ single-node scaling: N parallel produce pipelines ============")

    IO.puts(
      String.pad_leading("N", 3) <>
        String.pad_leading("agg rec/s", 14) <>
        String.pad_leading("per-pipe rec/s", 16) <>
        String.pad_leading("eff %", 8) <>
        String.pad_leading("wall s", 9) <>
        String.pad_leading("p50 ms", 9) <>
        String.pad_leading("p99 ms", 9) <>
        String.pad_leading("MB/s", 9)
    )

    Enum.each(results, fn r ->
      IO.puts(
        String.pad_leading(Integer.to_string(r.n), 3) <>
          String.pad_leading(Integer.to_string(r.agg), 14) <>
          String.pad_leading(Integer.to_string(r.per), 16) <>
          String.pad_leading("#{trunc(r.eff)}", 8) <>
          String.pad_leading(Float.to_string(r.wall_s), 9) <>
          String.pad_leading(Float.to_string(r.p50_ms), 9) <>
          String.pad_leading(Float.to_string(r.p99_ms), 9) <>
          String.pad_leading(Float.to_string(r.mb_s), 9)
      )
    end)

    crossed = Enum.find(results, fn r -> r.agg >= 1_000_000 end)

    IO.puts("\n" <> String.duplicate("=", 74))

    case crossed do
      nil ->
        IO.puts("Did NOT reach 1M rec/s aggregate up to N=#{List.last(@ns)}: sharding alone plateaus; the")
        IO.puts("lever is group commit / fewer fsyncs, not more pipelines.")

      r ->
        IO.puts("Crossed 1M rec/s aggregate at N=#{r.n} (#{r.agg} rec/s, #{trunc(r.eff)}% per-pipe efficiency).")
        IO.puts("Single-node >1M is feasible via data-plane sharding on this hardware.")
    end

    IO.puts(String.duplicate("=", 74))
  end
end

ScaleBench.run()
