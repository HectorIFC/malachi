# Single-node horizontal-scaling proof: run N independent produce pipelines (each a BrokerServer +
# ReplicationServer + topic) concurrently on ONE node and measure AGGREGATE throughput. This answers
# whether a sharded data plane can push a single node past 1M rec/s on this hardware, and the minimal N
# to do it, before committing to the production data-plane sharding refactor.
#
# The answer depends on the batch size, so the sweep has two dimensions: N and the records per produce.
# With small batches the per-produce cost dominates and a pipeline's serial BrokerServer is the limit;
# with large ones the disk is. Every cell sends the same number of produces per pipeline, so every cell
# has as many latency samples, and batch 1000 is 1M records per pipeline: N=1 at batch 1000 runs the
# regime benchmark/throughput_1m.exs runs, and anchors the sweep. Standalone; does not modify lib/.
#
# The regime is pinned and printed above each block (support/flush_regime.exs): group commit off, so one
# sync per produce, and segment preallocation off, on the filesystem under BENCH_DIR, which must not be
# memory-backed. Measurements count on Linux only.
#
# Run: mix run benchmark/single_node_scale.exs
#   SCALE_NS           pipeline counts to sweep, space separated (default: 1 2 4 8)
#   SCALE_BATCHES      records per produce to sweep, space separated (default: 10 100 1000)
#   BENCH_DIR          directory to write under (default: the system temp dir); must exist
#   BENCH_ALLOW_TMPFS  1 runs on tmpfs or ramfs anyway, with a warning

Code.require_file("support/measure.exs", __DIR__)
Code.require_file("support/paired_stats.exs", __DIR__)
Code.require_file("support/flush_regime.exs", __DIR__)
Code.require_file("support/scale_sweep.exs", __DIR__)

defmodule ScaleBench do
  alias Malachi.Bench.FlushRegime
  alias Malachi.Bench.ScaleSweep
  alias Malachi.BrokerServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record

  @produces_per_pipeline 1_000
  @value_bytes 100
  @ns [1, 2, 4, 8]
  @batches [10, 100, 1_000]
  @target_rate 1_000_000

  defdelegate mb(bytes), to: Malachi.Bench.Measure
  defdelegate dir_bytes(dir), to: Malachi.Bench.Measure
  defdelegate pctl(sorted, p), to: Malachi.Bench.PairedStats

  # One independent pipeline: its own ReplicationServer (named, own dir), its own BrokerServer (by pid, so
  # no name collision), and its own topic. Each broker keeps in-memory metadata, so the shards share
  # nothing but the disk and the schedulers, which is exactly what production shards would contend on.
  defp start_pipeline(base, run_id, i) do
    # Name is unique per run (run_id), so a run never collides with an earlier run's still-registered
    # server even if a stop is missed. With an external `brokers:` set the broker does not own this
    # ReplicationServer, so the cleanup below must stop it explicitly.
    repl = :"scale_repl_#{run_id}_#{i}"
    repl_dir = Path.join(base, "repl_#{i}")
    {:ok, repl_pid} = ReplicationServer.start_link([name: repl, directory: repl_dir] ++ FlushRegime.replication_opts())

    {:ok, broker} =
      BrokerServer.start_link(
        Path.join(base, "broker_#{i}"),
        [brokers: [repl], segment_max_bytes: 64 * 1024 * 1024] ++ FlushRegime.broker_opts()
      )

    topic = "bench_#{i}"
    {:ok, _} = BrokerServer.create_topic(broker, topic, 8)
    %{broker: broker, repl: repl_pid, topic: topic, dir: repl_dir}
  end

  defp produce_loop(pipe, batch) do
    lats =
      for _ <- 1..@produces_per_pipeline do
        b0 = System.monotonic_time(:microsecond)
        {:ok, _} = BrokerServer.produce(pipe.broker, pipe.topic, batch)
        System.monotonic_time(:microsecond) - b0
      end

    Enum.sort(lats)
  end

  defp run_cell(root, batch_size, n) do
    run_id = System.unique_integer([:positive])
    base = Path.join(root, "malachi_scale_#{batch_size}_#{n}_#{run_id}")
    File.rm_rf!(base)

    pipes = for i <- 1..n, do: start_pipeline(base, run_id, i)
    value = :binary.copy("x", @value_bytes)
    batch = for i <- 1..batch_size, do: Record.new(value, key: "k#{rem(i, 1000)}")

    # Warm each pipeline (opens the segment, registers it) outside the timed window.
    Enum.each(pipes, fn p -> {:ok, _} = BrokerServer.produce(p.broker, p.topic, batch) end)

    t0 = System.monotonic_time(:microsecond)
    tasks = Enum.map(pipes, fn p -> Task.async(fn -> produce_loop(p, batch) end) end)
    all_lats = Enum.map(tasks, &Task.await(&1, :infinity))
    wall_us = System.monotonic_time(:microsecond) - t0

    total = n * batch_size * @produces_per_pipeline
    wall_s = wall_us / 1_000_000
    agg = round(total / wall_s)
    disk = pipes |> Enum.map(&dir_bytes(&1.dir)) |> Enum.sum()
    # True aggregate percentiles over every observed batch (a median of per-pipeline medians and a max
    # of per-pipeline p99s are neither).
    merged = all_lats |> List.flatten() |> Enum.sort()

    # Stop the broker first (it holds the ReplicationServer ref), then the ReplicationServer, which the
    # broker did not own because we passed an external `brokers:` set.
    Enum.each(pipes, fn p ->
      GenServer.stop(p.broker)
      GenServer.stop(p.repl)
    end)

    File.rm_rf!(base)

    %{
      n: n,
      batch: batch_size,
      agg: agg,
      per: round(agg / n),
      eff: nil,
      wall_s: Float.round(wall_s, 2),
      p50_ms: Float.round(pctl(merged, 50) / 1000, 2),
      p99_ms: Float.round(pctl(merged, 99) / 1000, 2),
      mb_s: Float.round(mb(disk) / wall_s, 1)
    }
  end

  @doc """
  Runs the sweep: validates the ladders and the target directory, then measures every N at every batch
  size and prints one block per batch size under its regime, followed by the verdicts.
  """
  def run do
    ns = ladder!("SCALE_NS", @ns)
    batches = ladder!("SCALE_BATCHES", @batches)
    target = FlushRegime.prepare!()
    base_n = Enum.min(ns)

    IO.puts("""
    BEAM: schedulers_online=#{:erlang.system_info(:schedulers_online)}  \
    dirty_cpu=#{:erlang.system_info(:dirty_cpu_schedulers)}  \
    dirty_io=#{:erlang.system_info(:dirty_io_schedulers)}  \
    async_threads=#{:erlang.system_info(:thread_pool_size)}
    Sweeping N=#{Enum.join(ns, " ")} x batch=#{Enum.join(batches, " ")}: each pipeline sends \
    #{@produces_per_pipeline} produces of #{@value_bytes}B values.
    """)

    blocks = for batch <- batches, do: {batch, run_block(target, batch, ns)}

    IO.puts("\n============ single-node scaling: N parallel produce pipelines, per batch size ============")
    IO.puts("eff % is the per-pipeline rate against N=#{base_n} at the same batch size.")
    Enum.each(blocks, fn {batch, results} -> print_block(target, batch, results) end)

    IO.puts("\n" <> String.duplicate("=", 91))

    # Report only what this sweep measured: the result applies to this host and configuration, and a
    # miss through the swept N range is not proof of a plateau or of what the next lever is.
    Enum.each(blocks, fn {batch, results} -> print_verdict(batch, results) end)
    IO.puts("on this host/configuration. See each block above for where the curve bends.")
    IO.puts(String.duplicate("=", 91))
  end

  defp run_block(target, batch, ns) do
    ns |> Enum.map(&run_cell(target.dir, batch, &1)) |> ScaleSweep.with_efficiency()
  end

  defp print_block(target, batch, results) do
    IO.puts("\n-- #{FlushRegime.label(batch, @value_bytes, target.filesystem)} --")

    IO.puts(
      String.pad_leading("N", 3) <>
        String.pad_leading("batch", 7) <>
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
          String.pad_leading(Integer.to_string(r.batch), 7) <>
          String.pad_leading(Integer.to_string(r.agg), 14) <>
          String.pad_leading(Integer.to_string(r.per), 16) <>
          String.pad_leading("#{trunc(r.eff)}", 8) <>
          String.pad_leading(Float.to_string(r.wall_s), 9) <>
          String.pad_leading(Float.to_string(r.p50_ms), 9) <>
          String.pad_leading(Float.to_string(r.p99_ms), 9) <>
          String.pad_leading(Float.to_string(r.mb_s), 9)
      )
    end)
  end

  defp print_verdict(batch, results) do
    case ScaleSweep.crossing(results, @target_rate) do
      nil ->
        max_n = results |> Enum.map(& &1.n) |> Enum.max()
        IO.puts("batch #{batch}: did NOT reach 1M rec/s aggregate within the swept range (N up to #{max_n})")

      r ->
        IO.puts(
          "batch #{batch}: crossed 1M rec/s aggregate at N=#{r.n} " <>
            "(#{r.agg} rec/s, #{trunc(r.eff)}% per-pipe efficiency)"
        )
    end
  end

  # The ladder `name` asks for, or `default` when it is unset. Anything else stops the run before it starts.
  defp ladder!(name, default) do
    case ScaleSweep.ladder(name, System.get_env(name), default) do
      {:ok, ladder} ->
        ladder

      {:error, message} ->
        IO.puts(:stderr, "ERROR: " <> message)
        System.halt(2)
    end
  end
end

ScaleBench.run()
