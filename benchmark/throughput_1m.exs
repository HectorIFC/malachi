# 1M-message end-to-end throughput + resource benchmark on the REAL log stack.
#
# Produces and consumes 1_000_000 records through the actual produce -> disk (ReplicationServer) ->
# consume path of the BrokerServer, measuring throughput, per-batch latency, BEAM memory, on-disk bytes,
# and CPU (reductions). This is the SYSTEM baseline the streaming alternatives are judged against; it is
# a standalone script and does not modify lib/.
#
# The regime is pinned, and printed next to the result (support/flush_regime.exs): one producer sending
# fixed batches, one sync per produce because group commit is off, segments growing because
# preallocation is off, on the filesystem under BENCH_DIR, which must not be memory-backed. A number
# from another regime is not comparable with this one. Measurements count on Linux only.
#
# Run: mix run benchmark/throughput_1m.exs
#   BENCH_DIR          directory to write under (default: the system temp dir); must exist
#   BENCH_ALLOW_TMPFS  1 runs on tmpfs or ramfs anyway, with a warning
#
# benchmark/store_error_path_ab.exs reads the produce latency line (support/e2e_sample.exs): keep it as
# the only line of its shape.

Code.require_file("support/measure.exs", __DIR__)
Code.require_file("support/paired_stats.exs", __DIR__)
Code.require_file("support/flush_regime.exs", __DIR__)

defmodule Bench1M do
  alias Malachi.Bench.FlushRegime
  alias Malachi.BrokerServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record

  @total 1_000_000
  @batch 1_000
  @value_bytes 100
  @topic "bench"

  defdelegate mb(bytes), to: Malachi.Bench.Measure
  defdelegate dir_bytes(dir), to: Malachi.Bench.Measure
  defdelegate pctl(sorted, p), to: Malachi.Bench.PairedStats

  defp mem(key), do: mb(:erlang.memory(key))

  @doc """
  Runs the benchmark once: checks the target directory, produces and consumes 1M records under the pinned
  regime, prints the report, and removes everything it wrote.
  """
  def run do
    target = FlushRegime.prepare!()
    regime = FlushRegime.label(@batch, @value_bytes, target.filesystem)
    base = Path.join(target.dir, "malachi_bench_1m_#{System.unique_integer([:positive])}")
    File.rm_rf!(base)
    repl_dir = Path.join(base, "repl")

    {:ok, _repl} =
      ReplicationServer.start_link([name: :bench_repl, directory: repl_dir] ++ FlushRegime.replication_opts())

    {:ok, broker} =
      BrokerServer.start_link(
        Path.join(base, "broker"),
        [brokers: [:bench_repl], segment_max_bytes: 64 * 1024 * 1024] ++ FlushRegime.broker_opts()
      )

    {:ok, _root} = BrokerServer.create_topic(broker, @topic, 8)

    value = :binary.copy("x", @value_bytes)
    batch = for i <- 1..@batch, do: Record.new(value, key: "k#{rem(i, 1000)}")
    batches = div(@total, @batch)

    :erlang.garbage_collect()
    Process.sleep(50)
    mem_base = %{total: mem(:total), binary: mem(:binary), processes: mem(:processes), ets: mem(:ets)}
    {red0, _} = :erlang.statistics(:reductions)

    # ---- PRODUCE ----
    IO.puts("Producing #{@total} records, #{regime}...")
    t0 = System.monotonic_time(:microsecond)

    produce_lat =
      for _ <- 1..batches do
        b0 = System.monotonic_time(:microsecond)
        {:ok, _} = BrokerServer.produce(broker, @topic, batch)
        System.monotonic_time(:microsecond) - b0
      end

    produce_wall = System.monotonic_time(:microsecond) - t0
    mem_after_produce = mem(:total)
    disk = dir_bytes(repl_dir)

    # ---- CONSUME ----
    IO.puts("Consuming #{@total} records (fetch by cursor, pages of #{@batch})...")
    t1 = System.monotonic_time(:microsecond)
    {consumed, consume_lat} = consume_all(broker)
    consume_wall = System.monotonic_time(:microsecond) - t1

    {red1, _} = :erlang.statistics(:reductions)
    :erlang.garbage_collect()
    Process.sleep(50)
    mem_final = %{total: mem(:total), binary: mem(:binary), processes: mem(:processes), ets: mem(:ets)}

    report(%{
      regime: regime,
      produce_wall: produce_wall,
      produce_lat: Enum.sort(produce_lat),
      consume_wall: consume_wall,
      consume_lat: Enum.sort(consume_lat),
      consumed: consumed,
      disk: disk,
      reductions: red1 - red0,
      mem_base: mem_base,
      mem_after_produce: mem_after_produce,
      mem_final: mem_final
    })

    GenServer.stop(broker)
    File.rm_rf!(base)
  end

  defp consume_all(broker), do: consume_loop(broker, %{}, 0, [])

  defp consume_loop(broker, positions, count, lats) do
    b0 = System.monotonic_time(:microsecond)
    {records, next, _skips} = BrokerServer.consume(broker, @topic, positions, @batch, 0)
    lat = System.monotonic_time(:microsecond) - b0

    case records do
      [] -> {count, lats}
      _ -> consume_loop(broker, next, count + length(records), [lat | lats])
    end
  end

  defp report(m) do
    prod_s = m.produce_wall / 1_000_000
    cons_s = m.consume_wall / 1_000_000

    IO.puts("""

    ============ 1M-message end-to-end (BrokerServer + ReplicationServer, single node) ============
    REGIME    #{m.regime}
    PRODUCE   #{@total} recs in #{Float.round(prod_s, 2)}s  =>  #{round(@total / prod_s)} rec/s, #{Float.round(mb(m.disk) / prod_s, 1)} MB/s
      batch latency (#{@batch}/batch) us:  p50=#{pctl(m.produce_lat, 50)}  p99=#{pctl(m.produce_lat, 99)}  max=#{List.last(m.produce_lat)}
    CONSUME   #{m.consumed} recs in #{Float.round(cons_s, 2)}s  =>  #{round(m.consumed / cons_s)} rec/s
      page latency (#{@batch}/page) us:    p50=#{pctl(m.consume_lat, 50)}  p99=#{pctl(m.consume_lat, 99)}  max=#{List.last(m.consume_lat)}

    DISK      #{mb(m.disk)} MB on disk  =>  #{Float.round(m.disk / @total, 1)} bytes/record (payload #{@value_bytes}B)
    CPU       #{m.reductions} reductions total (#{round(m.reductions / (@total * 2))} per record round-trip)
    MEMORY    (MB)          total   binary  processes  ets
      baseline             #{fmt(m.mem_base)}
      after 1M produce      total=#{m.mem_after_produce}
      final (post-consume) #{fmt(m.mem_final)}
    ==============================================================================================
    """)
  end

  defp fmt(%{total: t, binary: b, processes: p, ets: e}), do: "#{t}\t#{b}\t#{p}\t#{e}"
end

Bench1M.run()
