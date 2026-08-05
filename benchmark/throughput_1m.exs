# 1M-message end-to-end throughput + resource benchmark on the REAL log stack.
#
# Produces and consumes 1_000_000 records through the actual produce -> disk (ReplicationServer) ->
# consume path of the BrokerServer, measuring throughput, per-batch latency, BEAM memory, on-disk bytes,
# and CPU (reductions). This is the SYSTEM baseline the streaming alternatives are judged against; it is
# a standalone script and does not touch lib/.
#
# Run: mix run benchmark/throughput_1m.exs

defmodule Bench1M do
  alias Malachi.BrokerServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record

  @total 1_000_000
  @batch 1_000
  @value_bytes 100
  @topic "bench"

  defp mb(bytes), do: Float.round(bytes / 1_048_576, 1)
  defp mem(key), do: mb(:erlang.memory(key))

  defp dir_bytes(dir) do
    dir |> Path.join("**/*") |> Path.wildcard() |> Enum.map(&File.stat!(&1).size) |> Enum.sum()
  end

  defp pctl(sorted, p), do: Enum.at(sorted, max(0, min(length(sorted) - 1, round(p / 100 * (length(sorted) - 1)))))

  def run do
    base = Path.join(System.tmp_dir!(), "malachi_bench_1m_#{System.unique_integer([:positive])}")
    File.rm_rf!(base)
    repl_dir = Path.join(base, "repl")

    {:ok, _repl} = ReplicationServer.start_link(name: :bench_repl, directory: repl_dir)
    {:ok, broker} = BrokerServer.start_link(Path.join(base, "broker"), brokers: [:bench_repl], segment_max_bytes: 64 * 1024 * 1024)
    {:ok, _root} = BrokerServer.create_topic(broker, @topic, 8)

    value = :binary.copy("x", @value_bytes)
    batch = for i <- 1..@batch, do: Record.new(value, key: "k#{rem(i, 1000)}")
    batches = div(@total, @batch)

    :erlang.garbage_collect()
    Process.sleep(50)
    mem_base = %{total: mem(:total), binary: mem(:binary), processes: mem(:processes), ets: mem(:ets)}
    {red0, _} = :erlang.statistics(:reductions)

    # ---- PRODUCE ----
    IO.puts("Producing #{@total} records (#{@value_bytes}B value each, batches of #{@batch})...")
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
      produce_wall: produce_wall, produce_lat: Enum.sort(produce_lat),
      consume_wall: consume_wall, consume_lat: Enum.sort(consume_lat), consumed: consumed,
      disk: disk, reductions: red1 - red0,
      mem_base: mem_base, mem_after_produce: mem_after_produce, mem_final: mem_final
    })

    GenServer.stop(broker)
    File.rm_rf!(base)
  end

  defp consume_all(broker), do: consume_loop(broker, %{}, 0, [])

  defp consume_loop(broker, positions, count, lats) do
    b0 = System.monotonic_time(:microsecond)
    {records, next} = BrokerServer.consume(broker, @topic, positions, @batch, 0)
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
    PRODUCE   #{@total} recs in #{Float.round(prod_s, 2)}s  =>  #{round(@total / prod_s)} rec/s, #{Float.round(mb(m.disk) / prod_s, 1)} MB/s
      batch latency (#{@batch}/batch) us:  p50=#{pctl(m.produce_lat, 50)}  p99=#{pctl(m.produce_lat, 99)}  max=#{List.last(m.produce_lat)}
    CONSUME   #{m.consumed} recs in #{Float.round(cons_s, 2)}s  =>  #{round(m.consumed / cons_s)} rec/s
      page latency (#{@batch}/page) us:    p50=#{pctl(m.consume_lat, 50)}  p99=#{pctl(m.consume_lat, 99)}  max=#{List.last(m.consume_lat)}

    DISK      #{mb(m.disk)} MB on disk  =>  #{Float.round(m.disk / @total, 1)} bytes/record (payload #{@value_bytes}B)
    CPU       #{m.reductions} reductions total (#{round((m.reductions) / (@total * 2))} per record round-trip)
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
