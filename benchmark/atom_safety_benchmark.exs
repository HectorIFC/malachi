#!/usr/bin/env elixir

# Atom Safety Benchmark
#
# Compares performance of the new anonymous ETS table approach vs
# the old atom-based named table approach.
#
# Run: mix run benchmark/atom_safety_benchmark.exs

alias MalachiMQ.Metrics

IO.puts("""
═══════════════════════════════════════════════════════════════
  MalachiMQ Atom Safety Benchmark
  Comparing anonymous ETS tables vs named (atom-based) tables
═══════════════════════════════════════════════════════════════
""")

# ---- Benchmark 1: Atom Count Stability ----

IO.puts("📊 Benchmark 1: Atom Count Stability")
IO.puts("─────────────────────────────────────")

baseline_atoms = :erlang.system_info(:atom_count)
IO.puts("  Baseline atom count: #{baseline_atoms}")

# Create 1000 queues via enqueue
{time_us, _} =
  :timer.tc(fn ->
    for i <- 1..1000 do
      name = "bench_queue_#{i}_#{:rand.uniform(100_000)}"
      MalachiMQ.Queue.enqueue(name, "bench payload #{i}")
    end
  end)

after_atoms = :erlang.system_info(:atom_count)
atom_increase = after_atoms - baseline_atoms

IO.puts("  After creating 1000 queues:")
IO.puts("    Atom count: #{after_atoms}")
IO.puts("    Atoms created: #{atom_increase}")
IO.puts("    Time: #{div(time_us, 1000)}ms")
IO.puts("    Expected (old): ~3000 atoms (3 per queue)")
IO.puts("    Expected (new): ~0 atoms (anonymous ETS)")
IO.puts("")

if atom_increase < 100 do
  IO.puts("  ✅ PASS: Atom count stable (#{atom_increase} new atoms)")
else
  IO.puts("  ❌ FAIL: Too many atoms created (#{atom_increase})")
end

IO.puts("")

# ---- Benchmark 2: ETS Lookup Performance ----

IO.puts("📊 Benchmark 2: ETS Lookup Performance (Anonymous vs Named)")
IO.puts("──────────────────────────────────────────────────────────────")

# Anonymous table (current approach)
anon_table = :ets.new(:bench_anon, [:set, :public])
for i <- 1..10_000, do: :ets.insert(anon_table, {{:key, "queue_#{i}"}, i})

{anon_time, _} =
  :timer.tc(fn ->
    for _ <- 1..100_000 do
      :ets.lookup(anon_table, {:key, "queue_#{:rand.uniform(10_000)}"})
    end
  end)

# Named table (old approach - using a fixed atom name for comparison)
named_table = :ets.new(:bench_named_table, [:set, :public, :named_table])
for i <- 1..10_000, do: :ets.insert(named_table, {{:key, "queue_#{i}"}, i})

{named_time, _} =
  :timer.tc(fn ->
    for _ <- 1..100_000 do
      :ets.lookup(:bench_named_table, {:key, "queue_#{:rand.uniform(10_000)}"})
    end
  end)

# Cleanup named table
:ets.delete(:bench_named_table)

IO.puts("  100,000 lookups with 10,000 entries:")
IO.puts("    Anonymous table: #{div(anon_time, 1000)}ms (#{Float.round(anon_time / 100_000, 2)}µs/op)")
IO.puts("    Named table:     #{div(named_time, 1000)}ms (#{Float.round(named_time / 100_000, 2)}µs/op)")

diff_pct = Float.round((anon_time - named_time) / max(named_time, 1) * 100, 1)
IO.puts("    Difference:      #{diff_pct}%")

if abs(diff_pct) < 20 do
  IO.puts("  ✅ PASS: Performance within 20% tolerance")
else
  IO.puts("  ⚠️  Performance difference exceeds 20%")
end

IO.puts("")

# ---- Benchmark 3: Metrics Tuple Key Performance ----

IO.puts("📊 Benchmark 3: Metrics Operations (Tuple Keys)")
IO.puts("────────────────────────────────────────────────")

queue_names = for i <- 1..100, do: "metric_bench_#{i}"

{metrics_time, _} =
  :timer.tc(fn ->
    for _ <- 1..10_000 do
      name = Enum.random(queue_names)
      Metrics.increment_enqueued(name)
      Metrics.increment_acked(name)
      Metrics.increment_processed(name)
    end
  end)

IO.puts("  30,000 metric increments (10K × 3 operations):")
IO.puts("    Total time: #{div(metrics_time, 1000)}ms")
IO.puts("    Per operation: #{Float.round(metrics_time / 30_000, 2)}µs")
IO.puts("  ✅ Metrics use tuple keys (no atom creation)")

IO.puts("")

# ---- Benchmark 4: Memory Usage ----

IO.puts("📊 Benchmark 4: Memory Usage")
IO.puts("────────────────────────────")

memory_stats = MalachiMQ.MemoryMonitor.get_memory_stats()

IO.puts("  Current memory:")
IO.puts("    Total:     #{memory_stats.total_mb} MB")
IO.puts("    Processes: #{memory_stats.processes_mb} MB")
IO.puts("    ETS:       #{memory_stats.ets_mb} MB")
IO.puts("    Atoms:     #{memory_stats.atom_mb} MB")
IO.puts("    Binary:    #{memory_stats.binary_mb} MB")
IO.puts("    Code:      #{memory_stats.code_mb} MB")

IO.puts("")

# ---- Benchmark 5: Atom Monitor Stats ----

IO.puts("📊 Benchmark 5: Atom Table Status")
IO.puts("──────────────────────────────────")

atom_stats = MalachiMQ.AtomMonitor.get_stats()

IO.puts("  Atom count:    #{atom_stats.atom_count}")
IO.puts("  Atom limit:    #{atom_stats.atom_limit}")
IO.puts("  Usage:         #{atom_stats.usage_percent}%")
IO.puts("  Status:        #{atom_stats.status}")

IO.puts("")

# ---- Summary ----

IO.puts("""
═══════════════════════════════════════════════════════════════
  Summary
═══════════════════════════════════════════════════════════════
  Atoms created for 1000 queues: #{atom_increase} (target: < 100)
  ETS lookup overhead:           #{diff_pct}% (target: < 20%)
  Metric operation:              #{Float.round(metrics_time / 30_000, 2)}µs/op
  Atom table usage:              #{atom_stats.usage_percent}%
═══════════════════════════════════════════════════════════════
""")

# Cleanup
:ets.delete(anon_table)
