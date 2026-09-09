# Viability benchmark: can pure-Elixir/BEAM file I/O meet NorthGuard's storage targets?
#
# NorthGuard fps-store targets (from blog + meetup video):
#   - fsync on ALL replicas BEFORE produce ACK
#   - flush triggers: every ~10ms OR 20k records OR 10MB
#   - segments up to 1GB, file-per-segment, Direct I/O (O_DIRECT) + RocksDB sparse index
#
# This measures the LOCAL single-replica write/read hot path in pure Elixir.
# (Replication/coordination is NOT the language-sensitive part, BEAM is strong there.)
#
# SYNC_AB=1 additionally runs the paired fsync-vs-fdatasync experiment behind issue #82 (see the
# SyncAB module below) and writes its result as JSON. Off by default so the CI benchmark job keeps
# its current runtime; the A/B mode is minutes, not seconds.
#
#   SYNC_AB=1 SYNC_AB_OUT=/tmp/sync-ab.json mix run --no-start benchmark/storage_viability.exs

defmodule Bench do
  @dir "/tmp/ng_bench_data"

  def now_us, do: System.monotonic_time(:microsecond)

  def pctl(sorted, p) do
    idx = max(0, round(p / 100 * (length(sorted) - 1)))
    Enum.at(sorted, idx)
  end

  def stats(label, lat_us, total_bytes, total_recs, wall_us) do
    sorted = Enum.sort(lat_us)
    p50 = pctl(sorted, 50) / 1000
    p99 = pctl(sorted, 99) / 1000
    pmax = (List.last(sorted) || 0) / 1000
    mbps = total_bytes / 1_048_576 / (wall_us / 1_000_000)
    recs = total_recs / (wall_us / 1_000_000)

    IO.puts("""
    #{label}
      batches: #{length(lat_us)}  | wall: #{Float.round(wall_us / 1_000_000, 2)}s
      per-flush latency ms:  p50=#{Float.round(p50, 3)}  p99=#{Float.round(p99, 3)}  max=#{Float.round(pmax, 3)}
      throughput:  #{Float.round(mbps, 1)} MB/s  |  #{round(recs)} records/s
    """)
  end

  def mk_record(size), do: :crypto.strong_rand_bytes(size)

  # Build one batch as a single iolist write (mimics WAL append of a batch)
  def make_batch(rec_size, count), do: for(_ <- 1..count, do: mk_record(rec_size))

  def setup do
    File.rm_rf!(@dir)
    File.mkdir_p!(@dir)
  end

  # Durable mode: write batch + fsync before "ack". This is the NorthGuard model.
  def durable(rec_size, batch_count, n_batches, sync_fun) do
    path = Path.join(@dir, "seg_durable.log")
    {:ok, fd} = :file.open(path, [:write, :raw, :binary])
    batch = make_batch(rec_size, batch_count)
    batch_bytes = IO.iodata_length(batch)
    t0 = now_us()

    lat =
      for _ <- 1..n_batches do
        s = now_us()
        :ok = :file.write(fd, batch)
        :ok = sync_fun.(fd)
        now_us() - s
      end

    wall = now_us() - t0
    :file.close(fd)

    stats(
      "  flush size #{Float.round(batch_bytes / 1_048_576, 2)}MB (#{batch_count} x #{rec_size}B)",
      lat,
      batch_bytes * n_batches,
      batch_count * n_batches,
      wall
    )
  end

  # Non-durable upper bound: delayed_write, no per-batch fsync (one sync at end).
  def buffered(rec_size, batch_count, n_batches) do
    path = Path.join(@dir, "seg_buffered.log")
    {:ok, fd} = :file.open(path, [:write, :raw, :binary, {:delayed_write, 8_388_608, 1000}])
    batch = make_batch(rec_size, batch_count)
    batch_bytes = IO.iodata_length(batch)
    t0 = now_us()

    lat =
      for _ <- 1..n_batches do
        s = now_us()
        :ok = :file.write(fd, batch)
        now_us() - s
      end

    :file.sync(fd)
    wall = now_us() - t0
    :file.close(fd)

    stats(
      "  buffered (delayed_write, fsync@end) #{batch_count} x #{rec_size}B",
      lat,
      batch_bytes * n_batches,
      batch_count * n_batches,
      wall
    )
  end

  def read_seq(path) do
    size = File.stat!(path).size
    {:ok, fd} = :file.open(path, [:read, :raw, :binary, {:read_ahead, 4_194_304}])
    t0 = now_us()
    total = read_loop(fd, 1_048_576, 0)
    wall = now_us() - t0
    :file.close(fd)
    mbps = total / 1_048_576 / (wall / 1_000_000)

    IO.puts(
      "  sequential read: #{Float.round(total / 1_048_576, 1)}MB in #{Float.round(wall / 1_000_000, 2)}s = #{Float.round(mbps, 1)} MB/s"
    )

    size
  end

  defp read_loop(fd, chunk, acc) do
    case :file.read(fd, chunk) do
      {:ok, data} -> read_loop(fd, chunk, acc + byte_size(data))
      :eof -> acc
    end
  end

  def fsync_floor do
    path = Path.join(@dir, "fsync_floor.log")
    {:ok, fd} = :file.open(path, [:write, :raw, :binary])

    lat =
      for _ <- 1..200 do
        :file.write(fd, <<0>>)
        s = now_us()
        :file.sync(fd)
        now_us() - s
      end

    :file.close(fd)
    sorted = Enum.sort(lat)

    IO.puts(
      "  fsync() floor latency (1-byte write): p50=#{Float.round(pctl(sorted, 50) / 1000, 3)}ms  p99=#{Float.round(pctl(sorted, 99) / 1000, 3)}ms  max=#{Float.round(List.last(sorted) / 1000, 3)}ms"
    )
  end
end

# ---------------------------------------------------------------------------------------------
# SyncAB: the paired fsync-vs-fdatasync experiment (issue #82).
#
# The measurement, not the code change, is the deliverable here, and a single before/after run
# cannot support a conclusion: the published ceiling sweep varies by more than 30% run to run on
# the same code, and any fdatasync win is a few percent at most. So this does three things a naive
# before/after does not:
#
#   1. INTERLEAVES the arms (A, B, A, B, ...) inside one process on one filesystem. Thermal drift,
#      a noisy neighbour on a shared runner, or page-cache state then hit both arms alike instead
#      of landing entirely on whichever arm ran second.
#   2. Runs an A-A CONTROL: two arms that are both plain fsync, labelled as if they differed. The
#      spread it reports is this harness's noise floor, MEASURED rather than assumed. A fsync-vs-
#      datasync delta smaller than the fsync-vs-fsync delta is noise by construction.
#   3. Bootstraps a 95% confidence interval for the difference of medians, so the answer comes with
#      an interval instead of a point estimate that invites reading a win into noise.
#
# The verdict rule is fixed here, before any number exists, precisely so it cannot be chosen after
# seeing the result: signal requires BOTH that the A/B delta exceeds the A-A control delta AND that
# the bootstrap interval excludes zero. Anything else reports as noise, and "noise" is a perfectly
# good answer that closes the issue with the measurement recorded.
# ---------------------------------------------------------------------------------------------
defmodule SyncAB do
  @dir "/tmp/ng_sync_ab"
  # Discarded before the measured repetitions: the first pass on a fresh directory pays for cold
  # page cache and first-touch allocation, which is a property of the harness and not of the syscall.
  @warmup_reps 1
  @bootstrap_iterations 10_000

  # Each case is {label, record_size, records_per_batch, batches_per_rep}. The first is the one that
  # matters most: the pinned ceiling harness runs batch 10 x 256B with no group commit, so it pays
  # one sync per ~2.5KB, which is the regime where the syscall's fixed cost dominates and where a
  # metadata flush is the largest share of the bill. The larger cases are there to show the delta
  # shrinking as data transfer takes over, which is the shape the theory predicts.
  @cases [
    {"batch 10 x 256B (the pinned ceiling regime)", 256, 10, 500},
    {"batch 100 x 256B", 256, 100, 300},
    {"batch 1024 x 1KB", 1024, 1024, 20}
  ]

  def run(reps) do
    File.rm_rf!(@dir)
    File.mkdir_p!(@dir)

    results =
      Enum.map(@cases, fn {label, rec_size, batch_count, n_batches} ->
        IO.puts("\n  case: #{label}")

        experiment =
          paired(label, rec_size, batch_count, n_batches, reps, &:file.sync/1, &:file.datasync/1)

        control =
          paired(label <> " [A-A control]", rec_size, batch_count, n_batches, reps, &:file.sync/1, &:file.sync/1)

        verdict = verdict(experiment, control)
        report(experiment, control, verdict)

        %{
          case: label,
          record_bytes: rec_size,
          records_per_batch: batch_count,
          batches_per_rep: n_batches,
          experiment: experiment,
          control: control,
          verdict: verdict
        }
      end)

    File.rm_rf!(@dir)
    results
  end

  # One arm, one repetition: a fresh file, `n_batches` write+sync cycles, the per-flush latencies.
  defp measure(rec_size, batch_count, n_batches, sync_fun, tag) do
    path = Path.join(@dir, "seg_#{tag}_#{System.unique_integer([:positive])}.log")
    {:ok, fd} = :file.open(path, [:write, :raw, :binary])
    batch = Bench.make_batch(rec_size, batch_count)

    latencies =
      for _ <- 1..n_batches do
        started = Bench.now_us()
        :ok = :file.write(fd, batch)
        :ok = sync_fun.(fd)
        Bench.now_us() - started
      end

    :file.close(fd)
    File.rm!(path)
    latencies
  end

  # Interleaved repetitions of two arms. Returns the per-rep p50 and p99 of each arm, which are the
  # samples the statistics below run on: one repetition is one independent observation of the arm.
  defp paired(label, rec_size, batch_count, n_batches, reps, a_fun, b_fun) do
    for _ <- 1..@warmup_reps do
      measure(rec_size, batch_count, n_batches, a_fun, "warm")
      measure(rec_size, batch_count, n_batches, b_fun, "warm")
    end

    samples =
      for _ <- 1..reps do
        # A then B inside the same iteration, so a slow moment on the machine is shared, not attributed.
        a = measure(rec_size, batch_count, n_batches, a_fun, "a")
        b = measure(rec_size, batch_count, n_batches, b_fun, "b")
        {summarize(a), summarize(b)}
      end

    %{
      label: label,
      reps: reps,
      a: Enum.map(samples, &elem(&1, 0)),
      b: Enum.map(samples, &elem(&1, 1))
    }
  end

  defp summarize(latencies) do
    sorted = Enum.sort(latencies)
    %{p50: Bench.pctl(sorted, 50), p99: Bench.pctl(sorted, 99)}
  end

  defp verdict(experiment, control) do
    Map.new([:p50, :p99], fn stat ->
      a = Enum.map(experiment.a, & &1[stat])
      b = Enum.map(experiment.b, & &1[stat])
      control_delta = abs(median(Enum.map(control.a, & &1[stat])) - median(Enum.map(control.b, & &1[stat])))

      delta = median(b) - median(a)
      {low, high} = bootstrap_ci(a, b)

      # Both conditions must hold. The control gate alone would call a tiny-but-consistent shift
      # signal on a very quiet machine; the interval alone would call a large-but-erratic one signal
      # on a noisy one. Requiring both is what keeps the answer honest in either direction.
      beats_control = abs(delta) > control_delta
      excludes_zero = (low > 0 and high > 0) or (low < 0 and high < 0)

      {stat,
       %{
         fsync_median_us: median(a),
         datasync_median_us: median(b),
         delta_us: delta,
         delta_pct: percent(delta, median(a)),
         control_delta_us: control_delta,
         ci95_low_us: low,
         ci95_high_us: high,
         beats_control: beats_control,
         excludes_zero: excludes_zero,
         signal: beats_control and excludes_zero
       }}
    end)
  end

  # Percentile bootstrap of the difference of medians: resample each arm's per-rep observations with
  # replacement, recompute the difference, and take the 2.5th/97.5th percentiles of that distribution.
  defp bootstrap_ci(a, b) do
    diffs =
      for _ <- 1..@bootstrap_iterations do
        median(resample(a)) - median(resample(b))
      end
      |> Enum.sort()

    # Negated because the statistic above is (a - b) while the reported delta is (b - a).
    {-Bench.pctl(diffs, 97.5), -Bench.pctl(diffs, 2.5)}
  end

  defp resample(samples) do
    count = length(samples)
    for _ <- 1..count, do: Enum.at(samples, :rand.uniform(count) - 1)
  end

  defp median([]), do: 0.0

  defp median(values) do
    sorted = Enum.sort(values)
    count = length(sorted)
    middle = div(count, 2)

    if rem(count, 2) == 1 do
      Enum.at(sorted, middle) * 1.0
    else
      (Enum.at(sorted, middle - 1) + Enum.at(sorted, middle)) / 2
    end
  end

  defp percent(_delta, 0.0), do: 0.0
  defp percent(delta, base), do: Float.round(delta / base * 100, 2)

  defp report(experiment, control, verdict) do
    for stat <- [:p50, :p99] do
      v = verdict[stat]

      IO.puts(
        "    #{stat}: fsync #{us(v.fsync_median_us)}  datasync #{us(v.datasync_median_us)}  " <>
          "delta #{us(v.delta_us)} (#{v.delta_pct}%)  " <>
          "ci95 [#{us(v.ci95_low_us)}, #{us(v.ci95_high_us)}]  " <>
          "noise floor #{us(v.control_delta_us)}  => #{if v.signal, do: "SIGNAL", else: "noise"}"
      )
    end

    _ = {experiment, control}
    :ok
  end

  defp us(value), do: "#{Float.round(value / 1000, 3)}ms"
end

Bench.setup()

IO.puts("\n========== ENV ==========")
IO.puts("  #{:erlang.system_info(:system_version) |> to_string() |> String.trim()}")
IO.puts("  schedulers online: #{:erlang.system_info(:schedulers_online)}")
IO.puts("  NOTE (macOS): :file.sync = fsync(2), which on macOS flushes to the DRIVE CACHE,")
IO.puts("        not stable media (no F_FULLFSYNC). Real Linux-server fsync will be SLOWER.")
IO.puts("        So latency here is OPTIMISTIC; throughput is representative.\n")

IO.puts("========== fsync floor ==========")
Bench.fsync_floor()

IO.puts("\n========== DURABLE: fsync-per-batch (the NorthGuard 'ack after fsync' model) ==========")
# size-driven flushes (NorthGuard flushes at 10MB)
# 1MB batches
Bench.durable(1024, 1024, 2000, &:file.sync/1)
# 4MB batches
Bench.durable(1024, 4096, 1000, &:file.sync/1)
# 10MB batches (NG threshold)
Bench.durable(1024, 10240, 500, &:file.sync/1)
# count-driven (NorthGuard flushes at 20k records), small records
# 20k x 256B ~= 5MB
Bench.durable(256, 20000, 300, &:file.sync/1)

IO.puts("\n========== NON-DURABLE upper bound (delayed_write) ==========")
# 10MB batches buffered
Bench.buffered(1024, 10240, 1000)
Bench.buffered(256, 20000, 500)

IO.puts("\n========== READ path ==========")
Bench.read_seq(Path.join("/tmp/ng_bench_data", "seg_buffered.log"))

IO.puts("\n========== per-broker reality check ==========")
IO.puts("  NorthGuard: 17 PB/day across ~10k brokers ~= 20 MB/s avg WRITE per broker")
IO.puts("  (x3 replication ~= 60 MB/s). Peak higher, but this is the steady-state bar.\n")

File.rm_rf!("/tmp/ng_bench_data")

# The paired fsync-vs-fdatasync experiment. Opt-in: it is minutes rather than seconds, and the CI
# benchmark job runs this script on every push.
if System.get_env("SYNC_AB") == "1" do
  reps = String.to_integer(System.get_env("SYNC_AB_REPS") || "15")

  IO.puts("\n========== SYNC A/B: fsync vs fdatasync (issue #82) ==========")
  IO.puts("  #{reps} interleaved repetitions per arm, plus an A-A control for the noise floor.")
  IO.puts("  Verdict rule (fixed before the run): signal requires the A/B delta to exceed the A-A")
  IO.puts("  control delta AND the bootstrapped 95% CI of the difference to exclude zero.\n")

  results = SyncAB.run(reps)

  report = %{
    schema: 1,
    generated_at: DateTime.utc_now() |> DateTime.to_iso8601(),
    otp: :erlang.system_info(:otp_release) |> to_string(),
    schedulers_online: :erlang.system_info(:schedulers_online),
    os: :os.type() |> Tuple.to_list() |> Enum.map(&to_string/1) |> Enum.join("/"),
    reps: reps,
    cases: results,
    # A run where no case shows signal is a complete answer, not a failed measurement: it says the
    # syscall swap is not worth shipping on this storage, which is what issue #82 asks.
    any_signal: Enum.any?(results, fn c -> c.verdict.p50.signal or c.verdict.p99.signal end)
  }

  out = System.get_env("SYNC_AB_OUT")

  if out do
    File.mkdir_p!(Path.dirname(out))
    File.write!(out, Jason.encode_to_iodata!(report, pretty: true))
    IO.puts("\n  wrote #{out}")
  else
    IO.puts("\n#{Jason.encode!(report, pretty: true)}")
  end

  IO.puts("\n  overall: #{if report.any_signal, do: "SIGNAL in at least one case", else: "NOISE in every case"}")
end
