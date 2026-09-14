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
# PREALLOC_AB=1 additionally runs the paired preallocation experiment behind issue #83 (see the
# PreallocAB module below) and writes its result as JSON. Off by default so the CI benchmark job
# keeps its current runtime; the A/B mode is minutes, not seconds.
#
#   PREALLOC_AB=1 PREALLOC_AB_OUT=/tmp/prealloc-ab.json mix run --no-start benchmark/storage_viability.exs

Code.require_file("support/paired_stats.exs", __DIR__)

defmodule Bench do
  @dir "/tmp/ng_bench_data"

  def now_us, do: System.monotonic_time(:microsecond)

  defdelegate pctl(sorted, p), to: Malachi.Bench.PairedStats

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
# datasync variant (fdatasync, metadata-light)
IO.puts("  -- datasync (fdatasync) variant --")
# 10MB w/ datasync
Bench.durable(1024, 10240, 500, &:file.datasync/1)

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

# ---------------------------------------------------------------------------------------------
# PreallocAB: the paired preallocation experiment (issue #83), which subsumes the fsync-vs-
# fdatasync one (issue #82).
#
# #82 measured the syscall swap and found NOTHING: fsync 341us, fdatasync 345us, and an A-A
# control that differed by 3us, on ubuntu-latest with 15 interleaved repetitions per arm. The
# reason is the useful part. A 1-byte fsync cost 303us there and a 2.5KB one cost 341us, so the
# bill is almost entirely fixed cost, the journal commit rather than the transfer, and fdatasync
# cannot remove that while the segment GROWS: every append changes the file size, that size change
# is metadata fdatasync must journal anyway, and what it saves rides along in a commit it has to
# make regardless.
#
# So preallocation is not a companion to the syscall swap, it is its PREREQUISITE, and the useful
# observable is the PAIR: with the file already sized, does fdatasync finally beat fsync? If it
# still ties, the fixed-cost hypothesis was wrong and that is the finding, not a failed run.
#
# What this keeps from the #82 harness, because it is what made a null result trustworthy:
#
#   1. INTERLEAVED arms in one process on one filesystem, so thermal drift or a noisy neighbour
#      hits every arm alike instead of landing on whichever ran last.
#   2. An A-A CONTROL: two arms that are identical, labelled as if they differed. The spread it
#      reports is this harness's noise floor, MEASURED rather than assumed.
#   3. A bootstrapped 95% confidence interval for the difference of medians.
#   4. The verdict rule fixed HERE, before any number exists: signal requires BOTH that the pair's
#      delta exceed the A-A control delta AND that the interval exclude zero.
#
# What it fixes and adds:
#
#   * ROTATED ORDER. #82's arms interleaved but always ran A before B, so any position effect
#      landed on B every time (all six of its deltas came out positive, which has no plausible
#      mechanism). The arm order now rotates by repetition.
#   * FOUR MECHANISMS, not one. Growing, sparse, :file.allocate and written zeros differ in what
#      they leave for the first append to journal, so they are not interchangeable. They run
#      through Malachi.Storage.Preallocation, the module the store itself uses, so the benchmark
#      measures the code that would ship.
#   * TWO STAGES. Stage 1 triages in the one regime that matters (batch 10 x 256B, what the pinned
#      ceiling harness runs). Stage 2 confirms only the winner against the production baseline
#      across all three batch shapes.
#   * CREATION COST and a per-mechanism 1-byte sync floor, which is the direct test of the
#      fixed-cost claim and the other side of the zero-write trade-off.
# ---------------------------------------------------------------------------------------------
defmodule PreallocAB do
  alias Malachi.Bench.PairedStats
  alias Malachi.Storage.Preallocation

  @dir "/tmp/ng_prealloc_ab"
  # Discarded before the measured repetitions: the first pass on a fresh directory pays for cold
  # page cache and first-touch allocation, which is a property of the harness, not of the syscall.
  @warmup_reps 1
  @floor_reps 200

  # {label, record_size, records_per_batch, batches_per_rep}. The first is the regime that matters:
  # the pinned ceiling harness runs batch 10 x 256B with no group commit, so it pays one sync per
  # ~2.5KB, where the syscall's fixed cost dominates. The larger ones show the delta shrinking as
  # data transfer takes over, which is the shape the theory predicts.
  @triage_case {"batch 10 x 256B (the pinned ceiling regime)", 256, 10, 500}
  @confirm_cases [
    @triage_case,
    {"batch 100 x 256B", 256, 100, 300},
    {"batch 1024 x 1KB", 1024, 1024, 20}
  ]

  # The crossover sweep. Preallocation wins big where the journal dominates the bill (a few KB per
  # flush) and loses where the transfer does (a megabyte per flush), so the number an operator needs
  # is not either end, it is WHERE it turns over. Record size is held at 1KB and the batch count is
  # what moves, so the only variable is bytes per flush; batches per repetition scale inversely to
  # keep each repetition writing about 20MB whatever the flush size.
  @sweep_cases [
    {"2.5KB per flush (batch 10 x 256B)", 256, 10, 500},
    {"25KB per flush (batch 100 x 256B)", 256, 100, 300},
    {"64KB per flush (batch 64 x 1KB)", 1024, 64, 320},
    {"128KB per flush (batch 128 x 1KB)", 1024, 128, 160},
    {"256KB per flush (batch 256 x 1KB)", 1024, 256, 80},
    {"512KB per flush (batch 512 x 1KB)", 1024, 512, 40},
    {"1MB per flush (batch 1024 x 1KB)", 1024, 1024, 20}
  ]

  @mechanisms [:grow, :sparse, :allocate, :zeros]

  @doc "Stage 1: every mechanism against both syncs, in the one regime that matters."
  def triage(reps) do
    arms = for m <- @mechanisms, sync <- [:sync, :datasync], do: arm(m, sync)
    run([@triage_case], arms ++ control_arms(), triage_comparisons(), reps)
  end

  @doc """
  Stage 2: only the winning mechanism against the production baseline (growing + fsync), across
  every batch shape, so the number that gets published is not the one that chose the winner.
  """
  def confirm(reps, mechanism, sync) do
    arms = [arm(:grow, :sync), arm(mechanism, sync)] ++ control_arms()
    comparisons = [{"#{mechanism}+#{sync} vs the production baseline", key(mechanism, sync), key(:grow, :sync)}]
    run(@confirm_cases, arms, comparisons ++ [control_comparison()], reps)
  end

  @doc """
  Stage 3: the winning mechanism against the production baseline across flush sizes, to find where
  the win turns into a loss.

  Same arms as stage 2 and the same A-A control, so each case is read the same way; only the ladder
  of flush sizes is new. It answers the one question the operator-facing knob actually needs: below
  what flush size is preallocation worth turning on.
  """
  def sweep(reps, mechanism, sync) do
    arms = [arm(:grow, :sync), arm(mechanism, sync)] ++ control_arms()
    comparisons = [{"#{mechanism}+#{sync} vs the production baseline", key(mechanism, sync), key(:grow, :sync)}]
    run(@sweep_cases, arms, comparisons ++ [control_comparison()], reps)
  end

  defp arm(mechanism, sync), do: %{key: key(mechanism, sync), mechanism: mechanism, sync: sync}

  defp key(mechanism, sync), do: :"#{mechanism}_#{sync}"

  # Both control arms are the SAME configuration, labelled as if they differed. Whatever they
  # report as a difference is this harness lying to itself, and every real comparison has to beat
  # it. They ride in the same rotation as the arms under test so they carry the same biases.
  defp control_arms do
    [
      %{key: :control_a1, mechanism: :zeros, sync: :sync},
      %{key: :control_a2, mechanism: :zeros, sync: :sync}
    ]
  end

  defp control_comparison, do: {"A-A control (identical arms)", :control_a2, :control_a1}

  # The four within-mechanism pairs answer "does fdatasync finally win", one per mechanism, and the
  # last two answer "does preallocation alone move anything" and "does the PAIR beat what runs in
  # production today", which is the observable issue #83 is actually about.
  defp triage_comparisons do
    within = for m <- @mechanisms, do: {"#{m}: fdatasync vs fsync", key(m, :datasync), key(m, :sync)}

    within ++
      [
        {"zeros+fsync vs grow+fsync (preallocation alone)", key(:zeros, :sync), key(:grow, :sync)},
        {"zeros+fdatasync vs grow+fsync (the pair vs production)", key(:zeros, :datasync), key(:grow, :sync)},
        control_comparison()
      ]
  end

  defp run(cases, arms, comparisons, reps) do
    File.rm_rf!(@dir)
    File.mkdir_p!(@dir)

    results =
      Enum.map(cases, fn {label, rec_size, batch_count, n_batches} = one_case ->
        IO.puts("\n  case: #{label}")
        samples = paired(one_case, arms, reps)
        verdicts = Enum.map(comparisons, &verdict(&1, samples))
        report(samples, verdicts)

        %{
          case: label,
          record_bytes: rec_size,
          records_per_batch: batch_count,
          batches_per_rep: n_batches,
          arms: Map.new(samples, fn {arm_key, values} -> {arm_key, summary(values)} end),
          comparisons: verdicts
        }
      end)

    File.rm_rf!(@dir)
    results
  end

  # Interleaved repetitions of every arm, SHUFFLED per repetition. Returns each arm's per-rep
  # p50/p99, which are the samples the statistics run on: one repetition is one independent
  # observation of that arm.
  #
  # It was a cyclic rotation first, and that was not enough, which the harness caught on itself. A
  # rotation changes which arm goes FIRST but preserves the circular order, so every arm still
  # follows the same neighbour every time, and whatever the neighbour leaves behind (page cache,
  # pending writeback) lands on the same arm forever. In the batch 1024 x 1KB case, where each
  # repetition writes tens of megabytes, three arms with IDENTICAL configuration split 2060us,
  # 2047us and 2897us: the two that were labelled controls agreed to within 13us while the third,
  # the only one that always followed the growing arm, sat 850us above them. A shuffle is what
  # actually breaks that, and keeping a third identical arm in the rotation is what made the bias
  # visible instead of letting it be read as a result.
  defp paired({_label, rec_size, batch_count, n_batches}, arms, reps) do
    for _ <- 1..@warmup_reps, arm <- arms do
      measure(rec_size, batch_count, n_batches, arm, "warm")
    end

    1..reps
    |> Enum.reduce(Map.new(arms, &{&1.key, []}), fn _rep, acc ->
      arms
      |> Enum.shuffle()
      |> Enum.reduce(acc, fn arm, inner ->
        sample = summarize(measure(rec_size, batch_count, n_batches, arm, "run"))
        Map.update!(inner, arm.key, &[sample | &1])
      end)
    end)
    |> Map.new(fn {arm_key, values} -> {arm_key, Enum.reverse(values)} end)
  end

  # One arm, one repetition: a fresh file, preallocated by this arm's mechanism, then `n_batches`
  # write+sync cycles at explicit positions. The pwrite mirrors what Malachi.Storage.ElixirStore
  # does, so an arm measures the shape of write the store actually issues. Preallocation happens
  # BEFORE the timed loop on purpose: its cost is a separate measurement (creation_cost/1), and
  # folding it in here would smear a one-off into a per-flush number.
  defp measure(rec_size, batch_count, n_batches, arm, tag) do
    path = Path.join(@dir, "seg_#{arm.key}_#{tag}_#{System.unique_integer([:positive])}.log")
    File.touch!(path)
    {:ok, fd} = :file.open(path, [:read, :write, :raw, :binary])
    batch = Bench.make_batch(rec_size, batch_count)
    batch_bytes = IO.iodata_length(batch)
    :ok = preallocate(fd, arm.mechanism, n_batches * batch_bytes)
    # Synced before the timed loop, and this is load-bearing. Preallocation leaves its whole region
    # dirty in the page cache, and an unsynced arm makes the first flushes pay for that writeback
    # instead of for their own data, which is measuring the harness rather than the mechanism. The
    # store syncs at the same point for the same reason.
    :ok = :file.sync(fd)
    sync = sync_fun(arm.sync)

    {latencies, _position} =
      Enum.map_reduce(1..n_batches, 0, fn _batch, position ->
        started = Bench.now_us()
        :ok = :file.pwrite(fd, position, batch)
        :ok = sync.(fd)
        {Bench.now_us() - started, position + batch_bytes}
      end)

    :file.close(fd)
    File.rm!(path)
    latencies
  end

  defp preallocate(_fd, :grow, _bytes), do: :ok
  defp preallocate(fd, mechanism, bytes), do: Preallocation.extend(fd, 0, bytes, mechanism)

  defp sync_fun(:sync), do: &:file.sync/1
  defp sync_fun(:datasync), do: &:file.datasync/1

  @doc """
  What creating a segment costs per mechanism, which is the other side of the zero-write trade-off:
  the appends get cheaper only if the one-off does not eat the saving. Reported separately rather
  than folded into the per-flush latency, because it is paid once per segment and amortised across
  its whole life.
  """
  def creation_cost(bytes) do
    File.mkdir_p!(@dir)

    costs =
      Map.new(@mechanisms, fn mechanism ->
        path = Path.join(@dir, "create_#{mechanism}.log")
        File.rm_rf!(path)
        File.touch!(path)
        {:ok, fd} = :file.open(path, [:read, :write, :raw, :binary])

        started = Bench.now_us()
        :ok = preallocate(fd, mechanism, bytes)
        :ok = :file.sync(fd)
        elapsed = Bench.now_us() - started

        :file.close(fd)
        File.rm_rf!(path)
        IO.puts("    #{pad(mechanism)} #{Float.round(elapsed / 1000, 2)}ms to create #{div(bytes, 1_048_576)}MB")
        {mechanism, elapsed}
      end)

    File.rm_rf!(@dir)
    costs
  end

  @doc """
  The 1-byte sync floor per mechanism: the direct test of the fixed-cost claim. If the bill really
  is the journal commit rather than the transfer, then a 1-byte sync costs nearly what a full batch
  does while the file grows, and preallocation is what should move it.
  """
  def sync_floor do
    File.mkdir_p!(@dir)

    combinations = for mechanism <- @mechanisms, sync <- [:sync, :datasync], do: {mechanism, sync}
    floors = Map.new(combinations, &one_floor/1)

    File.rm_rf!(@dir)
    floors
  end

  defp one_floor({mechanism, sync}) do
    path = Path.join(@dir, "floor_#{mechanism}_#{sync}.log")
    File.rm_rf!(path)
    File.touch!(path)
    {:ok, fd} = :file.open(path, [:read, :write, :raw, :binary])
    :ok = preallocate(fd, mechanism, @floor_reps)
    sync_call = sync_fun(sync)

    latencies =
      for i <- 0..(@floor_reps - 1) do
        :ok = :file.pwrite(fd, i, <<1>>)
        started = Bench.now_us()
        :ok = sync_call.(fd)
        Bench.now_us() - started
      end

    :file.close(fd)
    File.rm_rf!(path)
    p50 = median(latencies)
    IO.puts("    #{pad("#{mechanism}+#{sync}")} p50 #{Float.round(p50 / 1000, 3)}ms")
    {key(mechanism, sync), p50}
  end

  # The statistics and the verdict rule live in support/paired_stats.exs, shared with the storage
  # error-path experiment (issue #147), so two experiments cannot disagree about what a result is.
  defp summarize(latencies), do: PairedStats.summarize(latencies)
  defp summary(values), do: PairedStats.summary(values)
  defp verdict(comparison, samples), do: PairedStats.verdict(comparison, samples)
  defp median(values), do: PairedStats.median(values)
  defp report(samples, verdicts), do: PairedStats.report(samples, verdicts)
  defp pad(value), do: PairedStats.pad(value)
end

# The paired preallocation experiment. Opt-in: it is minutes rather than seconds, and the CI
# benchmark job runs this script on every push.
if System.get_env("PREALLOC_AB") == "1" do
  reps = String.to_integer(System.get_env("PREALLOC_AB_REPS") || "15")
  stage = System.get_env("PREALLOC_AB_STAGE") || "1"

  # Validated here, before anything runs, because both ways of getting this wrong waste a whole run.
  # An unknown NUMBER used to fall through to stage 1 and then be recorded as itself in the report,
  # so the artifact named a measurement that never happened; an unknown STRING ran stage 1 to
  # completion and then raised in String.to_integer/1 while building the report, minutes of runner
  # time for no file at all.
  if stage not in ["1", "2", "3"] do
    raise ArgumentError, "PREALLOC_AB_STAGE must be 1, 2 or 3, got: #{inspect(stage)}"
  end

  # Validated up front for the same reason the stage is: `chosen_arm` runs only after the sync floor
  # and four 64MB creation measurements, so a bad value costs all of that before it is noticed, and
  # some of them are not even noticed there. `Zeros` has no existing atom and raises in
  # `String.to_existing_atom/1`; `PREALLOC_AB_SYNC=zeros` parses cleanly and then raises a
  # FunctionClauseError inside the first warmup repetition instead.
  mechanism = System.get_env("PREALLOC_AB_MECHANISM") || "zeros"
  sync = System.get_env("PREALLOC_AB_SYNC") || "datasync"

  if mechanism not in ["grow", "sparse", "allocate", "zeros"] do
    raise ArgumentError, "PREALLOC_AB_MECHANISM must be grow, sparse, allocate or zeros, got: #{inspect(mechanism)}"
  end

  if sync not in ["sync", "datasync"] do
    raise ArgumentError, "PREALLOC_AB_SYNC must be sync or datasync, got: #{inspect(sync)}"
  end

  chosen_arm = fn -> {String.to_existing_atom(mechanism), String.to_existing_atom(sync)} end

  IO.puts("\n========== PREALLOCATION A/B (issue #83, blocking #82) ==========")
  IO.puts("  #{reps} interleaved repetitions per arm, arm order rotated per repetition,")
  IO.puts("  plus an A-A control (two identical arms) for the measured noise floor.")
  IO.puts("  Verdict rule (fixed before the run): signal requires the delta to exceed the A-A")
  IO.puts("  control delta AND the bootstrapped 95% CI of the difference to exclude zero.\n")

  IO.puts("  -- 1-byte sync floor per mechanism (the fixed-cost claim, tested directly) --")
  floors = PreallocAB.sync_floor()

  IO.puts("\n  -- segment creation cost per mechanism --")
  creation = PreallocAB.creation_cost(64 * 1024 * 1024)

  cases =
    case stage do
      "2" ->
        {mechanism, sync} = chosen_arm.()
        IO.puts("\n  stage 2: confirming #{mechanism}+#{sync} against the production baseline.")
        PreallocAB.confirm(reps, mechanism, sync)

      "3" ->
        {mechanism, sync} = chosen_arm.()
        IO.puts("\n  stage 3: sweeping flush sizes to find where #{mechanism}+#{sync} stops winning.")
        PreallocAB.sweep(reps, mechanism, sync)

      _stage_one ->
        IO.puts("\n  stage 1: triaging every mechanism against both syncs.")
        PreallocAB.triage(reps)
    end

  report = %{
    schema: 1,
    generated_at: DateTime.utc_now() |> DateTime.to_iso8601(),
    stage: String.to_integer(stage),
    otp: :erlang.system_info(:otp_release) |> to_string(),
    schedulers_online: :erlang.system_info(:schedulers_online),
    os: :os.type() |> Tuple.to_list() |> Enum.map(&to_string/1) |> Enum.join("/"),
    reps: reps,
    sync_floor_us: floors,
    creation_cost_us: creation,
    cases: cases,
    # A run where nothing shows signal is a COMPLETE answer, not a failed measurement: it says the
    # fixed-cost hypothesis behind #82 and #83 was wrong, which is what these issues ask.
    any_signal:
      Enum.any?(cases, fn one_case ->
        Enum.any?(one_case.comparisons, fn c -> c.stats.p50.signal or c.stats.p99.signal end)
      end)
  }

  out = System.get_env("PREALLOC_AB_OUT")

  if out do
    File.mkdir_p!(Path.dirname(out))
    File.write!(out, Jason.encode_to_iodata!(report, pretty: true))
    IO.puts("\n  wrote #{out}")
  else
    IO.puts("\n#{Jason.encode!(report, pretty: true)}")
  end

  IO.puts("\n  overall: #{if report.any_signal, do: "SIGNAL in at least one comparison", else: "NOISE everywhere"}")
end
