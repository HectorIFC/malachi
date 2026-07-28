# Viability benchmark: can pure-Elixir/BEAM file I/O meet NorthGuard's storage targets?
#
# NorthGuard fps-store targets (from blog + meetup video):
#   - fsync on ALL replicas BEFORE produce ACK
#   - flush triggers: every ~10ms OR 20k records OR 10MB
#   - segments up to 1GB, file-per-segment, Direct I/O (O_DIRECT) + RocksDB sparse index
#
# This measures the LOCAL single-replica write/read hot path in pure Elixir.
# (Replication/coordination is NOT the language-sensitive part, BEAM is strong there.)

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
      batches: #{length(lat_us)}  | wall: #{Float.round(wall_us/1_000_000,2)}s
      per-flush latency ms:  p50=#{Float.round(p50,3)}  p99=#{Float.round(p99,3)}  max=#{Float.round(pmax,3)}
      throughput:  #{Float.round(mbps,1)} MB/s  |  #{round(recs)} records/s
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
    stats("  flush size #{Float.round(batch_bytes/1_048_576,2)}MB (#{batch_count} x #{rec_size}B)",
          lat, batch_bytes * n_batches, batch_count * n_batches, wall)
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
    stats("  buffered (delayed_write, fsync@end) #{batch_count} x #{rec_size}B",
          lat, batch_bytes * n_batches, batch_count * n_batches, wall)
  end

  def read_seq(path) do
    size = File.stat!(path).size
    {:ok, fd} = :file.open(path, [:read, :raw, :binary, {:read_ahead, 4_194_304}])
    t0 = now_us()
    total = read_loop(fd, 1_048_576, 0)
    wall = now_us() - t0
    :file.close(fd)
    mbps = total / 1_048_576 / (wall / 1_000_000)
    IO.puts("  sequential read: #{Float.round(total/1_048_576,1)}MB in #{Float.round(wall/1_000_000,2)}s = #{Float.round(mbps,1)} MB/s")
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
    IO.puts("  fsync() floor latency (1-byte write): p50=#{Float.round(pctl(sorted,50)/1000,3)}ms  p99=#{Float.round(pctl(sorted,99)/1000,3)}ms  max=#{Float.round(List.last(sorted)/1000,3)}ms")
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
Bench.durable(1024, 1024, 2000, &:file.sync/1)      # 1MB batches
Bench.durable(1024, 4096, 1000, &:file.sync/1)      # 4MB batches
Bench.durable(1024, 10240, 500, &:file.sync/1)      # 10MB batches (NG threshold)
# count-driven (NorthGuard flushes at 20k records), small records
Bench.durable(256, 20000, 300, &:file.sync/1)       # 20k x 256B ~= 5MB
# datasync variant (fdatasync, metadata-light)
IO.puts("  -- datasync (fdatasync) variant --")
Bench.durable(1024, 10240, 500, &:file.datasync/1)  # 10MB w/ datasync

IO.puts("\n========== NON-DURABLE upper bound (delayed_write) ==========")
Bench.buffered(1024, 10240, 1000)                   # 10MB batches buffered
Bench.buffered(256, 20000, 500)

IO.puts("\n========== READ path ==========")
Bench.read_seq(Path.join("/tmp/ng_bench_data", "seg_buffered.log"))

IO.puts("\n========== per-broker reality check ==========")
IO.puts("  NorthGuard: 17 PB/day across ~10k brokers ~= 20 MB/s avg WRITE per broker")
IO.puts("  (x3 replication ~= 60 MB/s). Peak higher, but this is the steady-state bar.\n")

File.rm_rf!("/tmp/ng_bench_data")
