# What does it cost to PROVE a preallocated tail is really unwritten? (issue #149)
#
# `Malachi.Storage.ElixirStore` answers `:blank` for a zero frame header only once every byte after
# it has been read and found zero, and that is paid on every recovery of a HEALTHY segment, which is
# the common case. A bounded probe was tried instead and does not work: it looks only for non-zero
# bytes, and a record whose value is zeros carries almost none.
#
# Malachi runs on Linux and nowhere else, where recovery reads a file written moments earlier that is
# likely still in the page cache, and no measurement from a developer laptop says anything about
# that. This measures it there, warm and cold, alongside the recovery it is paid inside.
#
# It also measures the AGGREGATE, which is the number an operator actually feels: the blank tail
# exists only on a segment being written, there is one per range, and a restart recovers all of them.
# One segment is the unit; N segments is the restart. The chaos drill cannot answer this, since
# `wait_healthy` polls at five-second granularity and nothing on the recovery path is instrumented
# for duration, so a per-range cost in the tens of milliseconds is invisible there.
#
#   mix run --no-start benchmark/blank_tail_scan.exs

defmodule BlankTailScan do
  alias Malachi.Log.Record
  alias Malachi.Storage.{ElixirStore, Preallocation}

  @dir "/tmp/blank_tail_scan"
  @chunk 262_144
  @reps 5
  # How many active segments a restart recovers, which is one per range. 64 is not an upper bound,
  # it is a number a real deployment reaches without trying.
  @segment_counts [1, 8, 64]
  # Small on purpose for the aggregate: the question there is how the cost MULTIPLIES, and a
  # per-segment size large enough to be realistic would make the run about disk throughput instead.
  @aggregate_prealloc 4 * 1024 * 1024

  def run do
    File.rm_rf!(@dir)
    File.mkdir_p!(@dir)

    IO.puts("  os: #{:os.type() |> Tuple.to_list() |> Enum.join("/")}  otp: #{:erlang.system_info(:otp_release)}")

    IO.puts(
      "  page cache drop: #{if can_drop_caches?(), do: "available", else: "NOT available, cold numbers are warm"}\n"
    )

    sizes = [8 * 1024 * 1024, 64 * 1024 * 1024]
    scans = Enum.map(sizes, &scan_case/1)
    recoveries = Enum.map(sizes, &recover_case/1)
    aggregates = Enum.map(@segment_counts, &aggregate_case/1)

    File.rm_rf!(@dir)

    %{
      schema: 1,
      generated_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      os: :os.type() |> Tuple.to_list() |> Enum.join("/"),
      otp: :erlang.system_info(:otp_release) |> to_string(),
      reps: @reps,
      cache_drop_available: can_drop_caches?(),
      full_scan: scans,
      recovery: recoveries,
      aggregate: aggregates
    }
  end

  # The raw cost: read `size` bytes of zeros in bounded chunks, confirming each is zero.
  defp scan_case(size) do
    path = Path.join(@dir, "scan_#{size}.bin")
    File.write!(path, :binary.copy(<<0>>, size))

    warm = Enum.map(1..@reps, fn _ -> time_scan(path) end)

    cold =
      1..@reps
      |> Enum.map(fn _ -> if drop_caches(), do: time_scan(path) end)
      |> Enum.reject(&is_nil/1)

    # And the reassuring case: a non-zero byte early in the region, which is what a damaged tail
    # looks like. The expensive answer is the one that says everything is fine.
    early = Path.join(@dir, "early_#{size}.bin")
    File.write!(early, :binary.copy(<<0>>, 4096) <> "x" <> :binary.copy(<<0>>, size - 4097))
    bail = Enum.map(1..@reps, fn _ -> time_scan(early) end)

    File.rm_rf!(path)
    File.rm_rf!(early)

    report("full scan of #{mb(size)}", warm, cold, bail)

    %{
      bytes: size,
      warm_us: median(warm),
      cold_us: if(cold == [], do: nil, else: median(cold)),
      early_bail_us: median(bail),
      warm_all: warm,
      cold_all: cold
    }
  end

  # The cost in context: recovering a real preallocated segment as the store does today, then the
  # tail verification that closing #149 would add on top of it.
  defp recover_case(prealloc) do
    directory = Path.join(@dir, "seg_#{prealloc}")
    File.rm_rf!(directory)
    {:ok, store} = ElixirStore.open(directory, "segment-0", prealloc_bytes: prealloc)

    store =
      Enum.reduce(1..200, store, fn i, acc ->
        {:ok, acc, _f, _l} = ElixirStore.append(acc, [Record.new(:binary.copy("v", 256), key: "k#{i}")])
        {:ok, acc} = ElixirStore.sync(acc)
        acc
      end)

    valid_bytes = store.segment.byte_size
    path = Malachi.Log.Segment.path(store.segment)
    # Close the descriptor without trimming, which is the state a crashed node leaves behind.
    :ok = :file.close(store.file_descriptor)

    recovers =
      Enum.map(1..@reps, fn _ ->
        {us, {:ok, handle}} = :timer.tc(fn -> ElixirStore.recover(directory, "segment-0", prealloc_bytes: prealloc) end)
        :ok = ElixirStore.close(handle)
        # close/1 trims, so put the tail back for the next repetition.
        {:ok, fd} = :file.open(path, [:read, :write, :raw, :binary])
        :ok = Preallocation.extend(fd, valid_bytes, prealloc, :zeros)
        :ok = :file.close(fd)
        us
      end)

    # Measured apart, NOT added to the recovery above: `recover/3` already verifies the tail, so a
    # sum would count the same work twice. This is the share of the recovery that the verification
    # accounts for, which is what a different verification strategy would be replacing.
    verifications = Enum.map(1..@reps, fn _ -> time_scan_from(path, valid_bytes) end)
    File.rm_rf!(directory)

    IO.puts(
      "  recover a #{mb(prealloc)} segment holding #{valid_bytes} bytes of records:\n" <>
        "    whole recovery           #{ms(median(recovers))}\n" <>
        "    of which the tail check  #{ms(median(verifications))}\n"
    )

    %{
      prealloc_bytes: prealloc,
      valid_bytes: valid_bytes,
      recover_us: median(recovers),
      tail_verification_us: median(verifications)
    }
  end

  # What a restart costs: N active segments, each recovered in turn, which is what a node does when it
  # comes back. Reported both ways, because the decision needs both: today against the same recovery
  # with the tail verified whole.
  defp aggregate_case(count) do
    directories =
      Enum.map(1..count, fn i ->
        directory = Path.join(@dir, "agg_#{count}_#{i}")
        {:ok, store} = ElixirStore.open(directory, "segment-0", prealloc_bytes: @aggregate_prealloc)
        {:ok, store, _f, _l} = ElixirStore.append(store, [Record.new(:binary.copy("v", 256), key: "k")])
        {:ok, store} = ElixirStore.sync(store)
        valid_bytes = store.segment.byte_size
        :ok = :file.close(store.file_descriptor)
        {directory, Malachi.Log.Segment.path(store.segment), valid_bytes}
      end)

    total = median(Enum.map(1..@reps, fn _ -> time_recover_all(directories) end))
    verification = median(Enum.map(1..@reps, fn _ -> time_verify_all(directories) end))

    Enum.each(directories, fn {directory, _path, _valid} -> File.rm_rf!(directory) end)

    IO.puts(
      "  restart recovering #{count} active segment(s) of #{mb(@aggregate_prealloc)}:\n" <>
        "    whole restart            #{ms(total)}\n" <>
        "    of which the tail checks #{ms(verification)}\n"
    )

    %{
      segments: count,
      prealloc_bytes: @aggregate_prealloc,
      recover_us: total,
      tail_verification_us: verification
    }
  end

  defp time_recover_all(directories) do
    {us, _} =
      :timer.tc(fn ->
        Enum.each(directories, fn {directory, path, valid_bytes} ->
          {:ok, handle} = ElixirStore.recover(directory, "segment-0", prealloc_bytes: @aggregate_prealloc)
          :ok = ElixirStore.close(handle)
          restore_tail(path, valid_bytes)
        end)
      end)

    us
  end

  defp time_verify_all(directories) do
    {us, _} =
      :timer.tc(fn ->
        Enum.each(directories, fn {_directory, path, valid_bytes} -> scan_from(path, valid_bytes) end)
      end)

    us
  end

  # `close/1` trims the preallocated tail, so it is put back between repetitions.
  defp restore_tail(path, valid_bytes) do
    {:ok, fd} = :file.open(path, [:read, :write, :raw, :binary])
    :ok = Preallocation.extend(fd, valid_bytes, @aggregate_prealloc, :zeros)
    :ok = :file.close(fd)
  end

  defp scan_from(path, from) do
    {:ok, fd} = :file.open(path, [:read, :raw, :binary])
    result = all_zero_from(fd, from)
    :file.close(fd)
    result
  end

  defp time_scan(path), do: time_scan_from(path, 0)

  defp time_scan_from(path, from) do
    {:ok, fd} = :file.open(path, [:read, :raw, :binary])
    {us, _} = :timer.tc(fn -> all_zero_from(fd, from) end)
    :file.close(fd)
    us
  end

  defp all_zero_from(fd, position) do
    case :file.pread(fd, position, @chunk) do
      {:ok, bin} ->
        if bin == :binary.copy(<<0>>, byte_size(bin)),
          do: all_zero_from(fd, position + byte_size(bin)),
          else: false

      :eof ->
        true
    end
  end

  # Best effort: the runner allows passwordless sudo, a laptop generally does not. The EXIT STATUS
  # decides, not the existence of the file: a sudo that fails would otherwise leave the caches warm
  # while the report called the samples cold, which is a worse answer than admitting it could not.
  defp can_drop_caches?, do: File.exists?("/proc/sys/vm/drop_caches")

  defp drop_caches do
    case System.cmd("sudo", ["sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"], stderr_to_stdout: true) do
      {_output, 0} -> true
      {_output, _status} -> false
    end
  rescue
    ErlangError -> false
  end

  defp report(label, warm, cold, bail) do
    IO.puts(
      "  #{label}:\n" <>
        "    warm cache      #{ms(median(warm))}   (#{Enum.map_join(warm, ", ", &ms/1)})\n" <>
        "    cold cache      #{if cold == [], do: "unavailable", else: ms(median(cold))}\n" <>
        "    bails at 4KB    #{ms(median(bail))}\n"
    )
  end

  defp median(values) do
    sorted = Enum.sort(values)
    count = length(sorted)
    middle = div(count, 2)

    if rem(count, 2) == 1,
      do: Enum.at(sorted, middle) * 1.0,
      else: (Enum.at(sorted, middle - 1) + Enum.at(sorted, middle)) / 2
  end

  defp ms(us), do: "#{Float.round(us / 1000, 2)}ms"
  defp mb(bytes), do: "#{div(bytes, 1_048_576)}MB"
end

IO.puts("\n========== BLANK TAIL VERIFICATION COST (issue #149) ==========\n")
report = BlankTailScan.run()

case System.get_env("BLANK_SCAN_OUT") do
  nil ->
    IO.puts("\n#{Jason.encode!(report, pretty: true)}")

  out ->
    File.mkdir_p!(Path.dirname(out)) && File.write!(out, Jason.encode_to_iodata!(report, pretty: true)) &&
      IO.puts("\n  wrote #{out}")
end
