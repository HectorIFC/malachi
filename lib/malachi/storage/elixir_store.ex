defmodule Malachi.Storage.ElixirStore do
  @moduledoc """
  Pure-Elixir `Malachi.Storage.SegmentStore` implementation (Phase 0).

  File-per-segment, append-only, with batched writes and an fsync-before-ack durability
  contract. Maintains an in-memory sparse index (`{offset, file_pos}` every
  `:index_interval` bytes) for seeking; the index is persisted to a sidecar on `seal/1`
  and rebuilt by scanning on `recover/3`.

  This is deliberately a plain module operating on an immutable handle (no GenServer),
  so it is deterministic and trivial to property-test. Time-based flushing (NorthGuard's
  ~10ms trigger) and concurrency belong in a higher layer built on top of this.

  Reads via `:file.pread/3` and writes via `:file.pwrite/3` use explicit positions, so
  the single file descriptor serves both append and random read without position races.

  > Recovery currently reads the whole active segment file into memory to scan it. That is
  > fine for the prototype and bounded by the 1GB seal limit, but a production version
  > should scan in chunks. Tracked in `docs/NORTHGUARD_PORT.md`.
  """

  @behaviour Malachi.Storage.SegmentStore

  alias Malachi.Log.{Record, Segment}

  @default_index_interval 4096
  @read_window 262_144

  @type t :: %__MODULE__{
          segment: Segment.t(),
          fd: :file.fd(),
          write_pos: non_neg_integer(),
          next_offset: non_neg_integer(),
          pending: [{non_neg_integer(), iodata(), pos_integer()}],
          pending_bytes: non_neg_integer(),
          pending_count: non_neg_integer(),
          index: [{non_neg_integer(), non_neg_integer()}],
          index_interval: pos_integer(),
          last_index_pos: integer()
        }

  defstruct [
    :segment,
    :fd,
    write_pos: 0,
    next_offset: 0,
    pending: [],
    pending_bytes: 0,
    pending_count: 0,
    index: [],
    index_interval: @default_index_interval,
    last_index_pos: 0
  ]

  @impl true
  def open(dir, seg_id, opts \\ []) do
    File.mkdir_p!(dir)
    segment = Segment.new(seg_id, dir, opts)
    path = Segment.path(segment)

    if File.exists?(path) do
      {:error, :already_exists}
    else
      File.touch!(path)
      {:ok, fd} = :file.open(path, [:read, :write, :raw, :binary])

      {:ok,
       %__MODULE__{
         segment: segment,
         fd: fd,
         next_offset: segment.base_offset,
         index_interval: Keyword.get(opts, :index_interval, @default_index_interval),
         last_index_pos: -Keyword.get(opts, :index_interval, @default_index_interval)
       }}
    end
  end

  @impl true
  def recover(dir, seg_id, opts \\ []) do
    segment = Segment.new(seg_id, dir, opts)
    path = Segment.path(segment)

    if File.exists?(path) do
      bin = File.read!(path)
      {pairs, valid_bytes} = Record.decode_all(bin)
      interval = Keyword.get(opts, :index_interval, @default_index_interval)

      {:ok, fd} = :file.open(path, [:read, :write, :raw, :binary])

      # Drop any partial/corrupt trailing bytes from a crash mid-write.
      if valid_bytes < byte_size(bin) do
        {:ok, _} = :file.position(fd, valid_bytes)
        :ok = :file.truncate(fd)
      end

      record_count = length(pairs)
      base = segment.base_offset
      sealed? = File.exists?(Segment.seal_marker_path(segment))

      segment = %Segment{
        segment
        | state: if(sealed?, do: :sealed, else: :active),
          byte_size: valid_bytes,
          record_count: record_count,
          sealed_at: if(sealed?, do: System.system_time(:millisecond), else: nil)
      }

      index = build_index(pairs, interval)

      {:ok,
       %__MODULE__{
         segment: segment,
         fd: fd,
         write_pos: valid_bytes,
         next_offset: base + record_count,
         index: index,
         index_interval: interval,
         last_index_pos: last_index_pos(index, interval)
       }}
    else
      {:error, :enoent}
    end
  end

  @impl true
  def append(%__MODULE__{segment: %Segment{state: :sealed}}, _records), do: {:error, :sealed}

  def append(%__MODULE__{} = h, records) when is_list(records) and records != [] do
    first = h.next_offset

    {framed, bytes, count, next} =
      Enum.reduce(records, {[], 0, 0, h.next_offset}, fn %Record{} = rec, {acc, b, c, off} ->
        frame = Record.encode(%Record{rec | offset: off})
        size = byte_size(frame)
        {[{off, frame, size} | acc], b + size, c + 1, off + 1}
      end)

    h = %{
      h
      | pending: framed ++ h.pending,
        pending_bytes: h.pending_bytes + bytes,
        pending_count: h.pending_count + count,
        next_offset: next
    }

    {:ok, h, first, next - 1}
  end

  def append(%__MODULE__{} = h, []), do: {:ok, h, h.next_offset, h.next_offset - 1}

  @impl true
  def sync(%__MODULE__{pending_count: 0} = h) do
    :ok = :file.sync(h.fd)
    {:ok, h}
  end

  def sync(%__MODULE__{} = h) do
    frames = Enum.reverse(h.pending)

    # Build new sparse index entries while computing each frame's file position.
    {iodata, new_index_entries, end_pos, last_idx_pos} =
      Enum.reduce(frames, {[], [], h.write_pos, h.last_index_pos}, fn
        {offset, frame, size}, {io, idx, pos, last_idx} ->
          {idx, last_idx} =
            if pos - last_idx >= h.index_interval do
              {[{offset, pos} | idx], pos}
            else
              {idx, last_idx}
            end

          {[frame | io], idx, pos + size, last_idx}
      end)

    :ok = :file.pwrite(h.fd, h.write_pos, Enum.reverse(iodata))
    :ok = :file.sync(h.fd)

    %Segment{} = seg = h.segment

    segment = %Segment{
      seg
      | byte_size: end_pos,
        record_count: seg.record_count + h.pending_count
    }

    {:ok,
     %{
       h
       | segment: segment,
         write_pos: end_pos,
         pending: [],
         pending_bytes: 0,
         pending_count: 0,
         index: h.index ++ Enum.reverse(new_index_entries),
         last_index_pos: last_idx_pos
     }}
  end

  @impl true
  def read(%__MODULE__{segment: segment} = h, offset, max_records)
      when is_integer(offset) and is_integer(max_records) and max_records > 0 do
    committed_end = Segment.end_offset(segment)

    cond do
      offset < segment.base_offset -> {:error, :out_of_range}
      offset >= committed_end -> :eof
      true -> {:ok, do_read(h, offset, max_records)}
    end
  end

  @impl true
  def seal(%__MODULE__{segment: %Segment{state: :sealed}} = h), do: {:ok, h}

  def seal(%__MODULE__{} = h) do
    {:ok, h} = sync(h)
    :ok = persist_index(h)
    File.touch!(Segment.seal_marker_path(h.segment))

    %Segment{} = seg = h.segment
    segment = %Segment{seg | state: :sealed, sealed_at: System.system_time(:millisecond)}
    {:ok, %{h | segment: segment}}
  end

  @impl true
  def next_offset(%__MODULE__{next_offset: n}), do: n

  @impl true
  def close(%__MODULE__{fd: fd}), do: :file.close(fd)

  # --- reading ---

  defp do_read(h, target, max) do
    start_pos = floor_pos(h.index, target)
    collect(h, target, max, start_pos, <<>>, [])
  end

  defp collect(h, target, max, pos, carry, acc) do
    cond do
      length(acc) >= max ->
        Enum.reverse(acc)

      pos >= h.write_pos and carry == <<>> ->
        Enum.reverse(acc)

      true ->
        to_read = min(@read_window, h.write_pos - pos)

        case read_chunk(h.fd, pos, to_read) do
          :eof ->
            Enum.reverse(acc)

          {:ok, data} ->
            buffer = carry <> data
            {pairs, consumed} = Record.decode_all(buffer)
            acc = take_matching(pairs, target, max, acc)
            leftover = binary_part(buffer, consumed, byte_size(buffer) - consumed)
            collect(h, target, max, pos + byte_size(data), leftover, acc)
        end
    end
  end

  # Accumulates (reversed) records with offset >= target, up to `max` total.
  defp take_matching(pairs, target, max, acc) do
    Enum.reduce_while(pairs, acc, fn {rec, _pos}, a ->
      cond do
        length(a) >= max -> {:halt, a}
        rec.offset >= target -> {:cont, [rec | a]}
        true -> {:cont, a}
      end
    end)
  end

  defp read_chunk(_fd, _pos, 0), do: :eof

  defp read_chunk(fd, pos, len) do
    case :file.pread(fd, pos, len) do
      {:ok, data} -> {:ok, data}
      :eof -> :eof
    end
  end

  # --- sparse index ---

  # Greatest indexed file position whose offset is <= target; 0 if none.
  defp floor_pos(index, target) do
    index
    |> Enum.take_while(fn {offset, _pos} -> offset <= target end)
    |> List.last()
    |> case do
      nil -> 0
      {_offset, pos} -> pos
    end
  end

  defp build_index(pairs, interval), do: build_index(pairs, interval, -interval, [])

  defp build_index([], _interval, _last, acc), do: Enum.reverse(acc)

  defp build_index([{rec, pos} | rest], interval, last, acc) do
    if pos - last >= interval do
      build_index(rest, interval, pos, [{rec.offset, pos} | acc])
    else
      build_index(rest, interval, last, acc)
    end
  end

  defp last_index_pos([], interval), do: -interval
  defp last_index_pos(index, _interval), do: index |> List.last() |> elem(1)

  defp persist_index(h) do
    bin = for {offset, pos} <- h.index, into: <<>>, do: <<offset::64, pos::64>>
    File.write(Segment.index_path(h.segment), bin)
  end
end
