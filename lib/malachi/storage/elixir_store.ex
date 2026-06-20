defmodule Malachi.Storage.ElixirStore do
  @moduledoc """
  Pure-Elixir `Malachi.Storage.SegmentStore` implementation (Phase 0).

  File-per-segment, append-only, with batched writes and an fsync-before-ack durability
  contract. `append/2` buffers; the buffer is flushed and fsynced either on an explicit
  `sync/1` or automatically once it reaches `:flush_bytes` (NorthGuard's size trigger,
  default 10MB). Maintains an in-memory sparse index (`{offset, file_position}` every
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
  @read_window_bytes 262_144
  # NorthGuard flushes a batch once it reaches ~10MB; matched here as the size trigger.
  @default_flush_bytes 10_485_760

  @typedoc "One sparse-index entry: a logical offset and the byte position where it starts."
  @type index_entry :: {offset :: non_neg_integer(), file_position :: non_neg_integer()}

  @typedoc "One buffered, not-yet-flushed record: its offset, encoded frame, and frame size."
  @type pending_frame :: {offset :: non_neg_integer(), frame :: iodata(), frame_size :: pos_integer()}

  @type t :: %__MODULE__{
          segment: Segment.t(),
          fd: :file.fd(),
          write_position: non_neg_integer(),
          next_offset: non_neg_integer(),
          pending: [pending_frame()],
          pending_bytes: non_neg_integer(),
          pending_count: non_neg_integer(),
          index: [index_entry()],
          index_interval: pos_integer(),
          last_indexed_position: integer(),
          flush_bytes: pos_integer()
        }

  defstruct [
    :segment,
    :fd,
    write_position: 0,
    next_offset: 0,
    pending: [],
    pending_bytes: 0,
    pending_count: 0,
    index: [],
    index_interval: @default_index_interval,
    last_indexed_position: 0,
    flush_bytes: @default_flush_bytes
  ]

  @impl true
  def open(directory, segment_id, opts \\ []) do
    File.mkdir_p!(directory)
    segment = Segment.new(segment_id, directory, opts)
    path = Segment.path(segment)

    if File.exists?(path) do
      {:error, :already_exists}
    else
      File.touch!(path)
      {:ok, fd} = :file.open(path, [:read, :write, :raw, :binary])
      index_interval = Keyword.get(opts, :index_interval, @default_index_interval)

      {:ok,
       %__MODULE__{
         segment: segment,
         fd: fd,
         next_offset: segment.base_offset,
         index_interval: index_interval,
         # Start "behind" by one interval so the segment's first record is always indexed.
         last_indexed_position: -index_interval,
         flush_bytes: Keyword.get(opts, :flush_bytes, @default_flush_bytes)
       }}
    end
  end

  @impl true
  def recover(directory, segment_id, opts \\ []) do
    segment = Segment.new(segment_id, directory, opts)
    path = Segment.path(segment)

    if File.exists?(path) do
      file_contents = File.read!(path)
      {records_with_positions, valid_bytes} = Record.decode_all(file_contents)
      index_interval = Keyword.get(opts, :index_interval, @default_index_interval)

      {:ok, fd} = :file.open(path, [:read, :write, :raw, :binary])

      # Drop any partial/corrupt trailing bytes from a crash mid-write.
      if valid_bytes < byte_size(file_contents) do
        {:ok, _} = :file.position(fd, valid_bytes)
        :ok = :file.truncate(fd)
      end

      record_count = length(records_with_positions)
      base_offset = segment.base_offset
      sealed? = File.exists?(Segment.seal_marker_path(segment))

      segment = %Segment{
        segment
        | state: if(sealed?, do: :sealed, else: :active),
          byte_size: valid_bytes,
          record_count: record_count,
          sealed_at: if(sealed?, do: System.system_time(:millisecond), else: nil)
      }

      index = build_index(records_with_positions, index_interval)

      {:ok,
       %__MODULE__{
         segment: segment,
         fd: fd,
         write_position: valid_bytes,
         next_offset: base_offset + record_count,
         index: index,
         index_interval: index_interval,
         last_indexed_position: last_indexed_position(index, index_interval),
         flush_bytes: Keyword.get(opts, :flush_bytes, @default_flush_bytes)
       }}
    else
      {:error, :enoent}
    end
  end

  @impl true
  def open_read(directory, segment_id, opts) do
    segment = Segment.new(segment_id, directory, opts)
    path = Segment.path(segment)

    if File.exists?(path) do
      record_count = Keyword.fetch!(opts, :record_count)
      index_interval = Keyword.get(opts, :index_interval, @default_index_interval)
      {:ok, fd} = :file.open(path, [:read, :raw, :binary])
      file_size = File.stat!(path).size
      index = load_index_file(Segment.index_path(segment))

      segment = %Segment{segment | state: :sealed, byte_size: file_size, record_count: record_count}

      {:ok,
       %__MODULE__{
         segment: segment,
         fd: fd,
         write_position: file_size,
         next_offset: segment.base_offset + record_count,
         index: index,
         index_interval: index_interval,
         last_indexed_position: last_indexed_position(index, index_interval)
       }}
    else
      {:error, :enoent}
    end
  end

  @impl true
  def append(%__MODULE__{segment: %Segment{state: :sealed}}, _records), do: {:error, :sealed}

  def append(%__MODULE__{} = store, records) when is_list(records) and records != [] do
    first_offset = store.next_offset

    {framed_records, batch_bytes, batch_count, next_offset} =
      Enum.reduce(records, {[], 0, 0, store.next_offset}, fn
        %Record{} = record, {frames, bytes_so_far, count_so_far, offset} ->
          frame = Record.encode(%Record{record | offset: offset})
          frame_size = byte_size(frame)

          {[{offset, frame, frame_size} | frames], bytes_so_far + frame_size, count_so_far + 1, offset + 1}
      end)

    store = %{
      store
      | pending: framed_records ++ store.pending,
        pending_bytes: store.pending_bytes + batch_bytes,
        pending_count: store.pending_count + batch_count,
        next_offset: next_offset
    }

    flush_if_full(store, first_offset, next_offset - 1)
  end

  def append(%__MODULE__{} = store, []), do: {:ok, store, store.next_offset, store.next_offset - 1}

  # NorthGuard's size trigger: once the buffer reaches `:flush_bytes`, flush+fsync it
  # automatically so the batch is committed without waiting for an explicit `sync/1`.
  defp flush_if_full(
         %__MODULE__{pending_bytes: pending_bytes, flush_bytes: flush_bytes} = store,
         first_offset,
         last_offset
       )
       when pending_bytes >= flush_bytes do
    {:ok, flushed_store} = sync(store)
    {:ok, flushed_store, first_offset, last_offset}
  end

  defp flush_if_full(%__MODULE__{} = store, first_offset, last_offset),
    do: {:ok, store, first_offset, last_offset}

  @impl true
  def sync(%__MODULE__{pending_count: 0} = store) do
    :ok = :file.sync(store.fd)
    {:ok, store}
  end

  def sync(%__MODULE__{} = store) do
    frames_in_order = Enum.reverse(store.pending)

    # Write the frames and, in the same pass, compute each frame's file position so we can
    # add a sparse-index entry roughly every `index_interval` bytes.
    {frames_iodata, new_index_entries, end_position, last_indexed_position} =
      Enum.reduce(frames_in_order, {[], [], store.write_position, store.last_indexed_position}, fn
        {offset, frame, frame_size}, {iodata, index_entries, position, last_indexed_position} ->
          {index_entries, last_indexed_position} =
            if position - last_indexed_position >= store.index_interval do
              {[{offset, position} | index_entries], position}
            else
              {index_entries, last_indexed_position}
            end

          {[frame | iodata], index_entries, position + frame_size, last_indexed_position}
      end)

    :ok = :file.pwrite(store.fd, store.write_position, Enum.reverse(frames_iodata))
    :ok = :file.sync(store.fd)

    %Segment{} = current_segment = store.segment

    segment = %Segment{
      current_segment
      | byte_size: end_position,
        record_count: current_segment.record_count + store.pending_count
    }

    {:ok,
     %{
       store
       | segment: segment,
         write_position: end_position,
         pending: [],
         pending_bytes: 0,
         pending_count: 0,
         index: store.index ++ Enum.reverse(new_index_entries),
         last_indexed_position: last_indexed_position
     }}
  end

  @impl true
  def read(%__MODULE__{segment: segment} = store, offset, max_records)
      when is_integer(offset) and is_integer(max_records) and max_records > 0 do
    committed_end_offset = Segment.end_offset(segment)

    cond do
      offset < segment.base_offset -> {:error, :out_of_range}
      offset >= committed_end_offset -> :eof
      true -> {:ok, do_read(store, offset, max_records)}
    end
  end

  @impl true
  def seal(%__MODULE__{segment: %Segment{state: :sealed}} = store), do: {:ok, store}

  def seal(%__MODULE__{} = store) do
    {:ok, store} = sync(store)
    :ok = persist_index(store)
    File.touch!(Segment.seal_marker_path(store.segment))

    %Segment{} = current_segment = store.segment
    segment = %Segment{current_segment | state: :sealed, sealed_at: System.system_time(:millisecond)}
    {:ok, %{store | segment: segment}}
  end

  @impl true
  def next_offset(%__MODULE__{next_offset: next_offset}), do: next_offset

  @impl true
  def sealed?(%__MODULE__{segment: segment}), do: Segment.sealed?(segment)

  @impl true
  def should_seal?(%__MODULE__{segment: segment}, now_ms), do: Segment.should_seal?(segment, now_ms)

  @impl true
  def close(%__MODULE__{fd: fd}), do: :file.close(fd)

  # --- reading ---

  defp do_read(store, target_offset, max_records) do
    start_position = floor_position(store.index, target_offset)
    collect(store, target_offset, max_records, start_position, <<>>, [])
  end

  defp collect(store, target_offset, max_records, position, leftover_bytes, collected) do
    cond do
      length(collected) >= max_records ->
        Enum.reverse(collected)

      position >= store.write_position and leftover_bytes == <<>> ->
        Enum.reverse(collected)

      true ->
        bytes_to_read = min(@read_window_bytes, store.write_position - position)

        case read_chunk(store.fd, position, bytes_to_read) do
          :eof ->
            Enum.reverse(collected)

          {:ok, chunk} ->
            buffer = leftover_bytes <> chunk
            {records_with_positions, consumed_bytes} = Record.decode_all(buffer)
            collected = take_matching(records_with_positions, target_offset, max_records, collected)
            remaining_bytes = binary_part(buffer, consumed_bytes, byte_size(buffer) - consumed_bytes)
            collect(store, target_offset, max_records, position + byte_size(chunk), remaining_bytes, collected)
        end
    end
  end

  # Accumulates (reversed) records with offset >= target_offset, up to `max_records` total.
  defp take_matching(records_with_positions, target_offset, max_records, collected) do
    Enum.reduce_while(records_with_positions, collected, fn {record, _position}, collected ->
      cond do
        length(collected) >= max_records -> {:halt, collected}
        record.offset >= target_offset -> {:cont, [record | collected]}
        true -> {:cont, collected}
      end
    end)
  end

  defp read_chunk(_fd, _position, 0), do: :eof

  defp read_chunk(fd, position, length) do
    case :file.pread(fd, position, length) do
      {:ok, chunk} -> {:ok, chunk}
      :eof -> :eof
    end
  end

  # --- sparse index ---

  # Greatest indexed file position whose offset is <= target_offset; 0 if none.
  defp floor_position(index, target_offset) do
    index
    |> Enum.take_while(fn {offset, _position} -> offset <= target_offset end)
    |> List.last()
    |> case do
      nil -> 0
      {_offset, position} -> position
    end
  end

  defp build_index(records_with_positions, index_interval) do
    build_index(records_with_positions, index_interval, -index_interval, [])
  end

  defp build_index([], _index_interval, _last_indexed_position, entries), do: Enum.reverse(entries)

  defp build_index([{record, position} | remaining], index_interval, last_indexed_position, entries) do
    if position - last_indexed_position >= index_interval do
      build_index(remaining, index_interval, position, [{record.offset, position} | entries])
    else
      build_index(remaining, index_interval, last_indexed_position, entries)
    end
  end

  defp last_indexed_position([], index_interval), do: -index_interval
  defp last_indexed_position(index, _index_interval), do: index |> List.last() |> elem(1)

  defp persist_index(store) do
    binary = for {offset, position} <- store.index, into: <<>>, do: <<offset::64, position::64>>
    File.write(Segment.index_path(store.segment), binary)
  end

  defp load_index_file(path) do
    case File.read(path) do
      {:ok, binary} -> parse_index(binary, [])
      _ -> []
    end
  end

  defp parse_index(<<offset::64, position::64, remaining::binary>>, entries),
    do: parse_index(remaining, [{offset, position} | entries])

  defp parse_index(_remaining, entries), do: Enum.reverse(entries)
end
