defmodule Malachi.Storage.ElixirStore do
  @moduledoc """
  Pure-Elixir `Malachi.Storage.SegmentStore` implementation (Phase 0).

  File-per-segment, append-only, with batched writes and an fsync-before-ack durability
  contract. `append/2` buffers; the buffer is flushed and fsynced on an explicit `sync/1`
  or automatically once it reaches `:flush_bytes` (default 10MB) or `:flush_count` records
  (default 20k): NorthGuard's size and count triggers. Maintains an in-memory sparse index
  (`{offset, file_position}` every `:index_interval` bytes) for seeking, kept in an `:array`
  sorted by offset so a lookup is an O(log n) binary search; the index is persisted to a
  sidecar on `seal/1` and rebuilt by scanning on `recover/3`.

  This is deliberately a plain module operating on an immutable handle (no GenServer), so
  it is deterministic and trivial to property-test. The time-based flush trigger (~10ms)
  and concurrency belong in a higher layer (`Malachi.BrokerServer`) built on top of this.

  Reads via `:file.pread/3` and writes via `:file.pwrite/3` use explicit positions, so
  the single file descriptor serves both append and random read without position races.
  Recovery scans the segment in bounded chunks, so it never loads the whole file at once.
  """

  @behaviour Malachi.Storage.SegmentStore

  alias Malachi.Log.{Record, Segment}

  @default_index_interval 4096
  @read_window_bytes 262_144
  # NorthGuard flushes a batch once it reaches ~10MB or ~20k records.
  @default_flush_bytes 10_485_760
  @default_flush_count 20_000

  @typedoc "One sparse-index entry: a logical offset and the byte position where it starts."
  @type index_entry :: {offset :: non_neg_integer(), file_position :: non_neg_integer()}

  @typedoc "One buffered, not-yet-flushed record: its offset, encoded frame, and frame size."
  @type pending_frame :: {offset :: non_neg_integer(), frame :: iodata(), frame_size :: pos_integer()}

  @type t :: %__MODULE__{
          segment: Segment.t(),
          file_descriptor: :file.fd(),
          write_position: non_neg_integer(),
          next_offset: non_neg_integer(),
          pending: [pending_frame()],
          pending_bytes: non_neg_integer(),
          pending_count: non_neg_integer(),
          # sparse index entries kept sorted by offset in an :array for O(log n) floor lookup
          index: :array.array(),
          index_interval: pos_integer(),
          last_indexed_position: integer(),
          flush_bytes: pos_integer(),
          flush_count: pos_integer(),
          integrity: :ok | integrity_verdict()
        }

  @typedoc """
  What the last scan of this segment concluded: `:ok`, or the first damage it hit, with the byte
  position and how much of the file could not be read. Only `recover/3` scans, so a freshly opened
  or read-only handle is `:ok` by construction.
  """
  @type integrity_verdict :: %{
          reason: atom(),
          position: non_neg_integer(),
          unreadable_bytes: non_neg_integer(),
          sealed?: boolean()
        }

  defstruct [
    :segment,
    :file_descriptor,
    write_position: 0,
    next_offset: 0,
    pending: [],
    pending_bytes: 0,
    pending_count: 0,
    index: nil,
    index_interval: @default_index_interval,
    last_indexed_position: 0,
    flush_bytes: @default_flush_bytes,
    flush_count: @default_flush_count,
    integrity: :ok
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
      {:ok, file_descriptor} = :file.open(path, [:read, :write, :raw, :binary])
      index_interval = Keyword.get(opts, :index_interval, @default_index_interval)

      {:ok,
       %__MODULE__{
         segment: segment,
         file_descriptor: file_descriptor,
         next_offset: segment.base_offset,
         index: empty_index(),
         index_interval: index_interval,
         # Start "behind" by one interval so the segment's first record is always indexed.
         last_indexed_position: -index_interval,
         flush_bytes: Keyword.get(opts, :flush_bytes, @default_flush_bytes),
         flush_count: Keyword.get(opts, :flush_count, @default_flush_count)
       }}
    end
  end

  @impl true
  def recover(directory, segment_id, opts \\ []) do
    segment = Segment.new(segment_id, directory, opts)
    path = Segment.path(segment)

    if File.exists?(path) do
      index_interval = Keyword.get(opts, :index_interval, @default_index_interval)
      {:ok, file_descriptor} = :file.open(path, [:read, :write, :raw, :binary])

      # Scan the file in bounded chunks (never loading it whole), counting records and
      # building the sparse index. `valid_bytes` is where valid frames end.
      {record_count, valid_bytes, index_entries, halt} = scan_segment(file_descriptor, index_interval)

      base_offset = segment.base_offset
      sealed? = File.exists?(Segment.seal_marker_path(segment))
      integrity = integrity_verdict(path, valid_bytes, halt, sealed?)

      if truncate?(integrity) do
        {:ok, _} = :file.position(file_descriptor, valid_bytes)
        :ok = :file.truncate(file_descriptor)
      end

      segment = %Segment{
        segment
        | state: if(sealed?, do: :sealed, else: :active),
          byte_size: valid_bytes,
          record_count: record_count,
          sealed_at: if(sealed?, do: System.system_time(:millisecond), else: nil)
      }

      index = :array.from_list(index_entries)

      {:ok,
       %__MODULE__{
         segment: segment,
         file_descriptor: file_descriptor,
         write_position: valid_bytes,
         next_offset: base_offset + record_count,
         index: index,
         index_interval: index_interval,
         last_indexed_position: last_indexed_position(index, index_interval),
         flush_bytes: Keyword.get(opts, :flush_bytes, @default_flush_bytes),
         flush_count: Keyword.get(opts, :flush_count, @default_flush_count),
         integrity: integrity
       }}
    else
      {:error, :enoent}
    end
  end

  # Whether recovery may drop the bytes past the last valid frame. The rule is about WHAT the damage
  # is, not only about the segment's state, because a replica's file carries a seal marker only when
  # its log rolled locally (by size or age): sealing a segment is a control-plane decision, so a
  # segment the cluster considers immutable usually has no marker on disk, and keying the guard on
  # the marker alone would leave the destructive path wide open in exactly the deployment that
  # matters.
  #
  # An `:incomplete` tail is the crash-mid-write shape: the scan ran out of bytes inside a frame, so
  # by construction nothing valid follows it, and dropping it keeps the file clean and the next
  # append contiguous. A checksum or framing failure is different: the frame was written whole and
  # is wrong (rot, or a bug), and VALID frames may well follow it. Those are recoverable from a peer
  # and must not be destroyed by the very node that noticed the damage. Reads are bounded by
  # `write_position` either way, so leaving the bytes costs nothing but disk.
  defp truncate?(:ok), do: false
  defp truncate?(%{sealed?: true}), do: false
  defp truncate?(%{reason: :incomplete}), do: true
  defp truncate?(_rot), do: false

  # What the recovery scan concluded, for the caller to report (this module never logs). A scan that
  # consumed the file exactly is clean; anything else is described so the warning can name the byte
  # position and how much of the file is unreadable. Those trailing bytes are dropped only when
  # `truncate?/1` allows it; otherwise they stay on disk, unreadable but recoverable.
  defp integrity_verdict(path, valid_bytes, halt, sealed?) do
    unreadable_bytes = File.stat!(path).size - valid_bytes

    case halt do
      :eof when unreadable_bytes == 0 ->
        :ok

      :eof ->
        %{reason: :incomplete, position: valid_bytes, unreadable_bytes: unreadable_bytes, sealed?: sealed?}

      {:error, reason} ->
        %{reason: reason, position: valid_bytes, unreadable_bytes: unreadable_bytes, sealed?: sealed?}
    end
  end

  @impl true
  def verify(directory, segment_id, opts \\ []) do
    segment = Segment.new(segment_id, directory, opts)
    path = Segment.path(segment)

    if File.exists?(path) do
      {:ok, file_descriptor} = :file.open(path, [:read, :raw, :binary])

      try do
        {record_count, valid_bytes, halt} = check_scan(file_descriptor)
        verdict(path, record_count, valid_bytes, halt)
      after
        :file.close(file_descriptor)
      end
    else
      {:error, :enoent}
    end
  end

  # A clean scan must consume the file EXACTLY: `halt == :eof` with valid frames ending short of the
  # file size means the tail is a partial frame, which for a sealed segment is damage just like a
  # checksum mismatch (an active segment's torn tail is normal and is handled by `recover/3`).
  # The damage map carries the same keys `recover/3` reports, so a caller (and the telemetry event)
  # handles findings from either path identically.
  defp verdict(path, record_count, valid_bytes, halt) do
    file_size = File.stat!(path).size
    unreadable = file_size - valid_bytes

    case halt do
      :eof when unreadable == 0 ->
        {:ok, %{records: record_count, bytes: valid_bytes}}

      :eof ->
        {:error, %{position: valid_bytes, reason: :incomplete, unreadable_bytes: unreadable, file: path}}

      {:error, reason} ->
        {:error, %{position: valid_bytes, reason: reason, unreadable_bytes: unreadable, file: path}}
    end
  end

  # The verification scan. Deliberately separate from `do_scan/7`: this one uses
  # `Malachi.Log.Record.check_one/1`, which verifies the checksum without building a record struct,
  # and it keeps no sparse index. A scrub walks whole segments just to confirm their checksums, so
  # the per-record allocation `do_scan/7` needs for recovery would dominate its cost.
  defp check_scan(file_descriptor), do: do_check(file_descriptor, 0, <<>>, 0)

  defp do_check(file_descriptor, valid_bytes, carry, record_count) do
    case Record.check_one(carry) do
      {:ok, frame_size, rest} ->
        do_check(file_descriptor, valid_bytes + frame_size, rest, record_count + 1)

      :incomplete ->
        case :file.pread(file_descriptor, valid_bytes + byte_size(carry), @read_window_bytes) do
          {:ok, chunk} -> do_check(file_descriptor, valid_bytes, carry <> chunk, record_count)
          :eof -> {record_count, valid_bytes, :eof}
        end

      {:error, reason} ->
        {record_count, valid_bytes, {:error, reason}}
    end
  end

  @impl true
  def open_read(directory, segment_id, opts) do
    segment = Segment.new(segment_id, directory, opts)
    path = Segment.path(segment)

    if File.exists?(path) do
      record_count = Keyword.fetch!(opts, :record_count)
      index_interval = Keyword.get(opts, :index_interval, @default_index_interval)
      {:ok, file_descriptor} = :file.open(path, [:read, :raw, :binary])
      file_size = File.stat!(path).size
      index = load_index_file(Segment.index_path(segment))

      segment = %Segment{segment | state: :sealed, byte_size: file_size, record_count: record_count}

      {:ok,
       %__MODULE__{
         segment: segment,
         file_descriptor: file_descriptor,
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

  # NorthGuard's size and count triggers: once the buffer reaches `:flush_bytes` or
  # `:flush_count` records, flush+fsync it automatically without waiting for `sync/1`.
  defp flush_if_full(
         %__MODULE__{
           pending_bytes: pending_bytes,
           pending_count: pending_count,
           flush_bytes: flush_bytes,
           flush_count: flush_count
         } = store,
         first_offset,
         last_offset
       )
       when pending_bytes >= flush_bytes or pending_count >= flush_count do
    {:ok, flushed_store} = sync(store)
    {:ok, flushed_store, first_offset, last_offset}
  end

  defp flush_if_full(%__MODULE__{} = store, first_offset, last_offset),
    do: {:ok, store, first_offset, last_offset}

  @impl true
  def sync(%__MODULE__{pending_count: 0} = store) do
    :ok = :file.sync(store.file_descriptor)
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

    :ok = :file.pwrite(store.file_descriptor, store.write_position, Enum.reverse(frames_iodata))
    :ok = :file.sync(store.file_descriptor)

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
         index: append_index_entries(store.index, Enum.reverse(new_index_entries)),
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
  def integrity(%__MODULE__{integrity: integrity}), do: integrity

  @impl true
  def pending?(%__MODULE__{pending_count: pending_count}), do: pending_count > 0

  @impl true
  def should_seal?(%__MODULE__{segment: segment}, now_ms), do: Segment.should_seal?(segment, now_ms)

  @impl true
  def close(%__MODULE__{file_descriptor: file_descriptor}), do: :file.close(file_descriptor)

  # --- reading ---

  defp do_read(store, target_offset, max_records) do
    start_position = floor_position(store.index, target_offset)
    collect(store, target_offset, max_records, start_position, <<>>, [])
  end

  # `remaining_records` is a decreasing counter so we never call `length/1` per record
  # (which would make a large read O(max_records^2)).
  defp collect(store, target_offset, remaining_records, position, leftover_bytes, collected) do
    cond do
      remaining_records <= 0 ->
        Enum.reverse(collected)

      position >= store.write_position and leftover_bytes == <<>> ->
        Enum.reverse(collected)

      true ->
        bytes_to_read = min(@read_window_bytes, store.write_position - position)

        case read_chunk(store.file_descriptor, position, bytes_to_read) do
          :eof ->
            Enum.reverse(collected)

          {:ok, chunk} ->
            buffer = leftover_bytes <> chunk
            {records_with_positions, consumed_bytes} = Record.decode_all(buffer)

            {collected, remaining_records} =
              take_matching(records_with_positions, target_offset, remaining_records, collected)

            unconsumed_bytes = binary_part(buffer, consumed_bytes, byte_size(buffer) - consumed_bytes)

            collect(store, target_offset, remaining_records, position + byte_size(chunk), unconsumed_bytes, collected)
        end
    end
  end

  # Prepends records with offset >= target_offset to `collected`, decrementing the
  # remaining budget; stops once it reaches zero.
  defp take_matching(records_with_positions, target_offset, remaining_records, collected) do
    Enum.reduce_while(records_with_positions, {collected, remaining_records}, fn
      {record, _position}, {collected, remaining_records} ->
        cond do
          remaining_records <= 0 -> {:halt, {collected, remaining_records}}
          record.offset >= target_offset -> {:cont, {[record | collected], remaining_records - 1}}
          true -> {:cont, {collected, remaining_records}}
        end
    end)
  end

  defp read_chunk(_file_descriptor, _position, 0), do: :eof

  defp read_chunk(file_descriptor, position, length) do
    case :file.pread(file_descriptor, position, length) do
      {:ok, chunk} -> {:ok, chunk}
      :eof -> :eof
    end
  end

  # --- sparse index (an :array sorted by offset, for O(log n) floor lookups) ---

  # File position of the greatest indexed offset <= target_offset; 0 if none.
  defp floor_position(index, target_offset) do
    case :array.size(index) do
      0 -> 0
      size -> binary_floor(index, target_offset, 0, size - 1, 0)
    end
  end

  defp binary_floor(_index, _target_offset, low, high, best_position) when low > high,
    do: best_position

  defp binary_floor(index, target_offset, low, high, best_position) do
    middle = div(low + high, 2)
    {offset, position} = :array.get(middle, index)

    if offset <= target_offset do
      binary_floor(index, target_offset, middle + 1, high, position)
    else
      binary_floor(index, target_offset, low, middle - 1, best_position)
    end
  end

  # Appends already-ascending entries to the index array (each insert is O(log n)).
  defp append_index_entries(index, entries) do
    Enum.reduce(entries, index, fn entry, accumulated ->
      :array.set(:array.size(accumulated), entry, accumulated)
    end)
  end

  # Scans a segment file in bounded chunks, returning {record_count, valid_bytes, entries, halt}
  # where `entries` is the ascending sparse index, `valid_bytes` is the end of the last valid frame
  # (the safe truncation point after a crash), and `halt` says WHY the scan stopped: `:eof` (clean
  # end of file) or `{:error, reason}` from `Malachi.Log.Record`. Recovery ignores `halt` and simply
  # truncates; verification needs it to tell a torn tail from bit rot, and to report the offending
  # byte position. Never loads the whole file.
  defp scan_segment(file_descriptor, index_interval) do
    do_scan(file_descriptor, index_interval, 0, <<>>, 0, -index_interval, [])
  end

  defp do_scan(file_descriptor, index_interval, valid_bytes, carry, record_count, last_indexed_position, entries) do
    case Record.decode_one(carry) do
      {:ok, record, frame_size, rest} ->
        {entries, last_indexed_position} =
          if valid_bytes - last_indexed_position >= index_interval do
            {[{record.offset, valid_bytes} | entries], valid_bytes}
          else
            {entries, last_indexed_position}
          end

        do_scan(
          file_descriptor,
          index_interval,
          valid_bytes + frame_size,
          rest,
          record_count + 1,
          last_indexed_position,
          entries
        )

      :incomplete ->
        read_position = valid_bytes + byte_size(carry)

        case :file.pread(file_descriptor, read_position, @read_window_bytes) do
          {:ok, chunk} ->
            do_scan(
              file_descriptor,
              index_interval,
              valid_bytes,
              carry <> chunk,
              record_count,
              last_indexed_position,
              entries
            )

          :eof ->
            {record_count, valid_bytes, Enum.reverse(entries), :eof}
        end

      {:error, reason} ->
        {record_count, valid_bytes, Enum.reverse(entries), {:error, reason}}
    end
  end

  defp last_indexed_position(index, index_interval) do
    case :array.size(index) do
      0 -> -index_interval
      size -> elem(:array.get(size - 1, index), 1)
    end
  end

  defp persist_index(store) do
    binary =
      for {offset, position} <- :array.to_list(store.index), into: <<>> do
        <<offset::64, position::64>>
      end

    File.write(Segment.index_path(store.segment), binary)
  end

  defp load_index_file(path) do
    case File.read(path) do
      {:ok, binary} -> :array.from_list(parse_index(binary, []))
      _ -> empty_index()
    end
  end

  defp empty_index, do: :array.new([])

  defp parse_index(<<offset::64, position::64, remaining::binary>>, entries),
    do: parse_index(remaining, [{offset, position} | entries])

  defp parse_index(_remaining, entries), do: Enum.reverse(entries)
end
