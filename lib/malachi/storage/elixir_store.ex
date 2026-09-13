defmodule Malachi.Storage.ElixirStore do
  @moduledoc """
  Pure-Elixir `Malachi.Storage.SegmentStore` implementation.

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

  ## Preallocation

  With `:prealloc_bytes` set, a new segment is sized to that many bytes at creation
  (`Malachi.Storage.Preallocation`) so that appends overwrite an already-allocated region instead
  of extending the file. Measured on an `ubuntu-latest` runner in the regime the pinned ceiling
  harness runs (batch 10 x 256B, one sync per produce), that takes the per-flush p50 from 317us to
  96us, a 70% cut, against a measured noise floor of 2us. The reason is that a growing file changes
  its size on every append, and a size change is metadata the following sync has to journal; a
  1-byte sync costs 257us on a growing file and 75us on a sized one.

  It is a trade rather than a free win: it also gives up the filesystem's delayed allocation, which
  costs more than the journal saves once a flush is large. The crossover is around 128KB per flush
  and `Malachi.Storage.Preallocation` carries the measured curve.

  It costs one full-size write at creation (36ms for 64MB on that runner) and it changes what the
  unwritten tail looks like, which recovery has to understand: see `classify_tail/2`. The tail is
  truncated away on `seal/1` and on `close/1`, so a segment this store is not actively writing is
  byte-exact, and every size the rest of the system reads off disk keeps meaning what it meant.

  ## Failures

  Every file operation's error comes back as `{:error, posix}`, per the failure contract in
  `Malachi.Storage.SegmentStore`. The one deliberate exception is preallocation: it is an optimization,
  so a failure there degrades to an unpreallocated segment rather than failing the open or recovery,
  and the next commit reports the condition if it persists.
  """

  @behaviour Malachi.Storage.SegmentStore

  alias Malachi.Log.{Record, Segment}
  alias Malachi.Storage.Preallocation

  @default_index_interval 4096
  @read_window_bytes 262_144
  # One persisted sparse-index entry: `<<offset::64, position::64>>` (see persist_index/1).
  @index_entry_bytes 16
  # NorthGuard flushes a batch once it reaches ~10MB or ~20k records.
  @default_flush_bytes 10_485_760
  @default_flush_count 20_000
  # Off unless asked for. A store is a library and `open/3` writing tens of megabytes by default
  # would be hostile; `Malachi.Application` is what turns it on for a real deployment.
  @default_prealloc_bytes 0
  # How far past the last valid frame recovery looks to tell a torn write from rot. Bounded on
  # purpose: the alternative is scanning the whole preallocated region on every damaged recovery,
  # and what a bounded window cannot see (isolated garbage stranded in the middle of the unwritten
  # tail) is not a shape any crash produces.
  @tail_window_bytes 65_536
  # How far a zero frame header has to stay zero before the scan believes it is unwritten space
  # rather than damage. A zeroed region with valid frames behind it is a real disk failure (a
  # remapped sector, a firmware bug, a power loss with a lying cache), and taking the header at face
  # value would have recovery report the segment healthy while dropping every frame past the hole.
  #
  # Nothing is taken on faith here: a zero frame header is believed only once every byte after it has
  # been read and found zero. That costs 8.2 ms per MB of tail, measured on the runner, and a blank
  # tail exists only on a segment being written, so a restart pays it once per range.
  #
  # A bounded check was tried first and does not work, which is worth recording because it looks like
  # it should. Sampling the tail catches only NON-ZERO bytes that land on a sample, and a frame
  # carries very few of them: a 3MB record whose value is zeros has about thirty, in its header. Put
  # a hole in front of it so that header falls between two samples and the record vanishes with the
  # segment reporting itself healthy. Reproduced at a 4KB sample every 64KB. The blind spot is not in
  # the interval, it is in what the samples can see, so no interval fixes it.
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
          prealloc_bytes: non_neg_integer(),
          preallocated_to: non_neg_integer() | nil,
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
    prealloc_bytes: @default_prealloc_bytes,
    # How far this handle preallocated, or `nil` when it did not. It is what authorizes trimming
    # the tail on `seal/1` and `close/1`, and it is deliberately a field rather than a test on
    # `prealloc_bytes`: a read-only handle from `open_read/3` must never be able to truncate a
    # sealed segment, whatever options it was passed.
    preallocated_to: nil,
    integrity: :ok
  ]

  @impl true
  def open(directory, segment_id, opts \\ []) do
    segment = Segment.new(segment_id, directory, opts)
    path = Segment.path(segment)

    # A failure between creating the file and opening it leaves an empty `.log` behind, on purpose:
    # removing a file from a volume that just failed can fail too, and the leftover is harmless. `open/3`
    # answers `:already_exists` for it, and `recover/3`, which is what reopens a segment after a restart,
    # reads an empty file as a clean one.
    with :ok <- File.mkdir_p(directory),
         :ok <- absent(path),
         :ok <- File.touch(path),
         {:ok, file_descriptor} <- :file.open(path, [:read, :write, :raw, :binary]) do
      index_interval = Keyword.get(opts, :index_interval, @default_index_interval)
      prealloc_bytes = Keyword.get(opts, :prealloc_bytes, @default_prealloc_bytes)

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
         flush_count: Keyword.get(opts, :flush_count, @default_flush_count),
         prealloc_bytes: prealloc_bytes,
         preallocated_to: preallocate(file_descriptor, prealloc_bytes)
       }}
    end
  end

  defp absent(path), do: if(File.exists?(path), do: {:error, :already_exists}, else: :ok)

  @impl true
  def recover(directory, segment_id, opts \\ []) do
    segment = Segment.new(segment_id, directory, opts)
    path = Segment.path(segment)

    if File.exists?(path) do
      with {:ok, file_descriptor} <- :file.open(path, [:read, :write, :raw, :binary]) do
        # A recovery that fails hands back no handle, so nothing else could ever close this descriptor.
        case recover_from(segment, path, file_descriptor, opts) do
          {:ok, _store} = recovered ->
            recovered

          {:error, _reason} = error ->
            _ = :file.close(file_descriptor)
            error
        end
      end
    else
      {:error, :enoent}
    end
  end

  defp recover_from(%Segment{} = segment, path, file_descriptor, opts) do
    index_interval = Keyword.get(opts, :index_interval, @default_index_interval)
    prealloc_bytes = Keyword.get(opts, :prealloc_bytes, @default_prealloc_bytes)

    # Scan the file in bounded chunks (never loading it whole), counting records and
    # building the sparse index. `valid_bytes` is where valid frames end.
    {record_count, valid_bytes, index_entries, halt} = scan_segment(file_descriptor, index_interval)
    sealed? = File.exists?(Segment.seal_marker_path(segment))

    with {:ok, %{size: file_size}} <- File.stat(path),
         shape = tail_shape(file_descriptor, valid_bytes, file_size),
         classification = classify_tail(halt, shape),
         action = action_for(classification, sealed?),
         :ok <- apply_tail_action(action, file_descriptor, valid_bytes, shape, prealloc_bytes) do
      integrity = integrity_verdict(verdict_key(classification, halt), valid_bytes, shape, sealed?, prealloc_bytes > 0)

      # `:preserve` means this handle does not touch the file, and that has to include preallocating
      # it. Not because extending is destructive by itself (it starts from the file's size and only
      # ever adds room), but because `preallocated_to` is what authorizes `seal/1` and `close/1` to
      # trim back to `write_position`, and `write_position` is `valid_bytes`: the byte the damage
      # STARTS at. Setting it here would have closing the handle delete the damaged frame and every
      # valid frame behind it, which is the same loss the extension itself was just fixed for, one
      # door further along.
      preallocated_to =
        if sealed? or action == :preserve, do: nil, else: preallocate(file_descriptor, prealloc_bytes)

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
         next_offset: segment.base_offset + record_count,
         index: index,
         index_interval: index_interval,
         last_indexed_position: last_indexed_position(index, index_interval),
         flush_bytes: Keyword.get(opts, :flush_bytes, @default_flush_bytes),
         flush_count: Keyword.get(opts, :flush_count, @default_flush_count),
         prealloc_bytes: prealloc_bytes,
         preallocated_to: preallocated_to,
         integrity: integrity
       }}
    end
  end

  defp apply_tail_action(:discard_tail, file_descriptor, valid_bytes, shape, prealloc_bytes),
    do: discard_tail(file_descriptor, valid_bytes, shape, prealloc_bytes)

  defp apply_tail_action(_none_or_preserve, _file_descriptor, _valid_bytes, _shape, _prealloc_bytes), do: :ok

  @typedoc """
  What the bytes past the last valid frame are.

    * `:clean` - there are none: valid frames consumed the file exactly.
    * `:blank` - unwritten preallocated space. Not damage, and not a shape a growing segment can
      produce, since it has no room past its last write.
    * `:torn` - a write that did not finish. Either the file ended inside a frame (the only shape a
      growing segment can make) or, in a preallocated segment, a frame whose bytes stop partway and
      give out into the unwritten zeros behind it. Nothing valid follows it by construction.
    * `:rot` - a frame that is there and is wrong. Valid frames may well follow it, so these bytes
      are a peer's problem to fix and never this node's to discard.
  """
  @type tail_classification :: :clean | :blank | :torn | :rot

  @doc """
  Classifies the bytes past the last valid frame from the scan's `halt` and the shape of the tail.

  Pure, and separate from what recovery then DOES about it (`action_for/2`), because the two answer
  different questions and only one of them depends on whether the segment is sealed.

  Telling `:torn` from `:rot` is the whole difficulty, and preallocation is what makes it hard.
  While a segment grows, "the file ended inside this frame" is proof the writer died there, because
  there is nowhere else for bytes to be. A preallocated segment has no such end: a torn write leaves
  a frame header intact and its payload completed by the zeros that were already there, so it comes
  back as `:bad_crc`, exactly like rot.

  What still separates them is where the zeros start. A torn write stops partway through the frame
  and the unwritten region takes over from there, so the zeros begin INSIDE the frame the header
  describes. Rot flips bytes in a frame that was written whole, so the zeros (if any) begin at or
  after its end. That is the test, and it is bounded: it looks at one window past the damage rather
  than scanning the rest of the segment.

  It has one known way to be wrong, and it is deliberate: a record whose value legitimately ends in
  NUL bytes, sitting in the last frame of a segment, rotted, would be read as torn and dropped
  rather than reported. It is pinned by a test that names it. The trade is that the alternative,
  calling every interrupted flush corruption, would have every unclean restart report damage.
  """
  @spec classify_tail(:eof | :blank | {:error, atom()}, map()) :: tail_classification()
  def classify_tail(:eof, %{trailing_bytes: 0}), do: :clean
  def classify_tail(:eof, _shape), do: :torn
  def classify_tail(:blank, _shape), do: :blank

  def classify_tail({:error, _reason}, shape) do
    if shape.zeros_start_inside_frame? and shape.zeros_reach_the_end?, do: :torn, else: :rot
  end

  @doc """
  What recovery may do about a classified tail: nothing, discard it, or leave it alone and report.

  The state matters here and not in the classification, because it is about permission rather than
  about what the bytes are. The rule keys on the DAMAGE and not only on the seal marker, because a
  replica's file carries a marker only when its log rolled locally (by size or age): sealing a
  segment is a control-plane decision, so a segment the cluster considers immutable usually has no
  marker on disk, and keying the guard on the marker alone would leave the destructive path wide
  open in exactly the deployment that matters.
  """
  @spec action_for(tail_classification(), boolean()) :: :none | :discard_tail | :preserve
  def action_for(:clean, _sealed?), do: :none
  def action_for(:blank, _sealed?), do: :none
  def action_for(:torn, false), do: :discard_tail
  def action_for(:torn, true), do: :preserve
  def action_for(:rot, _sealed?), do: :preserve

  # What the recovery scan concluded, for the caller to report (this module never logs). A clean or
  # blank tail is `:ok`; anything else is described so the warning can name the byte position and
  # how much of the file is unreadable.
  #
  # `unreadable_bytes` counts the bytes past the last valid frame that are not preallocated space.
  # Without preallocation that is all of them, exactly as before, and it stays all of them even when
  # some happen to be zeros: a torn frame full of NULs is still a torn frame, and the number is
  # meant to say what recovery dropped. With preallocation the blank tail comes off, because
  # reporting a fresh 64MB segment as having 64MB unreadable would make the number meaningless.
  defp integrity_verdict(:clean, _valid_bytes, _shape, _sealed?, _preallocated?), do: :ok
  defp integrity_verdict(:blank, _valid_bytes, _shape, _sealed?, _preallocated?), do: :ok

  # `:incomplete` whichever way the scan hit it: the file ending inside a frame and a frame giving
  # out into unwritten space are the same event, a write that did not finish, and callers that
  # already handle the first must not have to learn a second name for it.
  defp integrity_verdict(:torn, valid_bytes, shape, sealed?, preallocated?) do
    verdict_map(:incomplete, valid_bytes, shape, sealed?, preallocated?)
  end

  defp integrity_verdict({:rot, reason}, valid_bytes, shape, sealed?, preallocated?) do
    verdict_map(reason, valid_bytes, shape, sealed?, preallocated?)
  end

  defp verdict_map(reason, valid_bytes, shape, sealed?, preallocated?) do
    %{
      reason: reason,
      position: valid_bytes,
      unreadable_bytes: unreadable_bytes(shape, preallocated?),
      sealed?: sealed?
    }
  end

  defp unreadable_bytes(shape, true), do: shape.trailing_bytes - shape.blank_tail_bytes
  defp unreadable_bytes(shape, false), do: shape.trailing_bytes

  # Reads one bounded window past the last valid frame and describes it, which is the only part of
  # the torn-versus-rot decision that touches the disk.
  #
  #   * `trailing_bytes` - how much of the file is past the last valid frame at all.
  #   * `written_bytes` - the leading run of the tail that is not zeros, so the extent of what a
  #     torn write actually left behind.
  #   * `blank_tail_bytes` - the rest, when the tail ends in zeros. The window is bounded, so zeros
  #     that fill it are taken to continue to the end of the file: garbage stranded past the window
  #     is not a shape a crash makes, and refusing to look for it is what keeps recovery O(1) in
  #     the size of the preallocated region.
  #   * `zeros_start_inside_frame?` - whether the zeros begin before the end of the frame the
  #     damaged header claims. A frame whose header is itself unreadable has no claimed end, so the
  #     answer is false and the tail is treated as rot, which is the conservative direction.
  #   * `zeros_reach_the_end?` - whether the tail ends in zeros at all.
  defp tail_shape(file_descriptor, valid_bytes, file_size) do
    trailing_bytes = max(file_size - valid_bytes, 0)
    window = min(@tail_window_bytes, trailing_bytes)

    bytes =
      case :file.pread(file_descriptor, valid_bytes, window) do
        {:ok, chunk} -> chunk
        _eof_or_error -> <<>>
      end

    written_bytes = non_zero_prefix(bytes)
    ends_in_zeros? = written_bytes < byte_size(bytes)

    %{
      trailing_bytes: trailing_bytes,
      written_bytes: written_bytes,
      blank_tail_bytes: if(ends_in_zeros?, do: trailing_bytes - written_bytes, else: 0),
      zeros_start_inside_frame?: written_bytes < claimed_frame_size(bytes),
      zeros_reach_the_end?: ends_in_zeros?
    }
  end

  # Whether everything from `position` to the end of the file is zero. Reads in the same bounded
  # windows the scan uses and stops at the first non-zero byte, so the expensive answer is the
  # reassuring one: a segment with data behind the hole bails almost immediately and a healthy one
  # pays in full.
  defp blank_tail?(file_descriptor, position) do
    case :file.pread(file_descriptor, position, @read_window_bytes) do
      {:ok, chunk} ->
        all_zero?(chunk) and blank_tail?(file_descriptor, position + byte_size(chunk))

      :eof ->
        true

      # A descriptor that cannot be read is not one that can vouch for unwritten space, and the scan
      # reports the damage rather than assuming the best about bytes it never saw.
      {:error, _reason} ->
        false
    end
  end

  defp all_zero?(binary), do: binary == :binary.copy(<<0>>, byte_size(binary))

  # Where the trailing run of zeros starts, as a length from the front. A window that is entirely
  # zeros answers 0, and one with no zeros at all answers its own size.
  defp non_zero_prefix(bytes), do: non_zero_prefix(bytes, byte_size(bytes))
  defp non_zero_prefix(_bytes, 0), do: 0

  defp non_zero_prefix(bytes, length) do
    case :binary.at(bytes, length - 1) do
      0 -> non_zero_prefix(bytes, length - 1)
      _non_zero -> length
    end
  end

  # How long the damaged frame says it is, from its own header. Zero when the header is not a frame
  # header at all, which makes `zeros_start_inside_frame?` false: with no claimed extent there is
  # nothing to say the write stopped short, so the bytes are treated as rot.
  defp claimed_frame_size(<<0x4D51::16, payload_length::32, _checksum::32, _rest::binary>>),
    do: 10 + payload_length

  defp claimed_frame_size(_bytes), do: 0

  # Drops the bytes a torn write left behind. Without preallocation that means truncating the file,
  # which is what has always happened. With it, truncating would throw away the preallocated region
  # itself, so the damaged run is overwritten with zeros instead: the next append starts at
  # `valid_bytes` and covers it anyway, and zeroing it now restores the invariant that everything
  # past the last valid frame is unwritten space, which is what keeps a recovered replica's file
  # byte-identical to one that never crashed.
  defp discard_tail(file_descriptor, valid_bytes, _shape, 0) do
    with {:ok, _position} <- :file.position(file_descriptor, valid_bytes) do
      :file.truncate(file_descriptor)
    end
  end

  defp discard_tail(file_descriptor, valid_bytes, shape, _prealloc_bytes) do
    Preallocation.extend(file_descriptor, valid_bytes, valid_bytes + shape.written_bytes, :zeros)
  end

  # Sizes a segment at creation, or re-sizes a recovered one, and answers how far it reached so the
  # handle knows whether it may trim the tail later. `nil` for a segment that was not preallocated.
  #
  # The sync is not optional, and it was measured. Preallocating leaves the whole region dirty in
  # the page cache, and without a sync here the first commits inherit that writeback: in the batch
  # 1024 x 1KB case, where the preallocated region is large relative to the number of flushes that
  # follow it, the per-flush p99 went from 23.7ms growing to 72.0ms preallocated, three times worse,
  # while the p50 in the small-batch regimes was already 55% better. Paying it once, here, is what
  # keeps the cost a property of creating a segment rather than of committing to one.
  defp preallocate(_file_descriptor, 0), do: nil

  defp preallocate(file_descriptor, prealloc_bytes) do
    # From the file's CURRENT size, never from `valid_bytes`. The bytes between the two belong to
    # whatever recovery just decided about them, and for a `:rot` tail that decision was `:preserve`:
    # extending from `valid_bytes` would zero the damaged frame AND every valid frame after it, so
    # the next recovery would read unwritten space, report `:ok`, and the node would call itself
    # healthy having silently lost the records a peer was supposed to repair.
    #
    # A size that cannot even be read degrades the same way an extension that fails does, and with
    # nothing to put back: no byte has been written yet.
    case Preallocation.file_size(file_descriptor) do
      {:ok, size} -> extend_or_restore(file_descriptor, size, prealloc_bytes)
      {:error, _reason} -> nil
    end
  end

  # Preallocation is an optimization, and a segment works without it, so a failure here degrades to the
  # behavior this store had before it existed rather than failing the open. ENOSPC is the realistic
  # trigger, and it is not hidden: claiming the space up front only moves WHEN a full volume is noticed,
  # and the append that follows still reports it. The file is put back to the size it had first, so a
  # partial extension cannot leave a tail behind that `seal/1` would then not trim (`preallocated_to`
  # stays nil, which is what authorizes trimming). A sync that fails after a whole extension is the same
  # device failing, and it degrades the same way.
  defp extend_or_restore(file_descriptor, size, prealloc_bytes) do
    with :ok <- Preallocation.extend(file_descriptor, size, prealloc_bytes, :zeros),
         :ok <- :file.sync(file_descriptor) do
      prealloc_bytes
    else
      {:error, _reason} ->
        _ = :file.position(file_descriptor, size)
        _ = :file.truncate(file_descriptor)
        nil
    end
  end

  # Gives back the preallocated tail, so a segment this store is no longer writing is byte-exact.
  # It is what keeps every file size the rest of the system reads meaning what it meant: the sparse
  # index sidecar, `Malachi.Cluster.Scrubber`, retention's `byte_size`, and the lost-copy probe in
  # `Malachi.Cluster.SelfHealing` all measure sealed segments off disk.
  defp trim_tail(%__MODULE__{preallocated_to: nil}), do: :ok

  defp trim_tail(%__MODULE__{} = store) do
    with {:ok, _position} <- :file.position(store.file_descriptor, store.write_position) do
      :file.truncate(store.file_descriptor)
    end
  end

  @impl true
  def verify(directory, segment_id, opts \\ []) do
    segment = Segment.new(segment_id, directory, opts)
    path = Segment.path(segment)

    if File.exists?(path) do
      case :file.open(path, [:read, :raw, :binary]) do
        {:ok, file_descriptor} ->
          # A scrub reaches this without the segment's options (it walks a directory, not a handle),
          # so an unstated preallocation makes the reported `unreadable_bytes` conservative rather
          # than wrong: a blank tail is already `:ok` by classification, and only the size attached to
          # real damage is affected.
          preallocated? = Keyword.get(opts, :prealloc_bytes, @default_prealloc_bytes) > 0

          try do
            {record_count, valid_bytes, halt} = check_scan(file_descriptor)

            # Frames first: with the segment itself damaged the sidecar's verdict is moot, and
            # rebuilding an index over damaged frames would only bake the damage in.
            with {:ok, counts} <- verdict(file_descriptor, path, record_count, valid_bytes, halt, preallocated?) do
              verify_index(segment, file_descriptor, valid_bytes, counts)
            end
          after
            :file.close(file_descriptor)
          end

        {:error, reason} ->
          unreadable(path, reason)
      end
    else
      {:error, :enoent}
    end
  end

  # A segment that cannot be opened or measured is reported the way a frame that cannot be read already
  # is: damage at byte 0 carrying the POSIX reason, because to a scrub a copy nobody can read is damaged.
  # Except `:enoent`, which keeps its own meaning: the segment was removed between the existence check
  # and the open (retention, mid-scan), and a deleted segment is not a damaged one.
  defp unreadable(_path, :enoent), do: {:error, :enoent}

  defp unreadable(path, reason),
    do: {:error, %{position: 0, reason: reason, unreadable_bytes: 0, file: path}}

  # The sparse index sidecar has no checksum of its own and is trusted by every sealed read, so the
  # scrub checks it too: each entry must point at the start of a real frame whose record carries the
  # offset the entry claims. An absent sidecar is not damage (reads simply scan from the start), and
  # a rotted one is repairable locally by `rebuild_index/3`, since the index is derived from the
  # segment and never holds anything the segment does not.
  defp verify_index(segment, file_descriptor, valid_bytes, counts) do
    index_path = Segment.index_path(segment)

    case File.read(index_path) do
      {:ok, binary} ->
        entries = parse_index(binary, [])

        cond do
          # A sidecar is a whole number of fixed-size entries, so a remainder means the file was cut
          # mid-entry. Counting the parsed entries cannot see that: `parse_index/2` drops the partial
          # tail, so the count always equals what the file size implies, damaged or not.
          rem(byte_size(binary), @index_entry_bytes) != 0 ->
            {:error, index_damage(index_path, valid_bytes, :trailing_bytes)}

          bad = Enum.find(entries, &bad_index_entry?(&1, file_descriptor, valid_bytes)) ->
            {:error, index_damage(index_path, elem(bad, 1), :entry)}

          true ->
            {:ok, counts}
        end

      {:error, :enoent} ->
        {:ok, counts}

      {:error, reason} ->
        {:error, index_damage(index_path, 0, reason)}
    end
  end

  defp index_damage(index_path, position, detail) do
    %{position: position, reason: :bad_index, unreadable_bytes: 0, file: index_path, detail: detail}
  end

  # An entry is good when a frame starts exactly at its position and that frame's record carries the
  # entry's offset. Reading one frame header plus a bounded window is enough: a frame that needs more
  # than the window is decoded as incomplete, which is itself a mismatch worth reporting.
  defp bad_index_entry?({offset, position}, file_descriptor, valid_bytes) do
    if position < 0 or position >= valid_bytes do
      true
    else
      case :file.pread(file_descriptor, position, @read_window_bytes) do
        {:ok, chunk} -> not match?({:ok, %Record{offset: ^offset}, _size, _rest}, Record.decode_one(chunk))
        :eof -> true
        # An entry whose frame cannot be read is not an entry a read can trust, whatever the reason.
        # Reporting it as a bad entry is also the safe verdict: the repair for a bad index is a local
        # rebuild, which re-reads the segment and fails loudly if the device is the real problem.
        {:error, _reason} -> true
      end
    end
  end

  # A clean scan must consume the file EXACTLY: `halt == :eof` with valid frames ending short of the
  # file size means the tail is a partial frame, which for a sealed segment is damage just like a
  # checksum mismatch (an active segment's torn tail is normal and is handled by `recover/3`).
  # The damage map carries the same keys `recover/3` reports, so a caller (and the telemetry event)
  # handles findings from either path identically.
  defp verdict(file_descriptor, path, record_count, valid_bytes, halt, preallocated?) do
    case File.stat(path) do
      {:ok, %{size: file_size}} ->
        shape = tail_shape(file_descriptor, valid_bytes, file_size)
        classified_verdict(classify_tail(halt, shape), path, record_count, valid_bytes, halt, shape, preallocated?)

      {:error, reason} ->
        unreadable(path, reason)
    end
  end

  defp classified_verdict(classification, _path, record_count, valid_bytes, _halt, _shape, _preallocated?)
       when classification in [:clean, :blank] do
    {:ok, %{records: record_count, bytes: valid_bytes}}
  end

  defp classified_verdict(classification, path, _record_count, valid_bytes, halt, shape, preallocated?) do
    damage = integrity_verdict(verdict_key(classification, halt), valid_bytes, shape, true, preallocated?)
    {:error, damage |> Map.delete(:sealed?) |> Map.put(:file, path)}
  end

  # Folds the scan's halt into the classification so the verdict has one thing to match on: only a
  # `:rot` verdict carries the halt's reason, and only a halt that is an error can produce one.
  defp verdict_key(:rot, {:error, reason}), do: {:rot, reason}
  defp verdict_key(classification, _halt), do: classification

  # The verification scan: `walk/4` driven by `Malachi.Log.Record.check_one/1`, which verifies the
  # checksum without building a record struct, and an accumulator that is just a count. A scrub
  # walks whole segments only to confirm their checksums, so the per-record allocation the recovery
  # scan needs would dominate its cost, which is why the two share the LOOP but not the callback.
  defp check_scan(file_descriptor) do
    {valid_bytes, record_count, halt} =
      walk(file_descriptor, &check_frame/1, fn _frame, _position, count -> count + 1 end, 0)

    {record_count, valid_bytes, halt}
  end

  # `check_one/1` answers without a decoded frame, so it is padded to the walker's shape. One
  # four-element tuple per frame is nothing next to the `Record` struct (with its key, value and
  # header binaries) that using `decode_one/1` here would allocate instead.
  defp check_frame(binary) do
    case Record.check_one(binary) do
      {:ok, frame_size, rest} -> {:ok, nil, frame_size, rest}
      incomplete_or_error -> incomplete_or_error
    end
  end

  @impl true
  def rebuild_index(directory, segment_id, opts \\ []) do
    segment = Segment.new(segment_id, directory, opts)
    path = Segment.path(segment)

    if File.exists?(path) do
      index_interval = Keyword.get(opts, :index_interval, @default_index_interval)

      with {:ok, file_descriptor} <- :file.open(path, [:read, :raw, :binary]) do
        try do
          {_record_count, _valid_bytes, entries, _halt} = scan_segment(file_descriptor, index_interval)
          write_index(Segment.index_path(segment), entries)
        after
          :file.close(file_descriptor)
        end
      end
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

      # Measured before opening, so a failure to measure cannot strand a descriptor. A sealed segment
      # is immutable, so the size cannot change between the two.
      with {:ok, %{size: file_size}} <- File.stat(path),
           {:ok, file_descriptor} <- :file.open(path, [:read, :raw, :binary]) do
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
      end
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
    with {:ok, flushed_store} <- sync(store) do
      {:ok, flushed_store, first_offset, last_offset}
    end
  end

  defp flush_if_full(%__MODULE__{} = store, first_offset, last_offset),
    do: {:ok, store, first_offset, last_offset}

  @impl true
  def sync(%__MODULE__{pending_count: 0} = store) do
    # Nothing buffered means nothing to make durable: the only write to this descriptor is the
    # pwrite in the clause below, which is followed by its fsync in the same breath, so no written
    # byte is ever left unsynced for this clause to catch up on.
    #
    # `recover/3` truncates a torn tail without syncing, and this clause used to make that
    # truncation durable by accident. Losing it is safe because recovery is deterministic and
    # idempotent: the scan stops at the last CRC-valid frame, so a crash before the truncation
    # reaches the disk simply has the next recovery compute the same boundary again.
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

    # On either failure the handle is returned untouched, still describing the file as it was before
    # this sync, which may no longer be true: the write can have landed and only its sync failed. That
    # is why a handle that answered an error must not be retried (see the failure contract).
    with :ok <- :file.pwrite(store.file_descriptor, store.write_position, Enum.reverse(frames_iodata)),
         :ok <- :file.sync(store.file_descriptor) do
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
  end

  @impl true
  def read(%__MODULE__{segment: segment} = store, offset, max_records)
      when is_integer(offset) and is_integer(max_records) and max_records > 0 do
    committed_end_offset = Segment.end_offset(segment)

    cond do
      offset < segment.base_offset ->
        {:error, :out_of_range}

      offset >= committed_end_offset ->
        :eof

      true ->
        case do_read(store, offset, max_records) do
          {:error, _reason} = error -> error
          records -> {:ok, records}
        end
    end
  end

  @impl true
  def seal(%__MODULE__{segment: %Segment{state: :sealed}} = store), do: {:ok, store}

  def seal(%__MODULE__{} = store) do
    # The trim comes before the index and the marker: past this point the segment is immutable, so the
    # tail it will never write into is given back, and the handle stops claiming it may trim anything.
    with {:ok, store} <- sync(store),
         :ok <- trim_tail(store),
         :ok <- persist_index(store),
         :ok <- File.touch(Segment.seal_marker_path(store.segment)) do
      %Segment{} = current_segment = store.segment
      segment = %Segment{current_segment | state: :sealed, sealed_at: System.system_time(:millisecond)}
      {:ok, %{store | segment: segment, preallocated_to: nil}}
    end
  end

  @impl true
  def next_offset(%__MODULE__{next_offset: next_offset}), do: next_offset

  @impl true
  def logical_bytes(%__MODULE__{segment: segment}), do: segment.byte_size

  @impl true
  def sealed?(%__MODULE__{segment: segment}), do: Segment.sealed?(segment)

  @impl true
  def integrity(%__MODULE__{integrity: integrity}), do: integrity

  @impl true
  def pending?(%__MODULE__{pending_count: pending_count}), do: pending_count > 0

  @impl true
  def should_seal?(%__MODULE__{segment: segment}, now_ms), do: Segment.should_seal?(segment, now_ms)

  @impl true
  def close(%__MODULE__{} = store) do
    # A cleanly closed segment is byte-exact too, so only a file whose process died still carries a
    # tail, and it carries one for as long as it takes something to reopen it. `preallocated_to` is
    # `nil` on a sealed handle and on every read-only one from `open_read/3`, which is what keeps
    # this from ever truncating a segment it did not size itself.
    #
    # Best-effort on both, and safely so. A trim that fails leaves the preallocated tail in place, which
    # is exactly what a process that died mid-write leaves, and recovery already reads a blank tail as
    # unwritten space. A close that fails has nothing left to do.
    _ = trim_tail(store)
    _ = :file.close(store.file_descriptor)
    :ok
  end

  # --- reading ---

  defp do_read(store, target_offset, max_records) do
    case floor_position(store.index, target_offset) do
      0 -> collect(store, target_offset, max_records, 0, <<>>, [])
      start_position -> read_from_hint(store, target_offset, max_records, start_position)
    end
  end

  # The sparse index is a HINT, and it comes from a sidecar file with no checksum of its own, so a
  # rotted entry must never change what a read returns. Two ways it can lie, both silent:
  #
  #   * it points inside a frame, so decoding fails from the first byte and the read comes back empty,
  #     which the broker reads as "this source is drained": the consumer stalls there forever;
  #   * it points at a real frame boundary but PAST the target, so the read skips the records in
  #     between and nobody notices.
  #
  # Both show up in the result, so they are caught by looking at it rather than by validating the
  # index up front: an empty page, or a first record already past what was asked for. Either way the
  # read is redone from the start of the segment, which is what an absent index does anyway. The
  # happy path pays nothing; a lying index costs one rescan and still answers correctly.
  defp read_from_hint(store, target_offset, max_records, start_position) do
    case collect(store, target_offset, max_records, start_position, <<>>, []) do
      [%Record{offset: offset} | _rest] = records when offset <= target_offset ->
        records

      # A device that cannot be read is not a lying index, and a rescan from byte 0 would only read the
      # same failing region again, and report success if the failure happened not to repeat.
      {:error, _reason} = error ->
        error

      _empty_or_past_the_target ->
        collect(store, target_offset, max_records, 0, <<>>, [])
    end
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

          {:error, _reason} = error ->
            error

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
      {:error, _reason} = error -> error
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
    on_frame = fn record, position, {count, entries, last_indexed_position} ->
      if position - last_indexed_position >= index_interval do
        {count + 1, [{record.offset, position} | entries], position}
      else
        {count + 1, entries, last_indexed_position}
      end
    end

    # Start "behind" by one interval so the segment's first record is always indexed.
    {valid_bytes, {record_count, entries, _last_indexed_position}, halt} =
      walk(file_descriptor, &Record.decode_one/1, on_frame, {0, [], -index_interval})

    {record_count, valid_bytes, Enum.reverse(entries), halt}
  end

  # The one loop both scans run. It reads the file in bounded windows, hands every complete frame to
  # `on_frame` with the byte position it starts at, and stops at the first thing that is not a
  # frame, returning {valid_bytes, accumulator, halt}. Having exactly one of these matters more than
  # the duplication it removes: `valid_bytes` is where the segment logically ends, and recovery and
  # the scrub disagreeing about that would be a silent split brain over what a copy contains.
  #
  # One behavior change comes with folding them together: the recovery scan used to have no clause
  # for a `:file.pread/3` error and would have crashed the caller with a FunctionClauseError. It now
  # reports the error as the halt, the way the verification scan already did, because a device that
  # cannot be read IS the damage these scans exist to find, and the detector must not die of the
  # condition it was built to report.
  defp walk(file_descriptor, decode, on_frame, accumulator) do
    do_walk(file_descriptor, decode, on_frame, 0, <<>>, accumulator)
  end

  defp do_walk(file_descriptor, decode, on_frame, valid_bytes, carry, accumulator) do
    case decode.(carry) do
      {:ok, frame, frame_size, rest} ->
        accumulator = on_frame.(frame, valid_bytes, accumulator)
        do_walk(file_descriptor, decode, on_frame, valid_bytes + frame_size, rest, accumulator)

      :incomplete ->
        case :file.pread(file_descriptor, valid_bytes + byte_size(carry), @read_window_bytes) do
          {:ok, chunk} -> do_walk(file_descriptor, decode, on_frame, valid_bytes, carry <> chunk, accumulator)
          :eof -> {valid_bytes, accumulator, :eof}
          {:error, reason} -> {valid_bytes, accumulator, {:error, reason}}
        end

      # Unwritten preallocated space, but only if it stays unwritten. A zero frame header with data
      # behind it is damage wearing the shape of empty room, and believing it would end the scan
      # early and report the segment clean while every frame past the hole disappeared.
      :blank ->
        if blank_tail?(file_descriptor, valid_bytes) do
          {valid_bytes, accumulator, :blank}
        else
          {valid_bytes, accumulator, {:error, :bad_magic}}
        end

      {:error, reason} ->
        {valid_bytes, accumulator, {:error, reason}}
    end
  end

  defp last_indexed_position(index, index_interval) do
    case :array.size(index) do
      0 -> -index_interval
      size -> elem(:array.get(size - 1, index), 1)
    end
  end

  defp persist_index(store) do
    write_index(Segment.index_path(store.segment), :array.to_list(store.index))
  end

  defp write_index(path, entries) do
    binary =
      for {offset, position} <- entries, into: <<>> do
        <<offset::64, position::64>>
      end

    File.write(path, binary)
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
