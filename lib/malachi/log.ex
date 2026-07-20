defmodule Malachi.Log do
  @moduledoc """
  An append-only log made of a sequence of segments: the building block beneath
  NorthGuard's `Range`.

  A log owns a directory of segment files. At most one segment is *active* (appendable);
  the rest are *sealed* (immutable). When the active segment hits a seal threshold (size
  or age), the log **rolls**: it seals the active segment and a subsequent append opens a
  new one. New brokers/segments are created cheaply, which is what gives NorthGuard its
  "balanced by design" property, modelled here at the single-node level.

  ## Offsets and segment naming

  Offsets are globally monotonic across the whole log. Each segment is named by its
  zero-padded `base_offset` (Kafka-style), so segments sort by base offset and each
  sealed segment's end offset is simply the next segment's base offset: no manifest and
  no scan of sealed segments is needed to route reads.

  ## Reads

  `read/3` serves committed records from the *single* segment containing `offset`. To
  read across a roll boundary, the caller advances `offset` and calls again (the normal
  consumer paging pattern). Sealed segments are opened read-only on demand using their
  persisted sparse index, then closed.

  Like `Malachi.Storage.ElixirStore`, this is a functional module over an immutable
  handle (no GenServer). Time-based flushing and concurrency belong in a layer on top.
  """

  alias Malachi.Storage.ElixirStore

  @default_store ElixirStore
  @segment_id_width 20

  @type t :: %__MODULE__{
          directory: Path.t(),
          store: module(),
          sealed_base_offsets: [non_neg_integer()],
          active: term() | nil,
          active_base_offset: non_neg_integer() | nil,
          next_offset: non_neg_integer(),
          segment_opts: keyword()
        }

  defstruct directory: nil,
            store: @default_store,
            sealed_base_offsets: [],
            active: nil,
            active_base_offset: nil,
            next_offset: 0,
            segment_opts: []

  @doc """
  Opens a fresh, empty log in `directory`.

  ## Options
    * `:base_offset` - first offset of the log (default `0`)
    * `:store` - `SegmentStore` implementation (default `ElixirStore`)
    * any remaining options (`:max_bytes`, `:max_age_ms`, `:index_interval`,
      `:flush_bytes`) are passed through to each segment.
  """
  @spec open(Path.t(), keyword()) :: {:ok, t()}
  def open(directory, opts \\ []) do
    File.mkdir_p!(directory)

    {:ok,
     %__MODULE__{
       directory: directory,
       store: Keyword.get(opts, :store, @default_store),
       next_offset: Keyword.get(opts, :base_offset, 0),
       segment_opts: Keyword.drop(opts, [:base_offset, :store])
     }}
  end

  @doc """
  Reopens an existing log, recovering all segments. Only the last (active) segment is
  scanned; sealed segments are trusted via their file names.
  """
  @spec recover(Path.t(), keyword()) :: {:ok, t()}
  def recover(directory, opts \\ []) do
    store = Keyword.get(opts, :store, @default_store)
    segment_opts = Keyword.drop(opts, [:base_offset, :store])

    base_offsets =
      directory
      |> Path.join("*.log")
      |> Path.wildcard()
      |> Enum.map(&base_offset_from_path/1)
      |> Enum.sort()

    if base_offsets == [] do
      open(directory, opts)
    else
      recover_with_segments(directory, store, segment_opts, base_offsets)
    end
  end

  @doc """
  Buffers `records`, assigning each a globally monotonic offset. Records are not durable
  until `sync/1`. Returns the updated log and the first/last offsets assigned.
  """
  @spec append(t(), [Malachi.Log.Record.t()]) ::
          {:ok, t(), non_neg_integer(), non_neg_integer()} | {:error, term()}
  def append(%__MODULE__{} = log, []), do: {:ok, log, log.next_offset, log.next_offset - 1}

  def append(%__MODULE__{} = log, records) when is_list(records) and records != [] do
    log = ensure_active(log)

    case log.store.append(log.active, records) do
      {:ok, active, first_offset, last_offset} ->
        {:ok, %{log | active: active, next_offset: last_offset + 1}, first_offset, last_offset}

      {:error, _reason} = error ->
        error
    end
  end

  @doc "Flushes and fsyncs the active segment, then rolls it if a seal threshold is hit."
  @spec sync(t()) :: {:ok, t()}
  def sync(%__MODULE__{active: nil} = log), do: {:ok, log}

  def sync(%__MODULE__{} = log) do
    {:ok, active} = log.store.sync(log.active)
    maybe_roll(%{log | active: active})
  end

  @doc "Forces the active segment to seal (no-op if there is no active segment)."
  @spec roll(t()) :: {:ok, t()}
  def roll(%__MODULE__{active: nil} = log), do: {:ok, log}
  def roll(%__MODULE__{} = log), do: do_roll(log)

  @doc """
  Reads up to `max_records` committed records from the segment containing `offset`.
  Returns `:eof` past the end of the log, or `{:error, :out_of_range}` below its start.
  """
  @spec read(t(), non_neg_integer(), pos_integer()) ::
          {:ok, [Malachi.Log.Record.t()]} | :eof | {:error, term()}
  def read(%__MODULE__{} = log, offset, max_records)
      when is_integer(offset) and is_integer(max_records) and max_records > 0 do
    case Enum.find(segment_entries(log), fn {base_offset, end_offset} ->
           offset >= base_offset and offset < end_offset
         end) do
      nil ->
        if offset >= log.next_offset, do: :eof, else: {:error, :out_of_range}

      {base_offset, end_offset} ->
        read_segment(log, base_offset, end_offset, offset, max_records)
    end
  end

  @doc "Whether the log has buffered records not yet flushed."
  @spec pending?(t()) :: boolean()
  def pending?(%__MODULE__{active: nil}), do: false
  def pending?(%__MODULE__{} = log), do: log.store.pending?(log.active)

  @doc "Closes the active segment's file handle, if any."
  @spec close(t()) :: :ok
  def close(%__MODULE__{active: nil}), do: :ok
  def close(%__MODULE__{} = log), do: log.store.close(log.active)

  @doc """
  Closes the log and removes its directory with every segment file, irreversible. The log owns its
  directory, so this drops all of its data (used by retention to expire a whole segment). Best-effort
  on the file removal: a leftover file without control-plane metadata is harmless.
  """
  @spec delete(t()) :: :ok
  def delete(%__MODULE__{} = log) do
    _ = close(log)
    _ = File.rm_rf(log.directory)
    :ok
  end

  # --- internals ---

  defp recover_with_segments(directory, store, segment_opts, base_offsets) do
    last_base_offset = List.last(base_offsets)

    {:ok, handle} =
      store.recover(directory, segment_id_for(last_base_offset), [base_offset: last_base_offset] ++ segment_opts)

    next_offset = store.next_offset(handle)

    log = %__MODULE__{
      directory: directory,
      store: store,
      segment_opts: segment_opts,
      next_offset: next_offset
    }

    if store.sealed?(handle) do
      :ok = store.close(handle)
      {:ok, %{log | sealed_base_offsets: base_offsets, active: nil, active_base_offset: nil}}
    else
      {:ok,
       %{
         log
         | sealed_base_offsets: base_offsets -- [last_base_offset],
           active: handle,
           active_base_offset: last_base_offset
       }}
    end
  end

  defp ensure_active(%__MODULE__{active: nil} = log) do
    base_offset = log.next_offset

    {:ok, active} =
      log.store.open(log.directory, segment_id_for(base_offset), [base_offset: base_offset] ++ log.segment_opts)

    %{log | active: active, active_base_offset: base_offset}
  end

  defp ensure_active(%__MODULE__{} = log), do: log

  defp maybe_roll(%__MODULE__{} = log) do
    now_ms = System.system_time(:millisecond)

    if not log.store.sealed?(log.active) and log.store.should_seal?(log.active, now_ms) do
      do_roll(log)
    else
      {:ok, log}
    end
  end

  defp do_roll(%__MODULE__{} = log) do
    {:ok, sealed} = log.store.seal(log.active)
    :ok = log.store.close(sealed)

    {:ok,
     %{
       log
       | sealed_base_offsets: log.sealed_base_offsets ++ [log.active_base_offset],
         active: nil,
         active_base_offset: nil
     }}
  end

  # Returns [{base_offset, end_offset}] for every segment, in ascending order.
  defp segment_entries(%__MODULE__{} = log) do
    active = if log.active_base_offset, do: [log.active_base_offset], else: []

    case log.sealed_base_offsets ++ active do
      [] -> []
      base_offsets -> Enum.zip(base_offsets, tl(base_offsets) ++ [log.next_offset])
    end
  end

  defp read_segment(
         %__MODULE__{active_base_offset: active_base_offset} = log,
         base_offset,
         _end_offset,
         offset,
         max_records
       )
       when base_offset == active_base_offset do
    log.store.read(log.active, offset, max_records)
  end

  defp read_segment(%__MODULE__{} = log, base_offset, end_offset, offset, max_records) do
    opts = [base_offset: base_offset, record_count: end_offset - base_offset] ++ log.segment_opts

    case log.store.open_read(log.directory, segment_id_for(base_offset), opts) do
      {:ok, handle} ->
        result = log.store.read(handle, offset, max_records)
        :ok = log.store.close(handle)
        result

      {:error, _reason} = error ->
        error
    end
  end

  defp segment_id_for(base_offset) do
    base_offset |> Integer.to_string() |> String.pad_leading(@segment_id_width, "0")
  end

  defp base_offset_from_path(path), do: path |> Path.basename(".log") |> String.to_integer()
end
