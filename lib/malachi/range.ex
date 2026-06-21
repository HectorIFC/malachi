defmodule Malachi.Range do
  @moduledoc """
  NorthGuard's range: the log abstraction associated with a contiguous slice of a
  keyspace. A range owns a `Malachi.Log` (its segments) and a key range
  `[key_start, key_end)` within a power-of-two keyspace `[0, keyspace_size)`.

  A record's key is mapped into the keyspace by hashing
  (`:erlang.phash2(key, keyspace_size)`), so the keyspace is evenly distributed.
  Ranges are **buddy-allocator blocks**: every range's size is a power of two and its
  start is aligned to that size. Splitting a range cuts it in half; merging is only
  allowed between the two halves of a common parent block (a range's unique *buddy*).

  ## Split / merge are logical

  Faithful to NorthGuard, split and merge never move records. Splitting **seals** the
  range and creates two fresh, empty child ranges over the two half-keyspaces; merging
  seals the two buddies and creates a fresh child over their union. Each range keeps the
  ids of its ancestors in `parents`, which captures NorthGuard's ordering guarantee:
  all records in a parent *happen-before* the records in its children (the parent is
  sealed before any child exists). See `happens_before?/2`.

  Each range stores only its own records; reading the full history of a key across a
  split boundary (parent epoch then child epoch) is a higher-level concern, not yet
  implemented here.
  """

  import Bitwise

  alias Malachi.Log

  @default_keyspace_bits 32
  # :erlang.phash2/2 supports a range of 1..2^32, so the keyspace can be at most 2^32.
  @max_keyspace_bits 32

  @type id :: String.t()

  @type t :: %__MODULE__{
          id: id(),
          key_start: non_neg_integer(),
          key_end: non_neg_integer(),
          keyspace_size: pos_integer(),
          state: :active | :sealed,
          log: Log.t(),
          parents: [id()],
          directory: Path.t(),
          segment_opts: keyword()
        }

  defstruct [
    :id,
    :key_start,
    :key_end,
    :keyspace_size,
    :log,
    :directory,
    state: :active,
    parents: [],
    segment_opts: []
  ]

  @doc """
  Opens a fresh root range covering the entire keyspace `[0, 2^keyspace_bits)`.

  ## Options
    * `:keyspace_bits` - keyspace is `[0, 2^keyspace_bits)` (default `32`)
    * remaining options are passed through to the underlying `Malachi.Log` segments.
  """
  @spec open(Path.t(), keyword()) :: {:ok, t()}
  def open(directory, opts \\ []) do
    keyspace_bits = Keyword.get(opts, :keyspace_bits, @default_keyspace_bits)

    if keyspace_bits < 1 or keyspace_bits > @max_keyspace_bits do
      raise ArgumentError,
            "keyspace_bits must be in 1..#{@max_keyspace_bits} (got #{inspect(keyspace_bits)}); " <>
              ":erlang.phash2/2 only supports a range up to 2^32"
    end

    keyspace_size = 1 <<< keyspace_bits
    # `:base_offset` is the range's keyspace concept, not its log's offset space — each
    # range's log always starts at offset 0, so it must not leak into the segment options.
    segment_opts = Keyword.drop(opts, [:keyspace_bits, :base_offset])
    build_range(directory, 0, keyspace_size, keyspace_size, [], segment_opts)
  end

  @doc """
  Buffers `records` in this range. Every record's key must hash within the range's
  keyspace bounds; a misrouted record yields `{:error, {:key_out_of_range, key}}`.
  Fails with `{:error, :sealed}` on a sealed range.
  """
  @spec append(t(), [Malachi.Log.Record.t()]) ::
          {:ok, t(), non_neg_integer(), non_neg_integer()} | {:error, term()}
  def append(%__MODULE__{state: :sealed}, _records), do: {:error, :sealed}

  def append(%__MODULE__{} = range, []), do: append_to_log(range, [])

  def append(%__MODULE__{} = range, records) when is_list(records) and records != [] do
    case Enum.find(records, fn record -> not covers?(range, record.key) end) do
      nil -> append_to_log(range, records)
      misrouted_record -> {:error, {:key_out_of_range, misrouted_record.key}}
    end
  end

  @doc "Flushes and fsyncs the range's active segment."
  @spec sync(t()) :: {:ok, t()}
  def sync(%__MODULE__{} = range) do
    {:ok, log} = Log.sync(range.log)
    {:ok, %{range | log: log}}
  end

  @doc "Reads up to `max_records` committed records starting at `offset`."
  @spec read(t(), non_neg_integer(), pos_integer()) ::
          {:ok, [Malachi.Log.Record.t()]} | :eof | {:error, term()}
  def read(%__MODULE__{} = range, offset, max_records),
    do: Log.read(range.log, offset, max_records)

  @doc """
  Splits an active range in half. Seals it and returns the sealed parent plus two fresh
  child ranges over `[key_start, mid)` and `[mid, key_end)`. A size-1 range cannot be
  split.
  """
  @spec split(t()) :: {:ok, sealed :: t(), left :: t(), right :: t()} | {:error, term()}
  def split(%__MODULE__{state: :sealed}), do: {:error, :sealed}

  def split(%__MODULE__{key_start: key_start, key_end: key_end}) when key_end - key_start < 2,
    do: {:error, :cannot_split}

  def split(%__MODULE__{} = range) do
    {:ok, sealed} = seal(range)
    midpoint = range.key_start + div(range.key_end - range.key_start, 2)
    parents = range.parents ++ [range.id]

    {:ok, left} =
      build_range(range.directory, range.key_start, midpoint, range.keyspace_size, parents, range.segment_opts)

    {:ok, right} =
      build_range(range.directory, midpoint, range.key_end, range.keyspace_size, parents, range.segment_opts)

    {:ok, sealed, left, right}
  end

  @doc """
  Merges two buddy ranges of the same topic. Seals both and returns them plus a fresh
  child range covering their union. Fails with `{:error, :different_topic}` if the ranges
  belong to different topics, or `{:error, :not_buddies}` if they are not buddies.
  """
  @spec merge(t(), t()) ::
          {:ok, sealed_a :: t(), sealed_b :: t(), child :: t()} | {:error, term()}
  def merge(%__MODULE__{} = range_a, %__MODULE__{} = range_b) do
    cond do
      range_a.directory != range_b.directory -> {:error, :different_topic}
      not buddy?(range_a, range_b) -> {:error, :not_buddies}
      true -> do_merge(range_a, range_b)
    end
  end

  defp do_merge(range_a, range_b) do
    {:ok, sealed_a} = seal(range_a)
    {:ok, sealed_b} = seal(range_b)
    union_start = min(range_a.key_start, range_b.key_start)
    union_end = max(range_a.key_end, range_b.key_end)
    parents = Enum.uniq(range_a.parents ++ range_b.parents ++ [range_a.id, range_b.id])

    {:ok, child} =
      build_range(range_a.directory, union_start, union_end, range_a.keyspace_size, parents, range_a.segment_opts)

    {:ok, sealed_a, sealed_b, child}
  end

  @doc "Seals the range (flushing any buffered records). Idempotent."
  @spec seal(t()) :: {:ok, t()}
  def seal(%__MODULE__{state: :sealed} = range), do: {:ok, range}

  def seal(%__MODULE__{} = range) do
    {:ok, log} = Log.roll(range.log)
    {:ok, %{range | log: log, state: :sealed}}
  end

  @doc """
  Whether `range_a` and `range_b` are buddies: same-size, same-keyspace blocks that are
  the two halves of a common parent block (buddy-allocator rule, `start XOR size`).
  """
  @spec buddy?(t(), t()) :: boolean()
  def buddy?(%__MODULE__{} = range_a, %__MODULE__{} = range_b) do
    size_a = range_a.key_end - range_a.key_start
    size_b = range_b.key_end - range_b.key_start

    size_a == size_b and range_a.keyspace_size == range_b.keyspace_size and
      bxor(range_a.key_start, size_a) == range_b.key_start
  end

  @doc "Whether `key` hashes within this range's keyspace bounds."
  @spec covers?(t(), term()) :: boolean()
  def covers?(%__MODULE__{} = range, key) do
    position = position_of(range, key)
    position >= range.key_start and position < range.key_end
  end

  @doc "The keyspace position a key hashes to within this range's keyspace."
  @spec position_of(t(), term()) :: non_neg_integer()
  def position_of(%__MODULE__{keyspace_size: keyspace_size}, key),
    do: :erlang.phash2(key, keyspace_size)

  @doc """
  Whether every record in `ancestor` happens-before every record in `descendant` — true
  when `ancestor` is in `descendant`'s lineage (a transitive parent via split/merge).
  """
  @spec happens_before?(t(), t()) :: boolean()
  def happens_before?(%__MODULE__{} = ancestor, %__MODULE__{} = descendant),
    do: ancestor.id in descendant.parents

  @doc "Whether the range has buffered records not yet flushed."
  @spec pending?(t()) :: boolean()
  def pending?(%__MODULE__{} = range), do: Log.pending?(range.log)

  @doc """
  A metadata-only view of the range — id, keyspace bounds, state and lineage — without the
  underlying log or its (process-owned) file handle. Safe to pass across processes.
  """
  @spec info(t()) :: %{
          id: id(),
          key_start: non_neg_integer(),
          key_end: non_neg_integer(),
          keyspace_size: pos_integer(),
          state: :active | :sealed,
          parents: [id()]
        }
  def info(%__MODULE__{} = range) do
    %{
      id: range.id,
      key_start: range.key_start,
      key_end: range.key_end,
      keyspace_size: range.keyspace_size,
      state: range.state,
      parents: range.parents
    }
  end

  @doc "Closes the range's active segment file handle, if any."
  @spec close(t()) :: :ok
  def close(%__MODULE__{} = range), do: Log.close(range.log)

  # --- internals ---

  defp append_to_log(range, records) do
    case Log.append(range.log, records) do
      {:ok, log, first_offset, last_offset} ->
        {:ok, %{range | log: log}, first_offset, last_offset}

      {:error, _reason} = error ->
        error
    end
  end

  defp build_range(directory, key_start, key_end, keyspace_size, parents, segment_opts) do
    id = new_id(key_start, key_end)
    {:ok, log} = Log.open(Path.join(directory, id), segment_opts)

    {:ok,
     %__MODULE__{
       id: id,
       key_start: key_start,
       key_end: key_end,
       keyspace_size: keyspace_size,
       state: :active,
       log: log,
       parents: parents,
       directory: directory,
       segment_opts: segment_opts
     }}
  end

  defp new_id(key_start, key_end) do
    unique = System.unique_integer([:positive, :monotonic])
    "#{key_start}-#{key_end}-#{unique}"
  end
end
