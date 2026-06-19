defmodule Malachi.Log.Segment do
  @moduledoc """
  Metadata describing one segment — the unit of replication in NorthGuard's model.

  A segment is a sequence of `Malachi.Log.Record`s persisted to a single file
  (file-per-segment). It is either `:active` (records may be appended) or `:sealed`
  (immutable). A segment is sealed when it reaches a size limit, exceeds a maximum
  active age, or (in a replicated deployment) on replica failure.

  This struct is pure data; durable I/O lives in `Malachi.Storage.SegmentStore`
  implementations. The counters here (`byte_size`, `record_count`) reflect *flushed*
  (durable, committed) state — not data still buffered in memory.
  """

  # NorthGuard seals at 1GB or after 1h active.
  @default_max_bytes 1_073_741_824
  @default_max_age_ms 3_600_000

  @type id :: term()
  @type state :: :active | :sealed

  @type t :: %__MODULE__{
          id: id(),
          dir: Path.t(),
          base_offset: non_neg_integer(),
          state: state(),
          byte_size: non_neg_integer(),
          record_count: non_neg_integer(),
          created_at: non_neg_integer(),
          sealed_at: non_neg_integer() | nil,
          max_bytes: pos_integer(),
          max_age_ms: pos_integer()
        }

  defstruct id: nil,
            dir: nil,
            base_offset: 0,
            state: :active,
            byte_size: 0,
            record_count: 0,
            created_at: 0,
            sealed_at: nil,
            max_bytes: @default_max_bytes,
            max_age_ms: @default_max_age_ms

  @doc """
  Builds active segment metadata.

  ## Options
    * `:base_offset` - logical offset of this segment's first record (default `0`)
    * `:max_bytes` - seal threshold in bytes (default 1GB)
    * `:max_age_ms` - seal threshold for active age (default 1h)
    * `:created_at` - epoch ms (default: now)
  """
  @spec new(id(), Path.t(), keyword()) :: t()
  def new(id, dir, opts \\ []) do
    %__MODULE__{
      id: id,
      dir: dir,
      base_offset: Keyword.get(opts, :base_offset, 0),
      created_at: Keyword.get(opts, :created_at, System.system_time(:millisecond)),
      max_bytes: Keyword.get(opts, :max_bytes, @default_max_bytes),
      max_age_ms: Keyword.get(opts, :max_age_ms, @default_max_age_ms)
    }
  end

  @doc "Absolute path of the segment's data file."
  @spec path(t()) :: Path.t()
  def path(%__MODULE__{dir: dir, id: id}), do: Path.join(dir, "#{id}.log")

  @doc "Absolute path of the segment's persisted sparse index sidecar."
  @spec index_path(t()) :: Path.t()
  def index_path(%__MODULE__{dir: dir, id: id}), do: Path.join(dir, "#{id}.idx")

  @doc "Absolute path of the segment's seal marker."
  @spec seal_marker_path(t()) :: Path.t()
  def seal_marker_path(%__MODULE__{dir: dir, id: id}), do: Path.join(dir, "#{id}.sealed")

  @doc "The logical offset *after* the last committed record (exclusive end)."
  @spec end_offset(t()) :: non_neg_integer()
  def end_offset(%__MODULE__{base_offset: base, record_count: count}), do: base + count

  @doc "Whether the segment is sealed (immutable)."
  @spec sealed?(t()) :: boolean()
  def sealed?(%__MODULE__{state: state}), do: state == :sealed

  @doc """
  Whether an active segment has hit a seal threshold (size or age) at time `now_ms`.
  Always `false` for an already-sealed segment.
  """
  @spec should_seal?(t(), non_neg_integer()) :: boolean()
  def should_seal?(%__MODULE__{state: :sealed}, _now_ms), do: false

  def should_seal?(%__MODULE__{} = seg, now_ms) do
    seg.byte_size >= seg.max_bytes or now_ms - seg.created_at >= seg.max_age_ms
  end
end
