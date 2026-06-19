defmodule Malachi.Storage.SegmentStore do
  @moduledoc """
  Behaviour for a pluggable segment storage engine.

  NorthGuard's segment storage is explicitly pluggable ("fps-store" is just the primary
  implementation). Malachi mirrors that: the durable hot path lives behind this
  behaviour so the pure-Elixir implementation (`Malachi.Storage.ElixirStore`) can later
  be swapped for a native one (Direct I/O via a Rust NIF) *only if* profiling under real
  concurrency shows the BEAM file layer to be the bottleneck. See
  `docs/NORTHGUARD_PORT.md`.

  ## Durability contract

  `append/2` only buffers; records become durable and readable after `sync/1`, which
  must fsync before returning. `read/3` serves only committed (synced) records — never
  buffered-but-unsynced data — matching NorthGuard's "ack only committed records".
  """

  alias Malachi.Log.Record

  @typedoc "Opaque handle to an open segment, threaded through calls."
  @type handle :: term()

  @doc "Creates and opens a new, empty segment. Fails if one already exists."
  @callback open(dir :: Path.t(), seg_id :: term(), opts :: keyword()) ::
              {:ok, handle()} | {:error, term()}

  @doc "Reopens an existing segment, recovering committed state and truncating any partial trailing write."
  @callback recover(dir :: Path.t(), seg_id :: term(), opts :: keyword()) ::
              {:ok, handle()} | {:error, term()}

  @doc """
  Buffers `records` (assigning each a logical offset). Returns the updated handle and the
  first/last offsets assigned. Records are NOT durable until `sync/1`.
  """
  @callback append(handle(), [Record.t()]) ::
              {:ok, handle(), first :: non_neg_integer(), last :: non_neg_integer()}
              | {:error, term()}

  @doc "Flushes buffered records and fsyncs. After this, appended records are committed and readable."
  @callback sync(handle()) :: {:ok, handle()} | {:error, term()}

  @doc """
  Reads up to `max_records` committed records starting at logical `offset`.
  Returns `:eof` if `offset` is at or beyond the committed end.
  """
  @callback read(handle(), offset :: non_neg_integer(), max_records :: pos_integer()) ::
              {:ok, [Record.t()]} | :eof | {:error, term()}

  @doc "Flushes, fsyncs, and seals the segment (immutable). Subsequent `append/2` must fail."
  @callback seal(handle()) :: {:ok, handle()} | {:error, term()}

  @doc "The logical offset the next appended record will receive."
  @callback next_offset(handle()) :: non_neg_integer()

  @doc "Closes the segment's file handle."
  @callback close(handle()) :: :ok
end
