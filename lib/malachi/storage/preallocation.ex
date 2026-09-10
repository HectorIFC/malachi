defmodule Malachi.Storage.Preallocation do
  @moduledoc """
  Extends a segment file to its full size **before** it is written, so that appends land inside an
  already-sized region instead of growing the file.

  This exists because of what a group commit actually pays for. A 1-byte `fsync` measured 303us on
  the CI runner and a 2.5KB one measured 341us, so the bill is almost entirely fixed cost: the
  filesystem's journal commit, not the transfer. `fdatasync(2)` is supposed to skip that by flushing
  only the metadata needed to read the data back, but while a segment GROWS there is always such
  metadata, because every append changes the file's size. Preallocation is what removes the size
  change, and only then does `fdatasync` have anything to skip (issue #83, blocking #82).

  ## The three mechanisms are not equivalent

  `fdatasync` must journal anything needed to retrieve the data, so what each mechanism leaves
  behind for the first write to do is the whole point:

  | mechanism | changes i_size per append | allocates a block on first touch | extent conversion |
  | --- | --- | --- | --- |
  | growing (no preallocation) | yes | yes | n/a |
  | `:sparse` | no | yes | n/a |
  | `:allocate` | no (Linux) | no | yes, unwritten to written |
  | `:zeros` | no | no | no |

  Only `:zeros` leaves an append as a pure data write: `posix_fallocate(3)` marks the extents
  UNWRITTEN, and converting one to written on first touch is itself a journaled metadata update.
  It is why PostgreSQL pre-creates WAL segments by writing zeros rather than by calling fallocate.
  That is a mechanism argument, not a measurement, which is why all three are kept here and
  `benchmark/storage_viability.exs` measures them against both syncs before one is chosen.

  ## It is a trade, and the flush size decides which way it goes

  Preallocation takes metadata out of the commit, which is most of the bill when a flush is a few
  KB. It also gives up the filesystem's delayed allocation, which is what the bill is made of when a
  flush is large and the transfer dominates. Measured on an `ubuntu-latest` runner, one sync per
  flush, 15 interleaved repetitions per arm with an A-A control (`benchmark/storage_viability.exs`
  stage 3):

  | bytes per flush | p50 | p99 |
  | --- | --- | --- |
  | 2.5KB | -69.7% | -47.2% |
  | 25KB | -68.2% | -33.7% |
  | 64KB | -41.2% | -11.7% |
  | 128KB | -2.7% | -6.7% |
  | 256KB | +5.0% | +10.5% |
  | 512KB | +13.4% | +81.7% |
  | 1MB | +16.2% | +258.0% |

  **The median turns over around 128KB per flush, and the tail turns over before it, around 256KB.**
  So which number to read depends on which one is defended: a service with a p99 objective should
  stop preallocating well before one that watches the median. Above 512KB the tail cost is not
  subtle.

  What sets the flush size is what a producer sends per `produce`, or what group commit coalesces,
  NOT `:flush_bytes`, which is only a ceiling (10MB by default). The pinned ceiling harness runs
  2.5KB per flush, fifty times below the crossover.

  ## What it assumes about the filesystem

  Preallocation turns every append from an extend into an **overwrite of already-allocated blocks**.
  On ext4 and xfs that is in-place and is exactly the saving. On copy-on-write filesystems (btrfs,
  zfs) overwriting allocated blocks is *more* expensive than appending, so preallocation can make
  latency worse there. That is why it is an explicit operator choice (`:prealloc_bytes`, off by
  default) rather than a default.

  Every function here is a plain function over an open file descriptor: no process, no state, and
  no knowledge of segments, so `Malachi.Storage.ElixirStore` and the benchmark can measure and run
  the same code.
  """

  # Zeros are written in bounded chunks from one reused buffer, so preallocating a 64MB segment
  # never builds a 64MB binary.
  @zero_chunk_bytes 1_048_576

  @typedoc """
  How to extend the file.

    * `:sparse` - move to the new end and truncate there, which sets the size without allocating
      any block. Free to do, and leaves every block allocation to the first write.
    * `:allocate` - `:file.allocate/3` (`posix_fallocate(3)` on Linux, `F_PREALLOCATE` on macOS).
      Allocates without writing, and falls back to `:zeros` where the platform cannot do it.
    * `:zeros` - write zeros over the whole region. Costs one full-size sequential write at
      creation, and is the only mechanism that leaves nothing for the first append to journal.
  """
  @type strategy :: :sparse | :allocate | :zeros

  @doc """
  Extends the file behind `file_descriptor` from `from_byte` to `to_byte`, so that reading anywhere
  in `[from_byte, to_byte)` yields zeros.

  `from_byte` is the file's current size. Returns `:ok`, or `{:error, reason}` from the underlying
  file operation. A `to_byte` at or below `from_byte` is a no-op rather than an error, so a caller
  that recovers a segment already longer than its preallocation target never shrinks it: shrinking
  is `Malachi.Storage.ElixirStore`'s decision to make, on the seal and close paths, and never a
  side effect of asking for room.

  `:allocate` verifies that the file's size actually changed, because `F_PREALLOCATE` reserves
  blocks without necessarily moving the size, which would leave every append still extending the
  file and quietly measure nothing. Whatever gap is left is written as zeros.
  """
  @spec extend(:file.fd(), non_neg_integer(), non_neg_integer(), strategy()) :: :ok | {:error, term()}
  def extend(file_descriptor, from_byte, to_byte, strategy)

  def extend(_file_descriptor, from_byte, to_byte, _strategy) when to_byte <= from_byte, do: :ok

  def extend(file_descriptor, _from_byte, to_byte, :sparse) do
    with {:ok, _position} <- :file.position(file_descriptor, to_byte) do
      :file.truncate(file_descriptor)
    end
  end

  def extend(file_descriptor, from_byte, to_byte, :allocate) do
    case :file.allocate(file_descriptor, from_byte, to_byte - from_byte) do
      :ok -> top_up(file_descriptor, to_byte)
      {:error, _unsupported} -> extend(file_descriptor, from_byte, to_byte, :zeros)
    end
  end

  def extend(file_descriptor, from_byte, to_byte, :zeros) do
    write_zeros(file_descriptor, from_byte, to_byte, :binary.copy(<<0>>, @zero_chunk_bytes))
  end

  @doc """
  The size of the file behind `file_descriptor`, in bytes.

  Reads it from the descriptor rather than from the path, so it describes the file this handle is
  writing to even if the name has since been replaced, and so no caller needs the path just to ask.
  """
  @spec file_size(:file.fd()) :: {:ok, non_neg_integer()} | {:error, term()}
  def file_size(file_descriptor), do: :file.position(file_descriptor, :eof)

  # `:file.allocate/3` can report success without the file having grown: `F_PREALLOCATE` reserves
  # blocks and, on a platform that does not follow it with a size change, leaves i_size alone. A
  # silent no-op there would be the worst outcome of all, since every append would still extend the
  # file and the benchmark would report a mechanism that never ran. So the size is re-read and any
  # remaining gap is written; when there is none, `extend/4`'s own no-op clause answers `:ok`.
  defp top_up(file_descriptor, to_byte) do
    with {:ok, size} <- file_size(file_descriptor) do
      extend(file_descriptor, size, to_byte, :zeros)
    end
  end

  defp write_zeros(_file_descriptor, position, to_byte, _chunk) when position >= to_byte, do: :ok

  defp write_zeros(file_descriptor, position, to_byte, chunk) do
    length = min(@zero_chunk_bytes, to_byte - position)
    slice = if length == @zero_chunk_bytes, do: chunk, else: binary_part(chunk, 0, length)

    case :file.pwrite(file_descriptor, position, slice) do
      :ok -> write_zeros(file_descriptor, position + length, to_byte, chunk)
      {:error, _reason} = error -> error
    end
  end
end
