defmodule Malachi.Wire.Batch do
  @moduledoc """
  A batch of records on the stream keys (`Malachi.Wire`, keys 24 to 34): the unit a producer appends and
  a consumer is pushed.

      <<codec::8, count::32, inflated_size::32, size::32, payload::binary-size(size)>>

  `payload` is the entries, one after the other, compressed with `codec` or not; `inflated_size` is their
  size once inflated, and `count` how many there are. Every entry is a record (`Malachi.Wire.encode_record/1`)
  behind a flags byte. A produce batch holds plain entries; a consume batch holds positioned ones, each
  also behind the position its record has in its range:

      plain:       <<flags::8, record>>
      positioned:  <<source_index::16, offset::64, flags::8, record>>

  Flag bit 0 marks a tombstone (#206), the delete marker log compaction keeps, so a producer can append one
  and a consumer is told of one. The other bits are reserved: a batch that sets one is refused as
  malformed, so a flag added later fails loudly on a reader that does not know it.

  ## Reading a batch from a client

  A batch comes from an untrusted client, and a compressed one can inflate to far more than it weighs on
  the wire. `decode/2` refuses, before any work, a batch whose `inflated_size` is above the cap it is
  given (`:max_inflated_bytes`, kept apart from the frame size limit, which counts bytes as received), or
  whose zstd frame declares a content size other than `inflated_size`. It then inflates in steps of at
  most 128 KiB (what OTP's `:zstd` stream produces per call) and stops as soon as the output passes
  `inflated_size`, so a frame that lies about its size, or declares none, costs at most `inflated_size`
  (itself at most the cap) plus one 128 KiB step in memory. A zstd payload shorter than the smallest
  whole frame (9 bytes) is refused unread: OTP reads a frame header out of fewer bytes than it
  has, and would answer from memory it never wrote.
  Last, it reads exactly `count` records out of exactly `inflated_size` bytes. Everything that fails is an
  error tuple, never a raise.
  """

  alias Malachi.Log.Record
  alias Malachi.Wire

  @codecs [none: 0, zstd: 1]

  # magic number (4), frame header descriptor (1), window descriptor or content size (1), block header (3)
  @min_zstd_frame 9

  @typedoc "How a batch's payload is encoded: as is, or compressed with zstd."
  @type codec :: :none | :zstd

  @typedoc "Plain records (a produce) or records behind their positions (a consume)."
  @type layout :: :plain | :positioned

  @typedoc "A record of a produce batch, and whether it is a tombstone."
  @type plain :: {Record.t(), boolean()}

  @typedoc "A record of a consume batch: its position in the range's history, the record, and whether it is a tombstone."
  @type positioned :: {Wire.position(), Record.t(), boolean()}

  @typedoc "Why a batch was refused."
  @type error ::
          :batch_too_large
          | :batch_size_mismatch
          | :batch_count_mismatch
          | :malformed_batch
          | {:unknown_codec, non_neg_integer()}

  @doc "The codecs, in code order."
  @spec codecs() :: [codec()]
  def codecs, do: Keyword.keys(@codecs)

  @doc "The wire code of `codec`."
  @spec codec_code(codec()) :: non_neg_integer()
  def codec_code(codec), do: Keyword.fetch!(@codecs, codec)

  @doc "The codec of a wire code. Raises on an unknown one, like every `Malachi.Wire` decoder."
  @spec codec_of(non_neg_integer()) :: codec()
  def codec_of(code) do
    case Enum.find(@codecs, fn {_codec, value} -> value == code end) do
      {codec, _code} -> codec
      nil -> raise ArgumentError, "unknown codec #{code}"
    end
  end

  @doc "A batch of `entries` (plain, or positioned: see the moduledoc), with its payload encoded by `codec`."
  @spec encode([plain()] | [positioned()], codec(), layout()) :: binary()
  def encode(entries, codec, layout \\ :plain) do
    inflated = entries |> Enum.map(&encode_entry(&1, layout)) |> IO.iodata_to_binary()
    payload = compress(inflated, codec)

    <<codec_code(codec)::8, length(entries)::32, byte_size(inflated)::32, byte_size(payload)::32, payload::binary>>
  end

  @doc """
  Splits one batch off the front of `binary`: its header and what follows it. Raises on a batch whose
  header or payload is cut short, the way a `Malachi.Wire` payload decoder does on a malformed frame.
  """
  @spec split(binary()) ::
          {%{codec: codec(), count: non_neg_integer(), inflated_size: non_neg_integer(), size: non_neg_integer()},
           binary()}
  def split(<<code::8, count::32, inflated::32, size::32, _payload::binary-size(size), rest::binary>>),
    do: {%{codec: codec_of(code), count: count, inflated_size: inflated, size: size}, rest}

  @doc """
  The records of one whole batch, inflated within `max_inflated_bytes` (see the moduledoc). Options:

    * `:max_inflated_bytes` (required): the most the batch may inflate to.
    * `:layout`: `:plain` (default) or `:positioned`.
  """
  @spec decode(binary(), keyword()) :: {:ok, [plain()] | [positioned()]} | {:error, error()}
  def decode(batch, opts) do
    cap = Keyword.fetch!(opts, :max_inflated_bytes)
    layout = Keyword.get(opts, :layout, :plain)

    with {:ok, code, count, inflated_size, payload} <- header(batch),
         {:ok, codec} <- codec(code),
         :ok <- within(inflated_size, cap),
         {:ok, inflated} <- inflate(codec, payload, inflated_size) do
      entries(inflated, count, layout)
    end
  end

  defp header(<<code::8, count::32, inflated::32, size::32, payload::binary-size(size)>>),
    do: {:ok, code, count, inflated, payload}

  defp header(_malformed), do: {:error, :malformed_batch}

  defp codec(code) do
    case Enum.find(@codecs, fn {_codec, value} -> value == code end) do
      {codec, _code} -> {:ok, codec}
      nil -> {:error, {:unknown_codec, code}}
    end
  end

  defp within(inflated_size, cap) when inflated_size <= cap, do: :ok
  defp within(_inflated_size, _cap), do: {:error, :batch_too_large}

  defp compress(inflated, :none), do: inflated
  defp compress(inflated, :zstd), do: inflated |> :zstd.compress() |> IO.iodata_to_binary()

  defp inflate(:none, payload, inflated_size) when byte_size(payload) == inflated_size, do: {:ok, payload}
  defp inflate(:none, _payload, _inflated_size), do: {:error, :batch_size_mismatch}

  defp inflate(:zstd, payload, _inflated_size) when byte_size(payload) < @min_zstd_frame,
    do: {:error, :malformed_batch}

  defp inflate(:zstd, payload, inflated_size) do
    with :ok <- declared_size(payload, inflated_size) do
      {:ok, context} = :zstd.context(:decompress)
      inflate_stream(context, payload, inflated_size, [], 0)
    end
  rescue
    # OTP's :zstd raises on bytes that are not a zstd frame, or a corrupt one
    ErlangError -> {:error, :malformed_batch}
  end

  # A frame that states its content size must state the batch's; one that states none is held to it by
  # the bounded inflation below.
  defp declared_size(payload, inflated_size) do
    case :zstd.get_frame_header(payload) do
      {:ok, %{frameContentSize: size}} when is_integer(size) and size != inflated_size -> {:error, :batch_size_mismatch}
      {:ok, _header} -> :ok
      {:error, _reason} -> {:error, :malformed_batch}
    end
  end

  # Each call inflates at most one block (128 KiB), so the output is checked against `inflated_size` before
  # the next one: inflating past it stops at once, at a cost of one block beyond the declaration. OTP answers
  # with three elements while input remains, at a full output block or at a frame's end (where the output
  # can be empty), and every call takes input or raises, so the loop ends within the payload's length.
  defp inflate_stream(context, input, inflated_size, acc, size) do
    case :zstd.stream(context, input) do
      {:continue, rest, output} ->
        produced = IO.iodata_length(output)
        size = size + produced

        if size > inflated_size,
          do: {:error, :batch_size_mismatch},
          else: inflate_stream(context, rest, inflated_size, [acc, output], size)

      {_continue_or_done, output} ->
        finish(size + IO.iodata_length(output), inflated_size, [acc, output])
    end
  end

  defp finish(size, inflated_size, acc) when size == inflated_size, do: {:ok, IO.iodata_to_binary(acc)}
  defp finish(_size, _inflated_size, _acc), do: {:error, :batch_size_mismatch}

  defp entries(inflated, count, layout) do
    case take_entries(inflated, count, layout, []) do
      {entries, <<>>} -> {:ok, entries}
      {_entries, _rest} -> {:error, :batch_count_mismatch}
    end
  rescue
    # a record cut short or a count past the payload: the record decoders match their input exactly
    _malformed in [MatchError, FunctionClauseError, CaseClauseError] -> {:error, :malformed_batch}
  end

  defp take_entries(rest, 0, _layout, acc), do: {Enum.reverse(acc), rest}

  defp take_entries(<<flags::8, rest::binary>>, n, :plain, acc) do
    {record, tombstone, rest} = take_flagged(flags, rest)
    take_entries(rest, n - 1, :plain, [{record, tombstone} | acc])
  end

  defp take_entries(<<source::16, offset::64, flags::8, rest::binary>>, n, :positioned, acc) do
    {record, tombstone, rest} = take_flagged(flags, rest)
    take_entries(rest, n - 1, :positioned, [{{source, offset}, record, tombstone} | acc])
  end

  # Only bit 0 is defined; a reserved bit makes the match fail, which `entries/3` answers as malformed.
  defp take_flagged(flags, rest) when flags in [0, 1] do
    {record, rest} = Wire.decode_record(rest)
    {record, flags == 1, rest}
  end

  defp encode_entry({%Record{} = record, tombstone}, :plain), do: flagged(record, tombstone)

  defp encode_entry({{source, offset}, %Record{} = record, tombstone}, :positioned),
    do: <<source::16, offset::64, flagged(record, tombstone)::binary>>

  defp flagged(record, tombstone) when is_boolean(tombstone),
    do: <<if(tombstone, do: 1, else: 0)::8, Wire.encode_record(record)::binary>>
end
