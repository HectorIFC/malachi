defmodule Malachi.Log.Record do
  @moduledoc """
  The most granular unit of data in Malachi's log storage, mirroring NorthGuard's
  record: a `key`, a `value`, and user-defined `headers`, all opaque bytes - plus a
  logical `offset` (assigned on append) and a `timestamp`.

  ## On-disk frame format

  Each record is persisted as a self-describing frame so the log can be scanned and
  recovered after a crash, and so corruption can be detected:

      <<magic::16, payload_length::32, crc32::32, payload::binary-size(payload_length)>>

  where `payload` is:

      <<offset::64, timestamp::64, flags::8,
        key_length::32, key::binary, value_length::32, value::binary,
        header_count::32, (key_length::32, key, value_length::32, value)... >>

  `flags` bit 0 distinguishes a `nil` key (absent) from an empty-binary key.
  The leading `magic`/`payload_length`/`crc32` header lets recovery (a) detect a partial
  trailing write (truncated frame) and stop cleanly, and (b) detect bit-rot via CRC.

  ## Where a scan stops

  A scan hits one of four things, and telling them apart is what recovery is built on:

    * a valid frame, and it continues;
    * `:incomplete`, meaning the bytes run out inside a frame;
    * `:blank`, meaning the frame header is all zeros. Zeros are not a frame that went wrong,
      they are space that was never written, which is what the unwritten region of a
      preallocated segment reads back as (`Malachi.Storage.Preallocation`). A segment that grows
      never produces this, because it has no space past its last write;
    * `{:error, reason}`, meaning a frame is there and is wrong.

  The magic is `0x4D51`, so a zero magic can never be a real frame, and a payload shorter than the
  fixed fields every record carries can never be one either.
  """

  import Bitwise

  @magic 0x4D51
  @frame_header_size 10
  # Fixed payload fields: offset(8) + timestamp(8) + flags(1) + key_length(4) + value_length(4)
  # + header_count(4). Variable parts (key, value, headers) are added on top.
  @fixed_payload_size 29
  @key_present 0x01

  @type t :: %__MODULE__{
          offset: non_neg_integer() | nil,
          timestamp: non_neg_integer(),
          key: binary() | nil,
          value: binary(),
          headers: [{binary(), binary()}]
        }

  defstruct offset: nil, timestamp: 0, key: nil, value: <<>>, headers: []

  @doc """
  Builds a record. `offset` is left `nil` and assigned by the store on append.

  ## Options
    * `:key` - binary key, or `nil` (default `nil`)
    * `:headers` - list of `{binary, binary}` tuples (default `[]`)
    * `:timestamp` - epoch milliseconds (default: now)
  """
  @spec new(binary(), keyword()) :: t()
  def new(value, opts \\ []) when is_binary(value) do
    %__MODULE__{
      value: value,
      key: Keyword.get(opts, :key),
      headers: Keyword.get(opts, :headers, []),
      timestamp: Keyword.get(opts, :timestamp, System.system_time(:millisecond))
    }
  end

  @doc "Encodes a record (with its `offset` already assigned) into a binary frame."
  @spec encode(t()) :: binary()
  def encode(%__MODULE__{offset: offset} = record) when is_integer(offset) do
    {flags, key_bytes, key_length} =
      case record.key do
        nil -> {0, <<>>, 0}
        key when is_binary(key) -> {@key_present, key, byte_size(key)}
      end

    headers_binary = encode_headers(record.headers)

    payload =
      <<offset::64, record.timestamp::64, flags::8, key_length::32, key_bytes::binary, byte_size(record.value)::32,
        record.value::binary, length(record.headers)::32, headers_binary::binary>>

    <<@magic::16, byte_size(payload)::32, :erlang.crc32(payload)::32, payload::binary>>
  end

  @doc """
  The exact on-disk frame size, in bytes, this record will occupy, matching `encode/1`
  byte-for-byte. The `offset` need not be assigned, since it is always a fixed 8 bytes. Used by
  the broker to drive size-based segment rollover with the same accounting the log writes.
  """
  @spec encoded_size(t()) :: pos_integer()
  def encoded_size(%__MODULE__{} = record) do
    key_size = if is_binary(record.key), do: byte_size(record.key), else: 0

    headers_size =
      Enum.reduce(record.headers, 0, fn {key, value}, acc ->
        acc + 8 + byte_size(key) + byte_size(value)
      end)

    @frame_header_size + @fixed_payload_size + key_size + byte_size(record.value) + headers_size
  end

  @doc """
  Decodes a single frame from the front of `binary`.

  Returns `{:ok, record, frame_size, rest}` on success, `:incomplete` if `binary` does
  not yet contain a full frame (partial/trailing write), `:blank` if the frame header is
  all zeros (unwritten space, never a damaged frame), or `{:error, reason}` if the framing
  is corrupt.
  """
  @spec decode_one(binary()) ::
          {:ok, t(), pos_integer(), binary()} | :incomplete | :blank | {:error, atom()}
  def decode_one(binary) do
    case split_frame(binary) do
      {:ok, payload, frame_size, rest} ->
        case decode_payload(payload) do
          {:ok, record} -> {:ok, record, frame_size, rest}
          :error -> {:error, :bad_payload}
        end

      incomplete_or_error ->
        incomplete_or_error
    end
  end

  @doc """
  Verifies the frame at the front of `binary` **without deserializing its payload**: the framing
  and the CRC are checked, then the frame is skipped. Same return shapes as `decode_one/1` minus
  the record itself, so `{:error, :bad_payload}` cannot occur here (the payload is never parsed).

  This is the integrity-scan path (`Malachi.Log.verify/2`): a scrub walks whole segments only to
  confirm that every frame still matches its checksum, and building a `Record` struct per frame
  would dominate that cost for no benefit.
  """
  @spec check_one(binary()) :: {:ok, pos_integer(), binary()} | :incomplete | :blank | {:error, atom()}
  def check_one(binary) do
    case split_frame(binary) do
      {:ok, _payload, frame_size, rest} -> {:ok, frame_size, rest}
      incomplete_or_error -> incomplete_or_error
    end
  end

  # The single source of truth for framing and checksum verification, shared by decode_one/1 and
  # check_one/1 so the two can never disagree about what a valid frame is.
  #
  # Note the CRC covers the PAYLOAD only: corruption inside the 10-byte header surfaces as
  # :bad_magic, or as :incomplete when a mangled length field claims more bytes than exist. Every
  # single-byte corruption is still caught, only the reported reason differs.
  @spec split_frame(binary()) ::
          {:ok, binary(), pos_integer(), binary()} | :incomplete | :blank | {:error, atom()}
  defp split_frame(<<@magic::16, payload_length::32, checksum::32, payload::binary-size(payload_length), rest::binary>>)
       when payload_length >= @fixed_payload_size do
    if :erlang.crc32(payload) == checksum do
      {:ok, payload, @frame_header_size + payload_length, rest}
    else
      {:error, :bad_crc}
    end
  end

  defp split_frame(<<@magic::16, payload_length::32, _checksum::32, partial::binary>>)
       when byte_size(partial) < payload_length,
       do: :incomplete

  # Unwritten space, not a damaged frame. It comes up because a preallocated segment reads back as
  # zeros past its last write, and it is checked BEFORE the short-binary clause so that a couple of
  # zero bytes at the very end of the preallocated region are still recognized for what they are.
  #
  # A zero magic cannot collide with a real frame (the magic is a fixed non-zero constant), so this
  # never hides damage: a frame whose header rotted to zeros is indistinguishable from unwritten
  # space by construction, and the CRC over the payload is what catches rot inside a frame.
  defp split_frame(<<0::16, _rest::binary>>), do: :blank

  defp split_frame(binary) when byte_size(binary) < @frame_header_size, do: :incomplete

  # Reached with a valid magic when `payload_length` is below the fixed fields every payload
  # carries, which no encoder can produce. It matters for a torn write into preallocated space: a
  # write cut just after the magic leaves a length of zero and a checksum of zero, and `crc32(<<>>)`
  # IS zero, so without this guard that shape would verify as a valid 10-byte frame in `check_one/1`
  # while `decode_one/1` rejected its empty payload, and the two scans would disagree by 10 bytes
  # about where the segment ends.
  defp split_frame(_binary), do: {:error, :bad_magic}

  @doc """
  Decodes every complete, valid frame from the front of `binary`.

  Returns `{records_with_positions, valid_bytes}` where `records_with_positions` is a
  list of `{record, byte_position_in_binary}` and `valid_bytes` is the number of bytes
  consumed by valid frames. Decoding stops at the first incomplete or corrupt frame,
  so `valid_bytes` is exactly the safe truncation point for crash recovery.
  """
  @spec decode_all(binary()) :: {[{t(), non_neg_integer()}], non_neg_integer()}
  def decode_all(binary), do: decode_all(binary, 0, [])

  defp decode_all(binary, position, decoded) do
    case decode_one(binary) do
      {:ok, record, frame_size, rest} ->
        decode_all(rest, position + frame_size, [{record, position} | decoded])

      # Incomplete, blank and corrupt all end the run at the same place: `position` is the last
      # byte a valid frame reached, which is the only thing this function promises.
      _incomplete_blank_or_error ->
        {Enum.reverse(decoded), position}
    end
  end

  # --- private encoding helpers ---

  defp encode_headers(headers) do
    for {key, value} <- headers, into: <<>> do
      <<byte_size(key)::32, key::binary, byte_size(value)::32, value::binary>>
    end
  end

  defp decode_payload(payload) do
    <<offset::64, timestamp::64, flags::8, key_length::32, key::binary-size(key_length), value_length::32,
      value::binary-size(value_length), header_count::32, headers_binary::binary>> = payload

    key = if (flags &&& @key_present) == @key_present, do: key, else: nil
    headers = decode_headers(headers_binary, header_count, [])

    {:ok, %__MODULE__{offset: offset, timestamp: timestamp, key: key, value: value, headers: headers}}
  rescue
    # A malformed payload fails the top-level binary match (MatchError) or the
    # header-decoding clauses (FunctionClauseError). Any other exception is a real
    # bug and is left to propagate rather than being silently swallowed.
    _ in [MatchError, FunctionClauseError] -> :error
  end

  defp decode_headers(_binary, 0, headers), do: Enum.reverse(headers)

  defp decode_headers(
         <<key_length::32, key::binary-size(key_length), value_length::32, value::binary-size(value_length),
           rest::binary>>,
         remaining_count,
         headers
       )
       when remaining_count > 0 do
    decode_headers(rest, remaining_count - 1, [{key, value} | headers])
  end
end
