defmodule Malachi.Log.Record do
  @moduledoc """
  The most granular unit of data in Malachi's log storage, mirroring NorthGuard's
  record: a `key`, a `value`, and user-defined `headers` — all opaque bytes — plus a
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
  The exact on-disk frame size, in bytes, this record will occupy — matching `encode/1`
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
  not yet contain a full frame (partial/trailing write), or `{:error, reason}` if the
  framing is corrupt.
  """
  @spec decode_one(binary()) ::
          {:ok, t(), pos_integer(), binary()} | :incomplete | {:error, atom()}
  def decode_one(<<@magic::16, payload_length::32, checksum::32, payload::binary-size(payload_length), rest::binary>>) do
    if :erlang.crc32(payload) == checksum do
      case decode_payload(payload) do
        {:ok, record} -> {:ok, record, @frame_header_size + payload_length, rest}
        :error -> {:error, :bad_payload}
      end
    else
      {:error, :bad_crc}
    end
  end

  def decode_one(<<@magic::16, payload_length::32, _checksum::32, partial::binary>>)
      when byte_size(partial) < payload_length,
      do: :incomplete

  def decode_one(binary) when byte_size(binary) < @frame_header_size, do: :incomplete
  def decode_one(_binary), do: {:error, :bad_magic}

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

      :incomplete ->
        {Enum.reverse(decoded), position}

      {:error, _reason} ->
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
