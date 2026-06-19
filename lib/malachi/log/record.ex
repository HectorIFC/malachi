defmodule Malachi.Log.Record do
  @moduledoc """
  The most granular unit of data in Malachi's log storage, mirroring NorthGuard's
  record: a `key`, a `value`, and user-defined `headers` — all opaque bytes — plus a
  logical `offset` (assigned on append) and a `timestamp`.

  ## On-disk frame format

  Each record is persisted as a self-describing frame so the log can be scanned and
  recovered after a crash, and so corruption can be detected:

      <<magic::16, payload_len::32, crc32::32, payload::binary-size(payload_len)>>

  where `payload` is:

      <<offset::64, timestamp::64, flags::8,
        key_len::32, key::binary, value_len::32, value::binary,
        header_count::32, (k_len::32, k, v_len::32, v)... >>

  `flags` bit 0 distinguishes a `nil` key (absent) from an empty-binary key.
  The leading `magic`/`payload_len`/`crc32` header lets recovery (a) detect a partial
  trailing write (truncated frame) and stop cleanly, and (b) detect bit-rot via CRC.
  """

  import Bitwise

  @magic 0x4D51
  @header_size 10
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
  def encode(%__MODULE__{offset: offset} = rec) when is_integer(offset) do
    {flags, key_bin, key_len} =
      case rec.key do
        nil -> {0, <<>>, 0}
        k when is_binary(k) -> {@key_present, k, byte_size(k)}
      end

    headers_bin = encode_headers(rec.headers)

    payload =
      <<offset::64, rec.timestamp::64, flags::8, key_len::32, key_bin::binary, byte_size(rec.value)::32,
        rec.value::binary, length(rec.headers)::32, headers_bin::binary>>

    <<@magic::16, byte_size(payload)::32, :erlang.crc32(payload)::32, payload::binary>>
  end

  @doc """
  Decodes a single frame from the front of `bin`.

  Returns `{:ok, record, frame_size, rest}` on success, `:incomplete` if `bin` does
  not yet contain a full frame (partial/trailing write), or `{:error, reason}` if the
  framing is corrupt.
  """
  @spec decode_one(binary()) ::
          {:ok, t(), pos_integer(), binary()} | :incomplete | {:error, atom()}
  def decode_one(<<@magic::16, plen::32, crc::32, payload::binary-size(plen), rest::binary>>) do
    if :erlang.crc32(payload) == crc do
      case decode_payload(payload) do
        {:ok, record} -> {:ok, record, @header_size + plen, rest}
        :error -> {:error, :bad_payload}
      end
    else
      {:error, :bad_crc}
    end
  end

  def decode_one(<<@magic::16, plen::32, _crc::32, partial::binary>>) when byte_size(partial) < plen,
    do: :incomplete

  def decode_one(bin) when byte_size(bin) < @header_size, do: :incomplete
  def decode_one(_), do: {:error, :bad_magic}

  @doc """
  Decodes every complete, valid frame from the front of `bin`.

  Returns `{records_with_positions, valid_bytes}` where `records_with_positions` is a
  list of `{record, byte_offset_in_bin}` and `valid_bytes` is the number of bytes
  consumed by valid frames. Decoding stops at the first incomplete or corrupt frame,
  so `valid_bytes` is exactly the safe truncation point for crash recovery.
  """
  @spec decode_all(binary()) :: {[{t(), non_neg_integer()}], non_neg_integer()}
  def decode_all(bin), do: decode_all(bin, 0, [])

  defp decode_all(bin, pos, acc) do
    case decode_one(bin) do
      {:ok, rec, size, rest} -> decode_all(rest, pos + size, [{rec, pos} | acc])
      :incomplete -> {Enum.reverse(acc), pos}
      {:error, _reason} -> {Enum.reverse(acc), pos}
    end
  end

  # --- private encoding helpers ---

  defp encode_headers(headers) do
    for {k, v} <- headers, into: <<>> do
      <<byte_size(k)::32, k::binary, byte_size(v)::32, v::binary>>
    end
  end

  defp decode_payload(payload) do
    <<offset::64, ts::64, flags::8, key_len::32, key::binary-size(key_len), val_len::32, value::binary-size(val_len),
      header_count::32, headers_bin::binary>> = payload

    key = if (flags &&& @key_present) == @key_present, do: key, else: nil
    headers = decode_headers(headers_bin, header_count, [])

    {:ok, %__MODULE__{offset: offset, timestamp: ts, key: key, value: value, headers: headers}}
  rescue
    _ -> :error
  end

  defp decode_headers(_bin, 0, acc), do: Enum.reverse(acc)

  defp decode_headers(<<kl::32, k::binary-size(kl), vl::32, v::binary-size(vl), rest::binary>>, n, acc)
       when n > 0 do
    decode_headers(rest, n - 1, [{k, v} | acc])
  end
end
