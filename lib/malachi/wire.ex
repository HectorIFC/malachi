defmodule Malachi.Wire do
  @moduledoc """
  The binary wire protocol for the NorthGuard log client (B1) — a length-prefixed, request/response
  framing that replaces the JSON+base64 line protocol (measured ~29% fewer bytes and 9-17x less
  serialization CPU in `bench/protocol_bench.exs`).

      Frame:     <<len::32, body::binary-size(len)>>
      Request:   <<api_key::16, correlation_id::32, payload::binary>>
      Response:  <<correlation_id::32, error_code::16, payload::binary>>

  `correlation_id` lets a client pipeline (match each response to its request). Records on the wire carry
  **no offset** — the client never sees one; the opaque cursor carries position — so this is a distinct
  encoding from `Record.encode/1` (the on-disk frame, which includes the offset). Keys and cursors are
  length-prefixed byte strings with a presence flag (`nil` vs empty are distinct). Pure — this module
  only encodes/decodes binaries; the socket wiring is B1b.

  `decode_frame/1` is tolerant (returns `:incomplete` for a partial frame), but the payload decoders
  (`decode_request/1`, `decode_produce_req/1`, …) assume a **well-formed** body and raise on a malformed
  one. A frame body comes from an untrusted client, so B1b must decode inside a `try` and answer an error
  (or close) on a raise — keeping the malformed-input handling at the connection boundary, not in the codec.
  """

  alias Malachi.Log.Record

  # api keys (request operations)
  @auth 0
  @create_topic 1
  @produce 2
  @fetch 3
  @commit 4
  @subscribe 5
  @stream_ack 6
  @leave_group 7

  # error codes (responses): 0 = ok, 1 = error with the reason as a string payload
  @ok 0
  @error 1

  @type api_key :: 0..7
  @type error_code :: non_neg_integer()

  @spec auth_key() :: api_key()
  def auth_key, do: @auth
  def create_topic_key, do: @create_topic
  def produce_key, do: @produce
  def fetch_key, do: @fetch
  def commit_key, do: @commit
  def subscribe_key, do: @subscribe
  def stream_ack_key, do: @stream_ack
  def leave_group_key, do: @leave_group
  def ok_code, do: @ok
  def error_code, do: @error

  @doc "A success response frame (error_code 0) for `correlation_id` carrying `payload`."
  @spec encode_ok(non_neg_integer(), binary()) :: binary()
  def encode_ok(correlation_id, payload), do: encode_response(correlation_id, @ok, payload)

  @doc "An error response frame (error_code 1) whose payload is `reason` as a string."
  @spec encode_error(non_neg_integer(), term()) :: binary()
  def encode_error(correlation_id, reason) do
    encode_response(correlation_id, @error, put_str(to_string(reason)))
  end

  # ---- frame ----

  @doc "Wraps a body in a length-prefixed frame."
  @spec encode_frame(binary()) :: binary()
  def encode_frame(body) when is_binary(body), do: <<byte_size(body)::32, body::binary>>

  @doc "Peels one frame off a buffer: `{:ok, body, rest}` or `:incomplete` if the frame is not all here."
  @spec decode_frame(binary()) :: {:ok, binary(), binary()} | :incomplete
  def decode_frame(<<len::32, body::binary-size(len), rest::binary>>), do: {:ok, body, rest}
  def decode_frame(_partial), do: :incomplete

  @doc """
  Like `decode_frame/1` but bounds the frame: as soon as the 4-byte length prefix is readable, a declared
  length over `max_size` is rejected with `{:error, :frame_too_large}` — **before** the body is buffered —
  so a hostile length prefix cannot force the server to accumulate unbounded memory.
  """
  @spec decode_frame(binary(), non_neg_integer()) ::
          {:ok, binary(), binary()} | :incomplete | {:error, :frame_too_large}
  def decode_frame(<<len::32, _::binary>>, max_size) when len > max_size, do: {:error, :frame_too_large}
  def decode_frame(buffer, _max_size), do: decode_frame(buffer)

  # ---- request / response envelope ----

  @spec encode_request(api_key(), non_neg_integer(), binary()) :: binary()
  def encode_request(api_key, correlation_id, payload) do
    encode_frame(<<api_key::16, correlation_id::32, payload::binary>>)
  end

  @spec decode_request(binary()) :: {api_key(), non_neg_integer(), binary()}
  def decode_request(<<api_key::16, correlation_id::32, payload::binary>>), do: {api_key, correlation_id, payload}

  @spec encode_response(non_neg_integer(), error_code(), binary()) :: binary()
  def encode_response(correlation_id, error_code, payload) do
    encode_frame(<<correlation_id::32, error_code::16, payload::binary>>)
  end

  @spec decode_response(binary()) :: {non_neg_integer(), error_code(), binary()}
  def decode_response(<<correlation_id::32, error_code::16, payload::binary>>),
    do: {correlation_id, error_code, payload}

  # ---- operation payloads (round-trip: encode_*_req/decode_*_req) ----

  def encode_auth_req(username, password), do: <<put_str(username)::binary, put_str(password)::binary>>

  def decode_auth_req(payload) do
    {username, rest} = take_str(payload)
    {password, <<>>} = take_str(rest)
    {username, password}
  end

  def encode_auth_resp(token), do: put_str(token)

  def decode_auth_resp(payload) do
    {token, <<>>} = take_str(payload)
    token
  end

  def encode_create_topic_req(topic, keyspace_bits), do: <<put_str(topic)::binary, keyspace_bits::8>>

  def decode_create_topic_req(payload) do
    {topic, <<keyspace_bits::8>>} = take_str(payload)
    {topic, keyspace_bits}
  end

  def encode_produce_req(topic, records) do
    <<put_str(topic)::binary, length(records)::32, encode_records(records)::binary>>
  end

  def decode_produce_req(payload) do
    {topic, <<count::32, rest::binary>>} = take_str(payload)
    {records, <<>>} = take_records(rest, count, [])
    {topic, records}
  end

  # cursor is an opaque byte string (nil = start), group is an optional consumer group (nil = none),
  # member is an optional consumer-group member id (nil = whole-group / single consumer); max and wait_ms
  # are the fetch bounds. Precedence: a `member` (grouped, server-scoped to its ranges) wins, then an
  # explicit `cursor` (client-managed paging), then a `group` resume. No range/offset ever crosses the
  # wire — the response is always records + an opaque cursor.
  def encode_fetch_req(topic, cursor, group, member, max, wait_ms) do
    <<put_str(topic)::binary, put_str(cursor)::binary, put_str(group)::binary, put_str(member)::binary, max::32,
      wait_ms::32>>
  end

  def decode_fetch_req(payload) do
    {topic, rest} = take_str(payload)
    {cursor, rest} = take_str(rest)
    {group, rest} = take_str(rest)
    {member, <<max::32, wait_ms::32>>} = take_str(rest)
    {topic, cursor, group, member, max, wait_ms}
  end

  def encode_fetch_resp(records, next_cursor) do
    <<length(records)::32, encode_records(records)::binary, put_str(next_cursor)::binary>>
  end

  def decode_fetch_resp(payload) do
    <<count::32, rest::binary>> = payload
    {records, rest} = take_records(rest, count, [])
    {next_cursor, <<>>} = take_str(rest)
    {records, next_cursor}
  end

  def encode_commit_req(topic, group, cursor) do
    <<put_str(topic)::binary, put_str(group)::binary, put_str(cursor)::binary>>
  end

  def decode_commit_req(payload) do
    {topic, rest} = take_str(payload)
    {group, rest} = take_str(rest)
    {cursor, <<>>} = take_str(rest)
    {topic, group, cursor}
  end

  # Streaming (B2): subscribe opens a server-push stream for a consumer `group`, bounded by a credit
  # `window` (max in-flight records) and a per-push `max` batch size. The server then pushes records as
  # ordinary success responses tagged with the subscribe's correlation id, each carrying an
  # `encode_fetch_resp/2` payload (records + the next opaque cursor).
  # member is an optional consumer-group member id (nil = whole-group subscription); with it set the
  # server scopes the push stream to the member's ranges (opaque — the push is still records + cursor).
  def encode_subscribe_req(topic, group, member, window, max) do
    <<put_str(topic)::binary, put_str(group)::binary, put_str(member)::binary, window::32, max::32>>
  end

  def decode_subscribe_req(payload) do
    {topic, rest} = take_str(payload)
    {group, rest} = take_str(rest)
    {member, <<window::32, max::32>>} = take_str(rest)
    {topic, group, member, window, max}
  end

  # stream_ack durably commits `group`'s position at `cursor` and returns `count` records of window
  # credit (unblocking further pushes). Fire-and-forget: the server sends no response, the credit shows
  # up as more pushes.
  def encode_stream_ack_req(topic, group, member, cursor, count) do
    <<put_str(topic)::binary, put_str(group)::binary, put_str(member)::binary, put_str(cursor)::binary, count::32>>
  end

  def decode_stream_ack_req(payload) do
    {topic, rest} = take_str(payload)
    {group, rest} = take_str(rest)
    {member, rest} = take_str(rest)
    {cursor, <<count::32>>} = take_str(rest)
    {topic, group, member, cursor, count}
  end

  # leave_group removes a member from a consumer group for a fast rebalance on a clean shutdown (otherwise
  # the coordinator evicts it on session timeout). Fire-and-forget-ish: the server acks with an empty ok.
  def encode_leave_group_req(topic, group, member) do
    <<put_str(topic)::binary, put_str(group)::binary, put_str(member)::binary>>
  end

  def decode_leave_group_req(payload) do
    {topic, rest} = take_str(payload)
    {group, rest} = take_str(rest)
    {member, <<>>} = take_str(rest)
    {topic, group, member}
  end

  # ---- wire record (no offset; key/value/headers/timestamp only) ----

  @doc "Encodes a record for the wire (no offset — the client never sees one)."
  @spec encode_record(Record.t()) :: binary()
  def encode_record(%Record{key: key, value: value, timestamp: ts, headers: headers}) do
    <<put_str(key)::binary, byte_size(value)::32, value::binary, ts::64, encode_headers(headers)::binary>>
  end

  @spec decode_record(binary()) :: {Record.t(), binary()}
  def decode_record(binary) do
    {key, <<value_len::32, value::binary-size(value_len), ts::64, rest::binary>>} = take_str(binary)
    {headers, rest} = take_headers(rest)
    {%Record{key: key, value: value, timestamp: ts, headers: headers, offset: nil}, rest}
  end

  # ---- internals ----

  # length-prefixed byte string with a presence flag: 0 => nil, 1 => len+bytes
  defp put_str(nil), do: <<0::8>>
  defp put_str(s) when is_binary(s), do: <<1::8, byte_size(s)::32, s::binary>>

  defp take_str(<<0::8, rest::binary>>), do: {nil, rest}
  defp take_str(<<1::8, len::32, s::binary-size(len), rest::binary>>), do: {s, rest}

  defp encode_records(records), do: records |> Enum.map(&encode_record/1) |> IO.iodata_to_binary()

  defp take_records(rest, 0, acc), do: {Enum.reverse(acc), rest}

  defp take_records(rest, n, acc) do
    {record, rest} = decode_record(rest)
    take_records(rest, n - 1, [record | acc])
  end

  defp encode_headers(headers) do
    body = for {k, v} <- headers, into: <<>>, do: <<put_str(k)::binary, put_str(v)::binary>>
    <<length(headers)::32, body::binary>>
  end

  defp take_headers(<<count::32, rest::binary>>), do: take_headers(rest, count, [])
  defp take_headers(rest, 0, acc), do: {Enum.reverse(acc), rest}

  defp take_headers(rest, n, acc) do
    {k, rest} = take_str(rest)
    {v, rest} = take_str(rest)
    take_headers(rest, n - 1, [{k, v} | acc])
  end
end
