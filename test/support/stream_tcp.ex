defmodule Malachi.Test.StreamTCP do
  @moduledoc false
  # Client side helpers for the stream keys over a real socket (`Malachi.Wire` keys 25 to 31), shared by the
  # producer and consume stream suites. A producer stream is opened with correlation id 10, a consume
  # stream with 20, so a push names the kind of stream it belongs to.

  import ExUnit.Assertions

  alias Malachi.Log.Record
  alias Malachi.Test.TCPHelper
  alias Malachi.Wire
  alias Malachi.Wire.Batch

  @producer_corr 10
  @consumer_corr 20

  def producer_corr, do: @producer_corr
  def consumer_corr, do: @consumer_corr

  def routes(socket, topic) do
    {code, payload} = TCPHelper.request(socket, Wire.topic_routes_key(), 3, Wire.encode_topic_routes_req(topic))
    assert code == Wire.ok_code()
    Wire.decode_topic_routes_resp(payload)
  end

  def open(socket, topic, opts \\ []) do
    routes = routes(socket, topic)

    req = %{
      topic: topic,
      range: Keyword.get(opts, :range, 0),
      routes_version: Keyword.get(opts, :routes_version, routes.version),
      codec: :none,
      window_appends: Keyword.get(opts, :window_appends, 16),
      window_bytes: Keyword.get(opts, :window_bytes, 1_048_576),
      producer_id: nil,
      label: "test"
    }

    case TCPHelper.request(socket, Wire.open_stream_key(), @producer_corr, Wire.encode_open_stream_req(req)) do
      {0, payload} -> {:ok, Wire.decode_open_stream_resp(payload)}
      {1, payload} -> {:error, Wire.decode_error_reason(payload)}
    end
  end

  # A consume stream on `range` (0 unless given) of `topic`, from `start` (earliest unless given).
  def open_consume(socket, topic, opts \\ []) do
    req = %{
      topic: topic,
      range: Keyword.get(opts, :range, 0),
      routes_version: Keyword.get_lazy(opts, :routes_version, fn -> routes(socket, topic).version end),
      start: Keyword.get(opts, :start, :earliest),
      window: Keyword.get(opts, :window, 100),
      max: Keyword.get(opts, :max, 100),
      max_bytes: Keyword.get(opts, :max_bytes, 1_048_576),
      accept: Keyword.get(opts, :accept, [:none, :zstd])
    }

    case TCPHelper.request(socket, Wire.open_consume_key(), @consumer_corr, Wire.encode_open_consume_req(req)) do
      {0, payload} -> {:ok, Wire.decode_open_consume_resp(payload)}
      {1, payload} -> {:error, Wire.decode_error_reason(payload)}
    end
  end

  # One `fetch_range` page of `range` (0 unless given) of `topic`.
  def fetch_range(socket, topic, opts \\ []) do
    req = %{
      topic: topic,
      range: Keyword.get(opts, :range, 0),
      routes_version: Keyword.get_lazy(opts, :routes_version, fn -> routes(socket, topic).version end),
      start: Keyword.get(opts, :start, :earliest),
      max: Keyword.get(opts, :max, 100),
      max_bytes: Keyword.get(opts, :max_bytes, 1_048_576),
      wait_ms: Keyword.get(opts, :wait_ms, 0),
      accept: Keyword.get(opts, :accept, [:none, :zstd])
    }

    :ok = :gen_tcp.send(socket, Wire.encode_request(Wire.fetch_range_key(), 30, Wire.encode_fetch_range_req(req)))
    {:ok, body} = TCPHelper.recv_frame(socket, timeout: req.wait_ms + 5_000)

    case Wire.decode_response(body) do
      {30, 0, payload} -> {:ok, Wire.decode_page(payload)}
      {30, 1, payload} -> {:error, Wire.decode_error_reason(payload)}
    end
  end

  def consume_ack(socket, stream_id, position, window \\ 0) do
    payload = Wire.encode_consume_ack_req(%{stream_id: stream_id, position: position, window: window})
    :ok = :gen_tcp.send(socket, Wire.encode_request(Wire.consume_ack_key(), 21, payload))
  end

  def batch(values, opts \\ []) do
    tombstone = Keyword.get(opts, :tombstone, false)

    Batch.encode(
      Enum.map(values, &{Record.new(&1, key: Keyword.get(opts, :key)), tombstone}),
      Keyword.get(opts, :codec, :none)
    )
  end

  def send_append(socket, stream_id, sequence, batch),
    do:
      :ok =
        :gen_tcp.send(
          socket,
          Wire.encode_request(
            Wire.append_key(),
            11,
            IO.iodata_to_binary(Wire.encode_append_req(stream_id, sequence, batch))
          )
        )

  # The next push on the stream opened with correlation id `corr`.
  def push(socket, corr \\ @producer_corr, timeout \\ 5_000) do
    {:ok, body} = TCPHelper.recv_frame(socket, timeout: timeout)
    {^corr, 0, payload} = Wire.decode_response(body)
    Wire.decode_push(payload)
  end

  # The values and positions of a page's records.
  def page_records(%{batch: batch}) do
    {:ok, entries} = Batch.decode(batch, max_inflated_bytes: 64 * 1_048_576, layout: :positioned)
    Enum.map(entries, fn {position, record, _tombstone} -> {position, record.value} end)
  end

  # Reads pushes until every append up to `sequence` is answered (an ack's `acked_sequence` is the next one
  # not yet answered), returning every error seen on the way.
  def acked_through(socket, sequence, errors \\ []) do
    {:append_ack, ack} = push(socket)
    errors = errors ++ ack.errors
    if ack.acked_sequence > sequence, do: {ack, errors}, else: acked_through(socket, sequence, errors)
  end

  def values(socket, topic) do
    {code, payload} =
      TCPHelper.request(socket, Wire.fetch_key(), 4, Wire.encode_fetch_req(topic, nil, nil, nil, 1000, 0))

    assert code == Wire.ok_code()
    {records, _cursor} = Wire.decode_fetch_resp(payload)
    Enum.map(records, & &1.value)
  end
end
