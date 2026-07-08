defmodule Malachi.TCPProtocol do
  @moduledoc """
  The Malachi binary log protocol (`Malachi.Wire`). `process_frame/4` decodes a request frame,
  dispatches by `api_key` to the NorthGuard log operations (create_topic/produce/fetch/commit), and sends
  a response frame. Auth is handled by the acceptor (`TCPAcceptor`).

  A frame body comes from an untrusted client, so `process_frame/4` decodes inside a `try`: Wire's payload
  decoders raise on a malformed body, and the boundary answers a single error frame rather than crashing
  the connection. The client deals in `topic` + key + an **opaque cursor** — never partitions or offsets.
  """

  alias Malachi.LogApi
  alias Malachi.Wire

  @doc "Processes one request frame body: decode, dispatch, and send a response frame. Returns `:ok`."
  @spec process_frame(term(), binary(), map(), atom()) :: :ok
  def process_frame(socket, frame_body, session, transport) do
    frame =
      case frame_body do
        # header readable → the correlation id is known, so an error still matches the request
        <<_api_key::16, correlation_id::32, _rest::binary>> ->
          {api_key, ^correlation_id, payload} = Wire.decode_request(frame_body)

          try do
            dispatch(api_key, correlation_id, payload, session)
          rescue
            _malformed -> Wire.encode_error(correlation_id, :malformed_request)
          end

        # too short to even read the envelope header
        _short ->
          Wire.encode_error(0, :malformed_request)
      end

    transport.send(socket, frame)
    :ok
  end

  # api_key is a value (not a literal), so match it against the Wire accessors with a cond.
  defp dispatch(api_key, correlation_id, payload, session) do
    cond do
      api_key == Wire.create_topic_key() -> create_topic(correlation_id, payload, session)
      api_key == Wire.produce_key() -> produce(correlation_id, payload, session)
      api_key == Wire.fetch_key() -> fetch(correlation_id, payload, session)
      api_key == Wire.commit_key() -> commit(correlation_id, payload, session)
      true -> Wire.encode_error(correlation_id, :unknown_api_key)
    end
  end

  defp create_topic(correlation_id, payload, session) do
    with_permission(session, :produce, correlation_id, fn ->
      {topic, _keyspace_bits} = Wire.decode_create_topic_req(payload)
      ok_or_error(correlation_id, LogApi.create_topic(Malachi.LogBroker, topic), <<>>)
    end)
  end

  defp produce(correlation_id, payload, session) do
    with_permission(session, :produce, correlation_id, fn ->
      {topic, records} = Wire.decode_produce_req(payload)

      case LogApi.produce_records(Malachi.LogBroker, topic, records) do
        {:ok, count} -> Wire.encode_ok(correlation_id, <<count::32>>)
        {:error, reason} -> Wire.encode_error(correlation_id, normalize(reason))
      end
    end)
  end

  defp fetch(correlation_id, payload, session) do
    with_permission(session, :consume, correlation_id, fn ->
      {topic, cursor, group, max_raw, wait_raw} = Wire.decode_fetch_req(payload)
      max = fetch_max(max_raw)
      wait_ms = fetch_wait(wait_raw)

      result =
        cond do
          # an explicit cursor (client-managed paging) takes precedence over a group resume
          cursor != nil -> LogApi.fetch(Malachi.LogBroker, topic, cursor, max, wait_ms)
          group != nil -> LogApi.fetch_group(Malachi.LogBroker, topic, group, max, wait_ms)
          true -> LogApi.fetch(Malachi.LogBroker, topic, :start, max, wait_ms)
        end

      case result do
        {:ok, records, next_cursor} ->
          Wire.encode_ok(correlation_id, Wire.encode_fetch_resp(records, next_cursor))

        {:error, reason} ->
          Wire.encode_error(correlation_id, normalize(reason))
      end
    end)
  end

  defp commit(correlation_id, payload, session) do
    with_permission(session, :consume, correlation_id, fn ->
      {topic, group, cursor} = Wire.decode_commit_req(payload)
      ok_or_error(correlation_id, LogApi.commit(Malachi.LogBroker, topic, group, cursor), <<>>)
    end)
  end

  # Runs `fun` (which returns a response frame) only if the session holds `permission`; otherwise a
  # permission-denied error frame.
  defp with_permission(session, permission, correlation_id, fun) do
    if Malachi.Auth.has_permission?(session.permissions, permission) do
      fun.()
    else
      Wire.encode_error(correlation_id, :permission_denied)
    end
  end

  defp ok_or_error(correlation_id, :ok, ok_payload), do: Wire.encode_ok(correlation_id, ok_payload)
  defp ok_or_error(correlation_id, {:error, reason}, _ok_payload), do: Wire.encode_error(correlation_id, normalize(reason))

  # error reasons must be an atom or string on the wire; tuple reasons (e.g. {:unroutable, key}) are inspected.
  defp normalize(reason) when is_atom(reason) or is_binary(reason), do: reason
  defp normalize(reason), do: inspect(reason)

  # Bound the client-supplied page size and long-poll wait (opt-in, capped).
  defp fetch_max(max) when is_integer(max) and max > 0, do: min(max, 1_000)
  defp fetch_max(_max), do: 100

  defp fetch_wait(wait) when is_integer(wait) and wait > 0, do: min(wait, 30_000)
  defp fetch_wait(_wait), do: 0
end
