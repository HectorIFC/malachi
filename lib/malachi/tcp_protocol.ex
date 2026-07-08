defmodule Malachi.TCPProtocol do
  @moduledoc """
  The Malachi TCP/JSON log protocol.

  Parses client requests and generates responses for the NorthGuard log actions, keeping connection
  handling (`TCPAcceptor`) separate from protocol logic (this module). Messages are newline-delimited
  JSON; responses are `{"s":"ok", ...}` or `{"s":"err","reason":"..."}`.

  ## Log actions

    * `create_topic` — create a topic (requires `:produce`)
    * `produce` — append records to a topic by key (requires `:produce`); `value`s are base64
    * `fetch` — read records by opaque cursor or consumer group (requires `:consume`)
    * `commit` — durably advance a consumer group's position (requires `:consume`)

  The client deals in `topic` + key + an **opaque cursor** — never partitions or offsets. This JSON
  protocol is being replaced by the binary wire protocol (`Malachi.Wire`); the queue/channel model was
  removed (B3a).
  """

  require Logger
  alias Malachi.I18n

  # ============================================================
  # PUBLIC API - Response Helpers
  # ============================================================

  @doc "Sends a success response with `data` merged into the ok envelope."
  def send_ok(socket, data, transport) when is_map(data) do
    response = Jason.encode!(Map.put(data, "s", "ok")) <> "\n"
    send_response(socket, response, transport)
  end

  @doc "Sends an error response with an atom or string reason."
  @compile {:inline, send_error: 3}
  def send_error(socket, reason, transport) when is_atom(reason) do
    json_msg = Jason.encode!(%{"s" => "err", "reason" => reason}) <> "\n"
    send_response(socket, json_msg, transport)
  end

  def send_error(socket, reason, transport) when is_binary(reason) do
    json_msg = Jason.encode!(%{"s" => "err", "reason" => reason}) <> "\n"
    send_response(socket, json_msg, transport)
  end

  @doc "Sends an error with extra context merged in, e.g. `retry_after_ms` (used by the auth flow)."
  def send_error(socket, reason, extra_data, transport) when is_map(extra_data) do
    base = %{"s" => "err", "reason" => to_string(reason)}
    json_msg = Jason.encode!(Map.merge(base, extra_data)) <> "\n"
    send_response(socket, json_msg, transport)
  end

  # ============================================================
  # PUBLIC API - Message Processing
  # ============================================================

  @doc "Processes an authenticated client request (compatibility wrapper without client IP)."
  @compile {:inline, process_message: 4}
  def process_message(socket, json_data, session, transport) do
    process_message(socket, json_data, session, transport, "unknown")
  end

  @doc """
  Processes a JSON message from an authenticated client: decodes, checks permissions, and dispatches to
  the log action handlers. Returns `:ok`.
  """
  @compile {:inline, process_message: 5}
  def process_message(socket, json_data, session, transport, client_ip) do
    case Jason.decode(json_data) do
      {:ok, decoded} ->
        handle_action(socket, decoded, session, transport, client_ip)

      {:error, _} ->
        send_error(socket, :invalid_json, transport)
        :ok
    end
  end

  # ============================================================
  # PRIVATE - Action Handlers (NorthGuard log; topic + key + opaque cursor, no offsets exposed)
  # ============================================================

  defp handle_action(socket, %{"action" => "create_topic", "topic" => topic}, session, transport, _client_ip) do
    with_permission(session, :produce, socket, transport, fn ->
      case Malachi.LogApi.create_topic(Malachi.LogBroker, topic) do
        :ok -> send_ok(socket, %{}, transport)
        {:error, reason} -> send_error(socket, normalize_reason(reason), transport)
      end

      :ok
    end)
  end

  defp handle_action(
         socket,
         %{"action" => "produce", "topic" => topic, "records" => records},
         session,
         transport,
         _client_ip
       )
       when is_list(records) do
    with_permission(session, :produce, socket, transport, fn ->
      with {:ok, decoded} <- decode_record_values(records),
           {:ok, count} <- Malachi.LogApi.produce(Malachi.LogBroker, topic, decoded) do
        send_ok(socket, %{"count" => count}, transport)
      else
        {:error, reason} -> send_error(socket, normalize_reason(reason), transport)
      end

      :ok
    end)
  end

  defp handle_action(socket, %{"action" => "fetch", "topic" => topic} = msg, session, transport, _client_ip) do
    with_permission(session, :consume, socket, transport, fn ->
      max = fetch_max(Map.get(msg, "max"))
      wait_ms = fetch_wait(Map.get(msg, "wait"))

      result =
        cond do
          # An explicit cursor (client-managed paging) takes precedence over a group resume. Any
          # present, non-null cursor is forwarded to `fetch`, which validates it and returns
          # `:invalid_cursor` for a malformed token (rather than silently restarting the stream).
          msg["cursor"] != nil -> Malachi.LogApi.fetch(Malachi.LogBroker, topic, msg["cursor"], max, wait_ms)
          is_binary(msg["group"]) -> Malachi.LogApi.fetch_group(Malachi.LogBroker, topic, msg["group"], max, wait_ms)
          true -> Malachi.LogApi.fetch(Malachi.LogBroker, topic, :start, max, wait_ms)
        end

      case result do
        {:ok, records, next_cursor} ->
          send_ok(socket, %{"records" => Enum.map(records, &record_to_json/1), "cursor" => next_cursor}, transport)

        {:error, reason} ->
          send_error(socket, normalize_reason(reason), transport)
      end

      :ok
    end)
  end

  defp handle_action(
         socket,
         %{"action" => "commit", "topic" => topic, "group" => group, "cursor" => cursor},
         session,
         transport,
         _client_ip
       )
       when is_binary(group) and is_binary(cursor) do
    with_permission(session, :consume, socket, transport, fn ->
      case Malachi.LogApi.commit(Malachi.LogBroker, topic, group, cursor) do
        :ok -> send_ok(socket, %{}, transport)
        {:error, reason} -> send_error(socket, normalize_reason(reason), transport)
      end

      :ok
    end)
  end

  defp handle_action(socket, _msg, _session, transport, _client_ip) do
    send_error(socket, :invalid_request, transport)
    :ok
  end

  # ============================================================
  # PRIVATE - Helper Functions
  # ============================================================

  defp with_permission(session, permission, socket, transport, action_fn) do
    if Malachi.Auth.has_permission?(session.permissions, permission) do
      action_fn.()
    else
      Logger.warning(I18n.t(:permission_denied, username: session.username, action: to_string(permission)))
      send_error(socket, :permission_denied, transport)
      :ok
    end
  end

  # send_error wants an atom or binary; tuple reasons (e.g. {:unroutable, key}) are made JSON-safe.
  defp normalize_reason(reason) when is_atom(reason) or is_binary(reason), do: reason
  defp normalize_reason(reason), do: inspect(reason)

  # Bound the client-supplied page size: a positive integer, capped, defaulting to 100.
  defp fetch_max(max) when is_integer(max) and max > 0, do: min(max, 1_000)
  defp fetch_max(_max), do: 100

  # Long-poll wait in ms, opt-in and clamped to a 30s ceiling. Absent/invalid means no wait (0).
  defp fetch_wait(wait) when is_integer(wait) and wait > 0, do: min(wait, 30_000)
  defp fetch_wait(_wait), do: 0

  # The client gets key/value/headers/timestamp — never an offset (the cursor carries position).
  defp record_to_json(record) do
    %{
      "key" => record.key,
      # value is always base64 on the wire, so arbitrary (non-UTF-8) bytes survive JSON transport.
      "value" => Base.encode64(record.value),
      "headers" => Map.new(record.headers),
      "timestamp" => record.timestamp
    }
  end

  # Every record `value` is base64 on the JSON wire; decode each to raw bytes before handing them to
  # the binary-native LogApi (key/headers stay UTF-8). A non-string or non-base64 value is rejected.
  defp decode_record_values(records) do
    Enum.reduce_while(records, {:ok, []}, fn record, {:ok, acc} ->
      case decode_record_value(record) do
        {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      error -> error
    end
  end

  defp decode_record_value(%{"value" => value} = record) when is_binary(value) do
    case Base.decode64(value) do
      {:ok, bytes} -> {:ok, %{record | "value" => bytes}}
      :error -> {:error, :invalid_base64}
    end
  end

  defp decode_record_value(_record), do: {:error, :invalid_record}

  # ============================================================
  # PRIVATE - Socket Operations
  # ============================================================

  @compile {:inline, send_response: 3}
  defp send_response(socket, data, :ssl), do: :ssl.send(socket, data)
  defp send_response(socket, data, :gen_tcp), do: :gen_tcp.send(socket, data)
  defp send_response(socket, data, transport) when is_atom(transport), do: transport.send(socket, data)
end
