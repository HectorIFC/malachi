defmodule Malachi.TCPProtocol do
  @moduledoc """
  The Malachi binary log protocol (`Malachi.Wire`). `process_frame/4` decodes a request frame,
  dispatches by `api_key` to the NorthGuard log operations (create_topic/produce/fetch/commit), and sends
  a response frame. Auth is handled by the acceptor (`TCPAcceptor`).

  A frame body comes from an untrusted client, so `process_frame/4` decodes inside a `try`: Wire's payload
  decoders raise on a malformed body, and the boundary answers a single error frame rather than crashing
  the connection. The client deals in `topic` + key + an **opaque cursor** — never partitions or offsets.
  """

  alias Malachi.Consumer.CoordinatorRouter
  alias Malachi.Consumer.GroupCoordinator
  alias Malachi.LogApi
  alias Malachi.Wire

  @coordinator_name Malachi.LogGroupCoordinator

  @doc """
  Processes one request frame body: decode, dispatch, and send a response frame; returns `:ok`. A
  `subscribe` frame is the exception — it registers a push stream and returns `{:stream, correlation_id}`
  (no immediate response), signalling the acceptor to switch that connection to its streaming loop.
  """
  @spec process_frame(term(), binary(), map(), atom()) :: :ok | {:stream, non_neg_integer()}
  def process_frame(socket, frame_body, session, transport) do
    result =
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

    case result do
      {:stream, _sub_corr} = stream ->
        stream

      frame when is_binary(frame) ->
        transport.send(socket, frame)
        :ok
    end
  end

  @doc """
  Processes one client frame while the connection is in streaming mode. The only inbound frame is a
  `stream_ack` (applied for its window credit + durable commit); any other frame gets an error response
  and the stream continues. Returns `:ok`. A malformed frame is answered, not fatal — the stream ends
  only when the socket closes (the broker then drops the subscriber via the process `:DOWN`).
  """
  @spec process_stream_frame(term(), binary(), atom()) :: :ok
  def process_stream_frame(socket, frame_body, transport) do
    {api_key, correlation_id, payload} = Wire.decode_request(frame_body)

    if api_key == Wire.stream_ack_key() do
      {topic, group, member, cursor, count} = Wire.decode_stream_ack_req(payload)
      # the subscription already gated :consume; the session's permissions are immutable, so an ack here
      # is necessarily authorized. Fire-and-forget: credit comes back as more pushes. A member ack also
      # heartbeats the coordinator and refreshes the member's ranges (an empty ack = a heartbeat).
      if member != nil and group != nil do
        LogApi.stream_ack_member(Malachi.LogBroker, coordinator_for(topic), topic, group, member, cursor, count)
      else
        LogApi.stream_ack(Malachi.LogBroker, topic, group, cursor, count)
      end

      :ok
    else
      transport.send(socket, Wire.encode_error(correlation_id, :unexpected_frame))
      :ok
    end
  rescue
    _malformed ->
      transport.send(socket, Wire.encode_error(0, :malformed_request))
      :ok
  end

  # api_key is a value (not a literal), so match it against the Wire accessors with a cond.
  defp dispatch(api_key, correlation_id, payload, session) do
    cond do
      api_key == Wire.create_topic_key() -> create_topic(correlation_id, payload, session)
      api_key == Wire.produce_key() -> produce(correlation_id, payload, session)
      api_key == Wire.fetch_key() -> fetch(correlation_id, payload, session)
      api_key == Wire.commit_key() -> commit(correlation_id, payload, session)
      api_key == Wire.subscribe_key() -> subscribe(correlation_id, payload, session)
      api_key == Wire.leave_group_key() -> leave_group(correlation_id, payload, session)
      api_key == Wire.create_user_key() -> create_user(correlation_id, payload, session)
      api_key == Wire.delete_user_key() -> delete_user(correlation_id, payload, session)
      api_key == Wire.change_password_key() -> change_password(correlation_id, payload, session)
      api_key == Wire.list_users_key() -> list_users(correlation_id, payload, session)
      true -> Wire.encode_error(correlation_id, :unknown_api_key)
    end
  end

  # Registers the caller as a push subscriber and signals the acceptor to enter streaming mode. Bounds the
  # client-supplied window/batch. On a permission failure returns an error frame (the connection stays in
  # request/response mode).
  defp subscribe(correlation_id, payload, session) do
    with_permission(session, :consume, correlation_id, fn ->
      {topic, group, member, window_raw, max_raw} = Wire.decode_subscribe_req(payload)
      window = stream_window(window_raw)
      max = fetch_max(max_raw)

      # a consumer-group member gets a stream scoped to its ranges (opaque); otherwise the whole group
      result =
        if member != nil and group != nil do
          LogApi.subscribe_member(Malachi.LogBroker, coordinator_for(topic), topic, group, member, window, max)
        else
          LogApi.subscribe(Malachi.LogBroker, topic, group, window, max)
        end

      # `:not_owner` (stale routing during a failover) answers an error frame instead of entering stream
      # mode; the client re-resolves and re-subscribes against the new owner.
      case result do
        :ok -> {:stream, correlation_id}
        {:error, reason} -> Wire.encode_error(correlation_id, normalize(reason))
      end
    end)
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
      {topic, cursor, group, member, max_raw, wait_raw} = Wire.decode_fetch_req(payload)
      max = fetch_max(max_raw)
      wait_ms = fetch_wait(wait_raw)

      result =
        cond do
          # a consumer-group member: the server scopes the fetch to the member's assigned ranges and
          # returns records + an opaque cursor (the client never sees a range id)
          member != nil and group != nil ->
            LogApi.fetch_member(Malachi.LogBroker, coordinator_for(topic), topic, group, member, max, wait_ms)

          # an explicit cursor (client-managed paging) takes precedence over a group resume
          cursor != nil ->
            LogApi.fetch(Malachi.LogBroker, topic, cursor, max, wait_ms)

          group != nil ->
            LogApi.fetch_group(Malachi.LogBroker, topic, group, max, wait_ms)

          true ->
            LogApi.fetch(Malachi.LogBroker, topic, :start, max, wait_ms)
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

  # Removes a member from its consumer group (fast rebalance on a clean shutdown). Acks with an empty ok.
  defp leave_group(correlation_id, payload, session) do
    with_permission(session, :consume, correlation_id, fn ->
      {topic, group, member} = Wire.decode_leave_group_req(payload)
      _ = GroupCoordinator.leave(coordinator_for(topic), group, topic, member)
      Wire.encode_ok(correlation_id, <<>>)
    end)
  end

  # --- admin user management: CRUD over the replicated user store, gated by the :admin permission. Passwords
  # cross the wire in the clear (as with the auth handshake), so run these over TLS in production. ---

  defp create_user(correlation_id, payload, session) do
    with_permission(session, :admin, correlation_id, fn ->
      {username, password, perm_strings} = Wire.decode_create_user_req(payload)

      case Malachi.Auth.parse_permissions(perm_strings) do
        {:ok, permissions} -> ok_or_error(correlation_id, Malachi.Auth.add_user(username, password, permissions), <<>>)
        :error -> Wire.encode_error(correlation_id, :invalid_permissions)
      end
    end)
  end

  defp delete_user(correlation_id, payload, session) do
    with_permission(session, :admin, correlation_id, fn ->
      username = Wire.decode_delete_user_req(payload)
      ok_or_error(correlation_id, Malachi.Auth.remove_user(username), <<>>)
    end)
  end

  defp change_password(correlation_id, payload, session) do
    with_permission(session, :admin, correlation_id, fn ->
      {username, new_password} = Wire.decode_change_password_req(payload)
      ok_or_error(correlation_id, Malachi.Auth.change_password(username, new_password), <<>>)
    end)
  end

  defp list_users(correlation_id, _payload, session) do
    with_permission(session, :admin, correlation_id, fn ->
      Wire.encode_ok(correlation_id, Wire.encode_list_users_resp(Malachi.Auth.list_users()))
    end)
  end

  # The consumer-group coordinator for a topic runs on the node owning the topic's vnode; resolve the
  # ref (the local name, or `{name, owner_node}` to forward) per request. Single-node → the local name.
  defp coordinator_for(topic), do: CoordinatorRouter.resolve(@coordinator_name, topic)

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

  defp ok_or_error(correlation_id, {:error, reason}, _ok_payload),
    do: Wire.encode_error(correlation_id, normalize(reason))

  # error reasons must be an atom or string on the wire; tuple reasons (e.g. {:unroutable, key}) are inspected.
  defp normalize(reason) when is_atom(reason) or is_binary(reason), do: reason
  defp normalize(reason), do: inspect(reason)

  # Bound the client-supplied page size and long-poll wait (opt-in, capped).
  defp fetch_max(max) when is_integer(max) and max > 0, do: min(max, 1_000)
  defp fetch_max(_max), do: 100

  defp fetch_wait(wait) when is_integer(wait) and wait > 0, do: min(wait, 30_000)
  defp fetch_wait(_wait), do: 0

  # Bound the streaming credit window (max in-flight records) the client may request.
  defp stream_window(window) when is_integer(window) and window > 0, do: min(window, 10_000)
  defp stream_window(_window), do: 100
end
