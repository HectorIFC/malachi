defmodule Malachi.TCPProtocol do
  @moduledoc """
  The Malachi binary log protocol (`Malachi.Wire`). `process_frame/4` decodes a request frame,
  dispatches by `api_key` to the NorthGuard log operations (create_topic/produce/fetch/commit), and sends
  a response frame. Auth is handled by the acceptor (`TCPAcceptor`).

  This is also where the configured publish/subscribe rate limits are applied (`Malachi.RateLimiter`),
  keyed by the authenticated username and enforced per node. Both are off unless an operator configures
  them. A rate-limited request answers `rate_limited`, deliberately distinct from the `overloaded` that
  the group-commit valve sheds under saturation, so a client can tell a quota from a busy broker.

  A frame body comes from an untrusted client, so `process_frame/4` decodes inside a `try`: Wire's payload
  decoders raise on a malformed body, and the boundary answers a single error frame rather than crashing
  the connection. The client deals in `topic` + key + an **opaque cursor**, never partitions or offsets.
  """

  alias Malachi.Auth.AclStore
  alias Malachi.Auth.Authorization
  alias Malachi.Consumer.CoordinatorRouter
  alias Malachi.Consumer.GroupCoordinator
  alias Malachi.DataPlaneRouter
  alias Malachi.LogApi
  alias Malachi.Metrics
  alias Malachi.RateLimiter
  alias Malachi.Wire

  @coordinator_name Malachi.LogGroupCoordinator

  @doc """
  Processes one request frame body: decode, dispatch, and send a response frame; returns `:ok`. A
  `subscribe` frame is the exception. It registers a push stream and returns `{:stream, correlation_id}`
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
  and the stream continues. Returns `:ok`. A malformed frame is answered, not fatal: the stream ends
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
        LogApi.stream_ack_member(broker_for(topic), coordinator_for(topic), topic, group, member, cursor, count)
      else
        LogApi.stream_ack(broker_for(topic), topic, group, cursor, count)
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
      # admin management ops (user + ACL CRUD) live in their own dispatch to keep each cond small
      true -> dispatch_admin(api_key, correlation_id, payload, session)
    end
  end

  # Admin management operations (user and per-topic ACL CRUD), each gated by the :admin permission inside.
  defp dispatch_admin(api_key, correlation_id, payload, session) do
    cond do
      api_key == Wire.create_user_key() -> create_user(correlation_id, payload, session)
      api_key == Wire.delete_user_key() -> delete_user(correlation_id, payload, session)
      api_key == Wire.change_password_key() -> change_password(correlation_id, payload, session)
      api_key == Wire.list_users_key() -> list_users(correlation_id, payload, session)
      api_key == Wire.grant_acl_key() -> grant_acl(correlation_id, payload, session)
      api_key == Wire.revoke_acl_key() -> revoke_acl(correlation_id, payload, session)
      api_key == Wire.list_acls_key() -> list_acls(correlation_id, payload, session)
      true -> Wire.encode_error(correlation_id, :unknown_api_key)
    end
  end

  # Registers the caller as a push subscriber and signals the acceptor to enter streaming mode. Bounds the
  # client-supplied window/batch. On a permission failure returns an error frame (the connection stays in
  # request/response mode).
  defp subscribe(correlation_id, payload, session) do
    {topic, group, member, window_raw, max_raw} = Wire.decode_subscribe_req(payload)

    with_topic_permission(session, :consume, topic, correlation_id, fn ->
      with_rate_limit(:subscribe, session, correlation_id, fn ->
        window = stream_window(window_raw)
        max = fetch_max(max_raw)

        # a consumer-group member gets a stream scoped to its ranges (opaque); otherwise the whole group
        result =
          if member != nil and group != nil do
            LogApi.subscribe_member(broker_for(topic), coordinator_for(topic), topic, group, member, window, max)
          else
            LogApi.subscribe(broker_for(topic), topic, group, window, max)
          end

        # `:not_owner` (stale routing during a failover) answers an error frame instead of entering stream
        # mode; the client re-resolves and re-subscribes against the new owner.
        case result do
          :ok -> {:stream, correlation_id}
          {:error, reason} -> Wire.encode_error(correlation_id, normalize(reason))
        end
      end)
    end)
  end

  defp create_topic(correlation_id, payload, session) do
    {topic, _keyspace_bits} = Wire.decode_create_topic_req(payload)

    with_topic_permission(session, :produce, topic, correlation_id, fn ->
      ok_or_error(correlation_id, LogApi.create_topic(broker_for(topic), topic), <<>>)
    end)
  end

  defp produce(correlation_id, payload, session) do
    {topic, records} = Wire.decode_produce_req(payload)

    with_topic_permission(session, :produce, topic, correlation_id, fn ->
      with_rate_limit(:publish, session, correlation_id, fn ->
        case LogApi.produce_records(broker_for(topic), topic, records) do
          {:ok, count} -> Wire.encode_ok(correlation_id, <<count::32>>)
          {:error, reason} -> Wire.encode_error(correlation_id, normalize(reason))
        end
      end)
    end)
  end

  defp fetch(correlation_id, payload, session) do
    {topic, cursor, group, member, max_raw, wait_raw} = Wire.decode_fetch_req(payload)

    with_topic_permission(session, :consume, topic, correlation_id, fn ->
      max = fetch_max(max_raw)
      wait_ms = fetch_wait(wait_raw)

      result =
        cond do
          # a consumer-group member: the server scopes the fetch to the member's assigned ranges and
          # returns records + an opaque cursor (the client never sees a range id)
          member != nil and group != nil ->
            LogApi.fetch_member(broker_for(topic), coordinator_for(topic), topic, group, member, max, wait_ms)

          # an explicit cursor (client-managed paging) takes precedence over a group resume
          cursor != nil ->
            LogApi.fetch(broker_for(topic), topic, cursor, max, wait_ms)

          group != nil ->
            LogApi.fetch_group(broker_for(topic), topic, group, max, wait_ms)

          true ->
            LogApi.fetch(broker_for(topic), topic, :start, max, wait_ms)
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
    {topic, group, cursor} = Wire.decode_commit_req(payload)

    with_topic_permission(session, :consume, topic, correlation_id, fn ->
      ok_or_error(correlation_id, LogApi.commit(broker_for(topic), topic, group, cursor), <<>>)
    end)
  end

  # Removes a member from its consumer group (fast rebalance on a clean shutdown). Acks with an empty ok.
  defp leave_group(correlation_id, payload, session) do
    {topic, group, member} = Wire.decode_leave_group_req(payload)

    with_topic_permission(session, :consume, topic, correlation_id, fn ->
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

  # --- admin per-topic ACL management: grant/revoke/list ACLs, gated by the :admin permission. ---

  defp grant_acl(correlation_id, payload, session) do
    with_permission(session, :admin, correlation_id, fn ->
      {username, operation, pattern} = Wire.decode_acl_req(payload)
      apply_acl(correlation_id, operation, &Malachi.Auth.grant_acl(username, &1, pattern))
    end)
  end

  defp revoke_acl(correlation_id, payload, session) do
    with_permission(session, :admin, correlation_id, fn ->
      {username, operation, pattern} = Wire.decode_acl_req(payload)
      apply_acl(correlation_id, operation, &Malachi.Auth.revoke_acl(username, &1, pattern))
    end)
  end

  defp list_acls(correlation_id, payload, session) do
    with_permission(session, :admin, correlation_id, fn ->
      username = Wire.decode_list_acls_req(payload)
      Wire.encode_ok(correlation_id, Wire.encode_list_acls_resp(Malachi.Auth.list_acls(username)))
    end)
  end

  # Parses the wire operation string and runs `fun` with the operation atom, or answers :invalid_operation.
  defp apply_acl(correlation_id, operation, fun) do
    case Malachi.Auth.parse_acl_operation(operation) do
      {:ok, op} -> ok_or_error(correlation_id, fun.(op), <<>>)
      :error -> Wire.encode_error(correlation_id, :invalid_operation)
    end
  end

  # The consumer-group coordinator for a topic runs on the node owning the topic's vnode; resolve the
  # ref (the local name, or `{name, owner_node}` to forward) per request. Single-node → the local name.
  defp coordinator_for(topic), do: CoordinatorRouter.resolve(@coordinator_name, topic)

  # The BrokerServer shard that owns `topic`. With the default single shard this is always
  # `Malachi.LogBroker`, so this is a pure no-op; with MALACHI_DATA_SHARDS > 1 it pins the topic to its
  # shard so every operation for it lands on the same broker.
  defp broker_for(topic), do: DataPlaneRouter.shard_for(topic)

  # Runs `fun` (which returns a response frame) only if the session holds `permission`; otherwise a
  # permission-denied error frame.
  defp with_permission(session, permission, correlation_id, fun) do
    if Malachi.Auth.has_permission?(session.permissions, permission) do
      fun.()
    else
      Wire.encode_error(correlation_id, :permission_denied)
    end
  end

  # Like `with_permission` but for a resource `operation` (`:produce`/`:consume`) on a specific `topic`:
  # composes the coarse RBAC with the per-topic ACL (`Malachi.Auth.Authorization`). The ACL store is queried
  # only when the coarse permission does not already settle it (the thunk), keeping the produce/consume hot
  # path free of an ACL lookup for the common non-strict case. A denial returns a permission-denied frame.
  defp with_topic_permission(session, operation, topic, correlation_id, fun) do
    strict? = Application.get_env(:malachi, :acl_strict, false)

    allowed? =
      Authorization.allow?(session.permissions, operation, strict?, fn ->
        AclStore.authorized?(session.username, operation, topic)
      end)

    if allowed?, do: fun.(), else: Wire.encode_error(correlation_id, :permission_denied)
  end

  # Runs `fun` (which returns a response frame) only if the session's user is within the configured limit
  # for `action`; otherwise a `rate_limited` error frame and a bump of the matching blocked counter, which
  # is what makes `rate_limit_blocked{action=...}` able to move at all.
  #
  # Ordering matters: this sits INSIDE the permission check, so a request the caller was never allowed to
  # make cannot spend tokens from the quota. Unconfigured is the default and the whole cost is one config
  # read. A produce spends one token per request, not per record (the batch size is already bounded by
  # `max_frame_size`); a per-record cost would be a different quota and is left as future work.
  #
  # The check runs in this connection's own process (`check_limit_in_caller/3`) rather than through the
  # limiter GenServer, which would put every connection in the system behind one process on the hottest
  # path. The count stays exact; what that door gives up is the token bucket's smoothing, so a client can
  # burst to 2x the limit across a window boundary. See the limiter's own docs for the measurements.
  #
  # `retry_after_ms` is computed by the limiter but deliberately not carried on the wire: the error frame's
  # payload is a bare reason string, and `Malachi.Wire` freezes that encoding, so carrying it would take a
  # new api_key.
  defp with_rate_limit(action, session, correlation_id, fun) do
    case RateLimiter.action_config(action) do
      nil ->
        fun.()

      config ->
        case RateLimiter.check_limit_in_caller(session.username, action, config) do
          :ok ->
            fun.()

          {:error, :rate_limit_exceeded, _retry_after_ms} ->
            Metrics.increment_rate_limit_blocked(action)
            Wire.encode_error(correlation_id, :rate_limited)
        end
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
