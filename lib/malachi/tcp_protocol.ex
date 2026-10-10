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
  alias Malachi.Auth.ConsoleRole
  alias Malachi.BrokerServer
  alias Malachi.Cluster.ClusterFlagsCache
  alias Malachi.Cluster.Policy
  alias Malachi.ConnectionStreams
  alias Malachi.Consumer.CoordinatorRouter
  alias Malachi.Consumer.GroupCoordinator
  alias Malachi.ConsumeStreams
  alias Malachi.DataPlaneRouter
  alias Malachi.LogApi
  alias Malachi.Metadata
  alias Malachi.Metrics
  alias Malachi.Policies
  alias Malachi.ProducerStreams
  alias Malachi.RateLimiter
  alias Malachi.Routing
  alias Malachi.StreamWindow
  alias Malachi.Wire

  @coordinator_name Malachi.LogGroupCoordinator

  @doc """
  Processes one request frame body: decode, dispatch, and send a response frame; returns `:ok`. A
  `subscribe` frame is the exception. It registers a push stream and returns `{:stream, correlation_id}`
  (no immediate response), signalling the acceptor to switch that connection to its streaming loop.
  """
  @spec process_frame(term(), binary(), map(), atom()) ::
          :ok | {:stream, non_neg_integer()} | {:opened, :producer | :consumer, non_neg_integer(), map()}
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

      {:opened, _kind, _corr, _opened} = opened ->
        opened

      frame when is_binary(frame) ->
        transport.send(socket, frame)
        :ok
    end
  end

  @doc """
  Processes one client frame on a connection that holds streams (`Malachi.ConnectionStreams`): appends,
  read acks and closes go to the streams, `open_stream` and `open_consume` open another, and every other
  request is answered as on any connection. A `subscribe` is refused: its push stream belongs to a
  connection of its own. Returns the streams after the frame.
  """
  @spec process_streams_frame(term(), binary(), map(), atom(), ConnectionStreams.t()) :: ConnectionStreams.t()
  def process_streams_frame(socket, frame_body, session, transport, streams) do
    case frame_body do
      <<api_key::16, correlation_id::32, payload::binary>> ->
        try do
          streams_frame(api_key, correlation_id, payload, session, streams, socket, transport)
        rescue
          _malformed ->
            transport.send(socket, Wire.encode_error(correlation_id, :malformed_request))
            streams
        end

      _short ->
        transport.send(socket, Wire.encode_error(0, :malformed_request))
        streams
    end
  end

  defp streams_frame(api_key, correlation_id, payload, session, streams, socket, transport) do
    cond do
      api_key == Wire.append_key() ->
        {stream_id, sequence, batch} = Wire.decode_append_req(payload)
        append(correlation_id, stream_id, sequence, batch, session, streams, socket, transport)

      api_key == Wire.consume_ack_key() ->
        consume_ack(correlation_id, Wire.decode_consume_ack_req(payload), streams, socket, transport)

      api_key == Wire.fetch_range_key() and ClusterFlagsCache.enabled?(Routing.flag()) ->
        fetch_range_async(correlation_id, payload, session, streams, socket, transport)

      api_key == Wire.close_stream_key() ->
        stream_id = Wire.decode_close_stream_req(payload)

        case ConnectionStreams.close(streams, stream_id) do
          {:ok, streams} ->
            transport.send(socket, Wire.encode_ok(correlation_id, <<>>))
            streams

          :unknown ->
            transport.send(socket, Wire.encode_error(correlation_id, :unknown_stream))
            streams
        end

      api_key == Wire.subscribe_key() ->
        transport.send(socket, Wire.encode_error(correlation_id, :unexpected_frame))
        streams

      true ->
        case process_frame(socket, <<api_key::16, correlation_id::32, payload::binary>>, session, transport) do
          {:opened, kind, corr, opened} -> stream_opened(socket, transport, streams, kind, corr, opened)
          :ok -> streams
        end
    end
  end

  @doc """
  Records a stream the broker opened (the `{:opened, kind, corr, opened}` `process_frame/4` returned) and
  answers the client with its id: a producer stream with its segment, window and routes version, a consume
  stream with the position its first push starts at. Returns the streams.
  """
  @spec stream_opened(term(), atom(), ConnectionStreams.t(), :producer | :consumer, non_neg_integer(), map()) ::
          ConnectionStreams.t()
  def stream_opened(socket, transport, streams, :producer, corr, opened) do
    {streams, stream_id} = ConnectionStreams.open_producer(streams, Map.put(opened, :corr, corr))

    resp = %{
      stream_id: stream_id,
      segment: Metadata.segment_seq(opened.segment_id),
      window_appends: opened.granted.appends,
      window_bytes: opened.granted.bytes,
      routes_version: opened.routes_version
    }

    transport.send(socket, Wire.encode_ok(corr, Wire.encode_open_stream_resp(resp)))
    streams
  end

  def stream_opened(socket, transport, streams, :consumer, corr, opened) do
    {streams, stream_id} = ConnectionStreams.open_consumer(streams, Map.put(opened, :corr, corr))
    resp = %{stream_id: stream_id, position: opened.position}
    transport.send(socket, Wire.encode_ok(corr, Wire.encode_open_consume_resp(resp)))
    streams
  end

  # A read ack has no response: what it changes shows up as pushes. One for a stream this connection does
  # not hold is answered, so a client acking the wrong id learns it.
  defp consume_ack(
         correlation_id,
         %{stream_id: stream_id, position: position, window: window},
         streams,
         socket,
         transport
       ) do
    # the window a consumer asks for is capped as at open
    window = if window > 0, do: stream_window(window), else: 0

    case ConsumeStreams.ack(streams.consumers, stream_id, position, window) do
      {:unknown, _stream_id} ->
        transport.send(socket, Wire.encode_error(correlation_id, :unknown_stream))
        streams

      {consumers, frames} ->
        Enum.each(frames, &transport.send(socket, &1))
        %{streams | consumers: consumers}
    end
  end

  # One append: the flag and the topic permission are checked again on every frame (an operator can revoke
  # either while the stream is open). The publish quotas are charged inside `ProducerStreams.append/6`, once
  # the batch is decoded, by its inflated records and bytes (`charge_publish/3`).
  defp append(correlation_id, stream_id, sequence, batch, session, streams, socket, transport) do
    case ProducerStreams.topic(streams.producers, stream_id) do
      nil ->
        transport.send(socket, Wire.encode_error(correlation_id, :unknown_stream))
        streams

      topic ->
        refusal =
          cond do
            not ClusterFlagsCache.enabled?(Routing.flag()) -> :unsupported
            not topic_allowed?(session, :produce, topic) -> :permission_denied
            true -> nil
          end

        if refusal do
          transport.send(socket, Wire.encode_error(correlation_id, refusal))
          streams
        else
          # `topic/2` above names only an open stream, so the append is taken or answered here
          charge = &charge_publish(session, &1, &2)

          {producers, frames} =
            ProducerStreams.append(streams.producers, stream_id, sequence, batch, max_inflated_batch_bytes(), charge)

          Enum.each(frames, &transport.send(socket, &1))
          %{streams | producers: producers}
        end
    end
  end

  # Charges a produce's records and bytes to the user's publish quotas (records under `:publish`, bytes
  # under `:publish_bytes`), both or neither. A cost bigger than a whole window of its quota can never be
  # admitted, so it is refused as `quota_too_small` rather than as a `rate_limited` the client would retry
  # forever. The quota charges the attempt: a produce the broker then refuses (overloaded, sealed, a key
  # outside its range) has spent its records and bytes all the same, since on the paths that can fail
  # after a partial write no refund could tell what landed. Unconfigured quotas, the default, cost five
  # config reads (each quota's limit and window, and whether limiting is on) and touch no table.
  defp charge_publish(session, records, bytes) do
    charges = [
      {:publish, RateLimiter.action_config(:publish), records},
      {:publish_bytes, RateLimiter.action_config(:publish_bytes), bytes}
    ]

    case RateLimiter.charge_in_caller(session.username, charges) do
      :ok ->
        :ok

      {:error, :rate_limit_exceeded, action, _retry_after_ms} ->
        Metrics.increment_rate_limit_blocked(action)
        {:error, :rate_limited}

      {:error, :cost_exceeds_limit, action} ->
        Metrics.increment_rate_limit_blocked(action)
        {:error, :quota_too_small}
    end
  end

  defp max_inflated_batch_bytes, do: Application.get_env(:malachi, :max_inflated_batch_bytes, 16_777_216)

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
      api_key == Wire.set_role_key() -> set_role(correlation_id, payload, session)
      api_key == Wire.list_users_with_roles_key() -> list_users_with_roles(correlation_id, payload, session)
      true -> dispatch_policy(api_key, correlation_id, payload, session)
    end
  end

  # Admin storage policy operations (#194), each gated by the :admin permission inside.
  defp dispatch_policy(api_key, correlation_id, payload, session) do
    cond do
      api_key == Wire.define_policy_key() -> define_policy(correlation_id, payload, session)
      api_key == Wire.delete_policy_key() -> delete_policy(correlation_id, payload, session)
      api_key == Wire.list_policies_key() -> list_policies(correlation_id, payload, session)
      api_key == Wire.bind_topic_policy_key() -> bind_topic_policy(correlation_id, payload, session)
      api_key == Wire.get_topic_policy_key() -> get_topic_policy(correlation_id, payload, session)
      true -> dispatch_routing(api_key, correlation_id, payload, session)
    end
  end

  # The routing and stream keys a routing client speaks (#275). `cluster_state` always answers: it is how a
  # client learns whether the rest are on. The rest wait for the `producer_streams` cluster flag
  # (`Malachi.Routing.flag/0`), which a node can only have once every node supports it.
  defp dispatch_routing(api_key, correlation_id, payload, session) do
    cond do
      api_key == Wire.cluster_state_key() ->
        cluster_state(correlation_id, payload)

      api_key in Wire.topic_routes_key()..Wire.commit_offsets_key() and not ClusterFlagsCache.enabled?(Routing.flag()) ->
        Wire.encode_error(correlation_id, :unsupported)

      api_key == Wire.topic_routes_key() ->
        topic_routes(correlation_id, payload, session)

      api_key == Wire.open_stream_key() ->
        open_stream(correlation_id, payload, session)

      api_key == Wire.open_consume_key() ->
        open_consume(correlation_id, payload, session)

      api_key == Wire.fetch_range_key() ->
        fetch_range(correlation_id, payload, session)

      # An append, read ack or close outside a connection that holds streams names a stream this connection
      # never opened.
      api_key in [Wire.append_key(), Wire.consume_ack_key(), Wire.close_stream_key()] ->
        Wire.encode_error(correlation_id, :unknown_stream)

      true ->
        Wire.encode_error(correlation_id, :unknown_api_key)
    end
  end

  # A producer stream on one range (`Malachi.ProducerStreams`). The client opens it against the routes it
  # read: routes that differ from the vnode's are `stale_routes`, and a range whose active segment another
  # node leads (or that a split or merge retired) is `moved`, so the client reads the routes again and
  # opens the stream where they say.
  defp open_stream(correlation_id, payload, session) do
    req = Wire.decode_open_stream_req(payload)

    with :ok <- open_allowed(session, req.topic),
         {:ok, %{version: version}} <- Routing.read_topic_routes(req.topic),
         :ok <- current_routes(version, req.routes_version),
         {:ok, token, segment_id, broker_pid} <-
           BrokerServer.open_stream(broker_for(req.topic), {req.topic, req.range}, self()),
         # opening can place the range's first segment, which changes the routes: the client is told the
         # version that names it
         {:ok, %{version: version}} <- Routing.read_topic_routes(req.topic) do
      {:opened, :producer, correlation_id,
       %{
         broker: broker_for(req.topic),
         broker_pid: broker_pid,
         topic: req.topic,
         range_id: {req.topic, req.range},
         token: token,
         segment_id: segment_id,
         routes_version: version,
         granted:
           StreamWindow.grant(
             req.window_appends,
             req.window_bytes,
             StreamWindow.max_appends(),
             StreamWindow.max_bytes()
           )
       }}
    else
      {:moved, _reason, _targets} -> Wire.encode_error(correlation_id, :moved)
      {:error, reason} -> Wire.encode_error(correlation_id, normalize(reason))
    end
  end

  defp open_allowed(session, topic) do
    if topic_allowed?(session, :produce, topic), do: :ok, else: {:error, :permission_denied}
  end

  # A consume stream on one range (`Malachi.ConsumeStreams`), opened where the range is served, against
  # the routes the client read, as a producer stream is. It spends one token of the subscribe quota, as a
  # subscribe does (`read_allowed/2` says when): the credit window, not a quota, bounds what it reads. The
  # pages go out with codec `none`, so a client that does not accept it is refused.
  defp open_consume(correlation_id, payload, session) do
    req = Wire.decode_open_consume_req(payload)

    with :ok <- read_allowed(session, req),
         {:ok, %{version: version}} <- Routing.read_topic_routes(req.topic),
         :ok <- current_routes(version, req.routes_version),
         :ok <- rate_limit_check(:subscribe, session),
         {:ok, token, position, broker_pid} <-
           BrokerServer.open_consume(broker_for(req.topic), {req.topic, req.range}, req.start, self()) do
      {:opened, :consumer, correlation_id,
       %{
         broker: broker_for(req.topic),
         broker_pid: broker_pid,
         topic: req.topic,
         range_id: {req.topic, req.range},
         token: token,
         position: position,
         window: stream_window(req.window),
         max: fetch_max(req.max),
         max_bytes: page_bytes(req.max_bytes),
         # checked again before every page, as an append is: an operator can revoke either while it is open
         allowed: fn -> consume_allowed(session, req.topic) end
       }}
    else
      {:moved, _reason, _targets} -> Wire.encode_error(correlation_id, :moved)
      {:error, reason} -> Wire.encode_error(correlation_id, normalize(reason))
    end
  end

  # One page of one range, read where the range is served, waiting up to `wait_ms` for records when there
  # are none past the start yet. It spends one subscribe token, as an `open_consume` does.
  defp fetch_range(correlation_id, payload, session) do
    case fetch_request(payload, session) do
      {:ok, fetch} ->
        reply = BrokerServer.fetch_range(fetch.broker, fetch.range_id, fetch.start, fetch.wait_ms)
        fetch_answer(correlation_id, fetch, reply)

      {:error, reason} ->
        Wire.encode_error(correlation_id, normalize(reason))
    end
  end

  # What a `fetch_range` asks of the session and the routes, and of the broker once those pass: the range,
  # its start, how long to wait and how big a page.
  defp fetch_request(payload, session) do
    req = Wire.decode_fetch_range_req(payload)

    with :ok <- read_allowed(session, req),
         {:ok, %{version: version}} <- Routing.read_topic_routes(req.topic),
         :ok <- current_routes(version, req.routes_version),
         :ok <- rate_limit_check(:subscribe, session) do
      {:ok,
       %{
         broker: broker_for(req.topic),
         range_id: {req.topic, req.range},
         start: req.start,
         wait_ms: fetch_wait(req.wait_ms),
         max: fetch_max(req.max),
         max_bytes: page_bytes(req.max_bytes)
       }}
    end
  end

  # The frame a `fetch_range` is answered with, from the broker's reply: the page read through the view it
  # handed over, or the refusal.
  defp fetch_answer(correlation_id, fetch, reply) do
    with {:ok, position, view, reporter} <- reply,
         {:ok, page, _count} <-
           ConsumeStreams.read_page(view, fetch.range_id, position, fetch.max, fetch.max_bytes, reporter) do
      Wire.encode_ok(correlation_id, IO.iodata_to_binary(Wire.encode_page(page)))
    else
      {:moved, _reason, _targets} -> Wire.encode_error(correlation_id, :moved)
      {:error, reason} -> Wire.encode_error(correlation_id, normalize(reason))
    end
  end

  # A `fetch_range` on a connection that holds streams: asked of the broker without waiting, so its wait does
  # not hold the connection's streams back, and answered when the broker replies
  # (`Malachi.ConnectionStreams.fetch/4`).
  defp fetch_range_async(correlation_id, payload, session, streams, socket, transport) do
    case fetch_request(payload, session) do
      {:ok, fetch} ->
        ConnectionStreams.fetch(streams, fetch.broker, {:fetch_range, fetch.range_id, fetch.start, fetch.wait_ms}, fn
          reply -> fetch_answer(correlation_id, fetch, reply)
        end)

      {:error, reason} ->
        transport.send(socket, Wire.encode_error(correlation_id, normalize(reason)))
        streams
    end
  end

  # What reading a range asks of the session and of the request before anything else: the consume
  # permission on the topic and a codec the pages can go out in. The subscribe token is spent after these
  # and the routes check, right before the broker is asked, so a request this node refuses on its own
  # spends none; a refusal from the broker (`moved`, a position it does not hold) has spent it.
  defp read_allowed(session, req) do
    cond do
      not topic_allowed?(session, :consume, req.topic) -> {:error, :permission_denied}
      :none not in req.accept -> {:error, :unsupported_codec}
      true -> :ok
    end
  end

  # Whether a consume stream on `topic` may still push: the flag and the consume permission.
  defp consume_allowed(session, topic) do
    cond do
      not ClusterFlagsCache.enabled?(Routing.flag()) -> {:error, :unsupported}
      not topic_allowed?(session, :consume, topic) -> {:error, :permission_denied}
      true -> :ok
    end
  end

  # The soft byte limit of a page, capped by the inflated batch size the server takes in.
  defp page_bytes(max_bytes), do: min(max_bytes, max_inflated_batch_bytes())

  defp current_routes(version, version), do: :ok
  defp current_routes(_current, _stale), do: {:error, :stale_routes}

  # Any authenticated session: the answer names brokers and vnodes, never a topic.
  defp cluster_state(correlation_id, payload) do
    :ok = Wire.decode_cluster_state_req(payload)

    case Routing.read_cluster_state() do
      {:ok, state} -> Wire.encode_ok(correlation_id, Wire.encode_cluster_state_resp(state))
      {:error, reason} -> Wire.encode_error(correlation_id, reason)
    end
  end

  # A producer and a consumer both need a topic's routes, so either permission on it is enough.
  defp topic_routes(correlation_id, payload, session) do
    topic = Wire.decode_topic_routes_req(payload)

    if topic_allowed?(session, :produce, topic) or topic_allowed?(session, :consume, topic) do
      case Routing.read_topic_routes(topic) do
        {:ok, routes} -> Wire.encode_ok(correlation_id, Wire.encode_topic_routes_resp(routes))
        {:error, reason} -> Wire.encode_error(correlation_id, reason)
      end
    else
      Wire.encode_error(correlation_id, :permission_denied)
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
      # this key carries no compression, so the bytes charged are the request's as received
      case charge_publish(session, length(records), byte_size(payload)) do
        :ok ->
          case LogApi.produce_records(broker_for(topic), topic, records) do
            {:ok, count} -> Wire.encode_ok(correlation_id, <<count::32>>)
            {:error, reason} -> Wire.encode_error(correlation_id, normalize(reason))
          end

        {:error, reason} ->
          Wire.encode_error(correlation_id, reason)
      end
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

  # Console roles (#228) over the wire: what a console role grants is decided by Malachi.Console.Access, but
  # managing it is user management, so it takes the wire :admin permission like every other user operation.
  # A version refusal (a cluster still below machine version 5) is answered with the upgrade message.
  defp set_role(correlation_id, payload, session) do
    with_permission(session, :admin, correlation_id, fn ->
      {username, role_string} = Wire.decode_set_role_req(payload)

      case ConsoleRole.parse(role_string) do
        {:ok, role} -> policy_reply(correlation_id, Malachi.Auth.set_role(username, role, session.username), <<>>)
        :error -> Wire.encode_error(correlation_id, :invalid_role)
      end
    end)
  end

  defp list_users_with_roles(correlation_id, _payload, session) do
    with_permission(session, :admin, correlation_id, fn ->
      Wire.encode_ok(correlation_id, Wire.encode_list_users_with_roles_resp(Malachi.Auth.list_users()))
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

  # --- admin storage policies: thin adapters over Malachi.Policies, which validates, audits and renders
  # every refusal (`Malachi.Policies.reason_string/1`), gated by the :admin permission. ---

  defp define_policy(correlation_id, payload, session) do
    with_permission(session, :admin, correlation_id, fn ->
      {name, fields} = Wire.decode_define_policy_req(payload)
      policy_reply(correlation_id, Policies.define(name, fields, session.username), <<>>)
    end)
  end

  defp delete_policy(correlation_id, payload, session) do
    with_permission(session, :admin, correlation_id, fn ->
      {name, force} = Wire.decode_delete_policy_req(payload)
      policy_reply(correlation_id, Policies.delete(name, session.username, force: force), <<>>)
    end)
  end

  defp list_policies(correlation_id, payload, session) do
    with_permission(session, :admin, correlation_id, fn ->
      :ok = Wire.decode_list_policies_req(payload)

      case Policies.list() do
        {:ok, policies} ->
          listed =
            policies |> Enum.sort_by(&elem(&1, 0)) |> Enum.map(fn {name, policy} -> {name, Policy.to_pairs(policy)} end)

          Wire.encode_ok(correlation_id, Wire.encode_list_policies_resp(listed))

        error ->
          policy_reply(correlation_id, error, <<>>)
      end
    end)
  end

  defp bind_topic_policy(correlation_id, payload, session) do
    with_permission(session, :admin, correlation_id, fn ->
      {topic, name} = Wire.decode_bind_topic_policy_req(payload)
      policy_reply(correlation_id, Policies.bind(topic, name, session.username), <<>>)
    end)
  end

  defp get_topic_policy(correlation_id, payload, session) do
    with_permission(session, :admin, correlation_id, fn ->
      case Policies.topic_policy(Wire.decode_get_topic_policy_req(payload)) do
        {:ok, topic_policy} ->
          resp = %{
            topic: topic_policy.topic,
            policy: topic_policy.policy,
            resolution: topic_policy.resolution,
            definition: topic_policy.definition && Policy.to_pairs(topic_policy.definition),
            effective: Policies.effective_pairs(topic_policy)
          }

          Wire.encode_ok(correlation_id, Wire.encode_topic_policy_resp(resp))

        error ->
          policy_reply(correlation_id, error, <<>>)
      end
    end)
  end

  defp policy_reply(correlation_id, :ok, ok_payload), do: Wire.encode_ok(correlation_id, ok_payload)

  defp policy_reply(correlation_id, {:error, reason}, _ok_payload),
    do: Wire.encode_error(correlation_id, Policies.reason_string(reason))

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
    if topic_allowed?(session, operation, topic),
      do: fun.(),
      else: Wire.encode_error(correlation_id, :permission_denied)
  end

  defp topic_allowed?(session, operation, topic) do
    strict? = Application.get_env(:malachi, :acl_strict, false)

    Authorization.allow?(session.permissions, operation, strict?, fn ->
      AclStore.authorized?(session.username, operation, topic)
    end)
  end

  # Runs `fun` (which returns a response frame) only if the session's user is within the configured limit
  # for `action`; otherwise a `rate_limited` error frame and a bump of the matching blocked counter, which
  # is what makes `rate_limit_blocked{action=...}` able to move at all.
  #
  # Ordering matters: this sits INSIDE the permission check, so a request the caller was never allowed to
  # make cannot spend tokens from the quota. Unconfigured is the default and the whole cost is two config
  # reads (the limit and the window). A subscribe spends one token; a produce is charged by its records and
  # bytes (`charge_publish/3`).
  #
  # The check runs in this connection's own process (`check_limit_in_caller/3`) rather than through the
  # limiter GenServer, which would put every connection in the system behind one process on the hottest
  # path. Its count is exact; what that door gives up is the token bucket's smoothing, so a client can
  # burst to 2x the limit across a window boundary. Windows follow the monotonic clock, so a system clock
  # step does not open a new one. See the limiter's own docs for the measurements.
  #
  # `retry_after_ms` is computed by the limiter but deliberately not carried on the wire: the error frame's
  # payload is a bare reason string, and `Malachi.Wire` freezes that encoding, so carrying it would take a
  # new api_key.
  defp with_rate_limit(action, session, correlation_id, fun) do
    case rate_limit_check(action, session) do
      :ok -> fun.()
      {:error, :rate_limited} -> Wire.encode_error(correlation_id, :rate_limited)
    end
  end

  defp rate_limit_check(action, session) do
    case RateLimiter.action_config(action) do
      nil ->
        :ok

      config ->
        case RateLimiter.check_limit_in_caller(session.username, action, config) do
          :ok ->
            :ok

          {:error, :rate_limit_exceeded, _retry_after_ms} ->
            Metrics.increment_rate_limit_blocked(action)
            {:error, :rate_limited}
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
