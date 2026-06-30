defmodule Malachi.TCPProtocol do
  @moduledoc """
  Implements the Malachi TCP/JSON protocol.

  Handles message parsing, response generation, and protocol logic for all
  client actions. Provides a clean separation between connection handling
  (TCPAcceptor) and protocol logic (this module).

  ## Protocol Format

  All messages are newline-delimited JSON. Responses follow a consistent format:

  - Success: `{"s":"ok"}` or `{"s":"ok", ...data}`
  - Error: `{"s":"err","reason":"..."}`
  - Warning: `{"s":"ok","warning":"..."}`

  ## Supported Actions

  ### Queue Operations
  - `publish` - Enqueue message (requires `:produce` permission)
  - `subscribe` - Subscribe to queue (requires `:consume` permission)
  - `ack` - Acknowledge message (requires `:consume` permission)
  - `nack` - Negative acknowledgment (requires `:consume` permission)

  ### Queue Management (Admin only)
  - `create_queue` - Create configured queue
  - `delete_queue` - Delete queue
  - `get_queue_info` - Get queue configuration and stats
  - `list_queues` - List all queues

  ### Channel Operations (Pub/Sub)
  - `channel_publish` - Publish to channel (requires `:produce` permission)
  - `channel_subscribe` - Subscribe to channel (requires `:consume` permission)
  - `get_channel_info` - Get channel statistics

  ## Examples

      # Publish message
      {"action": "publish", "queue_name": "orders", "payload": "data"}
      => {"s":"ok"}
      
      # Subscribe to queue
      {"action": "subscribe", "queue_name": "orders"}
      => {"s":"ok"}
      
      # Create queue with options
      {"action": "create_queue", "queue_name": "orders", "delivery_mode": "at_least_once"}
      => {"s":"ok", "config": {...}}
  """

  require Logger
  alias Malachi.I18n

  # ============================================================
  # PUBLIC API - Response Helpers
  # ============================================================

  @doc """
  Sends a simple success response.

  Returns: `{"s":"ok"}\\n`

  ## Examples

      send_ok(socket, :gen_tcp)
  """
  @compile {:inline, send_ok: 2}
  def send_ok(socket, transport) do
    send_response(socket, ~s({"s":"ok"}\n), transport)
  end

  @doc """
  Sends success response with additional data.

  The provided map is merged with `%{"s" => "ok"}` and JSON-encoded.

  ## Examples

      send_ok(socket, %{"token" => "abc123"}, transport)
      # => {"s":"ok","token":"abc123"}
      
      send_ok(socket, %{"queues" => [...]}, transport)
      # => {"s":"ok","queues":[...]}
  """
  def send_ok(socket, data, transport) when is_map(data) do
    response = Jason.encode!(Map.put(data, "s", "ok")) <> "\n"
    send_response(socket, response, transport)
  end

  @doc """
  Sends success with warning message.

  Used for operations that succeeded but have a non-critical issue.
  Returns: `{"s":"ok","warning":"reason"}\\n`

  ## Examples

      send_ok_warning(socket, :message_not_found, transport)
      # => {"s":"ok","warning":"message_not_found"}
  """
  def send_ok_warning(socket, reason, transport) when is_atom(reason) do
    response = Jason.encode!(%{"s" => "ok", "warning" => Atom.to_string(reason)}) <> "\n"
    send_response(socket, response, transport)
  end

  @doc """
  Sends error response with reason.

  Accepts both atom and string reasons for flexibility.
  Returns: `{"s":"err","reason":"..."}\\n`

  ## Examples

      send_error(socket, :permission_denied, transport)
      # => {"s":"err","reason":"permission_denied"}
      
      send_error(socket, "custom_error", transport)
      # => {"s":"err","reason":"custom_error"}
  """
  @compile {:inline, send_error: 3}
  def send_error(socket, reason, transport) when is_atom(reason) do
    json_msg = Jason.encode!(%{"s" => "err", "reason" => reason}) <> "\n"
    send_response(socket, json_msg, transport)
  end

  def send_error(socket, reason, transport) when is_binary(reason) do
    json_msg = Jason.encode!(%{"s" => "err", "reason" => reason}) <> "\n"
    send_response(socket, json_msg, transport)
  end

  @doc """
  Sends error response with reason and extra data.

  Used for errors that need additional context like retry_after_ms.
  Returns: `{"s":"err","reason":"...","extra_key":"extra_value"}\\n`

  ## Examples

      send_error(socket, :rate_limit_exceeded, %{"retry_after_ms" => 5000}, transport)
      # => {"s":"err","reason":"rate_limit_exceeded","retry_after_ms":5000}
  """
  def send_error(socket, reason, extra_data, transport) when is_map(extra_data) do
    base = %{"s" => "err", "reason" => to_string(reason)}
    json_msg = Jason.encode!(Map.merge(base, extra_data)) <> "\n"
    send_response(socket, json_msg, transport)
  end

  @doc """
  Sends a queue message to subscribed client.

  Used to push messages to consumers in active mode.

  ## Examples

      send_queue_message(socket, %{"id" => 1, "payload" => "data"}, transport)
      # => {"queue_message":{"id":1,"payload":"data"}}
  """
  def send_queue_message(socket, message, transport) do
    json_msg = Jason.encode!(%{"queue_message" => message}) <> "\n"
    send_response(socket, json_msg, transport)
  end

  @doc """
  Sends a channel message to subscribed client.

  Used to push messages to channel subscribers.

  ## Examples

      send_channel_message(socket, %{"payload" => "data"}, transport)
      # => {"channel_message":{"payload":"data"}}
  """
  def send_channel_message(socket, message, transport) do
    json_msg = Jason.encode!(%{"channel_message" => message}) <> "\n"
    send_response(socket, json_msg, transport)
  end

  @doc """
  Sends notification that client was kicked from channel.

  Informs the client they are no longer subscribed to the channel.

  ## Examples

      send_kicked_notification(socket, "notifications", transport)
      # => {"kicked_from_channel":"notifications"}
  """
  def send_kicked_notification(socket, channel_name, transport) do
    json_msg = Jason.encode!(%{"kicked_from_channel" => channel_name}) <> "\n"
    send_response(socket, json_msg, transport)
  end

  # ============================================================
  # PUBLIC API - Message Processing
  # ============================================================

  @doc """
  Processes an authenticated client request.

  This is the main entry point for protocol message handling. It decodes
  the JSON message and routes it to the appropriate action handler.

  ## Returns

  - `:ok` - Message processed successfully
  - `:subscribed` - Client subscribed to queue/channel (switches to active mode)

  ## Examples

      process_message(socket, ~s({"action":"publish",...}), session, transport)
      # => :ok
      
      process_message(socket, ~s({"action":"subscribe",...}), session, transport)
      # => :subscribed
  """
  @compile {:inline, process_message: 4}
  def process_message(socket, json_data, session, transport) do
    # Compatibility wrapper - call version with client_ip
    process_message(socket, json_data, session, transport, "unknown")
  end

  @doc """
  Process a JSON message from an authenticated client with client IP.

  Decodes JSON, validates permissions, and dispatches to appropriate handlers.
  Returns `:ok` for normal operations or `:subscribed` when client subscribes to a queue.

  ## Examples

      process_message(socket, ~s({"action":"publish",...}), session, transport, "192.168.1.1")
      # => :ok
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
  # PRIVATE - Action Handlers
  # ============================================================

  # Publish message to queue
  defp handle_action(
         socket,
         %{"action" => "publish", "queue_name" => q, "payload" => p} = msg,
         session,
         transport,
         _client_ip
       ) do
    with_permission(session, :produce, socket, transport, fn ->
      h = Map.get(msg, "headers", %{})

      # Check publish rate limit FIRST (before expensive validations)
      publish_limit = Application.get_env(:malachi, :publish_rate_limit, 1_000)
      publish_window_ms = Application.get_env(:malachi, :publish_rate_window_ms, 1_000)

      case Malachi.RateLimiter.check_limit(session.username, :publish, %{
             limit: publish_limit,
             window_ms: publish_window_ms
           }) do
        :ok ->
          # Validate inputs (message size first, before struct creation)
          config = Malachi.QueueConfig.get_config(q)
          max_size = config.max_message_size_bytes

          with :ok <- Malachi.Validator.validate_message_size(p, max_size),
               :ok <- Malachi.Validator.validate_queue_name(q),
               :ok <- Malachi.Validator.validate_payload(p),
               :ok <- Malachi.Validator.validate_headers(h) do
            case Malachi.Queue.enqueue(q, p, h) do
              {:ok, _message} ->
                Malachi.ConnectionRegistry.set_connection_type(self(), :producer, q)
                response_data = build_backpressure_response(q)
                send_ok(socket, response_data, transport)
                :ok

              {:error, :queue_full} ->
                send_error(socket, :queue_full, transport)
                :ok

              {:error, {:producer_blocked, timeout_ms}} ->
                send_error(socket, :producer_blocked, %{"timeout_ms" => timeout_ms}, transport)
                :ok

              {:error, reason} ->
                send_error(socket, reason, transport)
                :ok
            end
          else
            {:error, {:message_too_large, actual, max}} ->
              send_error(socket, :message_too_large, %{"actual_bytes" => actual, "max_bytes" => max}, transport)
              :ok

            {:error, reason} when is_atom(reason) ->
              send_error(socket, reason, transport)
              :ok
          end

        {:error, :rate_limit_exceeded, retry_after_ms} ->
          Malachi.Metrics.increment_rate_limit_blocked(:publish)
          send_error(socket, :rate_limit_exceeded, %{"retry_after_ms" => retry_after_ms}, transport)
          :ok
      end
    end)
  end

  # Subscribe to queue
  defp handle_action(socket, %{"action" => "subscribe", "queue_name" => q}, session, transport, _client_ip) do
    with_permission(session, :consume, socket, transport, fn ->
      # Validate queue name
      case Malachi.Validator.validate_queue_name(q) do
        :ok ->
          # Check subscribe rate limit
          subscribe_limit = Application.get_env(:malachi, :subscribe_rate_limit, 100)
          subscribe_window_ms = Application.get_env(:malachi, :subscribe_rate_window_ms, 60_000)

          case Malachi.RateLimiter.check_limit(session.username, :subscribe, %{
                 limit: subscribe_limit,
                 window_ms: subscribe_window_ms
               }) do
            :ok ->
              Malachi.Queue.subscribe(q, self())
              Malachi.ConnectionRegistry.set_connection_type(self(), :consumer, q)
              send_ok(socket, transport)
              :subscribed

            {:error, :rate_limit_exceeded, retry_after_ms} ->
              Malachi.Metrics.increment_rate_limit_blocked(:subscribe)
              send_error(socket, :rate_limit_exceeded, %{"retry_after_ms" => retry_after_ms}, transport)
              :ok
          end

        {:error, reason} when is_atom(reason) ->
          send_error(socket, reason, transport)
          :ok
      end
    end)
  end

  # Shorthand publish (without explicit "action" field)
  defp handle_action(socket, %{"queue_name" => q, "payload" => p} = msg, session, transport, _client_ip) do
    with_permission(session, :produce, socket, transport, fn ->
      h = Map.get(msg, "headers", %{})

      # Check publish rate limit FIRST (before expensive validations)
      publish_limit = Application.get_env(:malachi, :publish_rate_limit, 1_000)
      publish_window_ms = Application.get_env(:malachi, :publish_rate_window_ms, 1_000)

      case Malachi.RateLimiter.check_limit(session.username, :publish, %{
             limit: publish_limit,
             window_ms: publish_window_ms
           }) do
        :ok ->
          # Validate inputs (message size first, before struct creation)
          config = Malachi.QueueConfig.get_config(q)
          max_size = config.max_message_size_bytes

          with :ok <- Malachi.Validator.validate_message_size(p, max_size),
               :ok <- Malachi.Validator.validate_queue_name(q),
               :ok <- Malachi.Validator.validate_payload(p),
               :ok <- Malachi.Validator.validate_headers(h) do
            case Malachi.Queue.enqueue(q, p, h) do
              {:ok, _message} ->
                Malachi.ConnectionRegistry.set_connection_type(self(), :producer, q)
                response_data = build_backpressure_response(q)
                send_ok(socket, response_data, transport)
                :ok

              {:error, :queue_full} ->
                send_error(socket, :queue_full, transport)
                :ok

              {:error, {:producer_blocked, timeout_ms}} ->
                send_error(socket, :producer_blocked, %{"timeout_ms" => timeout_ms}, transport)
                :ok

              {:error, reason} ->
                send_error(socket, reason, transport)
                :ok
            end
          else
            {:error, {:message_too_large, actual, max}} ->
              send_error(socket, :message_too_large, %{"actual_bytes" => actual, "max_bytes" => max}, transport)
              :ok

            {:error, reason} when is_atom(reason) ->
              send_error(socket, reason, transport)
              :ok
          end

        {:error, :rate_limit_exceeded, retry_after_ms} ->
          Malachi.Metrics.increment_rate_limit_blocked(:publish)
          send_error(socket, :rate_limit_exceeded, %{"retry_after_ms" => retry_after_ms}, transport)
          :ok
      end
    end)
  end

  # Acknowledge message
  defp handle_action(socket, %{"action" => "ack", "message_id" => message_id}, session, transport, _client_ip) do
    with_permission(session, :consume, socket, transport, fn ->
      case Malachi.AckManager.ack(message_id) do
        :ok ->
          send_ok(socket, transport)

        {:error, :not_found} ->
          send_ok_warning(socket, :message_not_found, transport)
      end

      :ok
    end)
  end

  # Negative acknowledgment
  defp handle_action(socket, %{"action" => "nack", "message_id" => message_id} = msg, session, transport, _client_ip) do
    with_permission(session, :consume, socket, transport, fn ->
      requeue = Map.get(msg, "requeue", true)

      case Malachi.AckManager.nack(message_id, requeue: requeue) do
        :ok ->
          send_ok(socket, transport)

        {:error, :not_found} ->
          send_ok_warning(socket, :message_not_found, transport)
      end

      :ok
    end)
  end

  # Create queue with configuration
  defp handle_action(socket, %{"action" => "create_queue", "queue_name" => q} = msg, session, transport, _client_ip) do
    with_permission(session, :admin, socket, transport, fn ->
      case build_queue_options(msg) do
        {:ok, opts} ->
          case Malachi.QueueConfig.create_queue(q, opts) do
            {:ok, config} ->
              response_data = %{
                "config" => %{
                  "queue_name" => config.queue_name,
                  "delivery_mode" => to_string(config.delivery_mode),
                  "max_retries" => config.max_retries,
                  "dlq_enabled" => config.dlq_enabled
                }
              }

              send_ok(socket, response_data, transport)

            {:error, :queue_already_exists} ->
              send_error(socket, :queue_already_exists, transport)

            {:error, reason} ->
              send_error(socket, reason, transport)
          end

        {:error, reason} ->
          send_error(socket, reason, transport)
      end

      :ok
    end)
  end

  # Delete queue
  defp handle_action(socket, %{"action" => "delete_queue", "queue_name" => q} = msg, session, transport, _client_ip) do
    with_permission(session, :admin, socket, transport, fn ->
      force = Map.get(msg, "force", false)

      case Malachi.QueueConfig.delete_queue(q, force: force) do
        :ok ->
          send_ok(socket, transport)

        {:error, :queue_not_found} ->
          send_error(socket, :queue_not_found, transport)

        {:error, :queue_has_active_consumers} ->
          send_error(socket, :queue_has_active_consumers, transport)

        {:error, :queue_has_buffered_messages} ->
          send_error(socket, :queue_has_buffered_messages, transport)
      end

      :ok
    end)
  end

  # Get queue information
  defp handle_action(socket, %{"action" => "get_queue_info", "queue_name" => q}, session, transport, _client_ip) do
    # Any authenticated user can view queue info
    if has_any_permission?(session, [:admin, :produce, :consume]) do
      config = Malachi.QueueConfig.get_config(q)
      stats = Malachi.Queue.get_stats(q)
      {:ok, pressure_info} = Malachi.Backpressure.get_queue_pressure(q)

      buffer_utilization =
        if config.max_buffer_size > 0 do
          Float.round(stats.buffered / config.max_buffer_size * 100, 2)
        else
          0.0
        end

      response_data = %{
        "queue_info" => %{
          "queue_name" => config.queue_name,
          "delivery_mode" => to_string(config.delivery_mode),
          "max_retries" => config.max_retries,
          "dlq_enabled" => config.dlq_enabled,
          "implicit" => Map.get(config, :implicit, false),
          # Backpressure configuration
          "max_buffer_size" => config.max_buffer_size,
          "overflow_behavior" => to_string(config.overflow_behavior),
          "backpressure_threshold" => config.backpressure_threshold,
          "block_timeout_ms" => config.block_timeout_ms,
          "max_blocked_producers" => config.max_blocked_producers,
          "max_message_size_bytes" => config.max_message_size_bytes,
          "stats" => %{
            "consumers" => stats.consumers,
            "producers" => stats.producers,
            "buffered" => stats.buffered,
            "partition" => stats.partition,
            # Backpressure statistics
            "blocked_producers_count" => Map.get(stats, :blocked_producers_count, 0),
            "buffer_utilization_pct" => buffer_utilization,
            "backpressure_status" => to_string(pressure_info.status),
            "backpressure_active" => Malachi.Backpressure.should_apply_backpressure?(pressure_info)
          }
        }
      }

      send_ok(socket, response_data, transport)
      :ok
    else
      Logger.warning(I18n.t(:permission_denied, username: session.username, action: "get_queue_info"))
      send_error(socket, :permission_denied, transport)
      :ok
    end
  end

  # List all queues
  defp handle_action(socket, %{"action" => "list_queues"}, session, transport, _client_ip) do
    with_permission(session, :admin, socket, transport, fn ->
      queues = Malachi.QueueConfig.list_queues()

      queue_list =
        Enum.map(queues, fn config ->
          %{
            "queue_name" => config.queue_name,
            "delivery_mode" => to_string(config.delivery_mode),
            "max_retries" => config.max_retries,
            "dlq_enabled" => config.dlq_enabled,
            "implicit" => Map.get(config, :implicit, false)
          }
        end)

      send_ok(socket, %{"queues" => queue_list}, transport)
      :ok
    end)
  end

  # Publish to channel
  defp handle_action(
         socket,
         %{"action" => "channel_publish", "channel_name" => ch, "payload" => p} = msg,
         session,
         transport,
         _client_ip
       ) do
    with_permission(session, :produce, socket, transport, fn ->
      h = Map.get(msg, "headers", %{})

      # Validate inputs
      with :ok <- Malachi.Validator.validate_channel_name(ch),
           :ok <- Malachi.Validator.validate_payload(p),
           :ok <- Malachi.Validator.validate_headers(h) do
        # Check publish rate limit (unified with queue publish)
        publish_limit = Application.get_env(:malachi, :publish_rate_limit, 1_000)
        publish_window_ms = Application.get_env(:malachi, :publish_rate_window_ms, 1_000)

        case Malachi.RateLimiter.check_limit(session.username, :publish, %{
               limit: publish_limit,
               window_ms: publish_window_ms
             }) do
          :ok ->
            Malachi.Channel.publish(ch, p, h)
            Malachi.ConnectionRegistry.set_connection_type(self(), :channel_publisher, ch)
            send_ok(socket, transport)
            :ok

          {:error, :rate_limit_exceeded, retry_after_ms} ->
            Malachi.Metrics.increment_rate_limit_blocked(:publish)
            send_error(socket, :rate_limit_exceeded, %{"retry_after_ms" => retry_after_ms}, transport)
            :ok
        end
      else
        {:error, reason} when is_atom(reason) ->
          send_error(socket, reason, transport)
          :ok
      end
    end)
  end

  # Subscribe to channel
  defp handle_action(socket, %{"action" => "channel_subscribe", "channel_name" => ch}, session, transport, _client_ip) do
    with_permission(session, :consume, socket, transport, fn ->
      # Validate channel name
      case Malachi.Validator.validate_channel_name(ch) do
        :ok ->
          # Check subscribe rate limit (unified with queue subscribe)
          subscribe_limit = Application.get_env(:malachi, :subscribe_rate_limit, 100)
          subscribe_window_ms = Application.get_env(:malachi, :subscribe_rate_window_ms, 60_000)

          case Malachi.RateLimiter.check_limit(session.username, :subscribe, %{
                 limit: subscribe_limit,
                 window_ms: subscribe_window_ms
               }) do
            :ok ->
              Malachi.Channel.subscribe(ch, self())
              Malachi.ConnectionRegistry.set_connection_type(self(), :channel_subscriber, ch)
              send_ok(socket, transport)
              :subscribed

            {:error, :rate_limit_exceeded, retry_after_ms} ->
              Malachi.Metrics.increment_rate_limit_blocked(:subscribe)
              send_error(socket, :rate_limit_exceeded, %{"retry_after_ms" => retry_after_ms}, transport)
              :ok
          end

        {:error, reason} when is_atom(reason) ->
          send_error(socket, reason, transport)
          :ok
      end
    end)
  end

  # Get channel information
  defp handle_action(socket, %{"action" => "get_channel_info", "channel_name" => ch}, session, transport, _client_ip) do
    # Any authenticated user can view channel info
    if has_any_permission?(session, [:admin, :produce, :consume]) do
      stats = Malachi.Channel.get_stats(ch)
      metrics = Malachi.Metrics.get_channel_metrics(ch)

      response_data = %{
        "channel_info" => %{
          "channel_name" => ch,
          "exists" => stats.exists,
          "subscribers" => stats.subscribers,
          "published" => metrics.published,
          "delivered" => metrics.delivered,
          "dropped" => metrics.dropped,
          "uptime" => Map.get(stats, :uptime, 0)
        }
      }

      send_ok(socket, response_data, transport)
      :ok
    else
      Logger.warning(I18n.t(:permission_denied, username: session.username, action: "get_channel_info"))
      send_error(socket, :permission_denied, transport)
      :ok
    end
  end

  # Update queue configuration
  defp handle_action(
         socket,
         %{"action" => "update_queue_config", "queue_name" => q} = msg,
         session,
         transport,
         _client_ip
       ) do
    with_permission(session, :admin, socket, transport, fn ->
      force = Map.get(msg, "force", false)

      # Parse overflow_behavior if provided
      case parse_overflow_behavior(Map.get(msg, "overflow_behavior")) do
        {:ok, updates} ->
          # Add other optional fields
          updates =
            updates
            |> maybe_add_field(msg, "max_buffer_size", :max_buffer_size)
            |> maybe_add_field(msg, "backpressure_threshold", :backpressure_threshold)
            |> maybe_add_field(msg, "block_timeout_ms", :block_timeout_ms)
            |> maybe_add_field(msg, "max_blocked_producers", :max_blocked_producers)
            |> maybe_add_field(msg, "max_message_size_bytes", :max_message_size_bytes)

          case Malachi.QueueConfig.update_queue(q, updates, force: force) do
            {:ok, :updated} ->
              send_ok(socket, %{"status" => "updated"}, transport)
              :ok

            {:ok, :updated_with_warning, excess} ->
              send_ok(
                socket,
                %{
                  "status" => "updated_with_warning",
                  "warning" => "Queue buffer size exceeds new limit",
                  "excess_messages" => excess
                },
                transport
              )

              :ok

            {:ok, :forced_update, excess} ->
              send_ok(
                socket,
                %{
                  "status" => "forced_update",
                  "message" => "Configuration updated with force flag",
                  "excess_messages" => excess
                },
                transport
              )

              :ok

            {:error, :buffer_exceeds_new_limit, details} ->
              send_error(
                socket,
                :update_rejected,
                %{
                  "reason" => "Buffer size reduction would exceed threshold",
                  "current_buffer_size" => details.current_buffer_size,
                  "new_max_buffer_size" => details.new_max_buffer_size,
                  "excess_messages" => details.excess,
                  "threshold_pct" => details.threshold_pct,
                  "suggestion" => details.suggestion
                },
                transport
              )

              :ok

            {:error, :queue_not_found} ->
              send_error(socket, :queue_not_found, transport)
              :ok
          end

        {:error, :invalid_overflow_behavior} ->
          send_error(socket, :invalid_overflow_behavior, transport)
          :ok
      end
    end)
  end

  # Get queue configuration (detailed view)
  defp handle_action(socket, %{"action" => "get_queue_config", "queue_name" => q}, session, transport, _client_ip) do
    with_permission(session, :admin, socket, transport, fn ->
      case Malachi.QueueConfig.queue_exists?(q) do
        true ->
          config = Malachi.QueueConfig.get_config(q)
          stats = Malachi.Queue.get_stats(q)
          {:ok, pressure_info} = Malachi.Backpressure.get_queue_pressure(q)

          buffer_utilization =
            if config.max_buffer_size > 0 do
              Float.round(stats.buffered / config.max_buffer_size * 100, 2)
            else
              0.0
            end

          response_data = %{
            "config" => %{
              "queue_name" => config.queue_name,
              "delivery_mode" => to_string(config.delivery_mode),
              "max_retries" => config.max_retries,
              "dlq_enabled" => config.dlq_enabled,
              "max_buffer_size" => config.max_buffer_size,
              "overflow_behavior" => to_string(config.overflow_behavior),
              "backpressure_threshold" => config.backpressure_threshold,
              "block_timeout_ms" => config.block_timeout_ms,
              "max_blocked_producers" => config.max_blocked_producers,
              "max_message_size_bytes" => config.max_message_size_bytes,
              "implicit" => Map.get(config, :implicit, false)
            },
            "stats" => %{
              "consumers" => stats.consumers,
              "producers" => stats.producers,
              "buffered" => stats.buffered,
              "blocked_producers_count" => Map.get(stats, :blocked_producers_count, 0),
              "buffer_utilization_pct" => buffer_utilization,
              "backpressure_status" => to_string(pressure_info.status),
              "backpressure_active" => Malachi.Backpressure.should_apply_backpressure?(pressure_info)
            }
          }

          send_ok(socket, response_data, transport)
          :ok

        false ->
          send_error(socket, :queue_not_found, transport)
          :ok
      end
    end)
  end

  # List available protocol actions
  defp handle_action(socket, %{"action" => "list_actions"}, session, transport, _client_ip) do
    # Any authenticated user can list actions
    if has_any_permission?(session, [:admin, :produce, :consume]) do
      actions = [
        %{
          "action" => "publish",
          "permission" => "produce",
          "description" => "Publish message to queue",
          "required" => ["queue_name", "payload"],
          "optional" => ["headers"]
        },
        %{
          "action" => "subscribe",
          "permission" => "consume",
          "description" => "Subscribe to queue messages",
          "required" => ["queue_name"],
          "optional" => []
        },
        %{
          "action" => "ack",
          "permission" => "consume",
          "description" => "Acknowledge message receipt",
          "required" => ["message_id"],
          "optional" => []
        },
        %{
          "action" => "nack",
          "permission" => "consume",
          "description" => "Negative acknowledge with requeue option",
          "required" => ["message_id"],
          "optional" => ["requeue"]
        },
        %{
          "action" => "create_queue",
          "permission" => "admin",
          "description" => "Create new queue with configuration",
          "required" => ["queue_name"],
          "optional" => [
            "delivery_mode",
            "max_retries",
            "dlq_enabled",
            "max_buffer_size",
            "overflow_behavior",
            "backpressure_threshold",
            "block_timeout_ms",
            "max_blocked_producers",
            "max_message_size_bytes"
          ]
        },
        %{
          "action" => "delete_queue",
          "permission" => "admin",
          "description" => "Delete queue",
          "required" => ["queue_name"],
          "optional" => ["force"]
        },
        %{
          "action" => "get_queue_info",
          "permission" => "admin|produce|consume",
          "description" => "Get queue information and statistics",
          "required" => ["queue_name"],
          "optional" => []
        },
        %{
          "action" => "get_queue_config",
          "permission" => "admin",
          "description" => "Get detailed queue configuration",
          "required" => ["queue_name"],
          "optional" => []
        },
        %{
          "action" => "update_queue_config",
          "permission" => "admin",
          "description" => "Update queue configuration dynamically",
          "required" => ["queue_name"],
          "optional" => [
            "max_buffer_size",
            "overflow_behavior",
            "backpressure_threshold",
            "block_timeout_ms",
            "max_blocked_producers",
            "max_message_size_bytes",
            "force"
          ]
        },
        %{
          "action" => "list_queues",
          "permission" => "admin",
          "description" => "List all queues",
          "required" => [],
          "optional" => []
        },
        %{
          "action" => "list_actions",
          "permission" => "admin|produce|consume",
          "description" => "List available protocol actions",
          "required" => [],
          "optional" => []
        },
        %{
          "action" => "channel_publish",
          "permission" => "produce",
          "description" => "Publish to channel (pub/sub)",
          "required" => ["channel_name", "payload"],
          "optional" => ["headers"]
        },
        %{
          "action" => "channel_subscribe",
          "permission" => "consume",
          "description" => "Subscribe to channel",
          "required" => ["channel_name"],
          "optional" => []
        },
        %{
          "action" => "get_channel_info",
          "permission" => "admin|produce|consume",
          "description" => "Get channel information",
          "required" => ["channel_name"],
          "optional" => []
        }
      ]

      send_ok(socket, %{"actions" => actions}, transport)
      :ok
    else
      Logger.warning(I18n.t(:permission_denied, username: session.username, action: "list_actions"))
      send_error(socket, :permission_denied, transport)
      :ok
    end
  end

  # Invalid request (catch-all)
  # --- NorthGuard log protocol (topic + key + opaque cursor; no partitions/offsets exposed) ---

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

      result =
        cond do
          # An explicit cursor (client-managed paging) takes precedence over a group resume. Any
          # present, non-null cursor is forwarded to `fetch`, which validates it and returns
          # `:invalid_cursor` for a malformed token (rather than silently restarting the stream).
          msg["cursor"] != nil -> Malachi.LogApi.fetch(Malachi.LogBroker, topic, msg["cursor"], max)
          is_binary(msg["group"]) -> Malachi.LogApi.fetch_group(Malachi.LogBroker, topic, msg["group"], max)
          true -> Malachi.LogApi.fetch(Malachi.LogBroker, topic, :start, max)
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

  defp has_any_permission?(session, permissions) do
    Enum.any?(permissions, fn perm ->
      Malachi.Auth.has_permission?(session.permissions, perm)
    end)
  end

  # --- log protocol helpers ---

  # send_error wants an atom or binary; tuple reasons (e.g. {:unroutable, key}) are made JSON-safe.
  defp normalize_reason(reason) when is_atom(reason) or is_binary(reason), do: reason
  defp normalize_reason(reason), do: inspect(reason)

  # Bound the client-supplied page size: a positive integer, capped, defaulting to 100.
  defp fetch_max(max) when is_integer(max) and max > 0, do: min(max, 1_000)
  defp fetch_max(_max), do: 100

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

  defp maybe_add_field(map, msg, json_key, atom_key) do
    case Map.get(msg, json_key) do
      nil -> map
      value -> Map.put(map, atom_key, value)
    end
  end

  # Helper for adding optional fields to keyword list
  defp add_optional_field(opts, msg, json_key, atom_key) do
    case Map.get(msg, json_key) do
      nil -> opts
      value -> Keyword.put(opts, atom_key, value)
    end
  end

  # Parse overflow_behavior string to atom
  defp parse_overflow_behavior(nil), do: {:ok, %{}}
  defp parse_overflow_behavior("drop_newest"), do: {:ok, %{overflow_behavior: :drop_newest}}
  defp parse_overflow_behavior("drop_oldest"), do: {:ok, %{overflow_behavior: :drop_oldest}}
  defp parse_overflow_behavior("reject"), do: {:ok, %{overflow_behavior: :reject}}
  defp parse_overflow_behavior("block"), do: {:ok, %{overflow_behavior: :block}}
  defp parse_overflow_behavior(_), do: {:error, :invalid_overflow_behavior}

  # Build backpressure response data
  defp build_backpressure_response(queue_name) do
    {:ok, pressure_info} = Malachi.Backpressure.get_queue_pressure(queue_name)
    backpressure = Malachi.Backpressure.should_apply_backpressure?(pressure_info)

    if backpressure do
      %{"backpressure" => true, "pressure_status" => to_string(pressure_info.status)}
    else
      %{}
    end
  end

  # Parse delivery mode from JSON
  defp parse_delivery_mode("at_most_once"), do: {:ok, :at_most_once}
  defp parse_delivery_mode("at_least_once"), do: {:ok, :at_least_once}
  defp parse_delivery_mode(nil), do: {:ok, :at_least_once}
  defp parse_delivery_mode(_), do: {:error, :invalid_delivery_mode}

  # Parse overflow behavior from JSON
  defp parse_queue_overflow_behavior("drop_newest"), do: {:ok, :drop_newest}
  defp parse_queue_overflow_behavior("drop_oldest"), do: {:ok, :drop_oldest}
  defp parse_queue_overflow_behavior("reject"), do: {:ok, :reject}
  defp parse_queue_overflow_behavior("block"), do: {:ok, :block}
  defp parse_queue_overflow_behavior(nil), do: {:ok, nil}
  defp parse_queue_overflow_behavior(_), do: {:error, :invalid_overflow_behavior}

  defp build_queue_options(msg) do
    with {:ok, delivery_mode} <- parse_delivery_mode(Map.get(msg, "delivery_mode")),
         {:ok, overflow_behavior} <- parse_queue_overflow_behavior(Map.get(msg, "overflow_behavior")) do
      opts = [delivery_mode: delivery_mode]

      # Add all optional fields
      opts =
        [
          {:max_retries, "max_retries"},
          {:dlq_enabled, "dlq_enabled"},
          {:max_buffer_size, "max_buffer_size"},
          {:backpressure_threshold, "backpressure_threshold"},
          {:block_timeout_ms, "block_timeout_ms"},
          {:max_blocked_producers, "max_blocked_producers"},
          {:max_message_size_bytes, "max_message_size_bytes"}
        ]
        |> Enum.reduce(opts, fn {key, json_key}, acc ->
          add_optional_field(acc, msg, json_key, key)
        end)

      # Add overflow_behavior if present
      opts = if overflow_behavior, do: Keyword.put(opts, :overflow_behavior, overflow_behavior), else: opts

      {:ok, opts}
    end
  end

  # ============================================================
  # PRIVATE - Socket Operations
  # ============================================================

  @compile {:inline, send_response: 3}
  defp send_response(socket, data, :ssl), do: :ssl.send(socket, data)
  defp send_response(socket, data, :gen_tcp), do: :gen_tcp.send(socket, data)
  defp send_response(socket, data, transport) when is_atom(transport), do: transport.send(socket, data)
end
