defmodule Malachi.Validator do
  @moduledoc """
  Input validation and sanitization for Malachi.

  Prevents injection attacks, atom table exhaustion, and resource exhaustion
  by enforcing strict rules on queue names, channel names, payloads, and headers.

  Uses an ETS cache to minimize validation overhead on hot paths.
  """
  use GenServer
  require Logger
  alias Malachi.I18n

  @cache_table :malachi_validated_names
  @throttle_table :malachi_validation_logs

  # Validation rules
  @queue_name_regex ~r/^[a-zA-Z0-9_\-\.]+$/
  @max_name_length 255
  @reserved_names ["system", "admin", "internal"]

  # Payload limits
  # 10MB
  @max_payload_size 10_485_760

  # Header limits
  @max_header_count 50
  @max_header_key_length 128
  @max_header_value_length 1024

  # Log throttling
  @log_rate_limit_per_min 10

  ## Client API

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc """
  Validates a queue name.

  Rules:
  - Must match regex: [a-zA-Z0-9_\\-\\.]+
  - Length: 1-255 characters
  - Cannot be reserved: system, admin, internal
  - Cannot start with underscore

  Returns :ok or {:error, atom_reason}

  ## Examples

      iex> Malachi.Validator.validate_queue_name("orders")
      :ok
      
      iex> Malachi.Validator.validate_queue_name("my queue")
      {:error, :invalid_queue_name_invalid_characters}
      
      iex> Malachi.Validator.validate_queue_name("system")
      {:error, :invalid_queue_name_reserved}
  """
  def validate_queue_name(name) when is_binary(name) do
    cache_key = {:validated_queue, name}

    case :ets.lookup(@cache_table, cache_key) do
      [{^cache_key, :ok}] ->
        increment_cache_hit()
        :ok

      [] ->
        increment_cache_miss()
        result = do_validate_name(name, :queue)

        if result == :ok do
          :ets.insert(@cache_table, {cache_key, :ok})
        end

        if match?({:error, _}, result) do
          log_validation_error(result, %{name: name, type: :queue})
        end

        result
    end
  end

  def validate_queue_name(_), do: {:error, :invalid_queue_name_not_string}

  @doc """
  Validates a channel name.

  Uses the same rules as queue names.
  """
  def validate_channel_name(name) when is_binary(name) do
    cache_key = {:validated_channel, name}

    case :ets.lookup(@cache_table, cache_key) do
      [{^cache_key, :ok}] ->
        increment_cache_hit()
        :ok

      [] ->
        increment_cache_miss()
        result = do_validate_name(name, :channel)

        if result == :ok do
          :ets.insert(@cache_table, {cache_key, :ok})
        end

        if match?({:error, _}, result) do
          log_validation_error(result, %{name: name, type: :channel})
        end

        result
    end
  end

  def validate_channel_name(_), do: {:error, :invalid_channel_name_not_string}

  @doc """
  Validates message size against queue's configured limit.

  This validation happens BEFORE constructing the Message struct to save memory
  on rejections. Called early in the publish pipeline after rate limiting.

  Returns :ok or {:error, {:message_too_large, actual_bytes, max_bytes}}

  ## Examples

      iex> Malachi.Validator.validate_message_size("hello", 1000)
      :ok
      
      iex> payload = :crypto.strong_rand_bytes(2000)
      iex> Malachi.Validator.validate_message_size(payload, 1000)
      {:error, {:message_too_large, 2000, 1000}}
  """
  def validate_message_size(payload, max_size) when is_binary(payload) do
    actual_size = byte_size(payload)

    if actual_size <= max_size do
      :ok
    else
      log_validation_error({:error, :message_too_large}, %{actual: actual_size, max: max_size})
      {:error, {:message_too_large, actual_size, max_size}}
    end
  end

  def validate_message_size(_, _), do: {:error, :invalid_payload_type}

  @doc """
  Validates payload size.

  Maximum size: 10MB (10,485,760 bytes)

  ## Examples

      iex> Malachi.Validator.validate_payload("small", 10_485_760)
      :ok
      
      iex> payload = :crypto.strong_rand_bytes(11_000_000)
      iex> Malachi.Validator.validate_payload(payload, 10_485_760)
      {:error, :payload_too_large}
  """
  def validate_payload(payload, max_size \\ @max_payload_size)

  def validate_payload(payload, max_size) when is_binary(payload) do
    if byte_size(payload) <= max_size do
      :ok
    else
      log_validation_error({:error, :payload_too_large}, %{size: byte_size(payload), max: max_size})
      {:error, :payload_too_large}
    end
  end

  def validate_payload(_, _), do: {:error, :invalid_payload_type}

  @doc """
  Validates message headers.

  Rules:
  - Must be a map
  - Maximum 50 entries
  - Keys must be strings, max 128 bytes
  - Values must be string (max 1024 bytes), number, or boolean
  - NO nested maps, arrays, or nil values

  ## Examples

      iex> Malachi.Validator.validate_headers(%{"priority" => 1})
      :ok
      
      iex> Malachi.Validator.validate_headers(%{"nested" => %{"x" => 1}})
      {:error, :invalid_headers_invalid_type}
  """
  def validate_headers(headers) when is_map(headers) do
    with :ok <- check_header_count(headers),
         :ok <- validate_header_keys(headers),
         :ok <- validate_header_values(headers) do
      :ok
    else
      {:error, _reason} = error ->
        log_validation_error(error, %{header_count: map_size(headers)})
        error
    end
  end

  def validate_headers(_), do: {:error, :invalid_headers_not_map}

  @doc """
  Sanitizes text for safe HTML output.

  Escapes: &, <, >, ", '

  ## Examples

      iex> Malachi.Validator.sanitize_for_html("<script>alert(1)</script>")
      "&lt;script&gt;alert(1)&lt;/script&gt;"
  """
  def sanitize_for_html(text) when is_binary(text) do
    text
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace("\"", "&quot;")
    |> String.replace("'", "&#x27;")
  end

  def sanitize_for_html(text), do: text

  @doc """
  Sanitizes text for safe log output.

  - Removes CR/LF characters (prevents CRLF injection)
  - Limits length to 1000 characters

  ## Examples

      iex> Malachi.Validator.sanitize_for_log("text\\r\\ninjected")
      "text  injected"
  """
  def sanitize_for_log(text) when is_binary(text) do
    text
    |> String.replace(~r/[\r\n\t]/, " ")
    |> String.slice(0, 1000)
  end

  def sanitize_for_log(text), do: text

  ## Server Callbacks

  @impl true
  def init(:ok) do
    # Create cache table for validated names
    :ets.new(@cache_table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    # Create throttle table for rate-limited logging
    :ets.new(@throttle_table, [
      :set,
      :public,
      :named_table,
      write_concurrency: true
    ])

    Logger.info(I18n.t(:validator_started))
    {:ok, %{}}
  end

  ## Private Helpers

  defp do_validate_name(name, type) do
    cond do
      String.length(name) == 0 ->
        {:error, validation_error(type, :empty)}

      String.length(name) > @max_name_length ->
        {:error, validation_error(type, :too_long)}

      not String.match?(name, @queue_name_regex) ->
        {:error, validation_error(type, :invalid_characters)}

      name in @reserved_names or String.starts_with?(name, "_") ->
        {:error, validation_error(type, :reserved)}

      true ->
        :ok
    end
  end

  # Pre-compiled atom constants — no dynamic atom creation from user input
  defp validation_error(:queue, :empty), do: :invalid_queue_name_empty
  defp validation_error(:queue, :too_long), do: :invalid_queue_name_too_long
  defp validation_error(:queue, :invalid_characters), do: :invalid_queue_name_invalid_characters
  defp validation_error(:queue, :reserved), do: :invalid_queue_name_reserved
  defp validation_error(:channel, :empty), do: :invalid_channel_name_empty
  defp validation_error(:channel, :too_long), do: :invalid_channel_name_too_long
  defp validation_error(:channel, :invalid_characters), do: :invalid_channel_name_invalid_characters
  defp validation_error(:channel, :reserved), do: :invalid_channel_name_reserved

  defp check_header_count(headers) do
    if map_size(headers) <= @max_header_count do
      :ok
    else
      {:error, :invalid_headers_too_many}
    end
  end

  defp validate_header_keys(headers) do
    Enum.reduce_while(headers, :ok, fn {key, _value}, _acc ->
      case validate_header_key(key) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp validate_header_key(key) when is_binary(key) do
    if byte_size(key) > 0 and byte_size(key) <= @max_header_key_length do
      :ok
    else
      {:error, :invalid_headers_key_too_long}
    end
  end

  defp validate_header_key(_key), do: {:error, :invalid_headers_key_invalid_type}

  defp validate_header_values(headers) do
    Enum.reduce_while(headers, :ok, fn {_key, value}, _acc ->
      case validate_header_value(value) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp validate_header_value(value) when is_binary(value) do
    if byte_size(value) <= @max_header_value_length do
      :ok
    else
      {:error, :invalid_headers_value_too_long}
    end
  end

  defp validate_header_value(value) when is_number(value), do: :ok
  defp validate_header_value(value) when is_boolean(value), do: :ok
  # Explicitly reject nil, maps, and lists (arrays)
  defp validate_header_value(nil), do: {:error, :invalid_headers_invalid_type}
  defp validate_header_value(value) when is_map(value), do: {:error, :invalid_headers_invalid_type}
  defp validate_header_value(value) when is_list(value), do: {:error, :invalid_headers_invalid_type}
  defp validate_header_value(_value), do: {:error, :invalid_headers_invalid_type}

  defp increment_cache_hit do
    Malachi.Metrics.increment_validation_cache_hit()
  rescue
    # Metrics may not be started yet
    _ -> :ok
  end

  defp increment_cache_miss do
    Malachi.Metrics.increment_validation_cache_miss()
  rescue
    _ -> :ok
  end

  defp log_validation_error({:error, reason}, context) do
    throttle_key = {:log_throttle, reason}
    now = System.system_time(:second)

    # Check if we should log (rate limiting)
    should_log =
      case :ets.lookup(@throttle_table, throttle_key) do
        [] ->
          true

        [{^throttle_key, timestamps}] ->
          # Remove timestamps older than 1 minute
          recent = Enum.filter(timestamps, fn ts -> now - ts < 60 end)
          length(recent) < @log_rate_limit_per_min
      end

    if should_log do
      # Update throttle table with new timestamp
      recent_timestamps = get_recent_timestamps(throttle_key, now)
      :ets.insert(@throttle_table, {throttle_key, [now | recent_timestamps]})

      # Log the error
      sanitized_context = sanitize_log_context(context)
      Logger.warning(I18n.t(:validation_error, reason: reason, context: inspect(sanitized_context)))

      # Increment metrics
      try do
        category = categorize_error(reason)
        Malachi.Metrics.increment_validation_error(category)
      rescue
        _ -> :ok
      end
    end
  end

  defp get_recent_timestamps(throttle_key, now) do
    case :ets.lookup(@throttle_table, throttle_key) do
      [{^throttle_key, timestamps}] ->
        Enum.filter(timestamps, fn ts -> now - ts < 60 end)

      [] ->
        []
    end
  end

  defp sanitize_log_context(context) do
    Map.new(context, fn {k, v} ->
      sanitized_value = if is_binary(v), do: sanitize_for_log(v), else: v
      {k, sanitized_value}
    end)
  end

  defp categorize_error(reason) do
    cond do
      reason |> Atom.to_string() |> String.starts_with?("invalid_queue_name") ->
        :invalid_queue_name

      reason |> Atom.to_string() |> String.starts_with?("invalid_channel_name") ->
        :invalid_channel_name

      reason |> Atom.to_string() |> String.starts_with?("payload") ->
        :payload_too_large

      reason |> Atom.to_string() |> String.starts_with?("invalid_headers") ->
        :invalid_headers

      true ->
        :other
    end
  end
end
