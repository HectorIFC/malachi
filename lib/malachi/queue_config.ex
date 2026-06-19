defmodule Malachi.QueueConfig do
  @moduledoc """
  Manages queue configuration and metadata.
  Stores delivery mode, max retries, and DLQ settings per queue.
  """
  use GenServer
  require Logger
  alias Malachi.I18n

  @config_table :malachi_queue_config

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc """
  Creates or updates queue configuration.
  Returns {:ok, config} or {:error, reason}.

  ## Options

    * `:delivery_mode` - :at_least_once (default) or :at_most_once
    * `:max_retries` - Number of retry attempts before DLQ (default: 3)
    * `:dlq_enabled` - Enable dead letter queue (default: true)
    * `:max_message_size_bytes` - Maximum message size in bytes (default: 1MB)
    * `:max_buffer_size` - Maximum buffered messages (default: 10,000)
    * `:overflow_behavior` - :drop_newest | :drop_oldest | :reject | :block (default: :drop_newest)
    * `:backpressure_threshold` - Pressure signal threshold 0.0-1.0 (default: 0.8)
    * `:block_timeout_ms` - Timeout for :block strategy (default: 5000)
    * `:max_blocked_producers` - Max blocked producers (default: 1000)
  """
  def create_queue(queue_name, opts \\ []) do
    # Enforce dynamic queue limit to prevent resource exhaustion
    max_queues = Application.get_env(:malachi, :max_dynamic_queues, 10_000)
    current_count = :ets.info(@config_table, :size)

    if current_count >= max_queues do
      {:error, :max_queues_reached}
    else
      do_create_queue(queue_name, opts)
    end
  end

  defp do_create_queue(queue_name, opts) do
    delivery_mode = Keyword.get(opts, :delivery_mode, get_default_delivery_mode())
    max_retries = Keyword.get(opts, :max_retries, 3)
    dlq_enabled = Keyword.get(opts, :dlq_enabled, true)

    # Backpressure configuration
    max_message_size_bytes = Keyword.get(opts, :max_message_size_bytes, get_default_max_message_size())
    max_buffer_size = Keyword.get(opts, :max_buffer_size, get_default_max_buffer_size())
    overflow_behavior = Keyword.get(opts, :overflow_behavior, get_default_overflow_behavior())
    backpressure_threshold = Keyword.get(opts, :backpressure_threshold, get_default_backpressure_threshold())
    block_timeout_ms = Keyword.get(opts, :block_timeout_ms, get_default_block_timeout())
    max_blocked_producers = Keyword.get(opts, :max_blocked_producers, get_default_max_blocked_producers())

    with :ok <- validate_delivery_mode(delivery_mode),
         :ok <- validate_overflow_behavior(overflow_behavior) do
      config = %{
        queue_name: queue_name,
        delivery_mode: delivery_mode,
        max_retries: max_retries,
        dlq_enabled: dlq_enabled,
        max_message_size_bytes: max_message_size_bytes,
        max_buffer_size: max_buffer_size,
        overflow_behavior: overflow_behavior,
        backpressure_threshold: backpressure_threshold,
        block_timeout_ms: block_timeout_ms,
        max_blocked_producers: max_blocked_producers,
        created_at: System.system_time(:second)
      }

      case :ets.lookup(@config_table, queue_name) do
        [] ->
          :ets.insert(@config_table, {queue_name, config})
          Logger.info(I18n.t(:queue_created, queue: queue_name, mode: delivery_mode))
          {:ok, config}

        [{^queue_name, _existing}] ->
          {:error, :queue_already_exists}
      end
    end
  end

  @doc """
  Gets queue configuration.
  Returns config map or creates default if not exists.
  """
  def get_config(queue_name) do
    case :ets.lookup(@config_table, queue_name) do
      [{^queue_name, config}] ->
        config

      [] ->
        # Create implicit queue with defaults
        delivery_mode = get_default_delivery_mode()

        config = %{
          queue_name: queue_name,
          delivery_mode: delivery_mode,
          max_retries: 3,
          dlq_enabled: true,
          max_message_size_bytes: get_default_max_message_size(),
          max_buffer_size: get_default_max_buffer_size(),
          overflow_behavior: get_default_overflow_behavior(),
          backpressure_threshold: get_default_backpressure_threshold(),
          block_timeout_ms: get_default_block_timeout(),
          max_blocked_producers: get_default_max_blocked_producers(),
          created_at: System.system_time(:second),
          implicit: true
        }

        :ets.insert(@config_table, {queue_name, config})
        Logger.warning(I18n.t(:queue_created_implicitly, queue: queue_name, mode: delivery_mode))
        config
    end
  end

  @doc """
  Deletes queue configuration.
  Returns :ok or {:error, reason}.
  """
  def delete_queue(queue_name, opts \\ []) do
    force = Keyword.get(opts, :force, false)

    case :ets.lookup(@config_table, queue_name) do
      [] ->
        {:error, :queue_not_found}

      [{^queue_name, _config}] ->
        if force do
          :ets.delete(@config_table, queue_name)
          Logger.info(I18n.t(:queue_deleted, queue: queue_name))
          :ok
        else
          stats = Malachi.Queue.get_stats(queue_name)

          cond do
            stats.consumers > 0 ->
              {:error, :queue_has_active_consumers}

            stats.buffered > 0 ->
              {:error, :queue_has_buffered_messages}

            true ->
              :ets.delete(@config_table, queue_name)
              Logger.info(I18n.t(:queue_deleted, queue: queue_name))
              :ok
          end
        end
    end
  end

  @doc """
  Checks if queue exists in configuration.
  Returns true or false.
  """
  def queue_exists?(queue_name) do
    case :ets.lookup(@config_table, queue_name) do
      [] -> false
      _ -> true
    end
  end

  @doc """
  Updates queue configuration at runtime.
  Uses hybrid validation approach:
  - Allows updates when buffer excess <= update_excess_threshold (default 50%)
  - Rejects updates when buffer excess > threshold without force flag
  - Returns warning when messages may be dropped gradually

  Returns:

    - `{:ok, :updated}` - Update successful, no excess
    - `{:ok, :updated_with_warning, %{excess_messages: n}}` - Update successful with excess
    - `{:ok, :forced_update, %{dropped_messages: n}}` - Forced update
    - `{:error, :buffer_exceeds_new_limit, details}` - Update rejected, excess too large
    - `{:error, :queue_not_found}` - Queue doesn't exist

  """
  def update_queue(queue_name, opts, force \\ false) do
    # Normalize opts to keyword list (accept both map and keyword list)
    opts = if is_map(opts), do: Enum.to_list(opts), else: opts

    case :ets.lookup(@config_table, queue_name) do
      [] ->
        {:error, :queue_not_found}

      [{^queue_name, current_config}] ->
        new_max_buffer_size = Keyword.get(opts, :max_buffer_size)

        # Validate overflow behavior if provided
        case Keyword.get(opts, :overflow_behavior) do
          nil -> :ok
          behavior when is_atom(behavior) -> validate_overflow_behavior(behavior)
          behavior when is_binary(behavior) -> validate_overflow_behavior(String.to_existing_atom(behavior))
        end
        |> case do
          :ok -> perform_update(queue_name, current_config, opts, new_max_buffer_size, force)
          error -> error
        end
    end
  end

  defp perform_update(queue_name, current_config, opts, new_max_buffer_size, force) do
    # Check buffer size constraints if max_buffer_size is being updated
    if new_max_buffer_size && new_max_buffer_size != current_config.max_buffer_size do
      current_buffer_size = get_current_buffer_size(queue_name)
      excess = max(0, current_buffer_size - new_max_buffer_size)
      threshold = Application.get_env(:malachi, :update_excess_threshold, 0.5)
      max_allowed_excess = trunc(new_max_buffer_size * threshold)

      cond do
        # No excess - safe update
        excess == 0 ->
          apply_config_update(queue_name, current_config, opts)

        # Small excess within threshold - allow with warning
        excess > 0 && excess <= max_allowed_excess ->
          _result = apply_config_update(queue_name, current_config, opts)
          {:ok, :updated_with_warning, %{excess_messages: excess, will_drop_gradually: true}}

        # Large excess - require force
        excess > max_allowed_excess && !force ->
          {:error, :buffer_exceeds_new_limit,
           %{
             current_buffer_size: current_buffer_size,
             new_max_buffer_size: new_max_buffer_size,
             excess: excess,
             threshold_pct: threshold * 100,
             suggestion: "drain queue or use force: true"
           }}

        # Forced update
        force ->
          _result = apply_config_update(queue_name, current_config, opts)

          Logger.warning(
            I18n.t(:queue_config_force_updated,
              queue: queue_name,
              excess: excess,
              old_max: current_config.max_buffer_size,
              new_max: new_max_buffer_size
            )
          )

          {:ok, :forced_update, %{dropped_messages: excess}}
      end
    else
      # No buffer size change, just update other fields
      apply_config_update(queue_name, current_config, opts)
    end
  end

  defp apply_config_update(queue_name, current_config, opts) do
    updated_config =
      Map.merge(current_config, %{
        delivery_mode: Keyword.get(opts, :delivery_mode, current_config.delivery_mode),
        max_retries: Keyword.get(opts, :max_retries, current_config.max_retries),
        dlq_enabled: Keyword.get(opts, :dlq_enabled, current_config.dlq_enabled),
        max_message_size_bytes: Keyword.get(opts, :max_message_size_bytes, current_config.max_message_size_bytes),
        max_buffer_size: Keyword.get(opts, :max_buffer_size, current_config.max_buffer_size),
        overflow_behavior:
          parse_overflow_behavior(Keyword.get(opts, :overflow_behavior, current_config.overflow_behavior)),
        backpressure_threshold: Keyword.get(opts, :backpressure_threshold, current_config.backpressure_threshold),
        block_timeout_ms: Keyword.get(opts, :block_timeout_ms, current_config.block_timeout_ms),
        max_blocked_producers: Keyword.get(opts, :max_blocked_producers, current_config.max_blocked_producers)
      })

    :ets.insert(@config_table, {queue_name, updated_config})

    # Notify queue process of config change
    notify_config_updated(queue_name, updated_config)

    Logger.info(I18n.t(:queue_config_updated, queue: queue_name))
    {:ok, :updated}
  end

  defp get_current_buffer_size(queue_name) do
    case Malachi.Queue.get_stats(queue_name) do
      %{buffered: size} -> size
      _ -> 0
    end
  end

  defp notify_config_updated(queue_name, new_config) do
    # Send config update to all partitions
    partition_count = Application.get_env(:malachi, :partition_multiplier, 100) * System.schedulers_online()

    Enum.each(0..(partition_count - 1), fn partition ->
      # Use via_tuple to get the queue process pid
      via = {:via, Registry, {Malachi.QueueRegistry, {queue_name, partition}}}

      case GenServer.whereis(via) do
        nil ->
          :ok

        pid when is_pid(pid) ->
          send(pid, {:config_updated, new_config})
      end
    end)
  end

  defp parse_overflow_behavior(behavior) when is_atom(behavior), do: behavior
  defp parse_overflow_behavior(behavior) when is_binary(behavior), do: String.to_existing_atom(behavior)

  @doc """
  Lists all configured queues.
  """
  def list_queues do
    :ets.tab2list(@config_table)
    |> Enum.map(fn {_name, config} -> config end)
    |> Enum.sort_by(& &1.created_at)
  end

  @doc """
  Checks if queue exists.
  """
  def exists?(queue_name) do
    case :ets.lookup(@config_table, queue_name) do
      [] -> false
      _ -> true
    end
  end

  @impl true
  def init(:ok) do
    :ets.new(@config_table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    Logger.info(I18n.t(:queue_config_started))
    {:ok, %{}}
  end

  defp get_default_delivery_mode do
    mode_str = Application.get_env(:malachi, :default_delivery_mode, "at_least_once")

    case mode_str do
      "at_most_once" -> :at_most_once
      _ -> :at_least_once
    end
  end

  defp get_default_max_message_size do
    Application.get_env(:malachi, :default_max_message_size_bytes, 1_048_576)
  end

  defp get_default_max_buffer_size do
    Application.get_env(:malachi, :default_max_buffer_size, 10_000)
  end

  defp get_default_overflow_behavior do
    behavior_str = Application.get_env(:malachi, :default_overflow_behavior, "drop_newest")

    case behavior_str do
      "drop_oldest" -> :drop_oldest
      "reject" -> :reject
      "block" -> :block
      _ -> :drop_newest
    end
  end

  defp get_default_backpressure_threshold do
    Application.get_env(:malachi, :default_backpressure_threshold, 0.8)
  end

  defp get_default_block_timeout do
    Application.get_env(:malachi, :default_block_timeout_ms, 5_000)
  end

  defp get_default_max_blocked_producers do
    Application.get_env(:malachi, :default_max_blocked_producers, 1_000)
  end

  defp validate_delivery_mode(mode) when mode in [:at_least_once, :at_most_once], do: :ok
  defp validate_delivery_mode(_), do: {:error, :invalid_delivery_mode}

  defp validate_overflow_behavior(behavior)
       when behavior in [:drop_newest, :drop_oldest, :reject, :block],
       do: :ok

  defp validate_overflow_behavior(invalid) do
    {:error, :invalid_overflow_behavior,
     "Must be one of: :drop_newest, :drop_oldest, :reject, :block. Got: #{inspect(invalid)}"}
  end
end
