defmodule Malachi.Backpressure do
  @moduledoc """
  Monitors queue pressure and provides backpressure signals to producers.

  Pressure is calculated as: current_buffer_size / max_buffer_size

  Uses the queue's configured backpressure_threshold (default 0.8) to determine
  when to signal high pressure. Signals are sent independent of overflow strategy
  to provide producers with awareness of queue state.

  ## Pressure Levels

    * `:low_pressure` - Buffer < 50% full
    * `:medium_pressure` - Buffer 50-79% full
    * `:high_pressure` - Buffer >= threshold (default 80%)
    * `:full` - Buffer at 100% capacity

  ## Examples

      iex> Malachi.Backpressure.get_queue_pressure("orders")
      {:ok, %{pressure: 0.85, status: :high_pressure, buffer_size: 8500, max_size: 10000}}
      
      iex> Malachi.Backpressure.should_apply_backpressure?("orders")
      true
  """

  @doc """
  Gets detailed pressure information for a queue.

  Returns {:ok, pressure_info} or {:error, reason}.

  Pressure info includes:
  - `pressure`: Float 0.0-1.0+ (can exceed 1.0 if buffer > max after config update)
  - `status`: Atom indicating pressure level
  - `buffer_size`: Current number of buffered messages
  - `max_size`: Configured maximum buffer size
  - `threshold`: Configured backpressure threshold
  """
  def get_queue_pressure(queue_name) do
    # Check if queue exists first to avoid implicit creation
    if Malachi.QueueConfig.queue_exists?(queue_name) do
      config = Malachi.QueueConfig.get_config(queue_name)

      case Malachi.Queue.get_stats(queue_name) do
        %{buffered: current_size} ->
          pressure =
            if config.max_buffer_size > 0 do
              current_size / config.max_buffer_size
            else
              0.0
            end

          status =
            cond do
              pressure >= 1.0 -> :full
              pressure >= config.backpressure_threshold -> :high_pressure
              pressure >= 0.5 -> :medium_pressure
              true -> :low_pressure
            end

          {:ok,
           %{
             pressure: pressure,
             status: status,
             buffer_size: current_size,
             max_size: config.max_buffer_size,
             threshold: config.backpressure_threshold
           }}

        _ ->
          {:error, :queue_not_found}
      end
    else
      {:error, :queue_not_found}
    end
  end

  @doc """
  Determines if backpressure signal should be sent to producer.

  Returns true when pressure status is :high_pressure or :full,
  regardless of overflow strategy.

  Producers receiving backpressure signal should reduce their publish rate.

  Can be called with:
  - Queue name (string) - fetches pressure info first
  - Pressure info map (from get_queue_pressure/1) - direct check
  """
  def should_apply_backpressure?(queue_name) when is_binary(queue_name) do
    case get_queue_pressure(queue_name) do
      {:ok, %{status: status}} when status in [:high_pressure, :full] -> true
      _ -> false
    end
  end

  def should_apply_backpressure?(%{status: status}) when status in [:high_pressure, :full], do: true
  def should_apply_backpressure?(_), do: false

  @doc """
  Gets pressure status as a simple atom.

  Returns :low_pressure | :medium_pressure | :high_pressure | :full | :error
  """
  def get_pressure_status(queue_name) do
    case get_queue_pressure(queue_name) do
      {:ok, %{status: status}} -> status
      {:error, _} -> :error
    end
  end
end
