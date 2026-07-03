defmodule Malachi.PartitionManager do
  @moduledoc """
  Manages partitions to distribute load across available cores.
  Number of partitions calculated dynamically based on hardware.
  """
  use GenServer
  require Logger
  alias Malachi.I18n

  @doc "Starts the partition manager."
  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc "Maps `queue_name` to a `{queue_name, partition}` pair by consistent hashing over the partition count."
  def get_partition(queue_name) do
    partition = :erlang.phash2(queue_name, get_partition_count())
    {queue_name, partition}
  end

  @doc "The number of partitions: online schedulers times `:partition_multiplier` (default 100)."
  def get_partition_count do
    multiplier = Application.get_env(:malachi, :partition_multiplier, 100)
    System.schedulers_online() * multiplier
  end

  @impl true
  def init(:ok) do
    partitions = get_partition_count()
    schedulers = System.schedulers_online()
    multiplier = div(partitions, schedulers)

    Logger.info(
      I18n.t(:partition_manager_started,
        partitions: partitions,
        schedulers: schedulers,
        multiplier: multiplier
      )
    )

    {:ok, %{}}
  end
end
