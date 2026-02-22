defmodule MalachiMQ.Channel do
  @moduledoc """
  Channel with best-effort delivery (fire-and-forget).

  Channels are implicitly created when first published to or subscribed to.
  Messages are broadcast to all active subscribers without persistence.
  No message buffering - if no subscribers exist, messages are dropped.
  """
  use GenServer
  require Logger
  alias MalachiMQ.I18n

  def start_link(channel_name) do
    GenServer.start_link(__MODULE__, channel_name, name: via_tuple(channel_name))
  end

  def publish(channel_name, payload, headers \\ %{}) do
    with :ok <- MalachiMQ.Validator.validate_channel_name(channel_name) do
      ensure_started(channel_name)

      MalachiMQ.Metrics.increment_channel_published(channel_name)

      message = %{
        payload: payload,
        headers: headers,
        timestamp: System.monotonic_time(:microsecond),
        channel: channel_name
      }

      GenServer.cast(via_tuple(channel_name), {:broadcast, message})
    end
  end

  def subscribe(channel_name, subscriber_pid) do
    with :ok <- MalachiMQ.Validator.validate_channel_name(channel_name) do
      ensure_started(channel_name)
      GenServer.call(via_tuple(channel_name), {:subscribe, subscriber_pid})
    end
  end

  def unsubscribe(channel_name, subscriber_pid) do
    with :ok <- MalachiMQ.Validator.validate_channel_name(channel_name) do
      case GenServer.whereis(via_tuple(channel_name)) do
        nil -> :ok
        pid -> GenServer.call(pid, {:unsubscribe, subscriber_pid})
      end
    end
  end

  def get_stats(channel_name) do
    case MalachiMQ.Validator.validate_channel_name(channel_name) do
      :ok ->
        case GenServer.whereis(via_tuple(channel_name)) do
          nil ->
            %{exists: false, subscribers: 0, published: 0, delivered: 0, dropped: 0}

          pid ->
            GenServer.call(pid, :get_stats)
        end

      error ->
        error
    end
  end

  def list_subscribers(channel_name) do
    case GenServer.whereis(via_tuple(channel_name)) do
      nil -> []
      pid -> GenServer.call(pid, :list_subscribers)
    end
  end

  def kick_subscriber(channel_name, subscriber_pid) do
    case GenServer.whereis(via_tuple(channel_name)) do
      nil -> {:error, :channel_not_found}
      pid -> GenServer.call(pid, {:kick_subscriber, subscriber_pid})
    end
  end

  @impl true
  def init(channel_name) do
    Logger.info(I18n.t(:channel_started, channel: channel_name))

    {:ok,
     %{
       name: channel_name,
       subscribers: MapSet.new(),
       published_count: 0,
       delivered_count: 0,
       dropped_count: 0,
       created_at: System.system_time(:second)
     }}
  end

  @impl true
  def handle_call({:subscribe, subscriber_pid}, _from, state) do
    Process.monitor(subscriber_pid)
    new_subscribers = MapSet.put(state.subscribers, subscriber_pid)

    Logger.debug(
      I18n.t(:channel_subscriber_added,
        channel: state.name,
        pid: inspect(subscriber_pid),
        count: MapSet.size(new_subscribers)
      )
    )

    {:reply, :ok, %{state | subscribers: new_subscribers}}
  end

  @impl true
  def handle_call({:unsubscribe, subscriber_pid}, _from, state) do
    new_subscribers = MapSet.delete(state.subscribers, subscriber_pid)

    Logger.debug(
      I18n.t(:channel_subscriber_removed,
        channel: state.name,
        pid: inspect(subscriber_pid),
        count: MapSet.size(new_subscribers)
      )
    )

    {:reply, :ok, %{state | subscribers: new_subscribers}}
  end

  @impl true
  def handle_call(:get_stats, _from, state) do
    stats = %{
      exists: true,
      subscribers: MapSet.size(state.subscribers),
      published: state.published_count,
      delivered: state.delivered_count,
      dropped: state.dropped_count,
      uptime: System.system_time(:second) - state.created_at
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_call(:list_subscribers, _from, state) do
    {:reply, MapSet.to_list(state.subscribers), state}
  end

  @impl true
  def handle_call({:kick_subscriber, subscriber_pid}, _from, state) do
    if MapSet.member?(state.subscribers, subscriber_pid) do
      send(subscriber_pid, {:kicked_from_channel, state.name})
      new_subscribers = MapSet.delete(state.subscribers, subscriber_pid)

      Logger.info(
        I18n.t(:channel_subscriber_kicked,
          channel: state.name,
          pid: inspect(subscriber_pid)
        )
      )

      {:reply, :ok, %{state | subscribers: new_subscribers}}
    else
      {:reply, {:error, :not_subscribed}, state}
    end
  end

  @impl true
  def handle_cast({:broadcast, message}, state) do
    subscriber_count = MapSet.size(state.subscribers)

    if subscriber_count == 0 do
      MalachiMQ.Metrics.increment_channel_dropped(state.name)

      {:noreply,
       %{
         state
         | published_count: state.published_count + 1,
           dropped_count: state.dropped_count + 1
       }}
    else
      {ok_count, failed_count} = broadcast_to_subscribers(state.subscribers, message)

      update_metrics_after_broadcast(state.name, ok_count, failed_count)

      {:noreply,
       %{
         state
         | published_count: state.published_count + 1,
           delivered_count: state.delivered_count + ok_count,
           dropped_count: state.dropped_count + failed_count
       }}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    new_subscribers = MapSet.delete(state.subscribers, pid)

    Logger.debug(
      I18n.t(:channel_subscriber_down,
        channel: state.name,
        pid: inspect(pid),
        count: MapSet.size(new_subscribers)
      )
    )

    {:noreply, %{state | subscribers: new_subscribers}}
  end

  defp broadcast_to_subscribers(subscribers, message) do
    # Parallelize sends to subscribers using a Task.Supervisor to avoid
    # sequential delivery inside the Channel GenServer.
    concurrency = Application.get_env(:malachimq, :channel_send_concurrency, 5_000)
    per_task_timeout = Application.get_env(:malachimq, :channel_send_task_timeout_ms, 5_000)

    subscribers_list = MapSet.to_list(subscribers)
    subscriber_count = length(subscribers_list)

    # Execute sends in parallel without linking to the Channel process.
    results =
      Task.Supervisor.async_stream_nolink(
        MalachiMQ.TaskSupervisor,
        subscribers_list,
        fn subscriber_pid ->
          send(subscriber_pid, {:channel_message, message})
          :ok
        end,
        max_concurrency: concurrency,
        timeout: per_task_timeout,
        ordered: false
      )
      |> Enum.to_list()

    ok_count =
      Enum.count(results, fn
        {:ok, :ok} -> true
        _ -> false
      end)

    failed_count = subscriber_count - ok_count
    {ok_count, failed_count}
  end

  defp update_metrics_after_broadcast(channel_name, ok_count, failed_count) do
    if ok_count > 0, do: MalachiMQ.Metrics.increment_channel_delivered(channel_name, ok_count)

    if failed_count > 0,
      do: MalachiMQ.Metrics.increment_channel_dropped(channel_name, failed_count)
  end

  defp via_tuple(name) do
    {:via, Registry, {MalachiMQ.ChannelRegistry, name}}
  end

  defp ensure_started(channel_name) do
    case GenServer.whereis(via_tuple(channel_name)) do
      nil ->
        spec = {__MODULE__, channel_name}
        DynamicSupervisor.start_child(MalachiMQ.ChannelSupervisor, spec)
        :ok

      _pid ->
        :ok
    end
  end
end
