defmodule Malachi.Queue do
  @moduledoc """
  Individual queue using ETS for maximum performance.
  Optimized for machines with many cores.
  """
  use GenServer
  require Logger

  # ETS table for rate-limiting overflow logs (max 10/min per queue)
  @overflow_log_table :malachi_overflow_logs

  # Anonymous ETS tables (no :named_table) to prevent atom exhaustion
  # from dynamic queue names. Tables are accessed via references in state.
  @ets_opts [
    :bag,
    :public,
    read_concurrency: true,
    write_concurrency: true,
    decentralized_counters: true
  ]

  def start_link({name, partition}) do
    GenServer.start_link(__MODULE__, {name, partition}, name: via_tuple({name, partition}))
  end

  def enqueue(queue_name, payload, headers \\ %{}) do
    with :ok <- Malachi.Validator.validate_queue_name(queue_name) do
      {name, partition} = Malachi.PartitionManager.get_partition(queue_name)

      ensure_started({name, partition})

      # Ensure queue configuration exists (creates implicitly if needed)
      _config = Malachi.QueueConfig.get_config(queue_name)

      # Register producer for stats tracking
      GenServer.cast(via_tuple({name, partition}), {:register_producer, self()})

      Malachi.Metrics.increment_enqueued(queue_name)

      message_id = :erlang.unique_integer([:monotonic, :positive])

      message = %{
        id: message_id,
        payload: payload,
        headers: headers,
        timestamp: System.monotonic_time(:microsecond),
        queue: queue_name
      }

      GenServer.call(via_tuple({name, partition}), {:enqueue, message})
    end
  end

  def subscribe(queue_name, consumer_pid) do
    with :ok <- Malachi.Validator.validate_queue_name(queue_name) do
      {name, partition} = Malachi.PartitionManager.get_partition(queue_name)
      ensure_started({name, partition})

      GenServer.cast(via_tuple({name, partition}), {:subscribe, consumer_pid})
    end
  end

  def get_stats(queue_name) do
    case Malachi.Validator.validate_queue_name(queue_name) do
      :ok ->
        {name, partition} = Malachi.PartitionManager.get_partition(queue_name)

        case GenServer.whereis(via_tuple({name, partition})) do
          nil ->
            %{exists: false, consumers: 0, producers: 0, buffered: 0, partition: partition}

          pid ->
            GenServer.call(pid, :get_stats, 1000)
        end

      error ->
        error
    end
  end

  @doc """
  Removes all consumers from a queue.
  Returns {:ok, removed_count}
  """
  def kill_all_consumers(queue_name) do
    with :ok <- Malachi.Validator.validate_queue_name(queue_name) do
      {name, partition} = Malachi.PartitionManager.get_partition(queue_name)

      case GenServer.whereis(via_tuple({name, partition})) do
        nil ->
          {:ok, 0}

        pid ->
          GenServer.call(pid, :kill_all_consumers)
      end
    end
  end

  @doc """
  Lists all consumer PIDs from a queue.
  """
  def list_consumers(queue_name) do
    case Malachi.Validator.validate_queue_name(queue_name) do
      :ok ->
        {name, partition} = Malachi.PartitionManager.get_partition(queue_name)

        case GenServer.whereis(via_tuple({name, partition})) do
          nil ->
            []

          pid ->
            GenServer.call(pid, :list_consumers)
        end

      _error ->
        []
    end
  end

  @doc """
  Removes a specific consumer by PID.
  """
  def kill_consumer(queue_name, consumer_pid) do
    with :ok <- Malachi.Validator.validate_queue_name(queue_name) do
      {name, partition} = Malachi.PartitionManager.get_partition(queue_name)

      case GenServer.whereis(via_tuple({name, partition})) do
        nil ->
          {:error, :queue_not_found}

        pid ->
          GenServer.call(pid, {:kill_consumer, consumer_pid})
      end
    end
  end

  @impl true
  def init({name, partition}) do
    # Ensure overflow log table exists (shared across all queue processes)
    unless :ets.whereis(@overflow_log_table) != :undefined do
      try do
        :ets.new(@overflow_log_table, [
          :set,
          :public,
          :named_table,
          read_concurrency: true,
          write_concurrency: true
        ])
      rescue
        # Table already exists
        ArgumentError -> :ok
      end
    end

    # Anonymous ETS tables — no dynamic atoms created from queue names
    consumers_table = :ets.new(:queue_consumers, @ets_opts)

    buffer_table =
      :ets.new(:queue_buffer, [:ordered_set, :public, write_concurrency: true])

    producers_table =
      :ets.new(:queue_producers, [:set, :public, read_concurrency: true, write_concurrency: true])

    message_counter = :atomics.new(1, signed: false)
    :atomics.put(message_counter, 1, 0)

    consumer_index_counter = :atomics.new(1, signed: false)
    :atomics.put(consumer_index_counter, 1, 0)

    state = %{
      name: name,
      partition: partition,
      consumers_table: consumers_table,
      buffer_table: buffer_table,
      producers_table: producers_table,
      message_counter: message_counter,
      consumer_index_counter: consumer_index_counter,
      consumers_list: [],
      consumers_tuple: nil,
      # Backpressure: hybrid queue + map structure for blocked producers
      blocked_producers: %{
        queue: :queue.new(),
        lookup: %{}
      }
    }

    {:ok, state}
  end

  @impl true
  def handle_cast({:subscribe, consumer_pid}, state) do
    Process.monitor(consumer_pid)

    counter = :atomics.new(1, signed: false)
    :atomics.put(counter, 1, 0)

    :ets.insert(state.consumers_table, {consumer_pid, counter, System.monotonic_time()})

    new_consumers_list = state.consumers_list ++ [consumer_pid]

    # Optimize: use tuple for > 100 consumers (O(1) access vs O(n))
    new_consumers_tuple =
      if length(new_consumers_list) > 100 do
        List.to_tuple(new_consumers_list)
      else
        nil
      end

    new_state = %{state | consumers_list: new_consumers_list, consumers_tuple: new_consumers_tuple}

    flush_buffer(consumer_pid, state.buffer_table)

    # Try to unblock producers after draining buffer
    send(self(), :maybe_unblock_producers)

    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:register_producer, producer_pid}, state) do
    # Only insert if not already registered
    case :ets.lookup(state.producers_table, producer_pid) do
      [] ->
        :ets.insert(state.producers_table, {producer_pid, System.monotonic_time()})
        Process.monitor(producer_pid)

      _ ->
        :ok
    end

    {:noreply, state}
  end

  @impl true
  def handle_call(:get_stats, _from, state) do
    consumers = :ets.info(state.consumers_table, :size)
    producers = :ets.info(state.producers_table, :size)
    buffered = :ets.info(state.buffer_table, :size)
    total_messages = :atomics.get(state.message_counter, 1)

    stats = %{
      exists: true,
      name: state.name,
      partition: state.partition,
      consumers: consumers,
      producers: producers,
      buffered: buffered,
      total_messages: total_messages,
      memory_kb:
        div(
          (:ets.info(state.consumers_table, :memory) + :ets.info(state.buffer_table, :memory)) * 8,
          1024
        )
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_call(:kill_all_consumers, _from, state) do
    consumers = :ets.tab2list(state.consumers_table)
    count = length(consumers)

    Enum.each(consumers, fn {consumer_pid, _counter, _ts} ->
      Process.exit(consumer_pid, :kill)
      :ets.delete(state.consumers_table, consumer_pid)
    end)

    {:reply, {:ok, count}, %{state | consumers_list: [], consumers_tuple: nil}}
  end

  @impl true
  def handle_call(:list_consumers, _from, state) do
    consumers =
      :ets.tab2list(state.consumers_table)
      |> Enum.map(fn {pid, _counter, ts} ->
        %{
          pid: pid,
          alive: Process.alive?(pid),
          registered_at: ts
        }
      end)

    {:reply, consumers, state}
  end

  @impl true
  def handle_call({:kill_consumer, consumer_pid}, _from, state) do
    case :ets.lookup(state.consumers_table, consumer_pid) do
      [{^consumer_pid, _counter, _ts}] ->
        Process.exit(consumer_pid, :kill)
        :ets.delete(state.consumers_table, consumer_pid)
        new_consumers_list = List.delete(state.consumers_list, consumer_pid)

        # Rebuild tuple if needed
        new_consumers_tuple =
          if length(new_consumers_list) > 100 do
            List.to_tuple(new_consumers_list)
          else
            nil
          end

        {:reply, :ok, %{state | consumers_list: new_consumers_list, consumers_tuple: new_consumers_tuple}}

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:enqueue, message}, from, state) do
    config = Malachi.QueueConfig.get_config(message.queue)

    case state.consumers_list do
      [] ->
        # No consumers - check buffer limit and apply overflow behavior
        buffer_size = :ets.info(state.buffer_table, :size)

        if buffer_size < config.max_buffer_size do
          # Buffer has space - insert normally
          :ets.insert(state.buffer_table, {message.id, message})
          {:reply, {:ok, message}, state}
        else
          # Buffer full - apply overflow behavior
          apply_overflow_behavior(message, from, state, config, buffer_size)
        end

      consumers ->
        # Has consumers - dispatch immediately (bypasses buffer limit)
        dispatch_to_consumer(message, consumers, state)
        {:reply, {:ok, message}, state}
    end
  end

  @impl true
  def handle_call({:dispatch_message, message}, _from, state) do
    case state.consumers_list do
      [] ->
        {:reply, :no_consumers, state}

      consumers ->
        # Get next consumer in round-robin fashion
        current_index = :atomics.get(state.consumer_index_counter, 1)
        consumer_index = rem(current_index, length(consumers))
        consumer_pid = Enum.at(consumers, consumer_index)

        # Increment for next message
        :atomics.add(state.consumer_index_counter, 1, 1)

        config = Malachi.QueueConfig.get_config(message.queue)

        # Only track message for at_least_once delivery mode
        if config.delivery_mode == :at_least_once and Process.whereis(Malachi.AckManager) do
          Malachi.AckManager.track_message(
            message.id,
            message.queue,
            consumer_pid,
            message
          )
        end

        send(consumer_pid, {:queue_message, message})

        case :ets.lookup(state.consumers_table, consumer_pid) do
          [{^consumer_pid, counter, _ts}] ->
            :atomics.add(counter, 1, 1)

          _ ->
            :ok
        end

        {:reply, :dispatched, state}
    end
  end

  defp dispatch_to_consumer(message, consumers, state) do
    # Get next consumer in round-robin fashion
    current_index = :atomics.get(state.consumer_index_counter, 1)

    # Use tuple for O(1) access when available, otherwise list
    consumer_pid =
      if state.consumers_tuple do
        num = tuple_size(state.consumers_tuple)
        elem(state.consumers_tuple, rem(current_index, num))
      else
        num = length(consumers)
        Enum.at(consumers, rem(current_index, num))
      end

    # Increment for next message
    :atomics.add(state.consumer_index_counter, 1, 1)

    config = Malachi.QueueConfig.get_config(message.queue)

    # Only track message for at_least_once delivery mode
    if config.delivery_mode == :at_least_once and Process.whereis(Malachi.AckManager) do
      Malachi.AckManager.track_message(
        message.id,
        message.queue,
        consumer_pid,
        message
      )
    end

    send(consumer_pid, {:queue_message, message})

    case :ets.lookup(state.consumers_table, consumer_pid) do
      [{^consumer_pid, counter, _ts}] ->
        :atomics.add(counter, 1, 1)

      _ ->
        :ok
    end
  end

  defp apply_overflow_behavior(message, from, state, config, buffer_size) do
    case config.overflow_behavior do
      :drop_newest ->
        # O(1) - Simply don't insert the new message
        Malachi.Metrics.increment_dropped(message.queue)
        log_overflow_event(message.queue, :dropped_newest, buffer_size, config.max_buffer_size)
        {:reply, {:ok, message}, state}

      :drop_oldest ->
        # O(log N) - Delete oldest and insert newest
        case :ets.first(state.buffer_table) do
          :"$end_of_table" ->
            # Buffer empty (race condition) - just insert
            :ets.insert(state.buffer_table, {message.id, message})
            {:reply, {:ok, message}, state}

          oldest_key ->
            :ets.delete(state.buffer_table, oldest_key)
            :ets.insert(state.buffer_table, {message.id, message})
            Malachi.Metrics.increment_dropped(message.queue)
            log_overflow_event(message.queue, :dropped_oldest, buffer_size, config.max_buffer_size)
            {:reply, {:ok, message}, state}
        end

      :reject ->
        # O(1) - Reject the message
        Malachi.Metrics.increment_rejected(message.queue)
        log_overflow_event(message.queue, :rejected, buffer_size, config.max_buffer_size)
        {:reply, {:error, :queue_full}, state}

      :block ->
        # O(1) enqueue - Block producer until space available
        blocked_count = map_size(state.blocked_producers.lookup)

        if blocked_count >= config.max_blocked_producers do
          # Too many blocked producers - reject to prevent memory leak
          Malachi.Metrics.increment_rejected(message.queue)
          log_overflow_event(message.queue, :too_many_blocked, blocked_count, config.max_blocked_producers)
          {:reply, {:error, :too_many_blocked_producers}, state}
        else
          # Block the producer
          ref = make_ref()
          timer_ref = Process.send_after(self(), {:block_timeout, from, ref}, config.block_timeout_ms)

          new_queue = :queue.in({from, ref}, state.blocked_producers.queue)
          new_lookup = Map.put(state.blocked_producers.lookup, from, {message, ref, timer_ref})

          new_blocked = %{queue: new_queue, lookup: new_lookup}
          new_state = %{state | blocked_producers: new_blocked}

          # Update metrics
          Malachi.Metrics.set_blocked_producers_count(message.queue, map_size(new_lookup))
          Malachi.Metrics.increment_total_producers_blocked(message.queue)

          log_overflow_event(message.queue, :blocked, blocked_count + 1, config.max_blocked_producers)

          {:noreply, new_state}
        end
    end
  end

  @impl true
  def handle_info({:block_timeout, from, ref}, state) do
    # Producer timeout expired - send error and cleanup
    case Map.get(state.blocked_producers.lookup, from) do
      {_message, ^ref, _timer_ref} ->
        # Ref matches - this is the correct timeout
        GenServer.reply(from, {:error, :block_timeout})

        # Remove from structures
        new_queue =
          :queue.filter(
            fn {f, r} -> f != from or r != ref end,
            state.blocked_producers.queue
          )

        new_lookup = Map.delete(state.blocked_producers.lookup, from)
        new_blocked = %{queue: new_queue, lookup: new_lookup}

        # Update metrics
        Malachi.Metrics.set_blocked_producers_count(state.name, map_size(new_lookup))

        log_overflow_event(state.name, :timeout_exceeded, map_size(new_lookup), 0)

        {:noreply, %{state | blocked_producers: new_blocked}}

      _ ->
        # Ref doesn't match or producer already unblocked - ignore
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:config_updated, new_config}, state) do
    # Config was updated - try to unblock producers if buffer has space now
    buffer_size = :ets.info(state.buffer_table, :size)

    if buffer_size < new_config.max_buffer_size do
      {:noreply, try_unblock_producers(state, new_config)}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info(:maybe_unblock_producers, state) do
    # Called when consumer drains buffer - try to unblock waiting producers
    config = Malachi.QueueConfig.get_config(state.name)
    {:noreply, try_unblock_producers(state, config)}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    # Remove from consumers table
    :ets.delete(state.consumers_table, pid)
    # Remove from producers table
    :ets.delete(state.producers_table, pid)
    # Remove from consumers list
    new_consumers_list = List.delete(state.consumers_list, pid)

    # Rebuild tuple if needed
    new_consumers_tuple =
      if length(new_consumers_list) > 100 do
        List.to_tuple(new_consumers_list)
      else
        nil
      end

    {:noreply, %{state | consumers_list: new_consumers_list, consumers_tuple: new_consumers_tuple}}
  end

  defp via_tuple({name, partition}) do
    {:via, Registry, {Malachi.QueueRegistry, {name, partition}}}
  end

  # Dynamic atom-generating functions removed to prevent atom table exhaustion.
  # ETS tables are now anonymous and accessed via references stored in GenServer state.

  defp ensure_started({name, partition}) do
    case GenServer.whereis(via_tuple({name, partition})) do
      nil ->
        spec = {__MODULE__, {name, partition}}

        case DynamicSupervisor.start_child(Malachi.QueueSupervisor, spec) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> :ok
          error -> error
        end

      _pid ->
        :ok
    end
  end

  defp flush_buffer(consumer_pid, buffer_table) do
    case :ets.first(buffer_table) do
      :"$end_of_table" ->
        :ok

      key ->
        case :ets.lookup(buffer_table, key) do
          [{^key, message}] ->
            config = Malachi.QueueConfig.get_config(message.queue)

            # Only track message for at_least_once delivery mode
            if config.delivery_mode == :at_least_once and Process.whereis(Malachi.AckManager) do
              Malachi.AckManager.track_message(
                message.id,
                message.queue,
                consumer_pid,
                message
              )
            end

            send(consumer_pid, {:queue_message, message})
            :ets.delete(buffer_table, key)
            flush_buffer(consumer_pid, buffer_table)

          [] ->
            flush_buffer(consumer_pid, buffer_table)
        end
    end
  end

  defp try_unblock_producers(state, config) do
    buffer_size = :ets.info(state.buffer_table, :size)

    if buffer_size < config.max_buffer_size and not :queue.is_empty(state.blocked_producers.queue) do
      # Space available - unblock one producer (FIFO)
      case :queue.out(state.blocked_producers.queue) do
        {{:value, {from, ref}}, new_queue} ->
          case Map.get(state.blocked_producers.lookup, from) do
            {message, ^ref, timer_ref} ->
              # Cancel timeout timer
              Process.cancel_timer(timer_ref)

              # Insert message into buffer
              :ets.insert(state.buffer_table, {message.id, message})

              # Reply to producer
              GenServer.reply(from, {:ok, message})

              # Remove from lookup
              new_lookup = Map.delete(state.blocked_producers.lookup, from)
              new_blocked = %{queue: new_queue, lookup: new_lookup}

              # Update metrics
              Malachi.Metrics.set_blocked_producers_count(state.name, map_size(new_lookup))

              log_overflow_event(state.name, :unblocked, map_size(new_lookup), 0)

              # Try to unblock more if there's still space
              new_state = %{state | blocked_producers: new_blocked}
              try_unblock_producers(new_state, config)

            _ ->
              # Ref mismatch - skip this entry and try next
              new_blocked = %{queue: new_queue, lookup: state.blocked_producers.lookup}
              try_unblock_producers(%{state | blocked_producers: new_blocked}, config)
          end

        {:empty, _} ->
          state
      end
    else
      state
    end
  end

  defp log_overflow_event(queue_name, event_type, current_value, max_value) do
    # Rate limit: max 10 logs per minute per queue
    now = System.system_time(:millisecond)
    window_ms = 60_000
    rate_limit = 10

    key = {:overflow_log_rate, queue_name}

    case :ets.lookup(@overflow_log_table, key) do
      [{^key, count, window_start}] when now - window_start < window_ms ->
        if count < rate_limit do
          :ets.update_element(@overflow_log_table, key, {2, count + 1})
          do_log_overflow(queue_name, event_type, current_value, max_value)
        end

      _ ->
        # New window
        :ets.insert(@overflow_log_table, {key, 1, now})
        do_log_overflow(queue_name, event_type, current_value, max_value)
    end
  end

  defp do_log_overflow(queue_name, event_type, current, max) do
    Logger.warning(
      Malachi.I18n.t(:queue_overflow_event,
        queue: queue_name,
        event: event_type,
        current: current,
        max: max
      )
    )
  end
end
