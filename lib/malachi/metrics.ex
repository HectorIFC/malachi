defmodule Malachi.Metrics do
  @moduledoc """
  Real-time metrics system using ETS and atomic counters.
  """
  use GenServer
  require Logger
  alias Malachi.Auth.{LockoutManager, SessionManager}
  alias Malachi.I18n

  @metrics_table :malachi_metrics

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def increment_enqueued(queue_name) do
    key = {:enqueued, queue_name}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  def increment_processed(queue_name) do
    key = {:processed, queue_name}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  def increment_errors(queue_name) do
    key = {:errors, queue_name}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  def increment_acked(queue_name) do
    key = {:acked, queue_name}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  def increment_nacked(queue_name) do
    key = {:nacked, queue_name}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  def increment_retried(queue_name) do
    key = {:retried, queue_name}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  def increment_dead_lettered(queue_name) do
    key = {:dead_lettered, queue_name}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  def increment_channel_published(channel_name) do
    key = {:channel_published, channel_name}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  def increment_channel_delivered(channel_name, count \\ 1) do
    key = {:channel_delivered, channel_name}
    :ets.update_counter(@metrics_table, key, {2, count}, {key, 0})
    :ok
  end

  def increment_channel_dropped(channel_name, count \\ 1) do
    key = {:channel_dropped, channel_name}
    :ets.update_counter(@metrics_table, key, {2, count}, {key, 0})
    :ok
  end

  @doc """
  Increment rejected messages counter (buffer full with :reject strategy).
  """
  def increment_rejected(queue_name) do
    key = {:rejected, queue_name}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment dropped messages counter (:drop_oldest or :drop_newest strategy).
  """
  def increment_dropped(queue_name) do
    key = {:dropped, queue_name}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Set current count of blocked producers (gauge metric).
  This represents the current number of producers waiting for buffer space.
  """
  def set_blocked_producers_count(queue_name, count) do
    key = {:blocked_producers_count, queue_name}
    :ets.insert(@metrics_table, {key, count})
    :ok
  end

  @doc """
  Increment total producers blocked counter (cumulative).
  This is a historical counter of how many producers have been blocked.
  """
  def increment_total_producers_blocked(queue_name) do
    key = {:total_producers_blocked, queue_name}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Record buffer utilization percentage for a queue.
  """
  def record_buffer_utilization(queue_name, utilization_pct) do
    key = {:buffer_utilization_pct, queue_name}
    :ets.insert(@metrics_table, {key, utilization_pct})
    :ok
  end

  @doc """
  Increment rate limit blocked counter for specific action.
  """
  def increment_rate_limit_blocked(action) do
    key = {:rate_limit_blocked, action}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment connection limit blocked counter.
  """
  def increment_connection_limit_blocked do
    :ets.update_counter(@metrics_table, :connection_limit_blocked, {2, 1}, {:connection_limit_blocked, 0})
    :ok
  end

  @doc """
  Increment validation cache hit counter.
  """
  def increment_validation_cache_hit do
    key = :validation_cache_hit
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment validation cache miss counter.
  """
  def increment_validation_cache_miss do
    key = :validation_cache_miss
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment validation error counter.

  Category can be:
  - :invalid_queue_name
  - :invalid_channel_name
  - :payload_too_large
  - :invalid_headers
  - :other
  """
  def increment_validation_error(category) do
    key = {:validation_error, category}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment failed authentication attempt counter.
  """
  def increment_failed_auth_attempt do
    key = :failed_auth_attempts
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment account lockout counter.
  """
  def increment_account_lockout do
    key = :account_lockouts
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment account lockout blocked attempt counter.
  """
  def increment_account_lockout_blocked do
    key = :account_lockout_blocked
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment audit event counter by event type.
  """
  def increment_audit_event(event_type) do
    key = {:audit_event, event_type}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment dashboard authentication success counter.
  """
  def increment_dashboard_auth_success do
    key = :dashboard_auth_success
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment dashboard authentication failure counter.
  """
  def increment_dashboard_auth_failed do
    key = :dashboard_auth_failed
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment dashboard authentication blocked (rate limited) counter.
  """
  def increment_dashboard_auth_blocked do
    key = :dashboard_auth_blocked
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment TLS handshake success counter.
  """
  def increment_tls_handshake_success do
    key = :tls_handshake_success
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment TLS handshake failure counter.
  """
  def increment_tls_handshake_failed do
    key = :tls_handshake_failed
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Record the negotiated TLS version for a connection.
  """
  def record_tls_version(version) do
    key = {:tls_version, version}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  def record_latency(queue_name, latency_us) do
    count_key = {:latency_count, queue_name}
    sum_key = {:latency_sum, queue_name}
    min_key = {:latency_min, queue_name}
    max_key = {:latency_max, queue_name}

    # Atomic counter operations
    :ets.update_counter(@metrics_table, count_key, {2, 1}, {count_key, 0})
    :ets.update_counter(@metrics_table, sum_key, {2, latency_us}, {sum_key, 0})

    # Min - best effort with insert_new for initial value
    case :ets.lookup(@metrics_table, min_key) do
      [] -> :ets.insert_new(@metrics_table, {min_key, latency_us})
      [{^min_key, current}] when latency_us < current -> :ets.insert(@metrics_table, {min_key, latency_us})
      _ -> :ok
    end

    # Max - best effort with insert_new for initial value
    case :ets.lookup(@metrics_table, max_key) do
      [] -> :ets.insert_new(@metrics_table, {max_key, latency_us})
      [{^max_key, current}] when latency_us > current -> :ets.insert(@metrics_table, {max_key, latency_us})
      _ -> :ok
    end

    :ok
  end

  def get_metrics(queue_name) do
    enqueued = get_counter({:enqueued, queue_name})
    processed = get_counter({:processed, queue_name})
    errors = get_counter({:errors, queue_name})
    acked = get_counter({:acked, queue_name})
    nacked = get_counter({:nacked, queue_name})
    retried = get_counter({:retried, queue_name})
    dead_lettered = get_counter({:dead_lettered, queue_name})
    rejected = get_counter({:rejected, queue_name})
    dropped = get_counter({:dropped, queue_name})
    total_producers_blocked = get_counter({:total_producers_blocked, queue_name})
    latency = get_latency_stats({:latency, queue_name})

    pending_ack =
      if Code.ensure_loaded?(Malachi.AckManager) and Process.whereis(Malachi.AckManager) do
        Malachi.AckManager.pending_count(queue_name)
      else
        0
      end

    config =
      if Code.ensure_loaded?(Malachi.QueueConfig) and Process.whereis(Malachi.QueueConfig) do
        Malachi.QueueConfig.get_config(queue_name)
      else
        %{
          delivery_mode: :at_least_once,
          max_buffer_size: 10_000,
          max_message_size_bytes: 1_048_576,
          overflow_behavior: :drop_newest,
          backpressure_threshold: 0.8,
          max_blocked_producers: 1_000
        }
      end

    queue_stats = Malachi.Queue.get_stats(queue_name)

    # Calculate buffer utilization
    buffer_utilization_pct =
      if config.max_buffer_size > 0 do
        Float.round(queue_stats.buffered / config.max_buffer_size * 100, 1)
      else
        0.0
      end

    # Get backpressure status
    backpressure_status =
      if Code.ensure_loaded?(Malachi.Backpressure) do
        Malachi.Backpressure.get_pressure_status(queue_name)
      else
        :low_pressure
      end

    # Get current blocked producers count (gauge)
    blocked_producers_count = get_gauge({:blocked_producers_count, queue_name})

    %{
      queue: queue_name,
      delivery_mode: config.delivery_mode,
      enqueued: enqueued,
      processed: processed,
      errors: errors,
      acked: acked,
      nacked: nacked,
      retried: retried,
      dead_lettered: dead_lettered,
      rejected: rejected,
      dropped: dropped,
      total_producers_blocked: total_producers_blocked,
      blocked_producers_count: blocked_producers_count,
      pending_ack: pending_ack,
      latency_us: latency,
      buffer_utilization_pct: buffer_utilization_pct,
      max_buffer_size: config.max_buffer_size,
      max_message_size_bytes: config.max_message_size_bytes,
      overflow_behavior: Atom.to_string(config.overflow_behavior),
      backpressure_status: backpressure_status,
      backpressure_threshold: config.backpressure_threshold,
      max_blocked_producers: config.max_blocked_producers,
      queue_stats: queue_stats
    }
  end

  def get_all_metrics do
    queues = get_all_queues()

    Enum.map(queues, fn queue_name ->
      get_metrics(queue_name)
    end)
  end

  def get_channel_metrics(channel_name) do
    published = get_counter({:channel_published, channel_name})
    delivered = get_counter({:channel_delivered, channel_name})
    dropped = get_counter({:channel_dropped, channel_name})

    stats =
      if Code.ensure_loaded?(Malachi.Channel) do
        Malachi.Channel.get_stats(channel_name)
      else
        %{exists: false, subscribers: 0}
      end

    %{
      channel: channel_name,
      published: published,
      delivered: delivered,
      dropped: dropped,
      subscribers: stats[:subscribers] || 0,
      exists: stats[:exists] || false
    }
  end

  def get_all_channel_metrics do
    channels = get_all_channels()

    Enum.map(channels, fn channel_name ->
      get_channel_metrics(channel_name)
    end)
  end

  def get_system_metrics do
    memory = :erlang.memory()
    {{:input, input_bytes}, {:output, output_bytes}} = :erlang.statistics(:io)

    %{
      timestamp: System.system_time(:second),
      schedulers_online: :erlang.system_info(:schedulers_online),
      process_count: :erlang.system_info(:process_count),
      process_limit: :erlang.system_info(:process_limit),
      run_queue: :erlang.statistics(:run_queue),
      memory: %{
        total_mb: memory[:total] / 1_048_576,
        processes_mb: memory[:processes] / 1_048_576,
        ets_mb: memory[:ets] / 1_048_576,
        atom_mb: memory[:atom] / 1_048_576,
        binary_mb: memory[:binary] / 1_048_576
      },
      ets_tables: length(:ets.all()),
      io: %{
        input_bytes: input_bytes,
        output_bytes: output_bytes
      },
      uptime_seconds: :erlang.statistics(:wall_clock) |> elem(0) |> div(1000),
      rate_limiting: %{
        auth_blocked: get_counter({:rate_limit_blocked, :auth}),
        publish_blocked: get_counter({:rate_limit_blocked, :publish}),
        subscribe_blocked: get_counter({:rate_limit_blocked, :subscribe}),
        connection_blocks: get_counter(:connection_limit_blocked)
      },
      security: %{
        failed_auth_attempts: get_counter(:failed_auth_attempts),
        account_lockouts: get_counter(:account_lockouts),
        lockout_blocks: get_counter(:account_lockout_blocked),
        active_lockouts: get_active_lockout_count(),
        active_sessions: get_active_session_count(),
        dashboard: %{
          auth_success: get_counter(:dashboard_auth_success),
          auth_failed: get_counter(:dashboard_auth_failed),
          auth_blocked: get_counter(:dashboard_auth_blocked)
        },
        audit_events: %{
          auth_success: get_counter({:audit_event, :auth_success}),
          auth_failure: get_counter({:audit_event, :auth_failure}),
          auth_lockout: get_counter({:audit_event, :auth_lockout}),
          session_created: get_counter({:audit_event, :session_created}),
          session_revoked: get_counter({:audit_event, :session_revoked}),
          session_expired: get_counter({:audit_event, :session_expired}),
          session_hijack_attempt: get_counter({:audit_event, :session_hijack_attempt}),
          account_unlocked: get_counter({:audit_event, :account_unlocked}),
          config_validation_failed: get_counter({:audit_event, :config_validation_failed}),
          dashboard_access: get_counter({:audit_event, :dashboard_access}),
          dashboard_login_success: get_counter({:audit_event, :dashboard_login_success}),
          dashboard_auth_failure: get_counter({:audit_event, :dashboard_auth_failure})
        }
      },
      validation: %{
        cache_hits: get_counter(:validation_cache_hit),
        cache_misses: get_counter(:validation_cache_miss),
        errors: %{
          invalid_queue_name: get_counter({:validation_error, :invalid_queue_name}),
          invalid_channel_name: get_counter({:validation_error, :invalid_channel_name}),
          payload_too_large: get_counter({:validation_error, :payload_too_large}),
          invalid_headers: get_counter({:validation_error, :invalid_headers}),
          other: get_counter({:validation_error, :other})
        }
      },
      tls: %{
        enabled: Application.get_env(:malachi, :enable_tls, false),
        required: Application.get_env(:malachi, :require_tls, false),
        handshakes_success: get_counter(:tls_handshake_success),
        handshakes_failed: get_counter(:tls_handshake_failed),
        versions: %{
          "tlsv1.3": get_counter({:tls_version, :"tlsv1.3"}),
          "tlsv1.2": get_counter({:tls_version, :"tlsv1.2"})
        }
      },
      atom_table: get_atom_monitor_stats(),
      memory_details: get_memory_monitor_stats()
    }
  end

  defp get_atom_monitor_stats do
    if Code.ensure_loaded?(Malachi.AtomMonitor) and Process.whereis(Malachi.AtomMonitor) do
      try do
        Malachi.AtomMonitor.get_stats()
      rescue
        _ -> %{atom_count: 0, atom_limit: 0, usage_percent: 0.0, status: :unknown}
      end
    else
      %{atom_count: :erlang.system_info(:atom_count), atom_limit: 1_048_576, usage_percent: 0.0, status: :unavailable}
    end
  end

  defp get_memory_monitor_stats do
    if Code.ensure_loaded?(Malachi.MemoryMonitor) and Process.whereis(Malachi.MemoryMonitor) do
      try do
        Malachi.MemoryMonitor.get_memory_stats()
      rescue
        _ -> %{}
      end
    else
      %{}
    end
  end

  def reset_metrics(queue_name) do
    :ets.delete(@metrics_table, {:enqueued, queue_name})
    :ets.delete(@metrics_table, {:processed, queue_name})
    :ets.delete(@metrics_table, {:errors, queue_name})
    :ets.delete(@metrics_table, {:latency_count, queue_name})
    :ets.delete(@metrics_table, {:latency_sum, queue_name})
    :ets.delete(@metrics_table, {:latency_min, queue_name})
    :ets.delete(@metrics_table, {:latency_max, queue_name})
    :ok
  end

  @impl true
  def init(:ok) do
    :ets.new(@metrics_table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(:malachi_metrics_history, [
      :ordered_set,
      :public,
      :named_table,
      read_concurrency: true
    ])

    schedule_snapshot()
    schedule_cleanup()

    Logger.info(I18n.t(:metrics_started))
    {:ok, %{}}
  end

  @impl true
  def handle_info(:snapshot, state) do
    take_snapshot()
    schedule_snapshot()
    {:noreply, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    cleanup_old_snapshots()
    schedule_cleanup()
    {:noreply, state}
  end

  defp get_counter(key) do
    case :ets.lookup(@metrics_table, key) do
      [{^key, value}] -> value
      [] -> 0
    end
  end

  defp get_gauge(key) do
    # Gauges are stored as simple key-value pairs (not counters)
    case :ets.lookup(@metrics_table, key) do
      [{^key, value}] -> value
      [] -> 0
    end
  end

  defp get_latency_stats({:latency, queue_name}) do
    count = get_counter({:latency_count, queue_name})

    if count > 0 do
      sum = get_counter({:latency_sum, queue_name})

      min_val =
        case :ets.lookup(@metrics_table, {:latency_min, queue_name}) do
          [{_, val}] -> val
          [] -> 0
        end

      max_val =
        case :ets.lookup(@metrics_table, {:latency_max, queue_name}) do
          [{_, val}] -> val
          [] -> 0
        end

      %{
        avg: div(sum, count),
        min: min_val,
        max: max_val,
        count: count
      }
    else
      %{avg: 0, min: 0, max: 0, count: 0}
    end
  end

  defp get_all_queues do
    :ets.match(@metrics_table, {{:enqueued, :"$1"}, :_})
    |> Enum.map(fn [queue] -> queue end)
    |> Enum.uniq()
  end

  defp get_all_channels do
    :ets.match(@metrics_table, {{:channel_published, :"$1"}, :_})
    |> Enum.map(fn [channel] -> channel end)
    |> Enum.uniq()
  end

  defp get_active_lockout_count do
    if Code.ensure_loaded?(LockoutManager) and Process.whereis(LockoutManager) do
      try do
        LockoutManager.list_locked_accounts() |> length()
      rescue
        ArgumentError -> 0
      end
    else
      0
    end
  end

  defp get_active_session_count do
    if Code.ensure_loaded?(SessionManager) do
      try do
        SessionManager.list_sessions() |> length()
      rescue
        ArgumentError -> 0
      end
    else
      0
    end
  end

  defp schedule_snapshot do
    interval = Application.get_env(:malachi, :metrics_snapshot_interval_ms, 1_000)
    Process.send_after(self(), :snapshot, interval)
  end

  defp schedule_cleanup do
    interval = Application.get_env(:malachi, :metrics_cleanup_interval_ms, 60_000)
    Process.send_after(self(), :cleanup, interval)
  end

  defp take_snapshot do
    timestamp = System.system_time(:second)
    metrics = get_all_metrics()
    system = get_system_metrics()

    snapshot = %{
      timestamp: timestamp,
      queues: metrics,
      system: system
    }

    :ets.insert(:malachi_metrics_history, {timestamp, snapshot})
  end

  defp cleanup_old_snapshots do
    history_seconds = Application.get_env(:malachi, :metrics_history_seconds, 300)
    cutoff = System.system_time(:second) - history_seconds

    :ets.select_delete(:malachi_metrics_history, [
      {{:"$1", :_}, [{:<, :"$1", cutoff}], [true]}
    ])
  end

  def get_history(seconds \\ 60) do
    cutoff = System.system_time(:second) - seconds

    :ets.select(:malachi_metrics_history, [
      {{:"$1", :"$2"}, [{:>=, :"$1", cutoff}], [:"$2"]}
    ])
  end
end
