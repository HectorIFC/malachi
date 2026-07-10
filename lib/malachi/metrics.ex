defmodule Malachi.Metrics do
  @moduledoc """
  Real-time operational and security metrics kept in ETS (atomic `update_counter`), plus a
  periodically-sampled system snapshot and its recent history.

  The `increment_*`/`record_*` functions bump counters on the hot path (fast, lock-free) — rate-limit and
  connection-limit blocks, auth failures and account lockouts, audit events, dashboard-auth outcomes, and
  TLS handshakes; `get_system_metrics/0` reads the live BEAM snapshot
  (memory, processes, io) folded together with those counters, and `get_history/1` returns the recent
  snapshots. A counter is created on first touch, so callers never need to initialize one.
  """
  use GenServer
  require Logger
  alias Malachi.Auth.{LockoutManager, SessionManager}
  alias Malachi.I18n
  alias Malachi.Telemetry.MetricsReporter

  @metrics_table :malachi_metrics

  @doc "Starts the metrics server (owns the ETS counter table)."
  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
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

  # --- operation counters (fed by the telemetry -> metrics reporter, O4) ---

  @doc "Records `count` records and `bytes` value bytes produced (from the produce telemetry event)."
  def record_produce(count, bytes) do
    :ets.update_counter(@metrics_table, :records_produced, {2, count}, {:records_produced, 0})
    :ets.update_counter(@metrics_table, :bytes_produced, {2, bytes}, {:bytes_produced, 0})
    :ok
  end

  @doc "Records `count` records consumed (from the consume telemetry event)."
  def record_consume(count) do
    :ets.update_counter(@metrics_table, :records_consumed, {2, count}, {:records_consumed, 0})
    :ok
  end

  @doc "Records an authentication attempt with `result` (`:ok` / `:error`)."
  def record_auth(result) do
    key = {:auth_result, result}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc "Records a quorum replication with `result` (`:ok` / `:no_quorum`)."
  def record_replication(result) do
    key = {:replication_result, result}
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

  @doc "A snapshot of system-wide metrics (memory, processes, connections, auth) for the dashboard."
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
      operations: %{
        records_produced: get_counter(:records_produced),
        bytes_produced: get_counter(:bytes_produced),
        records_consumed: get_counter(:records_consumed),
        auth_ok: get_counter({:auth_result, :ok}),
        auth_error: get_counter({:auth_result, :error}),
        replication_ok: get_counter({:replication_result, :ok}),
        replication_no_quorum: get_counter({:replication_result, :no_quorum})
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

    # Fold the telemetry hot-path events into these counters (O4). Attached here so the ETS table exists
    # first; idempotent, so a Metrics restart re-attaches cleanly.
    MetricsReporter.attach()

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

    snapshot = %{
      timestamp: timestamp,
      system: get_system_metrics()
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

  @doc "The recent per-second history samples for the last `seconds` (default 60)."
  def get_history(seconds \\ 60) do
    cutoff = System.system_time(:second) - seconds

    :ets.select(:malachi_metrics_history, [
      {{:"$1", :"$2"}, [{:>=, :"$1", cutoff}], [:"$2"]}
    ])
  end
end
