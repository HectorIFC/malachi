defmodule Malachi.AuditLog do
  @moduledoc """
  Structured audit system with ETS storage and secondary indexes.

  Tracks security events including authentication, lockouts, sessions, and
  administrative activities. Maintains 30-day history with automatic cleanup.

  ## Event Types

  - `:auth_success` - Successful authentication
  - `:auth_failure` - Failed authentication
  - `:auth_lockout` - Account locked due to excessive attempts
  - `:session_created` - New session created
  - `:session_revoked` - Session manually revoked
  - `:session_expired` - Session expired by timeout
  - `:session_hijack_attempt` - Hijacking attempt detected
  - `:account_unlocked` - Account unlocked by admin
  - `:config_validation_failed` - Configuration validation failed
  - `:dashboard_access` - Dashboard HTTP endpoint accessed
  - `:dashboard_login_success` - Successful dashboard login
  - `:dashboard_auth_failure` - Failed dashboard authentication

  ## Event Structure

  Each event is stored as:
  ```elixir
  {event_id, timestamp, event_type, username, ip, action, status, metadata}
  ```
  """
  use GenServer
  require Logger
  alias Malachi.I18n

  @table_main :malachi_audit_log
  @table_by_type :malachi_audit_by_type
  @table_by_user :malachi_audit_by_user
  # 1 hour
  @cleanup_interval_ms 3_600_000
  @retention_days 30
  # Buffer flush interval for file/stdout output (1 second)
  @flush_interval_ms 1_000
  # Maximum buffer size before forced flush (events)
  @max_buffer_size 100

  ## Client API

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Records an audit event.

  ## Parameters

  - `event_type` - Atom representing the event type
  - `context` - Map with `username` and `ip` (optional)
  - `action` - String describing the action (e.g., "authenticate", "unlock_account")
  - `status` - `:success` or `:failure`
  - `metadata` - Map with event-specific additional data

  ## Examples

      iex> AuditLog.log_event(:auth_success, %{username: "admin", ip: {192, 168, 1, 1}}, 
      ...>   "authenticate", :success, %{})
      :ok
      
      iex> AuditLog.log_event(:auth_lockout, %{username: "user", ip: {10, 0, 0, 1}},
      ...>   "account_locked", :automatic, %{attempt_count: 5, lockout_duration_ms: 300_000})
      :ok
  """
  def log_event(event_type, context, action, status, metadata) do
    GenServer.cast(__MODULE__, {:log_event, event_type, context, action, status, metadata})
  end

  @doc """
  Flush buffered events to disk immediately.
  Useful for testing or ensuring events are written before shutdown.
  """
  def flush do
    if Process.whereis(__MODULE__) do
      GenServer.call(__MODULE__, :flush_now)
    else
      :ok
    end
  end

  @doc """
  Returns all events (limited to most recent).

  ## Parameters

  - `limit` - Maximum number of events to return (default: 1000)
  """
  def get_events(limit \\ 1000) do
    @table_main
    |> :ets.tab2list()
    |> Enum.sort_by(fn {_id, timestamp, _, _, _, _, _, _} -> timestamp end, :desc)
    |> Enum.take(limit)
    |> Enum.map(&format_event/1)
  rescue
    ArgumentError -> []
  end

  @doc """
  Returns events of a specific type.

  ## Parameters

  - `event_type` - Event type to filter
  - `limit` - Maximum number of events to return (default: 1000)
  """
  def get_events_by_type(event_type, limit \\ 1000) do
    # Uses secondary index for efficiency
    @table_by_type
    |> :ets.match({{event_type, :"$1"}, :"$2"})
    |> Enum.sort_by(fn [timestamp, _id] -> timestamp end, :desc)
    |> Enum.take(limit)
    |> Enum.map(fn [_timestamp, event_id] ->
      case :ets.lookup(@table_main, event_id) do
        [event] -> format_event(event)
        [] -> nil
      end
    end)
    |> Enum.reject(&is_nil/1)
  rescue
    ArgumentError -> []
  end

  @doc """
  Returns events for a specific user.

  ## Parameters

  - `username` - Username
  - `limit` - Maximum number of events to return (default: 1000)
  """
  def get_events_by_user(username, limit \\ 1000) do
    # Uses secondary index for efficiency
    @table_by_user
    |> :ets.match({{username, :"$1"}, :"$2"})
    |> Enum.sort_by(fn [timestamp, _id] -> timestamp end, :desc)
    |> Enum.take(limit)
    |> Enum.map(fn [_timestamp, event_id] ->
      case :ets.lookup(@table_main, event_id) do
        [event] -> format_event(event)
        [] -> nil
      end
    end)
    |> Enum.reject(&is_nil/1)
  rescue
    ArgumentError -> []
  end

  @doc """
  Returns event statistics.
  """
  def get_stats do
    total = :ets.info(@table_main, :size)

    by_type =
      @table_by_type
      |> :ets.tab2list()
      |> Enum.reduce(%{}, fn {{type, _timestamp}, _id}, acc ->
        Map.update(acc, type, 1, &(&1 + 1))
      end)

    %{
      total_events: total,
      by_type: by_type,
      retention_days: @retention_days
    }
  rescue
    ArgumentError -> %{total_events: 0, by_type: %{}, retention_days: @retention_days}
  end

  ## GenServer Callbacks

  @impl true
  def init(_opts) do
    # Main table: bag to allow multiple events with same ID (unlikely, but safe)
    main_table =
      :ets.new(@table_main, [
        :bag,
        :public,
        :named_table,
        read_concurrency: true,
        write_concurrency: true
      ])

    # Secondary index by event type
    type_table =
      :ets.new(@table_by_type, [
        :bag,
        :public,
        :named_table,
        read_concurrency: true,
        write_concurrency: true
      ])

    # Secondary index by user
    user_table =
      :ets.new(@table_by_user, [
        :bag,
        :public,
        :named_table,
        read_concurrency: true,
        write_concurrency: true
      ])

    # Initialize file/stdout output if configured
    output_mode = Application.get_env(:malachi, :audit_log_output, :ets_only)
    file_path = Application.get_env(:malachi, :audit_log_file)
    max_size_mb = Application.get_env(:malachi, :audit_log_max_size_mb, 1)
    max_file_size_bytes = round(max_size_mb * 1_048_576)

    file_handle =
      if output_mode in [:file, :both] and file_path do
        case open_audit_file(file_path) do
          {:ok, handle} ->
            Logger.info(I18n.t(:audit_log_file_enabled, path: file_path, max_mb: max_size_mb))
            handle

          {:error, reason} ->
            Logger.error(I18n.t(:audit_log_file_failed, path: file_path, reason: inspect(reason)))
            nil
        end
      else
        nil
      end

    if output_mode in [:stdout, :both] do
      Logger.info(I18n.t(:audit_log_stdout_enabled))
    end

    schedule_cleanup()
    schedule_flush()

    Logger.info(Malachi.I18n.t(:audit_log_started, retention_days: @retention_days))

    {:ok,
     %{
       main: main_table,
       by_type: type_table,
       by_user: user_table,
       output_mode: output_mode,
       file_handle: file_handle,
       file_path: file_path,
       max_file_size_bytes: max_file_size_bytes,
       buffer: [],
       buffer_size: 0
     }}
  end

  @impl true
  def handle_cast({:log_event, event_type, context, action, status, metadata}, state) do
    event_id = generate_event_id()
    timestamp = System.system_time(:millisecond)
    username = Map.get(context, :username, "system")
    ip = Map.get(context, :ip, nil)

    # Format IP as string for storage
    ip_string = format_ip(ip)

    # Insert into main index
    :ets.insert(state.main, {event_id, timestamp, event_type, username, ip_string, action, status, metadata})

    # Insert into secondary indexes
    :ets.insert(state.by_type, {{event_type, timestamp}, event_id})
    :ets.insert(state.by_user, {{username, timestamp}, event_id})

    # Increment metric (safe - checks if Metrics is available)
    try do
      Malachi.Metrics.increment_audit_event(event_type)
    rescue
      # Metrics ETS table does not exist (e.g., isolated tests)
      ArgumentError -> :ok
    end

    # Structured log
    Logger.debug(I18n.t(:audit_event_logged, event_type: event_type),
      event_id: event_id,
      type: event_type,
      username: username,
      ip: ip_string,
      action: action,
      status: status
    )

    # Add to buffer for file/stdout output
    new_state =
      if state.output_mode != :ets_only do
        event_json = build_json_event(event_id, timestamp, event_type, username, ip_string, action, status, metadata)
        updated_state = add_to_buffer(event_json, state)

        # Force flush if buffer is full
        if updated_state.buffer_size >= @max_buffer_size do
          flush_buffer(updated_state)
        else
          updated_state
        end
      else
        state
      end

    {:noreply, new_state}
  end

  @impl true
  def handle_call(:flush_now, _from, state) do
    # Drain any pending log_event casts still in the mailbox first
    state = drain_pending_events(state)

    # Then flush the buffer
    new_state = flush_buffer(state)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    perform_cleanup(state)
    schedule_cleanup()
    {:noreply, state}
  end

  @impl true
  def handle_info(:flush, state) do
    new_state = flush_buffer(state)
    schedule_flush()
    {:noreply, new_state}
  end

  @impl true
  def terminate(_reason, state) do
    # Drain any pending log_event casts still in the mailbox
    state = drain_pending_events(state)

    # Final flush before shutdown
    state = flush_buffer(state)

    if state.file_handle do
      File.close(state.file_handle)
    end

    :ok
  end

  defp drain_pending_events(state) do
    receive do
      {:"$gen_cast", {:log_event, event_type, context, action, status, metadata}} ->
        {:noreply, new_state} =
          handle_cast({:log_event, event_type, context, action, status, metadata}, state)

        drain_pending_events(new_state)
    after
      0 -> state
    end
  end

  ## Private Functions

  defp generate_event_id do
    :crypto.strong_rand_bytes(16)
    |> Base.url_encode64(padding: false)
  end

  defp format_ip(nil), do: "unknown"

  defp format_ip(ip) when is_tuple(ip) do
    case tuple_size(ip) do
      4 -> :inet.ntoa(ip) |> to_string()
      8 -> :inet.ntoa(ip) |> to_string()
      _ -> "invalid"
    end
  end

  defp format_ip(ip) when is_binary(ip), do: ip
  defp format_ip(_), do: "invalid"

  defp format_event({event_id, timestamp, event_type, username, ip, action, status, metadata}) do
    %{
      event_id: event_id,
      timestamp: timestamp,
      event_type: event_type,
      username: username,
      ip: ip,
      action: action,
      status: status,
      metadata: metadata
    }
  end

  defp perform_cleanup(state) do
    cutoff_ms = System.system_time(:millisecond) - @retention_days * 86_400_000

    # Delete old events from main table and capture count
    deleted_main =
      :ets.select_delete(state.main, [
        {{:"$1", :"$2", :_, :_, :_, :_, :_, :_}, [{:<, :"$2", cutoff_ms}], [true]}
      ])

    # Delete old events from secondary indexes using atomic operations
    # These deletes run in parallel at the ETS level (different tables)
    _deleted_by_type =
      :ets.select_delete(state.by_type, [
        {{{:_, :"$1"}, :_}, [{:<, :"$1", cutoff_ms}], [true]}
      ])

    _deleted_by_user =
      :ets.select_delete(state.by_user, [
        {{{:_, :"$1"}, :_}, [{:<, :"$1", cutoff_ms}], [true]}
      ])

    if deleted_main > 0 do
      Logger.info(I18n.t(:audit_log_cleanup, count: deleted_main))
    end
  end

  defp schedule_cleanup do
    Process.send_after(self(), :cleanup, @cleanup_interval_ms)
  end

  defp schedule_flush do
    Process.send_after(self(), :flush, @flush_interval_ms)
  end

  defp open_audit_file(file_path) do
    # Ensure directory exists
    file_path
    |> Path.dirname()
    |> File.mkdir_p()

    # Open file in append mode
    File.open(file_path, [:append, :utf8])
  end

  defp build_json_event(event_id, timestamp, event_type, username, ip, action, status, metadata) do
    %{
      timestamp: DateTime.from_unix!(timestamp, :millisecond) |> DateTime.to_iso8601(),
      event_id: event_id,
      event_type: event_type,
      actor: %{
        username: username,
        ip: ip
      },
      action: action,
      result: status,
      metadata: metadata,
      hostname: get_hostname(),
      node: Node.self()
    }
  end

  defp get_hostname do
    case :inet.gethostname() do
      {:ok, hostname} -> to_string(hostname)
      _ -> "unknown"
    end
  end

  defp add_to_buffer(event_json, state) do
    %{state | buffer: [event_json | state.buffer], buffer_size: state.buffer_size + 1}
  end

  defp flush_buffer(%{buffer: []} = state), do: state

  defp flush_buffer(state) do
    events = Enum.reverse(state.buffer)

    # Write to stdout if enabled
    if state.output_mode in [:stdout, :both] do
      Enum.each(events, fn event ->
        json = Jason.encode!(event)
        Logger.info("[AUDIT] #{json}")
      end)
    end

    # Write to file if enabled
    new_file_handle =
      if state.output_mode in [:file, :both] and state.file_handle do
        write_to_file_with_rotation(state.file_handle, state.file_path, state.max_file_size_bytes, events)
      else
        state.file_handle
      end

    %{state | buffer: [], buffer_size: 0, file_handle: new_file_handle}
  end

  defp write_to_file_with_rotation(file_handle, file_path, max_size_bytes, events) do
    # Check current file size
    case :file.position(file_handle, :eof) do
      {:ok, current_position} ->
        # Calculate size of events to write
        events_data = Enum.map_join(events, "\n", &Jason.encode!/1) <> "\n"
        events_size = byte_size(events_data)

        if current_position + events_size > max_size_bytes do
          # File will exceed limit after write - need rotation
          File.close(file_handle)
          rotate_file(file_path, max_size_bytes, events_data)
        else
          # Normal write
          IO.write(file_handle, events_data)
          :file.datasync(file_handle)
          file_handle
        end

      {:error, _} ->
        # File handle is invalid/terminated, write directly to file
        events_data = Enum.map_join(events, "\n", &Jason.encode!/1) <> "\n"
        File.write!(file_path, events_data, [:append])
        nil
    end
  end

  defp rotate_file(file_path, max_size_bytes, new_events_data) do
    new_events_size = byte_size(new_events_data)

    # If new events alone exceed max size, keep only them
    if new_events_size >= max_size_bytes do
      # Truncate to keep only last portion of new events
      start_pos = new_events_size - max_size_bytes
      truncated_data = binary_part(new_events_data, start_pos, max_size_bytes)

      # Find first complete line
      case String.split(truncated_data, "\n", parts: 2) do
        [_incomplete, rest] ->
          File.write!(file_path, rest)

        _ ->
          File.write!(file_path, truncated_data)
      end
    else
      # Read existing file and keep last N lines that fit
      existing_content = File.read!(file_path)
      existing_lines = String.split(existing_content, "\n", trim: true)

      # Calculate how much space we have for old events
      available_for_old = max_size_bytes - new_events_size

      # Take lines from the end while they fit
      {kept_lines, _} =
        Enum.reduce_while(Enum.reverse(existing_lines), {[], 0}, fn line, {acc, size} ->
          line_size = byte_size(line) + 1

          # +1 for newline
          if size + line_size <= available_for_old do
            {:cont, {[line | acc], size + line_size}}
          else
            {:halt, {acc, size}}
          end
        end)

      # Write kept old lines + new events
      final_content = Enum.join(kept_lines, "\n") <> if(kept_lines == [], do: "", else: "\n") <> new_events_data
      File.write!(file_path, final_content)
    end

    # Reopen file in append mode
    case File.open(file_path, [:append, :utf8]) do
      {:ok, handle} ->
        handle

      {:error, reason} ->
        Logger.error(I18n.t(:audit_log_file_reopen_failed, reason: inspect(reason)))
        nil
    end
  end
end
