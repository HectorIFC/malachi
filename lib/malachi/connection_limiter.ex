defmodule Malachi.ConnectionLimiter do
  @moduledoc """
  Tracks active connections per IP address and globally.
  Rejects new connections when limits exceeded.

  ## Schema

  Uses three ETS tables:

  - `:malachi_conn_limits_ip` - `{ip, count}` per-IP counters
  - `:malachi_conn_limits_global` - `{:total, count}` global counter
  - `:malachi_conn_pids` - `{pid, ip, monitor_ref}` process tracking

  ## Configuration

  - `connection_limit_enabled` - Enable/disable connection limiting (default: true)
  - `max_connections_per_ip` - Max connections per IP (default: 100)
  - `max_total_connections` - Max total connections (default: 10000)
  """

  use GenServer
  require Logger
  alias Malachi.I18n

  @table_ip :malachi_conn_limits_ip
  @table_global :malachi_conn_limits_global
  @table_pids :malachi_conn_pids

  # ============================================================
  # PUBLIC API
  # ============================================================

  @doc """
  Starts the connection limiter GenServer.
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Register a new connection.

  Returns `:ok` if allowed, or error tuple if limit exceeded.

  ## Examples

      register_connection(self(), "192.168.1.1")
      #=> :ok

      register_connection(self(), "10.0.0.1")
      #=> {:error, :connection_limit_exceeded}
  """
  def register_connection(pid, ip) do
    if cfg(:connection_limit_enabled, true) do
      GenServer.call(__MODULE__, {:register, pid, ip})
    else
      :ok
    end
  end

  @doc """
  Unregister a connection manually.

  Usually not needed as process monitoring handles cleanup automatically.
  """
  def unregister_connection(pid) do
    GenServer.call(__MODULE__, {:unregister, pid})
  end

  @doc """
  Get connection count for specific IP.
  """
  def get_connection_count(ip) do
    case :ets.lookup(@table_ip, ip) do
      [{^ip, count}] -> count
      [] -> 0
    end
  end

  @doc """
  Get total connection count.
  """
  def get_total_connections do
    case :ets.lookup(@table_global, :total) do
      [{:total, count}] -> count
      [] -> 0
    end
  end

  @doc """
  Get statistics about connections.
  """
  def get_stats do
    GenServer.call(__MODULE__, :get_stats)
  end

  @doc """
  List all connections by IP.

  Returns map of `%{ip => count}`.
  """
  def list_connections do
    GenServer.call(__MODULE__, :list_connections)
  end

  @doc """
  Clears all tracked connections — the per-IP and global counters and the process monitors. This does not
  change limiting behavior; it only resets the tracked state, so it is primarily for tests that need a
  known-clean baseline (the tracker is process-global, shared with the live server and every other test).
  """
  def reset do
    GenServer.call(__MODULE__, :reset)
  end

  # ============================================================
  # GENSERVER CALLBACKS
  # ============================================================

  @impl true
  def init(_opts) do
    :ets.new(@table_ip, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(@table_global, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(@table_pids, [
      :set,
      :public,
      :named_table,
      read_concurrency: true
    ])

    # Initialize global counter
    :ets.insert(@table_global, {:total, 0})

    Logger.info(I18n.t(:connection_limiter_started))
    {:ok, %{}}
  end

  @impl true
  def handle_call({:register, pid, ip}, _from, state) do
    max_per_ip = cfg(:max_connections_per_ip, 100)
    max_total = cfg(:max_total_connections, 10_000)

    # Try to increment IP counter
    new_ip_count = :ets.update_counter(@table_ip, ip, {2, 1}, {ip, 0})

    if new_ip_count > max_per_ip do
      # Rollback IP increment
      :ets.update_counter(@table_ip, ip, {2, -1})
      {:reply, {:error, :connection_limit_exceeded}, state}
    else
      # Try to increment global counter
      new_total = :ets.update_counter(@table_global, :total, {2, 1})

      if new_total > max_total do
        # Rollback both counters
        :ets.update_counter(@table_ip, ip, {2, -1})
        :ets.update_counter(@table_global, :total, {2, -1})
        {:reply, {:error, :global_limit_exceeded}, state}
      else
        # Success - monitor process and track
        ref = Process.monitor(pid)
        :ets.insert(@table_pids, {pid, ip, ref})
        {:reply, :ok, state}
      end
    end
  end

  @impl true
  def handle_call({:unregister, pid}, _from, state) do
    case :ets.lookup(@table_pids, pid) do
      [{^pid, ip, ref}] ->
        Process.demonitor(ref, [:flush])
        decrement_counters(ip)
        :ets.delete(@table_pids, pid)
        {:reply, :ok, state}

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call(:get_stats, _from, state) do
    total = get_total_connections()
    per_ip_count = :ets.info(@table_ip, :size)

    stats = %{
      total_connections: total,
      unique_ips: per_ip_count,
      max_per_ip: cfg(:max_connections_per_ip, 100),
      max_total: cfg(:max_total_connections, 10_000)
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_call(:list_connections, _from, state) do
    connections =
      :ets.tab2list(@table_ip)
      |> Enum.into(%{})

    {:reply, connections, state}
  end

  @impl true
  def handle_call(:reset, _from, state) do
    # Demonitor everything we track (flushing any queued :DOWN so it can't decrement after the reset),
    # then clear the counters back to a clean, empty baseline.
    :ets.foldl(
      fn {_pid, _ip, ref}, _acc -> Process.demonitor(ref, [:flush]) end,
      :ok,
      @table_pids
    )

    :ets.delete_all_objects(@table_pids)
    :ets.delete_all_objects(@table_ip)
    :ets.insert(@table_global, {:total, 0})

    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    # Process died - cleanup counters
    case :ets.lookup(@table_pids, pid) do
      [{^pid, ip, _ref}] ->
        decrement_counters(ip)
        :ets.delete(@table_pids, pid)

      [] ->
        # Already cleaned up
        :ok
    end

    {:noreply, state}
  end

  # ============================================================
  # PRIVATE FUNCTIONS
  # ============================================================

  defp decrement_counters(ip) do
    # Decrement IP counter
    new_count = :ets.update_counter(@table_ip, ip, {2, -1})

    # Remove IP entry if count reaches 0
    if new_count <= 0 do
      :ets.delete(@table_ip, ip)
    end

    # Decrement global counter
    :ets.update_counter(@table_global, :total, {2, -1})
  end

  defp cfg(key, default) do
    Application.get_env(:malachi, key, default)
  end
end
