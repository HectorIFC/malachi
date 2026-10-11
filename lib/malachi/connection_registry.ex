defmodule Malachi.ConnectionRegistry do
  @moduledoc """
  Tracks active client connections for graceful shutdown.
  Uses ETS for fast concurrent access.
  """
  use GenServer
  require Logger
  alias Malachi.I18n

  @table :malachi_connections

  @doc "Starts the registry: a named, ETS-backed `GenServer` (started once for the node)."
  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Register a client connection with its socket, transport and canonical client address.

  The address is passed in rather than read here: the acceptor has already read the peer to key the
  connection limiter (`Malachi.TCPAcceptor`), and reading it a second time would both cost a second
  `getpeername` per connection and let the registry record `"unknown"` for a live connection whose
  socket died in between. Use `Malachi.IPAddress.from_socket/2` to produce it.
  """
  @spec register(pid(), :gen_tcp.socket() | :ssl.sslsocket(), :gen_tcp | :ssl, String.t()) ::
          :ok
  def register(pid, socket, transport, client_ip) when is_binary(client_ip) do
    entry = {pid, socket, transport, System.monotonic_time(:millisecond), client_ip}
    # Register through the GenServer so Process.monitor/1 runs in the registry process, not the caller.
    # Otherwise the :DOWN would be delivered to the connection handler and the entry would never be pruned.
    GenServer.call(__MODULE__, {:register, entry})
  end

  @doc """
  Unregister a client connection.
  """
  def unregister(pid) do
    :ets.delete(@table, pid)
    :ok
  end

  @doc """
  Get count of active connections.
  """
  def count do
    :ets.info(@table, :size)
  end

  @doc """
  Close all active connections gracefully.
  Called during application shutdown.
  """
  def close_all do
    connections = :ets.tab2list(@table)
    Logger.info(I18n.t(:closing_connections, count: length(connections)))

    for entry <- connections do
      # matched in the body, not in the generator, where a row of another shape would be skipped silently
      {pid, socket, transport, _connected_at, _ip} = entry
      # Send shutdown notification to client
      try do
        shutdown_msg = Jason.encode!(%{"shutdown" => true, "reason" => "server_restart"})

        case transport do
          :ssl -> :ssl.send(socket, shutdown_msg <> "\n")
          :gen_tcp -> :gen_tcp.send(socket, shutdown_msg <> "\n")
        end

        # Give client time to receive the message
        Process.sleep(50)

        # Close the socket
        case transport do
          :ssl -> :ssl.close(socket)
          :gen_tcp -> :gen_tcp.close(socket)
        end
      rescue
        _ -> :ok
      catch
        _, _ -> :ok
      end

      # Stop the handler process
      Process.exit(pid, :shutdown)
    end

    :ets.delete_all_objects(@table)
    :ok
  end

  # GenServer callbacks

  @impl true
  def init(_opts) do
    :ets.new(@table, [
      :named_table,
      :public,
      :set,
      read_concurrency: true,
      write_concurrency: true
    ])

    {:ok, %{}}
  end

  @impl true
  def handle_call({:register, {pid, _socket, _transport, _connected_at, _ip} = entry}, _from, state) do
    :ets.insert(@table, entry)
    Process.monitor(pid)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    unregister(pid)
    {:noreply, state}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end
end
