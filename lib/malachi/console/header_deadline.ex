defmodule Malachi.Console.HeaderDeadline do
  @moduledoc """
  One deadline for the request line and the whole header block of every request on a console
  connection, the guarantee `Malachi.HTTP.Limits.recv_timeout_ms/0` describes and the dashboard keeps.

  Bandit only has a per-read timeout, restarted on every chunk it reads, so a client that sends one
  header byte just inside it holds a connection for as long as the header limits allow, which is
  weeks (slowloris). This module closes that gap without changing Bandit:

    * It is the Thousand Island handler of the console endpoint, delegating every callback to
      Bandit's own delegating handler. When a connection starts, it spawns a watchdog process for it and
      registers the watchdog under the connection's pid in `registry/0`.
    * The watchdog is armed from the start: if the deadline passes, it closes the socket. A
      connection blocked reading headers sees its read fail and ends; one waiting between keep alive
      requests receives the exit of its own socket and stops with `{:shutdown, :header_deadline}`.
    * `Malachi.Console.Router` disarms it when a request reaches the plug, which is when every header
      has been read, and arms it again once the response has been sent, so the idle wait for the next
      request and that request's headers share one deadline, whether the next request arrives later
      or was already pipelined behind this one.

  The watchdog is registered in a `Registry` rather than the process dictionary because Bandit clears
  the dictionary between keep alive requests. It monitors its connection and exits with it.

  Only HTTP/1 is served (see `Malachi.Console.Endpoint`): under HTTP/2 a request runs in a process of
  its own, so the plug could not disarm the connection's watchdog.
  """

  use ThousandIsland.Handler

  alias Bandit.DelegatingHandler
  alias Malachi.HTTP.Limits

  @registry Malachi.Console.HeaderDeadlines

  @doc "The name of the `Registry` the watchdogs are registered in, keyed by connection pid."
  @spec registry() :: atom()
  def registry, do: @registry

  @doc "Stops the calling connection's deadline, if it has one. A request has finished its headers."
  @spec disarm() :: :ok
  def disarm, do: notify(:disarm)

  @doc "Starts the calling connection's deadline again, if it has one. A response has been sent."
  @spec arm() :: :ok
  def arm, do: notify(:arm)

  defp notify(message) do
    case Registry.lookup(@registry, self()) do
      [{_connection, watchdog}] -> send(watchdog, message)
      [] -> :ok
    end

    :ok
  end

  @impl ThousandIsland.Handler
  def handle_connection(socket, state) do
    connection = self()
    deadline = Limits.recv_timeout_ms()
    watchdog = spawn(fn -> watch(connection, socket, deadline) end)
    {:ok, _owner} = Registry.register(@registry, connection, watchdog)

    DelegatingHandler.handle_connection(socket, state)
  end

  @impl ThousandIsland.Handler
  def handle_data(data, socket, state), do: DelegatingHandler.handle_data(data, socket, state)

  @impl ThousandIsland.Handler
  def handle_close(socket, state), do: DelegatingHandler.handle_close(socket, state)

  @impl ThousandIsland.Handler
  def handle_error(error, socket, state), do: DelegatingHandler.handle_error(error, socket, state)

  @impl ThousandIsland.Handler
  def handle_shutdown(socket, state), do: DelegatingHandler.handle_shutdown(socket, state)

  @impl ThousandIsland.Handler
  def handle_timeout(socket, state), do: DelegatingHandler.handle_timeout(socket, state)

  # Messages the connection process receives outside Thousand Island's own go to Bandit, as
  # Bandit.DelegatingHandler routes them, so wrapping it changes nothing but the deadline.
  @impl GenServer
  def handle_call(msg, from, state), do: DelegatingHandler.handle_call(msg, from, state)

  @impl GenServer
  def handle_cast(msg, state), do: DelegatingHandler.handle_cast(msg, state)

  # The watchdog closing the socket while the connection waits for its next request (between keep
  # alive requests, the socket is in active mode rather than a blocking read) reaches the connection,
  # which traps exits, as an exit from its own port. Bandit ignores that message, so without this
  # clause the connection would linger until its read timeout; it stops here instead.
  @impl GenServer
  def handle_info({:EXIT, port, _reason}, {%ThousandIsland.Socket{socket: port} = socket, state}) do
    {:stop, {:shutdown, :header_deadline}, {socket, state}}
  end

  def handle_info(msg, state), do: DelegatingHandler.handle_info(msg, state)

  defp watch(connection, socket, deadline) do
    monitor = Process.monitor(connection)
    armed(monitor, socket, deadline)
  end

  defp armed(monitor, socket, deadline) do
    receive do
      :disarm -> disarmed(monitor, socket, deadline)
      :arm -> armed(monitor, socket, deadline)
      {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
    after
      deadline -> socket.transport_module.close(socket.socket)
    end
  end

  defp disarmed(monitor, socket, deadline) do
    receive do
      :arm -> armed(monitor, socket, deadline)
      :disarm -> disarmed(monitor, socket, deadline)
      {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
    end
  end
end
