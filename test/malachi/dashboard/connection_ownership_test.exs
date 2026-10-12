defmodule Malachi.Dashboard.ConnectionOwnershipTest do
  # Each connection the dashboard accepts belongs to the process that serves it, not to the dashboard: a
  # socket left with the dashboard outlived a handler that ended without closing it.
  #
  # async: false. It counts the sockets the application's own dashboard holds.
  use ExUnit.Case, async: false

  alias Malachi.Test.PollingHelper

  defp dashboard_sockets do
    dashboard = Process.whereis(Malachi.Dashboard)

    for port <- :erlang.ports(),
        :erlang.port_info(port, :name) == {:name, ~c"tcp_inet"},
        :erlang.port_info(port, :connected) == {:connected, dashboard},
        do: port
  end

  test "an accepted connection is held by its handler, so the dashboard keeps only its listener" do
    before = length(dashboard_sockets())
    {:ok, client} = :gen_tcp.connect(~c"127.0.0.1", Malachi.Dashboard.port(), [:binary, active: false])
    on_exit(fn -> :gen_tcp.close(client) end)

    # accepted, with its handler waiting for the request line
    PollingHelper.wait_until!(fn ->
      Enum.any?(:erlang.ports(), fn port ->
        match?({:name, ~c"tcp_inet"}, :erlang.port_info(port, :name)) and
          :erlang.port_info(port, :connected) not in [
            {:connected, self()},
            {:connected, Process.whereis(Malachi.Dashboard)}
          ] and
          match?({:ok, {_, _}}, :inet.peername(port)) and
          :inet.peername(port) == :inet.sockname(client)
      end)
    end)

    assert length(dashboard_sockets()) == before
  end
end
