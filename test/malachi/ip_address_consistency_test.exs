defmodule Malachi.IPAddressConsistencyTest do
  @moduledoc """
  One address, one key, across every consumer.

  This is the property the six divergent formatters threatened, and the reason the issue calls the
  duplication security-relevant rather than cosmetic. The connection limiter's per-IP bucket, the
  replicated lockout key and the audit log's `ip` field are three different subsystems that key off
  the same string, and they used to derive it from three different implementations. For an IPv6 peer
  they disagreed, so one client would have occupied two limiter buckets while the lockout manager
  counted its attempts under a third spelling.

  Driven from a real accepted socket rather than from a literal tuple, because the divergence only
  showed up on what `:inet.peername/1` actually returns.
  """
  use ExUnit.Case, async: false

  @moduletag :linux

  alias Malachi.AuditLog
  alias Malachi.Auth.LockoutManager
  alias Malachi.ConnectionLimiter
  alias Malachi.IPAddress

  @loopback6 {0, 0, 0, 0, 0, 0, 0, 1}

  defp accept_pair(listen_opts, connect_address, connect_opts) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true] ++ listen_opts)
    {:ok, port} = :inet.port(listener)
    {:ok, client} = :gen_tcp.connect(connect_address, port, [:binary, active: false] ++ connect_opts, 2_000)
    {:ok, server} = :gen_tcp.accept(listener, 2_000)

    on_exit(fn ->
      :gen_tcp.close(client)
      :gen_tcp.close(server)
      :gen_tcp.close(listener)
    end)

    %{client: client, server: server, listener: listener, port: port}
  end

  describe "one address, one key, across every consumer" do
    setup do
      username = "ipv6_consistency_#{System.unique_integer([:positive])}"
      on_exit(fn -> LockoutManager.unlock_account(username, :all) end)
      {:ok, username: username}
    end

    test "the limiter, the lockout store and the audit log derive the same string", ctx do
      %{server: server} = accept_pair([:inet6], @loopback6, [:inet6])

      # Exactly what TCPAcceptor.handle_client/2 does with a fresh peer.
      client_ip = IPAddress.from_socket(server, :gen_tcp)
      assert client_ip == "::1"

      # The connection limiter's per-IP bucket.
      assert :ok = ConnectionLimiter.register_connection(self(), client_ip)
      on_exit(fn -> ConnectionLimiter.unregister_connection(self()) end)
      assert Map.has_key?(ConnectionLimiter.list_connections(), client_ip)

      # The replicated lockout key. Formatting the raw peer tuple has to land on the same string the
      # acceptor already computed, or the lockout store stops recognising repeated attempts from one
      # address the moment any caller passes a tuple instead of the formatted binary.
      assert {:ok, {peer, _port}} = :inet.peername(server)
      assert IPAddress.format(peer) == client_ip

      # The audit log's ip field.
      AuditLog.log_event(:auth_failure, %{username: ctx.username, ip: peer}, "ipv6_consistency", :failure, %{})
      AuditLog.flush()

      event =
        ctx.username
        |> AuditLog.get_events_by_user()
        |> Enum.find(&(&1.action == "ipv6_consistency"))

      assert event, "the audit event was not recorded"
      assert event.ip == client_ip
    end
  end
end
