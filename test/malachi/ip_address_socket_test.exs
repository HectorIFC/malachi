defmodule Malachi.IPAddressSocketTest do
  @moduledoc false

  # Socket-level proof that a real IPv6 peer is keyed canonically, end to end, through the same
  # functions production uses. These open their own :inet6 loopback listeners: nothing in lib/ or
  # config/ listens on IPv6, and this change deliberately does not add that (see #182, which lists
  # "do not enable an IPv6 listener until #102 lands" as an operating condition).
  #
  # Tagged :linux because measurements on this project only count on Linux, and because the
  # v4-mapped case depends on the host's ipv6_v6only default. A Linux environment without IPv6 makes
  # these fail loudly rather than skip silently: a test that disappears from the report looks exactly
  # like a test that passed.
  use ExUnit.Case, async: false

  @moduletag :linux

  alias Malachi.AuditLog
  alias Malachi.Auth.LockoutManager
  alias Malachi.ConnectionLimiter
  alias Malachi.IPAddress

  # The loopback v6 address every case below connects from.
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

  describe "from_socket/2 over a real IPv6 peer" do
    test "an IPv6 loopback client is keyed as ::1, not as the expanded form" do
      %{server: server} = accept_pair([:inet6], @loopback6, [:inet6])

      formatted = IPAddress.from_socket(server, :gen_tcp)

      assert formatted == "::1"
      # The shape the manual formatters produced. Asserting against it directly is what makes this
      # test fail on the code this change replaces.
      refute formatted == "0:0:0:0:0:0:0:1"
    end

    test "an IPv4 client on a dual-stack listener is keyed as the canonical mapped form" do
      # The case the issue calls out as mattering most: a listener on :: accepting an IPv4 client
      # gets an eight-element tuple, and it is exactly where the two implementations diverged most.
      %{server: server} = accept_pair([:inet6, {:ipv6_v6only, false}], {127, 0, 0, 1}, [:inet])

      assert {:ok, {peer, _port}} = :inet.peername(server)
      assert tuple_size(peer) == 8, "expected a v4-mapped peer tuple, got #{inspect(peer)}"

      formatted = IPAddress.from_socket(server, :gen_tcp)

      assert formatted == "::ffff:127.0.0.1"
      refute formatted == "0:0:0:0:0:FFFF:7F00:1"
    end

    test "an IPv4 client on an IPv4 listener is unchanged by this change" do
      %{server: server} = accept_pair([:inet], {127, 0, 0, 1}, [:inet])

      assert IPAddress.from_socket(server, :gen_tcp) == "127.0.0.1"
    end

    test "a peer that cannot be read answers unknown instead of raising" do
      %{client: client, server: server} = accept_pair([:inet6], @loopback6, [:inet6])
      :gen_tcp.close(client)
      :gen_tcp.close(server)

      assert IPAddress.from_socket(server, :gen_tcp) == "unknown"
    end
  end

  describe "from_socket/2 over TLS" do
    setup do
      dir = Path.join(System.tmp_dir!(), "malachi_ip_address_tls_#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      certfile = Path.join(dir, "cert.pem")
      keyfile = Path.join(dir, "key.pem")

      # A throwaway self-signed cert generated per run, exactly as tcp_acceptor_tls_test.exs does:
      # the dist certs under priv/dist_cert are gitignored, so they are absent in CI.
      {_out, 0} =
        System.cmd(
          "openssl",
          [
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            keyfile,
            "-out",
            certfile,
            "-days",
            "1",
            "-subj",
            "/CN=localhost"
          ],
          stderr_to_stdout: true
        )

      on_exit(fn -> File.rm_rf!(dir) end)
      {:ok, certfile: certfile, keyfile: keyfile}
    end

    test "the TLS transport is read through the same function and formatted the same way", ctx do
      listen_opts = [
        :binary,
        :inet6,
        packet: 0,
        active: false,
        reuseaddr: true,
        certfile: String.to_charlist(ctx.certfile),
        keyfile: String.to_charlist(ctx.keyfile),
        versions: [:"tlsv1.3", :"tlsv1.2"]
      ]

      {:ok, listener} = :ssl.listen(0, listen_opts)
      {:ok, {_address, port}} = :ssl.sockname(listener)

      task =
        Task.async(fn ->
          {:ok, socket} = :ssl.transport_accept(listener, 5_000)
          {:ok, server} = :ssl.handshake(socket, 5_000)
          IPAddress.from_socket(server, :ssl)
        end)

      {:ok, client} =
        :ssl.connect(~c"::1", port, [:binary, :inet6, active: false, verify: :verify_none], 5_000)

      formatted = Task.await(task, 10_000)

      :ssl.close(client)
      :ssl.close(listener)

      assert formatted == "::1"
    end
  end
end
