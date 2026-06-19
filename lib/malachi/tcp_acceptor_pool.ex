defmodule Malachi.TCPAcceptorPool do
  @moduledoc """
  TCP/TLS acceptor pool - quantity based on available cores.
  Supports both plain TCP and TLS encrypted connections.
  """
  use Supervisor
  require Logger
  alias Malachi.I18n

  def start_link(port) do
    Supervisor.start_link(__MODULE__, port, name: __MODULE__)
  end

  @impl true
  def init(port) do
    buffer_size = Application.get_env(:malachi, :tcp_buffer_size, 32_768)
    backlog = Application.get_env(:malachi, :tcp_backlog, 4096)
    send_timeout = Application.get_env(:malachi, :tcp_send_timeout, 30_000)
    enable_tls = Application.get_env(:malachi, :enable_tls, false)

    base_opts = [
      :binary,
      packet: :line,
      active: false,
      reuseaddr: true,
      reuseport: true,
      nodelay: true,
      keepalive: true,
      send_timeout: send_timeout,
      send_timeout_close: true,
      sndbuf: buffer_size,
      recbuf: buffer_size,
      backlog: backlog
    ]

    {transport, opts} =
      if enable_tls do
        tls_opts = get_tls_options()
        {:ssl, base_opts ++ tls_opts}
      else
        {:gen_tcp, base_opts}
      end

    listen_result =
      case transport do
        :ssl -> :ssl.listen(port, opts)
        :gen_tcp -> :gen_tcp.listen(port, opts)
      end

    case listen_result do
      {:ok, test_socket} ->
        # Close the test socket - each acceptor will create its own
        case transport do
          :ssl -> :ssl.close(test_socket)
          :gen_tcp -> :gen_tcp.close(test_socket)
        end

        num_acceptors = System.schedulers_online()
        transport_name = if enable_tls, do: "TLS", else: "TCP"
        Logger.info(I18n.t(:tcp_server_started, port: port, acceptors: num_acceptors))
        Logger.info(I18n.t(:transport_enabled, transport: transport_name, port: port))

        children =
          for i <- 1..num_acceptors do
            Supervisor.child_spec(
              {Malachi.TCPAcceptor, {port, opts, i, transport}},
              id: {:acceptor, i}
            )
          end

        Supervisor.init(children, strategy: :one_for_one)

      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp get_tls_options do
    certfile = Application.get_env(:malachi, :tls_certfile, "priv/cert/server.crt")
    keyfile = Application.get_env(:malachi, :tls_keyfile, "priv/cert/server.key")
    cacertfile = Application.get_env(:malachi, :tls_cacertfile)
    versions = Application.get_env(:malachi, :tls_versions, [:"tlsv1.3", :"tlsv1.2"])

    tls_opts = [
      certfile: certfile,
      keyfile: keyfile,
      versions: versions,
      ciphers: :ssl.cipher_suites(:default, :"tlsv1.3") ++ :ssl.cipher_suites(:default, :"tlsv1.2"),
      secure_renegotiate: true,
      reuse_sessions: true,
      honor_cipher_order: true
    ]

    tls_opts =
      if cacertfile do
        tls_opts ++ [cacertfile: cacertfile]
      else
        tls_opts
      end

    # Configure verify mode
    verify_mode =
      case Application.get_env(:malachi, :tls_verify, "verify_none") do
        "verify_peer" -> :verify_peer
        :verify_peer -> :verify_peer
        _ -> :verify_none
      end

    fail_if_no_peer_cert = Application.get_env(:malachi, :tls_fail_if_no_peer_cert, false)

    tls_opts ++ [verify: verify_mode, fail_if_no_peer_cert: fail_if_no_peer_cert, depth: 2]
  end
end
