defmodule Malachi.TCPAcceptorPool do
  @moduledoc """
  Supervises a pool of `Malachi.TCPAcceptor`s listening on the same port (plain TCP or TLS).

  On start it opens one throwaway listen socket to validate the transport/TLS options, failing fast
  if they are wrong: closes it, then starts one acceptor per online scheduler, each opening its own
  listen socket on the shared port (`reuseport`). Plain TCP or TLS is chosen from `:enable_tls`; the
  TLS options (cert/key files, protocol versions, ciphers, peer verification) are read from config.

  Port 0 asks the operating system for a free port, which is what the test environment does so that two
  test runs on one host never fight for a fixed one. The throwaway socket is where the port is actually
  bound, so the pool reads the number back from it and hands that number, never 0, to every acceptor: the
  acceptors all share one port, and one that restarts comes back on it. `port/1` answers that number.
  """
  use Supervisor
  require Logger
  alias Malachi.I18n

  @doc """
  Starts the acceptor pool listening on `port`, registered under the module name. `{port, name: name}`
  registers it under `name` instead, which is how a test runs a pool beside the application's.
  """
  @spec start_link(:inet.port_number() | {:inet.port_number(), keyword()}) :: Supervisor.on_start()
  def start_link({port, opts}) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, {port, name}, name: name)
  end

  def start_link(port), do: start_link({port, []})

  @doc """
  The port the pool registered as `name` bound, which differs from the configured one when that was 0.
  After the pool stops this is still the last port it bound; nil if it never started.
  """
  @spec port(atom()) :: :inet.port_number() | nil
  def port(name \\ __MODULE__), do: :persistent_term.get({__MODULE__, name}, nil)

  @impl true
  def init({port, name}) do
    buffer_size = Application.get_env(:malachi, :tcp_buffer_size, 32_768)
    backlog = Application.get_env(:malachi, :tcp_backlog, 4096)
    send_timeout = Application.get_env(:malachi, :tcp_send_timeout, 30_000)
    enable_tls = Application.get_env(:malachi, :enable_tls, false)

    base_opts = [
      :binary,
      # raw mode: the binary Malachi.Wire protocol frames messages itself (length-prefixed), so we do not
      # want line framing.
      packet: 0,
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
        # Read the bound port before closing the test socket (each acceptor then creates its own on it).
        # A closed socket has no port to ask for, and with port 0 this is the only place the number exists.
        {:ok, bound} = bound_port(transport, test_socket)

        case transport do
          :ssl -> :ssl.close(test_socket)
          :gen_tcp -> :gen_tcp.close(test_socket)
        end

        :persistent_term.put({__MODULE__, name}, bound)

        num_acceptors = System.schedulers_online()
        transport_name = if enable_tls, do: "TLS", else: "TCP"
        Logger.info(I18n.t(:tcp_server_started, port: bound, acceptors: num_acceptors))
        Logger.info(I18n.t(:transport_enabled, transport: transport_name, port: bound))

        children =
          for i <- 1..num_acceptors do
            Supervisor.child_spec(
              {Malachi.TCPAcceptor, {bound, opts, i, transport}},
              id: {:acceptor, i}
            )
          end

        Supervisor.init(children, strategy: :one_for_one)

      {:error, reason} ->
        # A supervisor's init may only answer `{:ok, spec}` or `:ignore`: `{:stop, reason}` is a bad
        # return, which buried the reason (`:eaddrinuse`, say) inside `{:bad_return, ...}`. Exiting makes
        # `start_link/1` answer `{:error, reason}` instead.
        exit(reason)
    end
  end

  # `:inet.port/1` does not accept an `:ssl` socket; `:ssl.sockname/1` answers the same for it.
  defp bound_port(:gen_tcp, socket), do: :inet.port(socket)

  defp bound_port(:ssl, socket) do
    with {:ok, {_address, port}} <- :ssl.sockname(socket), do: {:ok, port}
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
