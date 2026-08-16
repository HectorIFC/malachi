defmodule Malachi.Loadtest.Conn do
  @moduledoc """
  One load-generator connection: a raw TCP or TLS socket speaking the Malachi wire protocol, plus a
  receive buffer so frames are peeled correctly across partial reads. The protocol is stateful, one
  auth frame up front and then plain requests, so `authenticate/2` runs once after `connect/1`.

  A `%Conn{}` is transport-agnostic (`:gen_tcp` or `:ssl`); `request/4` is the closed-loop round-trip,
  while `send_frame/4` + `recv_frame/1` are split for pipelining (many in-flight correlation ids).
  """

  alias Malachi.Wire

  @enforce_keys [:transport, :socket]
  defstruct [:transport, :socket, buffer: ""]

  @type t :: %__MODULE__{transport: :gen_tcp | :ssl, socket: term(), buffer: binary()}

  @recv_timeout 15_000

  @doc """
  Connects to `host:port`. With `tls: true` uses `:ssl` (verify_peer when `:cacert` is given, else
  verify_none for dev), and attaches a client cert (`:cert`/`:key`) for mTLS.
  """
  @spec connect(keyword()) :: {:ok, t()} | {:error, term()}
  def connect(opts) do
    host = to_charlist(Keyword.get(opts, :host, "127.0.0.1"))
    port = Keyword.get(opts, :port, 4040)

    if Keyword.get(opts, :tls, false) do
      {:ok, _} = Application.ensure_all_started(:ssl)

      case :ssl.connect(host, port, ssl_opts(opts), @recv_timeout) do
        {:ok, socket} -> {:ok, %__MODULE__{transport: :ssl, socket: socket}}
        {:error, _} = err -> err
      end
    else
      case :gen_tcp.connect(host, port, socket_opts(), @recv_timeout) do
        {:ok, socket} -> {:ok, %__MODULE__{transport: :gen_tcp, socket: socket}}
        {:error, _} = err -> err
      end
    end
  end

  # Mirrors the server's listen options (tcp_acceptor_pool): nodelay so Nagle never delays a small
  # request frame behind an unacked send (a closed-loop round trip is exactly that shape), and 32k
  # send/receive buffers to match the server's.
  defp socket_opts do
    [:binary, packet: 0, active: false, nodelay: true, sndbuf: 32_768, recbuf: 32_768]
  end

  defp ssl_opts(opts) do
    base = socket_opts()

    verify =
      case Keyword.get(opts, :cacert) do
        nil -> [verify: :verify_none]
        cacert -> [verify: :verify_peer, cacertfile: cacert]
      end

    cert =
      case {Keyword.get(opts, :cert), Keyword.get(opts, :key)} do
        {nil, _} -> []
        {certfile, keyfile} -> [certfile: certfile, keyfile: keyfile]
      end

    base ++ verify ++ cert
  end

  @doc """
  Authenticates the connection per the opts: `:token` -> token_auth, `:cert` (with tls) -> mtls_auth,
  else user/pass auth. Returns `{:ok, conn}` (the token is discarded, the session is on the socket) or
  `{:error, reason}`.
  """
  @spec authenticate(t(), keyword()) :: {:ok, t()} | {:error, term()}
  def authenticate(conn, opts) do
    {api_key, payload} =
      cond do
        Keyword.get(opts, :token) ->
          {Wire.token_auth_key(), Wire.encode_token_auth_req(Keyword.fetch!(opts, :token))}

        Keyword.get(opts, :cert) ->
          {Wire.mtls_auth_key(), Wire.encode_mtls_auth_req()}

        true ->
          {Wire.auth_key(),
           Wire.encode_auth_req(Keyword.get(opts, :user, "admin"), Keyword.get(opts, :pass, "admin123"))}
      end

    case request(conn, api_key, 1, payload) do
      {:ok, error_code, resp, conn} ->
        if error_code == Wire.ok_code(), do: {:ok, conn}, else: {:error, Wire.decode_auth_resp(resp)}

      {:error, _} = err ->
        err
    end
  end

  @doc "Sends a request frame without waiting for a response (pipelining)."
  @spec send_frame(t(), non_neg_integer(), non_neg_integer(), binary()) :: :ok | {:error, term()}
  def send_frame(%__MODULE__{transport: t, socket: s}, api_key, correlation_id, payload) do
    tsend(t, s, Wire.encode_request(api_key, correlation_id, payload))
  end

  @doc """
  Reads one wire frame body, accumulating bytes across partial reads. Returns `{:ok, body, conn}` with
  the leftover buffer carried in `conn`, or `{:error, reason}`.
  """
  @spec recv_frame(t(), timeout()) :: {:ok, binary(), t()} | {:error, term()}
  def recv_frame(conn, timeout \\ @recv_timeout)

  def recv_frame(%__MODULE__{buffer: buffer} = conn, timeout) do
    case Wire.decode_frame(buffer) do
      {:ok, body, rest} ->
        {:ok, body, %{conn | buffer: rest}}

      :incomplete ->
        case trecv(conn.transport, conn.socket, timeout) do
          {:ok, data} -> recv_frame(%{conn | buffer: buffer <> data}, timeout)
          {:error, _} = err -> err
        end
    end
  end

  @doc "Closed-loop round-trip: send a request, read its response. Returns `{:ok, error_code, payload, conn}`."
  @spec request(t(), non_neg_integer(), non_neg_integer(), binary()) ::
          {:ok, non_neg_integer(), binary(), t()} | {:error, term()}
  def request(conn, api_key, correlation_id, payload) do
    with :ok <- send_frame(conn, api_key, correlation_id, payload),
         {:ok, body, conn} <- recv_frame(conn) do
      {_corr, error_code, resp} = Wire.decode_response(body)
      {:ok, error_code, resp, conn}
    end
  end

  @spec close(t()) :: :ok
  def close(%__MODULE__{transport: t, socket: s}), do: tclose(t, s)

  # Transport dispatch (explicit, so the two socket backends are obvious and no `apply/3` is needed).
  defp tsend(:gen_tcp, socket, data), do: :gen_tcp.send(socket, data)
  defp tsend(:ssl, socket, data), do: :ssl.send(socket, data)

  defp trecv(:gen_tcp, socket, timeout), do: :gen_tcp.recv(socket, 0, timeout)
  defp trecv(:ssl, socket, timeout), do: :ssl.recv(socket, 0, timeout)

  defp tclose(:gen_tcp, socket), do: :gen_tcp.close(socket)
  defp tclose(:ssl, socket), do: :ssl.close(socket)
end
