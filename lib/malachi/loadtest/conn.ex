defmodule Malachi.Loadtest.Conn do
  @moduledoc """
  One load-generator connection: a raw TCP or TLS socket speaking the Malachi wire protocol, plus a
  receive buffer so frames are peeled correctly across partial reads. The protocol is stateful, one
  auth frame up front and then plain requests, so `authenticate/2` runs once after `connect/1`.

  A `%Conn{}` is transport-agnostic (`:gen_tcp` or `:ssl`); `request/4` is the closed-loop round-trip,
  while `send_frame/4` + `recv_frame/1` are split for pipelining (many in-flight correlation ids).

  TLS verifies the server by default. `tls: true` alone used to mean `verify: :verify_none`, which is
  encryption without authentication: anyone on the path presents their own certificate, the client
  accepts it, and reads and rewrites the traffic. Now the server's certificate is always checked, and
  its hostname with it (a certificate legitimately issued for another host is not a match), against
  `:cacert` when one is given and against the operating system's trust store otherwise. `insecure:
  true` is the single remaining way to skip that, and it has to be asked for.
  """

  alias Malachi.Wire

  @enforce_keys [:transport, :socket]
  defstruct [:transport, :socket, buffer: ""]

  @type t :: %__MODULE__{transport: :gen_tcp | :ssl, socket: term(), buffer: binary()}

  @recv_timeout 15_000

  @doc """
  Connects to `host:port`. With `tls: true` uses `:ssl`, verifying the server against `:cacert` or the
  system trust store (and checking its hostname) unless `insecure: true`, and attaches a client cert
  (`:cert`/`:key`) for mTLS.
  """
  @spec connect(keyword()) :: {:ok, t()} | {:error, term()}
  def connect(opts) do
    host = to_charlist(Keyword.get(opts, :host, "127.0.0.1"))
    port = Keyword.get(opts, :port, 4040)

    if Keyword.get(opts, :tls, false) do
      tls_connect(host, port, opts)
    else
      case :gen_tcp.connect(host, port, socket_opts(), @recv_timeout) do
        {:ok, socket} -> {:ok, %__MODULE__{transport: :gen_tcp, socket: socket}}
        {:error, _} = err -> err
      end
    end
  end

  defp tls_connect(host, port, opts) do
    {:ok, _} = Application.ensure_all_started(:ssl)

    # A half-configured mTLS pair (cert without key, or the reverse) would reach :ssl.connect as
    # `keyfile: nil` and fail with a cryptic option error on OTP 28; name the mistake instead.
    with :ok <- validate_cert_pair(opts),
         {:ok, ssl_opts} <- ssl_opts(host, opts) do
      case :ssl.connect(host, port, ssl_opts, @recv_timeout) do
        {:ok, socket} -> {:ok, %__MODULE__{transport: :ssl, socket: socket}}
        {:error, reason} -> {:error, explain_tls_error(reason, opts)}
      end
    end
  end

  defp validate_cert_pair(opts) do
    case {Keyword.get(opts, :cert), Keyword.get(opts, :key)} do
      {cert, key} when (is_binary(cert) and is_nil(key)) or (is_nil(cert) and is_binary(key)) ->
        {:error, :cert_requires_key}

      _pair ->
        :ok
    end
  end

  # Mirrors the server's listen options (tcp_acceptor_pool): nodelay so Nagle never delays a small
  # request frame behind an unacked send (a closed-loop round trip is exactly that shape), and 32k
  # send/receive buffers to match the server's.
  defp socket_opts do
    [:binary, packet: 0, active: false, nodelay: true, sndbuf: 32_768, recbuf: 32_768]
  end

  @doc false
  # Public only so the security properties below can be asserted without a live server. Returns
  # {:ok, opts} or {:error, reason}: building these options can fail (no system trust store), and a
  # connection must not be attempted on a half-built verification policy.
  @spec ssl_opts(charlist(), keyword()) :: {:ok, keyword()} | {:error, term()}
  def ssl_opts(host, opts) do
    cert =
      case {Keyword.get(opts, :cert), Keyword.get(opts, :key)} do
        {nil, _} -> []
        {certfile, keyfile} -> [certfile: certfile, keyfile: keyfile]
      end

    with {:ok, verify} <- verify_opts(host, opts) do
      {:ok, socket_opts() ++ verify ++ cert}
    end
  end

  # How the server is authenticated. Verification is the default and skipping it is opt-in, because the
  # failure mode of the old default was silent: an unauthenticated TLS connection looks exactly like an
  # authenticated one from the caller's side, right up until someone is on the path.
  defp verify_opts(host, opts) do
    cond do
      Keyword.get(opts, :insecure, false) ->
        {:ok, [verify: :verify_none]}

      cacert = Keyword.get(opts, :cacert) ->
        {:ok, [verify: :verify_peer, cacertfile: cacert] ++ hostname_opts(host)}

      true ->
        # No CA named: trust what the operating system trusts, the same set a browser or curl would
        # use. A privately signed broker certificate is not in there, which is the point: it fails
        # closed, and explain_tls_error/2 turns that failure into the two ways out.
        case system_cacerts() do
          {:ok, cacerts} -> {:ok, [verify: :verify_peer, cacerts: cacerts] ++ hostname_opts(host)}
          {:error, _} = err -> err
        end
    end
  end

  defp system_cacerts do
    case :public_key.cacerts_get() do
      [] -> {:error, :no_system_trust_store}
      cacerts -> {:ok, cacerts}
    end
  rescue
    # cacerts_get/0 raises where the OS store is missing or unreadable. Refusing beats falling back to
    # an unverified connection, which is the bug this function exists to close.
    _ -> {:error, :no_system_trust_store}
  end

  # Checking the chain is only half of it: a certificate legitimately issued for another host still
  # chains to a trusted CA. SNI carries the name we asked for and the match_fun holds the certificate
  # to it, so an attacker cannot present a valid certificate for a host we did not ask to reach.
  defp hostname_opts(host) do
    [
      server_name_indication: host,
      customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]
    ]
  end

  # A verification failure is the expected outcome of pointing the client at a privately signed broker
  # without naming its CA, so it says what to do rather than surfacing a raw TLS alert.
  defp explain_tls_error({:tls_alert, {alert, _details}} = reason, opts)
       when alert in [:bad_certificate, :unknown_ca, :certificate_expired, :handshake_failure] do
    hint =
      if Keyword.get(opts, :cacert),
        do: "the server certificate did not verify against --cacert (wrong CA, or a hostname it is not issued for)",
        else:
          "the server certificate is not signed by a CA the system trusts: pass --cacert for a private CA, or --insecure for a development server"

    {:tls_verification_failed, hint, reason}
  end

  defp explain_tls_error(reason, _opts), do: reason

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
