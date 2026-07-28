defmodule Malachi.TCPAcceptorTLSTest do
  # A real TLS client against a live TCPAcceptor: proves that after moving the TLS handshake out of the
  # acceptor and into the spawned connection process, the handshake still completes and the server serves
  # the client past it. There is no other end-to-end TLS test in the suite.
  use ExUnit.Case, async: false

  alias Malachi.Wire

  # A throwaway self-signed server cert/key generated per run. The dist certs under priv/dist_cert are
  # gitignored (a private key is never committed), so they are absent in CI; generating here keeps the test
  # self-contained. openssl ships on the CI image and is what the project's own cert scripts use.
  setup do
    dir = Path.join(System.tmp_dir!(), "malachi_tls_test_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    certfile = Path.join(dir, "cert.pem")
    keyfile = Path.join(dir, "key.pem")

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

  defp free_port do
    {:ok, s} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(s)
    :gen_tcp.close(s)
    port
  end

  defp ssl_listen_opts(certfile, keyfile) do
    [
      :binary,
      packet: 0,
      active: false,
      reuseaddr: true,
      certfile: String.to_charlist(certfile),
      keyfile: String.to_charlist(keyfile),
      versions: [:"tlsv1.3", :"tlsv1.2"]
    ]
  end

  defp recv_tls_frame(socket, buffer \\ "", timeout \\ 5_000) do
    case Wire.decode_frame(buffer) do
      {:ok, body, _rest} ->
        {:ok, body}

      :incomplete ->
        case :ssl.recv(socket, 0, timeout) do
          {:ok, data} -> recv_tls_frame(socket, buffer <> data, timeout)
          other -> other
        end
    end
  end

  test "TLS handshake completes in the connection process and the server serves the client", ctx do
    # Keep this IP's auth rate limit clear so the bogus-credentials attempt below reaches authentication.
    Malachi.RateLimiter.reset_bucket("127.0.0.1", :auth)

    port = free_port()
    {:ok, acceptor} = Malachi.TCPAcceptor.start_link({port, ssl_listen_opts(ctx.certfile, ctx.keyfile), 1, :ssl})
    on_exit(fn -> if Process.alive?(acceptor), do: GenServer.stop(acceptor) end)

    # let the acceptor enter its accept loop
    Process.sleep(50)

    client_opts = [:binary, packet: 0, active: false, verify: :verify_none, versions: [:"tlsv1.3", :"tlsv1.2"]]
    assert {:ok, socket} = :ssl.connect(~c"127.0.0.1", port, client_opts, 5_000)

    # Past the handshake the connection process runs the auth flow: a bogus-credentials auth request must
    # come back as an error frame with the same correlation id, proving the server served the client.
    # encode_request/3 already length-frames the request, so it is sent as-is.
    frame = Wire.encode_request(Wire.auth_key(), 1, Wire.encode_auth_req("nobody", "nope"))
    assert :ok = :ssl.send(socket, frame)

    assert {:ok, body} = recv_tls_frame(socket)
    assert {1, error_code, _payload} = Wire.decode_response(body)
    assert error_code != 0

    :ssl.close(socket)
  end
end
