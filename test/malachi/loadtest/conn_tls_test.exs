defmodule Malachi.Loadtest.ConnTLSTest do
  # The client's side of GHSA-pr76-c2f9-qx6r: `tls: true` alone used to mean verify_none, so an
  # encrypted connection was not an authenticated one. These cases pin the verification policy itself
  # (no server needed) and then prove it end to end against a live TLS listener.
  use ExUnit.Case, async: false

  alias Malachi.Loadtest.Conn

  @host ~c"localhost"

  # A throwaway self-signed cert per run, following tcp_acceptor_tls_test: the dist certs are
  # gitignored, so generating here keeps the test self-contained. `cn` lets a case ask for a
  # certificate issued to a DIFFERENT host than the one it connects to.
  defp self_signed(cn \\ "localhost") do
    dir = Path.join(System.tmp_dir!(), "malachi_conn_tls_#{System.unique_integer([:positive])}")
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
          "/CN=#{cn}"
        ],
        stderr_to_stdout: true
      )

    on_exit(fn -> File.rm_rf!(dir) end)
    {certfile, keyfile}
  end

  # A private CA and a server certificate signed by it, which is the shape `--cacert` is for and what
  # scripts/generate-dist-certs.sh produces. A self-signed certificate cannot stand in: OTP rejects a
  # self-signed peer as `selfsigned_peer` even when it is named as the CA, so it could never show that
  # a legitimate private-CA deployment still connects. `cn` also lands in the SAN, which is what
  # hostname verification actually reads.
  defp ca_signed(cn \\ "localhost") do
    dir = Path.join(System.tmp_dir!(), "malachi_conn_ca_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    path = &Path.join(dir, &1)
    openssl = &({_out, 0} = System.cmd("openssl", &1, stderr_to_stdout: true))

    openssl.([
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-keyout",
      path.("ca-key.pem"),
      "-out",
      path.("ca.pem"),
      "-days",
      "1",
      "-subj",
      "/CN=Malachi Test CA"
    ])

    openssl.([
      "req",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-keyout",
      path.("key.pem"),
      "-out",
      path.("csr.pem"),
      "-subj",
      "/CN=#{cn}"
    ])

    File.write!(path.("ext.cnf"), "subjectAltName=DNS:#{cn}\n")

    openssl.([
      "x509",
      "-req",
      "-in",
      path.("csr.pem"),
      "-CA",
      path.("ca.pem"),
      "-CAkey",
      path.("ca-key.pem"),
      "-CAcreateserial",
      "-out",
      path.("cert.pem"),
      "-days",
      "1",
      "-extfile",
      path.("ext.cnf")
    ])

    on_exit(fn -> File.rm_rf!(dir) end)
    {path.("ca.pem"), path.("cert.pem"), path.("key.pem")}
  end

  # A TLS listener that accepts one connection and then just holds it: the handshake is the subject,
  # so nothing past it needs to speak the wire protocol.
  defp tls_server(certfile, keyfile) do
    {:ok, _} = Application.ensure_all_started(:ssl)

    {:ok, listen} =
      :ssl.listen(0, [
        :binary,
        packet: 0,
        active: false,
        reuseaddr: true,
        certfile: String.to_charlist(certfile),
        keyfile: String.to_charlist(keyfile),
        versions: [:"tlsv1.3", :"tlsv1.2"]
      ])

    {:ok, {_addr, port}} = :ssl.sockname(listen)

    server =
      spawn(fn ->
        case :ssl.transport_accept(listen, 5_000) do
          {:ok, socket} ->
            :ssl.handshake(socket, 5_000)
            Process.sleep(:infinity)

          _ ->
            :ok
        end
      end)

    on_exit(fn ->
      Process.exit(server, :kill)
      :ssl.close(listen)
    end)

    port
  end

  describe "verification policy (no server involved)" do
    test "tls without a CA verifies against the system trust store, never verify_none" do
      assert {:ok, opts} = Conn.ssl_opts(@host, tls: true)

      assert opts[:verify] == :verify_peer
      refute opts[:verify] == :verify_none
      assert is_list(opts[:cacerts]) and opts[:cacerts] != []
      assert opts[:server_name_indication] == @host
      assert opts[:customize_hostname_check][:match_fun]
    end

    test "tls with a CA verifies against it, and checks the hostname too" do
      assert {:ok, opts} = Conn.ssl_opts(@host, tls: true, cacert: "ca.pem")

      assert opts[:verify] == :verify_peer
      assert opts[:cacertfile] == "ca.pem"
      # Without this, a certificate legitimately issued for another host still chains to the CA and
      # would be accepted, which is half a fix.
      assert opts[:server_name_indication] == @host
      assert opts[:customize_hostname_check][:match_fun]
    end

    test "insecure: true is the only path that disables verification" do
      assert {:ok, opts} = Conn.ssl_opts(@host, tls: true, insecure: true)
      assert opts[:verify] == :verify_none
    end

    test "the client certificate is attached independently of how the server is verified" do
      assert {:ok, opts} = Conn.ssl_opts(@host, tls: true, cert: "c.pem", key: "k.pem")
      assert opts[:certfile] == "c.pem"
      assert opts[:keyfile] == "k.pem"
      assert opts[:verify] == :verify_peer
    end
  end

  describe "against a live TLS server" do
    test "a self-signed server is refused, with an error naming both ways forward" do
      {certfile, keyfile} = self_signed()
      port = tls_server(certfile, keyfile)

      assert {:error, {:tls_verification_failed, hint, _reason}} =
               Conn.connect(tls: true, host: "localhost", port: port)

      assert hint =~ "--cacert"
      assert hint =~ "--insecure"
    end

    test "a server signed by the CA named in --cacert is accepted" do
      {cafile, certfile, keyfile} = ca_signed()
      port = tls_server(certfile, keyfile)

      assert {:ok, conn} = Conn.connect(tls: true, host: "localhost", port: port, cacert: cafile)
      Conn.close(conn)
    end

    test "a certificate issued for another host is refused even with its CA trusted" do
      # Chain valid (same CA the client is given), hostname wrong: the only thing standing between the
      # client and an attacker holding a legitimate certificate for a host it does not mean to reach.
      {cafile, certfile, keyfile} = ca_signed("not-the-host.example")
      port = tls_server(certfile, keyfile)

      assert {:error, {:tls_verification_failed, hint, _reason}} =
               Conn.connect(tls: true, host: "localhost", port: port, cacert: cafile)

      assert hint =~ "hostname"
    end

    test "insecure: true connects to the self-signed server, so the escape hatch still works" do
      {certfile, keyfile} = self_signed()
      port = tls_server(certfile, keyfile)

      assert {:ok, conn} = Conn.connect(tls: true, host: "localhost", port: port, insecure: true)
      Conn.close(conn)
    end
  end
end
