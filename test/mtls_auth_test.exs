defmodule Malachi.MtlsAuthTest do
  # async: false — mutates global app config (mtls_auth / tls_verify) and shares the running acceptor.
  use ExUnit.Case, async: false

  alias Malachi.Test.TCPHelper
  alias Malachi.Wire

  @moduletag :security

  # Sends an mtls_auth frame over the (plain-TCP) test connection; returns `{ok?, reason_string}`.
  defp mtls_auth(socket) do
    {code, payload} = TCPHelper.request(socket, Wire.mtls_auth_key(), 1, Wire.encode_mtls_auth_req())
    {code == Wire.ok_code(), Wire.decode_auth_resp(payload)}
  end

  setup do
    prior = %{
      mtls_auth: Application.get_env(:malachi, :mtls_auth),
      tls_verify: Application.get_env(:malachi, :tls_verify),
      auth_rate_limit: Application.get_env(:malachi, :auth_rate_limit)
    }

    # Keep the shared auth rate limiter from interfering with the verify_peer gate test.
    Application.put_env(:malachi, :auth_rate_limit, 100_000)

    on_exit(fn -> Enum.each(prior, fn {key, value} -> restore(key, value) end) end)
    :ok
  end

  defp restore(key, nil), do: Application.delete_env(:malachi, key)
  defp restore(key, value), do: Application.put_env(:malachi, key, value)

  describe "mtls_auth handshake gate (security-critical)" do
    test "is rejected when the feature is disabled (default)" do
      Application.delete_env(:malachi, :mtls_auth)
      {:ok, socket} = TCPHelper.connect()
      assert {false, "mtls_auth_disabled"} = mtls_auth(socket)
    end

    test "is rejected when the listener does not verify peer certs (verify_none)" do
      # An unverified certificate could be forged, so mTLS auth must not be honored under verify_none even
      # when the feature is enabled.
      Application.put_env(:malachi, :mtls_auth, true)
      Application.put_env(:malachi, :tls_verify, "verify_none")
      {:ok, socket} = TCPHelper.connect()
      assert {false, "mtls_auth_unavailable"} = mtls_auth(socket)
    end

    test "reports no peer certificate over a non-TLS connection when enabled + verify_peer" do
      Application.put_env(:malachi, :mtls_auth, true)
      Application.put_env(:malachi, :tls_verify, "verify_peer")
      {:ok, socket} = TCPHelper.connect()
      assert {false, "no_peer_certificate"} = mtls_auth(socket)
    end
  end
end
