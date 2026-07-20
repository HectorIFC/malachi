defmodule Malachi.OidcAuthTest do
  # async: false: mutates global app config (oidc_*) and shares the running acceptor.
  use ExUnit.Case, async: false

  alias Malachi.Test.JwtFixtures
  alias Malachi.Test.TCPHelper
  alias Malachi.Wire

  @moduletag :security
  @issuer "https://idp.test"
  @audience "malachi"

  # Sends a token_auth frame; returns `{ok?, reason_or_session_token}`.
  defp token_auth(socket, jwt) do
    {code, payload} = TCPHelper.request(socket, Wire.token_auth_key(), 1, Wire.encode_token_auth_req(jwt))
    {code == Wire.ok_code(), Wire.decode_auth_resp(payload)}
  end

  setup do
    keys = [:oidc_auth, :oidc_public_key, :oidc_issuer, :oidc_audience, :oidc_identity_claim, :auth_rate_limit]
    prior = Map.new(keys, fn key -> {key, Application.get_env(:malachi, key)} end)
    Application.put_env(:malachi, :auth_rate_limit, 100_000)
    on_exit(fn -> Enum.each(prior, fn {key, value} -> restore(key, value) end) end)

    {sign, verify} = JwtFixtures.rs256_keypair()
    public_pem = verify.jwk |> JOSE.JWK.to_pem() |> elem(1)
    {:ok, sign: sign, public_pem: public_pem, now: System.system_time(:second)}
  end

  defp restore(key, nil), do: Application.delete_env(:malachi, key)
  defp restore(key, value), do: Application.put_env(:malachi, key, value)

  defp enable_oidc(pem) do
    Application.put_env(:malachi, :oidc_auth, true)
    Application.put_env(:malachi, :oidc_public_key, pem)
    Application.put_env(:malachi, :oidc_issuer, @issuer)
    Application.put_env(:malachi, :oidc_audience, @audience)
  end

  defp token_for(sign, sub, now), do: JwtFixtures.sign(sign, %{"iss" => @issuer, "aud" => @audience, "sub" => sub, "exp" => now + 300})

  describe "token_auth handshake gate" do
    test "is rejected when the feature is disabled (default)", %{sign: sign, now: now} do
      {:ok, socket} = TCPHelper.connect()
      assert {false, "oidc_auth_disabled"} = token_auth(socket, token_for(sign, "x", now))
    end

    test "is rejected when enabled but not fully configured (no key)" do
      Application.put_env(:malachi, :oidc_auth, true)
      Application.delete_env(:malachi, :oidc_public_key)
      {:ok, socket} = TCPHelper.connect()
      assert {false, "oidc_misconfigured"} = token_auth(socket, "any.jwt.here")
    end
  end

  describe "token_auth end to end" do
    test "a valid token for a provisioned user returns a session token", %{sign: sign, public_pem: pem, now: now} do
      enable_oidc(pem)
      username = "oidc_#{System.unique_integer([:positive])}"
      Malachi.Auth.add_user(username, "unused-password", [:produce, :consume])
      on_exit(fn -> Malachi.Auth.remove_user(username) end)

      {:ok, socket} = TCPHelper.connect()
      assert {true, session_token} = token_auth(socket, token_for(sign, username, now))
      assert is_binary(session_token) and session_token != ""
    end

    test "a valid token for an unprovisioned identity is rejected without leaking why", %{sign: sign, public_pem: pem, now: now} do
      enable_oidc(pem)
      {:ok, socket} = TCPHelper.connect()
      assert {false, "invalid_credentials"} = token_auth(socket, token_for(sign, "ghost_#{System.unique_integer([:positive])}", now))
    end

    test "a token signed by the wrong key is rejected as invalid_credentials", %{public_pem: pem, now: now} do
      enable_oidc(pem)
      {other_sign, _verify} = JwtFixtures.rs256_keypair()
      {:ok, socket} = TCPHelper.connect()
      assert {false, "invalid_credentials"} = token_auth(socket, token_for(other_sign, "x", now))
    end

    test "an expired token is rejected as invalid_credentials", %{sign: sign, public_pem: pem, now: now} do
      enable_oidc(pem)
      expired = JwtFixtures.sign(sign, %{"iss" => @issuer, "aud" => @audience, "sub" => "x", "exp" => now - 10})
      {:ok, socket} = TCPHelper.connect()
      assert {false, "invalid_credentials"} = token_auth(socket, expired)
    end
  end
end
