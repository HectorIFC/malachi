defmodule Malachi.Auth.JwtValidatorTest do
  use ExUnit.Case, async: true

  alias Malachi.Auth.JwtValidator
  alias Malachi.Test.JwtFixtures

  @issuer "https://idp.example"
  @audience "malachi"

  setup do
    {sign, verify} = JwtFixtures.rs256_keypair()
    config = %{signer: verify, issuer: @issuer, audience: @audience}
    now = System.system_time(:second)
    claims = %{"iss" => @issuer, "aud" => @audience, "sub" => "alice", "exp" => now + 300}
    {:ok, sign: sign, config: config, claims: claims, now: now}
  end

  describe "validate/2, happy path" do
    test "accepts a well-formed token and returns its claims", %{sign: sign, config: config, claims: claims} do
      token = JwtFixtures.sign(sign, claims)
      assert {:ok, decoded} = JwtValidator.validate(token, config)
      assert decoded["sub"] == "alice"
      assert decoded["iss"] == @issuer
    end
  end

  describe "validate/2, claim validation" do
    test "rejects an expired token", %{sign: sign, config: config, claims: claims, now: now} do
      token = JwtFixtures.sign(sign, %{claims | "exp" => now - 10})
      assert {:error, :token_expired} = JwtValidator.validate(token, config)
    end

    test "rejects a wrong issuer", %{sign: sign, config: config, claims: claims} do
      token = JwtFixtures.sign(sign, %{claims | "iss" => "https://evil.example"})
      assert {:error, :invalid_issuer} = JwtValidator.validate(token, config)
    end

    test "rejects a wrong audience", %{sign: sign, config: config, claims: claims} do
      token = JwtFixtures.sign(sign, %{claims | "aud" => "someone-else"})
      assert {:error, :invalid_audience} = JwtValidator.validate(token, config)
    end

    test "rejects a token with no exp claim (never-expiring tokens are not allowed)", %{
      sign: sign,
      config: config,
      claims: claims
    } do
      token = JwtFixtures.sign(sign, Map.delete(claims, "exp"))
      assert {:error, :missing_expiry} = JwtValidator.validate(token, config)
    end
  end

  describe "validate/2, signature attacks (security-critical)" do
    test "rejects a token signed by a different key", %{config: config, claims: claims} do
      {other_sign, _} = JwtFixtures.rs256_keypair()
      forged = JwtFixtures.sign(other_sign, claims)
      assert {:error, :invalid_signature} = JwtValidator.validate(forged, config)
    end

    test "rejects an unsigned alg:none token", %{config: config, claims: claims} do
      # Hand-crafted: header.payload. with an empty signature. Must never be trusted.
      b64 = fn map -> map |> Jason.encode!() |> Base.url_encode64(padding: false) end
      none_token = b64.(%{"alg" => "none", "typ" => "JWT"}) <> "." <> b64.(%{claims | "sub" => "admin"}) <> "."
      assert {:error, :invalid_signature} = JwtValidator.validate(none_token, config)
    end

    test "rejects an HS256/RS256 confusion token (HMAC using the public key as the secret)", %{
      config: config,
      claims: claims
    } do
      %Joken.Signer{} = verify = config.signer
      public_pem = verify.jwk |> JOSE.JWK.to_pem() |> elem(1)
      hs_signer = Joken.Signer.create("HS256", public_pem)
      confused = JwtFixtures.sign(hs_signer, claims)
      assert {:error, :invalid_signature} = JwtValidator.validate(confused, config)
    end

    test "rejects a malformed token", %{config: config} do
      assert {:error, :invalid_signature} = JwtValidator.validate("not.a.jwt", config)
    end
  end

  describe "validate/2, bad input" do
    test "rejects a non-binary token", %{config: config} do
      assert {:error, :invalid_token} = JwtValidator.validate(nil, config)
    end
  end

  describe "identity/2" do
    test "extracts the configured identity claim" do
      assert {:ok, "alice"} = JwtValidator.identity(%{"sub" => "alice"}, "sub")
      assert {:ok, "svc"} = JwtValidator.identity(%{"preferred_username" => "svc"}, "preferred_username")
    end

    test "rejects an absent or blank identity claim" do
      assert {:error, :missing_identity_claim} = JwtValidator.identity(%{"sub" => ""}, "sub")
      assert {:error, :missing_identity_claim} = JwtValidator.identity(%{}, "sub")
      assert {:error, :missing_identity_claim} = JwtValidator.identity(%{"sub" => 123}, "sub")
    end
  end
end
