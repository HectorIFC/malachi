defmodule Malachi.Auth.OidcConfigTest do
  # async: false, reads/mutates global app env (:oidc_*).
  use ExUnit.Case, async: false

  alias Malachi.Auth.JwtValidator
  alias Malachi.Auth.OidcConfig
  alias Malachi.Test.JwtFixtures

  @keys [:oidc_public_key, :oidc_issuer, :oidc_audience, :oidc_algorithm, :oidc_identity_claim]

  setup do
    prior = Map.new(@keys, fn key -> {key, Application.get_env(:malachi, key)} end)
    on_exit(fn -> Enum.each(prior, fn {key, value} -> restore(key, value) end) end)

    {sign, verify} = JwtFixtures.rs256_keypair()
    public_pem = verify.jwk |> JOSE.JWK.to_pem() |> elem(1)
    {:ok, sign: sign, public_pem: public_pem}
  end

  defp restore(key, nil), do: Application.delete_env(:malachi, key)
  defp restore(key, value), do: Application.put_env(:malachi, key, value)

  defp configure(env), do: Enum.each(env, fn {key, value} -> Application.put_env(:malachi, key, value) end)

  test "builds a config that validates a token signed by the configured key", %{sign: sign, public_pem: pem} do
    configure(oidc_public_key: pem, oidc_issuer: "https://idp", oidc_audience: "malachi")

    assert {:ok, config} = OidcConfig.load()
    assert config.issuer == "https://idp"
    assert config.identity_claim == "sub"

    now = System.system_time(:second)
    token = JwtFixtures.sign(sign, %{"iss" => "https://idp", "aud" => "malachi", "sub" => "alice", "exp" => now + 300})
    assert {:ok, %{"sub" => "alice"}} = JwtValidator.validate(token, config)
  end

  test "defaults algorithm to RS256 and identity_claim to sub, and honors overrides", %{public_pem: pem} do
    configure(oidc_public_key: pem, oidc_issuer: "i", oidc_audience: "a", oidc_identity_claim: "email")
    assert {:ok, %{identity_claim: "email"}} = OidcConfig.load()
  end

  test "fails closed when a required setting is missing or blank", %{public_pem: pem} do
    Enum.each(@keys, &Application.delete_env(:malachi, &1))
    assert {:error, :oidc_misconfigured} = OidcConfig.load()

    configure(oidc_public_key: pem, oidc_issuer: "", oidc_audience: "a")
    assert {:error, :oidc_misconfigured} = OidcConfig.load()
  end

  test "fails closed when the public key is not a valid PEM" do
    configure(oidc_public_key: "not a pem", oidc_issuer: "i", oidc_audience: "a")
    assert {:error, :oidc_misconfigured} = OidcConfig.load()
  end
end
