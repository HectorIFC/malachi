defmodule Malachi.Test.JwtFixtures do
  @moduledoc """
  Helpers for JWT/OIDC tests: generate a throwaway RSA keypair at runtime and sign tokens with it.

  Keys are generated in-process per call: nothing is written to disk, so no key material (public or private)
  is checked into the repo, mirroring how the mTLS tests avoid committing private keys.
  """

  @doc """
  A fresh RS256 keypair as `{sign_signer, verify_signer}`: `sign_signer` holds the private key (used by tests
  to mint tokens), `verify_signer` holds only the public key (what the validator is configured with).
  """
  @spec rs256_keypair() :: {Joken.Signer.t(), Joken.Signer.t()}
  def rs256_keypair do
    jwk = JOSE.JWK.generate_key({:rsa, 2048})
    {_meta, private_pem} = JOSE.JWK.to_pem(jwk)
    {_meta, public_pem} = jwk |> JOSE.JWK.to_public() |> JOSE.JWK.to_pem()
    {Joken.Signer.create("RS256", %{"pem" => private_pem}), Joken.Signer.create("RS256", %{"pem" => public_pem})}
  end

  @doc "Signs `claims` with `signer`, returning the compact JWT string."
  @spec sign(Joken.Signer.t(), map()) :: String.t()
  def sign(signer, claims) do
    {:ok, token, _claims} = Joken.encode_and_sign(claims, signer)
    token
  end
end
