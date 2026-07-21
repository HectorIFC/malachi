defmodule Malachi.Auth.OidcConfig do
  @moduledoc """
  Builds the OIDC validation config from application settings (P4, decision 2A: static configured key).

  Reads the operator's OIDC settings and produces the map `Malachi.Auth.JwtProvider` / `Malachi.Auth.JwtValidator`
  expect: a `Joken.Signer` (algorithm + the IdP's **public** key, from a configured PEM), the expected
  `issuer` and `audience`, and the `identity_claim` naming the username. Returns `{:error, :oidc_misconfigured}`
  when a required setting is missing/blank or the PEM cannot be parsed into a signer, so a half-configured
  deployment fails closed rather than trusting tokens it cannot verify.

  Settings (`:malachi` app env, populated from `MALACHI_OIDC_*` in `config/runtime.exs`):

    * `:oidc_public_key`: the IdP signing key as a PEM string (required)
    * `:oidc_issuer`: the expected `iss` (required)
    * `:oidc_audience`: the expected `aud` (required)
    * `:oidc_algorithm`, JWS algorithm, default `\"RS256\"`
    * `:oidc_identity_claim`: the claim naming the username, default `\"sub\"`
  """

  @default_algorithm "RS256"
  @default_identity_claim "sub"

  @type t :: %{
          signer: Joken.Signer.t(),
          issuer: String.t(),
          audience: String.t(),
          identity_claim: String.t()
        }

  @doc "The OIDC validation config, or `{:error, :oidc_misconfigured}` when settings are missing/invalid."
  @spec load() :: {:ok, t()} | {:error, :oidc_misconfigured}
  def load do
    with {:ok, pem} <- required(:oidc_public_key),
         {:ok, issuer} <- required(:oidc_issuer),
         {:ok, audience} <- required(:oidc_audience),
         {:ok, signer} <- build_signer(pem) do
      {:ok,
       %{
         signer: signer,
         issuer: issuer,
         audience: audience,
         identity_claim: setting(:oidc_identity_claim, @default_identity_claim)
       }}
    end
  end

  defp required(key) do
    case setting(key, nil) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing_or_blank -> {:error, :oidc_misconfigured}
    end
  end

  defp build_signer(pem) do
    # Joken.Signer.create is lenient on a bad PEM (it fails only later, at verify time, rejecting every
    # token). Validate the PEM up front so a misconfigured key fails at load: the operator learns
    # immediately instead of via silent auth failures.
    case :public_key.pem_decode(pem) do
      [] -> {:error, :oidc_misconfigured}
      [_ | _] -> {:ok, Joken.Signer.create(setting(:oidc_algorithm, @default_algorithm), %{"pem" => pem})}
    end
  rescue
    _bad_pem_or_algorithm -> {:error, :oidc_misconfigured}
  end

  defp setting(key, default), do: Application.get_env(:malachi, key, default)
end
