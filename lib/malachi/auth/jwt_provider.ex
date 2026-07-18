defmodule Malachi.Auth.JwtProvider do
  @moduledoc """
  The OIDC/JWT authentication provider (P4, decision 1A/2A/3A) — a `Malachi.Auth.AuthProvider`.

  Authenticates a client by a **signed JWT** issued by an external IdP: it verifies the token
  (`Malachi.Auth.JwtValidator` — signature + `iss`/`aud`/`exp`), reads the identity from a configured claim,
  and looks that username up in the replicated user store for its permissions (decision 3A — the token
  authenticates, the internal record authorizes, like Kafka/Pulsar). No shared secret crosses the wire; the
  server trusts the IdP's signature.

  `credentials` is the compact JWT string. `context` carries the validation config
  (`:signer`, `:issuer`, `:audience`, `:identity_claim` — typically from `Malachi.Auth.OidcConfig`) and,
  optionally, a `:lookup` seam (default `Malachi.Auth.UserStore.get_user/1`).
  """

  @behaviour Malachi.Auth.AuthProvider

  alias Malachi.Auth.AuthProvider
  alias Malachi.Auth.JwtValidator
  alias Malachi.Auth.UserStore

  @doc """
  Resolves a JWT to an identity. Returns `{:ok, %{username, permissions}}`, or `{:error, reason}` where
  `reason` is a validation error (`:invalid_signature`, `:token_expired`, `:missing_expiry`,
  `:invalid_issuer`, `:invalid_audience`, ...), `:no_identity` (the identity claim is absent),
  `:unknown_identity` (the identity is not a provisioned user), or a user-store error passed through.
  """
  @impl true
  def authenticate(token, context) when is_binary(token) and is_map(context) do
    lookup = Map.get(context, :lookup, &UserStore.get_user/1)
    identity_claim = Map.fetch!(context, :identity_claim)

    case resolve_identity(token, context, identity_claim) do
      {:ok, username} -> AuthProvider.resolve_permissions(username, lookup)
      {:error, _reason} = error -> error
    end
  end

  def authenticate(_no_token, _context), do: {:error, :invalid_token}

  # Validate the token, then pull the identity claim — collapsing an absent identity claim into :no_identity
  # while letting validation errors (bad signature, expired, wrong iss/aud) pass through as themselves.
  defp resolve_identity(token, context, identity_claim) do
    with {:ok, claims} <- JwtValidator.validate(token, config(context)),
         {:ok, username} <- JwtValidator.identity(claims, identity_claim) do
      {:ok, username}
    else
      {:error, :missing_identity_claim} -> {:error, :no_identity}
      {:error, _reason} = error -> error
    end
  end

  defp config(context), do: Map.take(context, [:signer, :issuer, :audience])
end
