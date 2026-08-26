defmodule Malachi.Auth.JwtValidator do
  @moduledoc """
  Pure validation of a signed JWT (JWS) and extraction of its identity claim: the deterministic core of the
  OIDC auth provider. Given a token and a validation `config`, it verifies the signature and the standard
  claims (`iss`, `aud`, `exp`) and returns the claim set, or a specific error.

  Signature verification is delegated to `Joken`/`jose` (a maintained library) rather than hand-rolled, and
  the algorithm is pinned by the **configured signer**, never taken from the token's own `alg` header, so the
  classic JWT footguns are closed: an `alg: none` (unsigned) token and an HS256/RS256 confusion attack (HMAC
  signing with the public key as the secret) both fail signature verification. Pure and side-effect free: it
  does not fetch keys or read a clock beyond `exp`/`nbf` comparison (the library uses the system clock for
  expiry, the one unavoidable time input).
  """

  @typedoc """
  Validation config: a `Joken.Signer` (algorithm + the IdP's **public** key), and the expected `issuer` /
  `audience` the token's `iss` / `aud` must equal.
  """
  @type config :: %{signer: Joken.Signer.t(), issuer: String.t(), audience: String.t()}

  @doc """
  Verifies `token`'s signature and standard claims against `config`. Returns `{:ok, claims}` (the decoded
  claim map) or `{:error, reason}` where `reason` is `:invalid_signature` (bad/`none`/confused signature or a
  malformed token), `:token_expired`, `:missing_expiry` (no `exp` claim: a token must expire),
  `:invalid_issuer`, `:invalid_audience`, `:invalid_claims`, or `:invalid_token`.
  """
  @spec validate(String.t(), config()) :: {:ok, map()} | {:error, atom()}
  def validate(token, %{signer: %Joken.Signer{} = signer, issuer: issuer, audience: audience})
      when is_binary(token) do
    token_config = Joken.Config.default_claims(iss: issuer, aud: audience, skip: [:jti, :iat, :nbf])

    case Joken.verify_and_validate(token_config, token, signer) do
      {:ok, claims} -> require_expiry(claims)
      {:error, :signature_error} -> {:error, :invalid_signature}
      {:error, details} when is_list(details) -> {:error, claim_error(details)}
      {:error, _other} -> {:error, :invalid_token}
    end
  end

  def validate(_token, _config), do: {:error, :invalid_token}

  # Require an `exp` claim: joken's default validator checks `exp` is in the future only when it is present,
  # so a token that omits `exp` would otherwise be accepted as never-expiring: a policy we reject.
  defp require_expiry(claims) do
    case Map.get(claims, "exp") do
      exp when is_integer(exp) -> {:ok, claims}
      _absent -> {:error, :missing_expiry}
    end
  end

  @doc """
  The identity string from `claims[claim_name]` (the configured identity claim, e.g. `\"sub\"`), or
  `{:error, :missing_identity_claim}` when it is absent or blank.
  """
  @spec identity(map(), String.t()) :: {:ok, String.t()} | {:error, :missing_identity_claim}
  def identity(claims, claim_name) when is_map(claims) and is_binary(claim_name) do
    case Map.get(claims, claim_name) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _absent_or_blank -> {:error, :missing_identity_claim}
    end
  end

  # Maps a joken claim-validation failure to a specific reason (the failing claim is in the details list).
  defp claim_error(details) do
    case Keyword.get(details, :claim) do
      "exp" -> :token_expired
      "iss" -> :invalid_issuer
      "aud" -> :invalid_audience
      _other -> :invalid_claims
    end
  end
end
