defmodule Malachi.Auth.MtlsProvider do
  @moduledoc """
  The mTLS-identity authentication provider (P4, decision 1A) — a `Malachi.Auth.AuthProvider`.

  Authenticates a client by the **certificate** it presented at the TLS handshake instead of a password: it
  reads an identity string from the peer certificate (`Malachi.Auth.CertIdentity`, per the configured policy)
  and looks that username up in the replicated user store for its permissions (decision 2A — the cert
  authenticates, the internal record authorizes). This is the NorthGuard-style service identity: a service
  proves who it is with its cert, no shared secret.

  The certificate's chain and validity are **not** re-checked here — that is the acceptor's TLS `verify_peer`
  (a certificate reaches this provider only after the TLS layer validated it). This provider maps an already
  trusted certificate to a malachi identity.

  `credentials` is the DER-encoded peer certificate. `context` carries:

    * `:policy` — which field names the identity (`:cn` | `{:san, :uri | :dns | :email}`, default `:cn`)
    * `:lookup` — a `(username -> {:ok, {username, hash, permissions}} | {:error, reason})` seam
      (default `Malachi.Auth.UserStore.get_user/1`)
  """

  @behaviour Malachi.Auth.AuthProvider

  alias Malachi.Auth.AuthProvider
  alias Malachi.Auth.CertIdentity
  alias Malachi.Auth.UserStore

  @doc """
  Resolves a DER peer certificate to an identity. Returns `{:ok, %{username, permissions}}`, or
  `{:error, reason}` where `reason` is `:no_peer_certificate` (no cert supplied), `:malformed_certificate`,
  `:no_identity` (the certificate has no usable identity for the policy), `:unknown_identity` (the identity
  is not a provisioned user), or a user-store error passed through.
  """
  @impl true
  def authenticate(der_cert, context) when is_binary(der_cert) and is_map(context) do
    policy = Map.get(context, :policy, :cn)
    lookup = Map.get(context, :lookup, &UserStore.get_user/1)

    case resolve_identity(der_cert, policy) do
      {:ok, username} -> AuthProvider.resolve_permissions(username, lookup)
      {:error, _reason} = error -> error
    end
  end

  def authenticate(_no_cert, _context), do: {:error, :no_peer_certificate}

  # The cert's identity string, collapsing "no usable identity" reasons into a single :no_identity while
  # keeping :malformed_certificate distinct (a decoding failure, not a policy miss).
  defp resolve_identity(der_cert, policy) do
    case CertIdentity.identity(der_cert, policy) do
      {:ok, username} -> {:ok, username}
      {:error, :malformed_certificate} = error -> error
      {:error, _no_common_name_or_matching_san} -> {:error, :no_identity}
    end
  end
end
