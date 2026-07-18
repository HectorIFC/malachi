defmodule Malachi.Auth.AuthProvider do
  @moduledoc """
  The contract every authentication mechanism implements — the plug point for **external** auth (P4).

  Malachi's boundary (the wire handshake, the dashboard) authenticates a client by asking a provider to
  turn some `credentials` into a malachi **identity** (`%{username, permissions}`); the boundary then mints
  the session. Authentication (proving *who* the client is) is thus pluggable — password today, and
  mTLS-identity / OIDC / LDAP as further providers — while **authorization** (the permissions) stays
  internal, sourced from the replicated user store (P2). This mirrors how Kafka/Pulsar separate a pluggable
  authenticator from an internal authorizer, and how NorthGuard leans on platform (mTLS) identity.

  Each provider owns its own `credentials` shape (e.g. `{username, password}` for the password provider, a
  DER-encoded peer certificate for the mTLS provider); `context` carries request metadata (client IP, the
  configured identity policy, ...). The uniform **result** — `{:ok, identity}` or `{:error, reason}` — lets
  the boundary handle every provider the same way once authentication resolves.
  """

  @typedoc "A resolved malachi identity: the authenticated username and its authorization permissions."
  @type identity :: %{username: String.t(), permissions: [atom()]}

  @typedoc "Resolves a username to its stored record, or an error. In production `UserStore.get_user/1`."
  @type lookup :: (String.t() -> {:ok, {String.t(), String.t(), [atom()]}} | {:error, term()})

  @doc """
  Resolves `credentials` (provider-specific) into an `identity`, or returns `{:error, reason}`. `context`
  carries request metadata (e.g. `:client_ip`, the identity `:policy`). Must not create a session — the
  caller does that from the returned identity.
  """
  @callback authenticate(credentials :: term(), context :: map()) :: {:ok, identity()} | {:error, term()}

  @doc """
  Turns an already-authenticated `username` into a full `identity` by looking up its permissions via
  `lookup`. Shared by the providers whose credential proves **identity** but not **authorization** (mTLS,
  OIDC — decision 2A/3A: the credential authenticates, the internal record authorizes).

  Fails **closed**: a lookup returning another user's record yields `:unknown_identity` rather than granting
  its permissions, and an unknown user is `:unknown_identity`; other lookup errors pass through.
  """
  @spec resolve_permissions(String.t(), lookup()) :: {:ok, identity()} | {:error, term()}
  def resolve_permissions(username, lookup) do
    case lookup.(username) do
      {:ok, {^username, _hash, permissions}} -> {:ok, %{username: username, permissions: permissions}}
      {:ok, _mismatched_record} -> {:error, :unknown_identity}
      {:error, :user_not_found} -> {:error, :unknown_identity}
      {:error, reason} -> {:error, reason}
    end
  end
end
