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

  @doc """
  Resolves `credentials` (provider-specific) into an `identity`, or returns `{:error, reason}`. `context`
  carries request metadata (e.g. `:client_ip`, the identity `:policy`). Must not create a session — the
  caller does that from the returned identity.
  """
  @callback authenticate(credentials :: term(), context :: map()) :: {:ok, identity()} | {:error, term()}
end
