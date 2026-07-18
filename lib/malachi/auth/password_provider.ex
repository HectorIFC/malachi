defmodule Malachi.Auth.PasswordProvider do
  @moduledoc """
  The username/password authentication provider — the built-in `Malachi.Auth.AuthProvider`.

  Wraps the session-less credential check (`Malachi.Auth.verify_credentials/2`) so the password mechanism
  fits the same contract as the external providers (mTLS today; OIDC/LDAP later): it resolves credentials to
  an identity (`%{username, permissions}`) and lets the boundary mint the session. It does **not** log or
  audit — that stays with the boundary, which maps the specific error to a client-facing `:invalid_credentials`.
  """

  @behaviour Malachi.Auth.AuthProvider

  alias Malachi.Auth

  @doc """
  Resolves `{username, password}` to an identity. `context` may carry a `:verify` seam
  (`(username, password -> {:ok, permissions} | {:error, reason})`, default `Auth.verify_credentials/2`) for
  testing. Returns `{:ok, %{username, permissions}}` or `{:error, :invalid_password | :user_not_found}`.
  """
  @impl true
  def authenticate({username, password}, context)
      when is_binary(username) and is_binary(password) and is_map(context) do
    verify = Map.get(context, :verify, &Auth.verify_credentials/2)

    case verify.(username, password) do
      {:ok, permissions} -> {:ok, %{username: username, permissions: permissions}}
      {:error, reason} -> {:error, reason}
    end
  end
end
