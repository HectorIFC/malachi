defmodule Malachi.Auth do
  @moduledoc """
  Simple authentication system for Malachi.
  Manages users with username/password credentials.
  """
  use GenServer
  require Logger
  alias Malachi.Auth.AclRegistry
  alias Malachi.Auth.AclStore
  alias Malachi.Auth.SessionManager
  alias Malachi.Auth.UserStore
  alias Malachi.I18n
  alias Malachi.Telemetry

  @sessions_table :malachi_sessions

  @doc "Starts the auth server, which owns the in-memory user and session ETS tables."
  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc """
  Authenticates a user with username and password.
  Returns {:ok, session_token} or {:error, reason}

  ## Parameters

  - `username` - Username to authenticate
  - `password` - Password to verify
  - `client_ip` - Client IP address (tuple) for session binding and audit logging
  """
  def authenticate(username, password, client_ip, user_agent \\ "") do
    result = do_authenticate(username, password, client_ip, user_agent)
    Telemetry.auth(if match?({:ok, _}, result), do: :ok, else: :error)
    result
  end

  defp do_authenticate(username, password, client_ip, user_agent) do
    case verify_credentials(username, password) do
      {:ok, permissions} ->
        {:ok, token} = SessionManager.create_session(username, permissions, client_ip, user_agent)

        Logger.info(I18n.t(:auth_success, username: username))
        Malachi.AuditLog.log_event(:auth_success, %{username: username, ip: client_ip}, "authenticate", :success, %{})

        {:ok, token}

      {:error, reason} ->
        # A wrong password and an unknown user are both reported to the client as :invalid_credentials (never
        # revealing which); the specific reason is preserved only in the audit trail.
        log_auth_failure(username, client_ip, reason)
        {:error, :invalid_credentials}
    end
  end

  @doc """
  Verifies a username/password against the user store **without** creating a session: the session-less core
  of authentication, used by `Malachi.Auth.PasswordProvider`. Returns `{:ok, permissions}` or
  `{:error, :invalid_password | :user_not_found}`.

  Runs a dummy hash (`Argon2.no_user_verify/0`) for an unknown user so the response time does not reveal
  whether the username exists (timing-attack mitigation). The caller mints the session and maps both error
  reasons to a single client-facing `:invalid_credentials`.
  """
  @spec verify_credentials(String.t(), String.t()) ::
          {:ok, [atom()]} | {:error, :invalid_password | :user_not_found}
  def verify_credentials(username, password) do
    case UserStore.get_user(username) do
      {:ok, {^username, stored_hash, permissions}} ->
        if verify_password(password, stored_hash), do: {:ok, permissions}, else: {:error, :invalid_password}

      {:error, _reason} ->
        Argon2.no_user_verify()
        {:error, :user_not_found}
    end
  end

  # Structured log + audit for a failed login; the reason distinguishes a wrong password from an unknown user
  # for the audit trail only (the client always sees :invalid_credentials).
  defp log_auth_failure(username, client_ip, :invalid_password) do
    Logger.warning(I18n.t(:auth_failed, username: username))

    Malachi.AuditLog.log_event(
      :auth_failure,
      %{username: username, ip: client_ip},
      "authenticate",
      :failure,
      %{reason: :invalid_password}
    )
  end

  defp log_auth_failure(username, client_ip, :user_not_found) do
    Logger.warning(I18n.t(:auth_user_not_found, username: username))

    Malachi.AuditLog.log_event(
      :auth_failure,
      %{username: username, ip: client_ip},
      "authenticate",
      :failure,
      %{reason: :user_not_found}
    )
  end

  @doc """
  Authenticates a user with username and password (legacy compatibility).
  Uses a dummy IP address. For production use, prefer authenticate/3 with actual client IP.
  Returns {:ok, session_token} or {:error, reason}
  """
  def authenticate(username, password) when is_binary(username) and is_binary(password) do
    # Use dummy IP that matches validate_token/1 for backward compatibility
    authenticate(username, password, {0, 0, 0, 0})
  end

  @doc """
  Validates a session token with IP binding.
  Returns {:ok, %{username: username, permissions: permissions}} or {:error, reason}

  ## Parameters

  - `token` - Session token to validate
  - `client_ip` - Current client IP address for binding verification
  """
  def validate_token(token, client_ip, user_agent \\ "") do
    case SessionManager.validate_session(token, client_ip, user_agent) do
      {:ok, session_data} ->
        {:ok, %{username: session_data.username, permissions: session_data.permissions}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Validates a session token without IP binding (legacy compatibility).
  """
  def validate_token(token) when is_binary(token) do
    # Legacy path - use dummy IP
    validate_token(token, {0, 0, 0, 0})
  end

  @doc """
  Invalidates a session token (logout).
  """
  def logout(token) do
    SessionManager.revoke_session(token)
    :ok
  end

  @doc """
  Adds a new user.
  Permissions: :admin, :produce, :consume
  """
  def add_user(username, password, permissions \\ [:produce, :consume]) do
    GenServer.call(__MODULE__, {:add_user, username, password, permissions})
  end

  @doc """
  Removes a user.
  """
  def remove_user(username) do
    GenServer.call(__MODULE__, {:remove_user, username})
  end

  @doc """
  Changes user password.
  """
  def change_password(username, new_password) do
    GenServer.call(__MODULE__, {:change_password, username, new_password})
  end

  @doc """
  Lists all users (without passwords).
  """
  def list_users, do: UserStore.list_users()

  @acl_operations [:produce, :consume]

  @doc """
  Grants `username` a per-topic ACL: `operation` (`:produce`/`:consume`) on `pattern` (an exact topic, or a
  `*`-suffixed prefix like `\"orders.*\"`). Returns `:ok`, or `{:error, :invalid_acl}` for a bad operation/pattern.
  """
  @spec grant_acl(String.t(), atom(), String.t()) :: :ok | {:error, term()}
  def grant_acl(username, operation, pattern) when operation in @acl_operations and is_binary(pattern) do
    AclStore.grant(username, operation, AclRegistry.parse_resource(pattern))
  end

  def grant_acl(_username, _operation, _pattern), do: {:error, :invalid_acl}

  @doc "Revokes a per-topic ACL grant (idempotent). Returns `:ok` or `{:error, reason}`."
  @spec revoke_acl(String.t(), atom(), String.t()) :: :ok | {:error, term()}
  def revoke_acl(username, operation, pattern) when operation in @acl_operations and is_binary(pattern) do
    AclStore.revoke(username, operation, AclRegistry.parse_resource(pattern))
  end

  def revoke_acl(_username, _operation, _pattern), do: {:error, :invalid_acl}

  @doc "Lists `username`'s ACL grants as `%{operation, resource}` maps (resource rendered as its pattern string)."
  @spec list_acls(String.t()) :: [%{operation: atom(), resource: String.t()}]
  def list_acls(username) do
    Enum.map(AclStore.list_grants(username), fn {operation, resource} ->
      %{operation: operation, resource: AclRegistry.render_resource(resource)}
    end)
  end

  @doc """
  Whether the subject has `permission` (or is `:admin`). Accepts either a `username`, looked up in
  the user table, where an unknown user has no permissions, or a permission list directly.
  """
  def has_permission?(username, permission) when is_binary(username) do
    case UserStore.get_user(username) do
      {:ok, {^username, _hash, permissions}} ->
        :admin in permissions or permission in permissions

      {:error, _reason} ->
        false
    end
  end

  def has_permission?(permissions, permission) when is_list(permissions) do
    :admin in permissions or permission in permissions
  end

  @doc """
  Parses a list of permission **strings** into the allowed permission atoms, or `:error` if any is unknown
  (or the input is not a list). The allowed permissions are `:admin`, `:produce`, `:consume`. Mapping
  explicitly (rather than `String.to_atom/1`) keeps an untrusted client from exhausting the atom table.
  """
  @spec parse_permissions([String.t()]) :: {:ok, [atom()]} | :error
  def parse_permissions(strings) when is_list(strings) do
    mapped =
      Enum.map(strings, fn
        "admin" -> :admin
        "produce" -> :produce
        "consume" -> :consume
        _other -> :invalid
      end)

    if :invalid in mapped, do: :error, else: {:ok, mapped}
  end

  def parse_permissions(_not_a_list), do: :error

  @doc """
  Parses an ACL operation **string** into `:produce`/`:consume`, or `:error`. Mapping explicitly (rather than
  `String.to_atom/1`) keeps an untrusted client from exhausting the atom table. Shared by the ACL management
  surfaces (wire ops, dashboard).
  """
  @spec parse_acl_operation(String.t()) :: {:ok, :produce | :consume} | :error
  def parse_acl_operation("produce"), do: {:ok, :produce}
  def parse_acl_operation("consume"), do: {:ok, :consume}
  def parse_acl_operation(_other), do: :error

  @impl true
  def init(:ok) do
    :ets.new(@sessions_table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    # Seed default users from config into the replicated user store (idempotent, skips existing). The
    # ra user cluster is started by Malachi.Application before this child, so writes have a leader.
    seed_default_users()

    # When configured (no explicit admin password), generate a random admin on first boot and log it once.
    generate_admin_if_absent()

    Logger.info(I18n.t(:auth_started))
    {:ok, %{}}
  end

  @impl true
  def handle_call({:add_user, username, password, permissions}, _from, state) do
    hash = hash_password(password)

    case UserStore.insert_user(username, hash, permissions) do
      :ok ->
        Logger.info(I18n.t(:user_created, username: username))
        {:reply, :ok, state}

      {:error, :user_exists} ->
        {:reply, {:error, :user_exists}, state}

      {:error, _reason} ->
        {:reply, {:error, :persist_failed}, state}
    end
  end

  @impl true
  def handle_call({:remove_user, username}, _from, state) do
    case UserStore.delete_user(username) do
      :ok ->
        # Revoke all sessions for this user via SessionManager
        SessionManager.revoke_all_sessions(username)
        # Drop the user's per-topic ACL grants too, so a deleted username leaves no dangling authorization
        # (best-effort: a store hiccup leaves grants that only matter if the username is later recreated).
        _ = AclStore.revoke_user(username)
        Logger.info(I18n.t(:user_removed, username: username))
        {:reply, :ok, state}

      {:error, _reason} ->
        {:reply, {:error, :persist_failed}, state}
    end
  end

  @impl true
  def handle_call({:change_password, username, new_password}, _from, state) do
    new_hash = hash_password(new_password)

    case UserStore.update_password(username, new_hash) do
      :ok ->
        Logger.info(I18n.t(:password_changed, username: username))
        {:reply, :ok, state}

      {:error, :user_not_found} ->
        {:reply, {:error, :user_not_found}, state}

      {:error, _reason} ->
        {:reply, {:error, :persist_failed}, state}
    end
  end

  defp seed_default_users do
    # Users to seed come entirely from config (config/dev.exs, config/test.exs, or env via
    # config/runtime.exs). No hard-coded fallback: an empty list seeds nothing.
    default_users = Application.get_env(:malachi, :default_users, [])

    # A shared deadline across all users: at cold boot a multi-node cluster may not have elected a leader
    # yet, so a transport error is transient (single-node forms instantly and never waits).
    deadline = System.monotonic_time(:millisecond) + 5_000

    seeded =
      Enum.reduce(default_users, 0, fn {username, password, permissions}, count ->
        hash = hash_password(password)

        case seed_insert(username, hash, permissions, deadline) do
          :ok ->
            count + 1

          {:error, :user_exists} ->
            count

          {:error, reason} ->
            Logger.error(I18n.t(:user_store_persist_error, reason: inspect(reason)))
            count
        end
      end)

    if seeded > 0 do
      Logger.info(I18n.t(:default_users_loaded, count: seeded))
    end
  end

  @doc """
  Generates a random password for `username` (an admin) and seeds it, but only when generation is enabled
  (`:generate_admin` config, set when no admin password is configured) and no such user exists yet. The
  password is **logged once**; the replicated store dedups, so on a multi-node boot exactly one node's seed
  succeeds and announces its password (the others get `:user_exists` and discard theirs). A no-op when
  generation is disabled or the admin already exists. Called at boot after `seed_default_users/0`.
  """
  @spec generate_admin_if_absent(String.t()) :: :ok
  def generate_admin_if_absent(username \\ "admin") do
    if Application.get_env(:malachi, :generate_admin, false) do
      password = generate_password()
      deadline = System.monotonic_time(:millisecond) + 5_000

      case seed_insert(username, hash_password(password), [:admin], deadline) do
        :ok -> announce_generated_admin(username, password)
        # already seeded (an explicit config or another node) or unreachable: no password to announce
        _other -> :ok
      end
    else
      :ok
    end
  end

  # A strong, URL-safe random password (192 bits of entropy).
  defp generate_password do
    24 |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false)
  end

  defp announce_generated_admin(username, password) do
    Logger.warning(I18n.t(:admin_password_generated, username: username, password: password))
    :ok
  end

  # Inserts a seed user, retrying a transient transport error until `deadline` (the cluster reaching quorum).
  # `:ok` and `{:error, :user_exists}` are terminal.
  defp seed_insert(username, hash, permissions, deadline) do
    case UserStore.insert_user(username, hash, permissions) do
      {:error, reason} = err when reason != :user_exists ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(100)
          seed_insert(username, hash, permissions, deadline)
        else
          err
        end

      reply ->
        reply
    end
  end

  defp hash_password(password) do
    Argon2.hash_pwd_salt(password)
  end

  defp verify_password(password, stored_hash) do
    Argon2.verify_pass(password, stored_hash)
  end

  # Session management moved to Malachi.Auth.SessionManager
  # Sessions table kept for backward compatibility but SessionManager is primary
end
