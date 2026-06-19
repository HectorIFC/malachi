defmodule Malachi.Auth do
  @moduledoc """
  Simple authentication system for Malachi.
  Manages users with username/password credentials.
  """
  use GenServer
  require Logger
  alias Malachi.Auth.SessionManager
  alias Malachi.Auth.UserStore
  alias Malachi.I18n

  @users_table :malachi_users
  @sessions_table :malachi_sessions

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
  def authenticate(username, password, client_ip) do
    case :ets.lookup(@users_table, username) do
      [{^username, stored_hash, permissions}] ->
        if verify_password(password, stored_hash) do
          # Create session with IP binding via SessionManager
          {:ok, token} =
            SessionManager.create_session(
              username,
              permissions,
              client_ip,
              # user_agent not implemented
              ""
            )

          Logger.info(I18n.t(:auth_success, username: username))

          # Log audit event
          Malachi.AuditLog.log_event(
            :auth_success,
            %{username: username, ip: client_ip},
            "authenticate",
            :success,
            %{}
          )

          {:ok, token}
        else
          Logger.warning(I18n.t(:auth_failed, username: username))

          # Log audit event
          Malachi.AuditLog.log_event(
            :auth_failure,
            %{username: username, ip: client_ip},
            "authenticate",
            :failure,
            %{reason: :invalid_password}
          )

          {:error, :invalid_credentials}
        end

      [] ->
        # Timing attack prevention: still hash to match timing
        Argon2.no_user_verify()

        Logger.warning(I18n.t(:auth_user_not_found, username: username))

        # Log audit event
        Malachi.AuditLog.log_event(
          :auth_failure,
          %{username: username, ip: client_ip},
          "authenticate",
          :failure,
          %{reason: :user_not_found}
        )

        {:error, :invalid_credentials}
    end
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
  def validate_token(token, client_ip) do
    case SessionManager.validate_session(token, client_ip, "") do
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
  def list_users do
    :ets.foldl(
      fn {username, _hash, permissions}, acc ->
        [%{username: username, permissions: permissions} | acc]
      end,
      [],
      @users_table
    )
  end

  @doc """
  Checks if user has a specific permission.
  """
  def has_permission?(username, permission) when is_binary(username) do
    case :ets.lookup(@users_table, username) do
      [{^username, _hash, permissions}] ->
        :admin in permissions or permission in permissions

      [] ->
        false
    end
  end

  def has_permission?(permissions, permission) when is_list(permissions) do
    :admin in permissions or permission in permissions
  end

  @impl true
  def init(:ok) do
    :ets.new(@users_table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true
    ])

    :ets.new(@sessions_table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    # Load persisted users from Mnesia into ETS
    case UserStore.load_into_ets() do
      :ok -> :ok
      _error -> :ok
    end

    # Seed default users from config into Mnesia (only if they don't exist yet)
    seed_default_users()

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
    default_users =
      Application.get_env(:malachi, :default_users, [
        {"admin", "admin123", [:admin]},
        {"producer", "producer123", [:produce]},
        {"consumer", "consumer123", [:consume]},
        {"app", "app123", [:produce, :consume]}
      ])

    seeded =
      Enum.reduce(default_users, 0, fn {username, password, permissions}, count ->
        hash = hash_password(password)

        case UserStore.insert_user(username, hash, permissions) do
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

  defp hash_password(password) do
    Argon2.hash_pwd_salt(password)
  end

  defp verify_password(password, stored_hash) do
    Argon2.verify_pass(password, stored_hash)
  end

  # Session management moved to Malachi.Auth.SessionManager
  # Sessions table kept for backward compatibility but SessionManager is primary
end
