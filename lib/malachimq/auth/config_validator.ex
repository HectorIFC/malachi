defmodule MalachiMQ.Auth.ConfigValidator do
  @moduledoc """
  Authentication configuration validator at initialization time.

  Prevents insecure production deployments by checking:

  - Absence of weak default passwords (admin123, producer123, etc.)
  - Minimum password length (default: 12 characters)
  - Existence of at least one admin user

  ## Execution

  Must be called at the beginning of `Application.start/2` before the supervisor tree:

      def start(_type, _args) do
        MalachiMQ.Auth.ConfigValidator.validate!(config_env())
        # ... rest of initialization
      end

  ## Behavior by Environment

  - **Production**: Strict validation - fails with `raise` if problems are found
  - **Dev/Test**: Relaxed validation - only warnings via Logger
  """

  require Logger
  alias MalachiMQ.I18n

  @dangerous_passwords [
    "admin123",
    "producer123",
    "consumer123",
    "app123",
    "password",
    "changeme",
    "CHANGEME",
    "123456",
    "qwerty",
    ""
  ]

  @doc """
  Validates authentication configuration.

  ## Parameters

  - `env` - Current environment (`:prod`, `:dev`, `:test`)

  ## Examples

      iex> ConfigValidator.validate!(:dev)
      :ok
      
      iex> ConfigValidator.validate!(:prod)
      # Raises if default passwords detected
  """
  def validate!(env) do
    if env == :prod do
      validate_no_default_passwords!()
      validate_password_strength!()
      validate_admin_exists!()
    else
      # In dev/test, only warnings
      validate_no_default_passwords_warn()
      validate_password_strength_warn()
      validate_admin_exists_warn()
    end

    :ok
  end

  ## Production Validators (strict - raise on error)

  defp validate_no_default_passwords! do
    users = Application.get_env(:malachimq, :default_users, [])

    if Enum.empty?(users) do
      # If no default users, OK (API management)
      :ok
    else
      Enum.each(users, fn {username, password, _perms} ->
        if password in @dangerous_passwords do
          raise """

          ═══════════════════════════════════════════════════════════════
          SECURITY ERROR: Insecure default password detected
          ═══════════════════════════════════════════════════════════════

          User '#{username}' has a weak or default password.
          Production deployments MUST use strong passwords.

          Fix options:

          1. Set strong password via environment variable:
             export MALACHIMQ_#{String.upcase(username)}_PASS="$(openssl rand -base64 32)"

          2. Disable default users and manage via API:
             export MALACHIMQ_DISABLE_DEFAULT_USERS=true

          Generate secure passwords:
             openssl rand -base64 32

          ═══════════════════════════════════════════════════════════════
          """
        end
      end)
    end
  end

  defp validate_password_strength! do
    if Application.get_env(:malachimq, :require_strong_passwords, false) do
      users = Application.get_env(:malachimq, :default_users, [])
      min_length = Application.get_env(:malachimq, :min_password_length, 12)

      Enum.each(users, fn {username, password, _perms} ->
        if String.length(password) < min_length do
          raise """

          ═══════════════════════════════════════════════════════════════
          SECURITY ERROR: Password too short
          ═══════════════════════════════════════════════════════════════

          User '#{username}' has password with #{String.length(password)} characters.
          Minimum required: #{min_length} characters

          Set MALACHIMQ_MIN_PASSWORD_LEN to adjust requirement,
          or provide a longer password.

          ═══════════════════════════════════════════════════════════════
          """
        end
      end)
    end
  end

  defp validate_admin_exists! do
    users = Application.get_env(:malachimq, :default_users, [])
    disabled = Application.get_env(:malachimq, :disable_default_users, false)

    has_admin = Enum.any?(users, fn {_user, _pass, perms} -> :admin in perms end)

    unless has_admin or disabled do
      Logger.warning(I18n.t(:warning_no_admin))
    end
  end

  ## Development Validators (warnings only)

  defp validate_no_default_passwords_warn do
    users = Application.get_env(:malachimq, :default_users, [])

    dangerous_users =
      Enum.filter(users, fn {_username, password, _perms} ->
        password in @dangerous_passwords
      end)

    case dangerous_users do
      [] ->
        :ok

      users ->
        usernames = Enum.map(users, fn {username, _, _} -> username end)

        Logger.warning(I18n.t(:warning_weak_passwords, usernames: inspect(usernames)))
    end
  end

  defp validate_password_strength_warn do
    if Application.get_env(:malachimq, :require_strong_passwords, false) do
      users = Application.get_env(:malachimq, :default_users, [])
      min_length = Application.get_env(:malachimq, :min_password_length, 12)

      weak_users =
        Enum.filter(users, fn {_username, password, _perms} ->
          String.length(password) < min_length
        end)

      case weak_users do
        [] ->
          :ok

        users ->
          usernames = Enum.map(users, fn {username, _, _} -> username end)

          Logger.warning(I18n.t(:warning_short_passwords, usernames: inspect(usernames), min_length: min_length))
      end
    end
  end

  defp validate_admin_exists_warn do
    users = Application.get_env(:malachimq, :default_users, [])
    has_admin = Enum.any?(users, fn {_user, _pass, perms} -> :admin in perms end)

    unless has_admin do
      Logger.warning(I18n.t(:warning_no_admin_dev))
    end
  end

  @doc """
  Returns list of passwords considered dangerous.

  Useful for tests and documentation.
  """
  def dangerous_passwords, do: @dangerous_passwords

  @doc """
  Checks if a password is considered dangerous.

  ## Examples

      iex> ConfigValidator.dangerous_password?("admin123")
      true
      
      iex> ConfigValidator.dangerous_password?("x9K$mP2qL#vR8nF")
      false
  """
  def dangerous_password?(password) do
    password in @dangerous_passwords
  end
end
