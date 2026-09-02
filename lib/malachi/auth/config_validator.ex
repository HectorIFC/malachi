defmodule Malachi.Auth.ConfigValidator do
  @moduledoc """
  Authentication configuration validator at initialization time.

  Prevents insecure production deployments by checking:

  - Absence of weak default passwords (admin123, producer123, etc.)
  - Minimum password length (default: 12 characters)
  - Existence of at least one admin user

  ## Execution

  Must be called at the beginning of `Application.start/2` before the supervisor tree:

      def start(_type, _args) do
        Malachi.Auth.ConfigValidator.validate!(config_env())
        # ... rest of initialization
      end

  ## Behavior by Environment

  - **Production**: Strict validation - fails with `raise` if problems are found
  - **Dev/Test**: Relaxed validation - only warnings via Logger
  """

  require Logger
  alias Malachi.I18n

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

    # Runs in every env because it is a no-op unless generation is on, and generation is only ever on
    # outside dev/test. A warning rather than a raise: an ephemeral store is exactly what you want in
    # development, and even in production it is a deliberate posture, just a risky one to hit by accident.
    validate_generated_admin_persistence_warn()

    :ok
  end

  ## Production Validators (strict - raise on error)

  defp validate_no_default_passwords! do
    users = Application.get_env(:malachi, :default_users, [])

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
             export MALACHI_#{String.upcase(username)}_PASS="$(openssl rand -base64 32)"

          2. Disable default users and manage via API:
             export MALACHI_DISABLE_DEFAULT_USERS=true

          Generate secure passwords:
             openssl rand -base64 32

          ═══════════════════════════════════════════════════════════════
          """
        end
      end)
    end
  end

  defp validate_password_strength! do
    if Application.get_env(:malachi, :require_strong_passwords, false) do
      users = Application.get_env(:malachi, :default_users, [])
      min_length = Application.get_env(:malachi, :min_password_length, 12)

      Enum.each(users, fn {username, password, _perms} ->
        if String.length(password) < min_length do
          raise """

          ═══════════════════════════════════════════════════════════════
          SECURITY ERROR: Password too short
          ═══════════════════════════════════════════════════════════════

          User '#{username}' has password with #{String.length(password)} characters.
          Minimum required: #{min_length} characters

          Set MALACHI_MIN_PASSWORD_LEN to adjust requirement,
          or provide a longer password.

          ═══════════════════════════════════════════════════════════════
          """
        end
      end)
    end
  end

  defp validate_admin_exists! do
    users = Application.get_env(:malachi, :default_users, [])
    disabled = Application.get_env(:malachi, :disable_default_users, false)
    generate_admin = Application.get_env(:malachi, :generate_admin, false)

    has_admin = Enum.any?(users, fn {_user, _pass, perms} -> :admin in perms end)

    # `generate_admin` means Malachi.Auth will create a random admin at boot, so no admin in config is fine.
    unless has_admin or disabled or generate_admin do
      Logger.warning(I18n.t(:warning_no_admin))
    end
  end

  ## Development Validators (warnings only)

  defp validate_no_default_passwords_warn do
    users = Application.get_env(:malachi, :default_users, [])

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
    if Application.get_env(:malachi, :require_strong_passwords, false) do
      users = Application.get_env(:malachi, :default_users, [])
      min_length = Application.get_env(:malachi, :min_password_length, 12)

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
    users = Application.get_env(:malachi, :default_users, [])
    generate_admin = Application.get_env(:malachi, :generate_admin, false)
    has_admin = Enum.any?(users, fn {_user, _pass, perms} -> :admin in perms end)

    unless has_admin or generate_admin do
      Logger.warning(I18n.t(:warning_no_admin_dev))
    end
  end

  # A generated admin lives only in the ra store. With MALACHI_RA_DATA_DIR unset, `:ra_data_dir` is nil
  # here (config/runtime.exs sets it only when the env var is present) and `Malachi.Application` falls back
  # to a temp directory, so the store is ephemeral and a new admin password is generated and logged on
  # every restart. Warn when both hold. Deployments that mount a volume set the dir, so this stays quiet
  # for them.
  defp validate_generated_admin_persistence_warn do
    generate_admin = Application.get_env(:malachi, :generate_admin, false)
    ra_data_dir = Application.get_env(:malachi, :ra_data_dir)

    if generate_admin and is_nil(ra_data_dir) do
      Logger.warning(I18n.t(:warning_generated_admin_ephemeral))
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
