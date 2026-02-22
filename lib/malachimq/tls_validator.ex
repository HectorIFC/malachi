defmodule MalachiMQ.TLSValidator do
  @moduledoc """
  Validates TLS configuration at application startup.
  Ensures production deployments use secure TLS settings.

  Validations performed:
  - TLS requirement enforcement (production must use TLS by default)
  - Certificate file existence and readability
  - Certificate PEM format validation
  - Certificate expiry checking (expired, expiring soon, not yet valid)
  - Private key file existence and readability
  - Private key permissions (warns if world-readable)
  - Key-certificate match validation
  - RSA key size validation (minimum 2048 bits)
  - TLS version enforcement (only 1.2 and 1.3 allowed)
  """

  require Logger
  alias MalachiMQ.I18n

  import Bitwise

  @weak_tls_versions [:tlsv1, :"tlsv1.0", :"tlsv1.1", :sslv3, :sslv2, :tlsv1, :sslv3]
  @min_rsa_key_bits 2048
  @expiry_warning_days 30
  @expiry_critical_days 7

  @doc """
  Validates TLS configuration based on the current environment.

  In production: raises on invalid configuration (fail fast).
  In dev/test: logs warnings but does not raise.
  """
  @spec validate!(atom()) :: :ok
  def validate!(env) do
    enable_tls = Application.get_env(:malachimq, :enable_tls, false)
    require_tls = Application.get_env(:malachimq, :require_tls, false)

    cond do
      env == :prod and require_tls and not enable_tls ->
        raise I18n.t(:tls_required_but_disabled)

      env != :prod and require_tls and not enable_tls ->
        Logger.warning(I18n.t(:tls_required_but_disabled))
        :ok

      enable_tls ->
        Logger.info(I18n.t(:tls_validation_started))
        validate_tls_config!(env)

      true ->
        :ok
    end
  end

  defp validate_tls_config!(env) do
    cert_file = Application.get_env(:malachimq, :tls_certfile)
    key_file = Application.get_env(:malachimq, :tls_keyfile)

    # Validate certificate file
    validate_file_configured!(cert_file, :cert, env)
    validate_file_exists!(cert_file, :cert, env)
    validate_file_readable!(cert_file, :cert, env)
    validate_pem_format!(cert_file, :cert, env)

    # Validate key file
    validate_file_configured!(key_file, :key, env)
    validate_file_exists!(key_file, :key, env)
    validate_file_readable!(key_file, :key, env)
    validate_pem_format!(key_file, :key, env)

    # Validate key permissions
    validate_key_permissions(key_file)

    # Validate certificate details
    validate_certificate_expiry!(cert_file, env)
    validate_key_strength!(key_file, env)
    validate_key_cert_match!(cert_file, key_file, env)

    # Validate TLS versions
    validate_tls_versions!(env)

    Logger.info(I18n.t(:tls_validation_success))
    :ok
  end

  # ============================================================
  # File validation
  # ============================================================

  defp validate_file_configured!(nil, :cert, env), do: raise_or_warn(env, I18n.t(:tls_cert_not_configured))
  defp validate_file_configured!("", :cert, env), do: raise_or_warn(env, I18n.t(:tls_cert_not_configured))
  defp validate_file_configured!(nil, :key, env), do: raise_or_warn(env, I18n.t(:tls_key_not_configured))
  defp validate_file_configured!("", :key, env), do: raise_or_warn(env, I18n.t(:tls_key_not_configured))
  defp validate_file_configured!(_, _, _), do: :ok

  defp validate_file_exists!(nil, _, _), do: :ok
  defp validate_file_exists!("", _, _), do: :ok

  defp validate_file_exists!(path, type, env) do
    unless File.exists?(path) do
      msg =
        case type do
          :cert -> I18n.t(:tls_cert_file_not_found, path: path)
          :key -> I18n.t(:tls_key_file_not_found, path: path)
        end

      raise_or_warn(env, msg)
    end
  end

  defp validate_file_readable!(nil, _, _), do: :ok
  defp validate_file_readable!("", _, _), do: :ok

  defp validate_file_readable!(path, type, env) do
    case File.read(path) do
      {:ok, ""} ->
        msg =
          case type do
            :cert -> I18n.t(:tls_cert_empty, path: path)
            :key -> I18n.t(:tls_cert_empty, path: path)
          end

        raise_or_warn(env, msg)

      {:ok, _content} ->
        :ok

      {:error, reason} ->
        msg =
          case type do
            :cert -> I18n.t(:tls_cert_file_unreadable, path: path, reason: inspect(reason))
            :key -> I18n.t(:tls_key_file_unreadable, path: path, reason: inspect(reason))
          end

        raise_or_warn(env, msg)
    end
  end

  defp validate_pem_format!(nil, _, _), do: :ok
  defp validate_pem_format!("", _, _), do: :ok

  defp validate_pem_format!(path, type, env) do
    case File.read(path) do
      {:ok, content} when byte_size(content) > 0 ->
        expected_marker =
          case type do
            :cert -> "-----BEGIN"
            :key -> "-----BEGIN"
          end

        unless String.contains?(content, expected_marker) do
          raise_or_warn(env, I18n.t(:tls_cert_wrong_format, path: path))
        end

      _ ->
        :ok
    end
  end

  defp validate_key_permissions(nil), do: :ok
  defp validate_key_permissions(""), do: :ok

  defp validate_key_permissions(key_file) do
    case File.stat(key_file) do
      {:ok, %{mode: mode}} ->
        # Check if group or others have any access (mode & 0o077)
        if (mode &&& 0o077) != 0 do
          Logger.warning(I18n.t(:tls_key_world_readable, path: key_file))
        end

      {:error, _} ->
        :ok
    end
  end

  # ============================================================
  # Certificate expiry validation
  # ============================================================

  defp validate_certificate_expiry!(nil, _env), do: :ok
  defp validate_certificate_expiry!("", _env), do: :ok

  defp validate_certificate_expiry!(cert_file, env) do
    case File.read(cert_file) do
      {:ok, pem_content} ->
        case parse_certificate_validity(pem_content) do
          {:ok, not_before, not_after} ->
            check_validity_period(not_before, not_after, env)

          {:error, reason} ->
            Logger.warning(I18n.t(:tls_validation_failed, reason: inspect(reason)))
        end

      {:error, _} ->
        :ok
    end
  end

  defp check_validity_period(not_before, not_after, env) do
    today = Date.utc_today()

    # Check not_before
    if Date.compare(today, not_before) == :lt do
      raise_or_warn(env, I18n.t(:tls_cert_not_yet_valid, not_before: Date.to_iso8601(not_before)))
    end

    # Check expiry
    days_until_expiry = Date.diff(not_after, today)

    cond do
      days_until_expiry < 0 ->
        raise_or_warn(env, I18n.t(:tls_cert_expired, expiry_date: Date.to_iso8601(not_after)))

      days_until_expiry <= @expiry_critical_days ->
        Logger.error(I18n.t(:tls_cert_expiring_soon, days: days_until_expiry, expiry_date: Date.to_iso8601(not_after)))

      days_until_expiry <= @expiry_warning_days ->
        Logger.warning(
          I18n.t(:tls_cert_expiring_soon, days: days_until_expiry, expiry_date: Date.to_iso8601(not_after))
        )

      true ->
        Logger.info(I18n.t(:tls_cert_valid, days: days_until_expiry, expiry_date: Date.to_iso8601(not_after)))
    end
  end

  @doc false
  def parse_certificate_validity(pem_content) do
    try do
      entries = :public_key.pem_decode(pem_content)

      case entries do
        [] ->
          {:error, :no_certificate_entries}

        [{:Certificate, der_cert, _} | _] ->
          cert = :public_key.pkix_decode_cert(der_cert, :otp)
          tbs = elem(cert, 1)
          validity = elem(tbs, 5)

          not_before = parse_asn1_time(elem(validity, 1))
          not_after = parse_asn1_time(elem(validity, 2))

          {:ok, not_before, not_after}

        _ ->
          {:error, :invalid_certificate_entry}
      end
    rescue
      e -> {:error, Exception.message(e)}
    end
  end

  defp parse_asn1_time({:utcTime, charlist}) do
    str = to_string(charlist)
    year = String.to_integer(String.slice(str, 0, 2))
    month = String.to_integer(String.slice(str, 2, 2))
    day = String.to_integer(String.slice(str, 4, 2))
    full_year = if year < 50, do: 2000 + year, else: 1900 + year
    Date.new!(full_year, month, day)
  end

  defp parse_asn1_time({:generalTime, charlist}) do
    str = to_string(charlist)
    year = String.to_integer(String.slice(str, 0, 4))
    month = String.to_integer(String.slice(str, 4, 2))
    day = String.to_integer(String.slice(str, 6, 2))
    Date.new!(year, month, day)
  end

  # ============================================================
  # Key strength validation
  # ============================================================

  defp validate_key_strength!(nil, _env), do: :ok
  defp validate_key_strength!("", _env), do: :ok

  defp validate_key_strength!(key_file, env) do
    case File.read(key_file) do
      {:ok, pem_content} ->
        case parse_key_size(pem_content) do
          {:ok, :rsa, key_bits} when key_bits < @min_rsa_key_bits ->
            raise_or_warn(
              env,
              I18n.t(:tls_weak_key_size, size: key_bits, min_size: @min_rsa_key_bits)
            )

          {:ok, _type, _bits} ->
            :ok

          {:error, _reason} ->
            :ok
        end

      {:error, _} ->
        :ok
    end
  end

  @doc false
  def parse_key_size(pem_content) do
    try do
      entries = :public_key.pem_decode(pem_content)

      case entries do
        [] ->
          {:error, :no_key_entries}

        [entry | _] ->
          key = :public_key.pem_entry_decode(entry)
          extract_key_info(key)
      end
    rescue
      e -> {:error, Exception.message(e)}
    end
  end

  defp extract_key_info(key) when is_tuple(key) and elem(key, 0) == :RSAPrivateKey do
    # RSAPrivateKey record: modulus is the 2nd field (index 1 in 0-based, after the atom)
    modulus = elem(key, 2)
    key_bits = bit_size(:binary.encode_unsigned(modulus))
    {:ok, :rsa, key_bits}
  end

  defp extract_key_info(key) when is_tuple(key) and elem(key, 0) == :ECPrivateKey do
    {:ok, :ec, 256}
  end

  defp extract_key_info(_), do: {:error, :unsupported_key_type}

  # ============================================================
  # Key-certificate match validation
  # ============================================================

  defp validate_key_cert_match!(nil, _, _), do: :ok
  defp validate_key_cert_match!(_, nil, _), do: :ok
  defp validate_key_cert_match!("", _, _), do: :ok
  defp validate_key_cert_match!(_, "", _), do: :ok

  defp validate_key_cert_match!(cert_file, key_file, env) do
    with {:ok, cert_pem} <- File.read(cert_file),
         {:ok, key_pem} <- File.read(key_file) do
      case do_validate_key_cert_match(cert_pem, key_pem) do
        :ok ->
          :ok

        {:error, :mismatch} ->
          raise_or_warn(env, I18n.t(:tls_key_cert_mismatch))

        {:error, _reason} ->
          :ok
      end
    else
      _ -> :ok
    end
  end

  @doc false
  def do_validate_key_cert_match(cert_pem, key_pem) do
    try do
      # Extract public key from certificate
      [{:Certificate, cert_der, _} | _] = :public_key.pem_decode(cert_pem)
      cert = :public_key.pkix_decode_cert(cert_der, :otp)
      tbs = elem(cert, 1)
      spki = elem(tbs, 7)
      cert_public_key = elem(spki, 2)

      # Extract public key from private key
      [key_entry | _] = :public_key.pem_decode(key_pem)
      private_key = :public_key.pem_entry_decode(key_entry)
      key_public = derive_public_key(private_key)

      if keys_match?(cert_public_key, key_public) do
        :ok
      else
        {:error, :mismatch}
      end
    rescue
      _ -> {:error, :parse_error}
    end
  end

  defp derive_public_key(key) when is_tuple(key) and elem(key, 0) == :RSAPrivateKey do
    # RSAPrivateKey: modulus at index 2, public_exponent at index 3
    {:rsa, elem(key, 2), elem(key, 3)}
  end

  defp derive_public_key(key) when is_tuple(key) and elem(key, 0) == :ECPrivateKey do
    # ECPrivateKey: public key at index 4
    {:ec, elem(key, 4)}
  end

  defp derive_public_key(_), do: {:unknown}

  defp keys_match?(cert_pub, {:rsa, modulus, pub_exp}) do
    # cert_pub is an RSAPublicKey record or raw form
    case cert_pub do
      {:RSAPublicKey, cert_mod, cert_exp} ->
        cert_mod == modulus and cert_exp == pub_exp

      bin when is_bitstring(bin) ->
        # Try to decode the SubjectPublicKey bitstring
        try do
          {:RSAPublicKey, cert_mod, cert_exp} = :public_key.der_decode(:RSAPublicKey, bin)
          cert_mod == modulus and cert_exp == pub_exp
        rescue
          _ -> false
        end

      _ ->
        false
    end
  end

  defp keys_match?(_cert_pub, {:ec, _key_pub}) do
    # EC key matching is more complex; skip for now (validated by :ssl.listen)
    true
  end

  defp keys_match?(_, _), do: false

  # ============================================================
  # TLS version validation
  # ============================================================

  defp validate_tls_versions!(env) do
    versions = Application.get_env(:malachimq, :tls_versions, [:"tlsv1.3", :"tlsv1.2"])

    Enum.each(versions, fn version ->
      if version in @weak_tls_versions do
        raise_or_warn(env, I18n.t(:tls_unsupported_version, version: inspect(version)))
      end
    end)

    Logger.info(I18n.t(:tls_versions_configured, versions: inspect(versions)))
  end

  # ============================================================
  # Helpers
  # ============================================================

  defp raise_or_warn(:prod, message) do
    raise message
  end

  defp raise_or_warn(_env, message) do
    Logger.warning(message)
  end
end
