defmodule Malachi.TLSValidatorTest do
  use ExUnit.Case, async: true

  alias Malachi.TLSValidator

  # ============================================================
  # Test certificate generation helpers
  # ============================================================

  defp generate_self_signed_cert(opts \\ []) do
    validity_days = Keyword.get(opts, :validity_days, 365)
    key_size = Keyword.get(opts, :key_size, 2048)
    not_before_offset = Keyword.get(opts, :not_before_offset_days, 0)

    # OTP 28's pkix_test_root_cert expects date tuples {Y, M, D}, not datetime tuples
    today_days = :calendar.date_to_gregorian_days(:erlang.date())

    not_before_days = today_days + not_before_offset
    not_after_days = not_before_days + validity_days

    # Ensure not_before <= not_after for valid ASN1 encoding
    {actual_nb_days, actual_na_days} =
      if not_before_days <= not_after_days do
        {not_before_days, not_after_days}
      else
        {not_after_days, not_before_days}
      end

    not_before_date = :calendar.gregorian_days_to_date(actual_nb_days)
    not_after_date = :calendar.gregorian_days_to_date(actual_na_days)

    # Use OTP's built-in test certificate generator (compatible with OTP 28+)
    root_opts = [
      key: {:rsa, key_size, 65_537},
      validity: {not_before_date, not_after_date},
      digest: :sha256
    ]

    %{cert: cert_der, key: private_key} =
      :public_key.pkix_test_root_cert(~c"Malachi Test", root_opts)

    cert_pem = :public_key.pem_encode([{:Certificate, cert_der, :not_encrypted}])
    key_der = :public_key.der_encode(:RSAPrivateKey, private_key)
    key_pem = :public_key.pem_encode([{:RSAPrivateKey, key_der, :not_encrypted}])

    {cert_pem, key_pem}
  end

  defp write_temp_files(cert_pem, key_pem) do
    dir = System.tmp_dir!()
    suffix = :rand.uniform(1_000_000)
    cert_path = Path.join(dir, "test_cert_#{suffix}.pem")
    key_path = Path.join(dir, "test_key_#{suffix}.pem")

    File.write!(cert_path, cert_pem)
    File.write!(key_path, key_pem)

    {cert_path, key_path}
  end

  defp cleanup_files(paths) do
    Enum.each(paths, fn path ->
      if path && File.exists?(path) do
        File.rm(path)
      end
    end)
  end

  # ============================================================
  # Helper to run validation with specific config
  # ============================================================

  defp with_tls_config(config, fun) do
    original = %{
      enable_tls: Application.get_env(:malachi, :enable_tls),
      require_tls: Application.get_env(:malachi, :require_tls),
      tls_certfile: Application.get_env(:malachi, :tls_certfile),
      tls_keyfile: Application.get_env(:malachi, :tls_keyfile),
      tls_cacertfile: Application.get_env(:malachi, :tls_cacertfile),
      tls_versions: Application.get_env(:malachi, :tls_versions),
      tls_verify: Application.get_env(:malachi, :tls_verify),
      tls_fail_if_no_peer_cert: Application.get_env(:malachi, :tls_fail_if_no_peer_cert)
    }

    try do
      Enum.each(config, fn {key, value} ->
        Application.put_env(:malachi, key, value)
      end)

      fun.()
    after
      Enum.each(original, fn {key, value} ->
        if value == nil do
          Application.delete_env(:malachi, key)
        else
          Application.put_env(:malachi, key, value)
        end
      end)
    end
  end

  # ============================================================
  # Tests: TLS requirement enforcement
  # ============================================================

  describe "validate!/1 - TLS requirement enforcement" do
    test "returns :ok when TLS is not required and not enabled" do
      with_tls_config(%{enable_tls: false, require_tls: false}, fn ->
        assert :ok = TLSValidator.validate!(:dev)
      end)
    end

    test "returns :ok when TLS is not required in test env" do
      with_tls_config(%{enable_tls: false, require_tls: false}, fn ->
        assert :ok = TLSValidator.validate!(:test)
      end)
    end

    test "raises in production when TLS is required but not enabled" do
      with_tls_config(%{enable_tls: false, require_tls: true}, fn ->
        assert_raise RuntimeError, ~r/TLS/, fn ->
          TLSValidator.validate!(:prod)
        end
      end)
    end

    test "warns in development when TLS is required but not enabled" do
      with_tls_config(%{enable_tls: false, require_tls: true}, fn ->
        assert :ok = TLSValidator.validate!(:dev)
      end)
    end
  end

  # ============================================================
  # Tests: Certificate file validation
  # ============================================================

  describe "validate!/1 - certificate file validation" do
    test "raises when cert file not configured in prod" do
      with_tls_config(%{enable_tls: true, require_tls: false, tls_certfile: nil, tls_keyfile: nil}, fn ->
        assert_raise RuntimeError, ~r/certificate/, fn ->
          TLSValidator.validate!(:prod)
        end
      end)
    end

    test "raises when cert file does not exist in prod" do
      with_tls_config(
        %{
          enable_tls: true,
          require_tls: false,
          tls_certfile: "/nonexistent/cert.pem",
          tls_keyfile: "/nonexistent/key.pem"
        },
        fn ->
          assert_raise RuntimeError, ~r/not found/, fn ->
            TLSValidator.validate!(:prod)
          end
        end
      )
    end

    test "raises when cert file is empty in prod" do
      dir = System.tmp_dir!()
      suffix = :rand.uniform(1_000_000)
      cert_path = Path.join(dir, "empty_cert_#{suffix}.pem")
      key_path = Path.join(dir, "empty_key_#{suffix}.pem")

      File.write!(cert_path, "")
      File.write!(key_path, "")

      try do
        with_tls_config(
          %{enable_tls: true, require_tls: false, tls_certfile: cert_path, tls_keyfile: key_path},
          fn ->
            assert_raise RuntimeError, ~r/empty/, fn ->
              TLSValidator.validate!(:prod)
            end
          end
        )
      after
        cleanup_files([cert_path, key_path])
      end
    end

    test "raises when cert file is not PEM format in prod" do
      dir = System.tmp_dir!()
      suffix = :rand.uniform(1_000_000)
      cert_path = Path.join(dir, "bad_cert_#{suffix}.pem")
      key_path = Path.join(dir, "bad_key_#{suffix}.pem")

      # Write binary (DER-like) content
      File.write!(cert_path, <<0x30, 0x82, 0x01, 0x00>>)
      File.write!(key_path, <<0x30, 0x82, 0x01, 0x00>>)

      try do
        with_tls_config(
          %{enable_tls: true, require_tls: false, tls_certfile: cert_path, tls_keyfile: key_path},
          fn ->
            assert_raise RuntimeError, ~r/format|PEM/, fn ->
              TLSValidator.validate!(:prod)
            end
          end
        )
      after
        cleanup_files([cert_path, key_path])
      end
    end

    test "warns in dev when cert file does not exist" do
      with_tls_config(
        %{
          enable_tls: true,
          require_tls: false,
          tls_certfile: "/nonexistent/cert.pem",
          tls_keyfile: "/nonexistent/key.pem"
        },
        fn ->
          assert :ok = TLSValidator.validate!(:dev)
        end
      )
    end
  end

  # ============================================================
  # Tests: Certificate expiry validation
  # ============================================================

  describe "validate!/1 - certificate expiry" do
    test "validates a valid certificate" do
      {cert_pem, key_pem} = generate_self_signed_cert(validity_days: 365)
      {cert_path, key_path} = write_temp_files(cert_pem, key_pem)

      try do
        with_tls_config(
          %{enable_tls: true, require_tls: false, tls_certfile: cert_path, tls_keyfile: key_path},
          fn ->
            assert :ok = TLSValidator.validate!(:dev)
          end
        )
      after
        cleanup_files([cert_path, key_path])
      end
    end

    test "raises on expired certificate in prod" do
      {cert_pem, key_pem} = generate_self_signed_cert(validity_days: -10, not_before_offset_days: -20)
      {cert_path, key_path} = write_temp_files(cert_pem, key_pem)

      try do
        with_tls_config(
          %{enable_tls: true, require_tls: false, tls_certfile: cert_path, tls_keyfile: key_path},
          fn ->
            assert_raise RuntimeError, ~r/EXPIRED/, fn ->
              TLSValidator.validate!(:prod)
            end
          end
        )
      after
        cleanup_files([cert_path, key_path])
      end
    end

    test "raises on not-yet-valid certificate in prod" do
      {cert_pem, key_pem} = generate_self_signed_cert(validity_days: 365, not_before_offset_days: 30)
      {cert_path, key_path} = write_temp_files(cert_pem, key_pem)

      try do
        with_tls_config(
          %{enable_tls: true, require_tls: false, tls_certfile: cert_path, tls_keyfile: key_path},
          fn ->
            assert_raise RuntimeError, ~r/not yet valid/, fn ->
              TLSValidator.validate!(:prod)
            end
          end
        )
      after
        cleanup_files([cert_path, key_path])
      end
    end
  end

  # ============================================================
  # Tests: Certificate parsing
  # ============================================================

  describe "parse_certificate_validity/1" do
    test "parses valid PEM certificate" do
      {cert_pem, _key_pem} = generate_self_signed_cert(validity_days: 365)

      assert {:ok, not_before, not_after} = TLSValidator.parse_certificate_validity(cert_pem)
      assert %Date{} = not_before
      assert %Date{} = not_after
      assert Date.compare(not_after, not_before) == :gt
    end

    test "returns error for invalid PEM" do
      assert {:error, _} = TLSValidator.parse_certificate_validity("not a certificate")
    end

    test "returns error for empty content" do
      assert {:error, :no_certificate_entries} = TLSValidator.parse_certificate_validity("")
    end
  end

  # ============================================================
  # Tests: Key size validation
  # ============================================================

  describe "parse_key_size/1" do
    test "parses RSA 2048-bit key" do
      {_cert_pem, key_pem} = generate_self_signed_cert(key_size: 2048)

      assert {:ok, :rsa, bits} = TLSValidator.parse_key_size(key_pem)
      assert bits >= 2048
    end

    test "returns error for invalid PEM" do
      assert {:error, _} = TLSValidator.parse_key_size("not a key")
    end
  end

  # ============================================================
  # Tests: Key strength validation
  # ============================================================

  describe "validate!/1 - key strength" do
    test "raises on weak RSA key (1024-bit) in prod" do
      {cert_pem, key_pem} = generate_self_signed_cert(key_size: 1024)
      {cert_path, key_path} = write_temp_files(cert_pem, key_pem)

      try do
        with_tls_config(
          %{enable_tls: true, require_tls: false, tls_certfile: cert_path, tls_keyfile: key_path},
          fn ->
            assert_raise RuntimeError, ~r/key size|2048/, fn ->
              TLSValidator.validate!(:prod)
            end
          end
        )
      after
        cleanup_files([cert_path, key_path])
      end
    end

    test "accepts 2048-bit RSA key in prod" do
      {cert_pem, key_pem} = generate_self_signed_cert(key_size: 2048)
      {cert_path, key_path} = write_temp_files(cert_pem, key_pem)

      try do
        with_tls_config(
          %{enable_tls: true, require_tls: false, tls_certfile: cert_path, tls_keyfile: key_path},
          fn ->
            assert :ok = TLSValidator.validate!(:prod)
          end
        )
      after
        cleanup_files([cert_path, key_path])
      end
    end
  end

  # ============================================================
  # Tests: Key-certificate match validation
  # ============================================================

  describe "do_validate_key_cert_match/2" do
    test "returns :ok for matching key and certificate" do
      {cert_pem, key_pem} = generate_self_signed_cert()
      assert :ok = TLSValidator.do_validate_key_cert_match(cert_pem, key_pem)
    end

    test "returns error for mismatched key and certificate" do
      {cert_pem, _key_pem1} = generate_self_signed_cert()
      {_cert_pem2, key_pem2} = generate_self_signed_cert()

      assert {:error, :mismatch} = TLSValidator.do_validate_key_cert_match(cert_pem, key_pem2)
    end

    test "returns error for invalid PEM" do
      assert {:error, :parse_error} = TLSValidator.do_validate_key_cert_match("bad", "bad")
    end
  end

  # ============================================================
  # Tests: TLS version validation
  # ============================================================

  describe "validate!/1 - TLS version validation" do
    test "raises on weak TLS version in prod" do
      {cert_pem, key_pem} = generate_self_signed_cert()
      {cert_path, key_path} = write_temp_files(cert_pem, key_pem)

      try do
        with_tls_config(
          %{
            enable_tls: true,
            require_tls: false,
            tls_certfile: cert_path,
            tls_keyfile: key_path,
            tls_versions: [:"tlsv1.1", :"tlsv1.2"]
          },
          fn ->
            assert_raise RuntimeError, ~r/insecure|Insecure|insegura/, fn ->
              TLSValidator.validate!(:prod)
            end
          end
        )
      after
        cleanup_files([cert_path, key_path])
      end
    end

    test "accepts TLS 1.2 and 1.3" do
      {cert_pem, key_pem} = generate_self_signed_cert()
      {cert_path, key_path} = write_temp_files(cert_pem, key_pem)

      try do
        with_tls_config(
          %{
            enable_tls: true,
            require_tls: false,
            tls_certfile: cert_path,
            tls_keyfile: key_path,
            tls_versions: [:"tlsv1.3", :"tlsv1.2"]
          },
          fn ->
            assert :ok = TLSValidator.validate!(:prod)
          end
        )
      after
        cleanup_files([cert_path, key_path])
      end
    end
  end

  # ============================================================
  # Tests: Full validation in production mode
  # ============================================================

  describe "validate!/1 - full production validation" do
    test "succeeds with valid configuration" do
      {cert_pem, key_pem} = generate_self_signed_cert(validity_days: 365, key_size: 2048)
      {cert_path, key_path} = write_temp_files(cert_pem, key_pem)

      try do
        with_tls_config(
          %{
            enable_tls: true,
            require_tls: false,
            tls_certfile: cert_path,
            tls_keyfile: key_path,
            tls_versions: [:"tlsv1.3", :"tlsv1.2"]
          },
          fn ->
            assert :ok = TLSValidator.validate!(:prod)
          end
        )
      after
        cleanup_files([cert_path, key_path])
      end
    end
  end
end
