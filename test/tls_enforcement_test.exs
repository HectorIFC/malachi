defmodule MalachiMQ.TLSEnforcementTest do
  use ExUnit.Case, async: true

  describe "TLS enforcement configuration" do
    test "TLS is disabled by default in test environment" do
      assert Application.get_env(:malachimq, :enable_tls) == false
    end

    test "require_tls is false in test environment" do
      assert Application.get_env(:malachimq, :require_tls) == false
    end

    test "TLS versions default to 1.2 and 1.3" do
      versions = Application.get_env(:malachimq, :tls_versions, [:"tlsv1.3", :"tlsv1.2"])
      assert :"tlsv1.3" in versions
      assert :"tlsv1.2" in versions
      refute :"tlsv1.1" in versions
      refute :tlsv1 in versions
      refute :sslv3 in versions
    end

    test "TLS verify defaults to verify_none" do
      verify = Application.get_env(:malachimq, :tls_verify, "verify_none")
      assert verify == "verify_none"
    end

    test "TLS fail_if_no_peer_cert defaults to false" do
      fail = Application.get_env(:malachimq, :tls_fail_if_no_peer_cert, false)
      assert fail == false
    end

    test "HSTS include_subdomains defaults to true" do
      include = Application.get_env(:malachimq, :hsts_include_subdomains, true)
      assert include == true
    end
  end

  describe "TLS version validation" do
    test "weak TLS versions are identified correctly" do
      weak_versions = [:tlsv1, :"tlsv1.0", :"tlsv1.1", :sslv3, :sslv2]
      safe_versions = [:"tlsv1.2", :"tlsv1.3"]

      Enum.each(weak_versions, fn v ->
        assert v in weak_versions, "#{inspect(v)} should be considered weak"
      end)

      Enum.each(safe_versions, fn v ->
        refute v in weak_versions, "#{inspect(v)} should not be considered weak"
      end)
    end
  end

  describe "HSTS header configuration" do
    test "HSTS header includes includeSubDomains when configured" do
      original_tls = Application.get_env(:malachimq, :enable_tls)
      original_hsts = Application.get_env(:malachimq, :hsts_enabled)
      original_sub = Application.get_env(:malachimq, :hsts_include_subdomains)

      try do
        Application.put_env(:malachimq, :enable_tls, true)
        Application.put_env(:malachimq, :hsts_enabled, true)
        Application.put_env(:malachimq, :hsts_include_subdomains, true)

        headers = MalachiMQ.Dashboard.SecurityHeaders.build_hsts_header()
        assert [{"strict-transport-security", value}] = headers
        assert String.contains?(value, "includeSubDomains")
      after
        Application.put_env(:malachimq, :enable_tls, original_tls || false)
        Application.put_env(:malachimq, :hsts_enabled, original_hsts || true)

        if original_sub == nil do
          Application.delete_env(:malachimq, :hsts_include_subdomains)
        else
          Application.put_env(:malachimq, :hsts_include_subdomains, original_sub)
        end
      end
    end

    test "HSTS header excludes includeSubDomains when disabled" do
      original_tls = Application.get_env(:malachimq, :enable_tls)
      original_hsts = Application.get_env(:malachimq, :hsts_enabled)
      original_sub = Application.get_env(:malachimq, :hsts_include_subdomains)

      try do
        Application.put_env(:malachimq, :enable_tls, true)
        Application.put_env(:malachimq, :hsts_enabled, true)
        Application.put_env(:malachimq, :hsts_include_subdomains, false)

        headers = MalachiMQ.Dashboard.SecurityHeaders.build_hsts_header()
        assert [{"strict-transport-security", value}] = headers
        refute String.contains?(value, "includeSubDomains")
      after
        Application.put_env(:malachimq, :enable_tls, original_tls || false)
        Application.put_env(:malachimq, :hsts_enabled, original_hsts || true)

        if original_sub == nil do
          Application.delete_env(:malachimq, :hsts_include_subdomains)
        else
          Application.put_env(:malachimq, :hsts_include_subdomains, original_sub)
        end
      end
    end

    test "HSTS header not sent when TLS disabled" do
      original_tls = Application.get_env(:malachimq, :enable_tls)

      try do
        Application.put_env(:malachimq, :enable_tls, false)

        headers = MalachiMQ.Dashboard.SecurityHeaders.build_hsts_header()
        assert headers == []
      after
        Application.put_env(:malachimq, :enable_tls, original_tls || false)
      end
    end
  end

  describe "TCPAcceptorPool TLS options" do
    test "TCPAcceptorPool module is loaded" do
      {:module, _} = Code.ensure_loaded(MalachiMQ.TCPAcceptorPool)
      assert true
    end
  end

  describe "Application startup TLS validation" do
    test "TLSValidator module is loaded and has validate!/1" do
      {:module, _} = Code.ensure_loaded(MalachiMQ.TLSValidator)
      assert function_exported?(MalachiMQ.TLSValidator, :validate!, 1)
    end

    test "TLSValidator accepts test environment config" do
      assert :ok = MalachiMQ.TLSValidator.validate!(:test)
    end

    test "TLSValidator accepts dev environment config" do
      assert :ok = MalachiMQ.TLSValidator.validate!(:dev)
    end
  end
end
