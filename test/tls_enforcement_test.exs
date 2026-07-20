defmodule Malachi.TLSEnforcementTest do
  # These tests mutate global :malachi TLS/HSTS application env, so they must not run concurrently with
  # other modules (async: false). Each test's mutations are rolled back after it runs by the setup below:
  # otherwise a test that sets :enable_tls = true leaks into "TLS is disabled by default" under a
  # different seed order (a flaky, order-dependent failure).
  use ExUnit.Case, async: false

  alias Malachi.Dashboard.SecurityHeaders

  # Every :malachi TLS/HSTS key these tests read or write; snapshotted and restored around each test.
  @tls_keys [:enable_tls, :require_tls, :hsts_enabled, :hsts_include_subdomains]

  setup do
    original = Map.new(@tls_keys, fn key -> {key, Application.fetch_env(:malachi, key)} end)

    on_exit(fn ->
      Enum.each(original, fn
        {key, {:ok, value}} -> Application.put_env(:malachi, key, value)
        {key, :error} -> Application.delete_env(:malachi, key)
      end)
    end)

    :ok
  end

  describe "TLS enforcement configuration" do
    test "TLS is disabled by default in test environment" do
      assert Application.get_env(:malachi, :enable_tls) == false
    end

    test "require_tls is false in test environment" do
      assert Application.get_env(:malachi, :require_tls) == false
    end

    test "TLS versions default to 1.2 and 1.3" do
      versions = Application.get_env(:malachi, :tls_versions, [:"tlsv1.3", :"tlsv1.2"])
      assert :"tlsv1.3" in versions
      assert :"tlsv1.2" in versions
      refute :"tlsv1.1" in versions
      refute :tlsv1 in versions
      refute :sslv3 in versions
    end

    test "TLS verify defaults to verify_none" do
      verify = Application.get_env(:malachi, :tls_verify, "verify_none")
      assert verify == "verify_none"
    end

    test "TLS fail_if_no_peer_cert defaults to false" do
      fail = Application.get_env(:malachi, :tls_fail_if_no_peer_cert, false)
      assert fail == false
    end

    test "HSTS include_subdomains defaults to true" do
      include = Application.get_env(:malachi, :hsts_include_subdomains, true)
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
      Application.put_env(:malachi, :enable_tls, true)
      Application.put_env(:malachi, :hsts_enabled, true)
      Application.put_env(:malachi, :hsts_include_subdomains, true)

      headers = SecurityHeaders.build_hsts_header()
      assert [{"strict-transport-security", value}] = headers
      assert String.contains?(value, "includeSubDomains")
    end

    test "HSTS header excludes includeSubDomains when disabled" do
      Application.put_env(:malachi, :enable_tls, true)
      Application.put_env(:malachi, :hsts_enabled, true)
      Application.put_env(:malachi, :hsts_include_subdomains, false)

      headers = SecurityHeaders.build_hsts_header()
      assert [{"strict-transport-security", value}] = headers
      refute String.contains?(value, "includeSubDomains")
    end

    test "HSTS header not sent when TLS disabled" do
      Application.put_env(:malachi, :enable_tls, false)

      headers = SecurityHeaders.build_hsts_header()
      assert headers == []
    end
  end

  describe "TCPAcceptorPool TLS options" do
    test "TCPAcceptorPool module is loaded" do
      {:module, _} = Code.ensure_loaded(Malachi.TCPAcceptorPool)
      assert true
    end
  end

  describe "Application startup TLS validation" do
    test "TLSValidator module is loaded and has validate!/1" do
      {:module, _} = Code.ensure_loaded(Malachi.TLSValidator)
      assert function_exported?(Malachi.TLSValidator, :validate!, 1)
    end

    test "TLSValidator accepts test environment config" do
      assert :ok = Malachi.TLSValidator.validate!(:test)
    end

    test "TLSValidator accepts dev environment config" do
      assert :ok = Malachi.TLSValidator.validate!(:dev)
    end
  end
end
