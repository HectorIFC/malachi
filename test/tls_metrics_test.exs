defmodule MalachiMQ.TLSMetricsTest do
  use ExUnit.Case, async: false

  alias MalachiMQ.Metrics

  describe "TLS metrics functions" do
    test "increment_tls_handshake_success/0 increments counter" do
      # Get initial value
      initial = get_tls_metric(:tls_handshake_success)

      Metrics.increment_tls_handshake_success()
      Metrics.increment_tls_handshake_success()

      current = get_tls_metric(:tls_handshake_success)
      assert current == initial + 2
    end

    test "increment_tls_handshake_failed/0 increments counter" do
      initial = get_tls_metric(:tls_handshake_failed)

      Metrics.increment_tls_handshake_failed()

      current = get_tls_metric(:tls_handshake_failed)
      assert current == initial + 1
    end

    test "record_tls_version/1 records version counters" do
      initial_13 = get_tls_metric({:tls_version, :"tlsv1.3"})
      initial_12 = get_tls_metric({:tls_version, :"tlsv1.2"})

      Metrics.record_tls_version(:"tlsv1.3")
      Metrics.record_tls_version(:"tlsv1.3")
      Metrics.record_tls_version(:"tlsv1.2")

      assert get_tls_metric({:tls_version, :"tlsv1.3"}) == initial_13 + 2
      assert get_tls_metric({:tls_version, :"tlsv1.2"}) == initial_12 + 1
    end
  end

  describe "TLS section in system metrics" do
    test "get_system_metrics/0 includes tls section" do
      metrics = Metrics.get_system_metrics()

      assert Map.has_key?(metrics, :tls)
      tls = metrics.tls

      assert Map.has_key?(tls, :enabled)
      assert Map.has_key?(tls, :required)
      assert Map.has_key?(tls, :handshakes_success)
      assert Map.has_key?(tls, :handshakes_failed)
      assert Map.has_key?(tls, :versions)

      assert is_boolean(tls.enabled)
      assert is_boolean(tls.required)
      assert is_integer(tls.handshakes_success)
      assert is_integer(tls.handshakes_failed)
      assert is_map(tls.versions)
    end

    test "TLS metrics reflect current config" do
      metrics = Metrics.get_system_metrics()

      # In test env, TLS should be disabled
      assert metrics.tls.enabled == false
      assert metrics.tls.required == false
    end

    test "TLS version counters are accessible in system metrics" do
      metrics = Metrics.get_system_metrics()

      versions = metrics.tls.versions
      assert Map.has_key?(versions, :"tlsv1.3")
      assert Map.has_key?(versions, :"tlsv1.2")
      assert is_integer(versions[:"tlsv1.3"])
      assert is_integer(versions[:"tlsv1.2"])
    end
  end

  describe "TLS metrics functions existence" do
    test "Metrics module exports TLS functions" do
      {:module, _} = Code.ensure_loaded(Metrics)

      assert function_exported?(Metrics, :increment_tls_handshake_success, 0)
      assert function_exported?(Metrics, :increment_tls_handshake_failed, 0)
      assert function_exported?(Metrics, :record_tls_version, 1)
    end
  end

  # Helper to read raw ETS counter
  defp get_tls_metric(key) do
    case :ets.lookup(:malachimq_metrics, key) do
      [{^key, value}] -> value
      [] -> 0
    end
  end
end
