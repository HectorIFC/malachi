defmodule Malachi.DashboardSecurityHeadersTest do
  use ExUnit.Case, async: true

  alias Malachi.Dashboard.SecurityHeaders

  doctest Malachi.Dashboard.SecurityHeaders

  # The full deny-all value, spelled out on purpose: repeating it here is what makes the test fail when a
  # feature is dropped from the policy by accident. Asserting only this one line (never the whole header
  # block) is also what keeps this file async: other suites put_env :enable_tls and :dashboard_csp, which
  # move HSTS and the CSP around underneath us.
  @permissions_policy "accelerometer=(), autoplay=(), camera=(), display-capture=(), " <>
                        "encrypted-media=(), fullscreen=(), geolocation=(), gyroscope=(), " <>
                        "magnetometer=(), microphone=(), midi=(), payment=(), picture-in-picture=(), " <>
                        "publickey-credentials-get=(), screen-wake-lock=(), usb=(), xr-spatial-tracking=()"

  @html_response "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 6\r\n\r\n<html>"

  describe "add_security_headers/3 Permissions-Policy" do
    test "emits the deny-all policy on a document response" do
      response = SecurityHeaders.add_security_headers(@html_response, "/")

      assert String.contains?(response, "\r\nPermissions-Policy: #{@permissions_policy}\r\n")
    end

    test "emits it on a non-document route too" do
      # /metrics serves Prometheus text, where the header is inert, but the policy is deliberately uniform
      # across every dashboard response rather than branching per route.
      metrics_response = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nmalachi_up 1"

      response = SecurityHeaders.add_security_headers(metrics_response, "/metrics")

      assert String.contains?(response, "\r\nPermissions-Policy: #{@permissions_policy}\r\n")
    end

    test "emits the header exactly once" do
      response = SecurityHeaders.add_security_headers(@html_response, "/")

      assert length(String.split(response, "Permissions-Policy:")) == 2
    end

    test "leaves the response body untouched" do
      response = SecurityHeaders.add_security_headers(@html_response, "/")

      assert String.ends_with?(response, "\r\n\r\n<html>")
    end
  end

  describe "prepend_headers/2" do
    test "capitalizes every dash-separated part of the header name" do
      response =
        SecurityHeaders.prepend_headers(
          "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\nBody",
          [{"permissions-policy", "camera=()"}]
        )

      assert response ==
               "HTTP/1.1 200 OK\r\nPermissions-Policy: camera=()\r\nContent-Type: text/html\r\n\r\nBody"
    end

    test "returns a malformed response unchanged" do
      # No CRLF at all, so there is no status line to insert after. The headers are dropped rather than
      # corrupting what the caller is about to write to the socket.
      malformed = "HTTP/1.1 200 OK"

      assert SecurityHeaders.prepend_headers(malformed, [{"permissions-policy", "camera=()"}]) ==
               malformed
    end

    test "keeps an empty header list a no-op" do
      response = "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\nBody"

      assert SecurityHeaders.prepend_headers(response, []) == response
    end
  end
end
