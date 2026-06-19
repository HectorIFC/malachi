defmodule Malachi.InjectionAttackTest do
  use ExUnit.Case, async: false
  alias Malachi.Validator

  describe "XSS injection attacks" do
    test "rejects script tags in queue names" do
      attacks = [
        "<script>alert('XSS')</script>",
        "<script>alert(document.cookie)</script>",
        "<img src=x onerror=alert('XSS')>",
        "<svg onload=alert('XSS')>",
        "<iframe src=javascript:alert('XSS')>",
        "javascript:alert('XSS')"
      ]

      for attack <- attacks do
        result = Validator.validate_queue_name(attack)
        assert match?({:error, _}, result), "Failed to reject XSS: #{attack}"
      end
    end

    test "sanitizes HTML correctly" do
      malicious = "<script>alert(1)</script>"
      safe = Validator.sanitize_for_html(malicious)

      refute String.contains?(safe, "<script>")
      assert String.contains?(safe, "&lt;script&gt;")
    end
  end

  describe "SQL injection attacks" do
    test "rejects SQL injection attempts in queue names" do
      attacks = [
        "'; DROP TABLE queues; --",
        "' OR '1'='1",
        "admin'--",
        "1' UNION SELECT * FROM users--",
        "'; DELETE FROM queues WHERE '1'='1"
      ]

      for attack <- attacks do
        result = Validator.validate_queue_name(attack)
        assert match?({:error, _}, result), "Failed to reject SQL injection: #{attack}"
      end
    end
  end

  describe "path traversal attacks" do
    test "rejects directory traversal attempts" do
      attacks = [
        "../../../etc/passwd",
        "..\\..\\..\\windows\\system32",
        "../../etc/shadow",
        "..\\..\\.ssh\\id_rsa",
        "....//....//etc/passwd"
      ]

      for attack <- attacks do
        result = Validator.validate_queue_name(attack)
        assert match?({:error, _}, result), "Failed to reject path traversal: #{attack}"
      end
    end
  end

  describe "command injection attacks" do
    test "rejects command injection attempts" do
      attacks = [
        "; rm -rf /",
        "| cat /etc/passwd",
        "&& whoami",
        "`cat /etc/passwd`",
        "$(cat /etc/passwd)",
        "; ls -la"
      ]

      for attack <- attacks do
        result = Validator.validate_queue_name(attack)
        assert match?({:error, _}, result), "Failed to reject command injection: #{attack}"
      end
    end
  end

  describe "CRLF injection attacks" do
    test "rejects CRLF injection in queue names" do
      attacks = [
        "queue\r\nINJECTED: header",
        "name\r\n\r\nHTTP/1.1 200 OK",
        "queue\nSet-Cookie: evil=true",
        "test\r\nContent-Length: 0\r\n\r\nHTTP/1.1 200 OK"
      ]

      for attack <- attacks do
        result = Validator.validate_queue_name(attack)
        assert match?({:error, _}, result), "Failed to reject CRLF injection: #{attack}"
      end
    end

    test "sanitizes CRLF for logs" do
      malicious = "queue\r\nINJECTED: evil header"
      safe = Validator.sanitize_for_log(malicious)

      refute String.contains?(safe, "\r")
      refute String.contains?(safe, "\n")
      assert String.contains?(safe, "queue")
      assert String.contains?(safe, "INJECTED")
    end
  end

  describe "null byte injection" do
    test "rejects null bytes in queue names" do
      attacks = [
        "queue\x00admin",
        "normal\x00../../etc/passwd",
        "test\x00name"
      ]

      for attack <- attacks do
        result = Validator.validate_queue_name(attack)
        assert match?({:error, _}, result), "Failed to reject null byte: #{inspect(attack)}"
      end
    end
  end

  describe "unicode and homograph attacks" do
    test "rejects unicode control characters" do
      # Right-to-left override
      rtl_attack = "queue\u202Eadmin"
      result = Validator.validate_queue_name(rtl_attack)
      assert match?({:error, _}, result)
    end

    test "rejects cyrillic lookalikes (homograph attack)" do
      # Cyrillic 'а' looks like Latin 'a' but has different code point
      # First char is Cyrillic
      cyrillic_attack = "аdmin"
      result = Validator.validate_queue_name(cyrillic_attack)
      assert match?({:error, _}, result)
    end
  end

  describe "ReDoS (Regular Expression Denial of Service)" do
    test "validates very long strings quickly (< 100ms)" do
      long_string = String.duplicate("a", 100_000)

      {time_us, result} =
        :timer.tc(fn ->
          Validator.validate_queue_name(long_string)
        end)

      time_ms = time_us / 1000
      assert time_ms < 100, "Validation took #{time_ms}ms, expected < 100ms"
      assert match?({:error, :invalid_queue_name_too_long}, result)
    end

    test "validates repetitive patterns quickly" do
      # Patterns that could cause catastrophic backtracking
      patterns = [
        String.duplicate("a", 10_000),
        String.duplicate("ab", 5_000),
        String.duplicate("abc", 3_000)
      ]

      for pattern <- patterns do
        {time_us, _result} =
          :timer.tc(fn ->
            Validator.validate_queue_name(pattern)
          end)

        time_ms = time_us / 1000
        assert time_ms < 100, "Pattern #{String.slice(pattern, 0..10)}... took #{time_ms}ms"
      end
    end
  end

  describe "header injection attacks" do
    test "rejects deeply nested maps (3+ levels)" do
      nested = %{
        "level1" => %{
          "level2" => %{
            "level3" => "value"
          }
        }
      }

      assert {:error, :invalid_headers_invalid_type} = Validator.validate_headers(nested)
    end

    test "rejects large arrays in headers" do
      large_array = Enum.to_list(1..1000)
      headers = %{"tags" => large_array}

      assert {:error, :invalid_headers_invalid_type} = Validator.validate_headers(headers)
    end

    test "rejects headers with mixed invalid types" do
      invalid_headers = [
        %{"nil_value" => nil},
        %{"array" => [1, 2, 3]},
        %{"map" => %{"nested" => true}},
        # If somehow passed through JSON
        %{"function" => fn -> :ok end}
      ]

      for headers <- invalid_headers do
        result = Validator.validate_headers(headers)
        assert match?({:error, _}, result), "Failed to reject: #{inspect(headers)}"
      end
    end
  end

  describe "resource exhaustion attacks" do
    test "rejects extremely long header keys" do
      long_key = String.duplicate("k", 10_000)
      headers = %{long_key => "value"}

      assert {:error, :invalid_headers_key_too_long} = Validator.validate_headers(headers)
    end

    test "rejects extremely long header values" do
      long_value = String.duplicate("v", 10_000)
      headers = %{"key" => long_value}

      assert {:error, :invalid_headers_value_too_long} = Validator.validate_headers(headers)
    end

    test "rejects massive number of headers" do
      headers = Map.new(1..1000, fn i -> {"key#{i}", "value"} end)

      assert {:error, :invalid_headers_too_many} = Validator.validate_headers(headers)
    end
  end

  describe "encoding attacks" do
    test "rejects URL-encoded special characters" do
      attacks = [
        # semicolon
        "queue%3B",
        # slash
        "queue%2F",
        # null byte
        "queue%00",
        # ../
        "%2E%2E%2F"
      ]

      for attack <- attacks do
        result = Validator.validate_queue_name(attack)
        assert match?({:error, _}, result), "Failed to reject URL-encoded: #{attack}"
      end
    end

    test "rejects HTML-encoded special characters" do
      attacks = [
        "queue&lt;script&gt;",
        "queue&#60;test",
        "queue&amp;evil"
      ]

      for attack <- attacks do
        result = Validator.validate_queue_name(attack)
        assert match?({:error, _}, result), "Failed to reject HTML-encoded: #{attack}"
      end
    end
  end
end
