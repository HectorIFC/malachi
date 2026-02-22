defmodule MalachiMQ.Test.DashboardHelper do
  @moduledoc """
  HTTP helper utilities for dashboard tests.

  Provides convenience functions for creating HTTP connections to the dashboard
  with cookie-based authentication, following the same pattern as `TCPHelper`
  for the MQ wire protocol.
  """

  @dashboard_port Application.compile_env(:malachimq, :dashboard_port, 4041)

  @doc """
  Connects to the MalachiMQ dashboard HTTP server.

  ## Options

  - `:timeout` - Connection timeout in ms (default: 1000)
  - `:port` - Dashboard port (default: from application config)

  ## Examples

      {:ok, socket} = DashboardHelper.connect()
      {:ok, socket} = DashboardHelper.connect(port: 4041)
  """
  def connect(opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 1000)
    port = Keyword.get(opts, :port, @dashboard_port)

    :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], timeout)
  end

  @doc """
  Sends an HTTP request and returns the response.

  ## Options

  - `:headers` - Map of extra headers (default: %{})
  - `:body` - Request body string (default: nil)
  - `:recv_timeout` - Response receive timeout in ms (default: 2000)

  ## Examples

      {:ok, response} = DashboardHelper.request(socket, :GET, "/")
      {:ok, response} = DashboardHelper.request(socket, :GET, "/", headers: %{"Cookie" => "malachimq_token=abc"})
      {:ok, response} = DashboardHelper.request(socket, :POST, "/login", body: body)
  """
  def request(socket, method, path, opts \\ []) do
    extra_headers = Keyword.get(opts, :headers, %{})
    body = Keyword.get(opts, :body, nil)
    recv_timeout = Keyword.get(opts, :recv_timeout, 2000)

    headers =
      Map.merge(%{"Host" => "localhost"}, extra_headers)

    headers =
      if body do
        Map.merge(headers, %{
          "Content-Type" => "application/json",
          "Content-Length" => to_string(byte_size(body))
        })
      else
        headers
      end

    header_lines =
      Enum.map_join(headers, fn {k, v} -> "#{k}: #{v}\r\n" end)

    request_str = "#{method} #{path} HTTP/1.1\r\n#{header_lines}\r\n#{body || ""}"

    :gen_tcp.send(socket, request_str)
    :gen_tcp.recv(socket, 0, recv_timeout)
  end

  @doc """
  Sends an authenticated HTTP request using a cookie.

  The token is sent as `Cookie: malachimq_token=<token>`, matching the
  HttpOnly cookie set by the dashboard on login.

  ## Options

  Same as `request/4`, but `Cookie` header is automatically added.

  ## Examples

      {:ok, response} = DashboardHelper.authenticated_request(socket, :GET, "/", token)
      {:ok, response} = DashboardHelper.authenticated_request(socket, :GET, "/", token, recv_timeout: 5000)
  """
  def authenticated_request(socket, method, path, token, opts \\ []) do
    extra_headers = Keyword.get(opts, :headers, %{})

    merged_headers =
      Map.put(extra_headers, "Cookie", "malachimq_token=#{token}")

    request(socket, method, path, Keyword.put(opts, :headers, merged_headers))
  end

  @doc """
  Performs a login via POST /login and returns the token.

  Extracts the token from both the Set-Cookie header and the JSON body.

  ## Examples

      {:ok, token} = DashboardHelper.login("admin", "admin_pass_123")
      {:error, reason} = DashboardHelper.login("admin", "wrong_password")
  """
  def login(username, password, opts \\ []) do
    port = Keyword.get(opts, :port, @dashboard_port)

    case connect(port: port) do
      {:ok, socket} ->
        body = Jason.encode!(%{"username" => username, "password" => password})

        case request(socket, :POST, "/login", body: body) do
          {:ok, response} ->
            :gen_tcp.close(socket)
            parse_login_response(response)

          {:error, reason} ->
            :gen_tcp.close(socket)
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Extracts the Set-Cookie value from an HTTP response string.

  ## Examples

      "abc123" = DashboardHelper.extract_set_cookie(response)
      nil = DashboardHelper.extract_set_cookie(response_without_cookie)
  """
  def extract_set_cookie(response) do
    response
    |> String.split("\r\n")
    |> Enum.find_value(fn line ->
      # Check header name case-insensitively, but preserve token value case
      if String.downcase(line) |> String.starts_with?("set-cookie: malachimq_token=") do
        # Extract value after "malachimq_token=" from the ORIGINAL line (preserving case)
        [_header_part, cookie_part] = String.split(line, ": ", parts: 2)

        case String.split(cookie_part, "=", parts: 2) do
          [_name, rest] -> rest |> String.split(";", parts: 2) |> List.first()
          _ -> nil
        end
      else
        nil
      end
    end)
  end

  # -- Private --

  defp parse_login_response(response) do
    cond do
      String.contains?(response, "200 OK") or String.contains?(response, "200") ->
        # Extract token from JSON body
        case extract_json_body(response) do
          {:ok, %{"s" => "ok", "token" => token}} -> {:ok, token}
          _ -> {:error, :parse_error}
        end

      String.contains?(response, "403") ->
        {:error, :forbidden}

      String.contains?(response, "429") ->
        {:error, :rate_limited}

      true ->
        {:error, :unexpected_response}
    end
  end

  defp extract_json_body(response) do
    case String.split(response, "\r\n\r\n", parts: 2) do
      [_headers, body] -> Jason.decode(String.trim(body))
      _ -> {:error, :no_body}
    end
  end
end
