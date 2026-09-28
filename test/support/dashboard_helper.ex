defmodule Malachi.Test.DashboardHelper do
  @moduledoc """
  HTTP helper utilities for dashboard tests.

  Provides convenience functions for creating HTTP connections to the dashboard
  with cookie-based authentication, following the same pattern as `TCPHelper`
  for the MQ wire protocol.
  """

  @doc """
  Connects to the Malachi dashboard HTTP server.

  ## Options

  - `:timeout` - Connection timeout in ms (default: 1000)
  - `:port` - Dashboard port (default: the one the application's dashboard bound, `Malachi.Dashboard.port/0`)

  ## Examples

      {:ok, socket} = DashboardHelper.connect()
      {:ok, socket} = DashboardHelper.connect(port: 4041)
  """
  def connect(opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 1000)
    port = Keyword.get_lazy(opts, :port, &Malachi.Dashboard.port/0)

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
      {:ok, response} = DashboardHelper.request(socket, :GET, "/", headers: %{"Cookie" => "malachi_token=abc"})
      {:ok, response} = DashboardHelper.request(socket, :POST, "/login", body: body)
  """
  def request(socket, method, path, opts \\ []) do
    :ok = send_request(socket, method, path, opts)
    :gen_tcp.recv(socket, 0, Keyword.get(opts, :recv_timeout, 2000))
  end

  @doc """
  Sends an HTTP request without reading the response, for a caller that reads it some other way (see
  `recv_framed/2`). Takes the `:headers` and `:body` options of `request/4`.
  """
  def send_request(socket, method, path, opts \\ []) do
    extra_headers = Keyword.get(opts, :headers, %{})
    body = Keyword.get(opts, :body, nil)

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
  end

  @doc """
  Sends an authenticated HTTP request using a cookie.

  The token is sent as `Cookie: malachi_token=<token>`, matching the
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
      Map.put(extra_headers, "Cookie", "malachi_token=#{token}")

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
    case connect(Keyword.take(opts, [:port])) do
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
  Scrapes `/metrics` the way a harness does, with a bearer `token` and an Accept header asking for the
  Prometheus exposition, and returns the whole response once the server closes it. Takes `:port`.
  """
  def scrape_metrics(token, opts \\ []) do
    {:ok, socket} = connect(Keyword.take(opts, [:port]))

    :ok =
      :gen_tcp.send(
        socket,
        "GET /metrics HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer #{token}\r\n" <>
          "Accept: text/plain\r\nConnection: close\r\n\r\n"
      )

    response = read_until_closed(socket, "")
    :gen_tcp.close(socket)
    response
  end

  @doc """
  Reads one response by its framing instead of by a single `recv`: the head up to the blank line, then
  exactly the `Content-Length` bytes of body, then everything else that arrives before the server closes
  the socket, returned as `trailing`. A correctly framed response has an empty `trailing`: any byte there
  would be read as the start of the next response on a reused connection.

  Returns `{:ok, %{status: integer, headers: [{lowercase_name, value}], body: binary | nil, trailing:
  binary}}`, with every header in the order sent, so a repeated one shows up twice. A response without a
  `Content-Length` (the SSE stream) comes back with `body: nil` and is not drained, since it never closes
  on its own. `{:error, {reason, bytes_read}}` when the socket fails or closes before the frame is whole.
  """
  def recv_framed(socket, timeout \\ 2000) do
    with {:ok, head, rest} <- recv_head(socket, "", timeout) do
      [status_line | header_lines] = String.split(head, "\r\n")
      [_version, code | _reason] = String.split(status_line, " ", parts: 3)

      headers =
        Enum.map(header_lines, fn line ->
          [name, value] = String.split(line, ":", parts: 2)
          {String.downcase(name), String.trim(value)}
        end)

      response = %{status: String.to_integer(code), headers: headers}

      case List.keyfind(headers, "content-length", 0) do
        nil ->
          {:ok, Map.merge(response, %{body: nil, trailing: ""})}

        {_name, length} ->
          with {:ok, body, extra} <- recv_body(socket, rest, String.to_integer(length), timeout),
               {:ok, trailing} <- drain(socket, extra, timeout) do
            {:ok, Map.merge(response, %{body: body, trailing: trailing})}
          end
      end
    end
  end

  defp recv_head(socket, acc, timeout) do
    case :binary.split(acc, "\r\n\r\n") do
      [head, rest] ->
        {:ok, head, rest}

      [_incomplete] ->
        case :gen_tcp.recv(socket, 0, timeout) do
          {:ok, data} -> recv_head(socket, acc <> data, timeout)
          {:error, reason} -> {:error, {reason, acc}}
        end
    end
  end

  defp recv_body(_socket, acc, length, _timeout) when byte_size(acc) >= length do
    <<body::binary-size(length), extra::binary>> = acc
    {:ok, body, extra}
  end

  defp recv_body(socket, acc, length, timeout) do
    case :gen_tcp.recv(socket, length - byte_size(acc), timeout) do
      {:ok, data} -> recv_body(socket, acc <> data, length, timeout)
      {:error, reason} -> {:error, {reason, acc}}
    end
  end

  defp drain(socket, acc, timeout) do
    case :gen_tcp.recv(socket, 0, timeout) do
      {:ok, data} -> drain(socket, acc <> data, timeout)
      {:error, :closed} -> {:ok, acc}
      {:error, reason} -> {:error, {reason, acc}}
    end
  end

  @doc """
  The integer value of the sample line for `series` (its name with any labels, exactly as rendered) in a
  scrape, or `nil` when the scrape has no such line.
  """
  def metric_value(scrape, series) do
    case scrape |> String.split("\n") |> Enum.find(&String.starts_with?(&1, series <> " ")) do
      nil -> nil
      line -> line |> String.split(" ") |> List.last() |> String.to_integer()
    end
  end

  defp read_until_closed(socket, acc) do
    case :gen_tcp.recv(socket, 0, 5000) do
      {:ok, data} -> read_until_closed(socket, acc <> data)
      {:error, :closed} -> acc
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
      if String.downcase(line) |> String.starts_with?("set-cookie: malachi_token=") do
        # Extract value after "malachi_token=" from the ORIGINAL line (preserving case)
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
