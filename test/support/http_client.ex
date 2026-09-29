defmodule Malachi.Test.HttpClient do
  @moduledoc """
  A raw HTTP/1.1 client for tests that need to see the wire: keep alive, pipelining, framing, and
  requests no well behaved client would send.

  Unlike `Malachi.Test.DashboardHelper`, which reads one `recv` and relies on the dashboard closing the
  socket, this reads a response by its framing (the header block, then exactly `Content-Length` bytes)
  and leaves the socket open with any bytes past that response carried over. That is what makes two
  responses on one socket, or two pipelined requests in one write, observable.
  """

  @type response :: %{status: non_neg_integer(), headers: [{String.t(), String.t()}], body: binary()}

  @doc "Connects to `port` on loopback."
  @spec connect(:inet.port_number()) :: {:ok, :gen_tcp.socket()} | {:error, term()}
  # nodelay, so a test that sends one byte at a time puts each byte on the wire when it says so.
  def connect(port), do: :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, nodelay: true], 1_000)

  @doc """
  The bytes of a request: `method path HTTP/1.1`, a `Host` header unless one is given, the given
  headers in order, and a blank line.
  """
  @spec encode(String.t(), String.t(), [{String.t(), String.t()}]) :: iodata()
  def encode(method, path, headers \\ []) do
    headers =
      if List.keymember?(headers, "host", 0) or List.keymember?(headers, "Host", 0),
        do: headers,
        else: [{"Host", "localhost"} | headers]

    [method, " ", path, " HTTP/1.1\r\n", Enum.map(headers, fn {k, v} -> [k, ": ", v, "\r\n"] end), "\r\n"]
  end

  @doc "Writes raw bytes."
  @spec send_raw(:gen_tcp.socket(), iodata()) :: :ok | {:error, term()}
  def send_raw(socket, bytes), do: :gen_tcp.send(socket, bytes)

  @doc """
  Reads one response. `buffer` holds bytes already read past the previous response; the result carries
  the bytes read past this one. A `HEAD` request's response has no body whatever its `Content-Length`,
  so the caller says which method it sent.
  """
  @spec recv(:gen_tcp.socket(), binary(), keyword()) :: {:ok, response(), binary()} | {:error, term()}
  def recv(socket, buffer \\ "", opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 2_000)
    method = Keyword.get(opts, :method, "GET")

    with {:ok, head, rest} <- read_until(socket, buffer, "\r\n\r\n", timeout),
         {status, headers} <- parse_head(head),
         length = body_length(status, headers, method),
         {:ok, body, rest} <- read_exactly(socket, rest, length, timeout) do
      {:ok, %{status: status, headers: headers, body: body}, rest}
    end
  end

  @doc "Sends one request and reads its response, on a socket with nothing buffered."
  @spec request(:gen_tcp.socket(), String.t(), String.t(), [{String.t(), String.t()}]) ::
          {:ok, response()} | {:error, term()}
  def request(socket, method, path, headers \\ []) do
    :ok = send_raw(socket, encode(method, path, headers))

    case recv(socket, "", method: method) do
      {:ok, response, ""} -> {:ok, response}
      {:ok, _response, extra} -> {:error, {:unexpected_bytes, extra}}
      error -> error
    end
  end

  @doc "Opens a connection, sends one request, reads its response and closes."
  @spec get(:inet.port_number(), String.t(), [{String.t(), String.t()}]) :: response()
  def get(port, path, headers \\ []) do
    {:ok, socket} = connect(port)
    {:ok, response} = request(socket, "GET", path, headers)
    :gen_tcp.close(socket)
    response
  end

  @doc "The value of the first response header named `name` (lowercase), or nil."
  @spec header(response(), String.t()) :: String.t() | nil
  def header(%{headers: headers}, name) do
    case List.keyfind(headers, name, 0) do
      {_, value} -> value
      nil -> nil
    end
  end

  @doc "Whether the peer closed the socket within `timeout` ms, with no further bytes."
  @spec closed?(:gen_tcp.socket(), non_neg_integer()) :: boolean()
  def closed?(socket, timeout \\ 1_000) do
    case :gen_tcp.recv(socket, 0, timeout) do
      {:error, :closed} -> true
      _ -> false
    end
  end

  defp read_until(socket, buffer, delimiter, timeout) do
    case :binary.split(buffer, delimiter) do
      [head, rest] ->
        {:ok, head, rest}

      [_incomplete] ->
        case :gen_tcp.recv(socket, 0, timeout) do
          {:ok, more} -> read_until(socket, buffer <> more, delimiter, timeout)
          {:error, reason} -> {:error, {reason, buffer}}
        end
    end
  end

  defp read_exactly(_socket, buffer, length, _timeout) when byte_size(buffer) >= length do
    <<body::binary-size(length), rest::binary>> = buffer
    {:ok, body, rest}
  end

  defp read_exactly(socket, buffer, length, timeout) do
    case :gen_tcp.recv(socket, 0, timeout) do
      {:ok, more} -> read_exactly(socket, buffer <> more, length, timeout)
      {:error, reason} -> {:error, {reason, buffer}}
    end
  end

  defp parse_head(head) do
    [status_line | lines] = String.split(head, "\r\n")
    ["HTTP/1." <> _minor, code | _reason] = String.split(status_line, " ", parts: 3)

    headers =
      for line <- lines do
        [name, value] = String.split(line, ":", parts: 2)
        {String.downcase(name), String.trim(value)}
      end

    {String.to_integer(code), headers}
  end

  defp body_length(status, _headers, _method) when status in [204, 304], do: 0
  defp body_length(_status, _headers, "HEAD"), do: 0

  defp body_length(_status, headers, _method) do
    case List.keyfind(headers, "content-length", 0) do
      {_, value} -> String.to_integer(value)
      nil -> 0
    end
  end
end
