defmodule Malachi.Test.AccessHelper do
  @moduledoc """
  Accounts and requests for the tests of `Malachi.Console.Access` across both HTTP endpoints: an account
  with given wire permissions and console role, its session token, and one request to either endpoint
  answered as `%{status, headers, json}`, with the body decoded when it is JSON.
  """

  import ExUnit.Callbacks, only: [on_exit: 1]

  alias Malachi.Auth
  alias Malachi.Console.Endpoint
  alias Malachi.Test.DashboardHelper
  alias Malachi.Test.HttpClient

  @password "access-helper-pass"

  @doc """
  Creates `username` with wire `permissions` and console `role`, removes it when the test exits, and
  returns a session token for it bound to loopback.
  """
  @spec account!(String.t(), [atom()], atom() | nil) :: String.t()
  def account!(username, permissions, role) do
    _ = Auth.remove_user(username)
    # Without a role the three argument form, so a test about wire permissions alone also runs against a
    # tree from before the console roles (how the red run of #228 was taken).
    :ok =
      if role,
        do: Auth.add_user(username, @password, permissions, role),
        else: Auth.add_user(username, @password, permissions)

    on_exit(fn -> Auth.remove_user(username) end)
    {:ok, token} = Auth.authenticate(username, @password, {127, 0, 0, 1})
    token
  end

  @doc """
  Sends `method` `path` to `endpoint` (`:dashboard` or `:console`) with `token` as a Bearer credential
  (none when `nil`) and an optional JSON `body`. Extra `headers` are a list of pairs.
  """
  @spec call(:dashboard | :console, String.t(), String.t(), String.t() | nil, keyword()) :: map()
  def call(endpoint, method, path, token, opts \\ []) do
    body = opts |> Keyword.get(:body) |> encode()
    headers = auth_headers(token) ++ body_headers(body) ++ Keyword.get(opts, :headers, [])
    response = send_to(endpoint, method, path, headers, body)
    Map.put(response, :json, decode(response))
  end

  defp send_to(:dashboard, method, path, headers, body) do
    {:ok, socket} = DashboardHelper.connect()
    :ok = :gen_tcp.send(socket, [HttpClient.encode(method, path, headers), body || ""])
    {:ok, response} = DashboardHelper.recv_framed(socket)
    :gen_tcp.close(socket)
    response
  end

  defp send_to(:console, method, path, headers, body) do
    {:ok, socket} = HttpClient.connect(Endpoint.port())
    :ok = HttpClient.send_raw(socket, [HttpClient.encode(method, path, headers), body || ""])
    {:ok, response, _rest} = HttpClient.recv(socket)
    :gen_tcp.close(socket)
    response
  end

  defp auth_headers(nil), do: []
  defp auth_headers(token), do: [{"Authorization", "Bearer #{token}"}]

  defp encode(nil), do: nil
  defp encode(body), do: Jason.encode!(body)

  defp body_headers(nil), do: []

  defp body_headers(body),
    do: [{"Content-Type", "application/json"}, {"Content-Length", Integer.to_string(byte_size(body))}]

  defp decode(%{body: body} = response) when is_binary(body) and body != "" do
    case header(response, "content-type") do
      "application/" <> _json -> Jason.decode!(body)
      _other -> nil
    end
  end

  defp decode(_response), do: nil

  @doc "The value of the first response header named `name` (lowercase), or nil."
  @spec header(map(), String.t()) :: String.t() | nil
  def header(%{headers: headers}, name) do
    Enum.find_value(headers, fn {key, value} -> if String.downcase(key) == name, do: value end)
  end
end
