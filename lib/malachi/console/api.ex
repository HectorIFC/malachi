defmodule Malachi.Console.Api do
  @moduledoc """
  The console's versioned API under `/api/v1`, matched by `Malachi.Console.Router` before the single page
  application fallback, which would otherwise answer any extensionless path with `index.html`.

  Every route is authorized by `Malachi.Console.Access` from its table, and every error is an
  `application/problem+json` body from `Malachi.HTTP.Problem`: a path no route serves is a 404 and a
  method a route does not take is a 405 with `Allow`, both answered before any credential is read, so
  probing for routes costs no rate limit budget and learns nothing about sessions.

  ## `GET /api/v1/me`

  Who the console is talking to, so the shell renders by role instead of discovering a 403 after a click
  (section 6.1 of `docs/design/operator-interfaces.md`, row 2 of table 10.3). It requires a valid session
  and no role: an account with only wire permissions is answered with `role: null` and no capabilities,
  which is what lets the shell say why it shows nothing.

      {
        "username": "alice",
        "authenticated": true,
        "role": "viewer",
        "capabilities": ["read_cluster"],
        "permissions": ["produce"],
        "locale": "en_US",
        "cluster": {"name": "prod-eu", "color": "#A1B2C3", "icon": "globe"}
      }

  `authenticated` is false, `username` null and `role` `"admin"` while authentication is disabled
  (`Malachi.Console.Access.anonymous/0`). `locale` is the node's (`Malachi.I18n.locale/0`); `cluster` comes
  from `MALACHI_CLUSTER_DISPLAY_NAME`, `MALACHI_CLUSTER_COLOR` and `MALACHI_CLUSTER_ICON`
  (`Malachi.Config.cluster_identity/3`), each `null` when unset.
  """

  import Plug.Conn

  alias Malachi.Console.Access
  alias Malachi.HTTP.Problem
  alias Malachi.IPAddress

  # Every route this module serves, by the path below /api/v1, with the methods it takes.
  @routes %{["me"] => ["GET"]}

  @doc "Whether `conn` is addressed to this API, so the router hands it here."
  @spec api_request?(Plug.Conn.t()) :: boolean()
  def api_request?(%Plug.Conn{path_info: ["api", "v1" | _rest]}), do: true
  def api_request?(_conn), do: false

  @doc "Answers an `/api/v1` request."
  @spec call(Plug.Conn.t()) :: Plug.Conn.t()
  def call(%Plug.Conn{path_info: ["api", "v1" | path]} = conn) do
    case Map.fetch(@routes, path) do
      :error ->
        problem(conn, :not_found)

      {:ok, methods} ->
        if conn.method in methods,
          do: authorized(conn, path),
          else: conn |> put_resp_header("allow", Enum.join(methods, ", ")) |> problem(:method_not_allowed)
    end
  end

  defp authorized(conn, path) do
    token = Access.token(header(conn, "authorization"), header(conn, "cookie"))

    request = %{
      ip: IPAddress.format(conn.remote_ip),
      user_agent: header(conn, "user-agent") || "",
      method: conn.method,
      path: conn.request_path
    }

    case Access.admit(token, Access.required_role(conn.method, conn.request_path), request) do
      {:ok, subject} ->
        serve(conn, path, subject)

      {:error, :api_rate_limit_exceeded, retry_after_ms, _digest, _username} ->
        problem(conn, {:rate_limited, retry_after_ms})

      {:error, :rate_limit_exceeded, retry_after_ms} ->
        problem(conn, {:rate_limited, retry_after_ms})

      {:error, reason} ->
        problem(conn, reason)
    end
  end

  defp serve(conn, ["me"], subject) do
    json(conn, 200, %{
      "username" => subject.username,
      "authenticated" => subject.authenticated,
      "role" => role_string(subject.role),
      "capabilities" => Enum.map(Access.capabilities(subject.role), &Atom.to_string/1),
      "permissions" => Enum.map(subject.permissions, &Atom.to_string/1),
      "locale" => Malachi.I18n.locale(),
      "cluster" => cluster_identity()
    })
  end

  defp cluster_identity do
    identity = Application.get_env(:malachi, :cluster_identity, %{})
    %{"name" => identity[:name], "color" => identity[:color], "icon" => identity[:icon]}
  end

  defp role_string(nil), do: nil
  defp role_string(role), do: Atom.to_string(role)

  defp header(conn, name) do
    case get_req_header(conn, name) do
      [value | _rest] -> value
      [] -> nil
    end
  end

  # Responses are about one operator and change with every role change, so nothing caches them.
  defp json(conn, status, body) do
    conn
    |> put_resp_header("content-type", "application/json")
    |> put_resp_header("cache-control", "no-store")
    |> send_resp(status, Jason.encode!(body))
    |> halt()
  end

  defp problem(conn, error) do
    {status, headers, body} = Problem.for_error(error)

    headers
    |> Enum.reduce(conn, fn {name, value}, acc -> put_resp_header(acc, String.downcase(name), value) end)
    |> put_resp_header("cache-control", "no-store")
    |> send_resp(status, body)
    |> halt()
  end
end
