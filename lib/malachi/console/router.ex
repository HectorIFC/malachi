defmodule Malachi.Console.Router do
  @moduledoc """
  The console endpoint's request pipeline, in the order a request meets it:

    1. **Header budget.** Bandit already bounds the header count and each header's length; the bytes of
       names plus values across all headers are bounded here, from the same setting the dashboard
       enforces (`Malachi.HTTP.Limits`), and past it the answer is the dashboard's 431 problem.
    2. **Security headers.** The dashboard's set, from `Malachi.Dashboard.SecurityHeaders.headers/3`,
       including its configurable CSP. CORS is left out: it belongs to the dashboard's API routes.
    3. **API.** A path under `/api/v1` goes to `Malachi.Console.Api`, which authorizes it through
       `Malachi.Console.Access` and answers every error as `application/problem+json`. It is matched
       before the static steps, because the single page application fallback answers any extensionless
       path with `index.html`.
    4. **Method.** `GET` and `HEAD` only; anything else is 405 with `Allow`.
    5. **Bundle.** With no bundle built into the release, 503 saying so, never cached.
    6. **Static.** `Malachi.Console.Static` answers everything else from the manifest.

  Nothing in this module touches the filesystem.
  """

  import Plug.Conn

  alias Malachi.Console.Api
  alias Malachi.Console.HeaderDeadline
  alias Malachi.Console.Static
  alias Malachi.Dashboard.SecurityHeaders
  alias Malachi.HTTP.Problem

  @behaviour Plug

  @allowed_methods ["GET", "HEAD"]

  @typedoc "What the endpoint hands the router at start: the manifest and the header byte budget."
  @type opts :: %{manifest: Malachi.Console.Assets.t(), max_header_bytes: pos_integer()}

  @impl true
  def init(%{manifest: _, max_header_bytes: _} = opts), do: opts

  @impl true
  def call(conn, opts) do
    # Every header has been read by the time a request gets here, so the connection's header
    # deadline stops, and it starts again once the response is out (Malachi.Console.HeaderDeadline).
    HeaderDeadline.disarm()

    try do
      route(conn, opts)
    after
      HeaderDeadline.arm()
    end
  end

  defp route(conn, %{manifest: manifest, max_header_bytes: max_header_bytes}) do
    cond do
      header_bytes(conn) > max_header_bytes ->
        {status, headers, body} = Problem.for_error(:header_fields_too_large)

        conn
        |> security_headers()
        |> merge_resp_headers(Enum.map(headers, fn {name, value} -> {String.downcase(name), value} end))
        |> send_resp(status, body)
        |> halt()

      Api.api_request?(conn) ->
        conn |> security_headers() |> Api.call()

      conn.method not in @allowed_methods ->
        conn
        |> security_headers()
        |> put_resp_header("allow", Enum.join(@allowed_methods, ", "))
        |> Static.plain(:method_not_allowed)

      manifest == :absent ->
        conn |> security_headers() |> Static.plain(:not_built)

      true ->
        conn |> security_headers() |> Static.call(Static.init(manifest))
    end
  end

  # The same count the dashboard keeps: bytes of every name and value, a repeated header counted each
  # time. Bandit lowercases names; the dashboard counts them lowercased too.
  defp header_bytes(conn) do
    Enum.reduce(conn.req_headers, 0, fn {name, value}, acc -> acc + byte_size(name) + byte_size(value) end)
  end

  defp security_headers(conn) do
    merge_resp_headers(conn, SecurityHeaders.headers(conn.request_path, nil, cors: false))
  end
end
