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

  ## `GET /api/v1/producers`

  The producer streams open on the node that answers, for the Producers view (section 6.8.1 of
  `docs/design/operator-interfaces.md`, row 14 of table 10.3), at the `viewer` role. Each connection that
  holds streams describes them (`Malachi.ProducerDirectory`); one that does not answer within the
  listing's bound is counted in `unanswered_connections`, and `total.relation` is then `"gte"`. The
  whole cluster's streams are each node's: a listing across nodes is not served yet.

  Paged as AIP-158 asks: `page_size` streams at a time (100 when absent or 0, at most 1000, a larger one
  lowered to it), oldest first, and an opaque `next_page_token` to pass as `page_token` for the next page,
  empty on the last page. A token names the position after the last stream it returned, so a stream that
  opened or ended in between shifts nothing already read; it is opaque, not secret, and carries the
  stream's position in the listing's order, which includes an internal name for its connection.
  `complete` is false once any page of the walk had a connection that did not answer, whose streams that
  sort before the position reached can no longer be returned: a walk whose last page says `complete:
  false` is restarted from the first page. A negative size, or a size or token that does not parse, is a
  400 problem.

      {
        "node": "malachi@10.0.0.1",
        "streams": [
          {
            "stream_id": 1, "topic": "orders", "range": 3, "segment": 7, "primary": "malachi@10.0.0.1",
            "label": "billing-writer", "client_ip": "10.0.0.9",
            "window": {"appends": 64, "bytes": 16777216},
            "next_sequence": 120, "acked_sequence": 118,
            "in_flight": {"appends": 2, "bytes": 4096},
            "opened_at": "2026-10-11T10:00:00.000Z"
          }
        ],
        "total": {"count": 1, "relation": "eq"},
        "unanswered_connections": 0,
        "next_page_token": "",
        "complete": true
      }

  `label` is what the client named itself when it opened the stream, printable ASCII only and at most 128
  bytes.

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
  @routes %{["me"] => ["GET"], ["producers"] => ["GET"]}

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

  defp serve(conn, ["producers"], _subject) do
    conn = fetch_query_params(conn)

    with {:ok, size} <- page_size(conn.query_params["page_size"]),
         {:ok, after_key, missed?} <- page_token(conn.query_params["page_token"]) do
      %{streams: streams, unanswered: unanswered} = Malachi.ProducerDirectory.list()
      {page, more} = streams |> after_position(after_key) |> Enum.split(size)
      # the walk missed streams once any of its pages had a connection that did not answer
      missed? = missed? or unanswered > 0

      json(conn, 200, %{
        "node" => Atom.to_string(node()),
        "streams" => Enum.map(page, &producer_stream/1),
        "total" => %{"count" => length(streams), "relation" => if(unanswered == 0, do: "eq", else: "gte")},
        "unanswered_connections" => unanswered,
        "next_page_token" => if(more == [], do: "", else: position_token(List.last(page), missed?)),
        "complete" => not missed?
      })
    else
      :error -> problem(conn, :invalid_request)
    end
  end

  @default_page_size 100
  @max_page_size 1_000

  defp page_size(nil), do: {:ok, @default_page_size}

  defp page_size(raw) when is_binary(raw) do
    case Integer.parse(raw) do
      {0, ""} -> {:ok, @default_page_size}
      {size, ""} when size > 0 -> {:ok, min(size, @max_page_size)}
      _negative_or_invalid -> :error
    end
  end

  # a repeated or bracketed parameter (`page_size[]=1`) arrives as a list or a map
  defp page_size(_not_text), do: :error

  defp after_position(streams, nil), do: streams

  defp after_position(streams, key),
    do: Enum.drop_while(streams, &(Malachi.ProducerDirectory.order_key(&1) <= key))

  # A token is the order key of the last stream a page returned and whether the walk missed streams so
  # far, as text: opaque to a client, parsed here without evaluating anything it carries.
  defp page_token(nil), do: {:ok, nil, false}
  defp page_token(""), do: {:ok, nil, false}

  defp page_token(raw) when is_binary(raw) do
    with {:ok, text} <- Base.url_decode64(raw, padding: false),
         [opened_at, stream_id, missed, connection] <- String.split(text, ":", parts: 4),
         {opened_at, ""} <- Integer.parse(opened_at),
         {stream_id, ""} <- Integer.parse(stream_id),
         true <- missed in ["0", "1"] do
      {:ok, {opened_at, connection, stream_id}, missed == "1"}
    else
      _invalid -> :error
    end
  end

  defp page_token(_not_text), do: :error

  defp position_token(stream, missed?) do
    {opened_at, connection, stream_id} = Malachi.ProducerDirectory.order_key(stream)
    Base.url_encode64("#{opened_at}:#{stream_id}:#{if(missed?, do: 1, else: 0)}:#{connection}", padding: false)
  end

  defp producer_stream(stream) do
    %{
      "stream_id" => stream.stream_id,
      "topic" => stream.topic,
      "range" => stream.range,
      "segment" => stream.segment,
      "primary" => stream.primary,
      "label" => stream.label,
      "client_ip" => stream.client_ip,
      "window" => %{"appends" => stream.window.appends, "bytes" => stream.window.bytes},
      "next_sequence" => stream.next_sequence,
      "acked_sequence" => stream.acked_sequence,
      "in_flight" => %{"appends" => stream.in_flight.appends, "bytes" => stream.in_flight.bytes},
      "opened_at" => stream.opened_at |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601()
    }
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
