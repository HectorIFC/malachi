defmodule Malachi.Console.Static do
  @moduledoc """
  Serves the console bundle from its manifest (`Malachi.Console.Assets`), and nothing else.

  A request path is percent-decoded segment by segment and looked up in the manifest as an exact key.
  It is never joined onto a directory, so there is no path to traverse: a segment that decodes to `.`,
  `..` or empty, or to anything holding `/`, `\\` or a NUL byte, is refused with 400 before the lookup,
  because no manifest key can contain one and a client sending one is not asking for a file.

  What a hit gets:

    * the representation chosen from `Accept-Encoding` by q-value: `br` or `gzip` when the file shipped
      that precompressed variant and the client gives it a q above zero (explicitly, or through `*`),
      the highest q winning and brotli breaking a tie; otherwise the uncompressed bytes. Compression
      never happens at request time;
    * `ETag` (the SHA-256 of that representation), `Cache-Control` (immutable for a name under `assets/`
      whose last segment looks like a build hash, `no-cache` otherwise) and `Vary: Accept-Encoding` when the file has a variant;
    * 304 with those same headers and no body when `If-None-Match` matches, compared weakly as RFC 9110
      section 13.1.2 requires: a list, `*`, and `W/` validators all count.

  A miss outside `assets/` whose last segment names no known file type (no extension, or one the MIME
  table does not know, as in a topic named `orders.v1`) and in which no segment starts with a dot is a
  route of the single page application, answered with `index.html` under the same rules. Any other
  miss is 404: a missing script must not come back as HTML with a 200, which a browser would try to
  execute and report as an opaque syntax error.

  Range requests are not supported and no `Accept-Ranges` is sent, which RFC 9110 permits.
  """

  import Plug.Conn

  alias Malachi.Console.Assets

  @behaviour Plug

  @impl true
  def init(manifest), do: manifest

  @impl true
  def call(conn, manifest) do
    case decoded_segments(conn.path_info) do
      {:ok, segments} -> serve(conn, manifest, segments)
      :error -> plain(conn, :bad_request)
    end
  end

  defp serve(conn, %{files: files, index: index}, segments) do
    case Map.fetch(files, Enum.join(segments, "/")) do
      {:ok, entry} ->
        respond(conn, entry)

      :error ->
        if spa_route?(segments),
          do: respond(conn, index),
          else: plain(conn, :not_found)
    end
  end

  defp decoded_segments(path_info) do
    segments = Enum.map(path_info, &URI.decode/1)
    # URI.decode/1 leaves a malformed escape such as "%zz" as it is; no manifest key holds one, so
    # such a path is simply not found.
    if Enum.any?(segments, &invalid_segment?/1), do: :error, else: {:ok, segments}
  end

  defp invalid_segment?(segment) when segment in ["", ".", ".."], do: true
  defp invalid_segment?(segment), do: String.contains?(segment, ["/", "\\", <<0>>])

  # "/" (no segments) is the application's root, and so is any path outside assets/ whose last segment
  # does not name a known file type. Known means the MIME table has the extension: a route such as
  # /topics/orders.v1 carries a topic name with a dot in it and is still a route, while /logo.png is a
  # missing file. A segment starting with a dot is never a route: it names a hidden file.
  defp spa_route?([]), do: true
  defp spa_route?(["assets" | _]), do: false

  defp spa_route?(segments) do
    not Enum.any?(segments, &String.starts_with?(&1, ".")) and
      MIME.from_path(List.last(segments)) == "application/octet-stream"
  end

  defp respond(conn, entry) do
    {coding, variant} = choose(entry.variants, get_req_header(conn, "accept-encoding"))

    conn =
      conn
      |> put_resp_header("etag", variant.etag)
      |> put_resp_header("cache-control", entry.cache_control)
      |> put_vary(entry.variants)

    if fresh?(get_req_header(conn, "if-none-match"), variant.etag) do
      conn |> send_resp(304, "") |> halt()
    else
      conn
      |> put_resp_header("content-type", entry.content_type)
      |> put_content_encoding(coding)
      |> send_file(200, variant.path)
      |> halt()
    end
  end

  defp put_vary(conn, variants) when map_size(variants) > 1,
    do: put_resp_header(conn, "vary", "Accept-Encoding")

  defp put_vary(conn, _identity_only), do: conn

  defp put_content_encoding(conn, "identity"), do: conn
  defp put_content_encoding(conn, coding), do: put_resp_header(conn, "content-encoding", coding)

  @doc """
  Picks the representation to send for the `Accept-Encoding` header values given: the precompressed
  variant with the highest q above zero, brotli winning a tie, else `identity`. Returns the coding
  name and its variant.
  """
  @spec choose(%{required(String.t()) => Assets.variant()}, [String.t()]) :: {String.t(), Assets.variant()}
  def choose(variants, accept_encoding) do
    weights = weights(accept_encoding)

    Assets.codings()
    |> Enum.filter(&Map.has_key?(variants, &1))
    |> Enum.map(&{&1, weight(weights, &1)})
    |> Enum.filter(fn {_coding, q} -> q > 0 end)
    # Stable sort, so the preference order from Assets.codings/0 breaks a tie.
    |> Enum.sort_by(fn {_coding, q} -> q end, :desc)
    |> case do
      [{coding, _q} | _] -> {coding, Map.fetch!(variants, coding)}
      [] -> {"identity", Map.fetch!(variants, "identity")}
    end
  end

  # %{"br" => 1.0, "*" => 0.0, ...} from every Accept-Encoding value; names are case-insensitive.
  defp weights(values) do
    for value <- values,
        member <- String.split(value, ","),
        [name | params] = member |> String.split(";") |> Enum.map(&String.trim/1),
        name != "",
        into: %{} do
      {String.downcase(name), q(params)}
    end
  end

  defp q(params) do
    Enum.find_value(params, 1.0, fn param ->
      case String.split(param, "=", parts: 2) do
        [key, value] when key in ["q", "Q"] -> parse_q(String.trim(value))
        _ -> nil
      end
    end)
  end

  # A q-value the grammar does not allow counts as 0: a client that sent garbage has not accepted
  # the coding.
  defp parse_q(value) do
    case Float.parse(value) do
      {q, ""} when q >= 0 and q <= 1 -> q
      _ -> 0.0
    end
  end

  defp weight(weights, coding), do: Map.get(weights, coding, Map.get(weights, "*", 0.0))

  @doc """
  Whether `If-None-Match` header values match `etag`, by the weak comparison RFC 9110 prescribes for
  this header: `*` matches anything, and `W/` is ignored on both sides.
  """
  @spec fresh?([String.t()], String.t()) :: boolean()
  def fresh?(if_none_match, etag) do
    opaque = strip_weak(etag)

    Enum.any?(if_none_match, fn value ->
      value
      |> String.split(",")
      |> Enum.map(&String.trim/1)
      |> Enum.any?(&(&1 == "*" or strip_weak(&1) == opaque))
    end)
  end

  defp strip_weak("W/" <> tag), do: tag
  defp strip_weak(tag), do: tag

  @doc """
  Sends one of the endpoint's constant plain text answers, which no cache may keep, and halts. Every
  answer that is not a file goes through here, so they share one content type and one cache rule, and
  each body is a literal in its own clause: nothing from the request can reach a response body.
  """
  @spec plain(Plug.Conn.t(), :bad_request | :not_found | :method_not_allowed | :not_built) :: Plug.Conn.t()
  def plain(conn, :bad_request), do: conn |> plain_headers() |> send_resp(400, "Bad request\n") |> halt()
  def plain(conn, :not_found), do: conn |> plain_headers() |> send_resp(404, "Not found\n") |> halt()

  def plain(conn, :method_not_allowed),
    do: conn |> plain_headers() |> send_resp(405, "Method not allowed\n") |> halt()

  def plain(conn, :not_built),
    do: conn |> plain_headers() |> send_resp(503, "The console was not built into this release.\n") |> halt()

  defp plain_headers(conn) do
    conn
    |> put_resp_header("content-type", "text/plain; charset=utf-8")
    |> put_resp_header("cache-control", "no-store")
  end
end
