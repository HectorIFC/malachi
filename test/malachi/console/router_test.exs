defmodule Malachi.Console.RouterTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  import Plug.Conn
  import Plug.Test

  alias Malachi.Console.Assets
  alias Malachi.Console.Router
  alias Malachi.Console.Static
  alias Malachi.Test.ConsoleFixture
  alias Malachi.Test.TmpDir

  @script "/assets/app-3f2aB9x1.js"

  setup do
    dir = ConsoleFixture.bundle!()
    manifest = Assets.build(dir)
    %{dir: dir, manifest: manifest, opts: Router.init(%{manifest: manifest, max_header_bytes: 32_768})}
  end

  defp call(opts, path, headers \\ []), do: request(opts, "GET", path, headers)

  defp request(opts, method, path, headers \\ []) do
    conn = Enum.reduce(headers, conn(method, path), fn {k, v}, conn -> put_req_header(conn, k, v) end)
    Router.call(conn, opts)
  end

  defp etag(manifest, path, coding \\ "identity"), do: manifest.files[path].variants[coding].etag

  describe "files" do
    test "a hashed asset is served with its type, ETag and a year of immutable caching", ctx do
      conn = call(ctx.opts, @script)

      assert conn.status == 200
      assert conn.resp_body == ConsoleFixture.script_body()
      assert get_resp_header(conn, "content-type") == ["text/javascript"]
      assert get_resp_header(conn, "etag") == [etag(ctx.manifest, "assets/app-3f2aB9x1.js")]
      assert get_resp_header(conn, "cache-control") == ["public, max-age=31536000, immutable"]
      assert get_resp_header(conn, "vary") == ["Accept-Encoding"]
      assert get_resp_header(conn, "content-encoding") == []
    end

    test "index.html revalidates on every use", ctx do
      conn = call(ctx.opts, "/index.html")

      assert conn.status == 200
      assert get_resp_header(conn, "cache-control") == ["no-cache"]
      assert get_resp_header(conn, "vary") == []
    end

    test "HEAD gets the same headers", ctx do
      conn = request(ctx.opts, "HEAD", @script)

      assert conn.status == 200
      assert get_resp_header(conn, "etag") == [etag(ctx.manifest, "assets/app-3f2aB9x1.js")]
    end

    test "every answer carries the dashboard's security headers and no CORS", ctx do
      for path <- [@script, "/", "/metrics", "/missing.png"] do
        conn = call(ctx.opts, path)
        assert get_resp_header(conn, "x-frame-options") == ["DENY"], path
        assert get_resp_header(conn, "x-content-type-options") == ["nosniff"], path
        assert [_csp] = get_resp_header(conn, "content-security-policy")
        assert get_resp_header(conn, "access-control-allow-origin") == [], path
      end
    end
  end

  describe "conditional requests" do
    test "a matching If-None-Match gets 304 with the validators and no body", ctx do
      tag = etag(ctx.manifest, "assets/app-3f2aB9x1.js")
      conn = call(ctx.opts, @script, [{"if-none-match", tag}])

      assert conn.status == 304
      assert conn.resp_body == ""
      assert get_resp_header(conn, "etag") == [tag]
      assert get_resp_header(conn, "cache-control") == ["public, max-age=31536000, immutable"]
      assert get_resp_header(conn, "vary") == ["Accept-Encoding"]
    end

    test "a list, a weak validator and * all match", ctx do
      tag = etag(ctx.manifest, "assets/app-3f2aB9x1.js")

      for value <- [~s("other", #{tag}), "W/" <> tag, "*"] do
        assert call(ctx.opts, @script, [{"if-none-match", value}]).status == 304, value
      end
    end

    test "a stale validator gets the file", ctx do
      assert call(ctx.opts, @script, [{"if-none-match", ~s("stale")}]).status == 200
    end

    test "the index revalidates to 304 through the fallback too", ctx do
      tag = ctx.manifest.index.variants["identity"].etag
      assert call(ctx.opts, "/topics/orders", [{"if-none-match", tag}]).status == 304
    end

    test "a validator for the gzip variant does not match the brotli one", ctx do
      gzip_tag = etag(ctx.manifest, "assets/app-3f2aB9x1.js", "gzip")
      conn = call(ctx.opts, @script, [{"accept-encoding", "br"}, {"if-none-match", gzip_tag}])

      assert conn.status == 200
      assert conn.resp_body == "BROTLI"
    end
  end

  describe "content coding" do
    test "brotli is preferred when both are accepted equally", ctx do
      conn = call(ctx.opts, @script, [{"accept-encoding", "gzip, deflate, br"}])

      assert conn.resp_body == "BROTLI"
      assert get_resp_header(conn, "content-encoding") == ["br"]
      assert get_resp_header(conn, "etag") == [etag(ctx.manifest, "assets/app-3f2aB9x1.js", "br")]
    end

    test "a higher q wins over the preference order", ctx do
      conn = call(ctx.opts, @script, [{"accept-encoding", "br;q=0.5, gzip;q=0.9"}])
      assert conn.resp_body == "GZIP"
      assert get_resp_header(conn, "content-encoding") == ["gzip"]
    end

    test "q=0 refuses a coding", ctx do
      assert call(ctx.opts, @script, [{"accept-encoding", "br;q=0, gzip"}]).resp_body == "GZIP"

      assert call(ctx.opts, @script, [{"accept-encoding", "br;q=0, gzip;q=0"}]).resp_body ==
               ConsoleFixture.script_body()
    end

    test "* accepts what is not named and *;q=0 refuses it", ctx do
      assert call(ctx.opts, @script, [{"accept-encoding", "*"}]).resp_body == "BROTLI"
      assert call(ctx.opts, @script, [{"accept-encoding", "gzip, *;q=0"}]).resp_body == "GZIP"
    end

    test "a coding with no variant falls back to what exists", ctx do
      conn = call(ctx.opts, "/assets/app-3f2aB9x1.css", [{"accept-encoding", "br, gzip"}])
      assert conn.resp_body == "CSSGZIP"
    end

    test "no Accept-Encoding, or an unparsable q, gets identity", ctx do
      assert call(ctx.opts, @script).resp_body == ConsoleFixture.script_body()
      assert call(ctx.opts, @script, [{"accept-encoding", "br;q=high"}]).resp_body == ConsoleFixture.script_body()
      assert call(ctx.opts, @script, [{"accept-encoding", "br;q=2"}]).resp_body == ConsoleFixture.script_body()
    end

    test "a parameter other than q leaves the coding fully accepted", ctx do
      assert call(ctx.opts, @script, [{"accept-encoding", "br;level=5"}]).resp_body == "BROTLI"
    end

    test "coding names are case-insensitive", ctx do
      assert call(ctx.opts, @script, [{"accept-encoding", "BR"}]).resp_body == "BROTLI"
    end
  end

  describe "single page fallback" do
    test "an extensionless route outside assets/ is the index with 200", ctx do
      for path <- ["/", "/topics", "/topics/orders/ranges", "/topics/orders.v1"] do
        conn = call(ctx.opts, path)
        assert conn.status == 200, path
        assert conn.resp_body == ConsoleFixture.index_body(), path
        assert get_resp_header(conn, "cache-control") == ["no-cache"], path
        assert get_resp_header(conn, "content-type") == ["text/html"], path
      end
    end

    test "a missing file with an extension is 404, not the index", ctx do
      for path <- ["/missing.png", "/topics/x.json", "/assets/missing.js", "/assets/chunk", "/assets/%zz"] do
        conn = call(ctx.opts, path)
        assert conn.status == 404, path
        assert get_resp_header(conn, "cache-control") == ["no-store"], path
      end
    end
  end

  describe "files changed after startup" do
    test "a file rewritten since startup is refused, not served under the old ETag", ctx do
      File.write!(Path.join(ctx.dir, "favicon.ico"), "A DIFFERENT ICON")

      conn = call(ctx.opts, "/favicon.ico")
      assert conn.status == 404
      refute conn.resp_body =~ "DIFFERENT"
    end

    test "a file replaced by a link to one outside the root is refused", ctx do
      outside = Path.join(TmpDir.path("console_planted"), "secret")
      File.mkdir_p!(Path.dirname(outside))
      File.write!(outside, "PLANTED")
      on_exit(fn -> File.rm_rf!(Path.dirname(outside)) end)

      target = Path.join(ctx.dir, "favicon.ico")
      File.rm!(target)
      File.ln_s!(outside, target)

      conn = call(ctx.opts, "/favicon.ico")
      assert conn.status == 404
      refute conn.resp_body =~ "PLANTED"
    end

    test "the assets directory swapped for a link to a copy elsewhere is refused", ctx do
      elsewhere = TmpDir.path("console_swapped")
      File.cp_r!(Path.join(ctx.dir, "assets"), elsewhere)
      on_exit(fn -> File.rm_rf!(elsewhere) end)

      File.rm_rf!(Path.join(ctx.dir, "assets"))
      File.ln_s!(elsewhere, Path.join(ctx.dir, "assets"))

      assert call(ctx.opts, @script).status == 404
    end

    test "a file moved out of the root and linked back is refused, though it keeps its inode", ctx do
      # rename(2) keeps the file's inode and modification time, so a stat through the link would still
      # match; the lstat the check uses sees the link, with an identity of its own.
      outside = TmpDir.path("console_moved")
      File.mkdir_p!(outside)
      on_exit(fn -> File.rm_rf!(outside) end)

      File.rename!(Path.join(ctx.dir, "favicon.ico"), Path.join(outside, "favicon.ico"))
      File.ln_s!(Path.join(outside, "favicon.ico"), Path.join(ctx.dir, "favicon.ico"))

      assert call(ctx.opts, "/favicon.ico").status == 404
    end

    test "the assets directory moved out and linked back is refused", ctx do
      # The file's own lstat reaches the original through the linked directory, identity intact; only
      # the check on the directory levels above it catches this.
      outside = TmpDir.path("console_moved_dir")
      on_exit(fn -> File.rm_rf!(outside) end)

      File.rename!(Path.join(ctx.dir, "assets"), outside)
      File.ln_s!(outside, Path.join(ctx.dir, "assets"))

      assert call(ctx.opts, @script).status == 404
    end

    test "a static root reached through a link is served as usual", ctx do
      link = TmpDir.path("console_root_link")
      File.ln_s!(ctx.dir, link)
      on_exit(fn -> File.rm(link) end)
      opts = Router.init(%{manifest: Assets.build(link), max_header_bytes: 32_768})

      assert call(opts, @script).status == 200
      assert call(opts, "/").status == 200
    end

    test "a file removed since startup is a 404, not a crash", ctx do
      File.rm!(Path.join(ctx.dir, "favicon.ico"))
      assert call(ctx.opts, "/favicon.ico").status == 404
    end

    test "a changed file never answers 304 on its old ETag", ctx do
      tag = etag(ctx.manifest, "favicon.ico")
      File.write!(Path.join(ctx.dir, "favicon.ico"), "A DIFFERENT ICON")

      assert call(ctx.opts, "/favicon.ico", [{"if-none-match", tag}]).status == 404
    end

    test "the index fallback is checked the same way", ctx do
      File.write!(Path.join(ctx.dir, "index.html"), "<!doctype html><title>other</title>")
      assert call(ctx.opts, "/topics").status == 404
    end
  end

  describe "paths that are not files" do
    test "traversal and malformed escapes are refused with 400", ctx do
      for path <- [
            "/assets/%2e%2e/index.html",
            "/%2e%2e/%2e%2e/etc/passwd",
            "/assets%2Fapp-3f2aB9x1.js",
            "/assets/a%5Cb",
            "/a%00b",
            "/./index.html"
          ] do
        assert call(ctx.opts, path).status == 400, path
      end
    end

    test "hidden files, links and precompressed siblings are never served by name", ctx do
      for path <- [
            "/.gitignore",
            "/.hidden/secret.js",
            "/linked.txt",
            "/assets/linked/secret.txt",
            "/assets/app-3f2aB9x1.js.br",
            "/assets/app-3f2aB9x1.js.gz"
          ] do
        conn = call(ctx.opts, path)
        assert conn.status in [400, 404], path
        refute conn.resp_body in ["*\n", "hidden", "outside", "BROTLI", "GZIP"], path
      end
    end
  end

  describe "the pipeline" do
    test "a method other than GET or HEAD is 405 with Allow", ctx do
      for method <- ["POST", "PUT", "DELETE", "PATCH", "OPTIONS"] do
        conn = request(ctx.opts, method, "/")
        assert conn.status == 405, method
        assert get_resp_header(conn, "allow") == ["GET, HEAD"]
      end
    end

    test "with no bundle every request is 503 and never cached" do
      opts =
        capture_log(fn -> send(self(), Assets.build(ConsoleFixture.headless_bundle!())) end)
        |> then(fn _ -> receive do: (manifest -> manifest) end)
        |> then(&Router.init(%{manifest: &1, max_header_bytes: 32_768}))

      for path <- ["/", "/assets/app.js", "/index.html"] do
        conn = call(opts, path)
        assert conn.status == 503, path
        assert conn.resp_body =~ "not built"
        assert get_resp_header(conn, "cache-control") == ["no-store"]
      end
    end

    test "headers past the byte budget are 431 with the dashboard's problem body", ctx do
      opts = %{ctx.opts | max_header_bytes: 100}

      assert call(opts, "/", [{"x-a", String.duplicate("a", 90)}]).status == 200

      conn = call(opts, "/", [{"x-a", String.duplicate("a", 97)}, {"x-b", "b"}])
      assert conn.status == 431
      assert Jason.decode!(conn.resp_body) == %{"type" => "errors.http.header_fields_too_large", "status" => 431}
      assert get_resp_header(conn, "content-type") == ["application/problem+json"]
      assert get_resp_header(conn, "x-frame-options") == ["DENY"]
    end
  end

  describe "Static.fresh?/2" do
    test "compares weakly and ignores unrelated tags" do
      assert Static.fresh?([~s(W/"a")], ~s("a"))
      assert Static.fresh?([~s("a")], ~s(W/"a"))
      assert Static.fresh?([~s("x"), ~s("y", "a")], ~s("a"))
      refute Static.fresh?([], ~s("a"))
      refute Static.fresh?([~s("b")], ~s("a"))
    end
  end
end
