defmodule Malachi.SiteTest do
  # Stages into a tmp_dir shaped like the ExDoc output plus a `site/` source tree, so the tests never
  # depend on `mix docs` having run and never touch the real `doc/`.
  use ExUnit.Case, async: true

  alias Malachi.Site

  @moduletag :tmp_dir

  @config %{
    version: "1.2.3",
    licenses: ["MIT"],
    site_url: "https://example.test/malachi",
    main: "introduction"
  }

  @redirect ~s(<meta http-equiv="refresh" content="0; url=introduction.html">)

  setup %{tmp_dir: root} do
    write(root, "site/index.html", """
    <!doctype html>
    <html><head><meta name="malachi:landing" content="1"><title>Malachi {{version}}</title></head>
    <body><a href="introduction.html">Docs</a><p>{{license_name}} ({{license}}) at {{base_url}}</p>
    <a href="api.html">API</a><link href="fonts/bricolage.woff2"></body></html>
    """)

    write(root, "site/llms.txt", """
    # Malachi

    > Summary for version {{version}}.

    - [Docs]({{base_url}}/introduction.html)
    """)

    write(root, "site/fonts/bricolage.woff2", "font-bytes")
    write(root, "site/fonts/OFL.txt", "SIL Open Font License")

    write(root, "doc/index.html", @redirect)
    write(root, "doc/introduction.html", "<h1>Introduction</h1>")
    write(root, "doc/api.html", "<h1>API</h1>")
    write(root, "doc/benchmarks/index.html", "<h1>Benchmarks</h1>")
    write(root, "doc/llms.txt", "# Malachi API\n\n- [Module](api.html)\n")

    %{root: root}
  end

  defp write(root, path, content) do
    full = Path.join(root, path)
    File.mkdir_p!(Path.dirname(full))
    File.write!(full, content)
  end

  defp read(root, path), do: File.read!(Path.join(root, path))

  defp stage!(root), do: Site.stage(root, @config)

  describe "metadata/1" do
    test "names the licence and strips the trailing slash from the base URL" do
      meta = Site.metadata(%{@config | site_url: "https://example.test/malachi/"})

      assert meta == %{
               "version" => "1.2.3",
               "license" => "MIT",
               "license_name" => "MIT License",
               "base_url" => "https://example.test/malachi"
             }
    end

    test "an unknown licence fails loudly rather than printing the raw identifier" do
      error =
        assert_raise ArgumentError, fn -> Site.metadata(%{@config | licenses: ["Apache-3.0"]}) end

      assert error.message =~ "Apache-3.0"
    end

    test "names Apache-2.0 by its full licence name" do
      assert Site.metadata(%{@config | licenses: ["Apache-2.0"]})["license_name"] == "Apache License 2.0"
    end

    test "exactly one licence is accepted" do
      assert_raise ArgumentError, fn -> Site.metadata(%{@config | licenses: []}) end
      assert_raise ArgumentError, fn -> Site.metadata(%{@config | licenses: ["MIT", "Apache-2.0"]}) end
    end
  end

  describe "fill/2" do
    test "replaces every known placeholder" do
      meta = Site.metadata(@config)
      assert Site.fill("{{version}} {{version}} {{license}}", meta) == "1.2.3 1.2.3 MIT"
    end

    test "an unknown placeholder fails, so a typo cannot ship as text" do
      meta = Site.metadata(@config)
      assert_raise ArgumentError, ~r/licnese/, fn -> Site.fill("{{licnese}}", meta) end
    end

    test "a placeholder in another case, or with spaces, is unknown and fails" do
      meta = Site.metadata(@config)

      assert_raise ArgumentError, ~r/Version/, fn -> Site.fill("{{Version}}", meta) end
      assert_raise ArgumentError, ~r/version /, fn -> Site.fill("{{version }}", meta) end
    end
  end

  describe "stage/2" do
    test "replaces the ExDoc redirect with the landing page, filled from the config", %{root: root} do
      stage!(root)

      index = read(root, "doc/index.html")
      assert index =~ ~s(<meta name="malachi:landing" content="1">)
      assert index =~ "Malachi 1.2.3"
      assert index =~ "MIT License (MIT) at https://example.test/malachi"
      refute index =~ "{{"
      refute index =~ @redirect
    end

    test "refuses when the page the landing page leads to is missing", %{root: root} do
      File.rm!(Path.join(root, "doc/introduction.html"))

      error = assert_raise Mix.Error, fn -> stage!(root) end
      assert error.message =~ "introduction.html"
    end

    test "refuses, naming the file, when the source page cannot be read, and leaves doc/ as it was", %{root: root} do
      File.rm!(Path.join(root, "site/index.html"))
      before = snapshot(root)

      error = assert_raise Mix.Error, fn -> stage!(root) end
      assert error.message =~ "site/index.html: cannot be read"
      assert snapshot(root) == before
    end

    test "refuses when site/fonts is absent, and leaves doc/ as it was", %{root: root} do
      File.rm_rf!(Path.join(root, "site/fonts"))
      before = snapshot(root)

      error = assert_raise Mix.Error, fn -> stage!(root) end
      assert error.message =~ "holds no font"
      assert snapshot(root) == before
    end

    test "refuses when site/fonts holds no font file", %{root: root} do
      File.rm!(Path.join(root, "site/fonts/bricolage.woff2"))
      File.rm!(Path.join(root, "site/fonts/OFL.txt"))

      assert_raise Mix.Error, ~r/holds no font/, fn -> stage!(root) end
    end

    test "copies the fonts and the licence text beside them", %{root: root} do
      stage!(root)

      assert read(root, "doc/fonts/bricolage.woff2") == "font-bytes"
      assert read(root, "doc/fonts/OFL.txt") == "SIL Open Font License"
    end

    test "writes robots.txt pointing at the sitemap", %{root: root} do
      stage!(root)

      robots = read(root, "doc/robots.txt")
      assert robots =~ "User-agent: *"
      assert robots =~ "Sitemap: https://example.test/malachi/sitemap.xml"
    end

    test "lists every HTML page once, with absolute URLs", %{root: root} do
      stage!(root)

      sitemap = read(root, "doc/sitemap.xml")
      locs = Regex.scan(~r{<loc>([^<]+)</loc>}, sitemap, capture: :all_but_first) |> List.flatten()

      assert Enum.sort(locs) == [
               "https://example.test/malachi/",
               "https://example.test/malachi/api.html",
               "https://example.test/malachi/benchmarks/",
               "https://example.test/malachi/introduction.html"
             ]
    end

    test "escapes all five characters that are special in XML inside the sitemap", %{root: root} do
      Site.stage(root, %{@config | site_url: ~s(https://example.test/a&b<c>"d'e)})

      assert read(root, "doc/sitemap.xml") =~ "https://example.test/a&amp;b&lt;c&gt;&quot;d&apos;e/api.html"
    end

    test "keeps ExDoc's API llms.txt after the landing summary", %{root: root} do
      stage!(root)

      llms = read(root, "doc/llms.txt")
      assert llms =~ "> Summary for version 1.2.3."
      assert llms =~ "- [Docs](https://example.test/malachi/introduction.html)"
      assert [_, api] = String.split(llms, "# Malachi API", parts: 2)
      assert api =~ "- [Module](api.html)"

      # The summary goes first, so ExDoc's content is appended after it, not the reverse.
      {summary_at, _} = :binary.match(llms, "> Summary")
      {api_at, _} = :binary.match(llms, "# Malachi API")
      assert summary_at < api_at
    end

    test "creates llms.txt from the template when ExDoc wrote none", %{root: root} do
      File.rm!(Path.join(root, "doc/llms.txt"))

      stage!(root)

      assert read(root, "doc/llms.txt") =~ "> Summary for version 1.2.3."
    end

    test "refuses to merge over a summary from another version, and asks for mix docs", %{root: root} do
      stage!(root)
      before = snapshot(root)

      error = assert_raise Mix.Error, fn -> Site.stage(root, %{@config | version: "1.2.4"}) end
      assert error.message =~ "Run mix docs first"
      # The refusal comes before any write, so the landing page from the first run is untouched.
      assert snapshot(root) == before
    end

    test "a failed write leaves no temporary file behind", %{root: root} do
      # A directory where the sitemap goes makes its rename fail, after the files before it were written.
      File.mkdir_p!(Path.join(root, "doc/sitemap.xml"))

      assert_raise File.RenameError, fn -> stage!(root) end
      assert Path.wildcard(Path.join(root, "doc/**/*.tmp")) == []
    end

    test "leaves no temporary file behind after a write", %{root: root} do
      stage!(root)

      assert Path.wildcard(Path.join(root, "doc/**/*.tmp")) == []
    end

    test "refuses a truncated summary block, which has its begin marker and no end", %{root: root} do
      File.write!(Path.join(root, "doc/llms.txt"), "<!-- malachi:summary begin -->\n# partial\n")

      error = assert_raise Mix.Error, fn -> stage!(root) end
      assert error.message =~ "Run mix docs first"
    end

    test "staging twice leaves every file as the first run wrote it", %{root: root} do
      stage!(root)
      first = snapshot(root)

      stage!(root)

      assert snapshot(root) == first
    end

    defp snapshot(root) do
      Path.wildcard(Path.join(root, "doc/**/*"))
      |> Enum.filter(&File.regular?/1)
      |> Map.new(&{&1, File.read!(&1)})
    end
  end

  describe "verify/2" do
    test "passes on a staged tree", %{root: root} do
      stage!(root)

      assert Site.verify(root, @config) == []
    end

    test "reports an unfilled placeholder, in any case", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> "{{Version}}")

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "Version"))
    end

    test "reports a page that is still the ExDoc redirect", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", @redirect)

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "malachi:landing"))
    end

    test "reports a missing landing page", %{root: root} do
      stage!(root)
      File.rm!(Path.join(root, "doc/index.html"))

      # The sitemap still lists the root URL, so it is reported too: a removed landing page is a dead entry.
      assert Site.verify(root, @config) == [
               "doc/index.html is missing",
               "doc/sitemap.xml links to https://example.test/malachi/, which is not in doc/"
             ]
    end

    test "reports a font the landing page references and the staged tree does not ship", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s(<link href="fonts/missing.woff2">))

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "fonts/missing.woff2"))
    end

    test "reports a sitemap entry whose page is gone", %{root: root} do
      stage!(root)
      File.rm!(Path.join(root, "doc/api.html"))

      # The llms.txt fixture also links api.html, so the exact sitemap message is what proves the sitemap check.
      assert "doc/sitemap.xml links to https://example.test/malachi/api.html, which is not in doc/" in Site.verify(
               root,
               @config
             )
    end

    test "reports an llms.txt link whose page is gone, when the sitemap does not list that page", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [Guide](guide.html)\n")

      assert Site.verify(root, @config) == ["doc/llms.txt links to guide.html, which is not in doc/"]
    end

    test "reports an llms.txt link whose page is gone, even when the sitemap also names it", %{root: root} do
      stage!(root)
      File.rm!(Path.join(root, "doc/introduction.html"))

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "doc/llms.txt links to"))
    end

    test "checks a link whose fragment contains a colon, by its page, not as a scheme", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [Type](guide.html#t:name/0)\n")

      assert Site.verify(root, @config) == ["doc/llms.txt links to guide.html#t:name/0, which is not in doc/"]
    end

    test "does not check external links, mailto links or bare fragments", %{root: root} do
      stage!(root)

      append(
        root,
        "doc/llms.txt",
        "- [Hex](https://hex.pm/packages/malachi)\n- [Mail](mailto:someone@example.test)\n- [Top](#top)\n"
      )

      assert Site.verify(root, @config) == []
    end

    test "reports a referenced font in a CSS url() that the staged tree does not ship", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s[<p style="background:url(fonts/gone.woff2)">])

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "fonts/gone.woff2"))
    end

    test "does not check a font on another host, which is not a file of this site", %{root: root} do
      stage!(root)

      write(
        root,
        "doc/index.html",
        read(root, "doc/index.html") <> "<link href=\"https://cdn.example.test/fonts/inter.css\">"
      )

      assert Site.verify(root, @config) == []
    end

    test "does not take a sibling site's URL for a page of this one", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [Notes](https://example.test/malachi-notes/x.html)\n")

      assert Site.verify(root, @config) == []
    end

    test "reports a benchmarks/ sitemap entry whose index page is gone", %{root: root} do
      stage!(root)
      File.rm!(Path.join(root, "doc/benchmarks/index.html"))

      assert "doc/sitemap.xml links to https://example.test/malachi/benchmarks/, which is not in doc/" in Site.verify(
               root,
               @config
             )
    end

    test "reports a font named in a src attribute that the staged tree does not ship", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s(<img src="fonts/missing.woff2">))

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "fonts/missing.woff2"))
    end

    # Every way a page can point at a font on this site, each to a file that is not shipped.
    # Each case names the message the check gives: a font on this site that is missing, or a reference that climbs
    # above the site's path (`../`), which is outside the site.
    for {label, markup, reported} <- [
          {"a root-relative path", ~s[<link href="/malachi/fonts/gone.woff2">], "gone.woff2, which is not in doc/"},
          {"a dot-relative path", ~s[<link href="./fonts/gone.woff2">], "gone.woff2, which is not in doc/"},
          {"a parent-relative path", ~s[<link href="../fonts/gone.woff2">],
           "gone.woff2, which is outside this site's path"},
          {"an unquoted attribute", "<link href=fonts/gone.woff2>", "gone.woff2, which is not in doc/"},
          {"a CSS url() with a parent-relative path", ~s[<p style="background:url(../fonts/gone.woff2)">],
           "gone.woff2, which is outside this site's path"},
          {"an absolute URL on this site", ~s[<link href="https://example.test/malachi/fonts/gone.woff2">],
           "gone.woff2, which is not in doc/"}
        ] do
      test "reports a font given as #{label} that the staged tree does not ship", %{root: root} do
        stage!(root)
        write(root, "doc/index.html", read(root, "doc/index.html") <> unquote(markup))

        assert Enum.any?(Site.verify(root, @config), &(&1 =~ unquote(reported)))
      end
    end

    test "reports a link to this site written with another host case, and not an http link on the default port", %{
      root: root
    } do
      stage!(root)
      append(root, "doc/llms.txt", "- [Gone](http://example.test/malachi/gone.html)\n")
      append(root, "doc/llms.txt", "- [Gone too](https://EXAMPLE.test/malachi/gone-too.html)\n")

      errors = Site.verify(root, @config)
      refute Enum.any?(errors, &(&1 =~ "gone.html"))
      assert Enum.any?(errors, &(&1 =~ "gone-too.html"))
    end

    test "an indented four-space line in llms.txt is a code example, not a reference definition", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "\n    [x]: https://example.test/malachi/gone.html\n")

      assert Site.verify(root, @config) == []
    end

    test "a reference definition indented by three spaces is read as a link, and its target checked", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "\n   [x]: https://example.test/malachi/gone.html\n")

      assert "doc/llms.txt links to https://example.test/malachi/gone.html, which is not in doc/" in Site.verify(
               root,
               @config
             )
    end

    test "an http site checks its own http links, which name its own port", %{root: root} do
      config = %{@config | site_url: "http://example.test/malachi"}
      Site.stage(root, config)
      append(root, "doc/llms.txt", "- <http://example.test/malachi/gone.html>\n")

      assert "doc/llms.txt links to http://example.test/malachi/gone.html, which is not in doc/" in Site.verify(
               root,
               config
             )
    end

    test "a missing non-font file under fonts/ is reported, since every file under fonts/ must exist", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s[<a href="fonts/licence.html">x</a>])

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "fonts/licence.html, which is not in doc/"))
    end

    test "a sitemap entry whose name holds an escaped entity is read back as the name it encodes", %{root: root} do
      stage!(root)
      write(root, "doc/a&lt;b.html", "<h1>a</h1>")

      write(
        root,
        "doc/sitemap.xml",
        "<urlset><url><loc>https://example.test/malachi/a&amp;lt;b.html</loc></url></urlset>"
      )

      assert Site.verify(root, @config) == []
    end

    test "a protocol-relative link with another port is another server, and is not reported", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [Gone](//example.test:8443/malachi/gone.html)\n")

      assert Site.verify(root, @config) == []
    end

    test "a site served on its own port checks its links on that port", %{root: root} do
      config = %{@config | site_url: "https://example.test:8443/malachi"}
      Site.stage(root, config)
      append(root, "doc/llms.txt", "- <https://example.test:8443/malachi/gone.html>\n")

      assert "doc/llms.txt links to https://example.test:8443/malachi/gone.html, which is not in doc/" in Site.verify(
               root,
               config
             )
    end

    test "a query string is not part of the file name of a link to this site", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [API](https://example.test/malachi/api.html?v=2)\n- [Local](api.html?v=2)\n")

      assert Site.verify(root, @config) == []
    end

    test "a percent-encoded link to this site names the file it encodes", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [API](%61pi.html)\n")

      assert Site.verify(root, @config) == []
    end

    test "a Markdown link with a title is checked by its URL", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [Guide](guide.html \"Guide\")\n")

      assert Site.verify(root, @config) == ["doc/llms.txt links to guide.html, which is not in doc/"]
    end

    test "reports a protocol-relative link to this site whose page is gone", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [Gone](//example.test/malachi/gone.html)\n")

      assert "doc/llms.txt links to //example.test/malachi/gone.html, which is not in doc/" in Site.verify(
               root,
               @config
             )
    end

    test "reports a font named by a root-relative path outside this site's path", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s[<link href="/fonts/gone.woff2">])

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "/fonts/gone.woff2"))
    end

    test "does not check a root-relative page outside this site's path", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s[<a href="/other/page.html">Other</a>])

      assert Site.verify(root, @config) == []
    end

    test "does not check a font on another host, even when its path begins with this site's path", %{root: root} do
      stage!(root)

      write(
        root,
        "doc/index.html",
        read(root, "doc/index.html") <> "<link href=\"https://cdn.example.test/malachi/fonts/gone.woff2\">"
      )

      assert Site.verify(root, @config) == []
    end

    test "a link to '.' names the index page of doc/", %{root: root} do
      stage!(root)
      File.rm!(Path.join(root, "doc/index.html"))
      append(root, "doc/llms.txt", "- [Home](.)\n")

      assert "doc/llms.txt links to ., which is not in doc/" in Site.verify(root, @config)
    end

    test "a Markdown link whose URL holds balanced parentheses is checked by its whole URL", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [Guide](guide_(b).html)\n")

      # No guide_(b).html is written, so the link can only be reported if its whole URL was read.
      assert Site.verify(root, @config) == ["doc/llms.txt links to guide_(b).html, which is not in doc/"]
    end

    test "a Markdown link with parentheses nested two deep is read, and resolves to its file", %{root: root} do
      stage!(root)
      File.mkdir_p!(Path.join(root, "doc/guides"))
      File.write!(Path.join(root, "doc/guides/a(b(c)).html"), "<h1>Guide</h1>")
      append(root, "doc/llms.txt", "- [Guide](guides/a(b(c)).html)\n")

      assert Site.verify(root, @config) == []
    end

    test "a Markdown link nested deeper than the check reads is reported, not passed over", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [Deep](guides/a(b(c(d))).html)\n")

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "1 Markdown link(s) whose URL this check cannot read"))
    end

    test "a parent segment in the middle of a path is resolved against the path before it", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [Up](guides/../api.html)\n")

      assert Site.verify(root, @config) == []
    end

    test "a font reached through '..' after this site's path is outside the site, whatever file it names", %{root: root} do
      stage!(root)
      write(root, "doc/fonts/gone.woff2", "font")

      write(
        root,
        "doc/index.html",
        read(root, "doc/index.html") <> ~s[<link href="https://example.test/malachi/../fonts/gone.woff2">]
      )

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "outside this site's path"))
    end

    test "reports a font named by an object's data attribute that the staged tree does not ship", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s[<object data="fonts/gone.woff2"></object>])

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "fonts/gone.woff2"))
    end

    test "reports a font in a CSS url() with a space after the parenthesis", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s[<p style="src:url( "fonts/gone.woff2")">])

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "fonts/gone.woff2"))
    end

    test "reports a font the source ships that is not staged, the licence text included", %{root: root} do
      stage!(root)
      File.rm!(Path.join(root, "doc/fonts/OFL.txt"))

      assert "doc/fonts/OFL.txt is missing" in Site.verify(root, @config)
    end

    test "a site_url with no path, as a custom domain has, stages and verifies", %{root: root} do
      config = %{@config | site_url: "https://example.test"}

      assert Site.stage(root, config) == :ok
      assert Site.verify(root, config) == []
    end

    test "reports a font named in an upper-case attribute, a spaced attribute, or a spaced upper-case url()", %{
      root: root
    } do
      stage!(root)

      write(
        root,
        "doc/index.html",
        read(root, "doc/index.html") <>
          ~s[<link HREF = "fonts/gone.woff2"><p style="src:URL( fonts/gone-two.woff2 )">]
      )

      errors = Site.verify(root, @config)
      assert Enum.any?(errors, &(&1 =~ "fonts/gone.woff2"))
      assert Enum.any?(errors, &(&1 =~ "fonts/gone-two.woff2"))
    end

    test "reports a font named in single-quoted attributes and url() arguments", %{root: root} do
      stage!(root)

      write(
        root,
        "doc/index.html",
        read(root, "doc/index.html") <>
          "<link href='fonts/gone.woff2'><p style=\"background:url('fonts/gone-two.woff2')\">"
      )

      errors = Site.verify(root, @config)
      assert Enum.any?(errors, &(&1 =~ "fonts/gone.woff2"))
      assert Enum.any?(errors, &(&1 =~ "fonts/gone-two.woff2"))
    end

    test "reports a link to a directory whose index page is missing, and passes one that has it", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [B](benchmarks)\n")

      assert Site.verify(root, @config) == []

      File.rm!(Path.join(root, "doc/benchmarks/index.html"))

      assert "doc/llms.txt links to benchmarks, which is not in doc/" in Site.verify(root, @config)
    end

    test "a link inside a code span or a fenced block is an example, not a link", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "Use `[text](nope.html)` for links.\n\n```\n[block](nada.html)\n```\n")

      assert Site.verify(root, @config) == []
    end

    test "an encoded slash stays inside its segment, so `..%2F` does not reach outside the site", %{root: root} do
      stage!(root)

      # A file one level above doc/ is what a traversal through the encoded slash would find. The link must not find it.
      File.mkdir_p!(Path.join(root, "fonts"))
      File.write!(Path.join(root, "fonts/gone.woff2"), "font")
      append(root, "doc/llms.txt", "- [Escape](https://example.test/malachi/..%2Ffonts%2Fgone.woff2)\n")

      assert "doc/llms.txt links to https://example.test/malachi/..%2Ffonts%2Fgone.woff2, which is not in doc/" in Site.verify(
               root,
               @config
             )
    end

    test "a link to a directory ending in '/.' whose index page is gone is reported", %{root: root} do
      stage!(root)
      File.rm!(Path.join(root, "doc/benchmarks/index.html"))
      append(root, "doc/llms.txt", "- [B](https://example.test/malachi/benchmarks/.)\n")

      assert "doc/llms.txt links to https://example.test/malachi/benchmarks/., which is not in doc/" in Site.verify(
               root,
               @config
             )
    end

    test "a link to the bare site root, without its trailing slash, is checked", %{root: root} do
      stage!(root)
      File.rm!(Path.join(root, "doc/index.html"))
      append(root, "doc/llms.txt", "- [Home](https://example.test/malachi)\n")

      assert "doc/llms.txt links to https://example.test/malachi, which is not in doc/" in Site.verify(root, @config)
    end

    test "reports a link with a trailing slash to a file, which a browser would not serve", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [API](api.html/)\n")

      assert "doc/llms.txt links to api.html/, which is not in doc/" in Site.verify(root, @config)
    end

    test "a relative '../' reference that climbs above the site's own path leaves the site, and is reported", %{
      root: root
    } do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s[<link href="../fonts/bricolage.woff2">])

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "outside this site's path"))
    end

    test "a checkout whose path holds glob characters still reports a font the staged tree does not ship", %{root: root} do
      stage!(root)
      bracket = Path.join(root, "[x]")
      File.mkdir_p!(bracket)
      File.cp_r!(Path.join(root, "site"), Path.join(bracket, "site"))
      File.cp_r!(Path.join(root, "doc"), Path.join(bracket, "doc"))
      File.rm!(Path.join(bracket, "doc/fonts/OFL.txt"))

      assert "doc/fonts/OFL.txt is missing" in Site.verify(bracket, @config)
    end

    test "a link to a file with a trailing '/.' or '/%2e' is a directory link, which the file does not satisfy", %{
      root: root
    } do
      stage!(root)
      append(root, "doc/llms.txt", "- [A](api.html/.)\n- [B](api.html/%2e)\n")

      errors = Site.verify(root, @config)
      assert "doc/llms.txt links to api.html/., which is not in doc/" in errors
      assert "doc/llms.txt links to api.html/%2e, which is not in doc/" in errors
    end

    test "a fonts path that cannot be listed is reported, not read as an empty folder", %{root: root} do
      File.rm_rf!(Path.join(root, "site/fonts"))
      File.write!(Path.join(root, "site/fonts"), "not a directory")

      error = assert_raise Mix.Error, fn -> stage!(root) end
      assert error.message =~ "site/fonts: cannot be listed"
      assert_raise Mix.Error, ~r/site\/fonts: cannot be listed/, fn -> Site.verify(root, @config) end
    end

    test "a dangling .html symbolic link is not a page, so the sitemap does not list it", %{root: root} do
      File.ln_s!(Path.join(root, "nowhere.html"), Path.join(root, "doc/broken.html"))

      assert stage!(root) == :ok
      refute read(root, "doc/sitemap.xml") =~ "broken.html"
    end

    test "a page elsewhere on this host with a fonts segment is not a font", %{root: root} do
      stage!(root)

      write(
        root,
        "doc/index.html",
        read(root, "doc/index.html") <> ~s[<a href="/other/fonts/gone.woff2">Other</a>]
      )

      refute Enum.any?(Site.verify(root, @config), &(&1 =~ "/other/fonts"))
    end

    test "two fonts with the same file name in different folders are refused, not overwritten", %{root: root} do
      File.mkdir_p!(Path.join(root, "site/fonts/a"))
      File.mkdir_p!(Path.join(root, "site/fonts/b"))
      File.write!(Path.join(root, "site/fonts/a/x.woff2"), "one")
      File.write!(Path.join(root, "site/fonts/b/x.woff2"), "two")

      error = assert_raise Mix.Error, fn -> stage!(root) end
      assert error.message =~ "x.woff2"
    end

    test "a unique font in a subfolder of site/fonts is copied to doc/fonts", %{root: root} do
      File.mkdir_p!(Path.join(root, "site/fonts/sub"))
      File.write!(Path.join(root, "site/fonts/sub/extra.woff2"), "extra-bytes")

      stage!(root)

      assert read(root, "doc/fonts/extra.woff2") == "extra-bytes"
      assert Site.verify(root, @config) == []
    end

    # Root reads any file or folder whatever its mode, so the permission cases below run only as an ordinary user:
    # test_helper excludes `:non_root` under root, and the run reports them as excluded.
    @tag :non_root
    test "a font that cannot be read is refused before anything is written", %{root: root} do
      bricolage = Path.join(root, "site/fonts/bricolage.woff2")
      File.chmod!(bricolage, 0o000)
      on_exit(fn -> File.chmod(bricolage, 0o644) end)
      before = snapshot(root)

      error = assert_raise Mix.Error, fn -> stage!(root) end
      assert error.message =~ "bricolage.woff2: cannot be read"
      assert snapshot(root) == before
    end

    test "a symbolic link in site/fonts is not a font of the source, and is not staged", %{root: root} do
      File.ln_s!(Path.join(root, "site/fonts/bricolage.woff2"), Path.join(root, "site/fonts/linked.woff2"))

      assert stage!(root) == :ok
      refute File.exists?(Path.join(root, "doc/fonts/linked.woff2"))
    end

    @tag :non_root
    test "a folder inside site/fonts that cannot be searched is an error, not an empty folder", %{root: root} do
      locked = Path.join(root, "site/fonts/locked")
      File.mkdir_p!(locked)
      File.write!(Path.join(locked, "extra.woff2"), "extra-bytes")
      File.chmod!(locked, 0o644)
      on_exit(fn -> File.chmod(locked, 0o755) end)

      error = assert_raise Mix.Error, fn -> stage!(root) end
      assert error.message =~ "extra.woff2: cannot be read"
    end

    test "two fonts whose names differ only in case are refused, since one file would hold both", %{root: root} do
      File.mkdir_p!(Path.join(root, "site/fonts/a"))
      File.mkdir_p!(Path.join(root, "site/fonts/b"))
      File.write!(Path.join(root, "site/fonts/a/Inter.woff2"), "AAAA")
      File.write!(Path.join(root, "site/fonts/b/inter.woff2"), "BBBB")

      error = assert_raise Mix.Error, fn -> stage!(root) end
      assert error.message =~ "nter.woff2"
    end

    @tag :non_root
    test "a llms.txt that exists but cannot be read is refused, rather than overwritten", %{root: root} do
      llms = Path.join(root, "doc/llms.txt")
      File.chmod!(llms, 0o000)
      on_exit(fn -> File.chmod(llms, 0o644) end)

      error = assert_raise Mix.Error, fn -> stage!(root) end
      assert error.message =~ "llms.txt: cannot be read"
    end

    test "a relative link in llms.txt that climbs out of the site is reported", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [Home](../index.html)\n")

      assert "doc/llms.txt links to ../index.html, which leaves this site" in Site.verify(root, @config)
    end

    test "a backslash in a font reference is read as a slash, so the font it names is checked", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> "<link href=\"fonts\\gone.woff2\">")

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "fonts\\gone.woff2, which is not in doc/"))
    end

    # Without the replacement the reference reads as `fonts\gone.woff2`, which is not under fonts/, so it is never
    # checked and nothing is reported. The message is the proof that it was read as a font.
    test "a backslash in an absolute URL on this site is read as a slash, so a missing font it names is reported", %{
      root: root
    } do
      stage!(root)

      write(
        root,
        "doc/index.html",
        read(root, "doc/index.html") <> "<link href=\"https://example.test/malachi/fonts\\gone.woff2\">"
      )

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "fonts\\gone.woff2, which is not in doc/"))
    end

    test "an absolute link on this host outside the site's path is another site, and is not reported", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [Other](https://example.test/other/page.html)\n")

      assert Site.verify(root, @config) == []
    end

    test "a sitemap entry on another host, left over from an earlier site_url, is reported", %{root: root} do
      stage!(root)

      write(
        root,
        "doc/sitemap.xml",
        String.replace(read(root, "doc/sitemap.xml"), "https://example.test", "https://old.test")
      )

      assert Enum.any?(
               Site.verify(root, @config),
               &(&1 =~ "doc/sitemap.xml lists https://old.test/malachi/, which is not on example.test")
             )
    end

    test "a sitemap entry on this host outside the site's path is reported, not passed over", %{root: root} do
      stage!(root)

      write(
        root,
        "doc/sitemap.xml",
        "<urlset><url><loc>https://example.test/other/introduction.html</loc></url></urlset>"
      )

      assert "doc/sitemap.xml lists https://example.test/other/introduction.html, which is outside this site's path; stage again to rewrite it" in Site.verify(
               root,
               @config
             )
    end

    test "a path from the root of the host in llms.txt is another site, and is not reported", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [Notes](/malachi-notes/x.html)\n")

      assert Site.verify(root, @config) == []
    end

    test "an http link with no host in llms.txt is reported as unverifiable, not passed over", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [X](https:foo.html)\n")

      assert "doc/llms.txt links to https:foo.html, which this check cannot read" in Site.verify(root, @config)
    end

    test "an http font reference with no host is reported as unverifiable, not passed over", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s(<link href="https:fonts/bricolage.woff2">))

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "https:fonts/bricolage.woff2, which this check cannot read"))
    end

    test "a ../ that climbs above the root of the site is clamped to the root, so the font it names is checked", %{
      root: root
    } do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s(<link href="../../malachi/fonts/gone.woff2">))

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "gone.woff2, which is not in doc/"))
    end

    test "an http link with an empty host in llms.txt is reported as unverifiable, not passed over", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [Gone](https:///example.test/malachi/gone.html)\n")

      assert "doc/llms.txt links to https:///example.test/malachi/gone.html, which this check cannot read" in Site.verify(
               root,
               @config
             )
    end

    test "a font reference with a space after its opening quote is checked, not passed over", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s(<link href=" fonts/missing.woff2">))

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "fonts/missing.woff2, which is not in doc/"))
    end

    test "a link that is not a font is not reported, even when this check cannot read it", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s(<a href="https:about">x</a>))

      assert Site.verify(root, @config) == []
    end

    test "a backslash in a root-relative link in llms.txt is another site's path, as the forward slash is", %{
      root: root
    } do
      stage!(root)
      append(root, "doc/llms.txt", "- [Notes](\\malachi-notes/x.html)\n")

      assert Site.verify(root, @config) == []
    end

    test "a link to the host's root, with no path, is another site's path, and does not crash", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [Home](https://example.test)\n")

      assert Site.verify(root, @config) == []
    end

    test "a non-http scheme naming this host in llms.txt is another site's link, and is not checked", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [Gone](ftp://example.test/malachi/gone.html)\n")

      assert Site.verify(root, @config) == []
    end

    test "an unverifiable sitemap entry is reported as unverifiable, not passed over", %{root: root} do
      stage!(root)
      write(root, "doc/sitemap.xml", "<urlset><url><loc>https:foo.html</loc></url></urlset>")

      assert "doc/sitemap.xml links to https:foo.html, which this check cannot read" in Site.verify(root, @config)
    end

    test "a url() with a space after its opening quote is checked, not passed over", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s[<p style="background:url(' fonts/gone.woff2')">])

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "fonts/gone.woff2, which is not in doc/"))
    end

    test "a reference definition in llms.txt is read as a link, and its target is checked", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [Docs][d]\n\n[d]: https://example.test/malachi/gone.html\n")

      assert "doc/llms.txt links to https://example.test/malachi/gone.html, which is not in doc/" in Site.verify(
               root,
               @config
             )
    end

    test "a reference use with no definition, and a bracketed pair in prose, are text and are not reported", %{
      root: root
    } do
      stage!(root)
      append(root, "doc/llms.txt", "- [Docs][missing]\n- Index with matrix[i][j] for each row\n")

      assert Site.verify(root, @config) == []
    end

    test "a valid reference definition and its use verify clean", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [Docs][d]\n\n[d]: https://example.test/malachi/introduction.html\n")

      assert Site.verify(root, @config) == []
    end

    test "a reference definition with its target in angle brackets is read, and its target checked", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "[g]: <https://example.test/malachi/gone.html>\n")

      assert "doc/llms.txt links to https://example.test/malachi/gone.html, which is not in doc/" in Site.verify(
               root,
               @config
             )
    end

    test "a url() in quotes with a space in its name is read whole, and an outside font is reported", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s[<p style="font:url('/fonts/Inter Bold.woff2')">])

      assert Enum.any?(
               Site.verify(root, @config),
               &(&1 =~ "/fonts/Inter Bold.woff2, which is outside this site's path")
             )
    end

    test "a staged in-site font whose name has a space is found, not reported as missing", %{root: root} do
      stage!(root)
      write(root, "doc/fonts/Inter Variable.woff2", "inter-bytes")

      write(
        root,
        "doc/index.html",
        read(root, "doc/index.html") <> ~s[<p style="font:url("fonts/Inter Variable.woff2")">]
      )

      assert Site.verify(root, @config) == []
    end

    test "a missing font named in double quotes inside url() is reported", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s[<p style="font:url("fonts/gone.woff2")">])

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "fonts/gone.woff2, which is not in doc/"))
    end

    test "a quoted font reference with a trailing space inside its quotes is trimmed before it is checked", %{
      root: root
    } do
      stage!(root)
      write(root, "doc/fonts/Inter.woff2", "inter-bytes")
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s[<link href="fonts/Inter.woff2 ">])

      assert Site.verify(root, @config) == []
    end

    test "a single-quoted reference with a double quote inside its value is read whole", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s[<link href='fonts/Inter "Bold".woff2'>])

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ ~s[fonts/Inter "Bold".woff2, which is not in doc/]))
    end

    test "a sitemap entry with an apostrophe in its name is read back as the file it names", %{root: root} do
      stage!(root)
      write(root, "doc/it's.html", "<h1>It's</h1>")

      write(
        root,
        "doc/sitemap.xml",
        "<urlset><url><loc>https://example.test/malachi/it&apos;s.html</loc></url></urlset>"
      )

      assert Site.verify(root, @config) == []
    end

    test "a non-font file under /fonts/ outside the site's path is not reported as a font", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s(<a href="/fonts/licence.html">x</a>))

      assert Site.verify(root, @config) == []
    end

    test "an autolink to this site in llms.txt is read as a link, and a missing target is reported", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- <https://example.test/malachi/missing.html>\n")

      assert "doc/llms.txt links to https://example.test/malachi/missing.html, which is not in doc/" in Site.verify(
               root,
               @config
             )
    end

    test "an autolink to a page that exists verifies clean", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- <https://example.test/malachi/introduction.html>\n")

      assert Site.verify(root, @config) == []
    end

    test "a missing in-site font with a space in its double-quoted url() is reported", %{root: root} do
      stage!(root)

      write(
        root,
        "doc/index.html",
        read(root, "doc/index.html") <> ~s[<p style="font:url("fonts/Inter Variable.woff2")">]
      )

      assert Enum.any?(
               Site.verify(root, @config),
               &(&1 =~ "fonts/Inter Variable.woff2, which is not in doc/")
             )
    end

    test "a missing font outside fonts/ on this site is reported", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s[<link href="dist/gone.woff2">])

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "dist/gone.woff2, which is not in doc/"))
    end

    test "a link to a page outside fonts/ that is not a font is not reported", %{root: root} do
      stage!(root)
      write(root, "doc/index.html", read(root, "doc/index.html") <> ~s[<a href="dist/notes.html">x</a>])

      assert Site.verify(root, @config) == []
    end

    test "a list-item reference definition in llms.txt is read as a link, and its target checked", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [Docs][d]\n- [d]: https://example.test/malachi/gone.html\n")

      assert "doc/llms.txt links to https://example.test/malachi/gone.html, which is not in doc/" in Site.verify(
               root,
               @config
             )
    end

    test "a blockquote reference definition in llms.txt is read as a link, and its target checked", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "> [g]: https://example.test/malachi/gone.html\n")

      assert "doc/llms.txt links to https://example.test/malachi/gone.html, which is not in doc/" in Site.verify(
               root,
               @config
             )
    end

    test "an autolink to this host on another port is another server, and is not reported", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- <https://example.test:8443/malachi/gone.html>\n")

      assert Site.verify(root, @config) == []
    end

    test "an autolink with a query whose target is missing is reported", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- <https://example.test/malachi/missing.html?v=2>\n")

      assert "doc/llms.txt links to https://example.test/malachi/missing.html?v=2, which is not in doc/" in Site.verify(
               root,
               @config
             )
    end

    test "a URL named both as a definition target and as an autolink is reported once", %{root: root} do
      stage!(root)

      append(
        root,
        "doc/llms.txt",
        "- <https://example.test/malachi/gone.html>\n[g]: https://example.test/malachi/gone.html\n"
      )

      errors = Site.verify(root, @config)

      assert Enum.count(
               errors,
               &(&1 == "doc/llms.txt links to https://example.test/malachi/gone.html, which is not in doc/")
             ) == 1
    end

    test "a blockquote reference definition with no space after its marker is read, and its target checked", %{
      root: root
    } do
      stage!(root)
      append(root, "doc/llms.txt", ">[g]: https://example.test/malachi/gone.html\n")

      assert "doc/llms.txt links to https://example.test/malachi/gone.html, which is not in doc/" in Site.verify(
               root,
               @config
             )
    end

    test "a list line that starts with a label and continues as prose is not a reference definition", %{root: root} do
      stage!(root)
      append(root, "doc/llms.txt", "- [Overview]: see below\n")

      assert Site.verify(root, @config) == []
    end

    test "a protocol-relative link on a site served on another port is another server, and is not reported", %{
      root: root
    } do
      config = %{@config | site_url: "https://example.test:8443/malachi"}
      Site.stage(root, config)
      append(root, "doc/llms.txt", "- [Gone](//example.test/malachi/gone.html)\n")

      assert Site.verify(root, config) == []
    end

    test "the :non_root tests are excluded only when the suite runs as root" do
      {uid, 0} = System.cmd("id", ["-u"])
      excluded = Keyword.get(ExUnit.configuration(), :exclude, [])

      assert :non_root in excluded == (String.trim(uid) == "0")
    end

    test "a dot-named file in site/fonts, such as .DS_Store, is not a font", %{root: root} do
      File.write!(Path.join(root, "site/fonts/.DS_Store"), "junk")

      assert stage!(root) == :ok
      refute File.exists?(Path.join(root, "doc/fonts/.DS_Store"))
    end

    test "reports a missing sitemap", %{root: root} do
      stage!(root)
      File.rm!(Path.join(root, "doc/sitemap.xml"))

      assert Enum.any?(Site.verify(root, @config), &(&1 =~ "sitemap.xml"))
    end

    test "reports a missing robots.txt", %{root: root} do
      stage!(root)
      File.rm!(Path.join(root, "doc/robots.txt"))

      assert Site.verify(root, @config) == ["doc/robots.txt is missing"]
    end

    test "reports a missing llms.txt", %{root: root} do
      stage!(root)
      File.rm!(Path.join(root, "doc/llms.txt"))

      assert Site.verify(root, @config) == ["doc/llms.txt is missing"]
    end
  end

  defp append(root, path, text), do: File.write!(Path.join(root, path), text, [:append])

  # Cross checks against the real project. When the licence changes, the mapping in Malachi.Site and the
  # LICENSE file change together, and these fail if only one of them moved.
  describe "the real project" do
    test "the licence the landing page names is the one the LICENSE file carries" do
      meta = Site.metadata(Site.from_project(Mix.Project.config()))
      license_file = File.read!(Path.join(File.cwd!(), "LICENSE"))

      assert hd(String.split(license_file, "\n")) == meta["license_name"]
    end

    test "the base URL the landing page uses is the one in mix.exs" do
      meta = Site.metadata(Site.from_project(Mix.Project.config()))

      assert meta["base_url"] == Mix.Project.config()[:site_url]
    end

    test "the version the landing page prints is the one in mix.exs" do
      meta = Site.metadata(Site.from_project(Mix.Project.config()))

      assert meta["version"] == Mix.Project.config()[:version]
    end
  end
end
