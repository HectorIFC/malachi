defmodule Malachi.Site do
  @moduledoc """
  Stages the landing page at the root of the published documentation site, and the files that go with it.

  `mix docs` writes ExDoc's output to `doc/`, where `index.html` is only a redirect to the introduction. This
  module replaces that redirect with the landing page, and writes `robots.txt` and `sitemap.xml` beside the
  pages it covers. It also merges the landing page's `llms.txt` summary with the one ExDoc already writes.

  The `docs` alias runs `mix malachi.docs.site` after the ExDoc build, so `mix docs` stages the landing page. CI
  runs `--check` after the docs build, and the browser checks in `site/checks` run on the staged tree.

  The source lives in `site/` at the repository root. `from_project/1` reads the version, the licence and the
  base URL from `mix.exs`, and `metadata/1` turns them into the values the `{{name}}` placeholders in the source
  take. The page, its structured data, `llms.txt`, `robots.txt` and `sitemap.xml` agree with `mix.exs` wherever
  they take those values from a placeholder; a literal copy typed into the source is not compared.

  `verify/2` checks a staged tree and returns the problems among these checks: the landing page is in place, fully
  filled, references no font on this site that the site does not ship, every font the source ships is staged,
  `robots.txt`, `sitemap.xml` and `llms.txt` are present, and every URL in `sitemap.xml` and every Markdown link to this site
  in `llms.txt` resolves to a file in `doc/`. A link to a directory resolves when that directory has an `index.html`.
  It does not compare the page with `mix.exs`, so run stage again after the version or the licence changes.
  Not checked: the landing page's own links other than fonts, fonts on other hosts, bare URLs in `llms.txt`,
  code examples, HTML links in `llms.txt`, and links to other sites, including other paths on this host, which belong to
  other sites. Fonts
  under `/fonts/` on this host are checked, and so is every font this site serves, whichever folder holds it.
  """

  @source "site"
  # The source is the design kit, laid out as the design project has it. The landing page is the kit's page.
  @landing_source "ui_kits/website/index.html"
  @llms_source "ui_kits/website/llms.txt"
  @fonts_source "assets/fonts"
  # The page loads its stylesheets, scripts and logo by paths relative to the kit, so they are staged beside
  # doc/index.html under the names the page uses, and its `../../` prefix is dropped (see stage_paths/1).
  @page_sources [
    "ui_kits/website/site.css",
    "ui_kits/website/site.js",
    "ui_kits/website/scenes.js",
    "ui_kits/website/model.js",
    "styles.css",
    "tokens/base.css",
    "tokens/colors.css",
    "tokens/effects.css",
    "tokens/marketing.css",
    "tokens/spacing.css",
    "tokens/typography.css",
    "assets/logo-mark.svg",
    "assets/logo.svg"
  ]
  @landing_marker ~s(<meta name="malachi:landing" content="1">)
  @summary_begin "<!-- malachi:summary begin -->"
  @summary_end "<!-- malachi:summary end -->"
  # Any `{{...}}` is a placeholder, so a misspelt or differently cased name is an error rather than text.
  @placeholder ~r/\{\{([^{}]+)\}\}/
  # A reference the landing page makes to a file: an `src`, `href` or `data` attribute, quoted or not, with spaces
  # around the `=`, or a CSS `url()`, quoted or not. A quoted value is read whole, spaces included. Names
  # are case-insensitive, as they are in HTML and CSS. The references that name a font are checked by `font_errors/3`,
  # which resolves each one with `local_path/2`.
  @reference ~r{\b(?:src|href|data)\s*=\s*(?:"\s*([^"]*)"|'\s*([^']*)'|\s*([^\s"'>]+))|url\(\s*(?:"\s*([^"]*)"|'\s*([^']*)'|\s*([^"')\s]+))}i
  # The file extensions of a font. A reference with another extension is not a font, but every file under fonts/
  # must exist in doc/, whatever its extension. Staging copies the regular files there that have no leading dot, and a
  # reference to any other file is checked on its own.
  @font_extension ~r{\.(?:woff2?|ttf|otf)(?:[?#]|$)}i
  @locs ~r{<loc>([^<]+)</loc>}
  @xml_entities %{"amp" => "&", "lt" => "<", "gt" => ">", "quot" => "\"", "apos" => "'"}
  # A Markdown link, with an optional title after the URL: `[text](url)` or `[text](url "title")`. The URL may
  # hold a balanced pair of parentheses, and a pair within it, as in `a(b(c)).html`. A deeper nesting is not
  # read, and `llms_errors/2` reports it rather than passing it over.
  @markdown_links ~r{\]\(((?:[^()\s]|\((?:[^()\s]|\([^()\s]*\))*\))+)(?:\s+"[^"]*")?\)}
  @link_opener ~r{\]\(}
  # A reference definition, `[id]: url`, may be indented up to three spaces (a tab is code), or follow a list marker or
  # `>`. Its URL is a link, and it may be in angle brackets. A use of a definition, `[text][id]`, is text when no
  # definition exists (CommonMark), so no use is reported.
  @reference_definition ~r/^(?:[ ]{0,3}|[ \t]*(?:(?:[-*+]|\d+[.)])[ \t]+|>[ \t]*)+)\[([^\]]+)\]:[ \t]*<?([^\s>]+)>?(?=[ \t]*(?:$|"[^"\n]*"[ \t]*$|'[^'\n]*'[ \t]*$|\([^)\n]*\)[ \t]*$))/m
  # An autolink, `<https://...>`, is a link to the URL it names.
  @autolink ~r{<(https?://[^\s<>]+)>}
  # Code spans and fenced blocks are examples, not links, so their text is not read for links.
  @code ~r{```.*?```|`[^`\n]*`}s

  # SPDX identifier to the name the page prints. Adding a licence means adding it here, and the test that
  # compares this with LICENSE fails until the two agree.
  @licenses %{"MIT" => "MIT License", "Apache-2.0" => "Apache License 2.0"}

  @doc "The project settings the landing page is built from, read from `Mix.Project.config/0`."
  @spec from_project(keyword()) :: map()
  def from_project(project) do
    %{
      version: project[:version],
      licenses: get_in(project, [:package, :licenses]),
      site_url: project[:site_url],
      main: get_in(project, [:docs, :main])
    }
  end

  @doc """
  The values `{{name}}` placeholders take. The base URL has no trailing slash, so `base_url <> "/page.html"`
  is always a valid URL.
  """
  @spec metadata(map()) :: %{String.t() => String.t()}
  def metadata(%{licenses: [spdx], site_url: site_url, version: version}) do
    name =
      Map.get(@licenses, spdx) ||
        raise ArgumentError,
              "unknown licence #{inspect(spdx)}: add it to Malachi.Site so the landing page can name it"

    %{
      "version" => version,
      "license" => spdx,
      "license_name" => name,
      "base_url" => String.trim_trailing(site_url, "/")
    }
  end

  def metadata(%{licenses: licenses}) do
    raise ArgumentError, "expected exactly one licence in package licenses, got: #{inspect(licenses)}"
  end

  @doc "Replaces every `{{name}}` in `text`. An unknown name, in any case, raises, so a typo cannot ship as text."
  @spec fill(String.t(), map()) :: String.t()
  def fill(text, metadata) do
    Regex.replace(@placeholder, text, fn _whole, name ->
      case Map.fetch(metadata, name) do
        {:ok, value} -> value
        :error -> raise ArgumentError, "unknown placeholder {{#{name}}} in the landing page"
      end
    end)
  end

  # The staged fonts are the ones the source ships now, so a font withdrawn from site/fonts is not left published. Only
  # regular files in doc/fonts are removed, the folder that stage writes.
  defp remove_stale_fonts(doc, fonts) do
    keep = MapSet.new(fonts, &Path.basename/1)
    fonts_dir = Path.join(doc, "fonts")

    for name <- File.ls!(fonts_dir),
        not MapSet.member?(keep, name),
        File.regular?(Path.join(fonts_dir, name)) do
      File.rm!(Path.join(fonts_dir, name))
    end
  end

  @doc """
  Writes the landing page, the fonts, `llms.txt`, `robots.txt` and `sitemap.xml` into `root/doc`.

  The checks run before anything is written. Each file is written to a temporary name and then renamed into
  place, so no file is left half-written; a run interrupted between two renames can leave `doc/` partly
  staged. Running `stage` again completes such a run, unless `doc/llms.txt` already holds a summary from
  another version, which needs `mix docs` first. Raises when `doc/<main>.html` is missing, because the landing
  page links to it; when `site/fonts` holds no font, or two fonts share a name ignoring case, or a font or the
  fonts folder cannot be read or listed; when `index.html`, `llms.txt` or the licence in `mix.exs` cannot be used.
  """
  @spec stage(Path.t(), map()) :: :ok
  def stage(root, config) do
    meta = metadata(config)
    doc = Path.join(root, "doc")
    main = config.main <> ".html"

    unless File.exists?(Path.join(doc, main)) do
      Mix.raise("doc/#{main} is missing, so the landing page would link to nothing. Run mix docs first.")
    end

    landing = root |> read!(@landing_source) |> fill(meta) |> stage_paths() |> with_marker()
    llms = merge_llms(summary_block(fill(read!(root, @llms_source), meta)), Path.join(doc, "llms.txt"))
    pages = for rel <- @page_sources, do: {dest_of(rel), stage_asset(dest_of(rel), read!(root, rel))}
    fonts = font_files!(root)
    sitemap_xml = sitemap(doc, meta)

    File.mkdir_p!(Path.join(doc, "fonts"))

    for file <- fonts do
      replace!(Path.join([doc, "fonts", Path.basename(file)]), &File.cp!(file, &1))
    end

    remove_stale_fonts(doc, fonts)
    write!(doc, "index.html", landing)
    write!(doc, "llms.txt", llms)
    for {dest, text} <- pages, do: write!(doc, dest, text)
    write!(doc, "robots.txt", robots(meta))
    write!(doc, "sitemap.xml", sitemap_xml)
    :ok
  end

  # The landing page's paths to the kit name `../../`, which is the kit's own folder depth. Staged at the root of
  # doc/, the page's font is `fonts/`, and everything else is at the root too.
  defp stage_paths(html) do
    html
    |> String.replace("../../assets/fonts/", "fonts/")
    |> String.replace("../../", "")
  end

  # A stylesheet reaches the kit's fonts by a path from the kit's own folder. Staged one level up, the fonts sit at
  # fonts/, so that path is rewritten here. Any other url() is checked by css_url_errors/1, not rewritten.
  defp stage_asset(dest, text) do
    if String.ends_with?(dest, ".css"),
      do: String.replace(text, "../assets/fonts/", "../fonts/"),
      else: text
  end

  # The marker tells the staged page apart from the ExDoc redirect that `mix docs` writes to doc/index.html.
  defp with_marker(html) do
    if String.contains?(html, @landing_marker),
      do: html,
      else: String.replace(html, "<head>", "<head>\n" <> @landing_marker, global: false)
  end

  # Where a page file of the kit is staged: the page's own folder is flattened to the root of doc/, and the rest keeps
  # its path.
  defp dest_of("ui_kits/website/" <> name), do: name
  defp dest_of(rel), do: rel

  @doc """
  Returns every problem in a staged `root/doc`, or `[]` when it is correct. See the moduledoc for what is checked.
  A fonts folder that cannot be listed raises instead: a list that left that folder out would not be complete.
  """
  @spec verify(Path.t(), map()) :: [String.t()]
  def verify(root, config) do
    meta = metadata(config)
    doc = Path.join(root, "doc")

    index_errors(doc, meta) ++
      page_errors(doc) ++
      css_url_errors(doc, site_path(meta)) ++
      ensure_file(doc, "robots.txt") ++
      staged_font_errors(root, doc) ++
      sitemap_errors(doc, meta) ++
      llms_errors(doc, meta)
  end

  @css_url ~r/url\(\s*["']?([^"')\s]+)["']?\s*\)/

  # A url() in a staged stylesheet names a file the page must load. A relative one resolves from the stylesheet's own
  # folder; a root-relative one names a path under the site's own path, which is doc/ once staged. External URLs
  # (a scheme, or a protocol-relative //host) are not local, and a fragment-only url() names nothing to load.
  defp css_url_errors(doc, site_path) do
    @page_sources
    |> Enum.map(&dest_of/1)
    |> Enum.filter(&String.ends_with?(&1, ".css"))
    |> Enum.flat_map(fn css ->
      path = Path.join(doc, css)

      case File.read(path) do
        {:ok, text} ->
          for [_, ref] <- Regex.scan(@css_url, text),
              local_url?(ref),
              error <- url_error(ref, Path.dirname(path), doc, site_path),
              do: "doc/#{css} #{error}"

        {:error, _} ->
          []
      end
    end)
  end

  # The error for one local url(), or nothing when the file it names is staged. The query and fragment are not part of
  # the file name, so `font.woff2?v=2` names font.woff2.
  defp url_error(ref, stylesheet_dir, doc, site_path) do
    name = ref_path(ref)

    case local_target(name, stylesheet_dir, doc, site_path) do
      :outside -> ["references #{ref}, which is outside the site"]
      target -> if File.regular?(target), do: [], else: ["references #{ref}, which is not staged"]
    end
  end

  defp local_target("/" <> _ = name, _dir, doc, site_path) do
    rel = if site_path == "", do: name, else: String.replace_prefix(name, site_path, "")

    cond do
      site_path == "" -> Path.join(doc, rel)
      rel != name and String.starts_with?(rel, "/") -> Path.join(doc, rel)
      true -> :outside
    end
  end

  defp local_target(name, dir, _doc, _site_path), do: Path.expand(name, dir)

  defp ref_path(ref), do: ref |> String.split(["?", "#"], parts: 2) |> hd()

  defp local_url?(ref) do
    not (String.contains?(ref, ":") or String.starts_with?(ref, ["#", "//"]))
  end

  # The path the site is published under, without a trailing slash: "" for a site at the root of its host.
  defp site_path(meta) do
    case URI.parse(meta["base_url"]).path do
      nil -> ""
      path -> String.trim_trailing(path, "/")
    end
  end

  # The files the staged page loads by path. A page without them renders unstyled, so each one must be staged.
  defp page_errors(doc) do
    for rel <- @page_sources, not File.exists?(Path.join(doc, dest_of(rel))), do: "doc/#{dest_of(rel)} is missing"
  end

  defp index_errors(doc, meta) do
    case File.read(Path.join(doc, "index.html")) do
      {:ok, html} ->
        if String.contains?(html, @landing_marker) do
          placeholder_errors(html) ++ font_errors(doc, html, meta)
        else
          ["doc/index.html is not the landing page (it lacks #{@landing_marker})"]
        end

      {:error, _} ->
        ["doc/index.html is missing"]
    end
  end

  defp placeholder_errors(html) do
    @placeholder
    |> Regex.scan(html, capture: :all_but_first)
    |> List.flatten()
    |> Enum.uniq()
    |> Enum.map(&"doc/index.html has an unfilled placeholder {{#{&1}}}")
  end

  # A font is a reference with a font extension that resolves to this site, whichever folder holds it, and every file
  # under fonts/ must be staged. A font outside fonts/ that is missing is reported, as is a font this check cannot
  # place, or one that names a font outside this site's path under /fonts/. Any other reference, outside fonts/, is
  # passed over, and the page's own links are not checked by any other function.
  defp font_errors(doc, html, meta) do
    @reference
    |> Regex.scan(html, capture: :all_but_first)
    |> List.flatten()
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
    |> Enum.flat_map(fn ref ->
      case local_path(ref, meta) do
        {:ok, "fonts/" <> _ = font} ->
          if file_here?(doc, font),
            do: [],
            else: ["doc/index.html references #{ref}, which is not in doc/"]

        {:ok, rel} ->
          if font_file?(ref) and not file_here?(doc, rel),
            do: ["doc/index.html references #{ref}, which is not in doc/"],
            else: []

        # A font the host serves from its root, outside this site's path, is not served by this site, so it cannot be
        # shipped. A page elsewhere on the host that happens to have a `fonts` segment is not a font.
        {:outside, path} ->
          if String.starts_with?(path, "/fonts/") and font_file?(ref),
            do: ["doc/index.html references #{ref}, which is outside this site's path"],
            else: []

        :unverifiable ->
          if font_file?(ref),
            do: ["doc/index.html references #{ref}, which this check cannot read"],
            else: []

        _ ->
          []
      end
    end)
  end

  # Every font the source ships must be staged, and the licence text that ships beside it is one of them.
  defp staged_font_errors(root, doc) do
    for file <- source_fonts(root), not File.exists?(Path.join([doc, "fonts", Path.basename(file)])) do
      "doc/fonts/#{Path.basename(file)} is missing"
    end
  end

  defp sitemap_errors(doc, meta) do
    case File.read(Path.join(doc, "sitemap.xml")) do
      {:ok, xml} ->
        xml
        |> locs()
        |> Enum.flat_map(&sitemap_entry_errors(doc, meta, &1))

      {:error, _} ->
        ["doc/sitemap.xml is missing"]
    end
  end

  # The sitemap is this site's own list, so an entry that is not under this site's path is reported, not passed over
  # as another site's link. An earlier site_url in mix.exs left it behind, and staging again rewrites it.
  defp sitemap_entry_errors(doc, meta, url) do
    case local_path(url, meta) do
      :external ->
        ["doc/sitemap.xml lists #{url}, which is not on #{URI.parse(meta["base_url"]).host}; stage again to rewrite it"]

      {:outside, _path} ->
        ["doc/sitemap.xml lists #{url}, which is outside this site's path; stage again to rewrite it"]

      _ ->
        unresolved(doc, meta, url, "doc/sitemap.xml")
    end
  end

  # Whether a parsed URL names this site's host and port, without regard to the host's case. Only http, https or no
  # scheme name one here, and a URL with another port is another server.
  defp this_host?(uri, site) do
    uri.scheme in [nil, "http", "https"] and uri.host != nil and
      String.downcase(uri.host) == String.downcase(site.host) and
      (uri.port || URI.default_port(site.scheme)) == site.port
  end

  # The targets of the reference definitions that begin a block. A plain definition is paragraph text when it follows
  # paragraph text directly, as CommonMark does, so only the ones that begin a block are read.
  defp definition_urls(text) do
    @reference_definition
    |> Regex.scan(text, return: :index)
    |> Enum.filter(fn [{start, len} | _] -> begins_block?(text, binary_part(text, start, len), start) end)
    |> Enum.map(fn [_whole, _label, {url_start, url_len}] -> binary_part(text, url_start, url_len) end)
  end

  # A definition after a list marker or `>` begins its own block. A plain one begins a block after a blank line, at the
  # start of the text, or after another definition.
  defp begins_block?(text, match, start) do
    if String.starts_with?(String.trim_leading(match), "["),
      do: plain_follows_block?(binary_part(text, 0, start)),
      else: true
  end

  defp plain_follows_block?(prefix) do
    cond do
      String.ends_with?(prefix, "\n\n") -> true
      String.ends_with?(prefix, "\n") -> block_line?(previous_line(prefix))
      true -> false
    end
  end

  # A definition or an ATX heading before it ends a block, so a plain definition after either begins one.
  defp block_line?(line) do
    Regex.match?(@reference_definition, line) or Regex.match?(~r/^ {0,3}\#{1,6}[ \t]/, line)
  end

  defp previous_line(prefix) do
    prefix |> String.split("\n") |> Enum.at(-2, "")
  end

  # A Markdown destination may be written in angle brackets, `[text](<url>)`, which is the same URL without them.
  defp strip_angle(url) do
    if String.starts_with?(url, "<") and String.ends_with?(url, ">"),
      do: String.slice(url, 1, String.length(url) - 2),
      else: url
  end

  defp llms_errors(doc, meta) do
    case File.read(Path.join(doc, "llms.txt")) do
      {:ok, raw} ->
        text = Regex.replace(@code, raw, "")

        links =
          @markdown_links |> Regex.scan(text, capture: :all_but_first) |> List.flatten() |> Enum.map(&strip_angle/1)

        unread = length(Regex.scan(@link_opener, text)) - length(links)

        definition_urls = definition_urls(text)

        autolinks = @autolink |> Regex.scan(text, capture: :all_but_first) |> List.flatten()

        # A URL can be named twice, as an angle-bracketed definition target is also an autolink; each is checked once.
        urls = Enum.uniq(links ++ definition_urls ++ autolinks)

        Enum.flat_map(urls, &unresolved(doc, meta, &1, "doc/llms.txt")) ++
          unread_link_errors(unread)

      {:error, _} ->
        ["doc/llms.txt is missing"]
    end
  end

  # A link whose URL is nested deeper than the pattern reads is counted here, so it is reported and not passed over.
  defp unread_link_errors(0), do: []

  defp unread_link_errors(count),
    do: ["doc/llms.txt has #{count} Markdown link(s) whose URL this check cannot read, so they are not verified"]

  defp unresolved(doc, meta, url, where) do
    case local_path(url, meta) do
      :external ->
        []

      {:outside, _path} ->
        # A relative link that climbs out of the site is this site's own link, and it does not resolve on the site. A
        # path from the root of the host, and an absolute link to another path on this host, belong to another site.
        if relative_link?(url), do: ["#{where} links to #{url}, which leaves this site"], else: []

      :unverifiable ->
        ["#{where} links to #{url}, which this check cannot read"]

      {:ok, rel} ->
        if resolves?(doc, rel), do: [], else: ["#{where} links to #{url}, which is not in doc/"]
    end
  end

  defp relative_link?(raw) do
    uri = URI.parse(normalised(raw))
    uri.scheme == nil and uri.host == nil and not String.starts_with?(uri.path || "", "/")
  end

  defp font_file?(ref), do: Regex.match?(@font_extension, ref)

  # A browser reads a backslash as a slash in an http(s) URL, so every URL is read with its backslashes as slashes.
  defp normalised(raw), do: String.replace(raw, "\\", "/")

  # A path resolves when it is a file, or a directory with an index page, which is what a browser serves for it.
  defp resolves?(doc, rel) do
    cond do
      not exact_path?(doc, rel) -> false
      File.dir?(Path.join(doc, rel)) -> exact_path?(doc, Path.join(rel, "index.html"))
      true -> true
    end
  end

  # A font is a regular file that exists under doc/ with exactly these letters.
  defp file_here?(doc, rel), do: exact_path?(doc, rel) and File.regular?(Path.join(doc, rel))

  # Whether every segment of a path under doc/ exists with exactly its letters. A case-insensitive file system would
  # otherwise match `API.html` to `api.html`, which the Linux host that serves the page does not.
  defp exact_path?(doc, rel) do
    rel
    |> Path.split()
    |> Enum.reduce_while(doc, fn segment, dir ->
      case File.ls(dir) do
        {:ok, names} -> if segment in names, do: {:cont, Path.join(dir, segment)}, else: {:halt, nil}
        {:error, _} -> {:halt, nil}
      end
    end)
    |> is_binary()
  end

  # Where a URL points, as a path relative to the root of doc/. It is :external when another scheme or another
  # host is named, and `{:outside, path}` when this host is named outside this site's own path. A URL is this
  # site's when it is relative, or names this site's host (the host is compared without case, with or without
  # a scheme). A relative path is taken from the root of doc/. The query and the fragment are not part of the
  # file name, and a fragment or a query alone names no file. An http or https URL with no host (`https:foo.html`)
  # names no host this check can compare, so it is `:unverifiable` rather than passed over as external.
  defp local_path(raw, %{"base_url" => base}) do
    uri = URI.parse(normalised(raw))
    site = URI.parse(base)
    site_dir = String.trim_trailing(site.path || "", "/")

    cond do
      uri.scheme == nil and uri.host == nil ->
        relative(uri.path, site_dir)

      uri.scheme in ["http", "https"] and uri.host in [nil, ""] ->
        :unverifiable

      this_host?(uri, site) ->
        under_site(resolve_dots(uri.path || "/"), site_dir)

      true ->
        :external
    end
  end

  defp relative(path, _site_dir) when path in [nil, ""], do: :external
  defp relative("/" <> _ = path, site_dir), do: under_site(resolve_dots(path), site_dir)

  # A relative reference is resolved from the site's own directory, as the browser resolves it on the landing page, so
  # a `../` that climbs above the site's path leaves the site and is reported.
  defp relative(path, site_dir), do: under_site(resolve_dots(site_dir <> "/" <> path), site_dir)

  # A path on this site begins with the site's own path, and the file is what follows it. A path on the same
  # host outside that path is `{:outside, path}`: the browser asks this host for it, and this site does not serve it.
  defp under_site(path, site_dir) do
    cond do
      path in [site_dir, site_dir <> "/"] -> {:ok, file_of("/")}
      String.starts_with?(path, site_dir <> "/") -> {:ok, file_of(String.replace_prefix(path, site_dir, ""))}
      true -> {:outside, path}
    end
  end

  # The absolute path with its `.` and `..` segments resolved, so `/malachi/../fonts/x` is `/fonts/x`, and it is
  # resolved before the site's path is checked, since that is where the browser resolves it. Each segment is
  # decoded once. An encoded slash stays inside its segment, as the browser keeps it, so it cannot become a
  # separator. A path that names a directory keeps its trailing slash.
  defp resolve_dots(raw) do
    # The last segment decides, decoded, so `%2e` names a directory as `.` does, and an empty last segment is a
    # trailing slash.
    last = raw |> String.split("/") |> List.last() |> URI.decode()
    names_directory? = last in ["", ".", ".."]

    segments =
      raw
      |> String.split("/")
      |> Enum.map(&decode_segment/1)
      |> Enum.reduce([], &push_segment/2)
      |> Enum.reverse()

    resolved = "/" <> Enum.join(segments, "/")
    if names_directory? and resolved != "/", do: resolved <> "/", else: resolved
  end

  defp decode_segment(segment) do
    decoded = URI.decode(segment)
    if String.contains?(decoded, "/"), do: segment, else: decoded
  end

  defp push_segment("", acc), do: acc
  defp push_segment(".", acc), do: acc
  defp push_segment("..", []), do: []
  defp push_segment("..", [_ | rest]), do: rest
  defp push_segment(segment, acc), do: [segment | acc]

  # A resolved path names a file, or a directory that ends in `/`, and that directory's index page is the file.
  defp file_of(path) do
    path = if path == "" or String.ends_with?(path, "/"), do: path <> "index.html", else: path
    String.trim_leading(path, "/")
  end

  defp ensure_file(doc, name) do
    if File.exists?(Path.join(doc, name)), do: [], else: ["doc/#{name} is missing"]
  end

  defp locs(xml) do
    @locs |> Regex.scan(xml, capture: :all_but_first) |> List.flatten() |> Enum.map(&xml_unescape/1)
  end

  # Undoes `xml_escape/1` in one pass, so `&amp;lt;` reads back as `&lt;` and not as `<`.
  defp xml_unescape(text) do
    Regex.replace(~r/&(amp|lt|gt|quot|apos);/, text, fn _, name -> Map.fetch!(@xml_entities, name) end)
  end

  # Fonts are staged flat, by file name, so two fonts with the same name in different folders would overwrite each
  # other in doc/fonts. That is refused here rather than shipping whichever was copied last.
  defp font_files!(root) do
    files =
      case source_fonts(root) do
        [] ->
          Mix.raise(
            "#{@source}/#{@fonts_source} holds no font, so the landing page would reference fonts it does not ship."
          )

        found ->
          found
      end

    names = Enum.map(files, &Path.basename/1)

    # Compared without case: the default filesystem on macOS keeps `Inter.woff2` and `inter.woff2` as one file.
    folded = Enum.map(names, &String.downcase/1)

    case folded -- Enum.uniq(folded) do
      [] ->
        Enum.each(files, &assert_readable!/1)
        files

      [_ | _] = clashing ->
        duplicates = names |> Enum.filter(&(String.downcase(&1) in clashing)) |> Enum.uniq()

        Mix.raise(
          "#{@source}/#{@fonts_source} has more than one font named #{Enum.join(duplicates, ", ")}, " <>
            "and they would overwrite each other in doc/fonts"
        )
    end
  end

  # A font must be readable before the copy starts. One found unreadable partway through the copy would leave doc/fonts
  # part written, so it is refused here, before anything is written.
  defp assert_readable!(file) do
    case File.open(file, [:read]) do
      {:ok, io} -> File.close(io)
      {:error, reason} -> Malachi.SourceFile.unreadable!(file, reason)
    end
  end

  defp source_fonts(root), do: list_files(Path.join([root, @source, @fonts_source]))

  # The regular files under a directory, found by listing it rather than by a glob, so a path that holds `[`, `{`,
  # `*` or `?` is read as the directory it is. A glob would match nothing under such a path, silently.
  # A directory that cannot be listed is an error, not an empty directory: an unreadable folder of fonts would
  # otherwise be skipped without a word, and the check would pass with a font missing from doc/.
  defp list_files(dir) do
    case File.ls(dir) do
      {:ok, names} ->
        names
        # A name that starts with a dot is not a font of the source (`.DS_Store` is one), as the console's asset walk
        # also skips it.
        |> Enum.reject(&String.starts_with?(&1, "."))
        |> Enum.flat_map(fn name ->
          path = Path.join(dir, name)

          case File.lstat(path) do
            {:ok, %File.Stat{type: :directory}} -> list_files(path)
            {:ok, %File.Stat{type: :regular}} -> [path]
            # A symbolic link is not followed, so a loop cannot recurse without end, and a font reached through a link
            # is not part of the source: a font belongs in the folder itself. The console's asset walk treats links the
            # same way.
            {:ok, %File.Stat{}} -> []
            {:error, reason} -> Malachi.SourceFile.unreadable!(path, reason)
          end
        end)

      # A folder that is not there holds nothing, so staging raises its own "no font" message, and verify reports none.
      # Any other failure means the folder exists and cannot be read.
      {:error, :enoent} ->
        []

      {:error, reason} ->
        Mix.raise("#{dir}: cannot be listed (#{:file.format_error(reason)})")
    end
  end

  # ExDoc's llms.txt is appended after the landing summary, which is wrapped in markers so a later run can
  # find it. A file that already starts with this summary was merged by an earlier run, and is left as it is.
  # A file with a summary from another version cannot be merged safely: ExDoc has to rewrite it first.
  defp summary_block(summary) do
    @summary_begin <> "\n" <> String.trim_trailing(summary) <> "\n" <> @summary_end <> "\n"
  end

  defp merge_llms(block, existing_path) do
    case File.read(existing_path) do
      {:ok, existing} ->
        cond do
          String.starts_with?(existing, block) ->
            existing

          String.contains?(existing, @summary_begin) ->
            Mix.raise(
              "doc/llms.txt holds a landing summary from an earlier build. ExDoc does not rewrite it here (formatters: html), so remove doc/llms.txt and stage again."
            )

          true ->
            block <> "\n" <> existing
        end

      # A llms.txt that is not there yet is the first run. One that is there but cannot be read is refused: writing
      # over it would throw away the ExDoc content it holds.
      {:error, :enoent} ->
        block

      {:error, reason} ->
        Malachi.SourceFile.unreadable!(existing_path, reason)
    end
  end

  defp robots(meta) do
    """
    User-agent: *
    Allow: /

    Sitemap: #{meta["base_url"]}/sitemap.xml
    """
  end

  # Symbolic links are not listed (see list_files/1), so a dangling link never reaches the sitemap.
  defp sitemap(doc, meta) do
    entries =
      doc
      |> html_pages()
      |> Enum.map(&"  <url><loc>#{xml_escape(meta["base_url"] <> url_path(&1))}</loc></url>")

    """
    <?xml version="1.0" encoding="UTF-8"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
    #{Enum.join(entries, "\n")}
    </urlset>
    """
  end

  defp html_pages(doc) do
    doc
    |> list_files()
    |> Enum.filter(&String.ends_with?(&1, ".html"))
    |> Enum.map(&Path.relative_to(&1, doc))
    |> Enum.sort()
  end

  # `index.html` is served at its directory, so `benchmarks/index.html` is `/benchmarks/`.
  defp url_path("index.html"), do: "/"

  defp url_path(rel) do
    if Path.basename(rel) == "index.html",
      do: "/" <> Path.dirname(rel) <> "/",
      else: "/" <> rel
  end

  defp xml_escape(text) do
    text
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace("\"", "&quot;")
    |> String.replace("'", "&apos;")
  end

  defp read!(root, name) do
    Malachi.SourceFile.read!(Path.join([root, @source, name]), "#{@source}/#{name}")
  end

  defp write!(dir, name, content) do
    path = Path.join(dir, name)
    File.mkdir_p!(Path.dirname(path))
    replace!(path, &File.write!(&1, content))
  end

  # Writes a file under a temporary name beside `path` and renames it into place, so `path` is never left
  # half-written. A failure removes the temporary file, so a failed run leaves no stray `.tmp` in doc/.
  defp replace!(path, write_to) do
    tmp = path <> ".tmp"

    try do
      write_to.(tmp)
      File.rename!(tmp, path)
    rescue
      error ->
        File.rm(tmp)
        reraise error, __STACKTRACE__
    end
  end
end
