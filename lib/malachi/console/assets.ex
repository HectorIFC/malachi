defmodule Malachi.Console.Assets do
  @moduledoc """
  The console bundle as the endpoint serves it: a manifest built once, when the endpoint starts, from
  the files under the static directory.

  The manifest is the whole serving surface. `Malachi.Console.Static` looks a request up in it by exact
  path and never turns a request into a filesystem path, so nothing outside the manifest can be served,
  whatever the request spells. Building it is where the rules about what counts as a servable file live:

    * a name starting with a dot (the directory's own `.gitignore` among them) is skipped, and so is
      everything below a directory whose name does;
    * a symbolic link is skipped, file or directory, so no link can reach outside the static root;
    * a file ending in `.br` or `.gz` whose uncompressed sibling exists is a precompressed variant of
      that sibling, not a file of its own; one without a sibling is an ordinary file;
    * every file and every variant is hashed with SHA-256, and the hash is its strong ETag. A hash of the
      content, not of size and modification time, is the same on every node of a cluster and across a
      redeploy of identical bytes.

  The set is fixed at startup: a file added later answers 404 until the endpoint restarts, which is what
  a release does anyway. A file that cannot be read when the manifest is built (its mode, its owner,
  or removed in between) is left out with a warning; a precompressed variant that cannot be read is
  left out of its file. With no readable `index.html` the bundle is `:absent` and the endpoint answers
  503. Nothing in a bundle can stop the endpoint, let alone the node, from starting.

  The manifest is handed to the plug as its options (`plug: {Malachi.Console.Router, opts}`), so no
  global state is involved. The price is memory: plug options are copied into every connection
  process, so each open connection holds its own copy of the manifest (large binaries excepted, which
  the VM shares), and the total grows with the number of files times the number of connections. It is
  bounded by `:console_max_connections`.
  """

  require Logger

  alias Malachi.I18n

  @typedoc "One servable representation of a file: where its bytes are, its ETag and its size."
  @type variant :: %{path: Path.t(), etag: String.t(), size: non_neg_integer()}

  @typedoc """
  One file: its content type, how browsers may cache it, and its representations, keyed by content
  coding (`"identity"`, and `"br"` or `"gzip"` when a precompressed variant was shipped).
  """
  @type entry :: %{
          content_type: String.t(),
          cache_control: String.t(),
          variants: %{required(String.t()) => variant()}
        }

  @type t :: %{files: %{required(String.t()) => entry()}, index: entry()} | :absent

  # Files under assets/ carry a content hash in their name (a Vite build puts it there), so a new
  # release never reuses a name for different bytes and a browser may keep them for a year without
  # asking. Everything else, index.html first, keeps its name across releases and is revalidated on
  # every use, which a matching ETag turns into a 304.
  @immutable "public, max-age=31536000, immutable"
  @revalidate "no-cache"

  # Brotli before gzip when a client accepts both equally; the order Malachi.Console.Static breaks ties in.
  @codings [{"br", ".br"}, {"gzip", ".gz"}]

  @read_chunk 65_536

  @doc "The directory the bundle is read from: `:console_static_dir`, else `priv/static/console`."
  @spec static_dir() :: Path.t()
  def static_dir do
    Application.get_env(:malachi, :console_static_dir) ||
      Application.app_dir(:malachi, "priv/static/console")
  end

  @doc """
  Builds the manifest for `dir`. A directory that does not exist, or one with no `index.html`, gives
  `:absent`, logged once as a warning.
  """
  @spec build(Path.t()) :: t()
  def build(dir) do
    files = dir |> regular_files() |> manifest_entries(dir)

    case Map.fetch(files, "index.html") do
      {:ok, index} ->
        %{files: files, index: index}

      :error ->
        Logger.warning(I18n.t(:console_bundle_absent, dir: dir))
        :absent
    end
  end

  @doc "The coding names a variant can carry, most preferred first."
  @spec codings() :: [String.t()]
  def codings, do: Enum.map(@codings, &elem(&1, 0))

  # Relative paths, with "/" separators, of every regular file below `dir` that is not hidden and not
  # reached through a symbolic link.
  defp regular_files(dir), do: walk(dir, "")

  defp walk(dir, relative) do
    case File.ls(Path.join(dir, relative)) do
      {:ok, names} -> names |> Enum.sort() |> Enum.flat_map(&walk_entry(dir, relative, &1))
      {:error, _} -> []
    end
  end

  defp walk_entry(_dir, _relative, "." <> _hidden), do: []

  defp walk_entry(dir, relative, name) do
    child = if relative == "", do: name, else: relative <> "/" <> name

    case File.lstat(Path.join(dir, child)) do
      {:ok, %File.Stat{type: :regular}} -> [child]
      {:ok, %File.Stat{type: :directory}} -> walk(dir, child)
      _symlink_or_other -> []
    end
  end

  defp manifest_entries(paths, dir) do
    present = MapSet.new(paths)

    for path <- paths,
        not variant_of_present_file?(path, present),
        {:ok, entry} <- [entry(path, dir, present)],
        into: %{} do
      {path, entry}
    end
  end

  defp variant_of_present_file?(path, present) do
    Enum.any?(@codings, fn {_coding, ext} ->
      String.ends_with?(path, ext) and MapSet.member?(present, String.replace_suffix(path, ext, ""))
    end)
  end

  # A file whose own bytes cannot be read is left out of the manifest; a precompressed variant that
  # cannot be read is left out of its file, which is then served uncompressed.
  defp entry(path, dir, present) do
    with {:ok, identity} <- variant(Path.join(dir, path)) do
      variants =
        for {coding, ext} <- @codings,
            MapSet.member?(present, path <> ext),
            {:ok, variant} <- [variant(Path.join(dir, path <> ext))],
            into: %{"identity" => identity} do
          {coding, variant}
        end

      {:ok,
       %{
         content_type: MIME.from_path(path),
         cache_control: if(String.starts_with?(path, "assets/"), do: @immutable, else: @revalidate),
         variants: variants
       }}
    end
  end

  # The file listing and the reads are separate moments, so a file can be unreadable (its mode, its
  # owner) or gone (a bundle being replaced) by the time it is hashed. That costs the file, logged,
  # never the endpoint and never the node.
  defp variant(absolute) do
    with {:ok, %File.Stat{size: size}} <- File.stat(absolute),
         {:ok, digest} <- sha256(absolute) do
      {:ok, %{path: absolute, etag: ~s("#{Base.url_encode64(digest, padding: false)}"), size: size}}
    else
      {:error, reason} ->
        Logger.warning(I18n.t(:console_asset_unreadable, path: absolute, reason: inspect(reason)))
        :error
    end
  end

  defp sha256(path) do
    with {:ok, io} <- File.open(path, [:read, :binary]) do
      try do
        hash_chunks(io, :crypto.hash_init(:sha256))
      after
        File.close(io)
      end
    end
  end

  defp hash_chunks(io, state) do
    case IO.binread(io, @read_chunk) do
      :eof -> {:ok, :crypto.hash_final(state)}
      {:error, reason} -> {:error, reason}
      chunk -> hash_chunks(io, :crypto.hash_update(state, chunk))
    end
  end
end
