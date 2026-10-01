defmodule Malachi.Console.AssetsTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Malachi.Console.Assets
  alias Malachi.Test.ConsoleFixture
  alias Malachi.Test.TmpDir

  setup do
    %{manifest: Assets.build(ConsoleFixture.bundle!())}
  end

  test "lists exactly the servable files, by relative path", %{manifest: manifest} do
    assert manifest.files |> Map.keys() |> Enum.sort() ==
             [
               "assets/app-3f2aB9x1.css",
               "assets/app-3f2aB9x1.js",
               "assets/logo.js",
               "data.gz",
               "favicon.ico",
               "index.html"
             ]
  end

  test "skips dot files and everything below a dot directory", %{manifest: manifest} do
    refute Map.has_key?(manifest.files, ".gitignore")
    refute Enum.any?(Map.keys(manifest.files), &String.contains?(&1, "secret"))
  end

  test "skips symbolic links, to files and to directories", %{manifest: manifest} do
    refute Map.has_key?(manifest.files, "linked.txt")
    refute Enum.any?(Map.keys(manifest.files), &String.starts_with?(&1, "assets/linked"))
  end

  test "folds precompressed siblings into the file they compress", %{manifest: manifest} do
    assert manifest.files["assets/app-3f2aB9x1.js"].variants |> Map.keys() |> Enum.sort() == ["br", "gzip", "identity"]
    assert manifest.files["assets/app-3f2aB9x1.css"].variants |> Map.keys() |> Enum.sort() == ["gzip", "identity"]
    refute Map.has_key?(manifest.files, "assets/app-3f2aB9x1.js.br")
    refute Map.has_key?(manifest.files, "assets/app-3f2aB9x1.js.gz")
  end

  test "keeps a .gz with no uncompressed sibling as a file of its own", %{manifest: manifest} do
    assert Map.keys(manifest.files["data.gz"].variants) == ["identity"]
  end

  test "each representation's ETag is the quoted SHA-256 of its bytes", %{manifest: manifest} do
    variants = manifest.files["assets/app-3f2aB9x1.js"].variants

    for {_coding, %{path: path, etag: etag, size: size}} <- variants do
      digest = :crypto.hash(:sha256, File.read!(path))
      assert etag == ~s("#{Base.url_encode64(digest, padding: false)}")
      assert size == byte_size(File.read!(path))
    end

    assert variants |> Map.values() |> Enum.map(& &1.etag) |> Enum.uniq() |> length() == 3
  end

  test "the same bytes give the same ETag in another directory", %{manifest: manifest} do
    again = Assets.build(ConsoleFixture.bundle!())

    assert again.files["index.html"].variants["identity"].etag ==
             manifest.files["index.html"].variants["identity"].etag
  end

  test "hashed assets are immutable and everything else revalidates", %{manifest: manifest} do
    assert manifest.files["assets/app-3f2aB9x1.js"].cache_control == "public, max-age=31536000, immutable"
    assert manifest.files["index.html"].cache_control == "no-cache"
    assert manifest.files["favicon.ico"].cache_control == "no-cache"
  end

  test "a file under assets/ without a build hash in its name revalidates", %{manifest: manifest} do
    # Replaced in place by the next release, it must not stay a year in a browser's cache.
    assert manifest.files["assets/logo.js"].cache_control == "no-cache"
  end

  test "the hash rule: a last segment of eight or more with a digit and no dash, under assets/ only" do
    # Six and seven character segments with a digit pin the lower bound at eight.
    dir = TmpDir.path("console_hash_names")
    on_exit(fn -> File.rm_rf!(dir) end)
    File.mkdir_p!(Path.join(dir, "assets"))
    File.write!(Path.join(dir, "index.html"), "x")

    names = %{
      "assets/index-B9x1C4d5.js" => true,
      "assets/vendor-react-Ab12cd34.js" => true,
      "assets/chunk.Q2w3e4r5.css" => true,
      "assets/vendor-manifest.js" => false,
      "assets/roboto-latin-400.woff2" => false,
      "assets/icon-arrow-2x.png" => false,
      "assets/font-inter-v3.woff2" => false,
      "assets/chart-v3-bundle.js" => false,
      "assets/index-BqWxYzAb.js" => false,
      "assets/index-Ab-9xYz1.js" => false,
      "assets/app-3f2a1b.js" => false,
      "assets/icon-retina2.png" => false,
      "assets/app-3f2a.js" => false,
      "assets/app.js" => false,
      "top-B9x1C4d5.js" => false
    }

    for {name, _} <- names, do: File.write!(Path.join(dir, name), name)
    manifest = Assets.build(dir)

    for {name, immutable?} <- names do
      expected = if immutable?, do: "public, max-age=31536000, immutable", else: "no-cache"
      assert manifest.files[name].cache_control == expected, name
    end
  end

  test "content types come from the extension", %{manifest: manifest} do
    assert manifest.files["index.html"].content_type == "text/html"
    assert manifest.files["assets/app-3f2aB9x1.js"].content_type == "text/javascript"
    assert manifest.files["assets/app-3f2aB9x1.css"].content_type == "text/css"
  end

  test "the index entry is the index.html entry", %{manifest: manifest} do
    assert manifest.index == manifest.files["index.html"]
  end

  test "a directory with no index.html is absent, and says so once" do
    dir = ConsoleFixture.headless_bundle!()
    log = capture_log(fn -> assert Assets.build(dir) == :absent end)
    assert log =~ dir
    assert log =~ "index.html"
  end

  test "a directory that does not exist is absent" do
    dir = TmpDir.path("console_missing")
    capture_log(fn -> assert Assets.build(dir) == :absent end)
  end

  test "static_dir/0 follows :console_static_dir" do
    # config/test.exs sets it, so no test serves a bundle someone built into priv/static/console.
    assert Assets.static_dir() == Application.fetch_env!(:malachi, :console_static_dir)
  end

  describe "files that cannot be read" do
    @describetag skip: ConsoleFixture.unreadable_skip()

    test "an unreadable file is left out and named in a warning, the rest is served" do
      dir = ConsoleFixture.bundle!()
      ConsoleFixture.unreadable!(dir, "favicon.ico")

      log = capture_log(fn -> send(self(), {:manifest, Assets.build(dir)}) end)
      assert_received {:manifest, manifest}

      refute Map.has_key?(manifest.files, "favicon.ico")
      assert Map.has_key?(manifest.files, "assets/app-3f2aB9x1.js")
      assert log =~ "favicon.ico"
      assert log =~ "eacces"
    end

    test "an unreadable precompressed variant is left out of its file, which is still served" do
      dir = ConsoleFixture.bundle!()
      ConsoleFixture.unreadable!(dir, "assets/app-3f2aB9x1.js.br")

      capture_log(fn -> send(self(), {:manifest, Assets.build(dir)}) end)
      assert_received {:manifest, manifest}

      assert manifest.files["assets/app-3f2aB9x1.js"].variants |> Map.keys() |> Enum.sort() == ["gzip", "identity"]
    end

    test "an unreadable index.html makes the bundle absent" do
      dir = ConsoleFixture.bundle!()
      ConsoleFixture.unreadable!(dir, "index.html")

      capture_log(fn -> send(self(), {:manifest, Assets.build(dir)}) end)
      assert_received {:manifest, :absent}
    end
  end

  test "unchanged?/1 holds for a file left alone and not for one rewritten", %{manifest: manifest} do
    variant = manifest.files["favicon.ico"].variants["identity"]
    assert Assets.unchanged?(variant)

    File.write!(variant.path, "A DIFFERENT ICON")
    refute Assets.unchanged?(variant)
  end

  test "codings/0 lists brotli before gzip" do
    assert Assets.codings() == ["br", "gzip"]
  end
end
