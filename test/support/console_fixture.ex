defmodule Malachi.Test.ConsoleFixture do
  @moduledoc """
  A console bundle on disk, shaped like a Vite build, for the console endpoint's tests, and a way to
  run an endpoint over it beside the application's.

  The bundle holds `index.html`, a hashed script under `assets/` with brotli and gzip variants, a
  stylesheet with only a gzip variant, a favicon, an orphan `.gz` with no uncompressed sibling, the
  directory's own `.gitignore`, a hidden directory, and two symbolic links (to a file and to a
  directory) that point outside the bundle. The variants hold distinct bytes, so a test can tell from
  the body which one was served.
  """

  import ExUnit.Callbacks, only: [on_exit: 1, start_supervised!: 1]

  alias Malachi.Console.Endpoint
  alias Malachi.Test.TmpDir

  @index "<!doctype html><title>console</title>"
  @script "console.log('app')"

  def index_body, do: @index
  def script_body, do: @script

  @doc "Writes the bundle into a fresh directory, removed when the test exits, and returns its path."
  @spec bundle!() :: Path.t()
  def bundle! do
    root = TmpDir.path("console_bundle")
    outside = TmpDir.path("console_outside")
    on_exit(fn -> Enum.each([root, outside], &File.rm_rf!/1) end)

    File.mkdir_p!(Path.join(root, "assets"))
    File.mkdir_p!(Path.join(root, ".hidden"))
    File.mkdir_p!(outside)

    write(root, "index.html", @index)
    write(root, "assets/app-3f2a.js", @script)
    write(root, "assets/app-3f2a.js.br", "BROTLI")
    write(root, "assets/app-3f2a.js.gz", "GZIP")
    write(root, "assets/app-3f2a.css", "body{}")
    write(root, "assets/app-3f2a.css.gz", "CSSGZIP")
    write(root, "favicon.ico", "ICO")
    write(root, "data.gz", "ORPHAN")
    write(root, ".gitignore", "*\n")
    write(root, ".hidden/secret.js", "hidden")

    write(outside, "secret.txt", "outside")
    File.ln_s!(Path.join(outside, "secret.txt"), Path.join(root, "linked.txt"))
    File.ln_s!(outside, Path.join(root, "assets/linked"))

    root
  end

  @doc "A directory with files but no index.html."
  @spec headless_bundle!() :: Path.t()
  def headless_bundle! do
    root = TmpDir.path("console_headless")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(Path.join(root, "assets"))
    write(root, "assets/app.js", @script)
    root
  end

  @doc """
  Starts an endpoint named `name` on an ephemeral port over `dir`, supervised by the test, and returns
  its port.
  """
  @spec start_endpoint!(atom(), Path.t()) :: :inet.port_number()
  def start_endpoint!(name, dir) do
    start_supervised!({Endpoint, {0, name: name, static_dir: dir}})
    Endpoint.port(name)
  end

  @doc """
  Makes `relative` under `dir` unreadable (mode 000) until the test exits.
  """
  @spec unreadable!(Path.t(), Path.t()) :: :ok
  def unreadable!(dir, relative) do
    path = Path.join(dir, relative)
    File.chmod!(path, 0o000)
    on_exit(fn -> File.chmod(path, 0o600) end)
  end

  @doc """
  Why a test that relies on an unreadable file must be skipped here, or false when it can run. Mode 000
  does not stop root from reading, so under root such a test would prove nothing.
  """
  @spec unreadable_skip() :: String.t() | false
  def unreadable_skip do
    probe = TmpDir.path("console_mode_probe")
    File.write!(probe, "x")
    File.chmod!(probe, 0o000)
    readable? = match?({:ok, _}, File.read(probe))
    File.chmod!(probe, 0o600)
    File.rm!(probe)
    if readable?, do: "running as root: mode 000 does not stop reads", else: false
  end

  defp write(root, relative, body), do: File.write!(Path.join(root, relative), body)
end
