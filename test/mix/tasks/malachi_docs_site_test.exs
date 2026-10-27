defmodule Mix.Tasks.Malachi.Docs.SiteTest do
  # Mix.shell/1 and File.cd!/2 are global process state, so this file is not async.
  use ExUnit.Case, async: false

  alias Mix.Tasks.Malachi.Docs.Site

  @moduletag :tmp_dir

  setup %{tmp_dir: root} do
    shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    on_exit(fn -> Mix.shell(shell) end)

    write(root, "site/index.html", ~s(<meta name="malachi:landing" content="1"><p>{{version}} {{license}}</p>))
    write(root, "site/llms.txt", "# Malachi\n\n> {{version}}\n")
    write(root, "site/fonts/bricolage.woff2", "font-bytes")
    write(root, "doc/introduction.html", "<h1>Introduction</h1>")
    write(root, "doc/index.html", ~s(<meta http-equiv="refresh" content="0; url=introduction.html">))

    %{root: root}
  end

  defp write(root, path, content) do
    full = Path.join(root, path)
    File.mkdir_p!(Path.dirname(full))
    File.write!(full, content)
  end

  defp messages do
    receive do
      {:mix_shell, :info, [message]} -> [message | messages()]
    after
      0 -> []
    end
  end

  test "stages the landing page with the version from mix.exs", %{root: root} do
    Site.run(["--root", root])

    version = Mix.Project.config()[:version]
    assert File.read!(Path.join(root, "doc/index.html")) =~ "#{version} MIT"
    assert messages() == ["staged the landing page at doc/index.html"]
  end

  test "without --root, stages into the current directory", %{root: root} do
    File.cd!(root, fn -> Site.run([]) end)

    assert File.read!(Path.join(root, "doc/index.html")) =~ "malachi:landing"
  end

  test "--check passes on a staged tree", %{root: root} do
    Site.run(["--root", root])

    # The stage message is drained first, so the only message left is the one --check prints.
    messages()

    assert Site.run(["--root", root, "--check"]) == :ok

    assert messages() == [
             "landing page: staged and filled; every sitemap URL and every link to this site in llms.txt resolves"
           ]
  end

  test "--check fails, listing the problem, when a linked page is gone", %{root: root} do
    Site.run(["--root", root])
    File.rm!(Path.join(root, "doc/sitemap.xml"))

    error = assert_raise Mix.Error, fn -> Site.run(["--root", root, "--check"]) end
    assert error.message =~ "doc/sitemap.xml is missing"
  end

  test "an argument the task does not take fails, rather than being ignored", %{root: root} do
    error = assert_raise Mix.Error, fn -> Site.run(["--root", root, "stray"]) end

    assert error.message == "unexpected arguments: stray"

    # The task stops before staging, so ExDoc's redirect is still in place.
    assert File.read!(Path.join(root, "doc/index.html")) =~ "refresh"
  end
end
