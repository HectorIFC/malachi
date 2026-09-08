defmodule SetupDevTest do
  # scripts/setup-dev.sh is what every contributor runs right after cloning, and it used to pipe a remote
  # script into `sudo bash` (issue #69). What replaced it is a checksum-verified download, so the cases that
  # matter are the ones where verification must REFUSE: a tampered artifact, a missing checksum line, a
  # machine with no hashing tool. Each of those is a way for the check to quietly degrade into decoration.
  #
  # The script runs for real here, in a throwaway tree, with a PATH whose `curl` serves a local fixture. The
  # fixture is a fake lefthook (a shell script that answers `version` and `install`) whose hash the test
  # computes itself, so the comparison being exercised is the script's own, not a mock of it. No network, no
  # sudo, nothing written outside the temporary directory.
  use ExUnit.Case, async: true

  @script Path.expand("../../scripts/setup-dev.sh", __DIR__)

  # The pinned version lives in the script. Reading it back keeps these tests correct across a version bump
  # instead of repeating the number in a second place that would drift.
  @version @script |> File.read!() |> then(&Regex.run(~r/^LEFTHOOK_VERSION=(\S+)$/m, &1)) |> Enum.at(1)

  # Every platform the script knows about gets the same fixture hash, so the tests never have to restate the
  # uname-to-asset mapping: whichever asset this machine asks for, the table has it.
  @platforms ~w(MacOS_arm64 MacOS_x86_64 Linux_x86_64 Linux_aarch64)

  # What setup-dev.sh needs to reach the verification step, minus the two hashing tools.
  @minimal_tools ~w(bash sh env dirname uname mktemp awk gunzip gzip mkdir mv cp rm chmod head cut cat)

  setup do
    dir = Path.join(System.tmp_dir!(), "setup-dev-#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(dir, "scripts"))
    File.mkdir_p!(Path.join(dir, "stub-bin"))
    File.cp!(@script, Path.join([dir, "scripts", "setup-dev.sh"]))
    on_exit(fn -> File.rm_rf!(dir) end)

    raw = fake_lefthook()
    gz = :zlib.gzip(raw)
    gz_path = Path.join(dir, "fixture.gz")
    File.write!(gz_path, gz)

    # No `git init`: the installed binary is a fake, so `lefthook install` here proves the script invoked the
    # binary it verified, not that git accepted a hook. That the real hook fires is checked by hand on macOS
    # and in a clean Linux container, which a stub could not prove anyway.
    %{dir: dir, raw: raw, gz: gz, gz_path: gz_path}
  end

  describe "verified install" do
    test "installs the pinned binary and runs it when the checksum matches", ctx do
      write_checksums!(ctx, ctx.raw, ctx.gz)
      stub_curl!(ctx, :serve)

      assert {output, 0} = run(ctx)
      assert output =~ "verified and installed"

      bin = Path.join(ctx.dir, ".lefthook/bin/lefthook")
      assert File.exists?(bin)
      assert executable?(bin)

      # The marker is written by the fixture's `install` branch: the script installed the binary AND used it.
      assert File.exists?(Path.join(ctx.dir, ".lefthook-installed"))
    end

    test "does not download again when the pinned version is already installed", ctx do
      write_checksums!(ctx, ctx.raw, ctx.gz)
      stub_curl!(ctx, :forbidden)

      bin = Path.join(ctx.dir, ".lefthook/bin/lefthook")
      File.mkdir_p!(Path.dirname(bin))
      File.write!(bin, ctx.raw)
      File.chmod!(bin, 0o755)

      assert {output, 0} = run(ctx)
      assert output =~ "already installed"
      refute File.exists?(Path.join(ctx.dir, "curl-was-called"))
    end
  end

  describe "refusals" do
    test "aborts and installs nothing when the checksum does not match", ctx do
      write_checksums!(ctx, "a different artifact entirely", :zlib.gzip("also different"))
      stub_curl!(ctx, :serve)

      assert {output, status} = run(ctx)
      assert status != 0
      assert output =~ "checksum mismatch"
      refute File.exists?(Path.join(ctx.dir, ".lefthook/bin/lefthook"))
      refute File.exists?(Path.join(ctx.dir, ".lefthook-installed"))
    end

    test "replaces a tampered cached binary instead of running it", ctx do
      # The cached-binary fast path used to accept whatever `.lefthook/bin/lefthook` reported as its
      # version, which meant EXECUTING an unverified binary to ask. Caught by review on PR #132. The
      # fixture writes a marker on any invocation, so the assertion is that it never ran at all.
      write_checksums!(ctx, ctx.raw, ctx.gz)
      stub_curl!(ctx, :serve)

      bin = Path.join(ctx.dir, ".lefthook/bin/lefthook")
      File.mkdir_p!(Path.dirname(bin))

      File.write!(bin, """
      #!/bin/sh
      : > tampered-was-executed
      echo "#{@version}"
      """)

      File.chmod!(bin, 0o755)

      assert {output, 0} = run(ctx)
      assert output =~ "does not match the pinned release"
      refute File.exists?(Path.join(ctx.dir, "tampered-was-executed"))
      assert File.read!(bin) == fake_lefthook()
    end

    test "aborts when the checksum table has no line for the asset", ctx do
      File.write!(checksums_path(ctx), "#{sha256(ctx.gz)}  some_unrelated_artifact.gz\n")
      stub_curl!(ctx, :serve)

      assert {output, status} = run(ctx)
      assert status != 0
      assert output =~ "no checksum recorded"
      refute File.exists?(Path.join(ctx.dir, ".lefthook/bin/lefthook"))
    end

    test "aborts when the download fails", ctx do
      write_checksums!(ctx, ctx.raw, ctx.gz)
      stub_curl!(ctx, :fail)

      assert {output, status} = run(ctx)
      assert status != 0
      assert output =~ "download failed"
      refute File.exists?(Path.join(ctx.dir, ".lefthook/bin/lefthook"))
    end

    test "aborts with an actionable message on an unsupported platform", ctx do
      write_checksums!(ctx, ctx.raw, ctx.gz)
      stub_curl!(ctx, :serve)

      write_stub!(ctx, "uname", ~S"""
      #!/bin/sh
      case "$1" in
        -s) echo "Plan9" ;;
        -m) echo "sparc64" ;;
      esac
      """)

      assert {output, status} = run(ctx)
      assert status != 0
      assert output =~ "unsupported platform Plan9 sparc64"
      assert output =~ "lefthook install"
    end

    test "aborts instead of installing unverified when no sha256 tool exists", ctx do
      write_checksums!(ctx, ctx.raw, ctx.gz)
      stub_curl!(ctx, :serve)

      assert {output, status} = run(ctx, path: [stub_dir(ctx), minimal_bin!(ctx)])
      assert status != 0
      assert output =~ "no sha256 tool found"
      refute File.exists?(Path.join(ctx.dir, ".lefthook/bin/lefthook"))
    end
  end

  describe "hook binary pinning" do
    # Measured, not assumed: the hook Lefthook generates looks for `lefthook` on PATH BEFORE the binary in
    # the clone, so without scripts/lefthook-rc.sh a contributor with another Lefthook installed would run
    # hooks through that one and the pinned version would be decorative.
    test "the rc file points the hook at the verified binary when it is installed", ctx do
      repo = git_repo!(ctx)
      bin = Path.join(repo, ".lefthook/bin/lefthook")
      File.mkdir_p!(Path.dirname(bin))
      File.write!(bin, fake_lefthook())
      File.chmod!(bin, 0o755)

      # Compared by content rather than by path string: on macOS `git rev-parse` reports the /private
      # realpath, and what matters is that LEFTHOOK_BIN names the binary we installed.
      exported = source_rc(repo)
      assert String.ends_with?(exported, "/.lefthook/bin/lefthook")
      assert File.read!(exported) == File.read!(bin)
    end

    test "the rc file leaves LEFTHOOK_BIN unset when no pinned binary exists", ctx do
      repo = git_repo!(ctx)

      assert source_rc(repo) == ""
    end

    test "lefthook.yml references an rc file that exists" do
      config = File.read!(Path.expand("../../lefthook.yml", __DIR__))
      assert [_, rc] = Regex.run(~r/^rc:\s*(\S+)$/m, config)
      assert File.exists?(Path.expand("../../#{rc}", __DIR__))
    end
  end

  defp git_repo!(ctx) do
    repo = Path.join(ctx.dir, "clone-#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(repo, "scripts"))
    File.cp!(Path.expand("../../scripts/lefthook-rc.sh", __DIR__), Path.join([repo, "scripts", "lefthook-rc.sh"]))
    {_, 0} = System.cmd("git", ["init", "--quiet", repo], stderr_to_stdout: true)
    repo
  end

  # Sources the rc the same way the generated hook does and reports what LEFTHOOK_BIN ended up as.
  defp source_rc(repo) do
    {out, 0} =
      System.cmd("sh", ["-c", ". scripts/lefthook-rc.sh; printf '%s' \"$LEFTHOOK_BIN\""],
        cd: repo,
        stderr_to_stdout: true
      )

    out
  end

  defp run(ctx, opts \\ []) do
    path = opts |> Keyword.get(:path, [stub_dir(ctx), System.get_env("PATH")]) |> Enum.join(":")

    System.cmd("bash", ["scripts/setup-dev.sh"],
      cd: ctx.dir,
      env: [{"PATH", path}],
      stderr_to_stdout: true
    )
  end

  defp fake_lefthook do
    """
    #!/bin/sh
    case "$1" in
      version) echo "#{@version}" ;;
      install) echo "hooks installed"; : > .lefthook-installed ;;
      *) echo "unexpected lefthook invocation: $*" >&2; exit 2 ;;
    esac
    """
  end

  defp write_checksums!(ctx, raw, gz) do
    lines =
      Enum.flat_map(@platforms, fn platform ->
        asset = "lefthook_#{@version}_#{platform}"
        ["#{sha256(raw)}  #{asset}", "#{sha256(gz)}  #{asset}.gz"]
      end)

    File.write!(checksums_path(ctx), Enum.join(lines, "\n") <> "\n")
  end

  defp checksums_path(ctx), do: Path.join([ctx.dir, "scripts", "lefthook.checksums"])

  defp stub_dir(ctx), do: Path.join(ctx.dir, "stub-bin")

  # :serve copies the fixture to wherever -o points, :fail exits like curl does on a 404, and :forbidden
  # leaves evidence behind so a test can assert the download never happened.
  defp stub_curl!(ctx, :serve) do
    write_stub!(ctx, "curl", ~s"""
    #!/bin/sh
    out=""
    prev=""
    for a in "$@"; do
      if [ "$prev" = "-o" ]; then out="$a"; fi
      prev="$a"
    done
    [ -n "$out" ] || { echo "stub curl: no -o argument" >&2; exit 2; }
    cp "#{ctx.gz_path}" "$out"
    """)
  end

  defp stub_curl!(ctx, :fail) do
    write_stub!(ctx, "curl", ~S"""
    #!/bin/sh
    echo "stub curl: simulated HTTP 404" >&2
    exit 22
    """)
  end

  defp stub_curl!(ctx, :forbidden) do
    write_stub!(ctx, "curl", ~s"""
    #!/bin/sh
    : > "#{ctx.dir}/curl-was-called"
    exit 22
    """)
  end

  defp write_stub!(ctx, name, body) do
    path = Path.join(stub_dir(ctx), name)
    File.write!(path, body)
    File.chmod!(path, 0o755)
    path
  end

  # A PATH that can download and unpack but cannot hash.
  defp minimal_bin!(ctx) do
    bin = Path.join(ctx.dir, "minimal-bin")
    File.mkdir_p!(bin)

    for tool <- @minimal_tools, path = System.find_executable(tool) do
      File.ln_s!(path, Path.join(bin, tool))
    end

    bin
  end

  defp sha256(content), do: :sha256 |> :crypto.hash(content) |> Base.encode16(case: :lower)

  defp executable?(path) do
    {:ok, %File.Stat{mode: mode}} = File.stat(path)
    Bitwise.band(mode, 0o100) != 0
  end
end
