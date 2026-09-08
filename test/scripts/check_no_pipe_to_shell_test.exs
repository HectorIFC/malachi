defmodule CheckNoPipeToShellTest do
  # The guard that keeps issue #69 from coming back. A grep-based check that is never exercised is
  # indistinguishable from a broken one: it stays green because it matches nothing. So both halves are
  # pinned here, the shapes it must reject and the near-misses it must not, plus a run over the real
  # repository so a future script that reintroduces the pattern fails this test and not only CI.
  use ExUnit.Case, async: true

  @guard Path.expand("../../scripts/check-no-pipe-to-shell.sh", __DIR__)
  @repo_root Path.expand("../..", __DIR__)

  setup do
    dir = Path.join(System.tmp_dir!(), "pipe-guard-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  describe "rejects" do
    test "a download piped into sudo bash, the exact shape issue #69 removed", ctx do
      file = fixture(ctx, "curl -1sLf 'https://example.com/setup.sh' | sudo -E bash")

      assert {output, status} = run([file])
      assert status == 1
      assert output =~ "piped into a shell interpreter"
      assert output =~ Path.basename(file)
    end

    test "wget piped into sh with no spaces around the pipe", ctx do
      file = fixture(ctx, "wget -qO- https://example.com/install.sh|sh")

      assert {_output, 1} = run([file])
    end

    test "an environment-prefixed interpreter", ctx do
      file = fixture(ctx, "curl -sL https://example.com/i | env FOO=1 bash -s -- --yes")

      assert {_output, 1} = run([file])
    end

    test "an interpreter written with an absolute path", ctx do
      # This one escaped the first version of the pattern, which required the command name to start
      # immediately after the pipe. Caught by review on PR #132.
      file = fixture(ctx, "curl -fsSL https://example.com/install | /bin/bash")

      assert {_output, 1} = run([file])
    end

    test "env with flags, and env itself written with a path", ctx do
      with_flags = fixture(ctx, "curl -sL https://example.com/i | env -i bash")
      with_path = fixture(ctx, "curl -sL https://example.com/i | /usr/bin/env sh")

      assert {_output, 1} = run([with_flags])
      assert {_output, 1} = run([with_path])
    end

    test "sudo in front of an absolute interpreter", ctx do
      file = fixture(ctx, "curl -sL https://example.com/i | sudo /bin/sh")

      assert {_output, 1} = run([file])
    end

    test "process substitution feeding the interpreter", ctx do
      file = fixture(ctx, "bash <(curl -sL https://example.com/install)")

      assert {_output, 1} = run([file])
    end
  end

  describe "accepts" do
    test "pipes that only look like the pattern", ctx do
      file =
        fixture(ctx, """
        curl -fsSL https://example.com/list -o out.txt | grep sh
        curl -s https://example.com/conf | sudo tee /etc/example.conf
        curl -fsSL "$url" -o "$dest" || die "download failed: $url"
        curl -s https://example.com/x | /usr/local/bin/fish
        curl -s https://example.com/x | tee /tmp/notes.sh
        """)

      assert {output, 0} = run([file])
      assert output =~ "no download is piped"
    end

    test "the repository's own scripts, Makefile and Dockerfile" do
      assert {output, 0} = run([])
      assert output =~ "files scanned"
    end
  end

  test "fails loudly rather than scanning nothing when a target does not exist" do
    assert {output, 1} = run([Path.join(System.tmp_dir!(), "no-such-script-#{System.unique_integer()}.sh")])
    assert output =~ "not a file"
  end

  defp fixture(ctx, body) do
    path = Path.join(ctx.dir, "fixture-#{System.unique_integer([:positive])}.sh")
    File.write!(path, "#!/bin/bash\n" <> body <> "\n")
    path
  end

  defp run(args) do
    System.cmd("bash", [@guard | args], cd: @repo_root, stderr_to_stdout: true)
  end
end
