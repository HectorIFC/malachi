defmodule WorktreeEnvTest do
  # scripts/worktree-env.sh gives every worktree its own ports, data directories, node name and compose
  # project, and the start-issue-work skill runs it for each new one. What matters is what it refuses:
  # the main checkout (whose compose volume a project name would orphan), an issue number whose ports
  # would land in the Linux ephemeral range, a port something already listens on, and rewriting a file a
  # session may already be running on. It runs for real here, against a throwaway repository with a
  # linked worktree, with a PATH whose `lsof` reports what the case needs.
  use ExUnit.Case, async: true

  alias Malachi.Test.DevCompose

  @script Path.expand("../../scripts/worktree-env.sh", __DIR__)
  @repo_root Path.expand("../..", __DIR__)

  setup do
    dir = Path.join(System.tmp_dir!(), "worktree-env-#{System.unique_integer([:positive])}")
    main = Path.join(dir, "main")
    File.mkdir_p!(main)
    on_exit(fn -> File.rm_rf!(dir) end)

    git!(main, ["init", "-q"])
    git!(main, ["-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-q", "--allow-empty", "-m", "x"])
    worktree = Path.join(dir, "wt")
    git!(main, ["worktree", "add", "-q", "-b", "wt", worktree])

    stub_bin = Path.join(dir, "stub-bin")
    File.mkdir_p!(stub_bin)
    stub_lsof!(stub_bin)

    # Resolved, as the script reports it: on macOS the temporary directory sits behind a symlink.
    %{dir: dir, main: main, worktree: resolve(worktree), stub_bin: stub_bin}
  end

  defp git!(cd, args) do
    {_out, 0} = System.cmd("git", args, cd: cd, stderr_to_stdout: true)
    :ok
  end

  defp resolve(path) do
    {out, 0} = System.cmd("pwd", ["-P"], cd: path)
    String.trim(out)
  end

  # An `lsof` that answers like the real one (`-Fpc`: a `p<pid>` line then a `c<command>` line) for the
  # one port named in STUB_TAKEN_PORT, and finds nothing listening anywhere else.
  defp stub_lsof!(stub_bin) do
    path = Path.join(stub_bin, "lsof")

    File.write!(path, """
    #!/bin/sh
    for arg in "$@"; do
      if [ -n "$STUB_TAKEN_PORT" ] && [ "$arg" = "-iTCP:$STUB_TAKEN_PORT" ]; then
        printf 'p4242\\ncholder-cmd\\n'
        exit 0
      fi
    done
    exit 1
    """)

    File.chmod!(path, 0o755)
  end

  defp run(ctx, issue, dir, opts \\ []) do
    path = Keyword.get(opts, :path, "#{ctx.stub_bin}:#{System.get_env("PATH")}")
    env = [{"PATH", path}, {"STUB_TAKEN_PORT", Keyword.get(opts, :taken, "")}]
    System.cmd("bash", [@script, to_string(issue), dir], env: env, stderr_to_stdout: true)
  end

  defp env_file(ctx), do: Path.join(ctx.worktree, "worktree.env")

  defp read_env(ctx) do
    for line <- ctx |> env_file() |> File.read!() |> String.split("\n", trim: true),
        not String.starts_with?(line, "#"),
        into: %{} do
      [key, value] = String.split(line, "=", parts: 2)
      {key, value}
    end
  end

  describe "a new worktree" do
    test "gets the issue's five ports, its data directories, node name and compose project", ctx do
      assert {output, 0} = run(ctx, 244, ctx.worktree)
      assert output =~ "dashboard: http://127.0.0.1:22441"

      assert read_env(ctx) == %{
               "MALACHI_TCP_PORT" => "22440",
               "MALACHI_DASHBOARD_PORT" => "22441",
               "JAEGER_UI_PORT" => "22442",
               "OTLP_PORT" => "22443",
               "PROMETHEUS_PORT" => "22444",
               "MALACHI_LOG_DATA_DIR" => Path.join(ctx.worktree, "tmp/data/log"),
               "MALACHI_RA_DATA_DIR" => Path.join(ctx.worktree, "tmp/data/ra"),
               "MALACHI_NODE" => "malachi_244@127.0.0.1",
               "COMPOSE_PROJECT_NAME" => "malachi-244"
             }
    end

    test "writes exactly the port variables the dev compose stack reads", ctx do
      assert {_output, 0} = run(ctx, 250, ctx.worktree)

      compose_vars = for mapping <- DevCompose.mappings(), {var, _default, _port} = DevCompose.parse(mapping), do: var
      written = ctx |> read_env() |> Map.keys() |> Enum.filter(&String.ends_with?(&1, "_PORT"))

      assert Enum.sort(written) == Enum.sort(compose_vars)
    end

    test "two issues get disjoint ports", ctx do
      assert {_output, 0} = run(ctx, 250, ctx.worktree)
      first = ctx |> read_env() |> Map.take(ports()) |> Map.values()
      File.rm!(env_file(ctx))

      assert {_output, 0} = run(ctx, 251, ctx.worktree)
      second = ctx |> read_env() |> Map.take(ports()) |> Map.values()

      assert MapSet.disjoint?(MapSet.new(first), MapSet.new(second))
    end

    test "works from a directory inside the worktree, writing at its root", ctx do
      sub = Path.join(ctx.worktree, "lib")
      File.mkdir_p!(sub)

      assert {_output, 0} = run(ctx, 250, sub)
      assert File.exists?(env_file(ctx))
    end
  end

  describe "refuses, writing nothing," do
    test "the main checkout", ctx do
      assert {output, 66} = run(ctx, 250, ctx.main)
      assert output =~ "main checkout"
      refute File.exists?(Path.join(ctx.main, "worktree.env"))
    end

    test "a directory outside any git checkout", ctx do
      outside = Path.join(ctx.dir, "outside")
      File.mkdir_p!(outside)

      assert {output, 66} = run(ctx, 250, outside)
      assert output =~ "not inside a git checkout"
    end

    test "an issue whose ports would reach the Linux ephemeral range, and not the last one below it", ctx do
      assert {output, 65} = run(ctx, 1277, ctx.worktree)
      assert output =~ "32770-32774"
      refute File.exists?(env_file(ctx))

      assert {_output, 0} = run(ctx, 1276, ctx.worktree)
      assert read_env(ctx)["PROMETHEUS_PORT"] == "32764"
    end

    test "an issue number that is not a positive integer", ctx do
      for bad <- ["abc", "0", "012", "-3", ""] do
        assert {output, 64} = run(ctx, bad, ctx.worktree)
        assert output =~ "positive integer", "accepted #{inspect(bad)}"
      end

      refute File.exists?(env_file(ctx))
    end

    test "a port something already listens on, naming what holds it", ctx do
      assert {output, 75} = run(ctx, 250, ctx.worktree, taken: "22503")
      assert output =~ "OTLP_PORT=22503 is held by holder-cmd (pid 4242)"
      refute File.exists?(env_file(ctx))
    end

    test "when there is no lsof to check the ports with", ctx do
      # A PATH holding only what the script needs before the check, so the host's lsof is out of reach.
      bare = Path.join(ctx.dir, "bare-bin")
      File.mkdir_p!(bare)

      for tool <- ~w(git awk sed mv) do
        File.ln_s!(System.find_executable(tool), Path.join(bare, tool))
      end

      assert {output, 69} = run(ctx, 250, ctx.worktree, path: bare)
      assert output =~ "lsof is required"
      refute File.exists?(env_file(ctx))
    end
  end

  describe "an existing worktree.env" do
    test "is kept as it is, even for another issue number", ctx do
      assert {_output, 0} = run(ctx, 250, ctx.worktree)
      before = File.read!(env_file(ctx))

      assert {output, 0} = run(ctx, 251, ctx.worktree)
      assert output =~ "kept existing"
      assert output =~ "dashboard: http://127.0.0.1:22501"
      assert File.read!(env_file(ctx)) == before
    end

    test "is kept when one of its ports is in use, with a warning naming the holder", ctx do
      assert {_output, 0} = run(ctx, 250, ctx.worktree)

      assert {output, 0} = run(ctx, 250, ctx.worktree, taken: "22501")
      assert output =~ "warning"
      assert output =~ "MALACHI_DASHBOARD_PORT=22501 is held by holder-cmd (pid 4242)"
    end
  end

  test "this repository ignores worktree.env and everything under tmp/" do
    for path <- ["worktree.env", "tmp/data/log/segment"] do
      assert {_out, 0} = System.cmd("git", ["check-ignore", "-q", path], cd: @repo_root), "#{path} is not ignored"
    end
  end

  defp ports, do: ~w(MALACHI_TCP_PORT MALACHI_DASHBOARD_PORT JAEGER_UI_PORT OTLP_PORT PROMETHEUS_PORT)
end
