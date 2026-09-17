defmodule PublishResultsTest do
  # scripts/publish-results.sh decides which measurement main holds after a push run of the results
  # workflow: a newer commit's result is never overwritten, a rerun replaces its own, an older one is
  # replaced, and a run with nothing left to write ends green. It used to be a rebase inside the
  # workflow, which stopped on those files as a conflict and failed the run with main already correct
  # (run 35147990781), and nothing tested it.
  #
  # Every case runs the real script against real git: a bare repository stands in for origin, a seed
  # clone writes main's history, and a runner clone is checked out at the commit a run measured, the way
  # the workflow's checkout is. Linux only, as the workflow is: the script needs bash 4.
  use ExUnit.Case, async: true

  @moduletag :linux
  @moduletag :tmp_dir

  @script Path.expand("../../scripts/publish-results.sh", __DIR__)
  @subject "chore(results): refresh published benchmark and chaos results from CI"

  setup_all do
    for tool <- ~w(bash git jq) do
      System.find_executable(tool) || flunk("#{tool} is required to test scripts/publish-results.sh")
    end

    :ok
  end

  setup %{tmp_dir: dir} do
    gitconfig = Path.join(dir, "gitconfig")
    File.write!(gitconfig, "[user]\n\tname = Test\n\temail = test@example.com\n[init]\n\tdefaultBranch = main\n")

    ctx = %{
      dir: dir,
      origin: Path.join(dir, "origin.git"),
      seed: Path.join(dir, "seed"),
      env: [{"GIT_CONFIG_GLOBAL", gitconfig}, {"GIT_CONFIG_NOSYSTEM", "1"}]
    }

    git!(ctx, dir, ["init", "--quiet", "--bare", "--initial-branch=main", ctx.origin])
    git!(ctx, dir, ["clone", "--quiet", ctx.origin, ctx.seed])

    # Three code commits, oldest first, before any result was ever published.
    shas =
      for n <- 1..3 do
        File.write!(Path.join(ctx.seed, "code.txt"), "version #{n}\n")
        git!(ctx, ctx.seed, ["add", "code.txt"])
        git!(ctx, ctx.seed, ["commit", "--quiet", "-m", "code #{n}"])
        git!(ctx, ctx.seed, ["rev-parse", "HEAD"])
      end

    git!(ctx, ctx.seed, ["push", "--quiet", "origin", "HEAD:main"])
    [c1, c2, c3] = shas
    Map.merge(ctx, %{c1: c1, c2: c2, c3: c3})
  end

  describe "refuses to run" do
    test "without PUBLISH_SHA", ctx do
      runner = runner_at(ctx, ctx.c2)
      assert {output, 1} = run_script(ctx, runner, [{"PUBLISH_SHA", nil}])
      assert output =~ "PUBLISH_SHA is required"
    end

    test "with a PUBLISH_SHA that is not a commit here", ctx do
      runner = runner_at(ctx, ctx.c2)
      assert {output, 1} = run_script(ctx, runner, [{"PUBLISH_SHA", "deadbeef"}])
      assert output =~ "PUBLISH_SHA deadbeef is not a commit in this repository"
    end

    for value <- ["0", "-1", "two"] do
      test "with PUBLISH_ATTEMPTS=#{value}", ctx do
        runner = runner_at(ctx, ctx.c2)
        assert {output, 1} = run_script(ctx, runner, [{"PUBLISH_SHA", ctx.c2}, {"PUBLISH_ATTEMPTS", unquote(value)}])
        assert output =~ "PUBLISH_ATTEMPTS must be a positive integer, got '#{unquote(value)}'"
      end
    end

    test "in a shallow checkout, which cannot order two commits", ctx do
      runner = Path.join(ctx.dir, "shallow")
      git!(ctx, ctx.dir, ["clone", "--quiet", "--depth", "1", "file://" <> ctx.origin, runner])
      write_results(runner, ctx.c3, %{"chaos-node.json" => "ours"})

      assert {output, 1} = run_script(ctx, runner, [{"PUBLISH_SHA", ctx.c3}])
      assert output =~ "the checkout is shallow"
      assert main_head(ctx) == ctx.c3
    end
  end

  describe "nothing to write" do
    test "when the collect step changed nothing", ctx do
      runner = runner_at(ctx, ctx.c2)
      assert {output, 0} = run_script(ctx, runner, [{"PUBLISH_SHA", ctx.c2}])
      assert output =~ "results are unchanged; nothing to commit"
      assert main_head(ctx) == ctx.c3
    end

    test "a deleted results file is not a result", ctx do
      publish_on_main(ctx, %{"chaos-node.json" => {ctx.c1, "published"}})
      runner = runner_at(ctx, main_head(ctx))
      File.rm!(Path.join([runner, "benchmark", "published", "chaos-node.json"]))

      assert {output, 0} = run_script(ctx, runner, [{"PUBLISH_SHA", ctx.c3}])
      assert output =~ "results are unchanged; nothing to commit"
      assert main_value(ctx, "chaos-node.json") == "published"
    end

    test "when main already holds a newer commit's results, it ends green and keeps them", ctx do
      publish_on_main(ctx, %{"chaos-node.json" => {ctx.c3, "newer"}})
      before = main_head(ctx)
      runner = runner_at(ctx, ctx.c2)
      write_results(runner, ctx.c2, %{"chaos-node.json" => "stale"})

      assert {output, 0} = run_script(ctx, runner, [{"PUBLISH_SHA", ctx.c2}])

      assert output =~
               "::notice::benchmark/published/chaos-node.json: main already holds a result for a newer commit (#{short(ctx.c3)}); keeping it"

      assert output =~ "::notice::nothing to publish: main already holds these results or newer ones"
      assert main_head(ctx) == before
      assert main_value(ctx, "chaos-node.json") == "newer"
    end

    test "when main's result is for a commit that cannot be ordered against this run's, it is kept", ctx do
      # A commit on another branch: neither it nor c3 contains the other.
      git!(ctx, ctx.seed, ["checkout", "--quiet", "-b", "side", ctx.c1])
      File.write!(Path.join(ctx.seed, "side.txt"), "side\n")
      git!(ctx, ctx.seed, ["add", "side.txt"])
      git!(ctx, ctx.seed, ["commit", "--quiet", "-m", "side"])
      side = git!(ctx, ctx.seed, ["rev-parse", "HEAD"])
      git!(ctx, ctx.seed, ["push", "--quiet", "origin", "side"])
      git!(ctx, ctx.seed, ["checkout", "--quiet", "main"])
      publish_on_main(ctx, %{"chaos-node.json" => {side, "sideways"}})

      runner = runner_at(ctx, ctx.c3)
      write_results(runner, ctx.c3, %{"chaos-node.json" => "ours"})

      assert {output, 0} = run_script(ctx, runner, [{"PUBLISH_SHA", ctx.c3}])
      assert output =~ "measured #{short(side)}, which cannot be ordered against #{short(ctx.c3)}; keeping it"
      assert main_value(ctx, "chaos-node.json") == "sideways"
    end
  end

  describe "writes this run's result" do
    test "when main has none, with the measured commit in the message", ctx do
      runner = runner_at(ctx, ctx.c3)
      write_results(runner, ctx.c3, %{"chaos-node.json" => "ours", "loadtest-node.json" => "ours too"})

      assert {output, 0} = run_script(ctx, runner, [{"PUBLISH_SHA", ctx.c3}])
      assert output =~ "benchmark/published/chaos-node.json: main has none; writing this run's result"
      assert output =~ "published the results of #{short(ctx.c3)} to main"

      assert main_value(ctx, "chaos-node.json") == "ours"
      assert main_value(ctx, "loadtest-node.json") == "ours too"
      assert main_message(ctx) == "#{@subject}\n\nMeasured on #{ctx.c3}."
      assert git!(ctx, ctx.origin, ["rev-parse", "main^"]) == ctx.c3
    end

    test "over main's result for an older commit", ctx do
      publish_on_main(ctx, %{"chaos-node.json" => {ctx.c1, "older"}})
      runner = runner_at(ctx, ctx.c3)
      write_results(runner, ctx.c3, %{"chaos-node.json" => "newer"})

      assert {output, 0} = run_script(ctx, runner, [{"PUBLISH_SHA", ctx.c3}])
      assert output =~ "main's copy measured an older commit (#{short(ctx.c1)}); writing this run's result"
      assert main_value(ctx, "chaos-node.json") == "newer"
    end

    test "over main's result for the same commit: a rerun replaces the earlier attempt", ctx do
      # Run 35147990781: attempt 1 published a failed certification, attempt 2 of the same commit passed.
      publish_on_main(ctx, %{"chaos-node.json" => {ctx.c3, "failed"}})
      runner = runner_at(ctx, ctx.c3)
      write_results(runner, ctx.c3, %{"chaos-node.json" => "passed"})

      assert {output, 0} = run_script(ctx, runner, [{"PUBLISH_SHA", ctx.c3}])
      assert output =~ "main's copy measured this same commit; this rerun replaces it"
      assert main_value(ctx, "chaos-node.json") == "passed"
    end

    test "when a rerun measured exactly what main holds, there is nothing to write", ctx do
      publish_on_main(ctx, %{"chaos-node.json" => {ctx.c3, "same"}})
      before = main_head(ctx)
      runner = runner_at(ctx, ctx.c3)
      write_results(runner, ctx.c3, %{"chaos-node.json" => "same"})

      assert {output, 0} = run_script(ctx, runner, [{"PUBLISH_SHA", ctx.c3}])
      assert output =~ "::notice::nothing to publish"
      assert main_head(ctx) == before
    end

    for {label, ref} <- [{"names no commit", nil}, {"names a commit this history lacks", "0badc0de"}] do
      test "over main's result when it #{label}", ctx do
        publish_on_main(ctx, %{"chaos-node.json" => {unquote(ref), "unknown"}})
        runner = runner_at(ctx, ctx.c2)
        write_results(runner, ctx.c2, %{"chaos-node.json" => "ours"})

        assert {output, 0} = run_script(ctx, runner, [{"PUBLISH_SHA", ctx.c2}])
        assert output =~ "main's copy names no commit this history has; writing this run's result"
        assert main_value(ctx, "chaos-node.json") == "ours"
      end
    end

    test "over main's copy that is not JSON at all", ctx do
      File.mkdir_p!(Path.join([ctx.seed, "benchmark", "published"]))
      File.write!(Path.join([ctx.seed, "benchmark", "published", "chaos-node.json"]), "{not json")
      git!(ctx, ctx.seed, ["add", "benchmark"])
      git!(ctx, ctx.seed, ["commit", "--quiet", "-m", "garbage"])
      git!(ctx, ctx.seed, ["push", "--quiet", "origin", "HEAD:main"])

      runner = runner_at(ctx, ctx.c3)
      write_results(runner, ctx.c3, %{"chaos-node.json" => "ours"})

      assert {_output, 0} = run_script(ctx, runner, [{"PUBLISH_SHA", ctx.c3}])
      assert main_value(ctx, "chaos-node.json") == "ours"
    end

    test "file by file: a newer result on main is kept while an older one is replaced", ctx do
      publish_on_main(ctx, %{
        "chaos-node.json" => {ctx.c3, "newer chaos"},
        "loadtest-node.json" => {ctx.c1, "older load"}
      })

      runner = runner_at(ctx, ctx.c2)
      write_results(runner, ctx.c2, %{"chaos-node.json" => "stale chaos", "loadtest-node.json" => "fresh load"})

      assert {output, 0} = run_script(ctx, runner, [{"PUBLISH_SHA", ctx.c2}])
      assert output =~ "chaos-node.json: main already holds a result for a newer commit"
      assert main_value(ctx, "chaos-node.json") == "newer chaos"
      assert main_value(ctx, "loadtest-node.json") == "fresh load"
    end

    test "on top of whatever main gained since the checkout, keeping it", ctx do
      runner = runner_at(ctx, ctx.c2)
      write_results(runner, ctx.c2, %{"chaos-node.json" => "ours"})
      publish_on_main(ctx, %{"loadtest-elixir.json" => {ctx.c3, "someone else's"}})
      before = main_head(ctx)

      assert {_output, 0} = run_script(ctx, runner, [{"PUBLISH_SHA", ctx.c2}])
      assert git!(ctx, ctx.origin, ["rev-parse", "main^"]) == before
      assert main_value(ctx, "chaos-node.json") == "ours"
      assert main_value(ctx, "loadtest-elixir.json") == "someone else's"
    end
  end

  describe "a push that loses" do
    test "a race is retried against the new main, and a newer result that landed in it is kept", ctx do
      # Prepared in the seed and pushed by the git wrapper just before the script's first push, so that
      # push loses to it exactly as it would to a newer run finishing first.
      publish_in_seed(ctx, %{"chaos-node.json" => {ctx.c3, "newer chaos"}})

      runner = runner_at(ctx, ctx.c2)
      write_results(runner, ctx.c2, %{"chaos-node.json" => "stale chaos", "loadtest-node.json" => "fresh load"})
      wrapper = racing_git(ctx)

      assert {output, 0} = run_script(ctx, runner, [{"PUBLISH_SHA", ctx.c2}, {"PATH", wrapper}])
      assert output =~ "push rejected (attempt 1); rebuilding on the current main"
      assert output =~ "chaos-node.json: main already holds a result for a newer commit"
      assert main_value(ctx, "chaos-node.json") == "newer chaos"
      assert main_value(ctx, "loadtest-node.json") == "fresh load"
    end

    test "fails after the attempts run out", ctx do
      hook = Path.join([ctx.origin, "hooks", "pre-receive"])
      File.write!(hook, "#!/usr/bin/env bash\necho rejected by the test >&2\nexit 1\n")
      File.chmod!(hook, 0o755)

      runner = runner_at(ctx, ctx.c3)
      write_results(runner, ctx.c3, %{"chaos-node.json" => "ours"})

      assert {output, 1} = run_script(ctx, runner, [{"PUBLISH_SHA", ctx.c3}, {"PUBLISH_ATTEMPTS", "2"}])
      assert output =~ "push rejected (attempt 1)"
      assert output =~ "push rejected (attempt 2)"
      assert output =~ "still could not push after 2 attempts"
      assert main_head(ctx) == ctx.c3
    end
  end

  # --- helpers ---

  defp git!(ctx, cwd, args) do
    case System.cmd("git", args, cd: cwd, env: ctx.env, stderr_to_stdout: true) do
      {output, 0} -> String.trim(output)
      {output, status} -> flunk("git #{Enum.join(args, " ")} exited #{status}: #{output}")
    end
  end

  defp short(sha), do: String.slice(sha, 0, 7)

  defp result(nil, value), do: Jason.encode!(%{"meta" => %{"git_ref" => nil}, "value" => value})
  defp result(ref, value), do: Jason.encode!(%{"meta" => %{"git_ref" => short(ref)}, "value" => value})

  # Commits `files` (name => {measured ref, value}) in the seed without pushing.
  defp publish_in_seed(ctx, files) do
    dir = Path.join([ctx.seed, "benchmark", "published"])
    File.mkdir_p!(dir)
    for {name, {ref, value}} <- files, do: File.write!(Path.join(dir, name), result(ref, value))
    git!(ctx, ctx.seed, ["add", "benchmark"])
    git!(ctx, ctx.seed, ["commit", "--quiet", "-m", @subject])
  end

  defp publish_on_main(ctx, files) do
    publish_in_seed(ctx, files)
    git!(ctx, ctx.seed, ["push", "--quiet", "origin", "HEAD:main"])
  end

  # A clone checked out at `sha`, as the workflow's checkout of the commit it measures.
  defp runner_at(ctx, sha) do
    runner = Path.join(ctx.dir, "runner-#{System.unique_integer([:positive])}")
    git!(ctx, ctx.dir, ["clone", "--quiet", ctx.origin, runner])
    git!(ctx, runner, ["checkout", "--quiet", "--detach", sha])
    runner
  end

  # This run's results, as the collect step leaves them: each names the commit the run measured.
  defp write_results(runner, sha, files) do
    dir = Path.join([runner, "benchmark", "published"])
    File.mkdir_p!(dir)
    for {name, value} <- files, do: File.write!(Path.join(dir, name), result(sha, value))
  end

  defp main_head(ctx), do: git!(ctx, ctx.origin, ["rev-parse", "main"])
  defp main_message(ctx), do: git!(ctx, ctx.origin, ["log", "-1", "--format=%B", "main"])

  defp main_value(ctx, name) do
    ctx |> git!(ctx.origin, ["show", "main:benchmark/published/#{name}"]) |> Jason.decode!() |> Map.fetch!("value")
  end

  # A PATH whose `git` pushes the seed's prepared commit to main right before the script's first push.
  defp racing_git(ctx) do
    bin = Path.join(ctx.dir, "racing-bin")
    File.mkdir_p!(bin)
    real = System.find_executable("git")
    marker = Path.join(ctx.dir, "raced")

    File.write!(Path.join(bin, "git"), """
    #!/usr/bin/env bash
    if [ "$1" = push ] && [ ! -e "#{marker}" ]; then
      : > "#{marker}"
      (cd "#{ctx.seed}" && "#{real}" push --quiet origin HEAD:main) || exit 97
    fi
    exec "#{real}" "$@"
    """)

    File.chmod!(Path.join(bin, "git"), 0o755)
    "#{bin}:#{System.get_env("PATH")}"
  end

  defp run_script(ctx, runner, env) do
    base = ctx.env ++ [{"PUBLISH_REMOTE", nil}, {"PUBLISH_BRANCH", nil}, {"PUBLISH_ATTEMPTS", nil}]
    env = Enum.reduce(env, base, fn {key, value}, acc -> List.keystore(acc, key, 0, {key, value}) end)
    System.cmd(System.find_executable("bash"), [@script], cd: runner, env: env, stderr_to_stdout: true)
  end
end
