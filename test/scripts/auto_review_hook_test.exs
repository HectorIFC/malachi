defmodule AutoReviewHookTest do
  # scripts/auto-review-hook.sh is the Claude Code Stop hook. It reminds the contributor, once, that a branch
  # with commits ahead of origin/main has never been reviewed, and it never blocks a turn: a blocking hook is
  # what kept a review going for days. Its silent cases keep it from nagging, and the round count it asks
  # review-state.sh is what makes the reminder stop. It runs for real here, against throwaway repositories.
  use ExUnit.Case, async: true

  alias Malachi.Test.TmpDir

  @script Path.expand("../../scripts/auto-review-hook.sh", __DIR__)
  @state Path.expand("../../scripts/review-state.sh", __DIR__)
  @repo_root Path.expand("../..", __DIR__)

  # Unset in every command this file runs, for the reason `WorktreeEnvTest` gives: git reads them ahead of
  # `cd`, so a suite started from a git hook would otherwise build its fixtures in another repository.
  @no_git_env [{"GIT_DIR", nil}, {"GIT_WORK_TREE", nil}, {"GIT_COMMON_DIR", nil}, {"GIT_INDEX_FILE", nil}]

  setup do
    dir = TmpDir.path("auto-review-hook")
    File.mkdir!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    repo = Path.join(dir, "repo")
    File.mkdir!(repo)
    git!(repo, ["init", "-q", "-b", "main"])
    File.write!(Path.join(repo, "a.ex"), "a\n")
    commit!(repo, "base")
    # What a clone has: origin/main at the commit the branch left from.
    git!(repo, ["update-ref", "refs/remotes/origin/main", "HEAD"])
    git!(repo, ["checkout", "-q", "-b", "feature"])

    %{dir: dir, repo: repo}
  end

  describe "reminds once" do
    test "a branch with commits ahead of origin/main gets one reminder, and it never blocks", ctx do
      commit_feature!(ctx)

      assert %{"systemMessage" => message} = spoken(run(ctx))
      assert message =~ "adversarial-review"
      assert message =~ "MALACHI_SKIP_AUTO_REVIEW=1"
      refute message =~ "\""
    end

    test "over four turns that each change content, it speaks once and then stays silent", ctx do
      # Each turn here is a fix to the work after the reminder. The hook that blocked on every change
      # would answer each of these turns with a request for another review.
      commit_feature!(ctx)
      assert %{"systemMessage" => _} = spoken(run(ctx))

      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")
      assert run(ctx) == {"", 0}

      File.write!(Path.join(ctx.repo, "a.ex"), "changed again\n")
      assert run(ctx) == {"", 0}

      File.write!(Path.join(ctx.repo, "new.ex"), "new\n")
      assert run(ctx) == {"", 0}

      commit!(ctx.repo, "more work")
      assert run(ctx) == {"", 0}
    end

    test "a branch that has had a round is not reminded at all", ctx do
      commit_feature!(ctx)
      {_, 0} = System.cmd("bash", [@state, "begin"], cd: ctx.repo, env: @no_git_env, stderr_to_stdout: true)

      assert run(ctx) == {"", 0}
    end
  end

  describe "stays silent" do
    test "on the stop that follows its own message, so it cannot loop", ctx do
      commit_feature!(ctx)

      assert run(ctx, input: ~s({"hook_event_name":"Stop","stop_hook_active": true})) == {"", 0}
    end

    test "when the contributor opted out", ctx do
      commit_feature!(ctx)

      assert run(ctx, env: [{"MALACHI_SKIP_AUTO_REVIEW", "1"}]) == {"", 0}
    end

    test "in plan mode, where nothing is implemented yet", ctx do
      commit_feature!(ctx)

      assert run(ctx, input: ~s({"stop_hook_active":false,"permission_mode":"plan"})) == {"", 0}
    end

    test "on a branch with no commits of its own, however much is uncommitted", ctx do
      # The reminder is about work committed on the branch. Uncommitted work is for the review the
      # contributor asks for by name, or for prepare-commits, which starts the first round.
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")
      File.write!(Path.join(ctx.repo, "new.ex"), "new\n")

      assert run(ctx) == {"", 0}
    end

    test "on a branch with nothing changed at all", ctx do
      assert run(ctx) == {"", 0}
    end

    test "outside a git work tree", ctx do
      plain = Path.join(ctx.dir, "plain")
      File.mkdir!(plain)

      assert run(ctx, project: plain) == {"", 0}
    end

    test "when the project directory does not exist", ctx do
      assert run(ctx, project: Path.join(ctx.dir, "missing")) == {"", 0}
    end

    test "without an origin/main to compare with", ctx do
      commit_feature!(ctx)
      git!(ctx.repo, ["update-ref", "-d", "refs/remotes/origin/main"])

      assert run(ctx) == {"", 0}
    end

    test "ignores a GIT_DIR inherited from a git hook and looks at the project", ctx do
      # The project has commits ahead. Read through another repository's GIT_DIR, the hook would look at a
      # repository with no origin/main and say nothing for the wrong reason.
      elsewhere = Path.join(ctx.dir, "elsewhere")
      File.mkdir!(elsewhere)
      git!(elsewhere, ["init", "-q"])
      File.write!(Path.join(elsewhere, "x.ex"), "x\n")
      commit!(elsewhere, "unrelated")

      commit_feature!(ctx)
      assert %{"systemMessage" => _} = spoken(run(ctx, env: [{"GIT_DIR", Path.join(elsewhere, ".git")}]))
    end
  end

  test "the repository's project settings run this script on Stop" do
    settings = @repo_root |> Path.join(".claude/settings.json") |> File.read!() |> Jason.decode!()

    commands =
      for group <- settings["hooks"]["Stop"], hook <- group["hooks"], hook["type"] == "command", do: hook["command"]

    assert ~s("$CLAUDE_PROJECT_DIR"/scripts/auto-review-hook.sh) in commands

    # The hook runs the script directly, so it has to be executable in the checkout.
    assert %File.Stat{mode: mode} = @repo_root |> Path.join("scripts/auto-review-hook.sh") |> File.stat!()
    assert Bitwise.band(mode, 0o111) != 0
  end

  test "the state script the hook asks is executable in the checkout" do
    assert %File.Stat{mode: mode} = File.stat!(@state)
    assert Bitwise.band(mode, 0o111) != 0
  end

  defp commit_feature!(ctx) do
    File.write!(Path.join(ctx.repo, "b.ex"), "b\n")
    commit!(ctx.repo, "feature work")
  end

  defp run(ctx, opts \\ []) do
    input = Keyword.get(opts, :input, ~s({"hook_event_name":"Stop","stop_hook_active":false}))
    input_file = Path.join(ctx.dir, "input-#{System.unique_integer([:positive])}.json")
    File.write!(input_file, input)

    env =
      Map.new(@no_git_env)
      |> Map.merge(%{"CLAUDE_PROJECT_DIR" => Keyword.get(opts, :project, ctx.repo), "MALACHI_SKIP_AUTO_REVIEW" => nil})
      |> Map.merge(Map.new(Keyword.get(opts, :env, [])))
      |> Enum.to_list()

    System.cmd("bash", ["-c", ~s(bash "$0" < "$1"), @script, input_file], env: env, stderr_to_stdout: true)
  end

  # What the hook printed, decoded. Output that is not one JSON object is a failure of the hook.
  defp spoken({output, 0}), do: Jason.decode!(output)

  defp commit!(repo, message) do
    git!(repo, ["add", "-A"])
    git!(repo, ["-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-q", "-m", message])
  end

  defp git!(cd, args) do
    {_out, 0} = System.cmd("git", args, cd: cd, env: @no_git_env, stderr_to_stdout: true)
    :ok
  end
end
