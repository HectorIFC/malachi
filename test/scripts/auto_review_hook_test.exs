defmodule AutoReviewHookTest do
  # scripts/auto-review-hook.sh is the Claude Code Stop hook that asks for an adversarial review when a
  # turn ends with the branch carrying changes. Its whole contract is when it speaks and when it stays
  # silent, and the silent cases are what keep it from looping or nagging: the stop that follows its own
  # request, the opt-out, plan mode, a clean branch, and the files the prepare-commits skill generates.
  # It runs for real here, against throwaway repositories.
  use ExUnit.Case, async: true

  alias Malachi.Test.TmpDir

  @script Path.expand("../../scripts/auto-review-hook.sh", __DIR__)
  @repo_root Path.expand("../..", __DIR__)

  # Unset in every command this file runs, for the reason `WorktreeEnvTest` gives: git reads them ahead
  # of `cd`, so a suite started from a git hook would otherwise build its fixtures in another repository.
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

  describe "asks for a review when the branch carries" do
    test "a modified tracked file", ctx do
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")

      assert %{"decision" => "block", "reason" => reason} = blocked(run(ctx))
      assert reason =~ "1 changed file(s)"
      assert reason =~ "adversarial-review"
      assert reason =~ "MALACHI_SKIP_AUTO_REVIEW=1"
    end

    test "an untracked file", ctx do
      File.write!(Path.join(ctx.repo, "new.ex"), "new\n")

      assert %{"decision" => "block"} = blocked(run(ctx))
    end

    test "a commit ahead of origin/main, with a clean working tree", ctx do
      File.write!(Path.join(ctx.repo, "b.ex"), "b\n")
      commit!(ctx.repo, "feature work")

      assert %{"reason" => reason} = blocked(run(ctx))
      assert reason =~ "1 changed file(s)"
    end

    test "a stop whose input says stop_hook_active is false", ctx do
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")

      assert %{"decision" => "block"} = blocked(run(ctx, input: ~s({"stop_hook_active": false})))
    end
  end

  describe "stays silent" do
    test "on the stop that follows its own request, so it never loops", ctx do
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")

      assert run(ctx, input: ~s({"hook_event_name":"Stop","stop_hook_active": true})) == {"", 0}
    end

    test "when the contributor opted out", ctx do
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")

      assert run(ctx, env: [{"MALACHI_SKIP_AUTO_REVIEW", "1"}]) == {"", 0}
    end

    test "in plan mode, where nothing is implemented yet", ctx do
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")

      assert run(ctx, input: ~s({"stop_hook_active":false,"permission_mode":"plan"})) == {"", 0}
    end

    test "on a branch with no changes", ctx do
      assert run(ctx) == {"", 0}
    end

    test "when the only files are the ones prepare-commits generates", ctx do
      for name <- ["commit_message.sh", "commit_1.patch", "commit_12.patch"] do
        File.write!(Path.join(ctx.repo, name), "generated\n")
      end

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
  end

  describe "asks once per diff" do
    test "a turn that changed nothing does not start the same review again", ctx do
      # The turn that answers the review's options, a question, a turn opened by a background task
      # finishing: the branch still differs from origin/main, but the review already covered it.
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")

      assert %{"decision" => "block"} = blocked(run(ctx))
      assert run(ctx) == {"", 0}
    end

    test "committing reviewed work does not start the same review again", ctx do
      # A commit moves new files from untracked to committed without changing a byte of them. The
      # review already covered those bytes, so asking again would review the same code twice.
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")
      File.write!(Path.join(ctx.repo, "new.ex"), "new\n")
      assert %{"decision" => "block"} = blocked(run(ctx))

      commit!(ctx.repo, "reviewed work")
      assert run(ctx) == {"", 0}
    end

    test "the contributor's own index is left as it was", ctx do
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")
      File.write!(Path.join(ctx.repo, "new.ex"), "new\n")
      git!(ctx.repo, ["add", "a.ex"])

      assert %{"decision" => "block"} = blocked(run(ctx))

      {status, 0} = System.cmd("git", ["status", "--porcelain"], cd: ctx.repo, env: @no_git_env)
      assert status == "M  a.ex\n?? new.ex\n"
    end

    test "a change to a tracked file asks again", ctx do
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")
      assert %{"decision" => "block"} = blocked(run(ctx))

      File.write!(Path.join(ctx.repo, "a.ex"), "changed again\n")
      assert %{"decision" => "block"} = blocked(run(ctx))
    end

    test "a change inside an untracked file asks again", ctx do
      # The file list stays the same; only the content moved, which is what a fix to a new file does.
      File.write!(Path.join(ctx.repo, "new.ex"), "one\n")
      assert %{"decision" => "block"} = blocked(run(ctx))

      File.write!(Path.join(ctx.repo, "new.ex"), "two\n")
      assert %{"decision" => "block"} = blocked(run(ctx))
    end

    test "a content edit to an untracked file whose name git would quote asks again", ctx do
      # git C-quotes a name with a non-ASCII byte or a tab, and a quoted name is a path that does not
      # exist: read that way, the file's content never reached the fingerprint.
      for name <- ["café.txt", "tab\tname.ex"] do
        path = Path.join(ctx.repo, name)
        File.write!(path, "one\n")
        assert %{"decision" => "block"} = blocked(run(ctx)), name

        File.write!(path, "two\n")
        assert %{"decision" => "block"} = blocked(run(ctx)), name
      end
    end

    test "a git directory it cannot write keeps the fingerprint elsewhere, quietly", ctx do
      # A read-only mount: the hook must neither print an error on every stop nor ask again every turn.
      tmp = Path.join(ctx.dir, "tmp")
      File.mkdir!(tmp)
      git_dir = read_only_git_dir!(ctx)
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")

      assert %{"decision" => "block"} = blocked(run(ctx, env: [{"TMPDIR", tmp}]))
      assert run(ctx, env: [{"TMPDIR", tmp}]) == {"", 0}

      # One directory of this user's own, closed to everyone else, holding the one fingerprint.
      assert [private] = File.ls!(tmp)
      assert private == "malachi-auto-review.#{uid()}"
      assert %File.Stat{mode: mode} = File.stat!(Path.join(tmp, private))
      assert Bitwise.band(mode, 0o777) == 0o700
      assert [_state] = File.ls!(Path.join(tmp, private))
      refute File.exists?(Path.join(git_dir, "malachi-auto-review"))
    end

    test "never writes through a link planted at its temporary directory's name", ctx do
      # A shared /tmp lets any user create that name first, pointing somewhere this user can write.
      tmp = Path.join(ctx.dir, "tmp")
      target = Path.join(ctx.dir, "elsewhere")
      File.mkdir!(tmp)
      File.mkdir!(target)
      File.ln_s!(target, Path.join(tmp, "malachi-auto-review.#{uid()}"))
      read_only_git_dir!(ctx)
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")

      assert run(ctx, env: [{"TMPDIR", tmp}]) == {"", 0}
      assert File.ls!(target) == []
    end

    test "the fingerprint lives in the git directory, never in the working tree", ctx do
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")
      assert %{"decision" => "block"} = blocked(run(ctx))

      assert File.exists?(Path.join([ctx.repo, ".git", "malachi-auto-review"]))
      {status, 0} = System.cmd("git", ["status", "--porcelain"], cd: ctx.repo, env: @no_git_env)
      assert status == " M a.ex\n"
    end

    test "the stop after its own request is let through even with a new diff", ctx do
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")
      assert %{"decision" => "block"} = blocked(run(ctx))

      File.write!(Path.join(ctx.repo, "a.ex"), "fixed during the review\n")
      assert run(ctx, input: ~s({"stop_hook_active": true})) == {"", 0}
    end
  end

  test "a file whose name only resembles a generated one is still reviewed", ctx do
    # The exclusion is by exact name: a wildcard would hide a real file such as a commit_message.ex.
    File.write!(Path.join(ctx.repo, "commit_message.ex"), "real\n")

    assert %{"decision" => "block"} = blocked(run(ctx))
  end

  test "without origin/main only the uncommitted changes count", ctx do
    git!(ctx.repo, ["update-ref", "-d", "refs/remotes/origin/main"])
    File.write!(Path.join(ctx.repo, "b.ex"), "b\n")
    commit!(ctx.repo, "feature work")

    assert run(ctx) == {"", 0}

    File.write!(Path.join(ctx.repo, "c.ex"), "c\n")
    assert %{"decision" => "block"} = blocked(run(ctx))
  end

  test "ignores a GIT_DIR inherited from a git hook and looks at the project", ctx do
    # The project is clean. Read through another repository's GIT_DIR, every file in it would look
    # untracked, and the hook would ask to review a branch with nothing on it.
    elsewhere = Path.join(ctx.dir, "elsewhere")
    File.mkdir!(elsewhere)
    git!(elsewhere, ["init", "-q"])
    File.write!(Path.join(elsewhere, "x.ex"), "x\n")
    commit!(elsewhere, "unrelated")

    assert run(ctx, env: [{"GIT_DIR", Path.join(elsewhere, ".git")}]) == {"", 0}
  end

  test "the repository's project settings run this script on Stop" do
    settings = @repo_root |> Path.join(".claude/settings.json") |> File.read!() |> Jason.decode!()

    commands =
      for group <- settings["hooks"]["Stop"], hook <- group["hooks"], hook["type"] == "command", do: hook["command"]

    assert ~s("$CLAUDE_PROJECT_DIR"/scripts/auto-review-hook.sh) in commands

    # The hook runs the script directly, so it has to be executable in the checkout.
    assert %File.Stat{mode: mode} = File.stat!(@script)
    assert Bitwise.band(mode, 0o111) != 0
  end

  test "the skill it asks for exists and REVIEW.md is where the skill reads it" do
    assert File.exists?(Path.join(@repo_root, ".claude/skills/adversarial-review/SKILL.md"))
    assert File.exists?(Path.join(@repo_root, "REVIEW.md"))
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

  defp blocked({output, 0}), do: Jason.decode!(output)

  # The repository's git directory made read-only, and proven so: as root a chmod stops nothing, and
  # this test would then check a fallback the hook never took.
  defp read_only_git_dir!(ctx) do
    git_dir = Path.join(ctx.repo, ".git")
    File.chmod!(git_dir, 0o555)
    on_exit(fn -> File.chmod(git_dir, 0o755) end)
    probe = Path.join(git_dir, "probe")

    assert {:error, :eacces} = File.write(probe, ""),
           "#{git_dir} still takes writes after chmod 555; is the suite running as root?"

    git_dir
  end

  defp uid do
    {out, 0} = System.cmd("id", ["-u"])
    String.trim(out)
  end

  defp commit!(repo, message) do
    git!(repo, ["add", "-A"])
    git!(repo, ["-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-q", "-m", message])
  end

  defp git!(cd, args) do
    {_out, 0} = System.cmd("git", args, cd: cd, env: @no_git_env, stderr_to_stdout: true)
    :ok
  end
end
