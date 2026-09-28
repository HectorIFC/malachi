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

    test "one unreadable file does not silence the review of the rest", ctx do
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")
      secret = Path.join(ctx.repo, "secret.ex")
      File.write!(secret, "secret\n")
      File.chmod!(secret, 0o000)
      on_exit(fn -> File.chmod(secret, 0o644) end)

      assert %{"reason" => reason} = blocked(run(ctx))
      assert reason =~ "2 changed file(s)"

      File.write!(Path.join(ctx.repo, "a.ex"), "changed again\n")
      assert %{"decision" => "block"} = blocked(run(ctx))
    end

    test "without a usable temporary directory it asks on every change, with no memory", ctx do
      # The tree is built in a private temporary directory; with none, the hook cannot remember what it
      # asked, and asking again is the side that errs toward a review rather than toward silence.
      missing = [{"TMPDIR", Path.join(ctx.dir, "missing")}]
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")

      assert %{"reason" => reason} = blocked(run(ctx, env: missing))
      assert reason =~ "1 changed file(s)"
      assert %{"decision" => "block"} = blocked(run(ctx, env: missing))
    end

    test "without one it neither touches the index nor counts the generated files", ctx do
      missing = [{"TMPDIR", Path.join(ctx.dir, "missing")}]
      index = Path.join([ctx.repo, ".git", "index"])
      File.touch!(Path.join(ctx.repo, "a.ex"), System.os_time(:second) + 5)
      File.write!(Path.join(ctx.repo, "commit_message.sh"), "generated\n")
      File.write!(Path.join(ctx.repo, "commit_1.patch"), "generated\n")
      before = File.stat!(index, time: :posix)

      assert run(ctx, env: missing) == {"", 0}, "a touched file and generated files are not work to review"
      later = File.stat!(index, time: :posix)
      assert {later.mtime, later.inode, later.size} == {before.mtime, before.inode, before.size}
    end

    test "a deleted file is a change, and restoring it is another", ctx do
      File.rm!(Path.join(ctx.repo, "a.ex"))
      assert %{"reason" => reason} = blocked(run(ctx))
      assert reason =~ "1 changed file(s)"

      File.write!(Path.join(ctx.repo, "a.ex"), "a\n")
      assert run(ctx) == {"", 0}, "back to the base content: nothing left to review"
    end

    test "the count reaches the reason as a bare number", ctx do
      # BSD wc pads its count with spaces; the reason is read by a person and by a model.
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")

      assert %{"reason" => "This turn ended with 1 changed file(s) on the branch." <> _rest} = blocked(run(ctx))
    end

    test "names a locale collates as equal are still two files", ctx do
      # In a UTF-8 locale many distinct names sort as equal; nothing here may depend on that.
      File.write!(Path.join(ctx.repo, "一.ex"), "one\n")
      File.write!(Path.join(ctx.repo, "丁.ex"), "one\n")

      assert %{"reason" => reason} = blocked(run(ctx, locale: "en_US.UTF-8"))
      assert reason =~ "2 changed file(s)"

      File.write!(Path.join(ctx.repo, "丁.ex"), "two\n")
      assert %{"decision" => "block"} = blocked(run(ctx, locale: "en_US.UTF-8"))
    end

    test "pointing a symlink somewhere else asks again, even at the same content", ctx do
      File.write!(Path.join(ctx.repo, "t1"), "same\n")
      File.write!(Path.join(ctx.repo, "t2"), "same\n")
      File.ln_s!("t1", Path.join(ctx.repo, "link"))
      assert %{"decision" => "block"} = blocked(run(ctx))

      File.rm!(Path.join(ctx.repo, "link"))
      File.ln_s!("t2", Path.join(ctx.repo, "link"))
      assert %{"decision" => "block"} = blocked(run(ctx))
    end

    test "a mode change alone asks again", ctx do
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")
      assert %{"decision" => "block"} = blocked(run(ctx))

      File.chmod!(Path.join(ctx.repo, "a.ex"), 0o755)
      assert %{"decision" => "block"} = blocked(run(ctx))
    end

    test "a committed file named like a generated one is the project's own", ctx do
      File.write!(Path.join(ctx.repo, "commit_message.sh"), "#!/bin/sh\n")
      commit!(ctx.repo, "the project ships one")
      git!(ctx.repo, ["update-ref", "refs/remotes/origin/main", "HEAD"])

      File.write!(Path.join(ctx.repo, "commit_message.sh"), "#!/bin/sh\necho changed\n")
      assert %{"reason" => reason} = blocked(run(ctx))
      assert reason =~ "1 changed file(s)"
    end

    test "thousands of untracked files stay well inside the hook's timeout", ctx do
      # The hook runs on every stop under a 30 second timeout; one process per file did not fit.
      dir = Path.join(ctx.repo, "gen")
      File.mkdir!(dir)
      for n <- 1..3_000, do: File.write!(Path.join(dir, "f#{n}.ex"), "#{n}\n")

      {elapsed_us, result} = :timer.tc(fn -> run(ctx) end)
      assert %{"reason" => reason} = blocked(result)
      assert reason =~ "3000 changed file(s)"
      assert elapsed_us < 10_000_000, "the hook took #{div(elapsed_us, 1000)}ms over 3000 files"
    end

    test "a warning git prints only before a commit does not make the commit look like new work", ctx do
      # The repository converts *.sh line endings; git warns about a CRLF file while it is untracked and
      # says nothing once it is committed. The content, and the tree, are the same either way.
      File.write!(Path.join(ctx.repo, ".gitattributes"), "*.sh text eol=lf\n")
      tool = Path.join(ctx.repo, "tool.sh")
      File.write!(tool, "echo one\r\necho two\r\n")
      # Older than the index, so git trusts its stat cache after the commit instead of re-reading the
      # file, which is when it stops printing the warning.
      File.touch!(tool, System.os_time(:second) - 10)
      assert %{"decision" => "block"} = blocked(run(ctx))

      commit!(ctx.repo, "reviewed work")
      assert run(ctx) == {"", 0}
    end

    test "a split index leaves nothing behind in the git directory", ctx do
      git!(ctx.repo, ["config", "core.splitIndex", "true"])
      git!(ctx.repo, ["update-index", "--split-index"])
      git_dir = Path.join(ctx.repo, ".git")
      shared = fn -> git_dir |> File.ls!() |> Enum.filter(&String.starts_with?(&1, "sharedindex.")) end
      before = shared.()

      for n <- 1..20, do: File.write!(Path.join(ctx.repo, "f#{n}.ex"), "#{n}\n")
      assert %{"decision" => "block"} = blocked(run(ctx))
      for n <- 1..20, do: File.write!(Path.join(ctx.repo, "f#{n}.ex"), "#{n} again\n")
      assert %{"decision" => "block"} = blocked(run(ctx))

      assert shared.() == before
    end

    test "an unreadable file is seen whatever language git speaks", ctx do
      # The one message read back from git is matched in English; git is made to speak it. (Where git
      # has no translation installed this passes either way; with one, it guards the C locale.)
      german = "de_DE.UTF-8"
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")
      assert %{"decision" => "block"} = blocked(run(ctx, locale: german))

      secret = Path.join(ctx.repo, "secret.ex")
      File.write!(secret, "secret\n")
      File.chmod!(secret, 0o000)
      on_exit(fn -> File.chmod(secret, 0o644) end)

      assert %{"reason" => reason} = blocked(run(ctx, locale: german))
      assert reason =~ "2 changed file(s)"
    end

    test "core.safecrlf does not hide every change behind one CRLF file", ctx do
      git!(ctx.repo, ["config", "core.safecrlf", "true"])
      File.write!(Path.join(ctx.repo, ".gitattributes"), "*.sh text eol=lf\n")
      File.write!(Path.join(ctx.repo, "tool.sh"), "echo hi\r\n")
      File.write!(Path.join(ctx.repo, "a.ex"), "changed\n")

      assert %{"decision" => "block"} = blocked(run(ctx))

      File.write!(Path.join(ctx.repo, "a.ex"), "changed again\n")
      assert %{"decision" => "block"} = blocked(run(ctx))

      # And it still remembers: with git add failing on the CRLF file it would ask on every stop.
      assert run(ctx) == {"", 0}
    end

    test "when git gives up building the tree it asks rather than stays silent", ctx do
      # A required clean filter that fails makes git add fatal; the private index is then still the
      # old one, and a tree built from it would hide every change.
      git!(ctx.repo, ["config", "filter.broken.clean", "false"])
      git!(ctx.repo, ["config", "filter.broken.required", "true"])
      File.write!(Path.join(ctx.repo, ".gitattributes"), "*.x filter=broken\n")
      File.write!(Path.join(ctx.repo, "data.x"), "data\n")

      assert %{"decision" => "block"} = blocked(run(ctx))
      assert %{"decision" => "block"} = blocked(run(ctx)), "no tree, no memory: it asks again"
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

    case Keyword.fetch(opts, :locale) do
      :error -> System.cmd("bash", ["-c", ~s(bash "$0" < "$1"), @script, input_file], env: env, stderr_to_stdout: true)
      {:ok, locale} -> run_in_locale(ctx, input_file, env, locale)
    end
  end

  # A locale the machine does not have installed (a CI runner has no de_DE) makes bash warn about it on
  # stderr, and a warning ahead of the JSON is not the hook's output. So the locale reaches only the
  # hook's own bash, not the wrapper, whose warning would land in the output; the hook's stderr goes to a
  # file; and anything written there other than that warning still fails the test.
  defp run_in_locale(ctx, input_file, env, locale) do
    errors = Path.join(ctx.dir, "stderr-#{System.unique_integer([:positive])}")
    command = ~s(LC_ALL="$2" bash "$0" < "$1" 2> "$3")
    result = System.cmd("bash", ["-c", command, @script, input_file, locale, errors], env: env, stderr_to_stdout: true)

    unexpected = errors |> File.read!() |> String.split("\n", trim: true) |> Enum.reject(&(&1 =~ "setlocale"))
    assert unexpected == [], "the hook wrote to stderr: #{inspect(unexpected)}"
    result
  end

  defp blocked({output, 0}), do: Jason.decode!(output)

  # The repository's whole git directory made read-only, the way a read-only mount is, and proven so:
  # as root a chmod stops nothing, and this test would then check a fallback the hook never took.
  defp read_only_git_dir!(ctx) do
    git_dir = Path.join(ctx.repo, ".git")
    {_out, 0} = System.cmd("chmod", ["-R", "a-w", git_dir])
    on_exit(fn -> System.cmd("chmod", ["-R", "u+w", git_dir]) end)
    probe = Path.join([git_dir, "objects", "probe"])

    assert {:error, :eacces} = File.write(probe, ""),
           "#{git_dir} still takes writes after chmod -R a-w; is the suite running as root?"

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
