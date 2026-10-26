defmodule ReviewStateTest do
  # scripts/review-state.sh is the adversarial review's memory: the rounds run against a branch's base, the
  # tree the last complete round reviewed, and the findings already decided. Its tree handling is the one
  # the Stop hook used to have, moved to scripts/review_lib.sh, so the tests of that handling live here.
  # Everything runs for real, against throwaway repositories, and the delta is cut by pin-diff.sh, the way
  # the skill cuts it.
  use ExUnit.Case, async: true

  alias Malachi.Test.TmpDir

  @state Path.expand("../../scripts/review-state.sh", __DIR__)
  @pin Path.expand("../../scripts/pin-diff.sh", __DIR__)

  # Unset in every command this file runs: git reads them ahead of `cd`, so a suite started from a git hook
  # would otherwise build its fixtures in another repository. The contributor's own global and system git
  # config is left out too: a signing key or a diff setting there would change the trees and the diffs.
  @env [
    {"GIT_DIR", nil},
    {"GIT_WORK_TREE", nil},
    {"GIT_COMMON_DIR", nil},
    {"GIT_INDEX_FILE", nil},
    {"GIT_CONFIG_GLOBAL", "/dev/null"},
    {"GIT_CONFIG_NOSYSTEM", "1"},
    {"TMPDIR", nil},
    {"MALACHI_SKIP_AUTO_REVIEW", nil}
  ]

  setup do
    dir = TmpDir.path("review-state")
    File.mkdir!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    repo = Path.join(dir, "repo")
    File.mkdir!(repo)
    git!(repo, ["init", "-q", "-b", "main"])
    File.write!(Path.join(repo, "a.ex"), "a\nb\n")
    commit!(repo, "base")
    # What a clone has: origin/main at the commit the branch left from.
    git!(repo, ["update-ref", "refs/remotes/origin/main", "HEAD"])
    git!(repo, ["checkout", "-q", "-b", "feature"])

    %{dir: dir, repo: repo}
  end

  describe "rounds" do
    test "begin counts a round and says which one it is", ctx do
      assert {"round 1 of 3\n", 0} = begin_round(ctx)
      assert {status, 0} = state(ctx, ["status"])
      assert status =~ "rounds: 1 of 3"
    end

    test "a fourth round is refused until the contributor asks for an extra one", ctx do
      for n <- 1..3 do
        assert {out, 0} = begin_round(ctx)
        assert out == "round #{n} of 3\n"
        assert {_, 0} = state(ctx, ["done"])
      end

      assert {out, 4} = begin_round(ctx)
      assert out =~ "cap is reached"
      assert {"extra round 4, past the cap of 3\n", 0} = begin_round(ctx, ["--extra"])
      assert {status, 0} = state(ctx, ["status"])
      assert status =~ "rounds: 4 of 3"
    end

    test "done makes the tree begin recorded the reviewed one, not the tree at done", ctx do
      write(ctx, "a.ex", "one\n")
      assert {_, 0} = begin_round(ctx)
      write(ctx, "a.ex", "two\n")
      assert {_, 0} = state(ctx, ["done"])

      # The edit after begin is not in the reviewed tree, so the next delta still holds it.
      assert {0, ["a.ex"], _} = delta(ctx, reviewed(ctx))
    end

    test "a round that never finished does not move the reviewed tree", ctx do
      write(ctx, "a.ex", "one\n")
      assert {_, 0} = begin_round(ctx)
      assert {_, 0} = state(ctx, ["done"])
      reviewed = reviewed(ctx)

      write(ctx, "a.ex", "two\n")
      assert {_, 0} = begin_round(ctx)
      write(ctx, "b.ex", "new\n")

      # Round 2 was counted and never finished: its work is still a change against what round 1 read.
      assert {0, ["a.ex", "b.ex"], _} = delta(ctx, reviewed)
      assert reviewed(ctx) == reviewed
    end

    test "committing reviewed work is not a change and does not restart the count", ctx do
      write(ctx, "a.ex", "one\n")
      assert {_, 0} = begin_round(ctx)
      assert {_, 0} = state(ctx, ["done"])
      reviewed = reviewed(ctx)

      commit!(ctx.repo, "reviewed work")

      assert {status, 0} = state(ctx, ["status"])
      assert status =~ "rounds: 1 of 3"
      assert reviewed(ctx) == reviewed
      assert {3, [], _} = delta(ctx, reviewed)
    end

    test "a new base starts the count and the reviewed tree again, and keeps the decisions", ctx do
      write(ctx, "a.ex", "one\n")
      assert {_, 0} = begin_round(ctx)
      assert {_, 0} = state(ctx, ["done"])
      assert {_, 0} = state(ctx, ["decide", "declined", "a.ex", "the old line", "a title"])

      # origin/main moves to a commit the branch then merges: the merge base moves, so the base is new.
      git!(ctx.repo, ["checkout", "-q", "-b", "side", "origin/main"])
      File.write!(Path.join(ctx.repo, "x.ex"), "x\n")
      commit!(ctx.repo, "upstream")
      git!(ctx.repo, ["update-ref", "refs/remotes/origin/main", "HEAD"])
      git!(ctx.repo, ["checkout", "-q", "feature"])

      git!(ctx.repo, [
        "-c",
        "user.email=t@example.com",
        "-c",
        "user.name=t",
        "merge",
        "-q",
        "--no-ff",
        "--no-edit",
        "origin/main"
      ])

      assert {status, 0} = state(ctx, ["status"])
      assert status =~ "rounds: 0 of 3"
      assert status =~ "reviewed: none"
      assert {"", 0} = state(ctx, ["reviewed"])
      assert {rows, 0} = state(ctx, ["decisions"])
      # Decided in round 1, of the base it was decided on; the new base does not renumber it.
      assert rows == "1\tdeclined\ta.ex\tthe old line\ta title\n"
    end

    test "without origin/main the base is no-base, and the count starts again once it exists", ctx do
      git!(ctx.repo, ["update-ref", "-d", "refs/remotes/origin/main"])
      assert {"round 1 of 3\n", 0} = begin_round(ctx)
      assert {status, 0} = state(ctx, ["status"])
      assert status =~ "base: no-base"

      git!(ctx.repo, ["update-ref", "refs/remotes/origin/main", "HEAD~0"])
      assert {status, 0} = state(ctx, ["status"])
      assert status =~ "rounds: 0 of 3"
    end

    test "two worktrees of one repository count their rounds apart", ctx do
      wt = Path.join(ctx.dir, "wt")
      write(ctx, "a.ex", "one\n")
      assert {_, 0} = begin_round(ctx)
      assert {_, 0} = state(ctx, ["done"])
      git!(ctx.repo, ["worktree", "add", "-q", wt, "-b", "other"])

      other = %{ctx | repo: wt}
      assert {status, 0} = state(other, ["status"])
      assert status =~ "rounds: 0 of 3"
      assert {"", 0} = state(other, ["reviewed"])
      assert reviewed(ctx) != ""
    end

    test "the reviewed tree survives git gc", ctx do
      write(ctx, "a.ex", "one\n")
      assert {_, 0} = begin_round(ctx)
      assert {_, 0} = state(ctx, ["done"])
      reviewed = reviewed(ctx)

      git!(ctx.repo, ["gc", "-q", "--prune=now"])

      assert reviewed(ctx) == reviewed
      git!(ctx.repo, ["cat-file", "-e", "#{reviewed}^{tree}"])
    end

    test "a round that cannot record its tree says so, and the next round is a full one", ctx do
      secret = Path.join(ctx.repo, "secret.ex")
      File.write!(secret, "secret\n")
      File.chmod!(secret, 0o000)
      on_exit(fn -> File.chmod(secret, 0o644) end)
      assert_unreadable!(secret)

      assert {out, 0} = begin_round(ctx)
      assert out =~ "could not be read"
      assert {_, 0} = state(ctx, ["done"])
      assert {"", 0} = state(ctx, ["reviewed"])
    end

    test "a git filter that makes git add fatal leaves no reviewed tree", ctx do
      git!(ctx.repo, ["config", "filter.broken.clean", "false"])
      git!(ctx.repo, ["config", "filter.broken.required", "true"])
      File.write!(Path.join(ctx.repo, ".gitattributes"), "*.x filter=broken\n")
      File.write!(Path.join(ctx.repo, "data.x"), "data\n")

      assert {out, 0} = begin_round(ctx)
      assert out =~ "could not be recorded"
      assert {_, 0} = state(ctx, ["done"])
      assert {"", 0} = state(ctx, ["reviewed"])
    end

    test "done with no round begun says so and changes nothing", ctx do
      assert {out, 0} = state(ctx, ["done"])
      assert out =~ "no round was recorded"
      assert {"", 0} = state(ctx, ["reviewed"])
    end
  end

  describe "the tree a round reviews" do
    test "a mode change alone is a change in the delta", ctx do
      write(ctx, "a.ex", "one\n")
      assert {_, 0} = begin_round(ctx)
      assert {_, 0} = state(ctx, ["done"])
      reviewed = reviewed(ctx)

      File.chmod!(Path.join(ctx.repo, "a.ex"), 0o755)
      assert {0, ["a.ex"], _} = delta(ctx, reviewed)
    end

    test "a symlink pointing somewhere else is a change, even at the same content", ctx do
      write(ctx, "t1", "same\n")
      write(ctx, "t2", "same\n")
      File.ln_s!("t1", Path.join(ctx.repo, "link"))
      assert {_, 0} = begin_round(ctx)
      assert {_, 0} = state(ctx, ["done"])
      reviewed = reviewed(ctx)

      File.rm!(Path.join(ctx.repo, "link"))
      File.ln_s!("t2", Path.join(ctx.repo, "link"))
      assert {0, ["link"], _} = delta(ctx, reviewed)
    end

    test "a file whose name git would quote is in the delta under its real name", ctx do
      assert {_, 0} = begin_round(ctx)
      assert {_, 0} = state(ctx, ["done"])
      reviewed = reviewed(ctx)

      write(ctx, "café.txt", "one\n")
      write(ctx, "tab\tname.ex", "one\n")
      assert {0, files, _} = delta(ctx, reviewed)
      assert Enum.sort(files) == Enum.sort(["café.txt", "tab\tname.ex"])
    end

    test "the files prepare-commits generates stay out of the tree, by exact name", ctx do
      assert {_, 0} = begin_round(ctx)
      assert {_, 0} = state(ctx, ["done"])
      reviewed = reviewed(ctx)

      for name <- ~w(commit_message.sh commit_1.patch commit_message.txt commit_message_2.txt) do
        write(ctx, name, "generated\n")
      end

      assert {3, [], _} = delta(ctx, reviewed)

      write(ctx, "commit_message.ex", "real\n")
      assert {0, ["commit_message.ex"], _} = delta(ctx, reviewed)
    end

    test "a committed file named like a generated one is the project's own", ctx do
      write(ctx, "commit_message.sh", "#!/bin/sh\n")
      commit!(ctx.repo, "the project ships one")
      git!(ctx.repo, ["update-ref", "refs/remotes/origin/main", "HEAD"])

      write(ctx, "commit_message.sh", "#!/bin/sh\necho changed\n")
      assert {_, 0} = begin_round(ctx)
      assert {_, 0} = state(ctx, ["done"])
      reviewed = reviewed(ctx)

      write(ctx, "commit_message.sh", "#!/bin/sh\necho again\n")
      assert {0, ["commit_message.sh"], _} = delta(ctx, reviewed)
    end

    test "hundreds of untracked files are all in the reviewed tree, not only the first", ctx do
      dir = Path.join(ctx.repo, "gen")
      File.mkdir!(dir)
      for n <- 1..300, do: File.write!(Path.join(dir, "f#{n}.ex"), "#{n}\n")

      assert {_, 0} = begin_round(ctx)
      assert {_, 0} = state(ctx, ["done"])
      reviewed = reviewed(ctx)

      File.write!(Path.join(dir, "f7.ex"), "changed\n")
      assert {0, ["gen/f7.ex"], _} = delta(ctx, reviewed)
    end

    test "a warning git prints only while a file is untracked does not make the commit new work", ctx do
      # The repository converts *.sh line endings: git warns about a CRLF file while it is untracked and
      # says nothing once it is committed. The content, and the tree, are the same either way.
      write(ctx, ".gitattributes", "*.sh text eol=lf\n")
      tool = Path.join(ctx.repo, "tool.sh")
      write(ctx, "tool.sh", "echo one\r\necho two\r\n")
      File.touch!(tool, System.os_time(:second) - 10)
      assert {_, 0} = begin_round(ctx)
      assert {_, 0} = state(ctx, ["done"])
      reviewed = reviewed(ctx)

      commit!(ctx.repo, "reviewed work")
      assert {3, [], _} = delta(ctx, reviewed)
    end

    test "core.safecrlf does not stop the round from being recorded", ctx do
      git!(ctx.repo, ["config", "core.safecrlf", "true"])
      write(ctx, ".gitattributes", "*.sh text eol=lf\n")
      write(ctx, "tool.sh", "echo hi\r\n")
      write(ctx, "a.ex", "changed\n")

      assert {_, 0} = begin_round(ctx)
      assert {_, 0} = state(ctx, ["done"])
      reviewed = reviewed(ctx)
      assert reviewed != ""

      write(ctx, "a.ex", "changed again\n")
      assert {0, ["a.ex"], _} = delta(ctx, reviewed)
    end

    test "a split index leaves nothing behind in the git directory", ctx do
      git!(ctx.repo, ["config", "core.splitIndex", "true"])
      git!(ctx.repo, ["update-index", "--split-index"])
      git_dir = Path.join(ctx.repo, ".git")
      shared = fn -> git_dir |> File.ls!() |> Enum.filter(&String.starts_with?(&1, "sharedindex.")) end
      before = shared.()

      for n <- 1..20, do: write(ctx, "f#{n}.ex", "#{n}\n")
      assert {_, 0} = begin_round(ctx)
      for n <- 1..20, do: write(ctx, "f#{n}.ex", "#{n} again\n")
      assert {_, 0} = begin_round(ctx)

      assert shared.() == before
    end

    test "the contributor's own index is left as it was", ctx do
      write(ctx, "a.ex", "changed\n")
      write(ctx, "new.ex", "new\n")
      git!(ctx.repo, ["add", "a.ex"])

      assert {_, 0} = begin_round(ctx)

      {status, 0} = System.cmd("git", ["status", "--porcelain"], cd: ctx.repo, env: @env)
      assert status == "M  a.ex\n?? new.ex\n"
    end

    test "begin run from a subdirectory records the whole checkout, not only that directory", ctx do
      # From sub/, git add would take in sub's paths alone, and a.ex would keep its HEAD content in the tree
      # the round records. The recorded blob must be the edit made in the root.
      File.mkdir!(Path.join(ctx.repo, "sub"))
      write(ctx, "a.ex", "changed-root\n")

      assert {_, 0} = state_in(ctx, Path.join(ctx.repo, "sub"), ["begin"])
      assert {_, 0} = state(ctx, ["done"])
      reviewed = reviewed(ctx)

      assert {"changed-root\n", 0} = System.cmd("git", ["show", "#{reviewed}:a.ex"], cd: ctx.repo, env: @env)
    end

    test "an untracked directory git cannot open stops the round from being recorded", ctx do
      # git only warns about it and its files leave the tree silently, so the tree would look complete.
      dir = Path.join(ctx.repo, "hidden")
      File.mkdir!(dir)
      File.write!(Path.join(dir, "h.ex"), "secret\n")
      File.chmod!(dir, 0o000)
      on_exit(fn -> File.chmod(dir, 0o755) end)
      assert_unlistable!(dir)

      assert {out, 0} = begin_round(ctx)
      assert out =~ "could not be read"
      assert {_, 0} = state(ctx, ["done"])
      assert {"", 0} = state(ctx, ["reviewed"])
    end

    test "a delta stops with exit 1 rather than skip an untracked directory it cannot open", ctx do
      assert {_, 0} = begin_round(ctx)
      assert {_, 0} = state(ctx, ["done"])
      reviewed = reviewed(ctx)

      dir = Path.join(ctx.repo, "hidden")
      File.mkdir!(dir)
      File.write!(Path.join(dir, "h.ex"), "changed\n")
      File.chmod!(dir, 0o000)
      on_exit(fn -> File.chmod(dir, 0o755) end)
      assert_unlistable!(dir)

      assert {1, _files, text} = delta(ctx, reviewed)
      assert text =~ "could not open directory"
    end
  end

  describe "the reminder" do
    test "remind says so once, while no round has run for the base", ctx do
      assert {_, 0} = state(ctx, ["remind"])
      assert {_, 1} = state(ctx, ["remind"])
    end

    test "a branch that already has a round is never reminded", ctx do
      assert {_, 0} = begin_round(ctx)
      assert {_, 1} = state(ctx, ["remind"])
    end
  end

  describe "decisions" do
    test "a decision is a row with the round it was made in, and a tab or a line break cannot split it", ctx do
      assert {_, 0} = begin_round(ctx)
      assert {_, 0} = state(ctx, ["decide", "declined", "a.ex", "the\tanchor", "two\nlines"])

      assert {rows, 0} = state(ctx, ["decisions"])
      assert rows == "1\tdeclined\ta.ex\tthe anchor\ttwo lines\n"
    end

    test "decisions keep their order across rounds", ctx do
      assert {_, 0} = begin_round(ctx)
      assert {_, 0} = state(ctx, ["decide", "fixed", "a.ex", "x", "first"])
      assert {_, 0} = state(ctx, ["done"])
      assert {_, 0} = begin_round(ctx)
      assert {_, 0} = state(ctx, ["decide", "followup", "a.ex", "y", "second"])

      assert {rows, 0} = state(ctx, ["decisions"])
      assert rows == "1\tfixed\ta.ex\tx\tfirst\n2\tfollowup\ta.ex\ty\tsecond\n"
    end

    test "a status outside the four is a usage error and records nothing", ctx do
      assert {_, 2} = state(ctx, ["decide", "maybe", "a.ex", "x", "t"])
      assert {"", 0} = state(ctx, ["decisions"])
    end

    test "decide takes exactly four arguments", ctx do
      assert {_, 2} = state(ctx, ["decide", "fixed", "a.ex", "x"])
    end
  end

  describe "where it lives" do
    test "the counts sit in this worktree's git directory, and the working tree stays clean", ctx do
      assert {_, 0} = begin_round(ctx)
      assert_file_exists!(Path.join([ctx.repo, ".git", "malachi-review-state"]))
      {status, 0} = System.cmd("git", ["status", "--porcelain"], cd: ctx.repo, env: @env)
      assert status == ""
    end

    test "a git directory that cannot be written keeps the counts under TMPDIR, in a directory of this user's own",
         ctx do
      tmp = Path.join(ctx.dir, "tmp")
      File.mkdir!(tmp)
      git_dir = read_only_git_dir!(ctx)
      env = [{"TMPDIR", tmp}]

      assert {out, 0} = begin_round(ctx, [], env)
      assert out =~ "round 1 of 3"
      assert out =~ "could not be recorded"
      assert {out, 0} = begin_round(ctx, [], env)
      assert out =~ "round 2 of 3"

      assert [private] = File.ls!(tmp)
      assert private == "malachi-review.#{uid()}"
      assert %File.Stat{mode: mode} = File.stat!(Path.join(tmp, private))
      assert Bitwise.band(mode, 0o777) == 0o700
      refute File.exists?(Path.join(git_dir, "malachi-review-state"))
    end

    test "never writes through a link planted at its temporary directory's name", ctx do
      # A shared temporary directory lets any user create that name first, pointing somewhere writable.
      tmp = Path.join(ctx.dir, "tmp")
      target = Path.join(ctx.dir, "elsewhere")
      File.mkdir!(tmp)
      File.mkdir!(target)
      File.ln_s!(target, Path.join(tmp, "malachi-review.#{uid()}"))
      read_only_git_dir!(ctx)

      assert {_, 1} = state(ctx, ["remind"], [{"TMPDIR", tmp}])
      assert File.ls!(target) == []
    end
  end

  # One begin, as the skill runs it: the round is counted and its tree recorded.
  defp begin_round(ctx, args \\ [], env \\ []), do: state(ctx, ["begin" | args], env)

  defp reviewed(ctx) do
    {out, 0} = state(ctx, ["reviewed"])
    String.trim(out)
  end

  # The per-test env wins over @env: a map keeps the last value given for a name.
  defp state_in(ctx, dir, args) do
    System.cmd("bash", [@state | args], cd: dir, env: @env, stderr_to_stdout: true)
  end

  defp assert_unlistable!(dir) do
    assert {:error, :eacces} = File.ls(dir), "#{dir} still lists after chmod 000; is the suite running as root?"
  end

  defp state(ctx, args, env \\ []) do
    env = (@env ++ env) |> Map.new() |> Enum.to_list()
    System.cmd("bash", [@state | args], cd: ctx.repo, env: env, stderr_to_stdout: true)
  end

  # The delta since a tree, as pin-diff.sh cuts it for the skill: {exit code, changed files, output}.
  defp delta(ctx, since) do
    out = Path.join(ctx.dir, "pin-#{System.unique_integer([:positive])}")
    File.mkdir_p!(out)

    {text, code} =
      System.cmd("bash", [@pin, "pin", "--since", since, out], cd: ctx.repo, env: @env, stderr_to_stdout: true)

    files =
      case File.read(Path.join(out, "files.txt")) do
        {:ok, listed} -> String.split(listed, "\n", trim: true)
        {:error, _} -> []
      end

    {code, files, text}
  end

  defp write(ctx, name, content), do: File.write!(Path.join(ctx.repo, name), content)
  defp assert_file_exists!(path), do: assert(File.exists?(path), "#{path} is missing")

  # Proven, not assumed: as root a chmod stops nothing, and the test would then check a case that never came.
  defp assert_unreadable!(path) do
    assert {:error, :eacces} = File.read(path),
           "#{path} is still readable after chmod 000; is the suite running as root?"
  end

  # The repository's whole git directory made read-only, the way a read-only mount is, and proven so.
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
    {_out, 0} = System.cmd("git", args, cd: cd, env: @env, stderr_to_stdout: true)
    :ok
  end
end
