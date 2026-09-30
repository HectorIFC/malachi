defmodule PinDiffTest do
  # scripts/pin-diff.sh is what every review in this repository reads: adversarial-review and
  # pr-agent-review both pin their diff with it, so they can never disagree about what the diff is. It
  # runs for real here, against throwaway repositories, the way auto_review_hook_test.exs runs the hook.
  use ExUnit.Case, async: true

  alias Malachi.Test.TmpDir

  @script Path.expand("../../scripts/pin-diff.sh", __DIR__)

  # Unset in every command this file runs: git reads them ahead of `cd`, so a suite started from a git
  # hook would otherwise build its fixtures in another repository. The developer's own global and system
  # git config are left out as well (as publish_results_test.exs does): commit signing, a forced color or
  # a diff setting there would otherwise fail the fixtures or change the reference diffs.
  @no_git_env [
    {"GIT_DIR", nil},
    {"GIT_WORK_TREE", nil},
    {"GIT_COMMON_DIR", nil},
    {"GIT_INDEX_FILE", nil},
    {"GIT_CONFIG_GLOBAL", "/dev/null"},
    {"GIT_CONFIG_NOSYSTEM", "1"}
  ]

  setup do
    dir = TmpDir.path("pin-diff")
    File.mkdir!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    repo = Path.join(dir, "repo")
    File.mkdir!(repo)
    git!(repo, ["init", "-q", "-b", "main"])
    File.write!(Path.join(repo, "a.ex"), "a\nb\nc\nd\n")
    commit!(repo, "base")
    # What a clone has: origin/main at the commit the branch left from.
    git!(repo, ["update-ref", "refs/remotes/origin/main", "HEAD"])
    git!(repo, ["checkout", "-q", "-b", "feature"])

    %{dir: dir, repo: repo, out: Path.join(dir, "out")}
  end

  describe "pin" do
    test "covers what the branch committed, what is modified and what is untracked", ctx do
      File.write!(Path.join(ctx.repo, "committed.ex"), "c\n")
      commit!(ctx.repo, "feature work")
      File.write!(Path.join(ctx.repo, "a.ex"), "a\nB\nc\nd\n")
      File.write!(Path.join(ctx.repo, "untracked.ex"), "u\n")

      assert {_, 0} = pin(ctx)
      assert files(ctx) == ["a.ex", "committed.ex", "untracked.ex"]
      assert base(ctx) == rev!(ctx.repo, "origin/main")

      patch = File.read!(Path.join(ctx.out, "diff.patch"))
      assert patch =~ "+B\n"
      assert patch =~ "+++ b/committed.ex\n@@ -0,0 +1 @@\n+c\n"
      assert patch =~ "+++ b/untracked.ex\n@@ -0,0 +1 @@\n+u\n"
    end

    test "a staged change counts like any other", ctx do
      File.write!(Path.join(ctx.repo, "a.ex"), "staged\n")
      git!(ctx.repo, ["add", "a.ex"])

      assert {_, 0} = pin(ctx)
      assert files(ctx) == ["a.ex"]
    end

    test "a deleted file and a binary file are in the diff", ctx do
      File.write!(Path.join(ctx.repo, "b.bin"), <<0, 1, 2>>)
      commit!(ctx.repo, "binary")
      git!(ctx.repo, ["update-ref", "refs/remotes/origin/main", "HEAD"])
      File.rm!(Path.join(ctx.repo, "a.ex"))
      File.write!(Path.join(ctx.repo, "b.bin"), <<0, 9, 9>>)

      assert {_, 0} = pin(ctx)
      assert files(ctx) == ["a.ex", "b.bin"]
      patch = File.read!(Path.join(ctx.out, "diff.patch"))
      assert patch =~ "deleted file mode"
      assert patch =~ "Binary files"
    end

    test "leaves out the files prepare-commits generates, by exact name, only while untracked", ctx do
      for name <- ~w(commit_message.sh commit_1.patch commit_12.patch commit_message.txt commit_message_3.txt) do
        File.write!(Path.join(ctx.repo, name), "generated\n")
      end

      assert {_, 3} = pin(ctx)

      # A name that only resembles a generated one is the project's own, and so is a generated name
      # somewhere other than the root.
      File.write!(Path.join(ctx.repo, "commit_message.ex"), "real\n")
      File.mkdir!(Path.join(ctx.repo, "sub"))
      File.write!(Path.join(ctx.repo, "sub/commit_1.patch"), "real\n")

      assert {_, 0} = pin(ctx)
      assert files(ctx) == ["commit_message.ex", "sub/commit_1.patch"]
    end

    test "a committed file named like a generated one is the project's own", ctx do
      File.write!(Path.join(ctx.repo, "commit_message.txt"), "tracked\n")
      commit!(ctx.repo, "a real file")

      assert {_, 0} = pin(ctx)
      assert files(ctx) == ["commit_message.txt"]
    end

    test "nothing to review exits 3 with an empty list", ctx do
      assert {_, 3} = pin(ctx)
      assert files(ctx) == []
      assert File.read!(Path.join(ctx.out, "diff.patch")) == ""
    end

    test "--base pins against the commit given, even one origin/main already contains", ctx do
      File.write!(Path.join(ctx.repo, "b.ex"), "b\n")
      commit!(ctx.repo, "one")
      one = rev!(ctx.repo, "HEAD")
      File.write!(Path.join(ctx.repo, "c.ex"), "c\n")
      commit!(ctx.repo, "two")
      # The history already merged: merge-base with origin/main is HEAD itself, so without --base the
      # diff is empty. That is what reviewing a commit from the past looks like.
      git!(ctx.repo, ["update-ref", "refs/remotes/origin/main", "HEAD"])

      assert {_, 3} = pin(ctx)
      assert {_, 0} = pin(ctx, ["--base", one])
      assert files(ctx) == ["c.ex"]
      assert base(ctx) == one
    end

    test "--base that is not a commit is a usage error", ctx do
      assert {out, 2} = pin(ctx, ["--base", "no-such-ref"])
      assert out =~ "--base is not a commit"
    end

    test "without origin/main only the uncommitted changes count", ctx do
      git!(ctx.repo, ["update-ref", "-d", "refs/remotes/origin/main"])
      File.write!(Path.join(ctx.repo, "b.ex"), "b\n")
      commit!(ctx.repo, "feature work")

      # Silent before: the committed work vanished from the diff with no word said.
      assert {out, 3} = pin(ctx)
      assert out =~ "no merge base with origin/main"

      File.write!(Path.join(ctx.repo, "c.ex"), "c\n")
      assert {_, 0} = pin(ctx)
      assert files(ctx) == ["c.ex"]
    end

    test "a repository with no commit yet compares against the empty tree", ctx do
      fresh = Path.join(ctx.dir, "fresh")
      File.mkdir!(fresh)
      git!(fresh, ["init", "-q"])
      File.write!(Path.join(fresh, "x.ex"), "x\n")

      assert {_, 0} = pin(%{ctx | repo: fresh})
      assert files(ctx) == ["x.ex"]
    end

    test "names with spaces, quotes and non-ASCII characters come out as they are, tracked or not", ctx do
      names = ["sp ace.ex", ~s(quo"te.ex), "ação.ex", ~S(back\slash.ex)]
      for name <- names, do: File.write!(Path.join(ctx.repo, "committed " <> name), "n\n")
      commit!(ctx.repo, "committed names")
      for name <- names, do: File.write!(Path.join(ctx.repo, "staged " <> name), "n\n")
      git!(ctx.repo, ["add", "-A"])
      for name <- names, do: File.write!(Path.join(ctx.repo, "untracked " <> name), "n\n")

      assert {_, 0} = pin(ctx)

      expected = for prefix <- ["committed ", "staged ", "untracked "], name <- names, do: prefix <> name
      assert Enum.sort(files(ctx)) == Enum.sort(expected)

      {rendered, 0} = run(ctx.repo, ["render", Path.join(ctx.out, "diff.patch")])
      for name <- expected, do: assert(rendered =~ "## File: '#{name}'\n", name)
    end

    test "the contributor's own git config does not change what is pinned or how it renders", ctx do
      # A forced color would put escapes in every header (and render would find no file at all), a
      # suppressed blank context line would throw the hunk counts off, and a mnemonic or missing prefix
      # would make every path wrong.
      config = Path.join(ctx.dir, "leaky.gitconfig")

      File.write!(config, """
      [color]
      \tui = always
      \tdiff = always
      [diff]
      \tsuppressBlankEmpty = true
      \tmnemonicPrefix = true
      \tnoprefix = true
      \trelative = true
      \texternal = /usr/bin/true
      """)

      File.mkdir!(Path.join(ctx.repo, "b"))
      File.write!(Path.join(ctx.repo, "a.ex"), "a\n\nb\nc\nd\n")
      commit!(ctx.repo, "a blank context line")
      git!(ctx.repo, ["update-ref", "refs/remotes/origin/main", "HEAD"])
      File.write!(Path.join(ctx.repo, "a.ex"), "a\n\nB\nc\nd\n")
      File.write!(Path.join(ctx.repo, "b/c.ex"), "c\n")
      File.write!(Path.join(ctx.repo, "z.ex"), "z\n")

      leaky = [{"GIT_CONFIG_GLOBAL", config}]
      assert {_, 0} = run(Path.join(ctx.repo, "b"), ["pin", ctx.out], leaky)
      assert files(ctx) == ["a.ex", "b/c.ex", "z.ex"]
      refute File.read!(Path.join(ctx.out, "diff.patch")) =~ "\e["

      {rendered, 0} = run(ctx.repo, ["render", Path.join(ctx.out, "diff.patch")], leaky)
      assert rendered =~ "## File: 'a.ex'\n\n@@ -1,5 +1,5 @@\n__new hunk__\n1  a\n2  \n3 +B\n"
      assert rendered =~ "## File: 'b/c.ex'\n"
      assert rendered =~ "## File: 'z.ex'\n"
    end

    test "a file git cannot read stops the pin with exit 1 rather than leaving it out", ctx do
      unreadable = Path.join(ctx.repo, "unreadable.ex")
      File.write!(unreadable, "secret\n")
      File.chmod!(unreadable, 0o000)
      on_exit(fn -> File.chmod(unreadable, 0o644) end)

      # As root a chmod stops nothing, and there is then nothing to show.
      if match?({:error, :eacces}, File.read(unreadable)) do
        assert {_, 1} = pin(ctx)
      end
    end

    test "an untracked directory git cannot open stops the pin with exit 1 rather than vanishing", ctx do
      # git ls-files only warns about it and succeeds, so without a check its files would silently be
      # left out, and a branch with nothing else would even read as nothing to review.
      secret = Path.join(ctx.repo, "secret")
      File.mkdir!(secret)
      File.write!(Path.join(secret, "s.ex"), "s\n")
      File.chmod!(secret, 0o000)
      on_exit(fn -> File.chmod(secret, 0o755) end)

      if match?({:error, :eacces}, File.ls(secret)) do
        assert {out, 1} = pin(ctx)
        assert out =~ "secret"
      end
    end

    test "names git C-quotes for a control character come out as they are", ctx do
      for name <- ["Icon\r", "a\u0001b", "t\tab", "bel\ab", "bs\bb", "vt\vb", "ff\fb"],
          do: File.write!(Path.join(ctx.repo, name), "n\n")

      assert {_, 0} = pin(ctx)
      {rendered, 0} = run(ctx.repo, ["render", Path.join(ctx.out, "diff.patch")])

      for name <- ["Icon\r", "a\u0001b", "t\tab", "bel\ab", "bs\bb", "vt\vb", "ff\fb"] do
        assert rendered =~ "## File: '#{name}'\n", inspect(name)
      end
    end

    test "the usage errors of pin exit 2", ctx do
      assert {_, 2} = run(ctx.repo, ["pin"])
      assert {_, 2} = run(ctx.repo, ["pin", "--base"])
      # An empty base (a merge-base that failed upstream) is refused, not taken as no base at all.
      assert {out, 2} = run(ctx.repo, ["pin", "--base", "", ctx.out])
      assert out =~ "--base"
      assert {_, 2} = run(ctx.repo, ["pin", "--frobnicate", ctx.out])
      assert {_, 2} = run(ctx.repo, ["pin", ctx.out, Path.join(ctx.dir, "second")])
    end

    test "runs from a subdirectory and still covers the whole work tree", ctx do
      File.mkdir!(Path.join(ctx.repo, "sub"))
      File.write!(Path.join(ctx.repo, "top.ex"), "t\n")

      assert {_, 0} = pin(ctx, [], Path.join(ctx.repo, "sub"))
      assert files(ctx) == ["top.ex"]
    end

    test "ignores a GIT_DIR inherited from a git hook and looks at the checkout it runs in", ctx do
      elsewhere = Path.join(ctx.dir, "elsewhere")
      File.mkdir!(elsewhere)
      git!(elsewhere, ["init", "-q"])
      File.write!(Path.join(elsewhere, "x.ex"), "x\n")

      assert {_, 3} = run(ctx.repo, ["pin", ctx.out], [{"GIT_DIR", Path.join(elsewhere, ".git")}])
    end

    test "outside a git work tree it refuses", ctx do
      plain = Path.join(ctx.dir, "plain")
      File.mkdir!(plain)

      assert {out, 2} = run(plain, ["pin", ctx.out])
      assert out =~ "not inside a git work tree"
    end

    test "pins exactly what adversarial-review's step 1 used to read", ctx do
      # The commands the skill ran inline before the extraction: `git diff "$base"` for everything
      # tracked, plus every untracked file. The pinned diff must start with those exact bytes, list the
      # same files, and carry every untracked file's content.
      File.write!(Path.join(ctx.repo, "committed.ex"), "c\n")
      commit!(ctx.repo, "feature work")
      File.write!(Path.join(ctx.repo, "a.ex"), "a\nB\nc\n")
      File.write!(Path.join(ctx.repo, "untracked.ex"), "u1\nu2\n")
      File.write!(Path.join(ctx.repo, "commit_message.sh"), "generated\n")

      base = git_out!(ctx.repo, ["merge-base", "HEAD", "origin/main"]) |> String.trim()
      old_tracked = git_out!(ctx.repo, ["diff", base])
      old_names = git_out!(ctx.repo, ["diff", "--name-only", base]) |> String.split("\n", trim: true)

      old_untracked =
        git_out!(ctx.repo, ["ls-files", "--others", "--exclude-standard"])
        |> String.split("\n", trim: true)
        |> Enum.reject(&(&1 in ["commit_message.sh", "commit_message.txt"]))

      assert {_, 0} = pin(ctx)
      patch = File.read!(Path.join(ctx.out, "diff.patch"))

      assert String.starts_with?(patch, old_tracked)
      assert files(ctx) == old_names ++ old_untracked

      for name <- old_untracked,
          line <- ctx.repo |> Path.join(name) |> File.read!() |> String.split("\n", trim: true) do
        assert patch =~ "+" <> line <> "\n"
      end
    end
  end

  describe "render" do
    test "a modified file becomes a numbered new hunk and an old hunk", ctx do
      File.write!(Path.join(ctx.repo, "a.ex"), "a\nB\nc\nd\ne\n")

      assert render(ctx) == """
             ## File: 'a.ex'

             @@ -1,4 +1,5 @@
             __new hunk__
             1  a
             2 +B
             3  c
             4  d
             5 +e
             __old hunk__
              a
             -b
              c
              d

             """
    end

    test "a hunk that removes nothing has no old hunk", ctx do
      File.write!(Path.join(ctx.repo, "a.ex"), "a\nb\nc\nd\ne\n")

      rendered = render(ctx)
      assert rendered =~ "5 +e\n"
      refute rendered =~ "__old hunk__"
    end

    test "several hunks keep their own numbering", ctx do
      lines = Enum.map_join(1..30, "", &"line#{&1}\n")
      File.write!(Path.join(ctx.repo, "long.ex"), lines)
      commit!(ctx.repo, "long")
      git!(ctx.repo, ["update-ref", "refs/remotes/origin/main", "HEAD"])

      changed = lines |> String.replace("line2\n", "two\n") |> String.replace("line28\n", "twenty-eight\n")
      File.write!(Path.join(ctx.repo, "long.ex"), changed)

      rendered = render(ctx)
      assert rendered =~ "2 +two\n"
      assert rendered =~ "28 +twenty-eight\n"
      assert length(String.split(rendered, "__new hunk__")) == 3
    end

    test "a deleted, a binary, an empty and a renamed file are one line each", ctx do
      File.write!(Path.join(ctx.repo, "b.bin"), <<0, 1>>)
      File.write!(Path.join(ctx.repo, "old.ex"), "same\n")
      commit!(ctx.repo, "more")
      git!(ctx.repo, ["update-ref", "refs/remotes/origin/main", "HEAD"])

      File.rm!(Path.join(ctx.repo, "a.ex"))
      File.write!(Path.join(ctx.repo, "b.bin"), <<0, 2>>)
      git!(ctx.repo, ["mv", "old.ex", "new.ex"])
      File.write!(Path.join(ctx.repo, "empty.ex"), "")

      rendered = render(ctx)
      assert rendered =~ "## File 'a.ex' was deleted\n"
      assert rendered =~ "## File 'b.bin' is binary, not shown\n"
      assert rendered =~ "## File 'new.ex' renamed from 'old.ex', content unchanged\n"
      assert rendered =~ "## File 'empty.ex' added, empty\n"
      # A deleted file's removed lines are not shown as a hunk.
      refute rendered =~ "-a\n"
    end

    test "a file whose mode alone changed is one line", ctx do
      File.chmod!(Path.join(ctx.repo, "a.ex"), 0o755)

      assert render(ctx) == "## File 'a.ex' mode changed, content unchanged\n\n"
    end

    test "a content line that looks like a diff header stays content", ctx do
      File.write!(Path.join(ctx.repo, "a.ex"), "a\n-- x\n++ y\ndiff --git a/q b/q\n")
      commit!(ctx.repo, "tricky")
      git!(ctx.repo, ["update-ref", "refs/remotes/origin/main", "HEAD"])
      File.write!(Path.join(ctx.repo, "a.ex"), "a\n++ y\ndiff --git a/q b/q\nz\n")

      rendered = render(ctx)
      assert rendered =~ "## File: 'a.ex'"
      refute rendered =~ "## File: 'q'"
      assert rendered =~ "--- x\n"
      assert rendered =~ "4 +z\n"
    end

    test "a file whose name has a space keeps it", ctx do
      File.write!(Path.join(ctx.repo, "sp ace.ex"), "x\n")

      assert render(ctx) =~ "## File: 'sp ace.ex'\n\n@@ -0,0 +1 @@\n__new hunk__\n1 +x\n"
    end

    test "a missing file is a usage error", ctx do
      assert {_, 2} = run(ctx.repo, ["render", Path.join(ctx.dir, "missing.patch")])
    end
  end

  describe "split" do
    setup ctx do
      rendered = Path.join(ctx.dir, "rendered.txt")

      File.write!(rendered, """
      ## File: 'lib/malachi/a.ex'

      aaaaaaaaaa

      ## File: 'mix.exs'

      mm

      ## File: 'test/malachi/a_test.exs'

      tttttttttt

      ## File 'z.ex' was deleted

      """)

      %{rendered: rendered, groups: Path.join(ctx.dir, "groups")}
    end

    test "a module stays with its test, and what does not fit is listed as unreviewed", ctx do
      assert {_, 0} = run(ctx.repo, ["split", ctx.rendered, ctx.groups, "30", "2"])

      assert group(ctx, 1) =~ "'lib/malachi/a.ex'"
      assert group(ctx, 1) =~ "'test/malachi/a_test.exs'"
      assert group(ctx, 2) =~ "'mix.exs'"
      refute File.exists?(Path.join(ctx.groups, "group-3.txt"))
      assert File.read!(Path.join(ctx.groups, "unreviewed.txt")) == "z.ex\n"
    end

    test "small files share a group up to the bound, and nothing is left out when all fit", ctx do
      assert {_, 0} = run(ctx.repo, ["split", ctx.rendered, ctx.groups])

      assert group(ctx, 1) =~ "'lib/malachi/a.ex'"
      assert group(ctx, 1) =~ "'z.ex'"
      refute File.exists?(Path.join(ctx.groups, "group-2.txt"))
      assert File.read!(Path.join(ctx.groups, "unreviewed.txt")) == ""
      # Every byte of the rendered diff lands in exactly one group.
      assert byte_size(group(ctx, 1)) == byte_size(File.read!(ctx.rendered))
    end

    test "a rerun replaces the groups of the previous one", ctx do
      assert {_, 0} = run(ctx.repo, ["split", ctx.rendered, ctx.groups, "30", "5"])
      assert File.exists?(Path.join(ctx.groups, "group-3.txt"))

      assert {_, 0} = run(ctx.repo, ["split", ctx.rendered, ctx.groups])
      refute File.exists?(Path.join(ctx.groups, "group-3.txt"))
      refute File.exists?(Path.join(ctx.groups, "sections"))
    end

    test "a bound that is not a positive number is a usage error", ctx do
      assert {_, 2} = run(ctx.repo, ["split", ctx.rendered, ctx.groups, "ten"])
      assert {_, 2} = run(ctx.repo, ["split", ctx.rendered, ctx.groups, "100", "0"])
    end
  end

  test "an unknown command is a usage error", ctx do
    assert {out, 2} = run(ctx.repo, ["frobnicate"])
    assert out =~ "pin-diff.sh"
  end

  test "the script is executable in the checkout" do
    assert %File.Stat{mode: mode} = File.stat!(@script)
    assert Bitwise.band(mode, 0o111) != 0
  end

  defp pin(ctx, args \\ [], cd \\ nil), do: run(cd || ctx.repo, ["pin" | args] ++ [ctx.out])

  defp render(ctx) do
    {_, 0} = pin(ctx)
    {out, 0} = run(ctx.repo, ["render", Path.join(ctx.out, "diff.patch")])
    out
  end

  defp group(ctx, n), do: File.read!(Path.join(ctx.groups, "group-#{n}.txt"))

  defp files(ctx), do: ctx.out |> Path.join("files.txt") |> File.read!() |> String.split("\n", trim: true)

  defp base(ctx), do: ctx.out |> Path.join("base.txt") |> File.read!() |> String.trim()

  # The per-test env wins over the defaults above: a map keeps the last value given for a name.
  defp run(cd, args, env \\ []) do
    env = (@no_git_env ++ env) |> Map.new() |> Enum.to_list()
    System.cmd("bash", [@script | args], cd: cd, env: env, stderr_to_stdout: true)
  end

  defp commit!(repo, message) do
    git!(repo, ["add", "-A"])
    git!(repo, ["-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-q", "-m", message])
  end

  defp rev!(repo, rev), do: repo |> git_out!(["rev-parse", rev]) |> String.trim()

  defp git_out!(cd, args) do
    {out, 0} = System.cmd("git", args, cd: cd, env: @no_git_env)
    out
  end

  defp git!(cd, args) do
    {_out, 0} = System.cmd("git", args, cd: cd, env: @no_git_env, stderr_to_stdout: true)
    :ok
  end
end
