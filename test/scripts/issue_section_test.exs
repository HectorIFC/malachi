defmodule IssueSectionTest do
  # scripts/issue-section.sh reads the sections of a Malachi issue that tooling consumes: the branch and
  # the description in `## PR`, and the `## Plan` and `## Verification` sections. open-issue-pr builds a
  # pull request from them and pr-agent-review checks a diff against them, so a wrong read opens a PR on
  # the wrong branch or checks the diff against requirements the issue never set.
  use ExUnit.Case, async: true

  alias Malachi.Test.TmpDir

  @script Path.expand("../../scripts/issue-section.sh", __DIR__)

  # The awk programs open-issue-pr ran inline before the extraction, verbatim, so the script is shown to
  # read every fixture exactly as they did.
  @old_branch_awk ~S"""
  /^```/ { fence = !fence; if (b && !c) { c = 1; next } else if (c) { exit } next }
  c && NF { print; exit }
  fence { next }
  !p && /^## PR[[:space:]]*$/ { p = 1; next }
  !p { next }
  /^## / { exit }
  !b && /^\*\*Branch\*\*/ { b = 1 }
  """

  @old_description_awk ~S"""
  /^```/ { fence = !fence; if (d && !c) { c = 1; next } else if (c) { exit } next }
  c { print; next }
  fence { next }
  !p && /^## PR[[:space:]]*$/ { p = 1; next }
  !p { next }
  /^## / { exit }
  !d && /^\*\*Description\*\*/ { d = 1 }
  """

  @old_verification_awk ~S"""
  /^```/ { fence = !fence; if (v) print; next }
  fence { if (v) print; next }
  !v && /^## Verification[[:space:]]*$/ { v = 1; next }
  !v { next }
  /^## / { exit }
  { print }
  """

  @issue ~S"""
  ## Context

  An issue that shows the template, so its markers also appear inside a code block:

  ```
  ## PR

  **Branch**

  ```

  ## Plan

  Do the thing.

  ```
  ## Not a heading, inside a block
  ```

  - still the plan

  ## Risks and open questions

  None.

  ## Verification

  - a check
  ```
  ## still verification, inside a block
  ```
  - another check

  ## PR

  **Branch**

  ```
  feat/the-real-branch
  ```

  **Description**

  ```
  The description.

  ## A heading inside the description block
  More description.
  ```

  ```
  a later block that is not the description
  ```
  """

  setup do
    dir = TmpDir.path("issue-section")
    File.mkdir!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir, issue: write(dir, "issue.md", @issue)}
  end

  test "the branch comes from the PR section, not from the example inside a code block", ctx do
    assert run(["branch", ctx.issue]) == {"feat/the-real-branch\n", 0}
  end

  test "the description is the first block after **Description**, a heading inside it included", ctx do
    assert {out, 0} = run(["pr-description", ctx.issue])
    assert out == "The description.\n\n## A heading inside the description block\nMore description.\n"
  end

  test "the plan runs to the next heading outside a block, without its leading blank lines", ctx do
    assert {out, 0} = run(["plan", ctx.issue])
    assert String.starts_with?(out, "Do the thing.\n")
    assert out =~ "## Not a heading, inside a block\n"
    assert out =~ "- still the plan\n"
    refute out =~ "None."
  end

  test "the verification keeps a code block that holds a heading", ctx do
    assert {out, 0} = run(["verification", ctx.issue])
    assert out =~ "## still verification, inside a block\n"
    assert out =~ "- another check\n"
    refute out =~ "feat/the-real-branch"
  end

  describe "fences other than three backticks" do
    test "a tilde fence holds a heading without ending the section", ctx do
      issue =
        write(ctx.dir, "tilde.md", "## Plan\n~~~\n## Example heading\n~~~\n- Required step\n## Verification\n- check\n")

      assert run(["plan", issue]) == {"~~~\n## Example heading\n~~~\n- Required step\n", 0}
    end

    test "a four-backtick block holds a three-backtick one", ctx do
      body = "## PR\n\n**Description**\n\n````\nText\n```\ninner\n```\nmore text\n````\n"
      issue = write(ctx.dir, "four.md", body)

      assert run(["pr-description", issue]) == {"Text\n```\ninner\n```\nmore text\n", 0}
    end

    test "a backtick line does not close a tilde fence, and a fence may be indented up to three spaces", ctx do
      issue =
        write(
          ctx.dir,
          "mixed.md",
          "## Verification\n   ~~~~\n```\n## not a heading\n~~~\nstill inside\n   ~~~~\n- after\n## Next\n"
        )

      assert {out, 0} = run(["verification", issue])
      assert out =~ "## not a heading\n"
      assert out =~ "still inside\n"
      assert out =~ "- after\n"
      refute out =~ "## Next"
    end

    test "four leading spaces make an indented code line, not a fence", ctx do
      issue = write(ctx.dir, "indented.md", "## Plan\n    ```\n- step\n## Verification\n- check\n")

      assert run(["plan", issue]) == {"    ```\n- step\n", 0}
    end

    test "a body with CRLF line endings still closes its fences", ctx do
      # What the GitHub web editor can save. The section comes back byte for byte, \r included.
      body = "## Plan\r\n```\r\n## inside\r\n```\r\n- step\r\n## Verification\r\n- check\r\n"
      issue = write(ctx.dir, "crlf.md", body)

      assert run(["plan", issue]) == {"```\r\n## inside\r\n```\r\n- step\r\n", 0}
      assert run(["verification", issue]) == {"- check\r\n", 0}
    end

    test "a branch block may be a tilde fence", ctx do
      issue = write(ctx.dir, "branch.md", "## PR\n\n**Branch**\n\n~~~\nfeat/tilde\n~~~\n")

      assert run(["branch", issue]) == {"feat/tilde\n", 0}
    end
  end

  test "a missing section exits 3 and prints nothing", ctx do
    bare = write(ctx.dir, "bare.md", "## Context\n\nNothing else.\n")

    for section <- ~w(branch pr-description verification plan) do
      assert run([section, bare]) == {"", 3}, section
    end
  end

  test "a blank section exits 3", ctx do
    blank = write(ctx.dir, "blank.md", "## Plan\n\n   \n\n## Verification\n")

    assert run(["plan", blank]) == {"", 3}
  end

  test "a branch that could run as shell syntax is refused", ctx do
    for bad <- ["$(id)", "`id`", "feat/x;rm", "feat/x y", "feat/..bad"] do
      issue = write(ctx.dir, "bad.md", "## PR\n\n**Branch**\n\n```\n#{bad}\n```\n")
      assert run(["branch", issue]) == {"", 3}, bad
    end
  end

  test "a branch name spread over a newline yields only its first line, which is then checked", ctx do
    issue = write(ctx.dir, "multi.md", "## PR\n\n**Branch**\n\n```\nfeat/ok\nrm -rf /\n```\n")

    assert run(["branch", issue]) == {"feat/ok\n", 0}
  end

  test "reads every section exactly as open-issue-pr's inline awk did", ctx do
    real = write(ctx.dir, "real.md", File.read!(Path.expand("../fixtures/issue_268_body.md", __DIR__)))

    for issue <- [ctx.issue, real] do
      assert run(["branch", issue]) == {old_awk(ctx, @old_branch_awk, issue), 0}
      assert run(["pr-description", issue]) == {old_awk(ctx, @old_description_awk, issue), 0}

      {old_verification, 0} =
        System.cmd("bash", ["-c", ~s(awk -f "$0" "$1" | sed -e '/./,$!d'), awk_file(ctx, @old_verification_awk), issue])

      assert run(["verification", issue]) == {old_verification, 0}
    end
  end

  test "an unknown section or a missing file is a usage error", ctx do
    assert {_, 2} = run(["frobnicate", ctx.issue])
    assert {_, 2} = run(["plan", Path.join(ctx.dir, "missing.md")])
    assert {_, 2} = run(["plan"])
  end

  test "the script is executable in the checkout" do
    assert %File.Stat{mode: mode} = File.stat!(@script)
    assert Bitwise.band(mode, 0o111) != 0
  end

  defp old_awk(ctx, program, issue) do
    {out, 0} = System.cmd("awk", ["-f", awk_file(ctx, program), issue])
    out
  end

  defp awk_file(ctx, program), do: write(ctx.dir, "old-#{System.unique_integer([:positive])}.awk", program)

  defp write(dir, name, content) do
    path = Path.join(dir, name)
    File.write!(path, content)
    path
  end

  defp run(args), do: System.cmd("bash", [@script | args], stderr_to_stdout: true)
end
