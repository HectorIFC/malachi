defmodule AdversarialReviewSkillTest do
  # The adversarial-review skill, REVIEW.md's round rules and the two skills that start or report a round are
  # instructions, not code. What can be checked is what they ship and how they agree: the round rules are
  # stated once, in REVIEW.md; the first round keeps the full criteria, so the managed Code Review on a pull
  # request (always a first review) is unchanged; the skill points at REVIEW.md rather than repeating it; the
  # cap lives in review-state.sh alone; and each script the skill calls exists and is executable.
  use ExUnit.Case, async: true

  @root Path.expand("../..", __DIR__)
  @review Path.join(@root, "REVIEW.md")
  @skill Path.join(@root, ".claude/skills/adversarial-review/SKILL.md")
  @prepare Path.join(@root, ".claude/skills/prepare-commits/SKILL.md")
  @open_pr Path.join(@root, ".claude/skills/open-issue-pr/SKILL.md")

  # The six items of the Important list, as REVIEW.md words them. The first round keeps every one.
  @important [
    "acknowledged data can be lost, misreported",
    "a destructive act (removing a directory, deleting a segment, fencing, sealing)",
    "a crash, deadlock or unbounded wait",
    "a divergence from the NorthGuard design above",
    "a test presented as a regression guard that passes without the change",
    "documentation that promises a guarantee the code does not deliver"
  ]

  # The round rules from round 2 on, as REVIEW.md words them. The skill must not say them again.
  @round_rules [
    "Only behaviour is important",
    "A documentation or comment fix is a nit",
    "A finding is raised only on lines the delta changed",
    "recommends narrowing the promise",
    "is not raised again unless the lines"
  ]

  test "REVIEW.md states the rounds in one section, and a prompt with no round is the first round" do
    review = File.read!(@review)

    assert review =~ "## Rounds"
    refute review =~ "## Re-review"
    assert review =~ "A review whose prompt names no round is the first round"
    assert review =~ "always that: a pull"
  end

  test "the first round keeps all six items of the Important list" do
    severity = section(File.read!(@review), "## Severity", "## Nit volume")

    for item <- @important do
      assert severity =~ item, "the Important list lost: #{item}"
    end
  end

  test "the round rules are stated in REVIEW.md, and only there" do
    review = File.read!(@review)
    rounds = section(review, "## Rounds", "## Summary shape")

    for rule <- @round_rules do
      assert rounds =~ rule, "REVIEW.md no longer states: #{rule}"
    end

    skill = File.read!(@skill)

    for rule <- @round_rules do
      refute skill =~ rule, "the skill repeats a round rule that REVIEW.md states: #{rule}"
    end
  end

  test "the skill points at REVIEW.md's Rounds section by name" do
    assert File.read!(@skill) =~ ~s(Its section "Rounds")
  end

  test "the cap is written only in review-state.sh, not in the skill's own text" do
    skill = File.read!(@skill)

    refute skill =~ ~r/\bof 3\b/
    refute skill =~ ~r/\bthree rounds\b/
    assert skill =~ "review-state.sh"
    assert File.read!(Path.join(@root, "scripts/review-state.sh")) =~ "CAP=3"
  end

  test "the skill runs the round through review-state.sh and cuts the delta with pin-diff.sh" do
    skill = File.read!(@skill)

    assert skill =~ "review-state.sh\" begin"
    assert skill =~ "review-state.sh\" done"
    assert skill =~ "review-state.sh\" decide"
    assert skill =~ "review-state.sh\" reviewed"
    assert skill =~ "pin-diff.sh\" pin --since"
    assert skill =~ "decided.tsv"
  end

  test "the skill gives the verifiers the plan and the reviewers not" do
    skill = File.read!(@skill)

    assert skill =~ "issue-section.sh\" plan"
    assert skill =~ "goes only to the verifiers"
    assert skill =~ "the plan file if one is given"
  end

  test "the skill marks the round done after its report, so the reviewed tree is the one it read" do
    skill = File.read!(@skill)
    [before_done, _] = String.split(skill, "review-state.sh\" done", parts: 2)

    assert before_done =~ "present every finding"
  end

  test "the round label follows the pin step 1 made, not the round number" do
    # A later round with no reviewed tree pins the whole branch: labelled as a delta, its reviewers would
    # apply the delta's scope rule to lines the branch changed and drop findings there.
    skill = File.read!(@skill)

    assert skill =~ "when step 1 pinned the whole branch"
    assert skill =~ "when step 1 pinned the delta"
    assert skill =~ "according to the pin step 1 made"
    refute skill =~ "after it in later rounds"
    refute skill =~ "in a later round, or"
  end

  test "prepare-commits asks for the status and starts round 1 when none has run" do
    prepare = File.read!(@prepare)

    assert prepare =~ "review-state.sh\" status"
    assert prepare =~ "`rounds: 0`"
    assert prepare =~ "adversarial-review"
    assert prepare =~ "begins round 1"
  end

  test "open-issue-pr reports the review and never starts one" do
    open_pr = File.read!(@open_pr)

    assert open_pr =~ "review-state.sh\" status"
    refute open_pr =~ "review-state.sh\" begin"
    refute open_pr =~ "adversarial-review skill"
    assert open_pr =~ "start a round from here"
  end

  test "the scripts the skill calls exist and are executable" do
    for script <- ["scripts/review-state.sh", "scripts/pin-diff.sh", "scripts/auto-review-hook.sh"] do
      assert %File.Stat{mode: mode} = File.stat!(Path.join(@root, script))
      assert Bitwise.band(mode, 0o111) != 0, script
    end
  end

  test "the hook's shared library is in the checkout and read by the scripts that need it" do
    assert File.exists?(Path.join(@root, "scripts/review_lib.sh"))

    for script <- ["scripts/review-state.sh", "scripts/pin-diff.sh"] do
      assert File.read!(Path.join(@root, script)) =~ "review_lib.sh", script
    end
  end

  # The text from the line starting with `start` up to, not including, the line starting with `stop`.
  defp section(text, start, stop) do
    [_, rest] = String.split(text, start, parts: 2)
    [body | _] = String.split(rest, stop, parts: 2)
    body
  end
end
