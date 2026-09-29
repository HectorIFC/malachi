defmodule PrAgentReviewSkillTest do
  # The pr-agent-review skill is instructions, not code, so what can be checked is what it ships: the
  # upstream license it owes, a provenance header on every adapted prompt (a later sync is a diff against
  # that upstream file at that commit), no em dash anywhere (a repository rule), and the scripts it calls
  # present and executable.
  use ExUnit.Case, async: true

  @root Path.expand("../..", __DIR__)
  @skill Path.join(@root, ".claude/skills/pr-agent-review")
  @upstream_commit "10bbd9a4"

  # Every file this work added outside lib/, where no other check looks for an em dash: the skill, its
  # command, the two scripts, and the tests and fixture that come with them (this file included).
  @added [
           ".claude/skills/pr-agent-review/**/*",
           ".claude/commands/pr-agent.md",
           "scripts/pin-diff.sh",
           "scripts/issue-section.sh",
           "test/scripts/pin_diff_test.exs",
           "test/scripts/issue_section_test.exs",
           "test/scripts/pr_agent_review_skill_test.exs",
           "test/docs_config_test.exs",
           "test/fixtures/issue_268_body.md"
         ]
         |> Enum.flat_map(&Path.wildcard(Path.join(@root, &1)))
         |> Enum.filter(&File.regular?/1)

  # Written as an escape, so this file does not carry the character it forbids.
  @em_dash "\u2014"

  @prompts %{
    "diff-format.md" => "pr_agent/settings/prompt_fragments.toml",
    "review.md" => "pr_agent/settings/pr_reviewer_prompts.toml",
    "improve.md" => "pr_agent/settings/code_suggestions/pr_code_suggestions_prompts.toml",
    "reflect.md" => "pr_agent/settings/code_suggestions/pr_code_suggestions_reflect_prompts.toml",
    "describe.md" => "pr_agent/settings/pr_description_prompts.toml"
  }

  test "no added file carries an em dash" do
    assert length(@added) == 15

    for path <- @added do
      refute path |> File.read!() |> String.contains?(@em_dash), path
    end
  end

  test "every prompt that judges the code receives REVIEW.md as its review standards" do
    # SKILL.md fills skills_context with REVIEW.md. A prompt without the slot would judge the code by
    # upstream's defaults alone: the scorer once gave 0 to a correct suggestion to remove an em dash,
    # holding that no rule forbids one.
    for file <- ["review.md", "improve.md", "reflect.md", "describe.md"] do
      assert @skill |> Path.join("prompts/#{file}") |> File.read!() =~ "{{ skills_context }}", file
    end
  end

  test "PR-Agent's MIT license is shipped beside the prompts" do
    license = File.read!(Path.join(@skill, "prompts/LICENSE-pr-agent"))

    assert license =~ ~r/\AMIT License\n\nCopyright \(c\) 2026 The PR Agent\n/
    assert license =~ "The above copyright notice and this permission notice shall be included in all"
  end

  test "every adapted prompt names its upstream file and commit, and points at the license" do
    assert @skill |> Path.join("prompts/*.md") |> Path.wildcard() |> Enum.map(&Path.basename/1) |> Enum.sort() ==
             @prompts |> Map.keys() |> Enum.sort()

    for {file, upstream} <- @prompts do
      header = @skill |> Path.join("prompts/#{file}") |> File.read!() |> String.split("-->") |> hd()

      assert header =~ "Upstream: #{upstream} @ #{@upstream_commit}", file
      assert header =~ "Copyright (c) 2026 The PR Agent", file
      assert header =~ "LICENSE-pr-agent", file
      assert header =~ "Changes from upstream:", file
    end
  end

  test "the skill's name is its directory, and its description names PR-Agent and hands plain reviews on" do
    [_, frontmatter | _] = @skill |> Path.join("SKILL.md") |> File.read!() |> String.split("---\n", parts: 3)

    assert frontmatter =~ ~r/^name: pr-agent-review$/m
    [description] = Regex.run(~r/^description: (.*)$/m, frontmatter, capture: :all_but_first)
    assert description =~ "PR-Agent"
    assert description =~ "adversarial-review"
  end

  test "every prompt slot the skill fills is one the skill documents" do
    skill = File.read!(Path.join(@skill, "SKILL.md"))

    slots =
      for file <- Map.keys(@prompts),
          [_, slot] <- Regex.scan(~r/\{\{ (\w+) \}\}/, File.read!(Path.join(@skill, "prompts/#{file}"))),
          uniq: true,
          do: slot

    documented = Regex.scan(~r/`(\w+)`/, skill) |> Enum.map(&List.last/1) |> MapSet.new()

    undocumented =
      Enum.reject(slots, fn slot ->
        slot in documented or String.starts_with?(slot, "ticket_")
      end)

    assert undocumented == []
  end

  test "the scripts the skill calls exist and are executable" do
    for script <- ["scripts/pin-diff.sh", "scripts/issue-section.sh"] do
      assert %File.Stat{mode: mode} = File.stat!(Path.join(@root, script))
      assert Bitwise.band(mode, 0o111) != 0, script
    end
  end
end
