defmodule DependencyUpdatesSkillTest do
  # The dependency-updates skill is instructions, so what can be checked is what it ships and what it
  # promises: no em dash in any file this work added (a repository rule), a name and a description that
  # hand single pull request reviews to the skills that own them, the outward commands it forbids named
  # where it forbids them, and a gate table and a Dependabot configuration that agree on every
  # dependency this project declares. The last two are the drift this test exists for: a dependency
  # added to mix.exs without a tier would get the full suite in silence, and a Dependabot group that
  # disagrees with the table would hand the skill a queue shaped differently from how it verifies it.
  use ExUnit.Case, async: true

  @root Path.expand("../..", __DIR__)
  @skill Path.join(@root, ".claude/skills/dependency-updates/SKILL.md")

  @added [
           ".claude/skills/dependency-updates/SKILL.md",
           ".claude/commands/deps.md",
           ".github/dependabot.yml",
           "scripts/deps_check.exs",
           "test/scripts/deps_check_test.exs",
           "test/scripts/dependency_updates_skill_test.exs",
           "test/fixtures/deps/**/*"
         ]
         |> Enum.flat_map(&Path.wildcard(Path.join(@root, &1)))
         |> Enum.filter(&File.regular?/1)

  # Written as an escape, so this file does not carry the character it forbids.
  @em_dash "\u2014"

  setup_all do
    # Safe to require: the script only runs a subcommand when no ExUnit server is running.
    Code.require_file("scripts/deps_check.exs")
    :ok
  end

  defp frontmatter do
    [_, frontmatter | _] = @skill |> File.read!() |> String.split("---\n", parts: 3)
    frontmatter
  end

  # The dependencies mix.exs declares, read from its source as atoms in `{:name, "..."` tuples inside
  # deps/0, so the test follows the file rather than a copy of its list.
  defp declared_deps do
    source = File.read!(Path.join(@root, "mix.exs"))
    [_, body] = String.split(source, "defp deps do", parts: 2)
    [deps | _] = String.split(body, "\n  end", parts: 2)
    for [_, name] <- Regex.scan(~r/^\s*\{:(\w+),/m, deps), do: name
  end

  test "no added file carries an em dash" do
    assert length(@added) == 18

    for path <- @added do
      refute path |> File.read!() |> String.contains?(@em_dash), path
    end
  end

  test "the skill's name is its directory, and its description hands reviews and CI on" do
    assert frontmatter() =~ ~r/^name: dependency-updates$/m
    [description] = Regex.run(~r/^description: (.*)$/m, frontmatter(), capture: :all_but_first)

    assert description =~ "Dependabot"
    assert description =~ "adversarial-review"
    assert description =~ "pr-agent-review"
    assert description =~ "review-pr-feedback"
  end

  test "the skill forbids every outward command where it lists what not to do" do
    [_, rules] = @skill |> File.read!() |> String.split("## What not to do", parts: 2)

    for command <- ["git commit", "git push", "gh pr merge", "gh pr close", "gh pr comment", "gh issue create"] do
      assert rules =~ "`#{command}`", command
    end
  end

  test "every subcommand the skill runs is one the script answers" do
    used = Regex.scan(~r/scripts\/deps_check\.exs (\S+)/, File.read!(@skill), capture: :all_but_first)
    subcommands = used |> List.flatten() |> Enum.uniq() |> Enum.sort()

    assert subcommands == ["floor-action", "floor-hex", "plan", "uses", "verdict"]

    header = File.read!(Path.join(@root, "scripts/deps_check.exs"))

    for subcommand <- subcommands do
      assert header =~ ~r/^#   elixir scripts\/deps_check\.exs #{subcommand} /m, subcommand
    end
  end

  test "the input checks of step 0 run under sh and pass only the values they describe", %{} do
    [checks] = Regex.run(~r/^```sh\n(# Input checks.*?)^```$/ms, File.read!(@skill), capture: :all_but_first)
    sha = String.duplicate("a1", 20)

    cases = [
      {"check_number", ["278", "60"], ["", "1;rm -rf /", "12a", "1\n2", "$(id)"]},
      {"check_sha", [sha], ["", "not a sha", String.duplicate("a", 39), sha <> "\n" <> sha, String.upcase(sha)]},
      {"check_package", ["ra", "opentelemetry_exporter"], ["", "Ra", "x;y", "ra\nevil", "../ra"]},
      {"check_version", ["3.2.0", "1.0.0-rc.1"], ["", "3.2", "3.2.0;id", "3.2.0\n1.0.0", "v3.2.0"]},
      {"check_repo", ["actions/checkout", "docker/login-action"],
       ["", "actions", "../etc/passwd", "a/b/c", "a/b\nc/d"]},
      {"check_tag", ["v7.0.1", "v4"], ["", "..", "v1;id", "v1\nv2", "-v1"]}
    ]

    for {check, good, bad} <- cases, {values, expected} <- [{good, 0}, {bad, 1}], value <- values do
      {_, status} = System.cmd("sh", ["-c", checks <> "\n#{check} \"$1\"", "sh", value])
      assert status == expected, "#{check} #{inspect(value)} returned #{status}"
    end
  end

  test "the verdict takes the group's own tiers from the plan, never a fixed one" do
    skill = File.read!(@skill)

    assert skill =~ ~s(elixir scripts/deps_check.exs verdict "$tiers" "$S/results.json" "$head" "$main" "$tree")
    assert skill =~ "`verdict tiers:`"
    assert Regex.scan(~r/deps_check\.exs verdict (\S+)/, skill, capture: :all_but_first) == [[~s("$tiers")]]

    # The tree the verdict holds every log to: taken from the worktree through a private index before
    # the local gates (the real index may hold something else), and read back from the pushed branch as
    # a tree, not a commit, so the two compare equal exactly when the commits hold what was tested.
    assert skill =~
             ~S<tree=$(GIT_INDEX_FILE="$S/tree.idx" sh -c '/usr/bin/git read-tree HEAD && /usr/bin/git add -A && /usr/bin/git write-tree')>

    assert skill =~ ~S<tree=$(/usr/bin/git rev-parse "origin/$branch^{tree}")>
  end

  test "the worktree fetches only the lock the floor checked" do
    # A plain mix deps.get resolves whatever the lock does not satisfy, outside the container and past
    # the floor; with --check-locked it fails instead.
    skill = File.read!(@skill)

    assert skill =~ "`mix deps.get --check-locked`"
    refute skill =~ ~r/mix deps\.get(?! --check-locked)/
  end

  test "the skill runs mix deps.update only inside the disposable container" do
    # Mix evaluates a rebar3 package's rebar.config.script while it fetches, so a command that runs
    # mix deps.update anywhere but the container runs the package's code before the floor sees it.
    commands =
      for block <- Regex.scan(~r/^```[a-z]*\n(.*?)^```$/ms, File.read!(@skill), capture: :all_but_first),
          line <- block |> hd() |> String.split("\n"),
          line =~ "mix deps.update",
          do: line

    assert commands == ["  sh -c 'mix local.hex --force > /dev/null && mix deps.update <packages>'"]
  end

  test "the skill names a run's workflow by its file and reads triage workflows from origin/main" do
    skill = File.read!(@skill)

    # gh run view gives the workflow's name: (CI), never its file (ci.yml), which is what the verdict
    # knows; the run's path does.
    assert skill =~ ~S<gh api "repos/{owner}/{repo}/actions/runs/$run" --jq '.path | sub("@.*$"; "")'>

    # releases/latest is not an action's version: github/codeql-action's latest release is a CodeQL
    # bundle (codeql-bundle-v2.27.1), and a tag with no release never appears there.
    refute skill =~ "releases/latest"
    assert skill =~ ~s(gh api "repos/$owner_repo/git/matching-refs/tags/v" --paginate)
    assert skill =~ ~s(/usr/bin/git archive origin/main .github/workflows | tar -x -C "$S/main")
  end

  test "the group issue follows the issue template, in the shape start-issue-work reads" do
    [_, step4] = String.split(File.read!(@skill), "## 4. Triage: one issue per group, proposed", parts: 2)
    [step4 | _] = String.split(step4, "\n## ", parts: 2)

    assert step4 =~ "`.github/ISSUE_TEMPLATE/issue.md`"
    assert step4 =~ "`**Branch**`"
    assert step4 =~ "`**Description**`"
    assert step4 =~ "do nothing"
  end

  test "triage compares a pull request with the base before its first commit, and only Dependabot's" do
    skill = File.read!(@skill)

    # The head's first parent already holds every earlier commit of the pull request, so a commit
    # someone added on top would hide the ones before it from the floor.
    assert skill =~ ~S<gh api "repos/{owner}/{repo}/pulls/$n/commits" --paginate>
    refute skill =~ ~S<commits/$sha" --jq '.parents[0].sha'>
    assert skill =~ "dependabot[bot]"

    # author.login comes from an unverified email: a real Dependabot commit is also committed by
    # web-flow and carries a verified signature, which a commit made to look like one does not.
    assert skill =~ ".commit.verification.verified"
    assert skill =~ ~S<$3 != "web-flow">

    # The list is the pull request's commits now; its last one must be the head recorded earlier, or
    # the head moved and the two sides would come from different bases.
    assert skill =~ ~S<last=$(tail -1 "$S/commits$n.tsv" | cut -f1)>
  end

  test "the skill proposes only Dependabot ignore commands GitHub documents" do
    skill = File.read!(@skill)

    refute skill =~ "@dependabot ignore <package> <version>"
    assert skill =~ "`@dependabot ignore <package> <major|minor|patch> version`"
    assert skill =~ "`@dependabot unignore <package>`"
  end

  # What each gate looks like in a workflow step's run:, for the gates a workflow run is taken to prove.
  # nil is the run itself (pull request CI). The named suites and plain mix test run inside the
  # coverage step.
  @gate_commands %{
    "mix deps.unlock --check-unused" => "mix deps.unlock --check-unused",
    "mix compile --warnings-as-errors" => "mix compile --warnings-as-errors",
    "mix format --check-formatted" => "mix format --check-formatted",
    "mix test" => "mix coveralls.json",
    "mix coveralls" => "mix coveralls.json",
    "mix dialyzer" => "mix dialyzer",
    "mix docs --warnings-as-errors" => "mix docs --warnings-as-errors",
    "scripts/docker-static-assets-check.sh" => "scripts/docker-static-assets-check.sh",
    "mix test --only multinode" => "mix test --only multinode",
    "mix sobelow --config" => "mix sobelow --config\n",
    "mix deps.audit" => "mix deps.audit",
    "scripts/docker-chaos-test.sh" => "scripts/docker-chaos-test.sh",
    "scripts/docker-storage-chaos.sh" => "scripts/docker-storage-chaos.sh",
    "scripts/docker-upgrade-chaos.sh" => "scripts/docker-upgrade-chaos.sh",
    "pull request CI" => nil
  }

  test "every gate a workflow run is taken to prove is a blocking step of that workflow" do
    for {workflow, gates} <- DepsCheck.proven_by(), gate <- gates do
      command =
        if String.starts_with?(gate, "mix test test/"),
          do: "mix coveralls.json",
          else: Map.fetch!(@gate_commands, gate)

      if command do
        steps = workflow_steps(Path.join(@root, ".github/workflows/#{workflow}"))

        assert Enum.any?(steps, fn step ->
                 String.contains?(step["run"] || "", command) and step["continue-on-error"] != true
               end),
               "#{workflow} has no blocking step running #{inspect(command)} for #{gate}"
      end
    end
  end

  defp workflow_steps(path) do
    [document] = :yamerl_constr.file(String.to_charlist(path), [:str_node_as_binary])
    {"jobs", jobs} = List.keyfind(document, "jobs", 0)

    for {_job, settings} <- jobs,
        {"steps", steps} <- [List.keyfind(settings, "steps", 0)],
        step <- steps,
        do: Map.new(step)
  end

  test "every dependency mix.exs declares has a tier in the gate table" do
    deps = declared_deps()
    assert "ra" in deps and "joken" in deps and length(deps) > 15

    assert deps -- Map.keys(DepsCheck.tiers_table()) == []
  end

  test "the Dependabot groups put every declared dependency where the skill groups it" do
    groups = mix_groups(File.read!(Path.join(@root, ".github/dependabot.yml")))

    assert Enum.map(groups, &elem(&1, 0)) == ["hex-auth", "hex"]

    for {_name, group} <- groups do
      assert group["update-types"] == ["minor", "patch"], "a major must arrive alone, as a decision"
    end

    for dep <- declared_deps() do
      expected = DepsCheck.group_for(DepsCheck.tiers_of(dep, %{}))
      assert dependabot_group(groups, dep) == expected, dep
    end
  end

  test "the actions arrive as one group of every action" do
    updates = YamlElixir.read_from_file!(Path.join(@root, ".github/dependabot.yml"))["updates"]
    actions = Enum.find(updates, &(&1["package-ecosystem"] == "github-actions"))

    assert actions["groups"] == %{"actions" => %{"patterns" => ["*"]}}
  end

  test "the group check follows the file's order, as Dependabot does" do
    # hex listed first with no exclusions takes joken, whatever hex-auth below it says.
    inverted = """
    updates:
      - package-ecosystem: "mix"
        groups:
          hex:
            patterns: ["*"]
          hex-auth:
            patterns: ["joken"]
    """

    assert inverted |> mix_groups() |> dependabot_group("joken") == "hex"
  end

  # The mix groups in the order the file lists them. Dependabot puts a dependency in the first group
  # that matches it, so the order is part of the configuration. YamlElixir reverses mapping keys even
  # with maps_as_keywords, so the file is read with yamerl, which keeps them as written.
  defp mix_groups(yaml) do
    [document] = :yamerl_constr.string(String.to_charlist(yaml), [:str_node_as_binary])
    {"updates", updates} = List.keyfind(document, "updates", 0)
    mix = Enum.find(updates, &(List.keyfind(&1, "package-ecosystem", 0) == {"package-ecosystem", "mix"}))
    {"groups", groups} = List.keyfind(mix, "groups", 0)
    for {name, settings} <- groups, do: {name, Map.new(settings)}
  end

  # The group Dependabot gives a dependency: the first group, in the file's order, whose patterns match
  # it and whose exclusions do not, or "ra" for none (ra is the one left ungrouped on purpose). Only
  # exact names and "*" are used in the file, so only those are matched here.
  defp dependabot_group(groups, dep) do
    match = fn patterns -> Enum.any?(patterns || [], &(&1 == "*" or &1 == dep)) end

    Enum.find_value(groups, "ra", fn {name, group} ->
      if match.(group["patterns"]) and not match.(group["exclude-patterns"]), do: name
    end)
  end
end
