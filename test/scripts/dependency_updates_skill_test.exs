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

  test "no mix command of the skill runs on the host: fetch and local gates run in the box" do
    # The floor proves a package is what hex.pm published, not that its code is benign: once the
    # lock passes, fetching and compiling run that code, so they run in a disposable container with
    # no home, no credentials and no Docker socket, never in the worktree on the host.
    skill = File.read!(@skill)

    host_mix =
      for block <- Regex.scan(~r/^```[a-z]*\n(.*?)^```$/ms, skill, capture: :all_but_first),
          line <- block |> hd() |> String.split("\n"),
          not (line =~ ~r/in_box(_net)?\b|sh -c/),
          line =~ ~r/(^|&&|\|\||;|\||\$\()\s*(MIX_ENV=\S+\s+)?mix\s/,
          do: line

    assert host_mix == []

    # The prose counts as much as the commands: every paragraph that tells to fetch says where, and it
    # is the box.
    prose = Regex.replace(~r/^```[a-z]*\n.*?^```$/ms, skill, "")

    for paragraph <- String.split(prose, ~r/\n\s*\n/), paragraph =~ "mix deps.get" do
      assert paragraph =~ "box", paragraph
    end

    assert skill =~
             ~S[in_box_net 'mix local.hex --force > /dev/null && mix local.rebar --force > /dev/null && mix deps.get --check-locked']

    # Only the fetch has a network; every gate runs without one, so dependency code cannot reach this
    # machine's loopback services (Colima forwards host.docker.internal to them).
    assert skill =~ ~S[in_box() {
  docker run --rm --network none]

    net_calls = Regex.scan(~r/^in_box_net '/m, skill)
    assert length(net_calls) == 1, "only the fetch may run with a network"

    # The image gate and the drills build and run dependency code with a network: the skill says so
    # instead of promising every gate runs without one.
    refute skill =~ "every gate runs with no network at all"
    assert skill =~ "declare that exposure"

    # The step 7 rule that keeps prepare-commits' checks off the host.
    assert skill =~
             "`mix format --check-formatted` and `mix credo --strict`\nrun in the box, and `mix test` is the pull request's CI"
  end

  test "every container the skill starts sees the copy it works on and nothing else of this machine" do
    # Each docker run is read whole, continuation lines included, and its only bind mount must be the
    # copy: no home directory, no credentials directory, no Docker socket.
    commands =
      File.read!(@skill)
      |> String.replace("\\\n", " ")
      |> String.split("\n")
      |> Enum.filter(&(&1 =~ "docker run"))
      |> Enum.reject(&(&1 =~ ~r/^\s*\S+.*`docker run/))

    assert length(commands) >= 3

    # A container either has no network at all, or the box network, whose traffic to the Mac
    # (192.168.5.2, where Colima forwards host.docker.internal) the VM's firewall drops: never the
    # default network, which reaches every service this Mac listens on at its loopback.
    for command <- commands do
      assert command =~ "--network none" or command =~ "--network malachi-box-net", command
    end

    skill = File.read!(@skill)
    assert skill =~ ~S[docker network create --subnet 172.31.250.0/24 malachi-box-net]
    # The firewall is an allowlist: every private destination and the VM itself are dropped from the
    # box subnet, so neither the Mac (its loopback forward or its LAN address) nor a port published in
    # the VM answers, and only the internet, hex.pm among it, does.
    for destination <- ~w(10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 169.254.0.0/16 100.64.0.0/10) do
      assert skill =~ "colima ssh -- sudo iptables -I DOCKER-USER -s 172.31.250.0/24 -d #{destination} -j DROP"
    end

    assert skill =~ ~S[colima ssh -- sudo iptables -I INPUT -s 172.31.250.0/24 -j DROP]

    # The probe is a guard that fails, not a message: it covers the Mac's forward, the network's gateway
    # and the Mac's LAN address, and every container with a network runs only after it passed.
    assert skill =~ ~S[for ip in 192.168.5.2 "$gw" $lan; do]
    assert skill =~ ~S[| grep -q 'timed out' || { echo "stop: the box network reaches $ip"; return 1; }]

    for block <- Regex.scan(~r/^```[a-z]*\n(.*?)^```$/ms, skill, capture: :all_but_first) |> Enum.map(&hd/1),
        [line] <- Regex.scan(~r/^.*--network malachi-box-net.*$/m, block),
        not (line =~ "wget -T 3") do
      [before | _] = String.split(block, line, parts: 2)
      guarded_here = before =~ "box_net_ok || exit 1"
      guarded_in_function = before =~ ~r/in_box_net\(\) \{[^}]*box_net_ok \|\| return 1[^}]*$/s
      assert guarded_here or guarded_in_function, "unguarded networked container: " <> line
    end

    for command <- commands do
      mounts = Regex.scan(~r/(?:-v|--volume|--mount)[\s=]+(\S+)/, command, capture: :all_but_first) |> List.flatten()

      # Every mount flag must be one the regex read, so an attached `-v"$HOME":/w` or a
      # `--volumes-from` cannot slip past as "no mount"; only the network probe mounts nothing.
      flags = Regex.scan(~r/\s(?:-v|--volume|--mount)\b/, command) |> length()
      assert flags == length(mounts), command
      refute command =~ "--volumes-from", command

      # The box gets the worktree copy, the resolver the HEAD copy, actionlint a copy of the workflows.
      copies = [[~s("$B":/w)], [~s("$R":/w)], [~s("$W":/w)]]
      allowed = if command =~ "wget -T 3", do: [[]], else: copies
      assert mounts in allowed, command
      refute command =~ "docker.sock", command
    end
  end

  test "actionlint runs only in a pinned Linux container with no network, on copies of the workflows" do
    skill = File.read!(@skill)
    blocks = Regex.scan(~r/^```[a-z]*\n(.*?)^```$/ms, skill, capture: :all_but_first) |> Enum.map(&hd/1)

    # A run on this Mac would carry host darwin, which the verdict refuses, so the gate could never
    # pass without a false host: no fenced line starts actionlint on the host.
    refute Enum.any?(blocks, &(&1 =~ ~r/^\s*actionlint\b/m))

    [run] =
      skill
      |> String.replace("\\\n", " ")
      |> String.split("\n")
      |> Enum.filter(&(&1 =~ "docker run" and &1 =~ "actionlint"))

    assert run =~ "--network none"
    assert run =~ ~r/rhysd\/actionlint@sha256:[0-9a-f]{64}/, "the image is pinned by digest"
    assert run =~ ~S[-c "actionlint -no-color -format '{{json .}}' .github/workflows/*.yml"]

    # The group and main run in the same image, from copies, so a finding main already had is not new.
    assert skill =~ ~S[cp -R .github/workflows "$A/group/.github/"]
    assert skill =~ ~S[/usr/bin/git archive origin/main .github/workflows | tar -x -C "$A/main"]
    assert skill =~ ~S[comm -23 "$S/al-group" "$S/al-main"]
  end

  test "the box helpers live in one block, and every call of them stops the run on a failure" do
    skill = File.read!(@skill)
    blocks = Regex.scan(~r/^```[a-z]*\n(.*?)^```$/ms, skill, capture: :all_but_first) |> Enum.map(&hd/1)

    # Shell functions do not survive between two calls of the agent's shell tool: the three helpers
    # are defined together, and the skill says to run them in the same call as their users.
    assert Enum.any?(blocks, &(&1 =~ "box_net_ok() {" and &1 =~ "in_box_net() {" and &1 =~ "in_box() {"))
    assert skill =~ "in the same shell call"

    calls = Regex.scan(~r/^in_box(?:_net)? '.*$/m, skill) |> List.flatten()
    assert length(calls) >= 3

    for call <- calls do
      [_, log] = Regex.run(~r/> "(\$S\/results\/[^"]+)" 2>&1/, call)
      assert String.ends_with?(call, ~s(|| { cat "#{log}"; exit 1; })), call
    end

    # What the guard checks, said as it is: it does not prove every rule is installed.
    refute skill =~ "proves the rules are in place"
  end

  test "prepare-commits keeps a changed lock off the host too" do
    # A session that loads only prepare-commits (a later /commits) must not run mix test on the host
    # against a lock the dependency-updates floor checked but the host never fetched.
    prepare = File.read!(Path.join(@root, ".claude/skills/prepare-commits/SKILL.md"))

    assert prepare =~ "dependency-updates"
    assert prepare =~ ~S[/usr/bin/git diff --quiet "$(/usr/bin/git merge-base origin/main HEAD)" -- mix.lock]

    # A later session has no box of its own: it builds a new one from the tree as it is now, fetches
    # with the network, and runs the checks without it.
    assert prepare =~ "a new box"
    assert prepare =~ "in_box_net"
    refute prepare =~ "in its box"
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

        assert Enum.any?(steps, &(String.contains?(&1.run, command) and blocking?(&1))),
               "#{workflow} has no blocking step running #{inspect(command)} for #{gate}"
      end
    end
  end

  # A step blocks when neither it nor its job may fail, and when, for at least one entry of the job's
  # matrix, both the job's and the step's `if` let it run. A run whose gate step was skipped still ends
  # green, and the verdict would take it as proof of a gate that never ran.
  defp blocking?(step) do
    step.continue_on_error == [] and
      Enum.any?(step.matrix, fn entry -> Enum.all?(step.conditions, &runs?(&1, entry)) end)
  end

  # Only the forms the workflows use are read; any other condition on a mapped step fails the test
  # rather than being guessed at.
  defp runs?(condition, entry) do
    case Regex.run(~r/^(?:\$\{\{\s*)?(!?)\s*matrix\.([a-z_-]+)\s*(?:\}\})?$/, String.trim(to_string(condition))) do
      [_, "", key] -> entry[key] == true
      [_, "!", key] -> entry[key] != true
      nil -> flunk("a gate step runs under a condition this test cannot read: #{condition}")
    end
  end

  defp workflow_steps(path) do
    [document] = :yamerl_constr.file(String.to_charlist(path), [:str_node_as_binary])
    {"jobs", jobs} = List.keyfind(document, "jobs", 0)

    for {_job, settings} <- jobs,
        job = Map.new(settings),
        step <- Map.get(job, "steps", []),
        step = Map.new(step) do
      %{
        run: step["run"] || "",
        conditions: Enum.reject([job["if"], step["if"]], &is_nil/1),
        continue_on_error:
          Enum.filter([job["continue-on-error"], step["continue-on-error"]], &(&1 not in [nil, false])),
        matrix: matrix_entries(job["strategy"])
      }
    end
  end

  # The entries a job runs for: every combination of its axes, plus its `include` entries, or one empty
  # entry when it has no matrix. An `exclude` is not read, so a matrix with one fails the test instead
  # of counting an entry that never runs.
  defp matrix_entries(nil), do: [%{}]

  defp matrix_entries(strategy) do
    matrix = Map.new(Map.new(strategy)["matrix"] || [])
    refute Map.has_key?(matrix, "exclude"), "a matrix exclude is not read"
    {include, axes} = Map.pop(matrix, "include", [])

    combinations =
      Enum.reduce(axes, [%{}], fn {key, values}, acc ->
        for entry <- acc, value <- values, do: Map.put(entry, key, value)
      end)

    combinations = if axes == %{} and include != [], do: [], else: combinations
    combinations ++ Enum.map(include, &Map.new/1)
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
