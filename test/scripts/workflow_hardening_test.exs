defmodule WorkflowHardeningTest do
  # The CI hardening of #303 is a shape, not a behaviour: every job starts with the StepSecurity runner in audit mode,
  # the runner is pinned by commit SHA, and the Scorecard job is the one place that may publish results. Those are
  # the properties a later edit can lose silently (a new job added without the runner, a tag swapped in for a SHA,
  # a second job quietly given id-token). This reads the workflow files as text, as dependency_updates_skill_test
  # does, because the repository has no YAML parser among its test dependencies.
  use ExUnit.Case, async: true

  @root Path.expand("../..", __DIR__)
  @workflows Path.join(@root, ".github/workflows")

  @harden_runner "step-security/harden-runner"

  defp workflow_files, do: Path.wildcard(Path.join(@workflows, "*.yml"))
  defp read(file), do: File.read!(Path.join(@workflows, file))

  # Every job's `steps:` key sits at four spaces. The step that follows it is the job's first step.
  defp job_first_steps(text) do
    Regex.scan(~r/^    steps:\n      - name: [^\n]*\n        uses: ([^\n]+)\n/m, text, capture: :all_but_first)
    |> Enum.map(fn [uses] -> uses end)
  end

  defp jobs_with_steps(text) do
    Regex.scan(~r/^    steps:$/m, text) |> length()
  end

  defp uses_names(text) do
    Regex.scan(~r/uses: ([^@\s]+)@/, text, capture: :all_but_first)
    |> Enum.map(fn [name] -> name end)
    |> MapSet.new()
  end

  describe "StepSecurity Harden-Runner" do
    test "every job in every workflow starts with Harden-Runner" do
      for file <- workflow_files() do
        text = File.read!(file)
        firsts = job_first_steps(text)

        assert length(firsts) == jobs_with_steps(text),
               "#{Path.basename(file)}: #{jobs_with_steps(text)} jobs have steps but only #{length(firsts)} start with a " <>
                 "uses: step as their first step, so a job lacks Harden-Runner"

        for uses <- firsts do
          assert String.starts_with?(uses, @harden_runner <> "@"),
                 "#{Path.basename(file)}: a job starts with #{uses}, not Harden-Runner"
        end
      end
    end

    test "Harden-Runner is pinned by a full commit SHA, with the release it came from in a comment" do
      for file <- workflow_files(),
          line <- String.split(read(Path.basename(file)), "\n"),
          String.contains?(line, @harden_runner) do
        assert line =~ ~r/#{@harden_runner}@[0-9a-f]{40} # v\d+\.\d+\.\d+\z/,
               "#{Path.basename(file)}: Harden-Runner is not pinned by SHA with its version: #{String.trim(line)}"
      end
    end

    test "every job runs Harden-Runner in audit mode, so nothing is blocked before an endpoint baseline exists" do
      for file <- workflow_files() do
        text = read(Path.basename(file))
        modes = Regex.scan(~r/egress-policy: (\S+)/, text, capture: :all_but_first) |> List.flatten()

        assert Enum.all?(modes, &(&1 == "audit")),
               "#{Path.basename(file)} sets egress-policy to #{inspect(Enum.uniq(modes))}; block is a follow-up issue"

        assert length(modes) == jobs_with_steps(text),
               "#{Path.basename(file)}: #{length(modes)} egress-policy lines for #{jobs_with_steps(text)} jobs"
      end
    end
  end

  describe "OpenSSF Scorecard workflow" do
    setup do
      %{text: read("scorecard.yml")}
    end

    test "is not triggered by pull_request or workflow_dispatch, which the scorecard action does not publish from", %{
      text: text
    } do
      refute text =~ ~r/^\s+pull_request/m
      refute text =~ ~r/^\s+workflow_dispatch/m
      refute text =~ "pull_request_target"
    end

    test "grants no permission beyond read at the top level", %{text: text} do
      assert text =~ ~r/^permissions: read-all$/m
    end

    test "grants id-token and security-events only to its analysis job", %{text: text} do
      assert text =~ ~r/^    permissions:\n      security-events: write\n      id-token: write\n      contents: read$/m
    end

    test "uses only the actions scorecard-action allows when results are published", %{text: text} do
      allowed =
        MapSet.new([
          @harden_runner,
          "actions/checkout",
          "ossf/scorecard-action",
          "actions/upload-artifact",
          "github/codeql-action/upload-sarif"
        ])

      assert uses_names(text) == allowed
    end

    test "publishes the results, and writes them as SARIF for the Security tab", %{text: text} do
      assert text =~ "publish_results: true"
      assert text =~ "results_format: sarif"
      assert text =~ ~r/sarif_file: results\.sarif/
    end

    test "does not keep credentials after checkout", %{text: text} do
      assert text =~ ~r/uses: actions\/checkout@v\d\n\s+with:\n\s+persist-credentials: false/
    end

    test "is the only workflow besides Pages that holds id-token" do
      holders =
        workflow_files()
        |> Enum.filter(&(read(Path.basename(&1)) =~ "id-token: write"))
        |> Enum.map(&Path.basename/1)
        |> Enum.sort()

      assert holders == ["pages.yml", "scorecard.yml"]
    end
  end
end
