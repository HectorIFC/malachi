defmodule Mix.Tasks.Malachi.Docs.ResultsTest do
  # Writes into a tmp_dir and reads Mix.shell messages, both of which are per-test. Not async only
  # because Mix.shell/1 is global process state.
  use ExUnit.Case, async: false

  alias Mix.Tasks.Malachi.Docs.Results

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    on_exit(fn -> Mix.shell(shell) end)

    published = Path.join(tmp_dir, "published")
    output = Path.join(tmp_dir, "generated")
    File.mkdir_p!(published)

    {:ok, published: published, output: output}
  end

  defp run(%{published: published, output: output}) do
    Results.run(["--published-dir", published, "--output-dir", output])
  end

  defp publish(%{published: published}, name, content) when is_binary(content) do
    File.write!(Path.join(published, name), content)
  end

  defp publish(context, name, content), do: publish(context, name, Jason.encode!(content))

  defp page(%{output: output}, name), do: File.read!(Path.join(output, name))

  defp loadtest_result(overrides \\ %{}) do
    Map.merge(
      %{
        "meta" => %{
          "command" => "mix malachi.loadtest --scenario produce",
          "timestamp" => "2026-08-22T00:00:00Z",
          "git_ref" => "abc1234",
          "git_ref_date" => "2026-08-21T12:00:00Z",
          "malachi_version" => "0.8.1",
          "hardware" => %{"cpu" => "aarch64", "cores" => 8, "os" => "darwin 25.5.0"}
        },
        "scenario" => "produce",
        "connections" => 20,
        "duration_s" => 10,
        "records" => 258_890,
        "ops" => 25_889,
        "records_per_s" => 25_889,
        "ops_per_s" => 2589,
        "mb_per_s" => 6.32,
        "errors" => 0,
        "latency_ms" => %{"p50" => 6.05, "p99" => 28.77}
      },
      overrides
    )
  end

  defp chaos_result(overrides \\ %{}) do
    Map.merge(
      %{
        "meta" => %{"command" => "scripts/docker-chaos-test.sh", "git_ref" => "abc1234"},
        "certification" => "CHAOS CERTIFICATION",
        "verdict" => "passed",
        "replication_factor" => 3,
        "events" => ["a: power pull", "b: partition"],
        "invariants" => %{"acked_writes" => 2851, "post_chaos_records_per_s" => 3047},
        "failures" => []
      },
      overrides
    )
  end

  describe "when no result has been recorded" do
    test "every page still renders, saying so and pointing at the guide", context do
      run(context)

      for {file, guide} <- [
            {"loadtest-node-results.md", "running-the-node-loadtest.md"},
            {"loadtest-elixir-results.md", "running-the-elixir-loadtest.md"},
            {"chaos-results.md", "running-chaos-drills.md"}
          ] do
        body = page(context, file)
        assert body =~ "No run has been recorded yet"
        assert body =~ guide
      end
    end

    test "the page names the file the task actually looked at", context do
      # It used to name the default directory whatever the caller passed, so with --published-dir the
      # page sent a reader to a path the task never opened.
      run(context)

      body = page(context, "chaos-results.md")
      assert body =~ Path.join(context.published, "chaos-node.json")
      refute body =~ "benchmark/published"
    end
  end

  describe "when a source is unreadable" do
    test "invalid JSON fails the build rather than publishing around it", context do
      publish(context, "loadtest-node.json", "{not json at all")

      # A missing file is an ordinary state; a file that exists and does not parse means something
      # wrote garbage where a result belongs, and generating a page over that would hide it.
      assert_raise Mix.Error, ~r/loadtest-node\.json is not valid JSON/, fn -> run(context) end
    end
  end

  describe "load test pages" do
    test "render the headline, the numbers and the reproduce block", context do
      publish(context, "loadtest-elixir.json", loadtest_result())
      run(context)

      body = page(context, "loadtest-elixir-results.md")

      # Grouped digits: the headline is read at a glance.
      assert body =~ "**25,889 records per second**"
      assert body =~ "| Records per second | 25,889 |"
      assert body =~ "| Data rate | 6.32 MB/s |"
      assert body =~ "| P50 | 6.05 ms |"
      assert body =~ "| Commit | `abc1234` (2026-08-21T12:00:00Z) |"
      assert body =~ "| Cores | 8 |"
    end

    test "a percentile the run did not record is dropped, not rendered blank", context do
      # The Node client keeps a full histogram and the BEAM one keeps four percentiles. A blank row
      # would read as measured and zero, which is a different claim from not measured.
      publish(context, "loadtest-elixir.json", loadtest_result())
      run(context)

      body = page(context, "loadtest-elixir-results.md")

      assert body =~ "| P99 | 28.77 ms |"
      refute body =~ "P99.9"
      refute body =~ "| Maximum |"
    end

    test "the backpressure section appears only for a run that counted it", context do
      publish(context, "loadtest-node.json", loadtest_result())
      publish(context, "loadtest-elixir.json", loadtest_result(%{"dropped" => 0, "overloaded" => 3, "reconnects" => 1}))
      run(context)

      refute page(context, "loadtest-node-results.md") =~ "## Backpressure"

      elixir = page(context, "loadtest-elixir-results.md")
      assert elixir =~ "## Backpressure"
      assert elixir =~ "| Server-shed produces | 3 |"
      assert elixir =~ "| Reconnects | 1 |"
    end

    test "a result without a meta block still renders", context do
      publish(context, "loadtest-node.json", loadtest_result() |> Map.delete("meta"))
      run(context)

      body = page(context, "loadtest-node-results.md")
      assert body =~ "**25,889 records per second**"
      assert body =~ "| Recorded | without metadata |"
    end

    test "the ceiling attribution renders: peak connections, server CPU and the lower-bound note", context do
      publish(
        context,
        "loadtest-node.json",
        loadtest_result(%{
          "connections" => 128,
          "server_cpu_cores" => 2.91,
          "server_cpu_budget" => 3,
          "peak_at_ladder_limit" => true
        })
      )

      run(context)
      body = page(context, "loadtest-node-results.md")

      assert body =~ "peaking at 128 connections (server at 2.91 of 3 cores)"
      assert body =~ "| Peak connections | 128 |"
      assert body =~ "| Server CPU (cores) | 2.91 of 3 |"
      assert body =~ "This is a lower bound"
    end

    test "server CPU is dropped when a run did not sample it, and a non-limited peak is not called a lower bound",
         context do
      # macOS smoke and any run without /proc leave these unset; a blank row would read as measured zero,
      # and the lower-bound caveat must appear only for a peak that actually hit the ladder limit.
      publish(context, "loadtest-node.json", loadtest_result(%{"peak_at_ladder_limit" => false}))
      run(context)

      body = page(context, "loadtest-node-results.md")
      refute body =~ "Server CPU (cores)"
      refute body =~ "lower bound"
    end
  end

  describe "the chaos page" do
    test "renders a passing certification with its faults and invariants", context do
      publish(context, "chaos-node.json", chaos_result())
      run(context)

      body = page(context, "chaos-results.md")

      assert body =~ "**CHAOS CERTIFICATION passed** at replication factor 3"
      assert body =~ "through 2 injected faults"
      assert body =~ "- a: power pull"
      assert body =~ "| Acknowledged writes still readable | 2,851 |"
      assert body =~ "| Post-chaos produce | 3,047 records/s |"
      refute body =~ "## Failures"
    end

    test "renders a failed certification with every broken invariant", context do
      publish(
        context,
        "chaos-node.json",
        chaos_result(%{"verdict" => "failed", "failures" => ["acked writes were lost", "did not reconverge"]})
      )

      run(context)
      body = page(context, "chaos-results.md")

      assert body =~ "**CHAOS CERTIFICATION FAILED** at replication factor 3"
      assert body =~ "## Failures"
      assert body =~ "- acked writes were lost"
      assert body =~ "- did not reconverge"
    end

    test "an invariant the drill did not measure is omitted rather than shown as zero", context do
      # The storage and config drills certify other things and leave these null. Rendering null as 0
      # would claim they measured nothing rather than that they measured nothing of this kind.
      publish(
        context,
        "chaos-node.json",
        chaos_result(%{"invariants" => %{"acked_writes" => nil, "post_chaos_records_per_s" => nil}})
      )

      run(context)
      body = page(context, "chaos-results.md")

      refute body =~ "Acknowledged writes still readable"
      refute body =~ "Post-chaos produce"
    end

    test "a drill that injected nothing says so instead of leaving an empty list", context do
      publish(context, "chaos-node.json", chaos_result(%{"events" => []}))
      run(context)

      assert page(context, "chaos-results.md") =~ "None recorded."
    end
  end

  describe "table cells" do
    test "a pipe in a recorded value does not split the row into extra columns", context do
      # The command and the CPU model both come from outside this module and both land in a cell. An
      # unescaped pipe ends the cell early, shifting every later column, so the page reads as though
      # the numbers describe something other than what was measured.
      publish(
        context,
        "loadtest-elixir.json",
        loadtest_result(%{"meta" => %{"command" => "mix run --topic=a|b", "cpu" => "x|y"}})
      )

      run(context)

      page = page(context, "loadtest-elixir-results.md")

      assert page =~ "a\\|b"
      refute page =~ "--topic=a|b"
    end

    test "a table with nothing to show says so instead of rendering an empty one", context do
      # Every value nil left a header and a separator with no rows under them, which reads as a
      # measurement that came back empty rather than a section with nothing to render.
      publish(context, "loadtest-elixir.json", loadtest_result(%{"meta" => nil, "latency_ms" => %{}}))
      run(context)

      page = page(context, "loadtest-elixir-results.md")

      refute page =~ "| Measure | Value |\n| --- | --- |\n\n"
      assert page =~ "None recorded."
    end
  end

  test "each written page is announced", context do
    run(context)

    for file <- ["loadtest-node-results.md", "loadtest-elixir-results.md", "chaos-results.md"] do
      assert_received {:mix_shell, :info, [message]} when is_binary(message)
      assert message =~ file
    end
  end
end
