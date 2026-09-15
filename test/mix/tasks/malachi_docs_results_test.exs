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

      publish(
        context,
        "loadtest-elixir.json",
        loadtest_result(%{"dropped" => 0, "overloaded" => 3, "rate_limited" => 2, "reconnects" => 1})
      )

      run(context)

      refute page(context, "loadtest-node-results.md") =~ "## Backpressure"

      elixir = page(context, "loadtest-elixir-results.md")
      assert elixir =~ "## Backpressure"
      assert elixir =~ "| Server-shed produces | 3 |"
      # the two refusals are reported apart: saturation and quota mean different things to an operator
      assert elixir =~ "| Quota-refused produces | 2 |"
      assert elixir =~ "| Reconnects | 1 |"
    end

    test "a result without a meta block still renders", context do
      publish(context, "loadtest-node.json", loadtest_result() |> Map.delete("meta"))
      run(context)

      body = page(context, "loadtest-node-results.md")
      assert body =~ "**25,889 records per second**"
      assert body =~ "| Recorded | without metadata |"
    end

    test "the ceiling attribution renders: peak connections, both CPU sides and the lower-bound note", context do
      publish(
        context,
        "loadtest-node.json",
        loadtest_result(%{
          "connections" => 128,
          "server_cpu_cores" => 2.91,
          "server_cpu_budget" => 3,
          "generator_cpu_cores" => 0.98,
          "generator_cpu_budget" => 1,
          "peak_at_ladder_limit" => true
        })
      )

      run(context)
      body = page(context, "loadtest-node-results.md")

      assert body =~ "peaking at 128 connections (server at 2.91 of 3 cores, generator at 0.98 of 1 cores)"
      assert body =~ "| Peak connections | 128 |"
      assert body =~ "| Server CPU (cores) | 2.91 of 3 |"
      assert body =~ "| Generator CPU (cores) | 0.98 of 1 |"
      assert body =~ "This is a lower bound"
    end

    test "a run that sampled only one CPU side renders that side alone", context do
      publish(
        context,
        "loadtest-node.json",
        loadtest_result(%{"server_cpu_cores" => 2.47, "server_cpu_budget" => 3})
      )

      run(context)
      body = page(context, "loadtest-node-results.md")

      assert body =~ "(server at 2.47 of 3 cores)"
      refute body =~ "generator at"
      refute body =~ "Generator CPU (cores)"
    end

    test "the regime sits inside the headline sentence, where a quote carries it along", context do
      publish(
        context,
        "loadtest-node.json",
        loadtest_result(%{"regime_label" => "batch 10 x 256B (2.5KB of values per request, group commit off)"})
      )

      run(context)

      assert page(context, "loadtest-node-results.md") =~
               "**25,889 records per second** at saturation, batch 10 x 256B (2.5KB of values per request, " <>
                 "group commit off), over 10s with 0 errors"
    end

    test "a result recorded before the regime was carried gets no clause, and nothing is parsed from the command",
         context do
      # meta.command is free text in two syntaxes (--batch=10 and --batch 10); reading the regime out of it
      # would break on the first third form, so an old result simply says nothing about its regime.
      command = "scripts/loadtest.js --scenario produce --batch 10 --record-size 256"
      publish(context, "loadtest-node.json", loadtest_result(%{"meta" => %{"command" => command}}))
      run(context)

      body = page(context, "loadtest-node-results.md")
      assert body =~ "**25,889 records per second** at saturation over 10s"
      refute body =~ "batch 10 x"
      refute body =~ "## Throughput by batch size"
    end

    test "a lower bound names every reason the sweep recorded", context do
      publish(
        context,
        "loadtest-node.json",
        loadtest_result(%{
          "peak_at_ladder_limit" => true,
          "lower_bound_reasons" => ["ladder_limit", "generator_saturated"]
        })
      )

      run(context)

      assert page(context, "loadtest-node-results.md") =~
               "This is a lower bound: the connection sweep peaked at its top rung and the single generator core " <>
                 "was saturated, so the true ceiling may be higher."
    end

    test "recorded reasons are authoritative: none means no note, whatever the legacy flag says", context do
      publish(
        context,
        "loadtest-node.json",
        loadtest_result(%{"peak_at_ladder_limit" => true, "lower_bound_reasons" => []})
      )

      run(context)

      refute page(context, "loadtest-node-results.md") =~ "lower bound"
    end

    test "CPU rows are dropped when a run did not sample them, and a non-limited peak is not called a lower bound",
         context do
      # macOS smoke and any run without /proc leave these unset; a blank row would read as measured zero,
      # and the lower-bound caveat must appear only for a peak that actually hit the ladder limit.
      publish(context, "loadtest-node.json", loadtest_result(%{"peak_at_ladder_limit" => false}))
      run(context)

      body = page(context, "loadtest-node-results.md")
      refute body =~ "Server CPU (cores)"
      refute body =~ "Generator CPU (cores)"
      refute body =~ "lower bound"
    end
  end

  describe "the batch-size curve" do
    defp swept_result(overrides \\ %{}) do
      loadtest_result(
        Map.merge(
          %{
            "records_per_s" => 40_057,
            "connections" => 256,
            "regime_label" => "batch 10 x 256B (2.5KB of values per request, group commit off)",
            "lower_bound_reasons" => [],
            "sweep" => %{
              "batch_ladder" => [10, 512, 1024, 4096],
              "conns_ladders" => %{"10" => [32, 256], "512" => [8, 16], "1024" => [32, 64], "4096" => [4]},
              "repetitions" => 1,
              "record_size" => 256,
              "group_commit" => false,
              "segment_prealloc_bytes" => 67_108_864,
              "aa_control" => %{
                "batch" => 10,
                "connections" => 256,
                "first_records_per_s" => 40_057,
                "repeat_records_per_s" => 38_120,
                "delta_pct" => -4.8
              },
              "aa_control_reason" => nil
            },
            "curve" => [
              %{
                "batch" => 10,
                "bytes_per_request" => 2560,
                "status" => "peak",
                "peak" => %{"records_per_s" => 40_057, "connections" => 256},
                "lower_bound_reasons" => []
              },
              %{
                "batch" => 512,
                "bytes_per_request" => 131_072,
                "status" => "peak",
                "peak" => %{"records_per_s" => 90_210, "connections" => 16},
                "lower_bound_reasons" => ["ladder_limit", "generator_saturated"]
              },
              %{
                "batch" => 1024,
                "bytes_per_request" => 262_144,
                "status" => "no_clean_rung",
                "peak" => nil,
                "lower_bound_reasons" => [],
                "rungs" => [
                  %{"connections" => 32, "status" => "errorful", "errors" => 4},
                  %{"connections" => 64, "status" => "errorful", "errors" => nil},
                  %{"connections" => 128, "status" => "failed"}
                ]
              },
              %{"batch" => 4096, "bytes_per_request" => 1_048_576, "status" => "no_completed_rung", "peak" => nil}
            ]
          },
          overrides
        )
      )
    end

    test "renders one row per batch size, including the ones with no peak", context do
      publish(context, "loadtest-node.json", swept_result())
      run(context)

      body = page(context, "loadtest-node-results.md")

      assert body =~ "## Throughput by batch size"
      assert body =~ "| Batch | Values per request | Peak records/s | At connections | Status | Lower bound because |"
      assert body =~ "| --- | --- | --- | --- | --- | --- |"
      assert body =~ "| 10 | 2.5KB | 40,057 | 256 | peak | no |"

      assert body =~
               "| 512 | 128KB | 90,210 | 16 | peak | the connection sweep peaked at its top rung; the single generator core was saturated |"

      assert body =~
               "| 1,024 | 256KB | none | none | no clean rung (32 connections, 4 errors; 64 connections, unrecorded errors) | n/a |"

      assert body =~ "| 4,096 | 1MB | none | none | no rung completed | n/a |"
    end

    test "states how far to trust a difference between rows", context do
      publish(context, "loadtest-node.json", swept_result())
      run(context)

      body = page(context, "loadtest-node-results.md")
      assert body =~ "1 repetition per point, with the points interleaved across batch sizes"
      assert body =~ "Repeating the headline peak moved it -4.8% (40,057 then 38,120 records per second)"
      assert body =~ "Compare batch sizes within this run rather than across runs"
    end

    test "says when there is no noise estimate, and why when the sweep recorded it", context do
      without_repeat = swept_result()

      sweep = %{
        without_repeat["sweep"]
        | "aa_control" => nil,
          "aa_control_reason" => "the A-A repeat produced no result"
      }

      publish(context, "loadtest-node.json", %{without_repeat | "sweep" => %{sweep | "repetitions" => 3}})
      run(context)

      body = page(context, "loadtest-node-results.md")
      assert body =~ "3 repetitions per point"
      assert body =~ "No A-A repeat of the headline peak was recorded (the A-A repeat produced no result)"

      publish(context, "loadtest-node.json", %{
        without_repeat
        | "sweep" => Map.drop(sweep, ["aa_control", "aa_control_reason"])
      })

      run(context)

      assert page(context, "loadtest-node-results.md") =~
               "No A-A repeat of the headline peak was recorded, so this run carries no noise estimate."
    end

    test "the reproduce table carries the sweep", context do
      publish(context, "loadtest-node.json", swept_result())
      run(context)

      body = page(context, "loadtest-node-results.md")
      assert body =~ "| Batch sizes | 10 512 1024 4096 |"
      assert body =~ "| Connection ladders | batch 10: 32 256; batch 512: 8 16; batch 1024: 32 64; batch 4096: 4 |"
      assert body =~ "| Repetitions per point | 1 |"
      assert body =~ "| Record size | 256B |"
      assert body =~ "| Group commit | off |"
      assert body =~ "| Segment preallocation | 64MB |"
    end

    test "a curve without its sweep block renders the table but claims no noise estimate either way", context do
      publish(context, "loadtest-node.json", Map.delete(swept_result(), "sweep"))
      run(context)

      body = page(context, "loadtest-node-results.md")
      assert body =~ "## Throughput by batch size"
      assert body =~ "| 10 | 2.5KB | 40,057 | 256 | peak | no |"
      refute body =~ "A-A repeat"
      refute body =~ "repetition"
      refute body =~ "| Batch sizes |"
    end

    test "a sweep that ran with group commit on says so", context do
      result = swept_result()
      publish(context, "loadtest-node.json", put_in(result, ["sweep", "group_commit"], true))
      run(context)

      assert page(context, "loadtest-node-results.md") =~ "| Group commit | on |"
    end

    test "fields a hand-edited or partial sweep lacks are dropped or named, never rendered as measured", context do
      curve = [
        %{
          "batch" => 10,
          "status" => "weird",
          "peak" => %{"records_per_s" => 1, "connections" => 2},
          "lower_bound_reasons" => ["cosmic_rays"]
        },
        %{"batch" => 20, "peak" => nil}
      ]

      sweep = %{"batch_ladder" => "10", "conns_ladders" => [], "group_commit" => "no", "repetitions" => 1}
      publish(context, "loadtest-node.json", swept_result(%{"curve" => curve, "sweep" => sweep}))
      run(context)

      body = page(context, "loadtest-node-results.md")
      assert body =~ "| 10 | not recorded | 1 | 2 | weird | cosmic_rays |"
      assert body =~ "| 20 | not recorded | none | none | not recorded | n/a |"
      refute body =~ "| Batch sizes |"
      refute body =~ "| Connection ladders |"
      refute body =~ "| Group commit |"
    end

    test "a sweep with no clean peak at the headline batch size says so instead of a blank number", context do
      result = swept_result() |> Map.drop(["records_per_s", "connections", "meta", "duration_s", "errors", "scenario"])
      publish(context, "loadtest-node.json", Map.put(result, "headline_status", "no_clean_rung"))
      run(context)

      body = page(context, "loadtest-node-results.md")

      assert body =~
               "**No clean peak was measured at the headline batch size**, batch 10 x 256B (2.5KB of values per " <>
                 "request, group commit off). The batch-size table below shows what each batch size recorded."

      publish(context, "loadtest-node.json", Map.delete(result, "regime_label"))
      run(context)

      assert page(context, "loadtest-node-results.md") =~
               "**No clean peak was measured at the headline batch size**. The batch-size table"
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
