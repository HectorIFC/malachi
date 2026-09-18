defmodule Malachi.Loadtest.CeilingTest do
  # Everything here decides what gets published as the ceiling, so each rule is pinned by the case that
  # would break it. Pure functions over maps; no server, no files.
  use ExUnit.Case, async: true

  alias Malachi.Loadtest.Ceiling

  @base_params %{
    batch_ladder: "10 100",
    conns_ladders: ["10=32 64", "100=16 32"],
    headline_batch: "10",
    repetitions: "1",
    record_size: "256",
    group_commit: "false",
    segment_prealloc_bytes: "67108864"
  }

  defp sweep(overrides \\ %{}) do
    {:ok, sweep} = Ceiling.plan(Map.merge(@base_params, overrides))
    sweep
  end

  defp result(batch, connections, overrides \\ %{}) do
    Map.merge(
      %{
        "meta" => %{"command" => "mix malachi.loadtest --batch=#{batch}", "git_ref" => "abc1234"},
        "batch" => batch,
        "record_size" => 256,
        "connections" => connections,
        "records_per_s" => 1_000,
        "errors" => 0,
        "latency_ms" => %{"p50" => 1.0, "p99" => 2.0},
        "server_cpu_cores" => 1.5,
        "server_cpu_budget" => 3,
        "generator_cpu_cores" => 0.2,
        "generator_cpu_budget" => 1
      },
      overrides
    )
  end

  # One entry per planned point; `fun` returns the generator JSON for it, or nil for a point that
  # produced nothing.
  defp runs(sweep, fun) do
    for {batch, connections, rep} <- Ceiling.run_order(sweep) do
      %{batch: batch, connections: connections, rep: rep, result: fun.(batch, connections, rep)}
    end
  end

  defp clean_runs(sweep, rates) do
    runs(sweep, fn batch, connections, _rep ->
      result(batch, connections, %{"records_per_s" => Map.fetch!(rates, {batch, connections})})
    end)
  end

  defp item(result, batch), do: Enum.find(result["curve"], &(&1["batch"] == batch))

  describe "plan/1" do
    test "builds a sweep from the environment's strings" do
      assert %{
               batch_ladder: [10, 100],
               conns_ladders: %{10 => [32, 64], 100 => [16, 32]},
               headline_batch: 10,
               repetitions: 1,
               record_size: 256,
               group_commit: false,
               segment_prealloc_bytes: 67_108_864
             } = sweep()
    end

    test "records group commit on when the environment has it on" do
      assert %{group_commit: true} = sweep(%{group_commit: "true"})
    end

    test "tolerates extra whitespace in a ladder" do
      assert %{batch_ladder: [10, 100]} = sweep(%{batch_ladder: "  10   100 "})
    end

    for {label, overrides, message} <- [
          {"an unset batch ladder", %{batch_ladder: nil}, "BATCH_LADDER is not set"},
          {"an empty batch ladder", %{batch_ladder: "   "}, "BATCH_LADDER is empty"},
          {"a batch ladder token that is not an integer", %{batch_ladder: "10 1e3"},
           ~s(BATCH_LADDER has "1e3", which is not an integer)},
          {"a descending batch ladder", %{batch_ladder: "100 10"}, "BATCH_LADDER must be strictly ascending"},
          {"a repeated batch size", %{batch_ladder: "10 10"}, "BATCH_LADDER must be strictly ascending"},
          {"a zero batch size", %{batch_ladder: "0 10"}, "BATCH_LADDER must hold positive integers only"},
          {"a batch size with no connection ladder", %{conns_ladders: ["10=32 64"]},
           "no connection ladder for batch size 100"},
          {"a connection ladder for a batch size outside the ladder", %{conns_ladders: ["10=32", "100=16", "7=1"]},
           "connection ladders were given for batch sizes outside BATCH_LADDER: 7"},
          {"two connection ladders for one batch size", %{conns_ladders: ["10=32", "10=64", "100=16"]},
           "more than one connection ladder for batch size 10"},
          {"a connection ladder entry without a separator", %{conns_ladders: ["10"]},
           ~s(connection ladder "10" is not <batch>=<ladder>)},
          {"a connection ladder entry whose batch is not an integer", %{conns_ladders: ["ten=32"]},
           "a connection ladder's batch size must be an integer"},
          {"a descending connection ladder", %{conns_ladders: ["10=64 32", "100=16"]},
           "CONNS_LADDER_10 must be strictly ascending"},
          {"an empty connection ladder", %{conns_ladders: ["10=", "100=16"]}, "CONNS_LADDER_10 is empty"},
          {"a headline batch outside the ladder", %{headline_batch: "50"},
           "HEADLINE_BATCH 50 is not in BATCH_LADDER (10 100)"},
          {"an unset headline batch", %{headline_batch: nil}, "HEADLINE_BATCH is not set"},
          {"zero repetitions", %{repetitions: "0"}, "REPS must be a positive integer, got 0"},
          {"repetitions that are not a number", %{repetitions: "a"}, ~s(REPS must be an integer, got "a")},
          {"a zero record size", %{record_size: "0"}, "RSIZE must be a positive integer"},
          {"a group commit that is not a boolean", %{group_commit: "yes"},
           ~s(MALACHI_GROUP_COMMIT must be true or false, got "yes")},
          {"negative preallocation", %{segment_prealloc_bytes: "-1"},
           "MALACHI_SEGMENT_PREALLOC_BYTES must be a non-negative integer"}
        ] do
      test "rejects #{label}" do
        assert {:error, message} = Ceiling.plan(Map.merge(@base_params, unquote(Macro.escape(overrides))))
        assert message =~ unquote(message)
      end
    end
  end

  describe "run_order/1" do
    test "interleaves: connection position outside, batch sizes rotated by one at each position" do
      sweep = sweep(%{batch_ladder: "10 100 512", conns_ladders: ["10=32 64", "100=16 32 64", "512=8"]})

      assert Ceiling.run_order(sweep) == [
               {10, 32, 1},
               {100, 16, 1},
               {512, 8, 1},
               {100, 32, 1},
               {10, 64, 1},
               {100, 64, 1}
             ]
    end

    test "repetitions of a rung run back to back" do
      assert Ceiling.run_order(sweep(%{batch_ladder: "10", conns_ladders: ["10=32 64"], repetitions: "2"})) == [
               {10, 32, 1},
               {10, 32, 2},
               {10, 64, 1},
               {10, 64, 2}
             ]
    end

    test "names the files the harness writes" do
      assert Ceiling.run_file(10, 32, 1) == "run-b10-c32-r1.json"
      assert Ceiling.aa_file(10, 32) == "aa-b10-c32.json"
    end
  end

  describe "encode_sweep/1 and decode_sweep/1" do
    test "round-trip through JSON" do
      sweep = sweep()
      json = sweep |> Ceiling.encode_sweep() |> Jason.encode!() |> Jason.decode!()

      assert json["conns_ladders"] == %{"10" => [32, 64], "100" => [16, 32]}
      assert Ceiling.decode_sweep(json) == {:ok, sweep}
    end

    test "a sweep that is not an object is refused" do
      assert Ceiling.decode_sweep([1, 2]) == {:error, "a sweep must be a JSON object"}
    end

    test "a hand-edited sweep is validated like a planned one" do
      json = Ceiling.encode_sweep(sweep())

      assert {:error, "BATCH_LADDER must hold positive integers only" <> _} =
               Ceiling.decode_sweep(%{json | "batch_ladder" => ["10", 100]})

      assert {:error, "connection ladders were given for batch sizes outside BATCH_LADDER: \"ten\""} =
               Ceiling.decode_sweep(%{json | "conns_ladders" => Map.put(json["conns_ladders"], "ten", [1])})

      assert {:error, "conns_ladders must map every batch size" <> _} =
               Ceiling.decode_sweep(%{json | "conns_ladders" => [[32]]})

      assert {:error, "MALACHI_GROUP_COMMIT must be true or false, got \"false\""} =
               Ceiling.decode_sweep(%{json | "group_commit" => "false"})
    end
  end

  describe "format_bytes/1 and regime_label/4" do
    for {bytes, text} <- [
          {100, "100B"},
          {1023, "1023B"},
          {1024, "1KB"},
          {2560, "2.5KB"},
          {25_600, "25KB"},
          {131_072, "128KB"},
          {262_144, "256KB"},
          {1_048_576, "1MB"},
          {1_572_864, "1.5MB"}
        ] do
      test "#{bytes} bytes is #{text}" do
        assert Ceiling.format_bytes(unquote(bytes)) == unquote(text)
      end
    end

    test "the label names the batch, the record size, the bytes per request, group commit and preallocation" do
      assert Ceiling.regime_label(10, 256, false, 67_108_864) ==
               "batch 10 x 256B (2.5KB of values per request, group commit off, segment preallocation 64MB)"

      assert Ceiling.regime_label(4096, 256, true, 0) ==
               "batch 4096 x 256B (1MB of values per request, group commit on, segment preallocation off)"

      assert Ceiling.regime_label(1000, 100, false, 8192) ==
               "batch 1000 x 100B (97.7KB of values per request, group commit off, segment preallocation 8KB)"
    end

    test "the label refuses values that describe no regime" do
      for {batch, record_size, group_commit, prealloc} <- [
            {0, 256, false, 0},
            {10, 0, false, 0},
            {10, 256, "false", 0},
            {10, 256, false, -1},
            {10, 256, false, nil}
          ] do
        assert_raise FunctionClauseError, fn -> Ceiling.regime_label(batch, record_size, group_commit, prealloc) end
      end
    end
  end

  describe "label/1" do
    defp label_params(overrides \\ %{}),
      do:
        Map.merge(
          %{batch: "100", record_size: "256", group_commit: "true", segment_prealloc_bytes: "0"},
          overrides
        )

    test "formats the regime exactly as regime_label/4 does" do
      assert Ceiling.label(label_params()) == {:ok, Ceiling.regime_label(100, 256, true, 0)}
      assert Ceiling.label(label_params(%{group_commit: "false"})) == {:ok, Ceiling.regime_label(100, 256, false, 0)}
      assert Ceiling.label(label_params(%{batch: "4096"})) == {:ok, Ceiling.regime_label(4096, 256, true, 0)}

      assert Ceiling.label(label_params(%{segment_prealloc_bytes: "67108864"})) ==
               {:ok, Ceiling.regime_label(100, 256, true, 67_108_864)}
    end

    test "names the preallocation, off at zero" do
      assert {:ok, "batch 100 x 256B (25KB of values per request, group commit on, segment preallocation off)"} =
               Ceiling.label(label_params())

      assert {:ok, "batch 100 x 256B (25KB of values per request, group commit off, segment preallocation 64MB)"} =
               Ceiling.label(label_params(%{group_commit: "false", segment_prealloc_bytes: "67108864"}))
    end

    test "trims the integers the way plan/1 does" do
      assert Ceiling.label(label_params(%{batch: " 100 ", segment_prealloc_bytes: " 8192 "})) ==
               {:ok, Ceiling.regime_label(100, 256, true, 8192)}
    end

    for {flag, key} <- [
          {"--batch", :batch},
          {"--record-size", :record_size},
          {"--group-commit", :group_commit},
          {"--segment-prealloc-bytes", :segment_prealloc_bytes}
        ] do
      test "a missing #{flag} is named" do
        assert Ceiling.label(Map.delete(label_params(), unquote(key))) == {:error, "#{unquote(flag)} is not set"}
      end
    end

    for value <- ["-1", "-67108864"] do
      test "--segment-prealloc-bytes #{value} is negative" do
        assert Ceiling.label(label_params(%{segment_prealloc_bytes: unquote(value)})) ==
                 {:error, "--segment-prealloc-bytes must be a non-negative integer, got #{unquote(value)}"}
      end
    end

    for {flag, key, value} <- [
          {"--batch", :batch, "0"},
          {"--batch", :batch, "-1"},
          {"--record-size", :record_size, "0"},
          {"--record-size", :record_size, "-256"}
        ] do
      test "#{flag} #{value} is not positive" do
        assert Ceiling.label(label_params(%{unquote(key) => unquote(value)})) ==
                 {:error, "#{unquote(flag)} must be a positive integer, got #{unquote(value)}"}
      end
    end

    for {flag, key, value} <- [
          {"--batch", :batch, "ten"},
          {"--batch", :batch, "10.5"},
          {"--record-size", :record_size, "256B"},
          {"--record-size", :record_size, ""},
          {"--segment-prealloc-bytes", :segment_prealloc_bytes, "64MB"},
          {"--segment-prealloc-bytes", :segment_prealloc_bytes, ""}
        ] do
      test "#{flag} #{inspect(value)} is not an integer" do
        assert Ceiling.label(label_params(%{unquote(key) => unquote(value)})) ==
                 {:error, "#{unquote(flag)} must be an integer, got #{inspect(unquote(value))}"}
      end
    end

    for value <- ["maybe", "TRUE", "1", ""] do
      test "--group-commit #{inspect(value)} is not a boolean" do
        assert Ceiling.label(label_params(%{group_commit: unquote(value)})) ==
                 {:error, "--group-commit must be true or false, got #{inspect(unquote(value))}"}
      end
    end
  end

  describe "summarize/3, the headline" do
    test "the top level is the headline peak's run, with the regime, the sweep and the curve beside it" do
      sweep = sweep()
      rates = %{{10, 32} => 1_000, {10, 64} => 2_000, {100, 16} => 5_000, {100, 32} => 4_000}

      assert {:ok, result} = Ceiling.summarize(sweep, clean_runs(sweep, rates), nil)

      # Every field the flat readers use, unchanged in meaning.
      assert result["records_per_s"] == 2_000
      assert result["connections"] == 64
      assert result["meta"]["git_ref"] == "abc1234"
      assert result["server_cpu_cores"] == 1.5

      assert result["batch"] == 10
      assert result["record_size"] == 256
      assert result["bytes_per_request"] == 2_560
      assert result["group_commit"] == false
      assert result["segment_prealloc_bytes"] == 67_108_864

      assert result["regime_label"] ==
               "batch 10 x 256B (2.5KB of values per request, group commit off, segment preallocation 64MB)"

      assert result["headline_status"] == "peak"
      assert result["peak_at_ladder_limit"] == true
      assert result["lower_bound_reasons"] == ["ladder_limit"]

      assert result["sweep"]["batch_ladder"] == [10, 100]
      assert result["sweep"]["order"] == "interleaved"
      assert result["sweep"]["repetitions"] == 1
      assert result["sweep"]["generator_saturation_threshold"] == 0.9
      assert result["sweep"]["segment_prealloc_bytes"] == 67_108_864

      assert [%{"batch" => 10}, %{"batch" => 100} = large] = result["curve"]
      assert large["status"] == "peak"
      assert large["peak"]["connections"] == 16
      assert large["peak_at_ladder_limit"] == false

      assert large["regime_label"] ==
               "batch 100 x 256B (25KB of values per request, group commit off, segment preallocation 64MB)"
    end

    test "the regime of every batch size comes from the settings the sweep recorded" do
      sweep = sweep(%{group_commit: "true", segment_prealloc_bytes: "0"})
      rates = %{{10, 32} => 1_000, {10, 64} => 2_000, {100, 16} => 5_000, {100, 32} => 4_000}

      assert {:ok, result} = Ceiling.summarize(sweep, clean_runs(sweep, rates), nil)

      assert result["group_commit"] == true
      assert result["segment_prealloc_bytes"] == 0

      assert result["regime_label"] ==
               "batch 10 x 256B (2.5KB of values per request, group commit on, segment preallocation off)"

      assert [small, large] = result["curve"]
      assert small["segment_prealloc_bytes"] == 0
      assert large["regime_label"] =~ "(25KB of values per request, group commit on, segment preallocation off)"
    end

    test "a tie goes to the fewer connections" do
      sweep = sweep()
      rates = %{{10, 32} => 2_000, {10, 64} => 2_000, {100, 16} => 1, {100, 32} => 1}

      assert {:ok, result} = Ceiling.summarize(sweep, clean_runs(sweep, rates), nil)
      assert result["connections"] == 32
      assert result["lower_bound_reasons"] == []
      assert result["peak_at_ladder_limit"] == false
    end

    test "a rung with errors never wins, however fast" do
      sweep = sweep()

      runs =
        runs(sweep, fn
          10, 64, _rep -> result(10, 64, %{"records_per_s" => 9_999, "errors" => 3})
          batch, connections, _rep -> result(batch, connections)
        end)

      assert {:ok, result} = Ceiling.summarize(sweep, runs, nil)
      assert result["connections"] == 32
      assert Enum.at(item(result, 10)["rungs"], 1)["status"] == "errorful"
    end

    test "a rung that did not record its error count is not clean" do
      sweep = sweep()

      runs =
        runs(sweep, fn
          10, 64, _rep -> result(10, 64, %{"records_per_s" => 9_999}) |> Map.delete("errors")
          batch, connections, _rep -> result(batch, connections)
        end)

      assert {:ok, result} = Ceiling.summarize(sweep, runs, nil)
      assert result["connections"] == 32
      assert Enum.at(item(result, 10)["rungs"], 1)["status"] == "errorful"
    end

    test "a run with no throughput counts as having produced nothing" do
      sweep = sweep()

      runs =
        runs(sweep, fn
          10, 64, _rep -> result(10, 64, %{"records_per_s" => nil})
          batch, connections, _rep -> result(batch, connections)
        end)

      assert {:ok, result} = Ceiling.summarize(sweep, runs, nil)
      assert %{"status" => "failed", "records_per_s" => nil} = Enum.at(item(result, 10)["rungs"], 1)
    end

    test "without a clean headline peak the result is still built, with no flat fields" do
      sweep = sweep()

      runs =
        runs(sweep, fn
          10, connections, _rep -> result(10, connections, %{"errors" => 2})
          batch, connections, _rep -> result(batch, connections)
        end)

      assert {:no_headline_peak, result} = Ceiling.summarize(sweep, runs, nil)
      refute Map.has_key?(result, "records_per_s")
      refute Map.has_key?(result, "meta")
      assert result["headline_status"] == "no_clean_rung"
      assert result["regime_label"] =~ "batch 10 x 256B"
      assert result["sweep"]["aa_control"] == nil
      assert result["sweep"]["aa_control_reason"] == "no clean peak at the headline batch size"
      assert item(result, 100)["status"] == "peak"
    end
  end

  describe "summarize/3, batch sizes without a peak" do
    test "a batch size whose every rung errored reports no_clean_rung and does not fail the sweep" do
      sweep = sweep()

      runs =
        runs(sweep, fn
          100, 16, _rep -> result(100, 16, %{"errors" => 4})
          100, 32, _rep -> result(100, 32) |> Map.delete("errors")
          batch, connections, _rep -> result(batch, connections)
        end)

      assert {:ok, result} = Ceiling.summarize(sweep, runs, nil)
      large = item(result, 100)

      assert large["status"] == "no_clean_rung"
      assert large["peak"] == nil
      assert large["peak_at_ladder_limit"] == nil
      assert large["lower_bound_reasons"] == []

      assert ("batch 100 x 256B (25KB of values per request, group commit off, segment preallocation 64MB): no clean rung; " <>
                "16 conns (4 errors), 32 conns (unrecorded errors)") in Ceiling.summary_lines(result)
    end

    test "a batch size where nothing completed reports no_completed_rung" do
      sweep = sweep()

      runs =
        runs(sweep, fn
          100, _connections, _rep -> nil
          batch, connections, _rep -> result(batch, connections)
        end)

      assert {:ok, result} = Ceiling.summarize(sweep, runs, nil)
      assert item(result, 100)["status"] == "no_completed_rung"
      assert Enum.all?(item(result, 100)["rungs"], &(&1["status"] == "failed" and &1["repetitions_completed"] == 0))

      assert "batch 100 x 256B (25KB of values per request, group commit off, segment preallocation 64MB): no rung completed" in Ceiling.summary_lines(
               result
             )
    end
  end

  describe "summarize/3, repetitions" do
    test "the rung's rate is the median, and that repetition supplies its other fields" do
      sweep = sweep(%{batch_ladder: "10", conns_ladders: ["10=32"], repetitions: "3"})
      rates = %{1 => 300, 2 => 100, 3 => 200}

      runs =
        runs(sweep, fn 10, 32, rep ->
          result(10, 32, %{"records_per_s" => rates[rep], "latency_ms" => %{"p50" => rep * 1.0}})
        end)

      assert {:ok, result} = Ceiling.summarize(sweep, runs, nil)
      assert result["records_per_s"] == 200
      assert result["latency_ms"] == %{"p50" => 3.0}

      assert %{"records_per_s_min" => 100, "records_per_s_max" => 300, "repetitions_completed" => 3} =
               hd(item(result, 10)["rungs"])
    end

    test "the representative repetition supplies the rung's flush latency too" do
      sweep = sweep(%{batch_ladder: "10", conns_ladders: ["10=32"], repetitions: "3"})
      rates = %{1 => 300, 2 => 100, 3 => 200}

      runs =
        runs(sweep, fn
          10, 32, 1 ->
            result(10, 32, %{"records_per_s" => rates[1], "flush_latency_error" => "login refused (HTTP 403)"})

          10, 32, rep ->
            result(10, 32, %{"records_per_s" => rates[rep], "flush_latency_seconds" => %{"p99" => rep / 1000}})
        end)

      assert {:ok, result} = Ceiling.summarize(sweep, runs, nil)
      assert result["flush_latency_seconds"] == %{"p99" => 0.003}

      assert %{"flush_latency_seconds" => %{"p99" => 0.003}, "flush_latency_error" => nil} =
               hd(item(result, 10)["rungs"])

      assert %{"flush_latency_seconds" => %{"p99" => 0.003}} = item(result, 10)["peak"]
    end

    test "a representative run without a flush window carries its reason" do
      sweep = sweep(%{batch_ladder: "10", conns_ladders: ["10=32"], repetitions: "1"})
      runs = runs(sweep, fn 10, 32, _rep -> result(10, 32, %{"flush_latency_error" => "curl not found"}) end)

      assert {:ok, result} = Ceiling.summarize(sweep, runs, nil)

      assert %{"flush_latency_seconds" => nil, "flush_latency_error" => "curl not found"} =
               hd(item(result, 10)["rungs"])
    end

    test "with an even count the median is the lower middle value, one a run actually measured" do
      sweep = sweep(%{batch_ladder: "10", conns_ladders: ["10=32"], repetitions: "2"})
      runs = runs(sweep, fn 10, 32, rep -> result(10, 32, %{"records_per_s" => rep * 100}) end)

      assert {:ok, result} = Ceiling.summarize(sweep, runs, nil)
      assert result["records_per_s"] == 100
    end

    test "equal rates break on the repetition number, not on the order the runs were listed in" do
      sweep = sweep(%{batch_ladder: "10", conns_ladders: ["10=32"], repetitions: "3"})
      runs = runs(sweep, fn 10, 32, rep -> result(10, 32, %{"latency_ms" => %{"p50" => rep * 1.0}}) end)

      assert Ceiling.summarize(sweep, runs, nil) == Ceiling.summarize(sweep, Enum.reverse(runs), nil)
      assert {:ok, %{"latency_ms" => %{"p50" => 2.0}}} = Ceiling.summarize(sweep, runs, nil)
    end

    test "one missing repetition fails the rung, even when the others were clean" do
      sweep = sweep(%{batch_ladder: "10", conns_ladders: ["10=32 64"], repetitions: "2"})

      runs =
        runs(sweep, fn
          10, 64, 2 -> nil
          10, connections, _rep -> result(10, connections)
        end)

      assert {:ok, result} = Ceiling.summarize(sweep, runs, nil)
      assert result["connections"] == 32
      assert %{"status" => "failed", "repetitions_completed" => 1} = Enum.at(item(result, 10)["rungs"], 1)
    end
  end

  describe "summarize/3, generator saturation and CPU sampling" do
    for {cores, budget, saturated} <- [{0.9, 1, true}, {2.7, 3, true}, {0.89, 1, false}, {1.0, 1, true}] do
      test "a generator at #{cores} of #{budget} cores is saturated: #{saturated}" do
        sweep = sweep(%{batch_ladder: "10", conns_ladders: ["10=32 64"]})

        runs =
          runs(sweep, fn
            10, 32, _rep ->
              result(10, 32, %{
                "records_per_s" => 5_000,
                "generator_cpu_cores" => unquote(cores),
                "generator_cpu_budget" => unquote(budget)
              })

            10, 64, _rep ->
              result(10, 64)
          end)

        assert {:ok, result} = Ceiling.summarize(sweep, runs, nil)
        assert hd(item(result, 10)["rungs"])["generator_saturated"] == unquote(saturated)
        assert "generator_saturated" in result["lower_bound_reasons"] == unquote(saturated)
      end
    end

    test "a peak can be a lower bound for both reasons at once" do
      sweep = sweep(%{batch_ladder: "10", conns_ladders: ["10=32"]})
      runs = runs(sweep, fn 10, 32, _rep -> result(10, 32, %{"generator_cpu_cores" => 0.97}) end)

      assert {:ok, %{"lower_bound_reasons" => ["ladder_limit", "generator_saturated"]} = result} =
               Ceiling.summarize(sweep, runs, nil)

      assert ("batch 10 x 256B (2.5KB of values per request, group commit off, segment preallocation 64MB): 1000 rec/s at 32 connections, " <>
                "lower bound (ladder_limit, generator_saturated)") in Ceiling.summary_lines(result)
    end

    test "an unsampled generator is never called saturated, and the sweep says CPU was not sampled" do
      sweep = sweep()

      runs =
        runs(sweep, fn batch, connections, _rep ->
          result(batch, connections, %{"generator_cpu_cores" => nil, "server_cpu_cores" => nil})
        end)

      assert {:ok, result} = Ceiling.summarize(sweep, runs, nil)
      assert result["sweep"]["cpu_sampled"] == false
      refute Enum.any?(item(result, 10)["rungs"], & &1["generator_saturated"])
    end

    test "the sweep says CPU was sampled only when every completed run sampled both sides" do
      sweep = sweep()

      assert {:ok, %{"sweep" => %{"cpu_sampled" => true}}} =
               Ceiling.summarize(sweep, runs(sweep, fn b, c, _ -> result(b, c) end), nil)

      one_unsampled =
        runs(sweep, fn
          100, 32, _rep -> result(100, 32, %{"server_cpu_cores" => nil})
          b, c, _rep -> result(b, c)
        end)

      assert {:ok, %{"sweep" => %{"cpu_sampled" => false}}} = Ceiling.summarize(sweep, one_unsampled, nil)

      assert {:no_headline_peak, %{"sweep" => %{"cpu_sampled" => false}}} =
               Ceiling.summarize(sweep, runs(sweep, fn _, _, _ -> nil end), nil)
    end
  end

  describe "summarize/3, the A-A control" do
    setup do
      sweep = sweep()
      rates = %{{10, 32} => 1_000, {10, 64} => 2_000, {100, 16} => 1, {100, 32} => 1}
      %{sweep: sweep, runs: clean_runs(sweep, rates)}
    end

    test "records the repeat of the headline peak and how far it moved", %{sweep: sweep, runs: runs} do
      aa = result(10, 64, %{"records_per_s" => 1_900, "errors" => 0})

      assert {:ok, result} = Ceiling.summarize(sweep, runs, aa)

      assert result["sweep"]["aa_control"] == %{
               "batch" => 10,
               "connections" => 64,
               "first_records_per_s" => 2_000,
               "repeat_records_per_s" => 1_900,
               "repeat_errors" => 0,
               "delta_pct" => -5.0
             }

      assert result["sweep"]["aa_control_reason"] == nil
      # The repeat is a noise estimate, never a candidate for the peak.
      assert result["records_per_s"] == 2_000
    end

    test "a repeat that produced nothing is reported as such", %{sweep: sweep, runs: runs} do
      assert {:ok, result} = Ceiling.summarize(sweep, runs, nil)
      assert result["sweep"]["aa_control"] == nil
      assert result["sweep"]["aa_control_reason"] == "the A-A repeat produced no result"

      assert {:ok, %{"sweep" => %{"aa_control" => nil}}} =
               Ceiling.summarize(sweep, runs, result(10, 64, %{"records_per_s" => nil}))
    end

    test "a repeat handed in without a headline peak is ignored, not checked against a peak that is not there" do
      sweep = sweep(%{batch_ladder: "10", conns_ladders: ["10=32"]})
      runs = runs(sweep, fn _batch, _connections, _rep -> nil end)

      assert {:no_headline_peak, result} = Ceiling.summarize(sweep, runs, result(10, 999))
      assert result["sweep"]["aa_control"] == nil
      assert result["sweep"]["aa_control_reason"] == "no clean peak at the headline batch size"
    end

    test "a repeat at other connections than the peak is an error", %{sweep: sweep, runs: runs} do
      assert {:error, message} = Ceiling.summarize(sweep, runs, result(10, 32))
      assert message =~ "the run for the A-A repeat reports batch 10, record_size 256, connections 32"
    end

    test "a peak of zero records per second leaves the delta unset rather than dividing by zero" do
      sweep = sweep(%{batch_ladder: "10", conns_ladders: ["10=32"]})
      runs = runs(sweep, fn 10, 32, _rep -> result(10, 32, %{"records_per_s" => 0}) end)

      assert {:ok, result} = Ceiling.summarize(sweep, runs, result(10, 32, %{"records_per_s" => 10}))
      assert result["sweep"]["aa_control"]["delta_pct"] == nil
    end
  end

  describe "a run that does not describe the point that launched it" do
    for {label, overrides, reported} <- [
          {"another batch size", %{"batch" => 11}, "batch 11, record_size 256, connections 32"},
          {"another record size", %{"record_size" => 128}, "batch 10, record_size 128, connections 32"},
          {"another connection count", %{"connections" => 33}, "batch 10, record_size 256, connections 33"}
        ] do
      test "reporting #{label} is an error, not a data point" do
        sweep = sweep()

        runs =
          runs(sweep, fn
            10, 32, _rep -> result(10, 32, unquote(Macro.escape(overrides)))
            b, c, _rep -> result(b, c)
          end)

        assert {:error, message} = Ceiling.summarize(sweep, runs, nil)
        assert message =~ "batch 10, 32 connections, repetition 1 reports #{unquote(reported)}"
        assert {:error, ^message} = Ceiling.headline_peak(sweep, runs)
      end
    end

    test "a run that does not report its batch size at all is an error too" do
      sweep = sweep()

      runs =
        runs(sweep, fn
          10, 32, _rep -> result(10, 32) |> Map.delete("batch")
          b, c, _rep -> result(b, c)
        end)

      assert {:error, message} = Ceiling.summarize(sweep, runs, nil)
      assert message =~ "reports batch nil"
    end
  end

  describe "headline_peak/2" do
    test "names the connections of the headline peak, or :none" do
      sweep = sweep()
      rates = %{{10, 32} => 3_000, {10, 64} => 2_000, {100, 16} => 9_000, {100, 32} => 1}

      assert Ceiling.headline_peak(sweep, clean_runs(sweep, rates)) == {:ok, 32}
      assert Ceiling.headline_peak(sweep, runs(sweep, fn _, _, _ -> nil end)) == :none
    end
  end
end
