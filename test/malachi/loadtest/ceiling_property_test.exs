defmodule Malachi.Loadtest.CeilingPropertyTest do
  # The election rules as invariants over arbitrary ladders and outcomes, rather than as the handful of
  # shapes the example tests happened to pick. A counterexample here is a published ceiling that does
  # not follow the stated rules.
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Loadtest.Ceiling

  defp ladder, do: 1..512 |> integer() |> uniq_list_of(min_length: 1, max_length: 5) |> map(&Enum.sort/1)

  # nil is a repetition that produced nothing; otherwise a rate and an error count.
  defp outcome, do: one_of([constant(nil), tuple({integer(0..5_000), member_of([0, 0, 0, 1, 7])})])

  defp sweep(ladder, reps) do
    {:ok, sweep} =
      Ceiling.plan(%{
        batch_ladder: "10",
        conns_ladders: ["10=" <> Enum.join(ladder, " ")],
        headline_batch: "10",
        repetitions: Integer.to_string(reps),
        record_size: "256",
        group_commit: "false",
        segment_prealloc_bytes: "0"
      })

    sweep
  end

  defp runs(sweep, outcomes) do
    sweep
    |> Ceiling.run_order()
    |> Enum.zip(outcomes)
    |> Enum.map(fn {{batch, connections, rep}, outcome} ->
      result =
        case outcome do
          nil ->
            nil

          {rate, errors} ->
            %{
              "batch" => batch,
              "record_size" => 256,
              "connections" => connections,
              "records_per_s" => rate,
              "errors" => errors
            }
        end

      %{batch: batch, connections: connections, rep: rep, result: result}
    end)
  end

  defp scenario do
    gen all(
          ladder <- ladder(),
          reps <- integer(1..3),
          outcomes <- list_of(outcome(), length: length(ladder) * reps)
        ) do
      sweep = sweep(ladder, reps)
      {sweep, runs(sweep, outcomes)}
    end
  end

  defp summary({sweep, runs}) do
    {_outcome, result} = Ceiling.summarize(sweep, runs, nil)
    hd(result["curve"])
  end

  property "the peak is a clean rung, and no clean rung beats it or ties it with fewer connections" do
    check all({_sweep, _runs} = scenario <- scenario()) do
      item = summary(scenario)
      clean = Enum.filter(item["rungs"], &(&1["status"] == "clean"))

      case item["peak"] do
        nil ->
          assert clean == []

        peak ->
          assert Enum.any?(clean, &(&1["connections"] == peak["connections"]))

          for rung <- clean do
            assert rung["records_per_s"] <= peak["records_per_s"]
            if rung["records_per_s"] == peak["records_per_s"], do: assert(rung["connections"] >= peak["connections"])
          end
      end
    end
  end

  property "the batch status follows from its rungs" do
    check all({_sweep, _runs} = scenario <- scenario()) do
      item = summary(scenario)
      statuses = Enum.map(item["rungs"], & &1["status"])

      expected =
        cond do
          "clean" in statuses -> "peak"
          "errorful" in statuses -> "no_clean_rung"
          true -> "no_completed_rung"
        end

      assert item["status"] == expected
    end
  end

  property "a peak is at the ladder limit exactly when it sits on the top rung" do
    check all({sweep, _runs} = scenario <- scenario()) do
      item = summary(scenario)

      if peak = item["peak"] do
        at_top = peak["connections"] == List.last(sweep.conns_ladders[10])
        assert item["peak_at_ladder_limit"] == at_top
        assert "ladder_limit" in item["lower_bound_reasons"] == at_top
      else
        assert item["peak_at_ladder_limit"] == nil
      end
    end
  end

  property "a rung's rate lies within its repetitions and it never claims more of them than ran" do
    check all({sweep, _runs} = scenario <- scenario()) do
      for rung <- summary(scenario)["rungs"] do
        assert rung["repetitions_completed"] <= sweep.repetitions

        if rung["records_per_s"] do
          assert rung["records_per_s_min"] <= rung["records_per_s"]
          assert rung["records_per_s"] <= rung["records_per_s_max"]
        end
      end
    end
  end

  property "the summary does not depend on the order the runs are listed in" do
    check all({sweep, runs} <- scenario(), shuffled <- constant(Enum.shuffle(runs))) do
      assert Ceiling.summarize(sweep, runs, nil) == Ceiling.summarize(sweep, shuffled, nil)
    end
  end
end
