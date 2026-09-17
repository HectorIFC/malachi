defmodule Mix.Tasks.Malachi.Loadtest.CeilingTest do
  # The file side of the ceiling rules: what the harness hands the task and what it gets back, including
  # the exit statuses the script passes through. Not async because Mix.shell/1 is global.
  use ExUnit.Case, async: false

  alias Mix.Tasks.Malachi.Loadtest.Ceiling, as: Task

  @moduletag :tmp_dir

  setup %{tmp_dir: dir} do
    shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    on_exit(fn -> Mix.shell(shell) end)

    run_dir = Path.join(dir, "runs")
    File.mkdir_p!(run_dir)
    %{run_dir: run_dir, sweep: Path.join(dir, "sweep.json"), out: Path.join(dir, "result.json")}
  end

  @plan_argv [
    "--batch-ladder",
    "10 100",
    "--conns-ladder",
    "10=32 64",
    "--conns-ladder",
    "100=16",
    "--headline-batch",
    "10",
    "--reps",
    "1",
    "--record-size",
    "256",
    "--group-commit",
    "false",
    "--segment-prealloc-bytes",
    "67108864"
  ]

  defp plan!(ctx), do: Task.run(["plan" | @plan_argv] ++ ["--out", ctx.sweep])

  defp write_run(ctx, file, batch, connections, overrides \\ %{}) do
    body =
      Map.merge(
        %{
          "batch" => batch,
          "record_size" => 256,
          "connections" => connections,
          "records_per_s" => connections * 10,
          "errors" => 0,
          "meta" => %{"git_ref" => "abc1234"}
        },
        overrides
      )

    File.write!(Path.join(ctx.run_dir, file), Jason.encode!(body))
  end

  defp write_clean_sweep(ctx) do
    write_run(ctx, "run-b10-c32-r1.json", 10, 32)
    write_run(ctx, "run-b10-c64-r1.json", 10, 64)
    write_run(ctx, "run-b100-c16-r1.json", 100, 16)
  end

  defp infos do
    Stream.repeatedly(fn ->
      receive do
        {:mix_shell, :info, [message]} -> message
      after
        0 -> nil
      end
    end)
    |> Enum.take_while(&(&1 != nil))
  end

  describe "plan" do
    test "writes the sweep and prints the points in the order they run", ctx do
      plan!(ctx)

      assert infos() == ["10 32 1", "100 16 1", "10 64 1"]

      assert %{"batch_ladder" => [10, 100], "conns_ladders" => %{"100" => [16]}} =
               ctx.sweep |> File.read!() |> Jason.decode!()
    end

    test "an invalid sweep prints why and exits with status 2", ctx do
      # Index 7 is the value of --headline-batch.
      argv = ["plan" | List.replace_at(@plan_argv, 7, "50")] ++ ["--out", ctx.sweep]

      assert catch_exit(Task.run(argv)) == {:shutdown, 2}
      assert_received {:mix_shell, :error, ["HEADLINE_BATCH 50 is not in BATCH_LADDER (10 100)"]}
      refute File.exists?(ctx.sweep)
    end

    test "--out is required" do
      assert_raise Mix.Error, "--out is required", fn -> Task.run(["plan" | @plan_argv]) end
    end
  end

  describe "peak" do
    test "prints the connections of the headline peak", ctx do
      plan!(ctx)
      infos()
      write_clean_sweep(ctx)

      Task.run(["peak", "--run-dir", ctx.run_dir, "--sweep", ctx.sweep])
      assert infos() == ["64"]
    end

    test "exits with status 1 when the headline batch size has no clean rung", ctx do
      plan!(ctx)
      write_run(ctx, "run-b10-c32-r1.json", 10, 32, %{"errors" => 1})

      assert catch_exit(Task.run(["peak", "--run-dir", ctx.run_dir, "--sweep", ctx.sweep])) == {:shutdown, 1}
      assert_received {:mix_shell, :error, ["no clean peak at the headline batch size 10"]}
    end

    test "a run that does not describe its point fails the task", ctx do
      plan!(ctx)
      write_run(ctx, "run-b10-c32-r1.json", 10, 33)

      assert_raise Mix.Error, ~r/does not describe the point that launched it/, fn ->
        Task.run(["peak", "--run-dir", ctx.run_dir, "--sweep", ctx.sweep])
      end
    end
  end

  describe "summarize" do
    test "writes the result, reads the A-A repeat and prints a line per batch size", ctx do
      plan!(ctx)
      infos()
      write_clean_sweep(ctx)
      write_run(ctx, "aa-b10-c64.json", 10, 64, %{"records_per_s" => 660})

      Task.run(["summarize", "--run-dir", ctx.run_dir, "--sweep", ctx.sweep, "--out", ctx.out])

      result = ctx.out |> File.read!() |> Jason.decode!()
      assert result["records_per_s"] == 640
      assert result["sweep"]["aa_control"]["repeat_records_per_s"] == 660
      assert [first, second] = infos()
      assert first =~ "batch 10 x 256B"
      assert second =~ "batch 100 x 256B"
    end

    test "without a headline peak it still writes the result, then exits with status 1", ctx do
      plan!(ctx)
      write_run(ctx, "run-b100-c16-r1.json", 100, 16)

      argv = ["summarize", "--run-dir", ctx.run_dir, "--sweep", ctx.sweep, "--out", ctx.out]
      assert catch_exit(Task.run(argv)) == {:shutdown, 1}

      assert %{"headline_status" => "no_completed_rung"} = ctx.out |> File.read!() |> Jason.decode!()
      assert_received {:mix_shell, :error, [message]}
      assert message =~ "is not a publishable result"
    end

    test "a run that does not describe its point fails the task without writing a result", ctx do
      plan!(ctx)
      write_clean_sweep(ctx)
      write_run(ctx, "run-b100-c16-r1.json", 100, 17)

      assert_raise Mix.Error, ~r/does not describe the point that launched it/, fn ->
        Task.run(["summarize", "--run-dir", ctx.run_dir, "--sweep", ctx.sweep, "--out", ctx.out])
      end

      refute File.exists?(ctx.out)
    end

    test "an empty run file is a repetition that produced nothing", ctx do
      plan!(ctx)
      write_clean_sweep(ctx)
      File.write!(Path.join(ctx.run_dir, "run-b100-c16-r1.json"), "  \n")

      Task.run(["summarize", "--run-dir", ctx.run_dir, "--sweep", ctx.sweep, "--out", ctx.out])
      assert %{"curve" => [_, %{"status" => "no_completed_rung"}]} = ctx.out |> File.read!() |> Jason.decode!()
    end

    test "a run file that does not parse fails the task, naming it", ctx do
      plan!(ctx)
      File.write!(Path.join(ctx.run_dir, "run-b10-c32-r1.json"), "{not json")

      assert_raise Mix.Error, ~r/run-b10-c32-r1\.json is not valid JSON/, fn ->
        Task.run(["summarize", "--run-dir", ctx.run_dir, "--sweep", ctx.sweep, "--out", ctx.out])
      end
    end

    test "a run file that parses to something other than an object fails the task", ctx do
      plan!(ctx)
      File.write!(Path.join(ctx.run_dir, "run-b10-c32-r1.json"), "[1, 2]")

      assert_raise Mix.Error, ~r/run-b10-c32-r1\.json is not a JSON object/, fn ->
        Task.run(["summarize", "--run-dir", ctx.run_dir, "--sweep", ctx.sweep, "--out", ctx.out])
      end
    end

    test "a run file that cannot be read fails the task", ctx do
      plan!(ctx)
      File.mkdir_p!(Path.join(ctx.run_dir, "run-b10-c32-r1.json"))

      assert_raise Mix.Error, ~r/cannot read .*run-b10-c32-r1\.json/, fn ->
        Task.run(["summarize", "--run-dir", ctx.run_dir, "--sweep", ctx.sweep, "--out", ctx.out])
      end
    end
  end

  describe "label" do
    defp label!(argv) do
      Task.run(["label" | argv])
      assert_received {:mix_shell, :info, [line]}
      line
    end

    @label_argv ["--batch", "100", "--record-size", "256", "--group-commit", "true", "--segment-prealloc-bytes", "0"]

    test "prints the regime in the words regime_label/4 uses" do
      assert label!(@label_argv) ==
               "batch 100 x 256B (25KB of values per request, group commit on, segment preallocation off)"

      assert label!([
               "--batch",
               "10",
               "--record-size",
               "256",
               "--group-commit",
               "false",
               "--segment-prealloc-bytes",
               "67108864"
             ]) ==
               "batch 10 x 256B (2.5KB of values per request, group commit off, segment preallocation 64MB)"

      assert label!([
               "--batch",
               "4096",
               "--record-size",
               "512",
               "--group-commit",
               "false",
               "--segment-prealloc-bytes",
               "8192"
             ]) ==
               "batch 4096 x 512B (2MB of values per request, group commit off, segment preallocation 8KB)"
    end

    test "prints nothing else" do
      Task.run([
        "label",
        "--batch",
        "1",
        "--record-size",
        "1",
        "--group-commit",
        "true",
        "--segment-prealloc-bytes",
        "0"
      ])

      assert_received {:mix_shell, :info,
                       ["batch 1 x 1B (1B of values per request, group commit on, segment preallocation off)"]}

      refute_received {:mix_shell, _, _}
    end

    # Each case is the valid argv with one flag dropped or replaced, so the flag named is the only fault.
    for {change, message} <- [
          {{:drop, "--batch"}, "--batch is not set"},
          {{:drop, "--record-size"}, "--record-size is not set"},
          {{:drop, "--group-commit"}, "--group-commit is not set"},
          {{:drop, "--segment-prealloc-bytes"}, "--segment-prealloc-bytes is not set"},
          {{:set, "--batch", "0"}, "--batch must be a positive integer, got 0"},
          {{:set, "--record-size", "-1"}, "--record-size must be a positive integer, got -1"},
          {{:set, "--group-commit", "maybe"}, ~s(--group-commit must be true or false, got "maybe")},
          {{:set, "--segment-prealloc-bytes", "-1"}, "--segment-prealloc-bytes must be a non-negative integer, got -1"},
          {{:set, "--segment-prealloc-bytes", "64MB"}, ~s(--segment-prealloc-bytes must be an integer, got "64MB")},
          {{:append, "extra"}, "label takes no positional arguments, got: extra"}
        ] do
      test "rejects #{inspect(change)}" do
        argv = change_argv(@label_argv, unquote(Macro.escape(change)))
        assert_raise Mix.Error, unquote(message), fn -> Task.run(["label" | argv]) end
      end
    end

    test "an unknown flag is rejected by name" do
      assert_raise OptionParser.ParseError, ~r/--connections/, fn ->
        Task.run(["label", "--batch", "100", "--connections", "32"])
      end
    end
  end

  defp change_argv(argv, {:drop, flag}) do
    index = Enum.find_index(argv, &(&1 == flag))
    List.delete_at(List.delete_at(argv, index), index)
  end

  defp change_argv(argv, {:set, flag, value}) do
    List.replace_at(argv, Enum.find_index(argv, &(&1 == flag)) + 1, value)
  end

  defp change_argv(argv, {:append, extra}), do: argv ++ [extra]

  describe "inputs" do
    test "a run directory that does not exist", ctx do
      plan!(ctx)

      assert_raise Mix.Error, ~r/run directory .* does not exist/, fn ->
        Task.run(["peak", "--run-dir", Path.join(ctx.run_dir, "nope"), "--sweep", ctx.sweep])
      end
    end

    test "a sweep file that does not exist", ctx do
      assert_raise Mix.Error, ~r/cannot read the sweep at/, fn ->
        Task.run(["peak", "--run-dir", ctx.run_dir, "--sweep", ctx.sweep])
      end
    end

    test "a sweep file that is not a valid sweep", ctx do
      File.write!(ctx.sweep, Jason.encode!(%{"batch_ladder" => []}))

      assert_raise Mix.Error, ~r/is not a valid sweep: BATCH_LADDER is empty/, fn ->
        Task.run(["peak", "--run-dir", ctx.run_dir, "--sweep", ctx.sweep])
      end
    end

    test "an unknown subcommand prints the usage" do
      assert_raise Mix.Error, ~r/usage: mix malachi.loadtest.ceiling plan\|peak\|summarize\|label/, fn ->
        Task.run(["elect"])
      end
    end
  end
end
