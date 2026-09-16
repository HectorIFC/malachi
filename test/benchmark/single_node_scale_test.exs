Code.require_file("../../benchmark/support/flush_regime.exs", __DIR__)

defmodule Malachi.Benchmark.SingleNodeScaleTest do
  # benchmark/single_node_scale.exs sweeps pipelines and batch sizes, and nothing else runs it. The script
  # runs for real, on a ladder small enough to take about a second: the knobs that choose the ladder are
  # what makes that possible, and they are the part most worth pinning, since a sweep over values nobody
  # asked for is worse than none.
  #
  # Malachi.Test.BenchScript runs it, with `--no-start` and exactly the knobs each test sets.
  use ExUnit.Case, async: false

  alias Malachi.Bench.FlushRegime
  alias Malachi.Test.BenchScript

  @moduletag :tmp_dir
  @moduletag timeout: 300_000

  @script "benchmark/single_node_scale.exs"
  @shm "/dev/shm"
  @shm_tmpfs? File.dir?(@shm) and FlushRegime.filesystem(@shm) == "tmpfs"
  @row ~r/^\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+[\d.]+\s+[\d.]+\s+[\d.]+\s+[\d.]+$/m

  setup_all do
    %{tools: BenchScript.executables!()}
  end

  test "sweeps the ladder it is given, one block per batch size under its regime", %{tools: tools, tmp_dir: dir} do
    {output, status} =
      BenchScript.run(tools, @script, [{"BENCH_DIR", dir}, {"SCALE_NS", "2 1"}, {"SCALE_BATCHES", "10 20"}])

    assert status == 0, output
    {:ok, %{dir: resolved, filesystem: filesystem}} = FlushRegime.prepare(%{"BENCH_DIR" => dir}, "/proc/self/mountinfo")

    assert output =~ "Sweeping N=2 1 x batch=10 20: each pipeline sends 1000 produces of 100B values."
    assert output =~ "eff % is the per-pipeline rate against N=1 at the same batch size."

    # Each block sits under its own regime, in the order asked for.
    [_, block_10, block_20] = String.split(output, ~r/^-- /m)
    assert block_10 =~ "#{FlushRegime.label(10, 100, filesystem)} --\n"
    assert block_20 =~ "#{FlushRegime.label(20, 100, filesystem)} --\n"

    for {block, batch} <- [{block_10, 10}, {block_20, 20}] do
      rows = for [n, b, agg, per, eff] <- Regex.scan(@row, block, capture: :all_but_first), do: {n, b, agg, per, eff}

      # N in the order asked for; efficiency against N=1 even though it ran second.
      assert [{"2", b2, agg2, per2, _}, {"1", b1, agg1, per1, "100"}] = rows
      assert {b2, b1} == {"#{batch}", "#{batch}"}
      assert String.to_integer(per2) == round(String.to_integer(agg2) / 2)
      assert per1 == agg1
    end

    assert output =~ "batch 10: did NOT reach 1M rec/s aggregate within the swept range (N up to 2)"
    assert output =~ "batch 20: did NOT reach 1M rec/s aggregate within the swept range (N up to 2)"
    assert output =~ "on this host/configuration."

    if match?({:unix, :linux}, :os.type()), do: assert(is_binary(filesystem))
    assert File.ls!(resolved) == []
  end

  describe "an invalid ladder stops the run before anything starts" do
    for {name, value, reason} <- [
          {"SCALE_NS", "", "is empty"},
          {"SCALE_NS", "   ", "is empty"},
          {"SCALE_NS", "1 x", ~s(has "x", not a positive integer)},
          {"SCALE_NS", "0", ~s(has "0", not a positive integer)},
          {"SCALE_NS", "-2", ~s(has "-2", not a positive integer)},
          {"SCALE_NS", "1.5", ~s(has "1.5", not a positive integer)},
          {"SCALE_NS", "2 1 2", "repeats a value"},
          {"SCALE_BATCHES", "10 abc", ~s(has "abc", not a positive integer)},
          {"SCALE_BATCHES", "", "is empty"},
          {"SCALE_BATCHES", "100 100", "repeats a value"}
        ] do
      test "#{name}=#{inspect(value)}", %{tools: tools, tmp_dir: dir} do
        {output, status} = BenchScript.run(tools, @script, [{"BENCH_DIR", dir}, {unquote(name), unquote(value)}])

        assert status == 2
        assert output =~ "ERROR: #{unquote(name)}=#{inspect(unquote(value))} #{unquote(reason)}"
        refute output =~ "Sweeping"
        assert File.ls!(dir) == []
      end
    end

    test "the ladder is checked before the directory", %{tools: tools, tmp_dir: dir} do
      {output, status} = BenchScript.run(tools, @script, [{"BENCH_DIR", Path.join(dir, "missing")}, {"SCALE_NS", "x"}])

      assert status == 2
      assert output =~ ~s(ERROR: SCALE_NS="x")
      refute output =~ "BENCH_DIR"
    end
  end

  test "a BENCH_DIR that does not exist stops the run", %{tools: tools, tmp_dir: dir} do
    missing = Path.join(dir, "missing")

    {output, status} =
      BenchScript.run(tools, @script, [{"BENCH_DIR", missing}, {"SCALE_NS", "1"}, {"SCALE_BATCHES", "10"}])

    assert status == 2
    assert output =~ "ERROR: BENCH_DIR #{missing} is not an existing directory"
    refute output =~ "Sweeping"
  end

  describe "on tmpfs" do
    unless @shm_tmpfs?, do: @describetag(skip: "#{@shm} is not a tmpfs mount on this host (Linux only)")

    test "the run is refused", %{tools: tools} do
      dir = Path.join(@shm, "malachi_scale_test_#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      {output, status} =
        BenchScript.run(tools, @script, [{"BENCH_DIR", dir}, {"SCALE_NS", "1"}, {"SCALE_BATCHES", "10"}])

      assert status == 2
      assert output =~ "ERROR: #{dir} is on tmpfs, which is not the durable path"
      refute output =~ "Sweeping"
    end
  end
end
