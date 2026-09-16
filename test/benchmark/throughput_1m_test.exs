Code.require_file("../../benchmark/support/flush_regime.exs", __DIR__)
Code.require_file("../../benchmark/support/e2e_sample.exs", __DIR__)

defmodule Malachi.Benchmark.Throughput1mTest do
  # benchmark/throughput_1m.exs is the system baseline, and store_error_path_ab.exs reads its output in
  # the benchmark workflow. Nothing else runs it, so a broken regime line, a changed latency line or a
  # refusal that stopped refusing would only show up there. The script runs for real, whole: 1M records,
  # a few seconds.
  #
  # `--no-start`: the suite already runs the application, and a second one would fight it for its ports.
  # The script needs nothing the application starts, which is measured, not assumed: it runs to the end
  # without it. The workflow runs it with the application started, so that path is covered there.
  #
  # Not async: each run writes 137MB and takes most of a core.
  use ExUnit.Case, async: false

  alias Malachi.Bench.E2ESample
  alias Malachi.Bench.FlushRegime

  @moduletag :tmp_dir
  @moduletag timeout: 300_000

  @script "benchmark/throughput_1m.exs"
  @project Path.expand("../..", __DIR__)
  @shm "/dev/shm"
  @shm_tmpfs? File.dir?(@shm) and FlushRegime.filesystem(@shm) == "tmpfs"

  setup_all do
    %{mix: System.find_executable("mix") || flunk("mix is required to run #{@script}")}
  end

  test "prints its regime next to a sample the A/B harness can read", %{mix: mix, tmp_dir: dir} do
    {output, status} = run(mix, [{"BENCH_DIR", dir}])

    assert status == 0, output
    {:ok, %{dir: resolved, filesystem: filesystem}} = FlushRegime.prepare(%{"BENCH_DIR" => dir}, "/proc/self/mountinfo")
    regime = FlushRegime.label(1000, 100, filesystem)

    assert output =~ "Producing 1000000 records, #{regime}...\n"
    assert output =~ "\nREGIME    #{regime}\n"
    assert %{p50: p50, p99: p99} = E2ESample.parse(output)
    assert p50 > 0 and p99 >= p50

    # Linux is where the number counts, so there the filesystem has to be known.
    if match?({:unix, :linux}, :os.type()), do: assert(is_binary(filesystem))
    # Everything the run wrote is gone again.
    assert File.ls!(resolved) == []
  end

  test "a BENCH_DIR that does not exist stops the run before anything starts", %{mix: mix, tmp_dir: dir} do
    missing = Path.join(dir, "missing")
    {output, status} = run(mix, [{"BENCH_DIR", missing}])

    assert status == 2
    assert output =~ "ERROR: BENCH_DIR #{missing} is not an existing directory"
    refute output =~ "Producing"
  end

  test "an invalid BENCH_ALLOW_TMPFS stops the run before anything starts", %{mix: mix, tmp_dir: dir} do
    {output, status} = run(mix, [{"BENCH_DIR", dir}, {"BENCH_ALLOW_TMPFS", "yes"}])

    assert status == 2
    assert output =~ ~s(ERROR: BENCH_ALLOW_TMPFS must be 1 or unset, got "yes")
    refute output =~ "Producing"
  end

  describe "on tmpfs" do
    unless @shm_tmpfs?, do: @describetag(skip: "#{@shm} is not a tmpfs mount on this host (Linux only)")

    setup do
      dir = Path.join(@shm, "malachi_throughput_1m_test_#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf!(dir) end)
      %{shm_dir: dir}
    end

    test "the run is refused", %{mix: mix, shm_dir: dir} do
      {output, status} = run(mix, [{"BENCH_DIR", dir}])

      assert status == 2
      assert output =~ "ERROR: #{dir} is on tmpfs, which is not the durable path"
      refute output =~ "Producing"
      assert File.ls!(dir) == []
    end

    test "BENCH_ALLOW_TMPFS=1 runs it with a warning and says so in the regime", %{mix: mix, shm_dir: dir} do
      {output, status} = run(mix, [{"BENCH_DIR", dir}, {"BENCH_ALLOW_TMPFS", "1"}])

      assert status == 0, output
      assert output =~ "WARNING: the log is on tmpfs, which is not the durable path"
      assert output =~ "\nREGIME    #{FlushRegime.label(1000, 100, "tmpfs")}\n"
    end
  end

  defp run(mix, env) do
    # The variables this test does not set are cleared, so a value in the developer's shell cannot
    # change what is being checked.
    env = Enum.uniq_by(env ++ [{"BENCH_DIR", nil}, {"BENCH_ALLOW_TMPFS", nil}, {"MIX_ENV", "test"}], &elem(&1, 0))
    System.cmd(mix, ["run", "--no-start", @script], cd: @project, env: env, stderr_to_stdout: true)
  end
end
