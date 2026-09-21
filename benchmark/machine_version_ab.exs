# Does the machine version gate cost the control plane anything? (issue #188)
#
# Every entry a metadata vnode applies now goes through `Malachi.Cluster.MachineVersion.apply/5` before
# `Malachi.Metadata.apply/2`: one tag lookup in a literal map and one integer comparison. That is expected
# to be noise next to the fsync each Raft entry already waits on, and "expected" is not a measurement, so
# this compares `MetadataMachine.apply/3` before and after with the paired protocol of
# store_error_path_ab.exs: arms interleaved by run in a shuffled order, an A-A control, and the fixed
# verdict rule of `support/paired_stats.exs`.
#
# One sample times @batches rounds of @applies_per_round applies of `:commit_offset`, the metadata command
# a consuming group sends most often, on a vnode holding one topic. `apply/3` is called directly (no ra, no
# log), so only the machine's own work is timed. The baseline ignores `meta`; the branch reads its
# `machine_version`, and both are handed the same map.
#
# Modes (normally driven by machine_version_ab.sh, not by hand):
#   AB_MODE=sample                          mix run --no-start benchmark/machine_version_ab.exs
#   AB_MODE=analyze AB_RESULTS=dir AB_OUT=file mix run --no-start benchmark/machine_version_ab.exs

Code.require_file("support/ab_run.exs", __DIR__)

defmodule MachineVersionAB do
  alias Malachi.Bench.ABRun
  alias Malachi.Bench.PairedStats
  alias Malachi.Cluster.MetadataMachine
  alias Malachi.Metadata

  @batches 300
  @applies_per_round 10_000
  @meta %{index: 1, term: 1, machine_version: 1, system_time: 1_700_000_000_000}

  @doc "One repetition, printed as a single marked JSON line."
  def sample do
    {state, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "orders", 4})
    command = {:commit_offset, "group", "orders", %{root => 42}}
    ABRun.emit(%{"commit_offset" => PairedStats.summarize(rounds(state, command))})
  end

  defp rounds(state, command) do
    for _ <- 1..@batches do
      started = System.monotonic_time(:microsecond)
      apply_many(state, command, @applies_per_round)
      System.monotonic_time(:microsecond) - started
    end
  end

  defp apply_many(_state, _command, 0), do: :ok

  defp apply_many(state, command, remaining) do
    {_next, :ok} = MetadataMachine.apply(@meta, command, state)
    apply_many(state, command, remaining - 1)
  end

  @doc "Reads the samples under `results` and judges them (see `Malachi.Bench.ABRun.analyze/5`)."
  def analyze(results, out) do
    cases = [{"commit_offset", "apply", &parse/1}]
    ABRun.analyze(results, out, ["commit_offset"], cases, %{applies_per_round: @applies_per_round, batches: @batches})
  end

  defp parse(output) do
    %{"p50" => p50, "p99" => p99} = output |> ABRun.marked() |> Map.fetch!("commit_offset")
    %{p50: p50, p99: p99}
  end
end

case System.get_env("AB_MODE") do
  "sample" -> MachineVersionAB.sample()
  "analyze" -> MachineVersionAB.analyze(System.fetch_env!("AB_RESULTS"), System.get_env("AB_OUT"))
  other -> raise ArgumentError, "AB_MODE must be sample or analyze, got: #{inspect(other)}"
end
