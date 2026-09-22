defmodule Malachi.Cluster.ClusterFlagsTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Cluster.ClusterFlags
  alias Malachi.Cluster.MachineVersion

  describe "new/0" do
    test "a fresh cluster has no flag on" do
      assert ClusterFlags.enabled(ClusterFlags.new()) == []
      refute ClusterFlags.enabled?(ClusterFlags.new(), :batch_format)
    end
  end

  describe "apply/2" do
    test "switching a flag on" do
      assert {state, :ok} = ClusterFlags.apply(ClusterFlags.new(), {:enable_flag, :batch_format})
      assert ClusterFlags.enabled?(state, :batch_format)
      assert ClusterFlags.enabled(state) == [:batch_format]
    end

    test "switching the same flag on again is idempotent and answers the same thing" do
      {once, :ok} = ClusterFlags.apply(ClusterFlags.new(), {:enable_flag, :batch_format})
      {twice, reply} = ClusterFlags.apply(once, {:enable_flag, :batch_format})

      assert reply == :ok
      assert twice == once
    end

    test "flags are independent of one another" do
      {state, :ok} = ClusterFlags.apply(ClusterFlags.new(), {:enable_flag, :batch_format})
      {state, :ok} = ClusterFlags.apply(state, {:enable_flag, :compaction})

      assert ClusterFlags.enabled(state) == [:batch_format, :compaction]
    end

    test "enabled/1 is sorted, so two reads of the same state compare equal" do
      {a, :ok} = ClusterFlags.apply(ClusterFlags.new(), {:enable_flag, :zeta})
      {a, :ok} = ClusterFlags.apply(a, {:enable_flag, :alpha})
      {b, :ok} = ClusterFlags.apply(ClusterFlags.new(), {:enable_flag, :alpha})
      {b, :ok} = ClusterFlags.apply(b, {:enable_flag, :zeta})

      assert ClusterFlags.enabled(a) == ClusterFlags.enabled(b)
      assert ClusterFlags.enabled(a) == [:alpha, :zeta]
    end

    test "there is no command that switches a flag off" do
      # The grow-only property is what makes the local cache safe to lag and the log safe to replay in
      # any order. A release that adds a :disable_flag command breaks both, and this is where it is
      # noticed.
      assert Map.keys(ClusterFlags.command_versions()) == [{:enable_flag, 2}]
    end
  end

  describe "command_versions/0" do
    test "enable_flag was introduced at machine version 2, not 0" do
      # At 0 a member on a pre-bridge build would skip the command instead of refusing it, which is the
      # exact divergence the machine version exists to close. Registering it at 2 is what makes the
      # refusal below happen.
      assert ClusterFlags.command_versions() == %{{:enable_flag, 2} => 2}
    end

    test "a member still at machine version 1 refuses the command rather than applying it" do
      refused = fn _meta, _command, _state ->
        flunk("the gate let a command through below the version that introduced it")
      end

      assert gate(1, {:enable_flag, :batch_format}, refused) ==
               {ClusterFlags.new(), {:error, {:unsupported_command, {:enable_flag, 2}, 2, 1}}}
    end

    test "a member at machine version 2 applies it" do
      assert {state, :ok} = gate(2, {:enable_flag, :batch_format}, &apply_pure/3)
      assert ClusterFlags.enabled?(state, :batch_format)
    end
  end

  property "the log applies in any order to the same state, because the set only grows" do
    check all(commands <- list_of(member_of([:a, :b, :c, :d]), max_length: 8)) do
      folded = Enum.reduce(commands, ClusterFlags.new(), &enable/2)
      shuffled = commands |> Enum.shuffle() |> Enum.reduce(ClusterFlags.new(), &enable/2)
      replayed = Enum.reduce(commands, folded, &enable/2)

      assert ClusterFlags.enabled(folded) == ClusterFlags.enabled(shuffled)
      # Replaying the same log on top of the state it produced changes nothing: a restart that replays
      # from the beginning reaches the same place.
      assert ClusterFlags.enabled(replayed) == ClusterFlags.enabled(folded)
      assert ClusterFlags.enabled(folded) == commands |> Enum.uniq() |> Enum.sort()
    end
  end

  defp enable(flag, state), do: state |> ClusterFlags.apply({:enable_flag, flag}) |> elem(0)

  # The command through the real gate, at a given effective machine version, on a fresh store.
  defp gate(effective, command, apply_fun) do
    meta = %{machine_version: effective, index: 1, term: 1, system_time: 0}
    MachineVersion.apply(meta, command, ClusterFlags.new(), ClusterFlags.command_versions(), apply_fun)
  end

  defp apply_pure(_meta, command, state), do: ClusterFlags.apply(state, command)
end
