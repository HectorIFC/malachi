defmodule Malachi.Cluster.MachineVersionTest do
  # async: false because the pin is node-wide application env and check/3 drives a real ra member.
  use ExUnit.Case, async: false
  use ExUnitProperties

  import ExUnit.CaptureLog

  alias Malachi.Cluster.MachineVersion
  alias Malachi.Cluster.MetadataMachine
  alias Malachi.Cluster.RaCluster
  alias Malachi.Test.StuckRaMember

  @table %{{:old, 2} => 0, {:current, 1} => 1, {:future, 2} => 2}

  defp meta(effective), do: %{machine_version: effective, index: 1, term: 1, system_time: 0}

  # An apply_fun that proves it ran: it records the command it was handed in the state.
  defp recording_apply(meta, command, state), do: {[{command, meta.machine_version} | state], :applied}

  setup do
    on_exit(fn -> Application.delete_env(:malachi, :ra_machine_version_pin) end)
  end

  describe "apply/5" do
    test "applies a command introduced at or below the effective version" do
      assert {[{{:old, 1}, 1}], :applied} = MachineVersion.apply(meta(1), {:old, 1}, [], @table, &recording_apply/3)
      assert {[{{:current}, 1}], :applied} = MachineVersion.apply(meta(1), {:current}, [], @table, &recording_apply/3)
    end

    test "refuses a known tag in a shape the table does not list, state untouched" do
      # The shape is what old code can apply, so a wider or narrower tuple under a known tag is as
      # foreign as a new tag: dispatching it would raise in a pure module that has no clause for it.
      assert {[:before], {:error, {:unknown_command, {:old, 3}, 1}}} =
               MachineVersion.apply(meta(1), {:old, 1, :extra}, [:before], @table, &recording_apply/3)

      assert {[:before], {:error, {:unknown_command, {:old, 1}, 1}}} =
               MachineVersion.apply(meta(1), {:old}, [:before], @table, &recording_apply/3)
    end

    test "refuses a command introduced above the effective version, state untouched" do
      assert {[:before], {:error, {:unsupported_command, {:future, 2}, 2, 1}}} =
               MachineVersion.apply(meta(1), {:future, :x}, [:before], @table, &recording_apply/3)
    end

    test "refuses a tag missing from the table, state untouched" do
      assert {[:before], {:error, {:unknown_command, {:bogus, 3}, 1}}} =
               MachineVersion.apply(meta(1), {:bogus, 1, 2}, [:before], @table, &recording_apply/3)
    end

    test "refuses a command that is not a tagged tuple or an atom as :invalid" do
      assert {[], {:error, {:unknown_command, {:invalid, 0}, 0}}} =
               MachineVersion.apply(meta(0), "garbage", [], @table, &recording_apply/3)

      assert {[], {:error, {:unknown_command, {:invalid, 0}, 0}}} =
               MachineVersion.apply(meta(0), {}, [], @table, &recording_apply/3)

      assert {[], {:error, {:unknown_command, {:invalid, 0}, 0}}} =
               MachineVersion.apply(meta(0), {"tag", 1}, [], @table, &recording_apply/3)
    end

    test "versions a bare atom command by the atom and size 0" do
      table = Map.put(@table, {:tick, 0}, 0)
      assert {[{:tick, 0}], :applied} = MachineVersion.apply(meta(0), :tick, [], table, &recording_apply/3)
    end

    test "accepts {:machine_version, from, to} with the state unchanged, whatever the table" do
      assert {[:before], :ok} =
               MachineVersion.apply(meta(0), {:machine_version, 0, 1}, [:before], %{}, &recording_apply/3)

      assert {[:before], :ok} =
               MachineVersion.apply(meta(1), {:machine_version, 1, 2}, [:before], %{}, &recording_apply/3)
    end

    test "refuses an insert_topic whose export format is above the effective version" do
      table = %{{:insert_topic, 2} => 0}
      export = %{topic: %{name: "t"}, export_format: 2}

      assert {[:before], {:error, {:unsupported_export_format, 2, 1}}} =
               MachineVersion.apply(meta(1), {:insert_topic, export}, [:before], table, &recording_apply/3)
    end

    test "admits an insert_topic whose export format the group has reached, or that predates the format" do
      table = %{{:insert_topic, 2} => 0}
      at_effective = %{topic: %{name: "t"}, export_format: 1}
      legacy = %{topic: %{name: "t"}}
      malformed = %{topic: %{name: "t"}, export_format: "2"}

      for export <- [at_effective, legacy, malformed] do
        assert {[{{:insert_topic, ^export}, 1}], :applied} =
                 MachineVersion.apply(meta(1), {:insert_topic, export}, [], table, &recording_apply/3)
      end
    end

    property "a refused command never changes the state and never reaches apply_fun" do
      check all(
              entries <-
                StreamData.list_of(
                  StreamData.tuple(
                    {StreamData.member_of([{:a, 2}, {:b, 2}, {:c, 2}, {:d, 2}]), StreamData.integer(0..3)}
                  )
                ),
              table = Map.new(entries),
              effective <- StreamData.integer(0..3),
              tags <- StreamData.list_of(StreamData.member_of([:a, :b, :c, :d, :e]), max_length: 30),
              max_runs: 300
            ) do
        Enum.reduce(tags, [], fn tag, state ->
          {next, reply} = MachineVersion.apply(meta(effective), {tag, :arg}, state, table, &recording_apply/3)

          case Map.fetch(table, {tag, 2}) do
            {:ok, introduced} when introduced <= effective ->
              assert reply == :applied
              assert next == [{{tag, :arg}, effective} | state]

            {:ok, introduced} ->
              assert reply == {:error, {:unsupported_command, {tag, 2}, introduced, effective}}
              assert next == state

            :error ->
              assert reply == {:error, {:unknown_command, {tag, 2}, effective}}
              assert next == state
          end

          next
        end)
      end
    end
  end

  describe "version/0 and pinned/1" do
    test "without a pin, a node advertises its code version" do
      assert MachineVersion.version() == MachineVersion.code_version()
      assert MachineVersion.pinned(5) == 5
    end

    test "a pin holds the advertised version down" do
      Application.put_env(:malachi, :ra_machine_version_pin, 0)
      assert MachineVersion.version() == 0
      assert MachineVersion.pinned(5) == 0
    end

    test "a pin above the code version has no effect" do
      Application.put_env(:malachi, :ra_machine_version_pin, 99)
      assert MachineVersion.version() == MachineVersion.code_version()
    end

    test "a malformed pin raises instead of being ignored" do
      Application.put_env(:malachi, :ra_machine_version_pin, "1")
      assert_raise ArgumentError, ~r/ra_machine_version_pin/, fn -> MachineVersion.version() end

      Application.put_env(:malachi, :ra_machine_version_pin, -1)
      assert_raise ArgumentError, fn -> MachineVersion.version() end
    end
  end

  describe "command_key/1" do
    test "is the leading atom with the tuple size, a bare atom with size 0, or :invalid" do
      assert MachineVersion.command_key({:create_topic, "t", 4}) == {:create_topic, 3}
      assert MachineVersion.command_key({:seal_topic, "t"}) == {:seal_topic, 2}
      assert MachineVersion.command_key(:tick) == {:tick, 0}
      assert MachineVersion.command_key({}) == {:invalid, 0}
      assert MachineVersion.command_key({1, 2}) == {:invalid, 0}
      assert MachineVersion.command_key([:create_topic]) == {:invalid, 0}
    end
  end

  describe "check/3 against a real ra member" do
    setup do
      name = :"mv_check_#{System.unique_integer([:positive])}"
      on_exit(fn -> StuckRaMember.cleanup({name, node()}) end)
      %{name: name}
    end

    test "a member that supports the effective version is :ok, with telemetry and no log", %{name: name} do
      {:ok, _member} = RaCluster.start(MetadataMachine, name, [node()])
      server_id = {name, node()}
      current = StuckRaMember.effective_version()
      attach_telemetry()

      assert eventually(fn -> StuckRaMember.effective(server_id) == current end)

      log =
        capture_log(fn ->
          assert {:ok, nil} = MachineVersion.check(MetadataMachine, server_id, :ok)
        end)

      assert log == ""
      assert_receive {:telemetry, %{effective: ^current, supported: ^current}, %{server_id: ^server_id, stuck: false}}
    end

    test "a member rolled back below the effective version is stuck, logged once, then recovers", %{name: name} do
      server_id = StuckRaMember.start(name)
      machine = MetadataMachine
      current = StuckRaMember.effective_version()
      behind = StuckRaMember.rolled_back_version()
      stuck = {:stuck, current, behind}
      attach_telemetry()

      log =
        capture_log(fn ->
          assert {^stuck, :stuck} = MachineVersion.check(machine, server_id, :ok)
        end)

      assert log =~ "stopped applying entries"
      assert_receive {:telemetry, %{effective: ^current, supported: ^behind}, %{server_id: ^server_id, stuck: true}}

      # Still stuck on the next tick: reported through telemetry, not logged again.
      log = capture_log(fn -> assert {^stuck, nil} = MachineVersion.check(machine, server_id, stuck) end)

      assert log == ""
      assert_receive {:telemetry, _measurements, %{stuck: true}}

      :ok = StuckRaMember.recover(server_id)

      log =
        capture_log([level: :info], fn ->
          assert {:ok, :recovered} = MachineVersion.check(machine, server_id, stuck)
        end)

      assert log =~ "supports the effective version #{current} again"
    end

    test "a member without counters keeps its last status and reports no transition" do
      ghost = {:"mv_ghost_#{System.unique_integer([:positive])}", node()}
      stuck = {:stuck, StuckRaMember.effective_version(), StuckRaMember.rolled_back_version()}

      assert {:ok, nil} = MachineVersion.check(MetadataMachine, ghost, :ok)
      assert {^stuck, nil} = MachineVersion.check(MetadataMachine, ghost, stuck)
    end
  end

  defp attach_telemetry do
    test_pid = self()
    handler = "mv-test-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler,
        [:malachi, :ra, :machine_version],
        fn _event, measurements, metadata, _config -> send(test_pid, {:telemetry, measurements, metadata}) end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp eventually(fun, remaining_ms \\ 5_000) do
    cond do
      fun.() -> true
      remaining_ms <= 0 -> false
      true -> Process.sleep(20) == :ok and eventually(fun, remaining_ms - 20)
    end
  end
end
