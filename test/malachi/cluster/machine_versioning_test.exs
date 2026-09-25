defmodule Malachi.Cluster.MachineVersioningTest do
  @moduledoc """
  The seven control-plane `ra` machines, checked against one contract: each declares the shared machine
  version, maps every version to itself, accepts `ra`'s `{:machine_version, from, to}` without touching
  its state, refuses a command it does not know with the versioned reply instead of skipping it or
  raising, and keeps its command table in step with its `@type command`.

  The property checks the wiring, the part a unit test of `Malachi.Cluster.MachineVersion` cannot see:
  for any sequence of real and unknown commands at any effective version, the machine answers exactly
  what its pure module answers for a known command (so the gate passes the right arguments, including
  the leader's `system_time`) and refuses an unknown one with the state unchanged.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Auth.AclMachine
  alias Malachi.Auth.AclRegistry
  alias Malachi.Auth.LockoutMachine
  alias Malachi.Auth.LockoutRegistry
  alias Malachi.Auth.UserMachine
  alias Malachi.Auth.UserRegistry
  alias Malachi.Cluster.ClusterFlags
  alias Malachi.Cluster.ClusterFlagsMachine
  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.Lease
  alias Malachi.Cluster.LeaseMachine
  alias Malachi.Cluster.MachineVersion
  alias Malachi.Cluster.MetadataMachine
  alias Malachi.Cluster.Ring
  alias Malachi.Cluster.RingMachine
  alias Malachi.Cluster.RingTopology
  alias Malachi.Metadata

  # {machine, pure module, how the pure module is applied without ra}
  @machines [
    {MetadataMachine, Metadata, :no_clock},
    {LeaseMachine, Lease, :clock},
    {RingMachine, Ring, :no_clock},
    {UserMachine, UserRegistry, :clock},
    {LockoutMachine, LockoutRegistry, :clock},
    {AclMachine, AclRegistry, :no_clock},
    {ClusterFlagsMachine, ClusterFlags, :no_clock}
  ]

  defp meta(effective, now \\ 1_700_000_000_000),
    do: %{machine_version: effective, index: 1, term: 1, system_time: now}

  defp pure_apply(pure, :clock, state, command, now), do: pure.apply(state, command, now)
  defp pure_apply(pure, :no_clock, state, command, _now), do: pure.apply(state, command)

  for {machine, pure, _clock} <- @machines do
    describe "#{inspect(machine)}" do
      test "declares the shared machine version and maps every version to itself" do
        assert unquote(machine).version() == MachineVersion.version()
        # Version 2 is where `Malachi.Cluster.ClusterFlags` introduced `{:enable_flag, flag}`; the other
        # six moved with it, paying nothing but a no-op `{:machine_version, 1, 2}`.
        assert unquote(machine).version() == 2

        for version <- 0..unquote(machine).version(),
            do: assert(unquote(machine).which_module(version) == unquote(machine))
      end

      test "accepts ra's machine version no-op with the state unchanged" do
        state = unquote(machine).init(%{})
        current = MachineVersion.code_version()

        for from <- 0..(current - 1) do
          assert {^state, :ok} = unquote(machine).apply(meta(current), {:machine_version, from, current}, state)
        end
      end

      test "refuses an unknown command with the versioned reply and the state unchanged" do
        state = unquote(machine).init(%{})

        assert {^state, {:error, {:unknown_command, {:bogus, 2}, 1}}} =
                 unquote(machine).apply(meta(1), {:bogus, "x"}, state)
      end

      test "refuses a known tag in a shape it does not implement, instead of raising or skipping it" do
        # What a later release does to an existing command: another field. Dispatching that on this code
        # raises where the pure module has no clause (lease, ring) and is skipped where it has a
        # catch-all (metadata, the auth registries), which is the divergence the gate exists to stop.
        state = unquote(machine).init(%{})
        # Taken from @type command rather than from the table, so this reads the same before and after.
        {tag, arity} = unquote(pure) |> command_keys() |> Enum.sort() |> hd()
        wider = List.to_tuple([tag | List.duplicate(:filler, arity)])

        assert {^state, {:error, {:unknown_command, {^tag, _wider_arity}, 1}}} =
                 unquote(machine).apply(meta(1), wider, state)
      end

      test "its command table lists exactly the shapes of its @type command, none above the code version" do
        table = unquote(pure).command_versions()

        assert MapSet.new(Map.keys(table)) == command_keys(unquote(pure))
        assert Enum.all?(Map.values(table), &(&1 in 0..MachineVersion.code_version()))
      end
    end
  end

  property "each machine answers what its pure module answers, and refuses what it does not know" do
    check all(
            {machine, pure, clock} <- StreamData.member_of(@machines),
            commands <- StreamData.list_of(command(pure), max_length: 25),
            effective <- StreamData.integer(0..2),
            now <- StreamData.integer(1_700_000_000_000..1_700_000_100_000),
            max_runs: 300
          ) do
      Enum.reduce(commands, machine.init(%{}), fn command, state ->
        {next, reply} = machine.apply(meta(effective, now), command, state)

        case Map.fetch(pure.command_versions(), MachineVersion.command_key(command)) do
          {:ok, introduced} when introduced <= effective ->
            assert {next, reply} == pure_apply(pure, clock, state, command, now)

          # Reached since `{:enable_flag, 2}` was introduced at version 2: a command introduced above the
          # group's effective version is refused, not applied, and identically on every member.
          {:ok, introduced} ->
            assert next == state

            assert reply ==
                     {:error, {:unsupported_command, MachineVersion.command_key(command), introduced, effective}}

          :error ->
            assert next == state
            assert reply == {:error, {:unknown_command, MachineVersion.command_key(command), effective}}
        end

        next
      end)
    end
  end

  test "a topic export carries the current export format, and an old export without it still inserts" do
    {state, {:ok, _root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 2})

    export = Metadata.export_topic(state, "events")
    assert export.export_format == Metadata.export_format()
    assert {_without, ^export} = Metadata.extract_topic(state, "events")

    legacy = Map.delete(export, :export_format)
    {inserted, :ok} = MetadataMachine.apply(meta(1), {:insert_topic, legacy}, Metadata.new())
    assert Metadata.get_topic(inserted, "events") == Metadata.get_topic(state, "events")
  end

  test "an export above the group's effective version is refused by the metadata machine" do
    {state, {:ok, _root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 2})
    newer = %{Metadata.export_topic(state, "events") | export_format: 2}
    empty = Metadata.new()

    assert {^empty, {:error, {:unsupported_export_format, 2, 1}}} =
             MetadataMachine.apply(meta(1), {:insert_topic, newer}, empty)
  end

  # --- command generators: plausible commands for each pure module, plus unknown ones ---

  defp command(pure), do: StreamData.one_of([known_command(pure), unknown_command()])

  defp unknown_command do
    StreamData.one_of([
      StreamData.tuple({StreamData.constant(:bogus), StreamData.integer()}),
      StreamData.constant(:not_a_command),
      StreamData.constant("garbage")
    ])
  end

  defp known_command(Metadata) do
    topic = StreamData.member_of(["a", "b"])

    StreamData.one_of([
      StreamData.tuple({StreamData.constant(:create_topic), topic, StreamData.integer(1..3)}),
      StreamData.tuple({StreamData.constant(:seal_topic), topic}),
      StreamData.tuple({StreamData.constant(:delete_topic), topic}),
      StreamData.tuple({StreamData.constant(:begin_migration), topic}),
      StreamData.tuple({StreamData.constant(:end_migration), topic}),
      StreamData.tuple({StreamData.constant(:extract_topic), topic}),
      StreamData.tuple({StreamData.constant(:commit_offset), StreamData.constant("g"), topic, StreamData.constant(%{})})
    ])
  end

  defp known_command(Lease) do
    holder = StreamData.member_of([:n1, :n2])

    StreamData.one_of([
      StreamData.tuple({StreamData.constant(:acquire_or_renew), holder, StreamData.integer(1..50_000)}),
      StreamData.tuple({StreamData.constant(:release), holder, StreamData.integer(0..3)})
    ])
  end

  defp known_command(Ring) do
    topology = StreamData.map(StreamData.integer(0..3), &%{RingTopology.new(HashRing.new(), %{}) | version: &1})

    StreamData.one_of([
      StreamData.map(topology, &{:init, &1}),
      StreamData.map(
        StreamData.tuple({StreamData.integer(0..3), StreamData.integer(0..3), topology}),
        fn {expected, fence, topology} -> {:advance, expected, fence, topology} end
      )
    ])
  end

  defp known_command(UserRegistry) do
    user = StreamData.member_of(["alice", "bob"])

    StreamData.one_of([
      StreamData.tuple(
        {StreamData.constant(:put_user), user, StreamData.constant("hash"), StreamData.constant([:produce])}
      ),
      StreamData.tuple({StreamData.constant(:delete_user), user}),
      StreamData.tuple({StreamData.constant(:update_password), user, StreamData.constant("hash2")}),
      StreamData.map(user, &{:import_users, [{&1, "hash", [:consume]}]})
    ])
  end

  defp known_command(LockoutRegistry) do
    key = StreamData.tuple({StreamData.member_of(["alice", "bob"]), StreamData.constant({127, 0, 0, 1})})
    config = %{max_attempts: 2, base_duration_ms: 1_000, progressive: true}

    StreamData.one_of([
      StreamData.map(key, &{:failed_attempt, &1, config}),
      StreamData.map(key, &{:successful_auth, &1}),
      StreamData.map(StreamData.member_of(["alice", "bob"]), &{:unlock_user, &1}),
      StreamData.map(key, &{:unlock_key, &1}),
      StreamData.map(StreamData.integer(0..100_000), &{:cleanup, &1})
    ])
  end

  defp known_command(ClusterFlags) do
    StreamData.map(StreamData.member_of([:batch_format, :compaction]), &{:enable_flag, &1})
  end

  defp known_command(AclRegistry) do
    user = StreamData.member_of(["alice", "bob"])
    operation = StreamData.member_of([:produce, :consume])
    resource = StreamData.member_of([{:literal, "t"}, {:prefix, "t."}])

    StreamData.one_of([
      StreamData.tuple({StreamData.constant(:grant), user, operation, resource}),
      StreamData.tuple({StreamData.constant(:revoke), user, operation, resource}),
      StreamData.tuple({StreamData.constant(:revoke_user), user})
    ])
  end

  # --- the shapes of a module's @type command, read from its compiled typespecs ---

  defp command_keys(module) do
    {:ok, types} = Code.Typespec.fetch_types(module)
    {:type, {:command, ast, []}} = Enum.find(types, &match?({:type, {:command, _ast, []}}, &1))
    ast |> tuple_keys() |> MapSet.new()
  end

  defp tuple_keys({:type, _line, :union, members}), do: Enum.flat_map(members, &tuple_keys/1)

  defp tuple_keys({:type, _line, :tuple, [{:atom, _atom_line, tag} | rest]}), do: [{tag, length(rest) + 1}]
end
