defmodule Malachi.Cluster.MetadataMachineTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.MachineVersion
  alias Malachi.Cluster.MetadataMachine
  alias Malachi.Metadata

  # What ra hands `apply/3`: the effective version and the timestamp the leader wrote into the entry.
  defp meta(index, system_time),
    do: %{index: index, term: 1, machine_version: MachineVersion.version(), system_time: system_time}

  defp replay(entries) do
    Enum.reduce(entries, MetadataMachine.init(%{}), fn {meta, command}, state ->
      {state, _reply} = MetadataMachine.apply(meta, command, state)
      state
    end)
  end

  defp log do
    [
      {meta(1, 1_000), {:create_topic, "events", 4}},
      {meta(2, 2_000), {:register_segment, {"events", 0}, {{"events", 0}, 0}, [:b1], 0}}
    ]
  end

  test "a segment opens at the timestamp of the log entry that registered it" do
    assert %{opened_at: 2_000} = Metadata.get_segment(replay(log()), {{"events", 0}, 0})
  end

  test "replaying the same log on another replica, or after a restart, stamps the same opening time" do
    assert replay(log()) == replay(log())
  end

  # No log can contain these shapes, so no replay reaches them. A member that is handed one anyway refuses
  # it by its tag, with the state unchanged, the same on every replica.
  test "refuses the retired policy commands as unknown, leaving the state alone" do
    state = replay(log())
    effective = MachineVersion.version()

    assert {^state, {:error, {:unknown_command, {:define_policy, 3}, ^effective}}} =
             MetadataMachine.apply(meta(3, 3_000), {:define_policy, "p", %{}}, state)

    assert {^state, {:error, {:unknown_command, {:set_topic_policy, 3}, ^effective}}} =
             MetadataMachine.apply(meta(3, 3_000), {:set_topic_policy, "events", "p"}, state)
  end
end
