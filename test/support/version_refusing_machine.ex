defmodule Malachi.Test.VersionRefusingMachine do
  @moduledoc """
  A metadata machine that refuses every `create_topic` the way `Malachi.Cluster.MachineVersion` refuses a
  command above a group's effective version: `{:unsupported_command, ...}` for a topic named
  `"unsupported"`, and `{:unknown_command, ...}` (the answer of an older member that has no such command
  at all) for any other. Everything else is the real `Malachi.Cluster.MetadataMachine`. Used to show a
  cache never takes a command every replica refused, although `Malachi.Metadata.apply/2` would apply it.
  """

  @behaviour :ra_machine

  alias Malachi.Cluster.MetadataMachine
  alias Malachi.Metadata

  @impl true
  def init(_config), do: Metadata.new()

  @impl true
  def apply(_meta, {:create_topic, "unsupported", _bits}, state),
    do: {state, {:error, {:unsupported_command, {:create_topic, 3}, 4, 3}}}

  def apply(_meta, {:create_topic, _name, _bits}, state),
    do: {state, {:error, {:unknown_command, {:create_topic, 3}, 3}}}

  def apply(meta, command, state), do: MetadataMachine.apply(meta, command, state)
end
