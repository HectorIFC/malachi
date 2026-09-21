defmodule Malachi.Test.RefusingInsertMachine do
  @moduledoc """
  A metadata machine whose group refuses every topic export, the way a vnode answers when its effective
  machine version is below the export's format. Everything else is the real
  `Malachi.Cluster.MetadataMachine`. Used to show a vnode split survives a destination that commits the
  insert and still says no.
  """

  @behaviour :ra_machine

  alias Malachi.Cluster.MetadataMachine
  alias Malachi.Metadata

  @impl true
  def init(_config), do: Metadata.new()

  @impl true
  def apply(_meta, {:insert_topic, _export}, state), do: {state, {:error, {:unsupported_export_format, 1, 0}}}
  def apply(meta, command, state), do: MetadataMachine.apply(meta, command, state)
end
