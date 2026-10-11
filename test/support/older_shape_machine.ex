defmodule Malachi.Test.OlderShapeMachine do
  @moduledoc """
  A metadata machine whose state has the shape an older build's has: `Malachi.Metadata` without the fields
  added since (`:group_fences`). Everything else is the real `Malachi.Cluster.MetadataMachine`. Used to show
  a read of a vnode led by an older build gives the caller every field.
  """

  @behaviour :ra_machine

  alias Malachi.Cluster.MetadataMachine
  alias Malachi.Metadata

  @impl true
  def init(_config), do: Map.delete(Metadata.new(), :group_fences)

  @impl true
  def apply(meta, command, state), do: MetadataMachine.apply(meta, command, state)
end
