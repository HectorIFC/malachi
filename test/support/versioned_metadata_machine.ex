defmodule Malachi.Test.VersionedMetadataMachine do
  @moduledoc """
  A metadata state machine one version ahead of production, for the mixed-version Raft tests.

  Version 2 adds `{:probe, topic}`, which creates `topic`, so whether a replica applied it is visible
  in its state. Everything else is the real `Malachi.Metadata` behind the real
  `Malachi.Cluster.MachineVersion` gate.

  One code path has to stand in for two binaries, so what this node's code "is" comes from the pin:
  `version/0` is `MachineVersion.pinned(2)`, and a node pinned to 1 plays a binary that has never heard
  of `:probe` (its command table lacks it). Reading that node-local fact inside `apply/3` is exactly the
  non-determinism a real mixed-version group carries, which is what these tests must show the gate
  neutralizing.
  """

  @behaviour :ra_machine

  alias Malachi.Cluster.MachineVersion
  alias Malachi.Metadata

  @code_version 2

  @impl true
  def init(_config), do: Metadata.new()

  @impl true
  def version, do: MachineVersion.pinned(@code_version)

  @impl true
  def which_module(_version), do: __MODULE__

  @impl true
  def apply(meta, command, %Metadata{} = state) do
    MachineVersion.apply(meta, command, state, command_versions(), fn _meta, command, state ->
      apply_command(state, command)
    end)
  end

  @doc "The table this node's code knows: `:probe` only when it implements version 2."
  @spec command_versions() :: MachineVersion.command_versions()
  def command_versions do
    if version() >= 2, do: Map.put(Metadata.command_versions(), {:probe, 2}, 2), else: Metadata.command_versions()
  end

  # The gate refuses :probe before this point on a node whose code is version 1, because that node's own
  # `command_versions/0` omits the shape. Without the gate this is where replicas diverge: one applies
  # the command and the others do not.
  defp apply_command(state, {:probe, topic}) do
    if version() >= 2 do
      Metadata.apply(state, {:create_topic, topic, 1})
    else
      # What a version-1 binary answers: it has no clause for :probe, so the command reaches
      # `Malachi.Metadata.apply/2` as an unknown one and its catch-all leaves the state untouched.
      # Written out rather than called, because that function's spec admits only the commands it
      # implements, and handing it a shape it does not is the very thing this double exists to model.
      {state, {:error, :unknown_command}}
    end
  end

  defp apply_command(state, command), do: Metadata.apply(state, command)
end
