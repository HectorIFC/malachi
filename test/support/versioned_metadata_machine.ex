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
    if version() >= 2, do: Map.put(Metadata.command_versions(), :probe, 2), else: Metadata.command_versions()
  end

  # A version-1 binary has no clause for :probe, so it lands in Metadata's catch-all like any unknown
  # command. The gate refuses it before this point; without the gate, this is where replicas diverge.
  defp apply_command(state, {:probe, topic} = command) do
    if version() >= 2, do: Metadata.apply(state, {:create_topic, topic, 1}), else: Metadata.apply(state, command)
  end

  defp apply_command(state, command), do: Metadata.apply(state, command)
end
