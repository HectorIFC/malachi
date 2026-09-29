defmodule Malachi.Cluster.PolicyMachine do
  @moduledoc """
  A `ra` (Raft) state machine that replicates a `Malachi.Cluster.PolicyRegistry` over a dedicated `ra`
  cluster: the cluster's storage policy definitions. Mirrors `Malachi.Auth.AclMachine`.

  `apply/3` delegates to the pure `PolicyRegistry.apply/3` with the group's effective machine version,
  which is what a definition's fields are validated against. A policy carries no timestamps, so the
  command metadata's `system_time` is unused: the machine stays fully deterministic and every replica
  converges on the same definitions.

  Versioned through `Malachi.Cluster.MachineVersion`.
  """

  @behaviour :ra_machine

  alias Malachi.Cluster.MachineVersion
  alias Malachi.Cluster.PolicyRegistry

  @impl true
  def init(_config), do: PolicyRegistry.new()

  @impl true
  def version, do: MachineVersion.version()

  @impl true
  def which_module(_version), do: __MODULE__

  @impl true
  def apply(meta, command, %PolicyRegistry{} = state) do
    MachineVersion.apply(meta, command, state, PolicyRegistry.command_versions(), fn meta, command, state ->
      PolicyRegistry.apply(state, command, meta.machine_version)
    end)
  end
end
