defmodule Malachi.Auth.AclMachine do
  @moduledoc """
  A `ra` (Raft) state machine that replicates a `Malachi.Auth.AclRegistry` over a dedicated `ra` cluster:
  the cluster's per-topic ACL store. Mirrors `Malachi.Auth.UserMachine`.

  `apply/3` delegates to the pure `AclRegistry.apply/2`. Unlike the user/lockout machines, ACL grants carry
  no timestamps, so the command metadata's `system_time` is unused (dropped): the machine stays fully
  deterministic and every replica converges on the same grant set.

  Versioned through `Malachi.Cluster.MachineVersion`.
  """

  @behaviour :ra_machine

  alias Malachi.Auth.AclRegistry
  alias Malachi.Cluster.MachineVersion

  @impl true
  def init(_config), do: AclRegistry.new()

  @impl true
  def version, do: MachineVersion.version()

  @impl true
  def which_module(_version), do: __MODULE__

  @impl true
  def apply(meta, command, %AclRegistry{} = state) do
    MachineVersion.apply(meta, command, state, AclRegistry.command_versions(), fn _meta, command, state ->
      AclRegistry.apply(state, command)
    end)
  end
end
