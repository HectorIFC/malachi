defmodule Malachi.Auth.UserMachine do
  @moduledoc """
  A `ra` (Raft) state machine that replicates a `Malachi.Auth.UserRegistry` over a dedicated `ra` cluster:
  the cluster's user store. Mirrors `Malachi.Cluster.LeaseMachine`.

  `apply/3` delegates to the pure `UserRegistry.apply/3`, feeding it the ra command metadata's `system_time`
  (the leader's clock, stamped once and replicated in the log) as `now`. The machine never reads a clock
  itself: that would be non-deterministic and break Raft - so every replica applies the same command at
  the same `now` and reaches the same user set.

  Versioned through `Malachi.Cluster.MachineVersion`.
  """

  @behaviour :ra_machine

  alias Malachi.Auth.UserRegistry
  alias Malachi.Cluster.MachineVersion

  @impl true
  def init(_config), do: UserRegistry.new()

  @impl true
  def version, do: MachineVersion.version()

  @impl true
  def which_module(_version), do: __MODULE__

  @impl true
  def apply(meta, command, %UserRegistry{} = state) do
    MachineVersion.apply(meta, command, state, UserRegistry.command_versions(), fn meta, command, state ->
      UserRegistry.apply(state, command, meta.system_time)
    end)
  end
end
