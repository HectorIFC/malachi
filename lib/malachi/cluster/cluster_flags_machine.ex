defmodule Malachi.Cluster.ClusterFlagsMachine do
  @moduledoc """
  A `ra` (Raft) state machine that replicates `Malachi.Cluster.ClusterFlags`, the cluster's
  operator-enabled feature flags.

  `apply/3` delegates to the pure, deterministic `ClusterFlags.apply/2`, so the flags become
  Raft-replicated with no logic in the machine itself, exactly the shape of
  `Malachi.Cluster.RingMachine` and `Malachi.Cluster.LeaseMachine`. Like the ring it needs no clock:
  switching a flag on is a set insert.

  Versioned through `Malachi.Cluster.MachineVersion`, which is also what keeps a member running a build
  from before this store existed from ever being told to enable a flag: `{:enable_flag, 2}` was
  introduced at machine version 2, so it is refused identically on every member until the last one runs
  a build that supports 2.
  """

  @behaviour :ra_machine

  alias Malachi.Cluster.ClusterFlags
  alias Malachi.Cluster.MachineVersion

  @impl true
  def init(_config), do: ClusterFlags.new()

  @impl true
  def version, do: MachineVersion.version()

  @impl true
  def which_module(_version), do: __MODULE__

  @impl true
  def apply(meta, command, %ClusterFlags{} = state) do
    MachineVersion.apply(meta, command, state, ClusterFlags.command_versions(), fn _meta, command, state ->
      ClusterFlags.apply(state, command)
    end)
  end
end
