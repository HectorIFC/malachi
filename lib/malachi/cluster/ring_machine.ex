defmodule Malachi.Cluster.RingMachine do
  @moduledoc """
  A `ra` (Raft) state machine that replicates `Malachi.Cluster.Ring`, the cluster's durable routing
  topology.

  `apply/3` delegates to the pure, deterministic `Ring.apply/2`, so the ring becomes Raft-replicated
  with no logic in the machine itself, exactly the shape of `Malachi.Cluster.MetadataMachine` and
  `Malachi.Cluster.LeaseMachine`. Unlike the lease machine it needs no clock: the ring's guards are
  compare-and-set on version and fence, both carried in the command.

  Versioned through `Malachi.Cluster.MachineVersion`, which also keeps an unknown command from
  raising here: `Ring.apply/2` has no catch-all clause.
  """

  @behaviour :ra_machine

  alias Malachi.Cluster.MachineVersion
  alias Malachi.Cluster.Ring

  @impl true
  def init(_config), do: Ring.new()

  @impl true
  def version, do: MachineVersion.version()

  @impl true
  def which_module(_version), do: __MODULE__

  @impl true
  def apply(meta, command, %Ring{} = state) do
    MachineVersion.apply(meta, command, state, Ring.command_versions(), fn _meta, command, state ->
      Ring.apply(state, command)
    end)
  end
end
