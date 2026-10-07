defmodule Malachi.Cluster.MetadataMachine do
  @moduledoc """
  A `ra` (Raft) state machine that replicates `Malachi.Metadata`.

  ra's `apply/3` delegates to the pure, deterministic `Metadata.apply/3`, exactly the
  contract the metadata machine was designed for, so the control-plane metadata becomes
  durable and Raft-replicated with **no change to the business logic**. The clock it hands over is
  `meta.system_time`, the timestamp the leader wrote into the log entry: the same on every replica
  and on every replay, which is what lets a registered segment carry its `opened_at` without the
  command carrying it. One ra cluster
  backs one DS-RSM vnode; leadership of that cluster is the vnode's coordinator.

  Determinism is what makes this safe: every replica applies the same command log and
  reaches the same `Metadata` state (the property the `MetadataPropertyTest` pins down).

  Versioned through `Malachi.Cluster.MachineVersion`: a command above the group's effective
  machine version, or one this code does not know, is refused identically on every replica.
  """

  @behaviour :ra_machine

  alias Malachi.Cluster.MachineVersion
  alias Malachi.Metadata

  @impl true
  def init(_config), do: Metadata.new()

  @impl true
  def version, do: MachineVersion.version()

  @impl true
  def which_module(_version), do: __MODULE__

  @impl true
  def apply(meta, command, %Metadata{} = state) do
    MachineVersion.apply(meta, command, state, Metadata.command_versions(), fn meta, command, state ->
      Metadata.apply(state, command, Map.get(meta, :system_time))
    end)
  end
end
