defmodule Malachi.Cluster.MetadataMachine do
  @moduledoc """
  A `ra` (Raft) state machine that replicates `Malachi.Metadata`.

  ra's `apply/3` delegates to the pure, deterministic `Metadata.apply/2`, exactly the
  contract the metadata machine was designed for, so the control-plane metadata becomes
  durable and Raft-replicated with **no change to the business logic**. One ra cluster
  backs one DS-RSM vnode; leadership of that cluster is the vnode's coordinator.

  Determinism is what makes this safe: every replica applies the same command log and
  reaches the same `Metadata` state (the property the `MetadataPropertyTest` pins down).
  """

  @behaviour :ra_machine

  alias Malachi.Metadata

  @impl true
  def init(_config), do: Metadata.new()

  @impl true
  def apply(_meta, command, %Metadata{} = state), do: Metadata.apply(state, command)
end
