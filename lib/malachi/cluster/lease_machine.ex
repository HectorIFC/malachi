defmodule Malachi.Cluster.LeaseMachine do
  @moduledoc """
  A `ra` (Raft) state machine that replicates a `Malachi.Cluster.Lease`.

  `apply/3` delegates to the pure `Lease.apply/3`, feeding it the ra command metadata's `system_time`
  (the leader's clock, stamped once and replicated in the log) as `now`. The machine never reads a clock
  itself: that would be non-deterministic and break Raft - so every replica applies the same command at
  the same `now` and reaches the same lease state. One dedicated ra cluster backs the cluster's lease.
  """

  @behaviour :ra_machine

  alias Malachi.Cluster.Lease

  @impl true
  def init(_config), do: Lease.new()

  @impl true
  def apply(meta, command, %Lease{} = state), do: Lease.apply(state, command, meta.system_time)
end
