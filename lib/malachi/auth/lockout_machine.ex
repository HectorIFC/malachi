defmodule Malachi.Auth.LockoutMachine do
  @moduledoc """
  A `ra` (Raft) state machine that replicates a `Malachi.Auth.LockoutRegistry` over a dedicated `ra` cluster:
  the cluster's account-lockout store. Mirrors `Malachi.Auth.UserMachine`.

  `apply/3` delegates to the pure `LockoutRegistry.apply/3`, feeding it the ra command metadata's `system_time`
  (the leader's clock, stamped once and replicated in the log) as `now`. The machine never reads a clock
  itself: that would be non-deterministic and break Raft - so every replica applies the same command at the
  same `now` and reaches the same lockout state.
  """

  @behaviour :ra_machine

  alias Malachi.Auth.LockoutRegistry

  @impl true
  def init(_config), do: LockoutRegistry.new()

  @impl true
  def apply(meta, command, %LockoutRegistry{} = state) do
    LockoutRegistry.apply(state, command, meta.system_time)
  end
end
