defmodule Malachi.Auth.AclMachine do
  @moduledoc """
  A `ra` (Raft) state machine that replicates a `Malachi.Auth.AclRegistry` over a dedicated `ra` cluster:
  the cluster's per-topic ACL store. Mirrors `Malachi.Auth.UserMachine`.

  `apply/3` delegates to the pure `AclRegistry.apply/2`. Unlike the user/lockout machines, ACL grants carry
  no timestamps, so the command metadata's `system_time` is unused (dropped): the machine stays fully
  deterministic and every replica converges on the same grant set.
  """

  @behaviour :ra_machine

  alias Malachi.Auth.AclRegistry

  @impl true
  def init(_config), do: AclRegistry.new()

  @impl true
  def apply(_meta, command, %AclRegistry{} = state) do
    AclRegistry.apply(state, command)
  end
end
