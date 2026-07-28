defmodule Malachi.Test.AliveMembersStub do
  @moduledoc """
  A tiny stand-in for `Malachi.Cluster.MembershipServer` that answers `:alive_members` with a fixed,
  already-sorted list of member refs. Lets tests drive `Malachi.Application.membership_leader/1`
  (the D-c-1d bootstrap-orchestrator policy) without a real SWIM cluster.
  """
  use GenServer

  @doc "Starts a stub that replies to `alive_members/1` with `members` (a sorted list of `{name, node}`)."
  @spec start_link([{module(), node()}]) :: GenServer.on_start()
  def start_link(members), do: GenServer.start_link(__MODULE__, members)

  @impl true
  def init(members), do: {:ok, members}

  @impl true
  def handle_call(:alive_members, _from, members), do: {:reply, members, members}
end
