defmodule Malachi.Test.RouterTopology do
  @moduledoc """
  The consumer-group router's topology cleared for the length of a test and put back after. The topic
  routing reads it, and a sharded broker started by another test can leave one behind that names vnodes
  this node's broker does not hold. Use as `setup {Malachi.Test.RouterTopology, :clear}`, or call `clear/1`
  from a setup, in a module that is not async.
  """

  # mirrors Malachi.Consumer.CoordinatorRouter's private @topology_key
  @router_topology {Malachi.Consumer.CoordinatorRouter, :topology}

  @doc "Clears the topology, arranging for it to be put back when the test exits. Returns `:ok`."
  @spec clear(map()) :: :ok
  def clear(_context \\ %{}) do
    topology = :persistent_term.get(@router_topology, nil)
    :persistent_term.erase(@router_topology)

    ExUnit.Callbacks.on_exit(fn ->
      if topology,
        do: :persistent_term.put(@router_topology, topology),
        else: :persistent_term.erase(@router_topology)
    end)
  end
end
