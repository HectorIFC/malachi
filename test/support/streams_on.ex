defmodule Malachi.Test.StreamsOn do
  @moduledoc """
  The `producer_streams` flag on for the length of a test, and the consumer-group router's topology cleared
  (the topic routing reads it), both put back after. Use as `setup {Malachi.Test.StreamsOn, :setup}`, or call
  `setup/1` from a setup, in a module that is not async (`Malachi.Test.ClusterFlagsPause`).
  """

  alias Malachi.Cluster.ClusterFlagsCache
  alias Malachi.Routing
  alias Malachi.Test.ClusterFlagsPause

  # mirrors Malachi.Consumer.CoordinatorRouter's private @topology_key
  @router_topology {Malachi.Consumer.CoordinatorRouter, :topology}

  @doc "Turns the flag on and clears the topology, arranging for both to be put back. Returns `:ok`."
  @spec setup(map()) :: :ok
  def setup(_context) do
    :ok = ClusterFlagsPause.pause()
    ClusterFlagsCache.put([Routing.flag()])
    topology = :persistent_term.get(@router_topology, nil)
    :persistent_term.erase(@router_topology)

    ExUnit.Callbacks.on_exit(fn ->
      if topology,
        do: :persistent_term.put(@router_topology, topology),
        else: :persistent_term.erase(@router_topology)
    end)
  end
end
