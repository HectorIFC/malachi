defmodule Malachi.Cluster.Failover do
  @moduledoc """
  Pure primary-failover policy for **active** segments: when an active segment's primary (the head
  of its `replica_set`) is no longer alive, a live replica must take over so writes can continue.

  `plan/2` returns the `:set_segment_replicas` commands that **promote** a live replica to primary
  by moving it to the head of the segment's replica set. The dead broker stays in the set as a
  follower that simply will not ack (the quorum comes from the live replicas); once the segment
  seals, `Malachi.Cluster.SelfHealing` replaces it. No data moves — the live replicas already hold
  the segment.

  Only active segments are considered: sealed segments are immutable, and their reads are restored
  by `SelfHealing` (whose placement already yields a live primary). The commands are applied
  through the control plane, where `Malachi.Broker.apply_heal/2` also updates the active-segment
  cache so the next produce routes to the new primary.
  """

  alias Malachi.Metadata

  @doc """
  The promotion commands for every active segment whose primary is dead, given the live broker
  set. A segment whose every replica is dead is skipped (nothing to promote). Returns a sorted
  (deterministic) list.
  """
  @spec plan(Metadata.t(), [Metadata.broker()]) :: [Metadata.command()]
  def plan(%Metadata{} = metadata, live_brokers) do
    live = MapSet.new(live_brokers)

    metadata.segments
    |> Map.values()
    |> Enum.filter(&active_primary_dead?(&1, live))
    |> Enum.flat_map(&promote(&1, live))
    |> Enum.sort()
  end

  defp active_primary_dead?(%{state: :active, replica_set: [primary | _]}, live) do
    not MapSet.member?(live, primary)
  end

  defp active_primary_dead?(_segment, _live), do: false

  defp promote(segment, live) do
    case Enum.filter(segment.replica_set, &MapSet.member?(live, &1)) do
      [] ->
        []

      [new_primary | _] ->
        new_set = [new_primary | List.delete(segment.replica_set, new_primary)]
        [{:set_segment_replicas, segment.id, new_set}]
    end
  end
end
