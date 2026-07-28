defmodule Malachi.Consumer.Assignment do
  @moduledoc """
  Assigns a topic's ranges across the live members of a consumer group, so each range is consumed by
  **exactly one** member and the work spreads in parallel: the consumer-group counterpart of Kafka's
  partition assignment, over NorthGuard's dynamic ranges.

  Reuses `Malachi.Cluster.Placement.place/4` (rendezvous/HRW hashing): each range goes to its
  top-ranked member (`replication_factor: 1`). HRW is **min-reshuffle**, which is exactly the property a
  consumer group wants. **strong stickiness**: a member leaving moves **only its own** ranges (every
  surviving member keeps all of its ranges), and a member joining moves ranges **only to it** (existing
  members only ever lose ranges, never trade them). That matters because each reassignment costs the new
  owner a re-read from the group's committed position. Balance is **statistical** (HRW spreads evenly in
  expectation) rather than exactly even: the right trade for NorthGuard, whose hot topics accumulate many
  ranges as they split, so the law of large numbers keeps consumers well balanced while churn stays
  minimal. The result is **deterministic** (the same ranges and members yield the same assignment on every
  node), so a replicated or failed-over coordinator computes an identical one.
  """

  alias Malachi.Cluster.Placement

  @doc """
  Assigns `range_ids` across `members`, returning `%{member => [range_id]}` with each range under exactly
  one member (ranges sorted into a canonical order per member). Every member appears in the map (with `[]`
  if it got none), so a caller can tell an idle member from an unknown one. `[]` members => `%{}`; `[]`
  ranges => every member maps to `[]`. Duplicate members or ranges are ignored.
  """
  @spec assign([term()], [term()]) :: %{term() => [term()]}
  def assign(_range_ids, []), do: %{}

  def assign(range_ids, members) do
    empty = Map.new(members, &{&1, []})

    range_ids
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.reduce(empty, fn range_id, acc ->
      {:ok, [member]} = Placement.place(range_id, members, 1)
      Map.update!(acc, member, &[range_id | &1])
    end)
    |> Map.new(fn {member, ranges} -> {member, Enum.reverse(ranges)} end)
  end
end
