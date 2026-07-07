defmodule Malachi.Cluster.Placement do
  @moduledoc """
  Pure replica **placement and self-healing** policy for segments — the decision layer of the
  data plane. The segment is NorthGuard's unit of replication: each lives on a *replica set* of
  `replication_factor` brokers. This module decides *which* brokers, and which segments have
  lost replicas and must be re-replicated. It is a pure function of `(metadata, available
  brokers, replication factor)` — it never touches storage or the network.

  Placement uses **rendezvous (HRW) hashing**: for a segment, every available broker is scored
  by `:erlang.phash2({segment_id, broker})`, and the top `replication_factor` brokers win. Two
  properties make this the right fit for a Raft-replicated control plane:

    * **Deterministic** — the same inputs yield the same replica set on every replica, so a
      placement decision can be derived inside (or fed through) the `Malachi.Metadata` machine
      without diverging across nodes.
    * **Minimum reshuffle** — removing a broker only moves the segments that broker hosted; the
      surviving replicas keep their ranks, so `heal/3` preserves live members and only fills the
      vacated slots.

  The policy *decides*; `Malachi.Metadata` *records*. `heal/3` returns a list of
  `{:set_segment_replicas, ...}` commands to apply through the RSM (and, later, Raft) — it does
  not mutate anything itself.

  Self-healing covers **all** segments, sealed as well as active: a sealed segment is immutable
  but its data must still survive `replication_factor` failures, so a lost replica is re-placed.

  `available_brokers` is an abstract broker set supplied by the caller. Membership (which
  brokers are actually alive) is a separate concern (SWIM, later); taking it as a parameter is
  what keeps this layer pure and pins down the contract that membership will have to satisfy.
  """

  alias Malachi.Metadata

  @typedoc "The target replica count, clamped to the number of available brokers."
  @type target :: non_neg_integer()

  @doc """
  Chooses the replica set for `segment_id` from `available_brokers` via rendezvous hashing,
  returning the `min(replication_factor, length(available_brokers))` highest-scoring brokers
  (all distinct). Duplicate brokers in the input are ignored.

  ## Options
    * `:spread` - `{attribute_key, attributes}` to spread replicas across the distinct values of an
      attribute (e.g. `{"rack", %{broker => %{"rack" => "a"}}}`): the best-ranked broker of each
      value is taken first, then the next of each, until `replication_factor`. With `rf <= number of
      values` every replica lands in a different value; otherwise it is best-effort round-robin. This
      is deterministic (rendezvous ranking + stable grouping) and is what makes placement rack/DC
      aware. Omitted → the plain top-`rf` ranking.

  Returns `{:error, :no_brokers}` if there are none, or `{:error, :invalid_replication_factor}`
  if `replication_factor < 1`.
  """
  @spec place(Metadata.segment_id(), [Metadata.broker()], pos_integer(), keyword()) ::
          {:ok, [Metadata.broker()]} | {:error, :no_brokers | :invalid_replication_factor}
  def place(segment_id, available_brokers, replication_factor, opts \\ [])

  def place(_segment_id, _available_brokers, replication_factor, _opts) when replication_factor < 1 do
    {:error, :invalid_replication_factor}
  end

  def place(segment_id, available_brokers, replication_factor, opts) do
    case Enum.uniq(available_brokers) do
      [] -> {:error, :no_brokers}
      brokers -> {:ok, ranked(segment_id, brokers) |> select(replication_factor, opts)}
    end
  end

  @doc """
  Places a **whole set** of `items` across `available_brokers` with **best-effort load balancing** (A2 —
  global balancing). Each item still ranks brokers by rendezvous hashing, but a broker at capacity is
  skipped in favour of the next-ranked one, so load spreads instead of piling on the highest-ranked few.
  The soft capacity is `ceil(total_replicas / brokers) + max_skew - 1`: with `max_skew` 1 it targets the
  even share (rounded up); a larger `max_skew` leaves slack, staying closer to plain HRW (which reshuffles
  fewer replicas when membership changes). Returns `[{item, replica_set}]` in the input order; `[]`
  brokers → each item gets `[]`.

  The cap is a **preference, not a hard bound**: replication factor is never sacrificed for balance, so
  when an item cannot otherwise reach `min(rf, brokers)` distinct replicas it takes the least-loaded
  broker even if that exceeds the cap (this can happen with `rf > 1`, where a per-item greedy cannot
  always pack perfectly). In practice HRW is uniform, so the cap holds and load lands even; the hard
  guarantees are that every item gets `min(rf, brokers)` **distinct** replicas and the result is
  **deterministic** (ranking, input order, and running counts are identical on every node). This is a
  **standalone** balancer: it does not combine with `:spread` (rack-aware placement); use one or the
  other for now.
  """
  @spec place_balanced([term()], [Metadata.broker()], pos_integer(), pos_integer()) ::
          [{term(), [Metadata.broker()]}]
  def place_balanced(items, available_brokers, replication_factor, max_skew \\ 1) do
    case Enum.uniq(available_brokers) do
      [] ->
        Enum.map(items, &{&1, []})

      brokers ->
        target = min(replication_factor, length(brokers))
        cap = capacity(length(items) * target, length(brokers), max_skew)

        {placed, _counts} =
          Enum.map_reduce(items, Map.new(brokers, &{&1, 0}), fn item, counts ->
            chosen = pick_balanced(ranked(item, brokers), target, cap, counts)
            {{item, chosen}, bump(counts, chosen)}
          end)

        placed
    end
  end

  @doc """
  The ids of segments that are under-replicated for the given live broker set: those with fewer
  live replicas than the achievable target `min(replication_factor, length(available_brokers))`.

  A replica counts as live only if it is in `available_brokers`, so a segment whose broker has
  left is flagged. The target is clamped to what is achievable, so a cluster that is simply too
  small to reach `replication_factor` is not reported as perpetually under-replicated. Returns a
  sorted list (deterministic).
  """
  @spec under_replicated(Metadata.t(), [Metadata.broker()], pos_integer()) ::
          [Metadata.segment_id()]
  def under_replicated(%Metadata{} = metadata, available_brokers, replication_factor) do
    live = MapSet.new(available_brokers)
    target = min(replication_factor, MapSet.size(live))

    metadata.segments
    |> Enum.filter(fn {_id, segment} -> live_count(segment, live) < target end)
    |> Enum.map(fn {id, _segment} -> id end)
    |> Enum.sort()
  end

  @doc """
  A self-healing plan: a list of `{:set_segment_replicas, segment_id, replica_set}` commands
  that restore every under-replicated segment to a full replica set, to be applied through
  `Malachi.Metadata`. Re-placing over the live broker set preserves the surviving replicas (HRW
  retention) and only fills the vacated slots, so one pass reaches a fully-replicated fixpoint.
  Returns `[]` when nothing needs healing.
  """
  @spec heal(Metadata.t(), [Metadata.broker()], pos_integer()) :: [Metadata.command()]
  def heal(%Metadata{} = metadata, available_brokers, replication_factor) do
    metadata
    |> under_replicated(available_brokers, replication_factor)
    |> Enum.map(fn segment_id ->
      # under_replicated only yields ids when target > 0, which requires a non-empty broker
      # set, so place/3 cannot fail here.
      {:ok, replica_set} = place(segment_id, available_brokers, replication_factor)
      {:set_segment_replicas, segment_id, replica_set}
    end)
  end

  # --- internals ---

  # Brokers ordered by descending rendezvous score, tie-broken by the broker term for a stable,
  # input-order-independent ranking.
  defp ranked(segment_id, brokers) do
    Enum.sort_by(brokers, fn broker -> {:erlang.phash2({segment_id, broker}), broker} end, :desc)
  end

  # Max replicas a broker may hold so counts differ by at most `max_skew`: ceil(total / brokers) rounds
  # the even share up, and max_skew - 1 adds the allowed slack. ceil via integer division (no floats).
  defp capacity(total_replicas, num_brokers, max_skew) do
    div(total_replicas + num_brokers - 1, num_brokers) + max_skew - 1
  end

  # Picks `target` distinct brokers from the rank-ordered list, preferring ones still under `cap`; if too
  # few remain under cap for this item (a corner case), fills from the least-loaded of the rest so the
  # item still gets `target` replicas.
  defp pick_balanced(ranked, target, cap, counts) do
    {under, at_cap} = Enum.split_with(ranked, fn broker -> Map.fetch!(counts, broker) < cap end)
    chosen = Enum.take(under, target)

    if length(chosen) == target do
      chosen
    else
      fill = at_cap |> Enum.sort_by(&Map.fetch!(counts, &1)) |> Enum.take(target - length(chosen))
      chosen ++ fill
    end
  end

  defp bump(counts, chosen), do: Enum.reduce(chosen, counts, &Map.update!(&2, &1, fn n -> n + 1 end))

  # Picks the replica set from the rank-ordered brokers: plain top-`rf`, or spread across an
  # attribute's values when `:spread` is set.
  defp select(ranked, replication_factor, opts) do
    case Keyword.get(opts, :spread) do
      nil -> Enum.take(ranked, replication_factor)
      {attribute_key, attributes} -> spread(ranked, replication_factor, attribute_key, attributes)
    end
  end

  # Round-robin across the distinct values of `attribute_key`, taking the best-ranked broker of each
  # value first, then the next of each. Groups keep rank order internally and are visited by their
  # best rank, so the result is deterministic and spreads replicas across values (rack/DC aware).
  defp spread(ranked, replication_factor, attribute_key, attributes) do
    ranked
    |> Enum.group_by(fn broker -> attributes |> Map.get(broker, %{}) |> Map.get(attribute_key) end)
    |> Enum.sort_by(fn {_value, [best | _]} -> Enum.find_index(ranked, &(&1 == best)) end)
    |> Enum.map(fn {_value, brokers} -> brokers end)
    |> round_robin()
    |> Enum.take(replication_factor)
  end

  defp round_robin(groups) do
    case Enum.reject(groups, &(&1 == [])) do
      [] -> []
      non_empty -> Enum.map(non_empty, &hd/1) ++ round_robin(Enum.map(non_empty, &tl/1))
    end
  end

  # Count *distinct* live replicas: a replica set that happens to list a broker twice still
  # provides only one copy, so it must not be mistaken for two toward the target.
  defp live_count(segment, live) do
    segment.replica_set |> Enum.uniq() |> Enum.count(&MapSet.member?(live, &1))
  end
end
