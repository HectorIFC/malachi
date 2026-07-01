defmodule Malachi.Cluster.Retention do
  @moduledoc """
  Pure retention policy for **sealed** segments — the decision layer, no storage or timers.

  Given the control-plane `Malachi.Metadata`, the current time, and a policy, it returns the sealed
  segment ids to expire. Two independent rules, unioned:

    * **age** — a sealed segment older than `:max_age_ms` (by its `sealed_at`) expires;
    * **size** — per range, if the sealed segments total more than `:max_bytes`, the oldest expire
      until the range is back under the limit.

  Size is scoped **per range** (the natural unit; a range's segments have contiguous offsets), age
  is per segment. The **active** segment is never returned — it is still being written. A `nil`
  bound disables that rule. `Malachi.Cluster.RetentionCoordinator` executes the returned ids.
  """

  alias Malachi.Metadata

  @typedoc "A retention policy. A `nil` (or absent) bound disables that rule."
  @type policy :: %{
          optional(:max_age_ms) => non_neg_integer() | nil,
          optional(:max_bytes) => non_neg_integer() | nil
        }

  @doc "The sealed segment ids to expire under `policy` at `now_ms` (epoch ms)."
  @spec expired(Metadata.t(), non_neg_integer(), policy()) :: [Metadata.segment_id()]
  def expired(%Metadata{} = metadata, now_ms, policy) do
    max_age = Map.get(policy, :max_age_ms)
    max_bytes = Map.get(policy, :max_bytes)

    metadata.segments
    |> Map.values()
    |> Enum.filter(&(&1.state == :sealed))
    |> Enum.group_by(& &1.range_id)
    |> Enum.flat_map(fn {_range_id, sealed} ->
      oldest_first = Enum.sort_by(sealed, & &1.start_offset)
      expired_by_age(oldest_first, now_ms, max_age) ++ expired_by_size(oldest_first, max_bytes)
    end)
    |> Enum.uniq()
  end

  defp expired_by_age(_segments, _now_ms, nil), do: []

  defp expired_by_age(segments, now_ms, max_age_ms) do
    for segment <- segments, is_integer(segment.sealed_at), now_ms - segment.sealed_at > max_age_ms do
      segment.id
    end
  end

  defp expired_by_size(_segments, nil), do: []

  defp expired_by_size(segments, max_bytes) do
    total = Enum.reduce(segments, 0, fn segment, acc -> acc + bytes(segment) end)

    # Drop the oldest segments (segments is oldest-first) until the range is back under max_bytes.
    {expired, _running} =
      Enum.reduce(segments, {[], total}, fn segment, {expired, running} ->
        if running > max_bytes do
          {[segment.id | expired], running - bytes(segment)}
        else
          {expired, running}
        end
      end)

    expired
  end

  defp bytes(segment), do: segment.byte_size || 0
end
