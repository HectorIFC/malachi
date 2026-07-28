defmodule Malachi.Cluster.Retention do
  @moduledoc """
  Pure retention policy for **sealed** segments: the decision layer, no storage or timers.

  Given the control-plane `Malachi.Metadata`, the current time, and a policy, it returns the sealed
  segment ids to expire. Two independent rules, unioned:

    * **age**. A sealed segment older than `:max_age_ms` (by its `sealed_at`) expires;
    * **size**. Per range, if the sealed segments total more than `:max_bytes`, the oldest expire
      until the range is back under the limit.

  Size is scoped **per range** (the natural unit; a range's segments have contiguous offsets), age
  is per segment. The **active** segment is never returned: it is still being written. A `nil`
  bound disables that rule. `Malachi.Cluster.RetentionCoordinator` executes the returned ids.

  The bound applied to each range is its **topic's policy retention merged over the global policy**
  (the policy overrides only the keys it sets; `Malachi.Metadata.topic_policy/2`), or the global
  policy passed in when the topic has no policy, so retention is per-topic (C3c-2b).
  """

  alias Malachi.Metadata

  @typedoc "A retention policy. A `nil` (or absent) bound disables that rule."
  @type policy :: %{
          optional(:max_age_ms) => non_neg_integer() | nil,
          optional(:max_bytes) => non_neg_integer() | nil
        }

  @doc """
  The sealed segment ids to expire at `now_ms` (epoch ms). `global_policy` is the fallback; each
  range uses its topic's policy retention merged over it (see the module doc).
  """
  @spec expired(Metadata.t(), non_neg_integer(), policy()) :: [Metadata.segment_id()]
  def expired(%Metadata{} = metadata, now_ms, global_policy) do
    metadata.segments
    |> Map.values()
    |> Enum.filter(&(&1.state == :sealed))
    |> Enum.group_by(& &1.range_id)
    |> Enum.flat_map(fn {range_id, sealed} ->
      retention = effective_retention(metadata, range_id, global_policy)
      oldest_first = Enum.sort_by(sealed, & &1.start_offset)

      expired_by_age(oldest_first, now_ms, Map.get(retention, :max_age_ms)) ++
        expired_by_size(oldest_first, Map.get(retention, :max_bytes))
    end)
    |> Enum.uniq()
  end

  # A range's effective retention: its topic's policy retention merged over the global policy (the
  # policy overrides only the keys it sets), or the global policy when the topic has no policy.
  defp effective_retention(metadata, range_id, global_policy) do
    case Metadata.topic_policy(metadata, elem(range_id, 0)) do
      %{retention: retention} when is_map(retention) -> Map.merge(global_policy, retention)
      _no_policy_retention -> global_policy
    end
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
