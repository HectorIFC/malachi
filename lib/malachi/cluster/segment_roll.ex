defmodule Malachi.Cluster.SegmentRoll do
  @moduledoc """
  Pure decision for the **age roll**: which active segments are old enough that the retention sweep
  should ask for them to be sealed (the `segment.ms` of other logs).

  Age retention (`Malachi.Cluster.Retention`) only ever sees sealed segments, and a segment seals by
  size on its own. A topic too quiet to fill `:segment_max_bytes` would never seal one, so it would
  never expire anything, however old its records got. Rolling the active segment once it is older than
  `:segment_max_age_ms` closes that gap: a record lives at most the roll interval plus the age bound, plus up to two sweep
  intervals (the sweep that asks for the roll and the one that expires the segment) and the fence's answer.

  The limit for each range is its topic's effective `:segment_max_age_ms`, resolved exactly as the
  retention bounds are (`Malachi.Cluster.Retention.effective/4`): the topic's policy over the global
  value, `nil` (the policy turned it off) rolls nothing, and a topic bound to a policy name nothing
  defines rolls nothing either, as it expires nothing.

  A segment is due when `now_ms - opened_at >= limit`. One with no `opened_at` (an apply that had no
  clock, or a topic export from before segments carried one, see `Malachi.Metadata.apply/3`) has no
  known age and is due at once: rolling it costs one seal, and the segment that follows it carries a
  real opening time. A segment map with no `opened_at` key at all comes from a Raft leader still on older
  code, during a rolling upgrade, and is left alone until the upgraded leader's replay stamps it.

  Emptiness is not judged here. Only the fence knows where a segment ends, and a frontend's own offset
  counter is not the control plane's for a range other nodes produce to. An active segment exists only
  once a produce registered it, so an empty one is rare, and the zero-length seal it gets is one the
  read path already serves (`Malachi.Broker`); no successor is opened until the next produce, so it
  happens once.
  """

  alias Malachi.Cluster.Policy
  alias Malachi.Cluster.Retention
  alias Malachi.Metadata

  @doc """
  The active segments that are due for an age roll at `now_ms` (epoch ms), at most one per range (a
  range has one write head). `global_policy`, `policies` and `unresolved_max_age_ms` are what
  `Malachi.Cluster.Retention.expired/5` takes, resolved once per sweep by the caller.
  """
  @spec due(
          Metadata.t(),
          non_neg_integer(),
          Retention.policy(),
          %{Metadata.policy_name() => Policy.t()},
          non_neg_integer() | nil
        ) :: [Metadata.segment_meta()]
  def due(%Metadata{} = metadata, now_ms, global_policy, policies, unresolved_max_age_ms \\ nil) do
    for {topic, _meta} <- metadata.topics,
        limit = limit(metadata, topic, global_policy, policies, unresolved_max_age_ms),
        limit != nil,
        range <- Metadata.ranges_of_topic(metadata, topic),
        segment <- Metadata.segments_of_range(metadata, range.id),
        segment.state == :active,
        due?(segment, now_ms, limit),
        do: segment
  end

  defp limit(metadata, topic, global_policy, policies, unresolved_max_age_ms) do
    %{retention: %{segment_max_age_ms: {limit, _origin}}} =
      metadata
      |> Metadata.topic_policy_name(topic)
      |> Retention.effective(policies, global_policy, unresolved_max_age_ms)

    limit
  end

  # A segment map with no `:opened_at` key at all was built by older code: a cache seeded from a Raft
  # leader that has not been upgraded yet answers its own state. Unlike a `nil`, it says nothing about the
  # segment, and treating it as due would roll every such head on every sweep for as long as that leader
  # stays, opening a fresh segment the same old code registers without the key again. It waits instead:
  # once the leader runs this code its replay stamps the real opening time.
  defp due?(segment, now_ms, limit) do
    case Map.fetch(segment, :opened_at) do
      {:ok, nil} -> true
      {:ok, opened_at} -> now_ms - opened_at >= limit
      :error -> false
    end
  end
end
