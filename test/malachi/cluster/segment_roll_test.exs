defmodule Malachi.Cluster.SegmentRollTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Cluster.SegmentRoll
  alias Malachi.Metadata

  @hour 3_600_000

  # Topic "t" with one range, holding `sealed` sealed segments and then one active segment opened at
  # `opened_at` (`nil` for an apply with no clock), or no active segment when `opened_at` is `:none`.
  defp topic(opened_at, opts \\ []) do
    name = Keyword.get(opts, :name, "t")
    {metadata, {:ok, range}} = Metadata.apply(Keyword.get(opts, :metadata, Metadata.new()), {:create_topic, name, 4})

    metadata =
      Enum.reduce(0..(Keyword.get(opts, :sealed, 0) - 1)//1, metadata, fn i, acc ->
        {acc, :ok} = Metadata.apply(acc, {:register_segment, range, {range, i}, [:b1], i}, 0)
        {acc, :ok} = Metadata.apply(acc, {:seal_segment, {range, i}, 1, 10, 0})
        acc
      end)

    case opened_at do
      :none ->
        metadata

      at ->
        seq = Keyword.get(opts, :sealed, 0)
        {metadata, :ok} = Metadata.apply(metadata, {:register_segment, range, {range, seq}, [:b1], seq}, at)
        metadata
    end
  end

  defp due_ids(metadata, now, global, policies \\ %{}),
    do: metadata |> SegmentRoll.due(now, global, policies) |> Enum.map(& &1.id) |> Enum.sort()

  test "an active segment older than the limit is due, and one younger is not" do
    metadata = topic(1_000)

    assert due_ids(metadata, 1_000 + @hour, %{segment_max_age_ms: @hour}) == [{{"t", 0}, 0}]
    assert due_ids(metadata, 1_000 + @hour - 1, %{segment_max_age_ms: @hour}) == []
  end

  test "a sealed segment is never due, however old" do
    assert due_ids(topic(:none, sealed: 3), 10 * @hour, %{segment_max_age_ms: @hour}) == []
  end

  test "only the active segment of a range with history is due" do
    assert due_ids(topic(0, sealed: 2), @hour, %{segment_max_age_ms: @hour}) == [{{"t", 0}, 2}]
  end

  test "a segment with no known opening time is due at once" do
    assert due_ids(topic(nil), 0, %{segment_max_age_ms: @hour}) == [{{"t", 0}, 0}]
  end

  test "a segment written by older code, with no opened_at key at all, is left alone rather than raising" do
    metadata = topic(0)
    legacy = %{metadata | segments: Map.new(metadata.segments, fn {id, seg} -> {id, Map.delete(seg, :opened_at)} end)}

    assert due_ids(legacy, 100 * @hour, %{segment_max_age_ms: @hour}) == []
  end

  test "no limit anywhere rolls nothing" do
    assert due_ids(topic(0), 100 * @hour, %{}) == []
    assert due_ids(topic(0), 100 * @hour, %{segment_max_age_ms: nil}) == []
  end

  test "a topic's policy overrides the global limit, and nil in the policy turns the roll off" do
    bind = fn metadata, name -> elem(Metadata.apply(metadata, {:bind_topic_policy, "t", name}), 0) end

    policies = %{
      "fast" => %{retention: %{segment_max_age_ms: 60_000}},
      "off" => %{retention: %{segment_max_age_ms: nil}}
    }

    global = %{segment_max_age_ms: @hour}

    assert due_ids(bind.(topic(0), "fast"), 60_000, global, policies) == [{{"t", 0}, 0}]
    assert due_ids(bind.(topic(0), "off"), 100 * @hour, global, policies) == []
  end

  test "a topic bound to a policy nothing defines rolls nothing, as it expires nothing" do
    metadata = elem(Metadata.apply(topic(0), {:bind_topic_policy, "t", "ghost"}), 0)
    assert due_ids(metadata, 100 * @hour, %{segment_max_age_ms: @hour}) == []
  end

  test "each topic is judged against its own limit" do
    metadata = topic(0, name: "quiet", metadata: topic(0, name: "busy"))
    metadata = elem(Metadata.apply(metadata, {:bind_topic_policy, "busy", "short"}), 0)
    policies = %{"short" => %{retention: %{segment_max_age_ms: 60_000}}}

    assert due_ids(metadata, 60_000, %{segment_max_age_ms: @hour}, policies) == [{{"busy", 0}, 0}]
  end

  test "the returned segment is the control plane's metadata, which the broker builds the roll from" do
    assert [%{state: :active, replica_set: [:b1], start_offset: 0, opened_at: 0}] =
             SegmentRoll.due(topic(0), @hour, %{segment_max_age_ms: @hour}, %{})
  end

  # --- properties ---

  # A random set of topics, each a range with some sealed history and maybe an active segment, the
  # topics bound to random policies, and a random global limit.
  defp world do
    gen all(
          specs <-
            list_of(
              {integer(0..3), one_of([constant(:none), constant(nil), integer(0..(10 * @hour))]),
               member_of([nil, "fast", "off", "ghost"])},
              max_length: 6
            ),
          global <- one_of([constant(nil), integer(60_000..(5 * @hour))]),
          now <- integer(0..(20 * @hour))
        ) do
      metadata =
        specs
        |> Enum.with_index()
        |> Enum.reduce(Metadata.new(), fn {{sealed, opened_at, policy}, i}, acc ->
          acc = topic(opened_at, name: "t#{i}", sealed: sealed, metadata: acc)
          if policy, do: elem(Metadata.apply(acc, {:bind_topic_policy, "t#{i}", policy}), 0), else: acc
        end)

      policies = %{
        "fast" => %{retention: %{segment_max_age_ms: 60_000}},
        "off" => %{retention: %{segment_max_age_ms: nil}}
      }

      {metadata, %{segment_max_age_ms: global}, policies, now}
    end
  end

  property "never a sealed segment, and at most one per range" do
    check all({metadata, global, policies, now} <- world()) do
      due = SegmentRoll.due(metadata, now, global, policies)
      assert Enum.all?(due, &(&1.state == :active))
      assert length(due) == length(Enum.uniq_by(due, & &1.range_id))
    end
  end

  property "a segment is due if and only if it is active, has a limit, and is unknown or old enough" do
    check all({metadata, global, policies, now} <- world()) do
      due = metadata |> SegmentRoll.due(now, global, policies) |> MapSet.new(& &1.id)

      for {id, segment} <- metadata.segments do
        limit =
          case Metadata.topic_policy_name(metadata, elem(segment.range_id, 0)) do
            nil -> global.segment_max_age_ms
            "fast" -> 60_000
            _off_or_ghost -> nil
          end

        expected =
          segment.state == :active and limit != nil and
            (segment.opened_at == nil or now - segment.opened_at >= limit)

        assert MapSet.member?(due, id) == expected
      end
    end
  end

  property "once due, a segment stays due as time passes" do
    check all({metadata, global, policies, now} <- world(), later <- integer(0..(10 * @hour))) do
      due_now = metadata |> SegmentRoll.due(now, global, policies) |> MapSet.new(& &1.id)
      due_later = metadata |> SegmentRoll.due(now + later, global, policies) |> MapSet.new(& &1.id)
      assert MapSet.subset?(due_now, due_later)
    end
  end

  property "the decision is deterministic" do
    check all({metadata, global, policies, now} <- world()) do
      assert SegmentRoll.due(metadata, now, global, policies) == SegmentRoll.due(metadata, now, global, policies)
    end
  end
end
