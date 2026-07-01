defmodule Malachi.Cluster.RetentionTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.Retention
  alias Malachi.Metadata

  @range {"t", 0}

  # Builds metadata with topic "t" and the given sealed segments.
  # Each: {segment_id, start_offset, byte_size, sealed_at}.
  defp with_sealed(segments) do
    base = elem(Metadata.apply(Metadata.new(), {:create_topic, "t", 4}), 0)

    Enum.reduce(segments, base, fn {id, start_offset, bytes, sealed_at}, metadata ->
      {metadata, :ok} = Metadata.apply(metadata, {:register_segment, @range, id, [:b1], start_offset})
      {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, id, 1, bytes, sealed_at})
      metadata
    end)
  end

  defp expired(metadata, now, policy), do: metadata |> Retention.expired(now, policy) |> Enum.sort()

  test "expires sealed segments older than max_age_ms" do
    metadata = with_sealed([{"old", 0, 100, 1_000}, {"new", 1, 100, 9_500}])
    # at now=10_000, old's age is 9000 (> 5000), new's is 500
    assert expired(metadata, 10_000, %{max_age_ms: 5_000}) == ["old"]
  end

  test "a nil bound disables that rule" do
    metadata = with_sealed([{"old", 0, 100, 1_000}])
    assert expired(metadata, 10_000, %{max_age_ms: nil, max_bytes: nil}) == []
    assert expired(metadata, 10_000, %{}) == []
  end

  test "expires the oldest sealed segments once a range exceeds max_bytes" do
    metadata = with_sealed([{"s0", 0, 100, 0}, {"s1", 1, 100, 0}, {"s2", 2, 100, 0}])
    # total 300, keep <= 150: drop the two oldest (s0, s1), keep the newest (s2)
    assert expired(metadata, 999, %{max_bytes: 150}) == ["s0", "s1"]
  end

  test "size retention is per range, not across ranges" do
    # split the root so segments land on two different ranges
    base = elem(Metadata.apply(Metadata.new(), {:create_topic, "t", 4}), 0)
    {base, {:ok, left, right}} = Metadata.apply(base, {:split_range, @range})

    seal = fn metadata, range, id ->
      {metadata, :ok} = Metadata.apply(metadata, {:register_segment, range, id, [:b1], 0})
      elem(Metadata.apply(metadata, {:seal_segment, id, 1, 100, 0}), 0)
    end

    metadata = base |> seal.(left, "l") |> seal.(right, "r")
    # each range holds 100 bytes; a 150-byte limit is per range, so neither is over
    assert expired(metadata, 999, %{max_bytes: 150}) == []
  end

  test "never expires the active segment, even under an aggressive policy" do
    base = elem(Metadata.apply(Metadata.new(), {:create_topic, "t", 4}), 0)
    {metadata, :ok} = Metadata.apply(base, {:register_segment, @range, "active", [:b1], 0})

    assert Retention.expired(metadata, 10_000, %{max_age_ms: 0, max_bytes: 0}) == []
  end

  test "unions the age and size rules" do
    # s0 expired by age (old); s2 also dropped by size (total 300 > 150)
    metadata = with_sealed([{"s0", 0, 100, 1_000}, {"s1", 1, 100, 9_900}, {"s2", 2, 100, 9_900}])
    result = expired(metadata, 10_000, %{max_age_ms: 5_000, max_bytes: 150})
    # by age: s0. by size (oldest-first until <= 150): s0, s1. union: s0, s1
    assert result == ["s0", "s1"]
  end
end
