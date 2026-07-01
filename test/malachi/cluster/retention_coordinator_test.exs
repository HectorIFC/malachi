defmodule Malachi.Cluster.RetentionCoordinatorTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.RetentionCoordinator
  alias Malachi.Metadata

  @range {"t", 0}

  defp with_sealed(segments) do
    base = elem(Metadata.apply(Metadata.new(), {:create_topic, "t", 4}), 0)

    Enum.reduce(segments, base, fn {id, start_offset, bytes, sealed_at}, metadata ->
      {metadata, :ok} = Metadata.apply(metadata, {:register_segment, @range, id, [:b1], start_offset})
      {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, id, 1, bytes, sealed_at})
      metadata
    end)
  end

  defp start(opts) do
    test_pid = self()

    defaults = [
      metadata_source: fn -> with_sealed([{"old", 0, 100, 1_000}, {"new", 1, 100, 9_500}]) end,
      expire_segment: fn segment -> send(test_pid, {:expired, segment.id}) end,
      policy: %{max_age_ms: 5_000},
      clock: fn -> 10_000 end,
      interval: 60_000
    ]

    {:ok, server} = RetentionCoordinator.start_link(Keyword.merge(defaults, opts))
    server
  end

  test "run_now expires segments per policy and calls expire_segment on each" do
    server = start([])

    assert RetentionCoordinator.run_now(server) == ["old"]
    assert_receive {:expired, "old"}
    refute_receive {:expired, "new"}
  end

  test "expire_segment receives the segment's full metadata (for its replica set)" do
    test_pid = self()
    server = start(expire_segment: fn segment -> send(test_pid, {:expired, segment}) end)

    RetentionCoordinator.run_now(server)
    assert_receive {:expired, %{id: "old", replica_set: [:b1], state: :sealed}}
  end

  test "a sweep runs on the tick" do
    test_pid = self()
    _server = start(expire_segment: fn segment -> send(test_pid, {:expired, segment.id}) end, interval: 20)

    # no synchronous run_now — the scheduled tick drives it
    assert_receive {:expired, "old"}, 1_000
  end
end
