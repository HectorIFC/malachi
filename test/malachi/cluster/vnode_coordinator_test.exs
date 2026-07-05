defmodule Malachi.Cluster.VnodeCoordinatorTest do
  # async: false — ra is global/stateful (one data dir, on-disk Raft logs).
  use ExUnit.Case, async: false

  alias Malachi.Application, as: App
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.RetentionCoordinator
  alias Malachi.Metadata

  @range {"t", 0}

  setup_all do
    data_dir = Path.join(System.tmp_dir!(), "malachi_ra_#{System.unique_integer([:positive])}")
    File.rm_rf!(data_dir)
    _ = :ra.start_in(String.to_charlist(data_dir))
    on_exit(fn -> File.rm_rf!(data_dir) end)
    :ok
  end

  defp start_vnode do
    name = :"vnode_#{System.unique_integer([:positive])}"
    {:ok, server_id} = MetadataServer.start(name)
    on_exit(fn -> MetadataServer.delete(name) end)
    {name, server_id}
  end

  # Populates the vnode with topic "t" and a set of sealed segments {id, start_offset, bytes, sealed_at}.
  defp seal_segments(server_id, segments) do
    {:ok, {:ok, _root}} = MetadataServer.command(server_id, {:create_topic, "t", 4})

    Enum.each(segments, fn {id, start_offset, bytes, sealed_at} ->
      {:ok, :ok} = MetadataServer.command(server_id, {:register_segment, @range, id, [:b1], start_offset})
      {:ok, :ok} = MetadataServer.command(server_id, {:seal_segment, id, 1, bytes, sealed_at})
    end)
  end

  describe "vnode_metadata_source/1" do
    test "yields empty Metadata for an unformed/unreachable vnode (tolerant, no crash)" do
      source = App.vnode_metadata_source(:"no_such_vnode_#{System.unique_integer([:positive])}")
      assert source.() == Metadata.new()
    end

    test "reads this vnode's own shard (only its segments)" do
      {name, server_id} = start_vnode()
      seal_segments(server_id, [{"old", 0, 100, 1_000}])

      metadata = App.vnode_metadata_source(name).()
      assert Metadata.get_segment(metadata, "old").state == :sealed
    end
  end

  test "a retention coordinator bound to a vnode expires only that vnode's expired segments (1C-b-ii-a)" do
    test_pid = self()
    {name, server_id} = start_vnode()
    seal_segments(server_id, [{"old", 0, 100, 1_000}, {"new", 1, 100, 9_500}])

    {:ok, coordinator} =
      RetentionCoordinator.start_link(
        metadata_source: App.vnode_metadata_source(name),
        expire_segment: fn segment -> send(test_pid, {:expired, segment.id}) end,
        policy: %{max_age_ms: 5_000},
        clock: fn -> 10_000 end,
        interval: 60_000
      )

    assert RetentionCoordinator.run_now(coordinator) == ["old"]
    assert_receive {:expired, "old"}
    refute_receive {:expired, "new"}
  end
end
