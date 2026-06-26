defmodule Malachi.Cluster.ReplicatedMetadataTest do
  # async: false — ra is global/stateful (one data dir, on-disk Raft logs).
  use ExUnit.Case, async: false

  alias Malachi.Cluster.ReplicatedMetadata
  alias Malachi.Metadata

  setup_all do
    data_dir = Path.join(System.tmp_dir!(), "malachi_ra_rm_#{System.unique_integer([:positive])}")
    File.rm_rf!(data_dir)
    _ = :ra.start_in(String.to_charlist(data_dir))
    on_exit(fn -> File.rm_rf!(data_dir) end)
    :ok
  end

  defp start do
    {:ok, replicated} = ReplicatedMetadata.start(:"rm_#{System.unique_integer([:positive])}")
    on_exit(fn -> ReplicatedMetadata.delete(replicated) end)
    replicated
  end

  test "a committed command updates the local cache (read-your-writes)" do
    replicated = start()

    {reply, replicated} = ReplicatedMetadata.command(replicated, {:create_topic, "events", 4})
    assert {:ok, root_id} = reply

    # the cache reflects the committed command without any further query
    topic = ReplicatedMetadata.metadata(replicated) |> Metadata.get_topic("events")
    assert topic.name == "events"

    {{:ok, left, right}, replicated} = ReplicatedMetadata.command(replicated, {:split_range, root_id})
    active = ReplicatedMetadata.metadata(replicated) |> Metadata.active_ranges_of_topic("events")
    assert Enum.sort(Enum.map(active, & &1.id)) == Enum.sort([left, right])
  end

  test "a rejected command leaves the cache unchanged and surfaces the machine error" do
    replicated = start()
    {{:ok, _root}, replicated} = ReplicatedMetadata.command(replicated, {:create_topic, "events", 4})

    {reply, replicated} = ReplicatedMetadata.command(replicated, {:create_topic, "events", 4})
    assert reply == {:error, :already_exists}

    # still exactly one topic in the cache
    assert ReplicatedMetadata.metadata(replicated).topics |> map_size() == 1
  end

  test "the cache equals the replicated state (refresh is a no-op for the sole writer)" do
    replicated = start()
    {{:ok, root_id}, replicated} = ReplicatedMetadata.command(replicated, {:create_topic, "events", 4})
    {{:ok, _l, _r}, replicated} = ReplicatedMetadata.command(replicated, {:split_range, root_id})

    {:ok, refreshed} = ReplicatedMetadata.refresh(replicated)
    assert refreshed.cache == replicated.cache
  end
end
