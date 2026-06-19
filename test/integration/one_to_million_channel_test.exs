defmodule Malachi.OneToMillionChannelTest do
  use ExUnit.Case, async: true
  alias Malachi.Channel
  alias Malachi.Metrics
  alias Malachi.Test.MassSpawnHelper
  alias Malachi.Test.PollingHelper

  @moduletag :integration
  @timeout_ms 300_000

  setup do
    # Clean up any orphaned tables from previous test runs
    MassSpawnHelper.cleanup_all()

    :ok
  end

  @tag timeout: @timeout_ms
  test "one producer publishes to one million logical subscribers via sharding" do
    channel = "one_to_million_#{:rand.uniform(999_999)}"

    # configuration for sharded test
    shard_count = Application.get_env(:malachi, :shard_count, 1_000)
    logical_per_process = div(1_000_000, shard_count)

    # start shards (each shard subscribes and counts deliveries)
    {:ok, shard_pids} = MassSpawnHelper.start_shards(channel, shard_count, logical_per_process)

    # ensure metrics reset
    Metrics.reset_metrics(channel)

    # publish single message
    Channel.publish(channel, "heavy-test", %{})

    # wait for deliveries based on ETS counter from helper
    PollingHelper.wait_until!(fn -> MassSpawnHelper.delivered_count(channel) >= 1_000_000 end,
      timeout: 300_000
    )

    delivered = MassSpawnHelper.delivered_count(channel)

    assert delivered == 1_000_000

    # Wait for metrics to update (async operation after broadcast completes)
    PollingHelper.wait_until!(
      fn ->
        Metrics.get_channel_metrics(channel).delivered >= shard_count
      end,
      timeout: 5_000
    )

    # cross-check metrics ETS (counts real processes, not logical subscribers)
    metrics = Metrics.get_channel_metrics(channel)
    assert metrics.delivered == shard_count

    # Schedule cleanup to run after test completes successfully
    on_exit(fn ->
      MassSpawnHelper.cleanup_shards(shard_pids)
    end)
  end
end
