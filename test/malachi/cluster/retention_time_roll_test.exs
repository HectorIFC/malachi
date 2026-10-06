defmodule Malachi.Cluster.RetentionTimeRollTest do
  # The failure #197 exists for, end to end on one node: a topic far below `:segment_max_bytes` seals
  # its active segment once it is older than `:segment_max_age_ms`, with no produce driving it, and age
  # retention then expires it on time. Before the age roll its only segment stayed active forever and
  # retention, which only ever sees sealed segments, never expired a record of it.
  use ExUnit.Case, async: true

  import Malachi.Test.PollingHelper

  alias Malachi.BrokerServer
  alias Malachi.Cluster.RetentionCoordinator
  alias Malachi.LogApi
  alias Malachi.Metadata
  alias Malachi.Retention.SkipReporter

  @minute 60_000
  @hour 3_600_000

  setup %{tmp_dir: directory} = context do
    name = :"time_roll_broker_#{System.unique_integer([:positive])}"
    start_supervised!({SkipReporter, name: SkipReporter.name_for(name)})

    # The default segment size, 64 MiB: nothing this test writes comes near it, so no roll is by size.
    {:ok, broker} = BrokerServer.start_link(directory, name: name)
    on_exit(fn -> if Process.alive?(broker), do: BrokerServer.stop(broker) end)

    # The sweep's clock, moved by the test.
    {:ok, clock} = Agent.start_link(fn -> System.system_time(:millisecond) end)

    {:ok, coordinator} =
      RetentionCoordinator.start_link(
        metadata_source: fn -> BrokerServer.metadata(name) end,
        expire_segment: &Malachi.Application.expire_segment(&1, name),
        roll_segments: &BrokerServer.request_rolls(name, &1),
        policy: %{max_age_ms: @hour, segment_max_age_ms: @minute},
        policies: fn -> {:ok, Map.get(context, :policies, %{})} end,
        clock: fn -> Agent.get(clock, & &1) end,
        interval: 3_600_000
      )

    %{name: name, clock: clock, coordinator: coordinator}
  end

  defp segments(name, range_id), do: name |> BrokerServer.metadata() |> Metadata.segments_of_range(range_id)
  defp advance_to(clock, at), do: Agent.update(clock, fn _now -> at end)

  @tag :tmp_dir
  test "an idle topic seals its segment by age, and age retention then expires it on time", ctx do
    %{name: name, clock: clock, coordinator: coordinator} = ctx

    :ok = LogApi.create_topic(name, "audit")
    {:ok, 1} = LogApi.produce(name, "audit", [%{"key" => "k0", "value" => "v0"}])
    [range_id] = BrokerServer.active_range_ids(name, "audit")
    :ok = BrokerServer.commit_offset(name, "billing", "audit", %{range_id => {0, 0}})

    [%{state: :active, opened_at: opened_at} = head] = segments(name, range_id)

    # Younger than the limit: nothing is asked of the broker.
    advance_to(clock, opened_at + @minute - 1)
    assert RetentionCoordinator.run_now(coordinator) == []
    assert [%{state: :active}] = segments(name, range_id)

    # Older than the limit: the sweep asks for the roll and the fence seals it where the store ends,
    # with no produce and no successor.
    advance_to(clock, opened_at + @minute)
    assert RetentionCoordinator.run_now(coordinator) == []
    wait_until!(fn -> match?([%{state: :sealed}], segments(name, range_id)) end)
    assert [%{id: id, state: :sealed, start_offset: 0, length: 1, sealed_at: sealed_at}] = segments(name, range_id)
    assert id == head.id

    # Age retention sees it now: kept until it is older than max_age_ms by its seal, expired after.
    advance_to(clock, sealed_at + @hour)
    assert RetentionCoordinator.run_now(coordinator) == []

    advance_to(clock, sealed_at + @hour + 1)
    assert RetentionCoordinator.run_now(coordinator) == [id]
    assert segments(name, range_id) == []

    # The topic goes on: the next produce opens a successor at the expired edge, and a group positioned
    # before it is moved past what was deleted.
    {:ok, 1} = LogApi.produce(name, "audit", [%{"key" => "k1", "value" => "v1"}])
    assert [%{state: :active, start_offset: 1}] = segments(name, range_id)
    assert {:ok, [%{value: "v1"}], _cursor} = LogApi.fetch_group(name, "audit", "billing", 100)
  end

  @tag :tmp_dir
  @tag policies: %{"forever" => %{retention: %{segment_max_age_ms: nil}}}
  test "a topic whose policy turns the roll off keeps its segment active", ctx do
    %{name: name, clock: clock, coordinator: coordinator} = ctx

    :ok = LogApi.create_topic(name, "keep")
    {:ok, 1} = LogApi.produce(name, "keep", [%{"key" => "k0", "value" => "v0"}])
    [range_id] = BrokerServer.active_range_ids(name, "keep")
    :ok = BrokerServer.bind_topic_policy(name, "keep", "forever")

    advance_to(clock, System.system_time(:millisecond) + 100 * @hour)
    RetentionCoordinator.run_now(coordinator)
    Process.sleep(100)

    assert [%{state: :active}] = segments(name, range_id)
  end
end
