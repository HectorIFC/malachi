defmodule Malachi.SingleNodeRestartTest do
  @moduledoc """
  A single node keeps what it acknowledged across a restart (#273).

  The suite's own application is a single node in its default configuration, so this restarts ITS log
  stack the way a node restart does (the broker, its replication server and the metadata member all go
  down and come back over the same directories) and then asks what a user would: are the records still
  readable, is the group's position still there, and does the orphan sweep leave the segments alone.

  It used to keep the metadata in memory: after the restart the topic was gone, and the sweep, whose
  only authority was that empty memory, took every segment directory written before the boot.
  """
  # async: false: it restarts the application's broker, which every other test may be using.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Malachi.BrokerServer
  alias Malachi.LogApi
  alias Malachi.Retention.OrphanSweeper
  alias Malachi.Storage.Layout

  @broker Malachi.LogBroker

  # The restart reopens every active segment the suite's broker holds by then, one by one, and on a disk
  # where preallocation is slow (a container's overlay filesystem) that is seconds per hundred segments.
  @tag timeout: 300_000
  test "a restart keeps every acknowledged record, the group's position and the segments on disk" do
    topic = "restart_#{System.unique_integer([:positive])}"
    group = "#{topic}_group"
    values = for i <- 1..200, do: "v#{i}"

    :ok = LogApi.create_topic(@broker, topic)
    {:ok, 200} = LogApi.produce(@broker, topic, Enum.map(values, &%{"key" => &1, "value" => &1}))
    {:ok, first_page, cursor} = LogApi.fetch(@broker, topic, :start, 50)
    assert length(first_page) == 50
    :ok = LogApi.commit(@broker, topic, group, cursor)
    committed = BrokerServer.committed_offsets(@broker, group, topic)
    directories = segment_directories(topic)
    assert directories != []

    capture_log(fn -> restart_log_stack() end)

    # The records, all of them and nothing else. A refusal while the broker still learns where a range
    # ends is retried; an empty or short successful page is not, since that is the failure.
    assert {:ok, records, _next} = fetch_once_recovered(topic)
    assert records |> Enum.map(& &1.value) |> Enum.sort() == Enum.sort(values)

    # The group resumes where it committed.
    assert BrokerServer.committed_offsets(@broker, group, topic) == committed

    # The sweep, as eager as it can be made and only reporting, would take none of them.
    result = sweep_reporting_everything()
    assert result.skipped == nil

    for directory <- directories do
      assert File.dir?(Path.join(log_data_dir(), directory))
      refute directory in result.held, "the sweep would delete #{directory}, a segment of #{topic}"
      refute directory in result.removed
    end
  end

  defp segment_directories(topic) do
    BrokerServer.metadata(@broker).segments
    |> Map.keys()
    |> Enum.filter(fn {{segment_topic, _range}, _seq} -> segment_topic == topic end)
    |> Enum.map(&Path.basename(Layout.segment_directory(log_data_dir(), &1)))
  end

  defp log_data_dir, do: Application.fetch_env!(:malachi, :log_data_dir)

  # The log stack's broker and replication server, stopped and started again in the wiring's order, with
  # the metadata member stopped between them so it comes back from disk, as on a node restart. A
  # deployment whose broker owns its replication server has no separate child, and restarting the broker
  # restarts both.
  defp restart_log_stack do
    children = Supervisor.which_children(Malachi.Supervisor) |> Enum.map(&elem(&1, 0))
    stack = Enum.filter([Malachi.LogReplication, @broker], &(&1 in children))

    for child <- Enum.reverse(stack), do: :ok = Supervisor.terminate_child(Malachi.Supervisor, child)

    case Application.get_env(:malachi, :log_cluster) do
      nil -> :ok
      cluster -> :ok = :ra.stop_server(:default, {cluster, node()})
    end

    for child <- stack, do: {:ok, _pid} = Supervisor.restart_child(Malachi.Supervisor, child)
    :ok = BrokerServer.reconcile_now(@broker, 120_000)
  end

  defp fetch_once_recovered(topic, attempts \\ 300) do
    case LogApi.fetch(@broker, topic, :start, 1_000) do
      {:error, :metadata_unavailable} when attempts > 1 ->
        Process.sleep(100)
        fetch_once_recovered(topic, attempts - 1)

      answer ->
        answer
    end
  end

  # One pass of the application's own sweeper with no age or sighting guard, in report mode so nothing is
  # deleted: what it holds is exactly what a deleting pass would take. Its own state is put back after.
  defp sweep_reporting_everything do
    [sweeper] =
      for {_id, pid, :worker, [OrphanSweeper]} <- Supervisor.which_children(Malachi.Supervisor), do: pid

    original = :sys.get_state(sweeper)
    :sys.replace_state(sweeper, &%{&1 | mode: :report, min_age_ms: 0, sightings: 1, seen: %{}})

    try do
      capture_log(fn -> send(self(), {:result, OrphanSweeper.sweep_now(sweeper)}) end)
      assert_received {:result, result}
      result
    after
      :sys.replace_state(sweeper, fn _state -> original end)
    end
  end
end
