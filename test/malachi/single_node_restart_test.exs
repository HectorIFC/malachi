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
  alias Malachi.ConsumeStreams
  alias Malachi.Log.Record
  alias Malachi.LogApi
  alias Malachi.Retention.OrphanSweeper
  alias Malachi.Storage.Layout
  alias Malachi.Test.ReadBack
  alias Malachi.Wire.Batch

  @broker Malachi.LogBroker

  # The restart reopens every active segment the suite's broker holds by then, one by one, and on a disk
  # where preallocation is slow (a container's overlay filesystem) that is seconds per hundred segments.
  @tag timeout: 300_000
  test "a restart keeps every acknowledged record, the group's position and the segments on disk" do
    topic = "restart_#{System.unique_integer([:positive])}"
    group = "#{topic}_group"
    values = for i <- 1..200, do: "v#{i}"

    :ok = LogApi.create_topic(@broker, topic)
    {:ok, _} = BrokerServer.produce(@broker, topic, Enum.map(values, &Record.new(&1, key: &1)))
    [root] = BrokerServer.active_range_ids(@broker, topic)
    {first_page, next} = first_page(root, 50)
    assert length(first_page) == 50
    :ok = BrokerServer.commit_offset(@broker, group, topic, %{root => next}, 1)
    committed = committed_offsets(group, topic)
    assert committed == %{root => next}
    directories = segment_directories(topic)
    assert directories != []

    capture_log(fn -> restart_log_stack() end)

    # The records, all of them and nothing else. A refusal while the broker still learns where a range
    # ends is retried; an empty or short successful page is not, since that is the failure.
    assert topic |> fetch_once_recovered() |> Enum.sort() == Enum.sort(values)

    # The group resumes where it committed.
    assert committed_offsets(group, topic) == committed

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

  # The first `max` records of range `root` and where its next page starts, as a member reading from the
  # start gets them.
  defp first_page(root, max) do
    {:ok, position, view, reporter} = BrokerServer.fetch_range(@broker, root, :earliest, 0)
    {:ok, page, _count} = ConsumeStreams.read_page(view, root, position, max, 1_048_576, reporter)
    {:ok, entries} = Batch.decode(page.batch, max_inflated_bytes: 1_048_576, layout: :positioned)
    {entries, page.next}
  end

  # The checkpoints of `group` on `topic` in the broker's metadata.
  defp committed_offsets(group, topic),
    do: @broker |> BrokerServer.metadata() |> Map.fetch!(:committed_offsets) |> Map.get({group, topic}, %{})

  defp fetch_once_recovered(topic, attempts \\ 300) do
    case ReadBack.values(@broker, topic) do
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
