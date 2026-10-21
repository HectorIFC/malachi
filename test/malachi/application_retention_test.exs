defmodule Malachi.ApplicationRetentionTest do
  @moduledoc """
  What the running application wires up for retention, asserted against the real supervision tree rather
  than against the functions that build it.

  The suite runs with no retention environment at all (`config/test.exs` sets neither
  `MALACHI_RETENTION_MAX_AGE_MS` nor `MALACHI_RETENTION_MAX_BYTES`), which is the default a fresh
  deployment has and the case the gate used to swallow: with both unset no coordinator started, so a
  per-topic policy was inert and nothing could ever expire.
  """
  use ExUnit.Case, async: true

  import Malachi.Test.PollingHelper

  alias Malachi.BrokerServer
  alias Malachi.Cluster.RetentionCoordinator
  alias Malachi.LogApi
  alias Malachi.Metadata
  alias Malachi.Retention.OrphanSweeper

  test "the retention coordinator runs with no global limit configured" do
    refute Application.get_env(:malachi, :retention_max_age_ms)
    refute Application.get_env(:malachi, :retention_max_bytes)

    assert is_pid(Process.whereis(Malachi.LogRetention))
    # And it sweeps: with no bound anywhere the pass is a no-op, which is the point.
    assert RetentionCoordinator.run_now(Malachi.LogRetention) == []
  end

  test "the global policy carries the age roll's limit, 7 days unless MALACHI_SEGMENT_MAX_AGE_MS says otherwise" do
    assert Malachi.Application.retention_policy() == %{
             max_age_ms: nil,
             max_bytes: nil,
             segment_max_age_ms: 604_800_000
           }
  end

  # Every coordinator the application starts takes its seams from `retention_opts/2`, and the roll seam's
  # default is a no-op: without it here, nothing in production would ever roll a quiet topic.
  test "the roll seam the coordinators are given seals a due segment on this node's broker" do
    topic = "app_roll_#{System.unique_integer([:positive])}"
    broker = Malachi.LogBroker
    :ok = LogApi.create_topic(broker, topic)
    {:ok, 1} = LogApi.produce(broker, topic, [%{"key" => "k", "value" => "v"}])

    head = fn ->
      BrokerServer.metadata(Malachi.LogBroker)
      |> Metadata.segments_of_range({topic, 0})
      |> List.first()
    end

    opts = Malachi.Application.retention_opts(fn -> Metadata.new() end, fn -> true end)
    opts[:roll_segments].([head.()])

    wait_until!(fn -> match?(%{state: :sealed, length: 1}, head.()) end)
  end

  test "the orphan sweeper runs beside the data-plane broker, on every node" do
    sweeper = Process.whereis(Malachi.LogOrphanSweeper)
    assert is_pid(sweeper)
    assert OrphanSweeper.mode(sweeper) == :delete
  end

  # The suite runs with no MALACHI_LOG_CLUSTER, the default a single node has. Its sweeper must ask a
  # control plane that survives a restart, never the broker's memory (#273).
  test "a single node in its default configuration runs a one-member control plane cluster" do
    assert Application.get_env(:malachi, :log_cluster) == :malachi_log
    member = {:malachi_log, node()}
    assert {:ok, [^member], ^member} = :ra.members({:local, member}, 5_000)
  end
end
