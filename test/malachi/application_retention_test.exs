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

  alias Malachi.Cluster.RetentionCoordinator
  alias Malachi.Retention.OrphanSweeper

  test "the retention coordinator runs with no global limit configured" do
    refute Application.get_env(:malachi, :retention_max_age_ms)
    refute Application.get_env(:malachi, :retention_max_bytes)

    assert is_pid(Process.whereis(Malachi.LogRetention))
    # And it sweeps: with no bound anywhere the pass is a no-op, which is the point.
    assert RetentionCoordinator.run_now(Malachi.LogRetention) == []
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
