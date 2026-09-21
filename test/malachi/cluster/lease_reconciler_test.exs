defmodule Malachi.Cluster.LeaseReconcilerTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.LeaseReconciler

  test "reconciles right after start and again on demand (level-triggered)" do
    test_pid = self()

    {:ok, reconciler} =
      LeaseReconciler.start_link(
        reconcile: fn -> send(test_pid, :reconciled) end,
        # long interval so the scheduled tick never fires mid-test; reconcile_now drives extra passes
        interval: 60_000
      )

    # the handle_continue pass runs right after start
    assert_receive :reconciled

    # and a manual pass runs synchronously
    assert LeaseReconciler.reconcile_now(reconciler) == :ok
    assert_receive :reconciled
  end

  test "keeps reconciling on its own every interval" do
    test_pid = self()
    {:ok, _reconciler} = LeaseReconciler.start_link(reconcile: fn -> send(test_pid, :reconciled) end, interval: 10)

    # the pass right after start, then at least two scheduled ticks
    for _ <- 1..3, do: assert_receive(:reconciled, 1_000)
  end
end

defmodule Malachi.Cluster.LeaseReconcilerVersionTest do
  # async: false: the stuck member is produced with the node-wide machine version pin.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Malachi.Cluster.LeaseReconciler
  alias Malachi.Cluster.MetadataMachine
  alias Malachi.Test.StuckRaMember

  test "without a version check the status stays :ok" do
    {:ok, reconciler} = LeaseReconciler.start_link(reconcile: fn -> :ok end, interval: 60_000)
    assert LeaseReconciler.version_status(reconciler) == :ok
  end

  test "watches its member each tick, logging once when it gets stuck and once when it recovers" do
    name = :"lr_stuck_#{System.unique_integer([:positive])}"
    on_exit(fn -> StuckRaMember.cleanup({name, node()}) end)
    server_id = StuckRaMember.start(name)

    log =
      capture_log(fn ->
        {:ok, reconciler} =
          LeaseReconciler.start_link(
            reconcile: fn -> :ok end,
            version_check: {MetadataMachine, server_id},
            interval: 60_000
          )

        assert LeaseReconciler.reconcile_now(reconciler) == :ok
        assert LeaseReconciler.reconcile_now(reconciler) == :ok
        assert LeaseReconciler.version_status(reconciler) == {:stuck, 1, 0}

        :ok = StuckRaMember.recover(server_id)
        assert LeaseReconciler.reconcile_now(reconciler) == :ok
        assert LeaseReconciler.version_status(reconciler) == :ok
      end)

    assert length(Regex.scan(~r/stopped applying entries/, log)) == 1
  end
end
