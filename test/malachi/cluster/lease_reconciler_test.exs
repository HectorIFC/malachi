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
end
