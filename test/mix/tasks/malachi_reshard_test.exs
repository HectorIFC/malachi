defmodule Mix.Tasks.Malachi.ReshardTest do
  use ExUnit.Case, async: true

  alias Mix.Tasks.Malachi.Reshard

  # A `call` seam that records the (module, fun, args) to the test process and returns a canned result.
  defp recording_call(result) do
    parent = self()

    fn module, fun, args ->
      send(parent, {:called, module, fun, args})
      result
    end
  end

  test "asks the remote coordinator to grow to the target and reports success" do
    call = recording_call({:ok, :ok})

    assert {:ok, msg} = Reshard.execute([], [to: 16], call)
    assert msg =~ "16 vnodes"

    assert_received {:called, Malachi.Cluster.ReshardCoordinator, :reshard, [Malachi.LogReshardCoordinator, 16]}
  end

  test "a missing or invalid target returns usage without calling the seam" do
    call = recording_call({:ok, :ok})

    assert {:error, missing} = Reshard.execute([], [], call)
    assert missing =~ "usage:"

    assert {:error, zero} = Reshard.execute([], [to: 0], call)
    assert zero =~ "usage:"

    refute_received {:called, _, _, _}
  end

  test "stray positional arguments return usage" do
    assert {:error, msg} = Reshard.execute(["16"], [to: 16], recording_call({:ok, :ok}))
    assert msg =~ "usage:"
  end

  test "coordinator errors are explained in operator terms" do
    assert {:error, msg} = Reshard.execute([], [to: 8], recording_call({:ok, {:error, :not_leader}}))
    assert msg =~ "lease"

    assert {:error, msg} = Reshard.execute([], [to: 8], recording_call({:ok, {:error, :no_topology}}))
    assert msg =~ "no ring"

    assert {:error, msg} = Reshard.execute([], [to: 2], recording_call({:ok, {:error, :cannot_shrink}}))
    assert msg =~ "growing only"
  end

  test "an unmapped coordinator error is still surfaced" do
    reason = {:split_failed, :vn_42, :migrate_failed}
    assert {:error, msg} = Reshard.execute([], [to: 8], recording_call({:ok, {:error, reason}}))
    assert msg =~ "split_failed"
  end

  test "an RPC transport failure is reported, not crashed" do
    assert {:error, msg} = Reshard.execute([], [to: 8], recording_call({:error, :nodedown}))
    assert msg =~ "rpc failed"
    assert msg =~ "nodedown"
  end
end
