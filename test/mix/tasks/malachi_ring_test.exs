defmodule Mix.Tasks.Malachi.RingTest do
  @moduledoc """
  The operator's answer to "why was my MALACHI_LOG_VNODES ignored?". The rendering matters as much as
  the read: a node that logged the divergence points here, so this output has to name the version, the
  vnodes and where they live.
  """
  use ExUnit.Case, async: true

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.Ring
  alias Malachi.Cluster.RingTopology
  alias Mix.Tasks.Malachi.Ring, as: RingTask

  @node :"malachi@127.0.0.1"

  # A `call` seam that records the (module, fun, args) to the test process and returns a canned result.
  defp recording_call(result) do
    parent = self()

    fn module, fun, args ->
      send(parent, {:called, module, fun, args})
      result
    end
  end

  defp stored(vnodes, version, fence, pending \\ nil) do
    ring =
      Enum.reduce(vnodes, HashRing.new(), fn {id, token}, ring ->
        {:ok, ring} = HashRing.add_vnode(ring, id, token)
        ring
      end)

    placements = Map.new(vnodes, fn {id, _token} -> {id, [@node]} end)
    topology = %{RingTopology.new(ring, placements) | version: version, pending: pending}
    %Ring{topology: topology, fence: fence}
  end

  test "reads the ring store on the target node and renders version, fence and vnodes" do
    call = recording_call({:ok, {:ok, stored([{:vn_a, 0}, {:vn_b, 2_000}], 4, 3)}})

    assert {:ok, text} = RingTask.execute([], @node, call)
    assert text =~ "version 4"
    assert text =~ "fence 3"
    assert text =~ "2 vnodes"
    assert text =~ "wins over MALACHI_LOG_VNODES"
    assert text =~ "vn_a"
    assert text =~ "2000\tvn_b"
    assert text =~ to_string(@node)

    assert_received {:called, Malachi.Cluster.RingServer, :get, [{Malachi.LogRing, @node}]}
  end

  test "vnodes are listed in token order, which is how the ring is read" do
    call = recording_call({:ok, {:ok, stored([{:vn_late, 9_000}, {:vn_early, 10}], 1, 1)}})

    assert {:ok, text} = RingTask.execute([], @node, call)
    assert text =~ ~r/vn_early.*vn_late/s
  end

  test "an interrupted split is reported, since it survives a restart and will be completed" do
    pending = %{new_vnode: :vn_new, token: 500, nodes: [@node]}
    call = recording_call({:ok, {:ok, stored([{:vn_a, 0}], 2, 1, pending)}})

    assert {:ok, text} = RingTask.execute([], @node, call)
    assert text =~ "pending split: vn_new at token 500"
    assert text =~ "carry it to completion"
  end

  test "a cluster that has never been sharded says so, rather than printing an empty ring" do
    call = recording_call({:ok, {:ok, %Ring{}}})

    assert {:ok, text} = RingTask.execute([], @node, call)
    assert text =~ "no durable ring recorded"
    assert text =~ "MALACHI_LOG_VNODES still decides"
  end

  test "a store that cannot answer is explained in operator terms" do
    assert {:error, message} = RingTask.execute([], @node, recording_call({:ok, {:error, :timeout}}))
    assert message =~ "did not answer in time"
    assert message =~ "quorum"

    assert {:error, other} = RingTask.execute([], @node, recording_call({:ok, {:error, :noproc}}))
    assert other =~ "clustered control plane"
  end

  test "an rpc transport failure is reported as such" do
    assert {:error, message} = RingTask.execute([], @node, recording_call({:error, :nodedown}))
    assert message =~ "rpc failed"
  end

  test "stray positional arguments return usage without calling the seam" do
    assert {:error, message} = RingTask.execute(["extra"], @node, recording_call({:ok, {:ok, %Ring{}}}))
    assert message =~ "usage:"

    refute_received {:called, _, _, _}
  end
end
