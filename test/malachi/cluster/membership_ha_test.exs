defmodule Malachi.Cluster.MembershipHaTest do
  # Real multi-node SWIM membership: spins up peer BEAM nodes, forms a membership across them using
  # node-qualified self_refs ({name, node}), and verifies the views converge and a dead node is
  # detected. async: false and tagged :multinode so it is excluded where distribution is unavailable.
  use ExUnit.Case, async: false

  @moduletag :multinode

  alias Malachi.Cluster.MembershipServer

  @name Malachi.LogMembership
  # Tight timings so the detector converges quickly under test.
  @timings [protocol_period: 50, ack_timeout: 50, indirect_timeout: 50, suspicion_timeout: 300]

  setup_all do
    _ = System.cmd("epmd", ["-daemon"])

    case :net_kernel.start([:"malachi_primary@127.0.0.1", :longnames]) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    :ok
  end

  defp start_peer do
    name = :"malachi_mempeer_#{System.unique_integer([:positive])}"
    {:ok, peer, node} = :peer.start_link(%{name: name, host: ~c"127.0.0.1", longnames: true})
    on_exit(fn -> try_stop(peer) end)
    :ok = :erpc.call(node, :code, :add_paths, [:code.get_path()])
    {peer, node}
  end

  defp try_stop(peer) do
    :peer.stop(peer)
  catch
    _kind, _reason -> :ok
  end

  # The member reference for a node: node-qualified so it resolves from any node (the bug the
  # :self_ref option fixes: a bare local name would resolve to a different server on each node).
  defp ref(node), do: {@name, node}

  defp start_membership(node, seeds) do
    opts = [name: @name, self_ref: ref(node), peers: seeds] ++ @timings
    {:ok, _pid} = :erpc.call(node, MembershipServer, :start, [opts])
  end

  defp alive(node), do: :erpc.call(node, MembershipServer, :alive_members, [@name])

  defp eventually(check, remaining_ms \\ 5_000) do
    cond do
      check.() -> true
      remaining_ms <= 0 -> false
      true -> Process.sleep(50) && eventually(check, remaining_ms - 50)
    end
  end

  test "views converge across nodes and a dead node is detected" do
    peers = for _ <- 1..3, do: start_peer()
    nodes = Enum.map(peers, &elem(&1, 1))
    peer_by_node = Map.new(peers, fn {peer, node} -> {node, peer} end)
    refs = Enum.map(nodes, &ref/1)

    # each node seeds with the others; SWIM gossip must make every view list all three
    for node <- nodes, do: start_membership(node, refs -- [ref(node)])

    full = Enum.sort(refs)
    assert eventually(fn -> Enum.all?(nodes, fn node -> alive(node) == full end) end)

    # kill one node abruptly; the survivors must detect it and drop it from their alive set
    [dead | survivors] = nodes
    :ok = try_stop(Map.fetch!(peer_by_node, dead))

    expected = Enum.sort(refs -- [ref(dead)])
    assert eventually(fn -> Enum.all?(survivors, fn node -> alive(node) == expected end) end)
  end
end
