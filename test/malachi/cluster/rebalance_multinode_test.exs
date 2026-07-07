defmodule Malachi.Cluster.RebalanceMultinodeTest do
  # Real multi-node rebalancing: spins up peer BEAM nodes and moves a vnode's ra membership across them.
  # async: false and tagged so it can be excluded where multi-node networking is unavailable.
  use ExUnit.Case, async: false

  @moduletag :multinode

  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.Rebalance
  alias Malachi.Metadata

  setup_all do
    _ = System.cmd("epmd", ["-daemon"])

    case :net_kernel.start([:"malachi_primary@127.0.0.1", :longnames]) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    {:ok, _} = Application.ensure_all_started(:ra)
    :ok
  end

  defp start_peer do
    name = :"malachi_rebal_#{System.unique_integer([:positive])}"
    {:ok, peer, node} = :peer.start_link(%{name: name, host: ~c"127.0.0.1", longnames: true})
    on_exit(fn -> try_stop(peer) end)

    :ok = :erpc.call(node, :code, :add_paths, [:code.get_path()])
    {:ok, _} = :erpc.call(node, :application, :ensure_all_started, [:ra])
    data_dir = ~c"#{System.tmp_dir!()}/malachi_ra_rebal_#{name}_#{System.unique_integer([:positive])}"
    {:ok, _} = :erpc.call(node, :ra, :start_in, [data_dir])

    {peer, node}
  end

  defp try_stop(peer) do
    :peer.stop(peer)
  catch
    _kind, _reason -> :ok
  end

  # After a membership/leadership change ra needs a moment; retry until the fun returns {:ok, _}.
  defp retry(fun, remaining_ms \\ 5_000) do
    case fun.() do
      {:ok, _value} = ok -> ok
      _other when remaining_ms > 0 -> Process.sleep(100) && retry(fun, remaining_ms - 100)
      other -> other
    end
  end

  test "ra_add_member grows a vnode onto a new node (ra transfers its state), ra_remove_member shrinks it" do
    {_peer_a, node_a} = start_peer()
    {_peer_b, node_b} = start_peer()

    vnode = :"vn_#{System.unique_integer([:positive])}"

    # a single-node vnode living on node_a, populated with a topic
    {:ok, _server} = :erpc.call(node_a, MetadataServer, :start, [vnode, [node_a]])
    {:ok, {:ok, _root}} = retry(fn -> MetadataServer.command({vnode, node_a}, {:create_topic, "t", 4}) end)

    # grow it onto node_b
    assert Rebalance.ra_add_member(vnode, node_b, [node_a]) == :ok

    # node_b is a member now, and ra replicated the vnode's state to it
    assert {:ok, members, _leader} = :ra.members({vnode, node_a})
    assert {vnode, node_b} in members

    {:ok, state} = retry(fn -> MetadataServer.query({vnode, node_b}, &Function.identity/1) end)
    assert Metadata.get_topic(state, "t").name == "t"

    # idempotent: adding node_b again is a no-op
    assert Rebalance.ra_add_member(vnode, node_b, [node_a]) == :ok

    # shrink it back off node_b
    assert Rebalance.ra_remove_member(vnode, node_b, [node_a, node_b]) == :ok
    assert {:ok, members_after, _} = :ra.members({vnode, node_a})
    refute {vnode, node_b} in members_after

    # idempotent: removing node_b again is a no-op
    assert Rebalance.ra_remove_member(vnode, node_b, [node_a]) == :ok
  end
end
