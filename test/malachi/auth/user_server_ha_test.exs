defmodule Malachi.Auth.UserServerHaTest do
  # Real multi-node Raft for the user store: spins up peer BEAM nodes, forms the user cluster across them,
  # and verifies a user written on one node replicates to another node's local replica (what the old
  # node-local Mnesia store could not do) and survives losing a member. Tagged so it can be excluded where
  # multi-node networking is unavailable.
  use ExUnit.Case, async: false

  @moduletag :multinode

  alias Malachi.Auth.UserServer

  setup_all do
    _ = System.cmd("epmd", ["-daemon"])

    case :net_kernel.start([:"malachi_primary@127.0.0.1", :longnames]) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    {:ok, _} = Application.ensure_all_started(:ra)
    # the local node is itself a cluster member here, so ra must run locally too
    _ = :ra.start_in(~c"#{System.tmp_dir!()}/malachi_ra_users_local_#{System.unique_integer([:positive])}")
    :ok
  end

  defp start_peer do
    name = :"malachi_upeer_#{System.unique_integer([:positive])}"
    {:ok, peer, node} = :peer.start_link(%{name: name, host: ~c"127.0.0.1", longnames: true})
    on_exit(fn -> try_stop(peer) end)

    :ok = :erpc.call(node, :code, :add_paths, [:code.get_path()])
    {:ok, _} = :erpc.call(node, :application, :ensure_all_started, [:ra])
    data_dir = ~c"#{System.tmp_dir!()}/malachi_ra_users_#{name}_#{System.unique_integer([:positive])}"
    {:ok, _} = :erpc.call(node, :ra, :start_in, [data_dir])

    {peer, node}
  end

  defp try_stop(peer) do
    :peer.stop(peer)
  catch
    _kind, _reason -> :ok
  end

  # ra may need a moment to elect after cluster start / member loss; retry the write until it lands.
  defp put(server_id, username, hash, perms, remaining_ms \\ 5_000) do
    case UserServer.put_user(server_id, username, hash, perms) do
      {:error, _reason} when remaining_ms > 0 ->
        Process.sleep(100) && put(server_id, username, hash, perms, remaining_ms - 100)

      reply ->
        reply
    end
  end

  # a follower's local replica may lag the leader by a replication round; retry the read until it appears.
  defp eventually(fun, remaining_ms \\ 5_000) do
    case fun.() do
      {:ok, _} = ok -> ok
      _other when remaining_ms > 0 -> Process.sleep(100) && eventually(fun, remaining_ms - 100)
      other -> other
    end
  end

  test "a user written on one node is readable on another node's local replica (replicated, not node-local)" do
    peers = for _ <- 1..2, do: start_peer()
    [n1, n2] = Enum.map(peers, &elem(&1, 1))
    name = :"users_ha_#{System.unique_integer([:positive])}"
    nodes = [node(), n1, n2]
    on_exit(fn -> UserServer.delete(name) end)

    {:ok, server_id} = UserServer.start(name, nodes)
    assert {:ok, :ok} = put(server_id, "admin", "hash1", [:admin])

    # each peer reads from its OWN local replica — the user replicated cluster-wide
    for n <- [n1, n2] do
      assert {:ok, {"admin", "hash1", [:admin]}} =
               eventually(fn -> :erpc.call(n, UserServer, :get_user, [{name, n}, "admin"]) end)
    end
  end

  test "the user store still commits after losing a member (HA)" do
    peers = for _ <- 1..2, do: start_peer()
    [n1, n2] = Enum.map(peers, &elem(&1, 1))
    name = :"users_hb_#{System.unique_integer([:positive])}"
    nodes = [node(), n1, n2]
    on_exit(fn -> UserServer.delete(name) end)

    {:ok, server_id} = UserServer.start(name, nodes)
    assert {:ok, :ok} = put(server_id, "admin", "hash1", [:admin])

    # kill one member — a 3-node cluster keeps quorum with the remaining 2
    {peer2, _} = List.last(peers)
    :ok = try_stop(peer2)

    # the cluster still commits and serves reads (the write may re-elect; put/5 retries)
    assert {:ok, :ok} = put(server_id, "producer", "hp", [:produce])
    assert {:ok, {"producer", "hp", [:produce]}} = UserServer.get_user(server_id, "producer")
    assert {:ok, {"admin", "hash1", [:admin]}} = UserServer.get_user(server_id, "admin")
  end
end
