defmodule Malachi.Auth.AclServerTest do
  # async: false — ra is global/stateful (one data dir, on-disk Raft logs).
  use ExUnit.Case, async: false

  alias Malachi.Auth.AclServer

  defp start_cluster do
    name = :"acls_#{System.unique_integer([:positive])}"
    {:ok, server_id} = AclServer.start(name)
    on_exit(fn -> AclServer.delete(name) end)
    server_id
  end

  test "a grant replicates through the log and authorized? reads it back from the local replica" do
    server_id = start_cluster()

    assert {:ok, :ok} = AclServer.grant(server_id, "alice", :produce, {:literal, "orders.eu"})
    assert {:ok, true} = AclServer.authorized?(server_id, "alice", :produce, "orders.eu")
    assert {:ok, false} = AclServer.authorized?(server_id, "alice", :produce, "orders.us")
    assert {:ok, false} = AclServer.authorized?(server_id, "bob", :produce, "orders.eu")
  end

  test "a prefix grant authorizes any topic under the prefix" do
    server_id = start_cluster()
    assert {:ok, :ok} = AclServer.grant(server_id, "team", :consume, {:prefix, "orders."})

    assert {:ok, true} = AclServer.authorized?(server_id, "team", :consume, "orders.eu")
    assert {:ok, true} = AclServer.authorized?(server_id, "team", :consume, "orders.us.west")
    assert {:ok, false} = AclServer.authorized?(server_id, "team", :consume, "payments.eu")
  end

  test "revoke removes a grant; revoke_user clears the user" do
    server_id = start_cluster()
    assert {:ok, :ok} = AclServer.grant(server_id, "alice", :produce, {:literal, "a"})
    assert {:ok, :ok} = AclServer.grant(server_id, "alice", :consume, {:prefix, "b."})

    assert {:ok, :ok} = AclServer.revoke(server_id, "alice", :produce, {:literal, "a"})
    assert {:ok, false} = AclServer.authorized?(server_id, "alice", :produce, "a")
    assert {:ok, true} = AclServer.authorized?(server_id, "alice", :consume, "b.1")

    assert {:ok, :ok} = AclServer.revoke_user(server_id, "alice")
    assert {:ok, []} = AclServer.list_grants(server_id, "alice")
  end

  test "list_grants and list_all read the local replica" do
    server_id = start_cluster()
    assert {:ok, :ok} = AclServer.grant(server_id, "alice", :produce, {:literal, "a"})
    assert {:ok, :ok} = AclServer.grant(server_id, "bob", :consume, {:prefix, "b."})

    assert {:ok, [{:produce, {:literal, "a"}}]} = AclServer.list_grants(server_id, "alice")

    assert {:ok, all} = AclServer.list_all(server_id)

    assert Enum.sort(all) == [
             {"alice", :produce, {:literal, "a"}},
             {"bob", :consume, {:prefix, "b."}}
           ]
  end
end
