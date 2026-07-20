defmodule Malachi.Auth.UserServerTest do
  # async: false: ra is global/stateful (one data dir, on-disk Raft logs).
  use ExUnit.Case, async: false

  alias Malachi.Auth.UserServer

  setup_all do
    :ok
  end

  defp start_cluster do
    name = :"users_#{System.unique_integer([:positive])}"
    {:ok, server_id} = UserServer.start(name)
    on_exit(fn -> UserServer.delete(name) end)
    server_id
  end

  test "put_user replicates through the log and get_user reads it back from the local replica" do
    server_id = start_cluster()

    assert {:ok, :ok} = UserServer.put_user(server_id, "admin", "hash1", [:admin])
    assert {:ok, {"admin", "hash1", [:admin]}} = UserServer.get_user(server_id, "admin")
    assert {:error, :user_not_found} = UserServer.get_user(server_id, "ghost")
  end

  test "put_user rejects a duplicate (machine reply surfaced)" do
    server_id = start_cluster()
    assert {:ok, :ok} = UserServer.put_user(server_id, "admin", "hash1", [:admin])
    assert {:ok, {:error, :user_exists}} = UserServer.put_user(server_id, "admin", "hash2", [:produce])
    # the original record is untouched
    assert {:ok, {"admin", "hash1", [:admin]}} = UserServer.get_user(server_id, "admin")
  end

  test "update_password changes the hash; a missing user surfaces :user_not_found" do
    server_id = start_cluster()
    assert {:ok, :ok} = UserServer.put_user(server_id, "admin", "hash1", [:admin])

    assert {:ok, :ok} = UserServer.update_password(server_id, "admin", "hash2")
    assert {:ok, {"admin", "hash2", [:admin]}} = UserServer.get_user(server_id, "admin")

    assert {:ok, {:error, :user_not_found}} = UserServer.update_password(server_id, "ghost", "h")
  end

  test "delete_user removes a user and is idempotent" do
    server_id = start_cluster()
    assert {:ok, :ok} = UserServer.put_user(server_id, "admin", "hash1", [:admin])

    assert {:ok, :ok} = UserServer.delete_user(server_id, "admin")
    assert {:error, :user_not_found} = UserServer.get_user(server_id, "admin")
    # deleting again is still :ok
    assert {:ok, :ok} = UserServer.delete_user(server_id, "admin")
  end

  test "import_users bulk-inserts skipping existing, and list/export read the local replica" do
    server_id = start_cluster()
    assert {:ok, :ok} = UserServer.put_user(server_id, "admin", "hash1", [:admin])

    users = [{"admin", "other", [:admin]}, {"producer", "hp", [:produce]}, {"app", "ha", [:produce, :consume]}]
    assert {:ok, {:ok, %{imported: 2, skipped: 1}}} = UserServer.import_users(server_id, users)

    assert {:ok, listed} = UserServer.list_users(server_id)
    assert Enum.sort_by(listed, & &1.username) == [
             %{username: "admin", permissions: [:admin]},
             %{username: "app", permissions: [:produce, :consume]},
             %{username: "producer", permissions: [:produce]}
           ]

    assert {:ok, exported} = UserServer.export_users(server_id)
    admin = Enum.find(exported, &(&1.username == "admin"))
    assert admin.permissions == ["admin"]
    refute Map.has_key?(admin, :hash)
  end
end
