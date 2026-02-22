defmodule MalachiMQ.Auth.UserStoreTest do
  @moduledoc """
  Unit tests for MalachiMQ.Auth.UserStore (Mnesia persistence layer).
  """
  use ExUnit.Case, async: false

  alias MalachiMQ.Auth.UserStore

  @users_table :malachimq_users

  @test_prefixes [
    "test_",
    "dup_",
    "admin_",
    "multi_",
    "del_",
    "pwd_",
    "perms_",
    "get_",
    "list_",
    "exp_",
    "existing",
    "imported",
    "str_",
    "good",
    "sync_",
    "concurrent_"
  ]

  setup do
    on_exit(fn ->
      # Only clean up test-created users, preserve default users
      :mnesia.transaction(fn ->
        :mnesia.foldl(
          fn {_table, username, _hash, _perms, _created, _updated}, acc ->
            if Enum.any?(@test_prefixes, &String.starts_with?(username, &1)) do
              [username | acc]
            else
              acc
            end
          end,
          [],
          UserStore.table_name()
        )
      end)
      |> case do
        {:atomic, usernames} ->
          Enum.each(usernames, fn username ->
            :mnesia.transaction(fn -> :mnesia.delete({UserStore.table_name(), username}) end)
            :ets.delete(@users_table, username)
          end)

        _ ->
          :ok
      end
    end)

    :ok
  end

  describe "insert_user/3" do
    test "inserts a new user into Mnesia and ETS" do
      assert :ok = UserStore.insert_user("test_user", "hashed_pass", [:produce])

      # Verify Mnesia
      assert {:ok, {"test_user", "hashed_pass", [:produce]}} = UserStore.get_user("test_user")

      # Verify ETS cache
      assert [{"test_user", "hashed_pass", [:produce]}] = :ets.lookup(@users_table, "test_user")
    end

    test "returns error for duplicate username" do
      assert :ok = UserStore.insert_user("dup_user", "hash1", [:produce])
      assert {:error, :user_exists} = UserStore.insert_user("dup_user", "hash2", [:consume])
    end

    test "preserves different permission types" do
      assert :ok = UserStore.insert_user("admin_user", "hash", [:admin])
      assert {:ok, {"admin_user", "hash", [:admin]}} = UserStore.get_user("admin_user")

      assert :ok = UserStore.insert_user("multi_user", "hash", [:produce, :consume])
      assert {:ok, {"multi_user", "hash", [:produce, :consume]}} = UserStore.get_user("multi_user")
    end
  end

  describe "delete_user/1" do
    test "removes user from Mnesia and ETS" do
      UserStore.insert_user("del_user", "hash", [:produce])
      assert :ok = UserStore.delete_user("del_user")

      assert {:error, :user_not_found} = UserStore.get_user("del_user")
      assert [] = :ets.lookup(@users_table, "del_user")
    end

    test "is idempotent — deleting non-existent user returns :ok" do
      assert :ok = UserStore.delete_user("nonexistent")
    end
  end

  describe "update_password/2" do
    test "updates password hash in Mnesia and ETS" do
      UserStore.insert_user("pwd_user", "old_hash", [:consume])
      assert :ok = UserStore.update_password("pwd_user", "new_hash")

      assert {:ok, {"pwd_user", "new_hash", [:consume]}} = UserStore.get_user("pwd_user")
      assert [{"pwd_user", "new_hash", [:consume]}] = :ets.lookup(@users_table, "pwd_user")
    end

    test "returns error for non-existent user" do
      assert {:error, :user_not_found} = UserStore.update_password("ghost", "hash")
    end

    test "preserves permissions when changing password" do
      UserStore.insert_user("perms_user", "hash1", [:admin, :produce])
      UserStore.update_password("perms_user", "hash2")

      assert {:ok, {"perms_user", "hash2", [:admin, :produce]}} = UserStore.get_user("perms_user")
    end
  end

  describe "get_user/1" do
    test "returns user data" do
      UserStore.insert_user("get_user", "hash", [:produce])
      assert {:ok, {"get_user", "hash", [:produce]}} = UserStore.get_user("get_user")
    end

    test "returns error for non-existent user" do
      assert {:error, :user_not_found} = UserStore.get_user("nobody")
    end
  end

  describe "list_users/0" do
    test "returns all users without password hashes" do
      UserStore.insert_user("list_a", "hash_a", [:produce])
      UserStore.insert_user("list_b", "hash_b", [:consume])

      users = UserStore.list_users()
      usernames = Enum.map(users, & &1.username)

      assert "list_a" in usernames
      assert "list_b" in usernames

      # Verify no password hashes leaked
      Enum.each(users, fn user ->
        assert Map.has_key?(user, :username)
        assert Map.has_key?(user, :permissions)
        refute Map.has_key?(user, :password_hash)
      end)
    end

    test "returns default users initially" do
      users = UserStore.list_users()
      assert length(users) >= 4
      usernames = Enum.map(users, & &1.username)
      assert "admin" in usernames
    end
  end

  describe "export_users/0" do
    test "exports users as serializable maps" do
      UserStore.insert_user("exp_user", "hash", [:admin])
      assert {:ok, users} = UserStore.export_users()

      exported = Enum.find(users, &(&1.username == "exp_user"))
      assert exported
      assert exported.permissions == ["admin"]
      assert is_integer(exported.created_at)
      assert is_integer(exported.updated_at)
    end
  end

  describe "import_users/1" do
    test "imports new users and skips existing" do
      UserStore.insert_user("existing", "hash", [:produce])

      import_data = [
        %{username: "imported1", password_hash: "hash1", permissions: [:consume]},
        %{username: "imported2", password_hash: "hash2", permissions: ["produce"]},
        %{username: "existing", password_hash: "new_hash", permissions: [:admin]}
      ]

      assert {:ok, %{imported: 2, skipped: 1}} = UserStore.import_users(import_data)

      # Verify imported users exist
      assert {:ok, {"imported1", "hash1", [:consume]}} = UserStore.get_user("imported1")
      assert {:ok, {"imported2", "hash2", [:produce]}} = UserStore.get_user("imported2")

      # Verify existing user was NOT overwritten
      assert {:ok, {"existing", "hash", [:produce]}} = UserStore.get_user("existing")
    end

    test "handles string-keyed maps" do
      import_data = [
        %{"username" => "str_user", "password_hash" => "hash", "permissions" => ["admin"]}
      ]

      assert {:ok, %{imported: 1, skipped: 0}} = UserStore.import_users(import_data)
      assert {:ok, {"str_user", "hash", [:admin]}} = UserStore.get_user("str_user")
    end

    test "skips entries with missing required fields" do
      import_data = [
        %{username: nil, password_hash: "hash", permissions: [:produce]},
        %{username: "good", password_hash: "hash", permissions: [:produce]}
      ]

      assert {:ok, %{imported: 1, skipped: 1}} = UserStore.import_users(import_data)
    end
  end

  describe "load_into_ets/0" do
    test "syncs Mnesia data to ETS cache" do
      UserStore.insert_user("sync_user", "hash", [:produce])

      # Clear ETS manually
      :ets.delete(@users_table, "sync_user")
      assert [] = :ets.lookup(@users_table, "sync_user")

      # Reload from Mnesia
      assert :ok = UserStore.load_into_ets()
      assert [{"sync_user", "hash", [:produce]}] = :ets.lookup(@users_table, "sync_user")
    end
  end

  describe "concurrent operations" do
    test "handles concurrent inserts safely" do
      tasks =
        for i <- 1..20 do
          Task.async(fn ->
            UserStore.insert_user("concurrent_#{i}", "hash_#{i}", [:produce])
          end)
        end

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))

      # All should be persisted
      users = UserStore.list_users()
      concurrent_users = Enum.filter(users, &String.starts_with?(&1.username, "concurrent_"))
      assert length(concurrent_users) == 20
    end
  end
end
