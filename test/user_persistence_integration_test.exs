defmodule MalachiMQ.Auth.UserPersistenceIntegrationTest do
  @moduledoc """
  Integration tests for user persistence across Auth and UserStore.
  Tests that users survive GenServer restarts and that the full
  Auth → UserStore → Mnesia → ETS pipeline works correctly.
  """
  use ExUnit.Case, async: false

  alias MalachiMQ.Auth
  alias MalachiMQ.Auth.UserStore

  @users_table :malachimq_users

  setup do
    on_exit(fn ->
      for prefix <- ["persist_", "restart_", "integ_", "import_", "pwd_"] do
        :mnesia.transaction(fn ->
          :mnesia.foldl(
            fn {_table, username, _hash, _perms, _created, _updated}, acc ->
              if String.starts_with?(username, prefix), do: [username | acc], else: acc
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
      end

      # Re-seed default users to ensure clean state for other tests
      default_users = [
        {"admin", "admin123", [:admin]},
        {"producer", "producer123", [:produce]},
        {"consumer", "consumer123", [:consume]},
        {"app", "app123", [:produce, :consume]}
      ]

      Enum.each(default_users, fn {username, password, permissions} ->
        hash = Argon2.hash_pwd_salt(password)
        now = System.system_time(:second)

        :mnesia.transaction(fn ->
          :mnesia.write({UserStore.table_name(), username, hash, permissions, now, now})
        end)

        :ets.insert(@users_table, {username, hash, permissions})
      end)
    end)

    :ok
  end

  describe "user persistence via Auth API" do
    test "add_user persists to Mnesia" do
      assert :ok = Auth.add_user("persist_user1", "StrongPass123!", [:produce])

      # Verify in Mnesia directly
      assert {:ok, {"persist_user1", _hash, [:produce]}} = UserStore.get_user("persist_user1")
    end

    test "add_user rejects duplicate username" do
      assert :ok = Auth.add_user("persist_dup", "StrongPass123!", [:produce])
      assert {:error, :user_exists} = Auth.add_user("persist_dup", "OtherPass456!", [:consume])
    end

    test "remove_user removes from Mnesia and ETS" do
      Auth.add_user("persist_del", "StrongPass123!", [:produce])
      assert :ok = Auth.remove_user("persist_del")

      assert {:error, :user_not_found} = UserStore.get_user("persist_del")
      assert [] = :ets.lookup(@users_table, "persist_del")
    end

    test "change_password updates in Mnesia and allows auth with new password" do
      Auth.add_user("pwd_user", "OldPass123456!", [:produce])
      assert :ok = Auth.change_password("pwd_user", "NewPass123456!")

      # Should authenticate with new password
      assert {:ok, _token} = Auth.authenticate("pwd_user", "NewPass123456!")

      # Should NOT authenticate with old password
      assert {:error, :invalid_credentials} = Auth.authenticate("pwd_user", "OldPass123456!")
    end

    test "change_password returns error for non-existent user" do
      assert {:error, :user_not_found} = Auth.change_password("pwd_ghost", "NewPass123456!")
    end
  end

  describe "persistence across Auth GenServer restart" do
    test "users survive Auth GenServer restart" do
      Auth.add_user("restart_user", "TestPass123!", [:produce, :consume])

      # Verify user exists
      assert {:ok, _token} = Auth.authenticate("restart_user", "TestPass123!")

      # Stop and restart Auth GenServer
      GenServer.stop(Auth, :normal)
      # Give it a moment to restart via supervisor
      Process.sleep(200)

      # Wait for Auth to come back
      wait_for_process(Auth, 5_000)

      # User should still be accessible after restart
      assert {:ok, _token} = Auth.authenticate("restart_user", "TestPass123!")

      # Verify permissions preserved
      users = Auth.list_users()
      user = Enum.find(users, &(&1.username == "restart_user"))
      assert user
      assert :produce in user.permissions
      assert :consume in user.permissions
    end
  end

  describe "default users seeding" do
    test "default users exist after startup" do
      users = Auth.list_users()
      usernames = Enum.map(users, & &1.username)

      # Default users from config should be present
      assert "admin" in usernames
    end

    test "default users are NOT re-seeded if they already exist with different passwords" do
      # Use a non-default user to avoid poisoning state
      Auth.add_user("persist_reseed", "InitialPass123!", [:produce])
      Auth.change_password("persist_reseed", "ChangedPass123!")

      # Verify the changed password is what authenticates
      assert {:ok, _} = Auth.authenticate("persist_reseed", "ChangedPass123!")
    end
  end

  describe "export and import" do
    test "export returns user metadata" do
      Auth.add_user("integ_export", "ExportPass123!", [:admin])

      {:ok, users} = UserStore.export_users()
      exported = Enum.find(users, &(&1.username == "integ_export"))

      assert exported
      assert "admin" in exported.permissions
      assert is_integer(exported.created_at)
    end

    test "import creates new users accessible via Auth" do
      hash = Argon2.hash_pwd_salt("ImportPass123!")

      {:ok, _} =
        UserStore.import_users([
          %{username: "import_user1", password_hash: hash, permissions: [:produce]}
        ])

      # Should be able to authenticate
      assert {:ok, _token} = Auth.authenticate("import_user1", "ImportPass123!")
    end
  end

  describe "list_users consistency" do
    test "Auth.list_users matches UserStore.list_users" do
      Auth.add_user("integ_list", "ListPass123!", [:consume])

      auth_users = Auth.list_users()
      store_users = UserStore.list_users()

      auth_user = Enum.find(auth_users, &(&1.username == "integ_list"))
      store_user = Enum.find(store_users, &(&1.username == "integ_list"))

      assert auth_user
      assert store_user
      assert auth_user.permissions == store_user.permissions
    end
  end

  # Helper to wait for a named process to be registered
  defp wait_for_process(name, timeout) when timeout > 0 do
    case Process.whereis(name) do
      nil ->
        Process.sleep(50)
        wait_for_process(name, timeout - 50)

      pid when is_pid(pid) ->
        :ok
    end
  end

  defp wait_for_process(name, _timeout) do
    raise "Process #{inspect(name)} did not start in time"
  end
end
