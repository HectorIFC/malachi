defmodule Malachi.Auth.UserRegistryTest do
  use ExUnit.Case, async: true

  alias Malachi.Auth.UserRegistry, as: Reg

  defp put(state, username, hash, perms, now \\ 1000) do
    {state, reply} = Reg.apply(state, {:put_user, username, hash, perms}, now)
    {state, reply}
  end

  describe "put_user" do
    test "inserts a new user and stamps created_at/updated_at with `now`" do
      {state, reply} = put(Reg.new(), "admin", "h1", [:admin], 1000)
      assert reply == :ok
      assert Reg.get_user(state, "admin") == {:ok, {"admin", "h1", [:admin]}}
      assert [%{created_at: 1000, updated_at: 1000}] = Reg.export_users(state)
    end

    test "rejects a duplicate username, leaving the existing record untouched" do
      {state, :ok} = put(Reg.new(), "admin", "h1", [:admin], 1000)
      {state, reply} = put(state, "admin", "h2", [:produce], 2000)

      assert reply == {:error, :user_exists}
      # the original hash/permissions survive
      assert Reg.get_user(state, "admin") == {:ok, {"admin", "h1", [:admin]}}
    end
  end

  describe "delete_user" do
    test "removes a user and is idempotent on an absent one" do
      {state, :ok} = put(Reg.new(), "admin", "h1", [:admin])
      {state, reply} = Reg.apply(state, {:delete_user, "admin"}, 1)
      assert reply == :ok
      assert Reg.get_user(state, "admin") == {:error, :user_not_found}

      {state2, reply2} = Reg.apply(state, {:delete_user, "ghost"}, 1)
      assert reply2 == :ok
      assert state2 == state
    end
  end

  describe "update_password" do
    test "updates the hash and updated_at, keeping created_at and permissions" do
      {state, :ok} = put(Reg.new(), "admin", "h1", [:admin], 1000)
      {state, reply} = Reg.apply(state, {:update_password, "admin", "h2"}, 2000)

      assert reply == :ok
      assert Reg.get_user(state, "admin") == {:ok, {"admin", "h2", [:admin]}}
      assert [%{created_at: 1000, updated_at: 2000}] = Reg.export_users(state)
    end

    test "returns :user_not_found for a missing user" do
      {state, reply} = Reg.apply(Reg.new(), {:update_password, "ghost", "h"}, 1)
      assert reply == {:error, :user_not_found}
      assert state == Reg.new()
    end
  end

  describe "import_users" do
    test "imports new users, skips existing ones, and counts both" do
      {state, :ok} = put(Reg.new(), "admin", "h1", [:admin])

      to_import = [
        {"admin", "other", [:admin]},
        {"producer", "hp", [:produce]},
        {"consumer", "hc", [:consume]}
      ]

      {state, reply} = Reg.apply(state, {:import_users, to_import}, 5000)

      assert reply == {:ok, %{imported: 2, skipped: 1}}
      # existing user not overwritten
      assert Reg.get_user(state, "admin") == {:ok, {"admin", "h1", [:admin]}}
      assert Reg.get_user(state, "producer") == {:ok, {"producer", "hp", [:produce]}}
    end

    test "skips malformed entries (non-binary username or hash) without crashing" do
      {state, reply} = Reg.apply(Reg.new(), {:import_users, [{nil, "h", [:admin]}, {"ok", "h", [:consume]}]}, 1)
      assert reply == {:ok, %{imported: 1, skipped: 1}}
      assert Reg.get_user(state, "ok") == {:ok, {"ok", "h", [:consume]}}
    end
  end

  describe "queries" do
    test "list_users returns usernames and permissions but never hashes" do
      {state, :ok} = put(Reg.new(), "admin", "secret", [:admin])
      {state, :ok} = put(state, "app", "secret2", [:produce, :consume])

      users = Reg.list_users(state) |> Enum.sort_by(& &1.username)

      assert users == [
               %{username: "admin", permissions: [:admin], role: nil},
               %{username: "app", permissions: [:produce, :consume], role: nil}
             ]

      refute Enum.any?(users, &Map.has_key?(&1, :hash))
    end

    test "export_users renders permissions as strings with timestamps, no hashes" do
      {state, :ok} = put(Reg.new(), "app", "secret", [:produce, :consume], 1000)

      assert [
               %{
                 username: "app",
                 permissions: ["produce", "consume"],
                 role: nil,
                 created_at: 1000,
                 updated_at: 1000
               }
             ] = Reg.export_users(state)
    end

    test "get_principal returns permissions and role, never the hash" do
      {state, :ok} = Reg.apply(Reg.new(), {:put_user, "ops", "secret", [], :viewer}, 1)

      assert Reg.get_principal(state, "ops") == {:ok, %{username: "ops", permissions: [], role: :viewer}}
      assert Reg.get_principal(state, "ghost") == {:error, :user_not_found}
    end
  end

  describe "console roles (machine version 5)" do
    test "the three shapes are introduced at version 5, the older ones stay at 0" do
      versions = Reg.command_versions()

      assert versions[{:set_role, 3}] == 5
      assert versions[{:put_user, 5}] == 5
      assert versions[{:import_users_with_roles, 2}] == 5
      assert versions[{:put_user, 4}] == 0
      assert versions[{:import_users, 2}] == 0
    end

    test "put_user/5 stores the role; put_user/4 stores none" do
      {state, :ok} = Reg.apply(Reg.new(), {:put_user, "ed", "h", [:produce], :editor}, 1)
      {state, :ok} = put(state, "app", "h", [:produce])

      assert {:ok, %{role: :editor}} = Reg.get_principal(state, "ed")
      assert {:ok, %{role: nil}} = Reg.get_principal(state, "app")
      assert Reg.get_user(state, "ed") == {:ok, {"ed", "h", [:produce]}}
    end

    test "put_user/5 refuses a duplicate and an unknown role, changing nothing" do
      {state, :ok} = put(Reg.new(), "app", "h", [:produce])

      assert {^state, {:error, :user_exists}} = Reg.apply(state, {:put_user, "app", "h2", [], :viewer}, 2)
      assert {^state, {:error, :invalid_role}} = Reg.apply(state, {:put_user, "new", "h", [], :root}, 2)
      assert {^state, {:error, :invalid_role}} = Reg.apply(state, {:put_user, "new", "h", [], "viewer"}, 2)
    end

    test "set_role changes the role and updated_at, and nil removes it" do
      {state, :ok} = put(Reg.new(), "app", "h", [:produce], 1000)

      {state, :ok} = Reg.apply(state, {:set_role, "app", :viewer}, 2000)
      assert {:ok, %{role: :viewer, permissions: [:produce]}} = Reg.get_principal(state, "app")
      assert [%{created_at: 1000, updated_at: 2000, role: "viewer"}] = Reg.export_users(state)

      {state, :ok} = Reg.apply(state, {:set_role, "app", nil}, 3000)
      assert {:ok, %{role: nil}} = Reg.get_principal(state, "app")
    end

    test "set_role refuses an unknown user and an unknown role, changing nothing" do
      {state, :ok} = put(Reg.new(), "app", "h", [:produce])

      assert {^state, {:error, :user_not_found}} = Reg.apply(state, {:set_role, "ghost", :viewer}, 2)
      assert {^state, {:error, :invalid_role}} = Reg.apply(state, {:set_role, "app", :superuser}, 2)
    end

    test "import_users_with_roles imports roles, skips existing and malformed entries" do
      {state, :ok} = put(Reg.new(), "admin", "h1", [:admin])

      entries = [
        {"admin", "other", [:admin], :viewer},
        {"ops", "ho", [], :viewer},
        {"app", "ha", [:produce], nil},
        {"bad", "hb", [], :root},
        {nil, "hn", [], :viewer},
        {"short", "hs", []}
      ]

      {state, reply} = Reg.apply(state, {:import_users_with_roles, entries}, 5)

      assert reply == {:ok, %{imported: 2, skipped: 4}}
      assert {:ok, %{role: nil}} = Reg.get_principal(state, "admin")
      assert {:ok, %{role: :viewer, permissions: []}} = Reg.get_principal(state, "ops")
      assert {:ok, %{role: nil, permissions: [:produce]}} = Reg.get_principal(state, "app")
      assert Reg.get_principal(state, "bad") == {:error, :user_not_found}
    end

    test "a record written before version 5 reads as having no role" do
      # The state an older release left behind: a user map with no :role key at all.
      legacy = %Reg{users: %{"old" => %{hash: "h", permissions: [:consume], created_at: 1, updated_at: 1}}}

      assert Reg.get_principal(legacy, "old") == {:ok, %{username: "old", permissions: [:consume], role: nil}}
      assert [%{role: nil}] = Reg.list_users(legacy)
      assert [%{role: nil}] = Reg.export_users(legacy)

      {state, :ok} = Reg.apply(legacy, {:set_role, "old", :admin}, 2)
      assert {:ok, %{role: :admin}} = Reg.get_principal(state, "old")
    end
  end

  describe "replication safety" do
    test "the same command log at the same `now` yields the same state (deterministic)" do
      log = [
        {{:put_user, "a", "ha", [:admin]}, 1000},
        {{:put_user, "b", "hb", [:produce]}, 1001},
        {{:update_password, "a", "ha2"}, 1002},
        {{:delete_user, "b"}, 1003},
        {{:import_users, [{"c", "hc", [:consume]}]}, 1004},
        {{:put_user, "d", "hd", [], :viewer}, 1005},
        {{:set_role, "c", :editor}, 1006},
        {{:import_users_with_roles, [{"e", "he", [:produce], :admin}]}, 1007}
      ]

      replay = fn -> Enum.reduce(log, Reg.new(), fn {cmd, now}, st -> elem(Reg.apply(st, cmd, now), 0) end) end
      assert replay.() == replay.()
    end

    test "an unknown command is a no-op error, never a crash" do
      state = Reg.new()
      assert {^state, {:error, :unknown_command}} = Reg.apply(state, {:bogus, "x"}, 1)
    end
  end
end
