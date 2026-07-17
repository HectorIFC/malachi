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
               %{username: "admin", permissions: [:admin]},
               %{username: "app", permissions: [:produce, :consume]}
             ]

      refute Enum.any?(users, &Map.has_key?(&1, :hash))
    end

    test "export_users renders permissions as strings with timestamps, no hashes" do
      {state, :ok} = put(Reg.new(), "app", "secret", [:produce, :consume], 1000)

      assert [%{username: "app", permissions: ["produce", "consume"], created_at: 1000, updated_at: 1000}] =
               Reg.export_users(state)
    end
  end

  describe "replication safety" do
    test "the same command log at the same `now` yields the same state (deterministic)" do
      log = [
        {{:put_user, "a", "ha", [:admin]}, 1000},
        {{:put_user, "b", "hb", [:produce]}, 1001},
        {{:update_password, "a", "ha2"}, 1002},
        {{:delete_user, "b"}, 1003},
        {{:import_users, [{"c", "hc", [:consume]}]}, 1004}
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
