defmodule Mix.Tasks.Malachi.UserTest do
  use ExUnit.Case, async: false

  alias Malachi.Auth.UserStore
  alias Mix.Tasks.Malachi.User

  # A `call` seam that records the (module, fun, args) to the test process and returns a canned result.
  defp recording_call(result) do
    parent = self()

    fn module, fun, args ->
      send(parent, {:called, module, fun, args})
      result
    end
  end

  # A `call` seam that actually runs the function in-process (the real Auth) — proves the integration
  # end to end (only the cross-node RPC transport is skipped).
  defp local_call, do: fn module, fun, args -> {:ok, apply(module, fun, args)} end

  describe "execute/3 — parsing and dispatch" do
    test "create parses --perms into atoms, calls add_user, and reports success" do
      call = recording_call({:ok, :ok})
      assert {:ok, msg} = User.execute(["create", "alice", "pw"], [perms: "produce,consume"], call)
      assert msg =~ "created user alice"
      assert_received {:called, Malachi.Auth, :add_user, ["alice", "pw", [:produce, :consume]]}
    end

    test "create defaults to produce,consume when --perms is omitted" do
      call = recording_call({:ok, :ok})
      assert {:ok, _msg} = User.execute(["create", "bob", "pw"], [], call)
      assert_received {:called, Malachi.Auth, :add_user, ["bob", "pw", [:produce, :consume]]}
    end

    test "create with an unknown permission fails without calling add_user" do
      call = recording_call({:ok, :ok})
      assert {:error, msg} = User.execute(["create", "eve", "pw"], [perms: "superuser"], call)
      assert msg =~ "invalid permissions"
      refute_received {:called, _, _, _}
    end

    test "create surfaces an Auth error (e.g. :user_exists)" do
      call = recording_call({:ok, {:error, :user_exists}})
      assert {:error, "user_exists"} = User.execute(["create", "dup", "pw"], [], call)
    end

    test "passwd calls change_password; delete calls remove_user" do
      call = recording_call({:ok, :ok})

      assert {:ok, msg} = User.execute(["passwd", "alice", "new"], [], call)
      assert msg =~ "changed password for alice"
      assert_received {:called, Malachi.Auth, :change_password, ["alice", "new"]}

      assert {:ok, msg} = User.execute(["delete", "alice"], [], call)
      assert msg =~ "deleted user alice"
      assert_received {:called, Malachi.Auth, :remove_user, ["alice"]}
    end

    test "list formats the returned users" do
      call = recording_call({:ok, [%{username: "b", permissions: [:consume]}, %{username: "a", permissions: [:admin]}]})
      assert {:ok, msg} = User.execute(["list"], [], call)
      # sorted by username, permissions rendered, no hashes
      assert msg == "a\t[admin]\nb\t[consume]"
    end

    test "an RPC transport failure is reported, not crashed" do
      call = recording_call({:error, :nodedown})
      assert {:error, msg} = User.execute(["delete", "x"], [], call)
      assert msg =~ "rpc failed"
      assert msg =~ "nodedown"
    end

    test "an unknown command returns usage" do
      assert {:error, msg} = User.execute(["bogus"], [], recording_call({:ok, :ok}))
      assert msg =~ "usage:"
    end
  end

  describe "execute/3 — integration against the real Auth (local seam)" do
    test "create then delete actually round-trips through the replicated user store" do
      username = "mixtask_#{System.unique_integer([:positive])}"
      on_exit(fn -> Malachi.Auth.remove_user(username) end)

      assert {:ok, _} = User.execute(["create", username, "Mix-Pass-1"], [perms: "produce"], local_call())
      assert {:ok, {^username, _hash, [:produce]}} = UserStore.get_user(username)

      assert {:ok, _} = User.execute(["delete", username], [], local_call())
      assert {:error, :user_not_found} = UserStore.get_user(username)
    end
  end
end
