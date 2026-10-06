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

  # A `call` seam that actually runs the function in-process (the real Auth), proves the integration
  # end to end (only the cross-node RPC transport is skipped).
  defp local_call, do: fn module, fun, args -> {:ok, apply(module, fun, args)} end

  describe "execute/3: parsing and dispatch" do
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

    test "create with --role passes the role, and an empty --perms asks for no wire permission" do
      call = recording_call({:ok, :ok})
      assert {:ok, msg} = User.execute(["create", "ops", "pw"], [perms: "", role: "viewer"], call)
      assert msg =~ "with console role viewer"
      assert_received {:called, Malachi.Auth, :add_user, ["ops", "pw", [], :viewer]}
    end

    test "create with an unknown role fails without calling add_user" do
      assert {:error, msg} = User.execute(["create", "ops", "pw"], [role: "root"], recording_call({:ok, :ok}))
      assert msg =~ "invalid console role"
      refute_received {:called, _, _, _}
    end

    test "role sets and removes a console role, naming the task as the actor" do
      call = recording_call({:ok, :ok})

      assert {:ok, msg} = User.execute(["role", "alice", "editor"], [], call)
      assert msg =~ "set console role of alice to editor"
      assert_received {:called, Malachi.Auth, :set_role, ["alice", :editor, "mix malachi.user"]}

      assert {:ok, _msg} = User.execute(["role", "alice", "none"], [], call)
      assert_received {:called, Malachi.Auth, :set_role, ["alice", nil, "mix malachi.user"]}

      assert {:error, msg} = User.execute(["role", "alice", "root"], [], call)
      assert msg =~ "invalid console role"
    end

    test "a cluster still below the roles' machine version is told what to finish" do
      call = recording_call({:ok, {:error, {:unsupported_command, {:set_role, 3}, 5, 4}}})
      assert {:error, msg} = User.execute(["role", "alice", "viewer"], [], call)
      assert msg =~ "machine version 4 and this needs 5"
    end

    test "a tuple reason is shown, not crashed on" do
      call = recording_call({:ok, {:error, {:odd, 1}}})
      assert {:error, "{:odd, 1}"} = User.execute(["role", "alice", "viewer"], [], call)
    end

    test "list shows a console role when there is one" do
      users = [%{username: "b", permissions: [], role: :viewer}, %{username: "a", permissions: [:admin], role: nil}]
      assert {:ok, "a\t[admin]\nb\t[]\trole: viewer"} = User.execute(["list"], [], recording_call({:ok, users}))
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

  describe "execute/3: integration against the real Auth (local seam)" do
    test "create then delete actually round-trips through the replicated user store" do
      username = "mixtask_#{System.unique_integer([:positive])}"
      on_exit(fn -> Malachi.Auth.remove_user(username) end)

      assert {:ok, _} = User.execute(["create", username, "Mix-Pass-1"], [perms: "produce"], local_call())
      assert {:ok, {^username, _hash, [:produce]}} = UserStore.get_user(username)

      assert {:ok, _} = User.execute(["delete", username], [], local_call())
      assert {:error, :user_not_found} = UserStore.get_user(username)
    end

    test "create with a role, change it and list it through the real store" do
      username = "mixtask_role_#{System.unique_integer([:positive])}"
      on_exit(fn -> Malachi.Auth.remove_user(username) end)

      assert {:ok, _} = User.execute(["create", username, "Mix-Pass-1"], [perms: "", role: "viewer"], local_call())
      assert {:ok, %{role: :viewer, permissions: []}} = UserStore.get_principal(username)

      assert {:ok, _} = User.execute(["role", username, "admin"], [], local_call())
      assert {:ok, %{role: :admin}} = UserStore.get_principal(username)

      assert {:ok, listing} = User.execute(["list"], [], local_call())
      assert listing =~ "#{username}\t[]\trole: admin"

      assert {:error, "user_not_found"} = User.execute(["role", username <> "_x", "admin"], [], local_call())
    end
  end

  describe "run/1 against a live node" do
    # The suite's own VM is a named, running Malachi node, so pointing the task at it exercises the path the
    # seam tests skip: resolve the node, connect, RPC, and print.
    setup do
      shell = Mix.shell()
      Mix.shell(Mix.Shell.Process)
      on_exit(fn -> Mix.shell(shell) end)
      %{target: to_string(node())}
    end

    test "role reaches the node, sets the role and prints the answer", %{target: target} do
      username = "mixtask_live_#{System.unique_integer([:positive])}"
      on_exit(fn -> Malachi.Auth.remove_user(username) end)
      :ok = Malachi.Auth.add_user(username, "Mix-Pass-1", [])

      User.run(["role", username, "editor", "--node", target])

      assert_received {:mix_shell, :info, [message]}
      assert message =~ "set console role of #{username} to editor"
      assert {:ok, %{role: :editor}} = UserStore.get_principal(username)
    end

    test "a refusal is printed on the error channel and exits non-zero", %{target: target} do
      assert catch_exit(User.run(["role", "mixtask_nobody", "viewer", "--node", target])) == {:shutdown, 1}

      assert_received {:mix_shell, :error, [message]}
      assert message == "user_not_found"
    end
  end

  test "an unknown option aborts with usage and never resolves or connects to a node" do
    # `--nod` lands in OptionParser's invalid list and is absent from opts, so falling through would
    # target $MALACHI_NODE (or the default) as though it had been asked for. For a mistyped `--node`
    # that means the command succeeds against a different cluster and reports the answer as yours.
    assert_raise Mix.Error, ~r/unknown option\(s\): --nod.*usage:/s, fn ->
      User.run(["--nod", "malachi@somewhere"])
    end
  end
end
