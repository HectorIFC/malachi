defmodule Mix.Tasks.Malachi.AclTest do
  use ExUnit.Case, async: false

  alias Mix.Tasks.Malachi.Acl

  # A `call` seam that records the (module, fun, args) to the test process and returns a canned result.
  defp recording_call(result) do
    parent = self()

    fn module, fun, args ->
      send(parent, {:called, module, fun, args})
      result
    end
  end

  # A `call` seam that actually runs the function in-process (the real Auth), proves the integration end to
  # end (only the cross-node RPC transport is skipped).
  defp local_call, do: fn module, fun, args -> {:ok, apply(module, fun, args)} end

  describe "execute/3: parsing and dispatch" do
    test "grant parses the operation to an atom, calls grant_acl, and reports success" do
      call = recording_call({:ok, :ok})
      assert {:ok, msg} = Acl.execute(["grant", "alice", "produce", "orders.*"], [], call)
      assert msg =~ "granted produce on orders.*"
      assert_received {:called, Malachi.Auth, :grant_acl, ["alice", :produce, "orders.*"]}
    end

    test "revoke calls revoke_acl with the parsed operation" do
      call = recording_call({:ok, :ok})
      assert {:ok, msg} = Acl.execute(["revoke", "alice", "consume", "orders.eu"], [], call)
      assert msg =~ "revoked consume on orders.eu"
      assert_received {:called, Malachi.Auth, :revoke_acl, ["alice", :consume, "orders.eu"]}
    end

    test "an invalid operation fails without calling the seam" do
      call = recording_call({:ok, :ok})
      assert {:error, msg} = Acl.execute(["grant", "alice", "superuser", "t.*"], [], call)
      assert msg =~ "invalid operation"
      refute_received {:called, _, _, _}
    end

    test "list formats the returned acls" do
      acls = [%{operation: :produce, resource: "b.*"}, %{operation: :consume, resource: "a"}]
      call = recording_call({:ok, acls})
      assert {:ok, msg} = Acl.execute(["list", "alice"], [], call)
      # sorted by (operation, resource)
      assert msg == "consume\ta\nproduce\tb.*"
    end

    test "list of an empty acl set renders a placeholder" do
      call = recording_call({:ok, []})
      assert {:ok, "(no acls)"} = Acl.execute(["list", "alice"], [], call)
    end

    test "an RPC transport failure is reported, not crashed" do
      call = recording_call({:error, :nodedown})
      assert {:error, msg} = Acl.execute(["list", "x"], [], call)
      assert msg =~ "rpc failed"
      assert msg =~ "nodedown"
    end

    test "an unknown command returns usage" do
      assert {:error, msg} = Acl.execute(["bogus"], [], recording_call({:ok, :ok}))
      assert msg =~ "usage:"
    end
  end

  describe "execute/3: integration against the real Auth (local seam)" do
    test "grant then revoke actually round-trips through the replicated ACL store" do
      username = "acltask_#{System.unique_integer([:positive])}"

      assert {:ok, _} = Acl.execute(["grant", username, "produce", "orders.*"], [], local_call())
      assert [%{operation: :produce, resource: "orders.*"}] = Malachi.Auth.list_acls(username)

      assert {:ok, _} = Acl.execute(["revoke", username, "produce", "orders.*"], [], local_call())
      assert [] = Malachi.Auth.list_acls(username)
    end
  end
end
