defmodule Malachi.Auth.AclRegistryTest do
  use ExUnit.Case, async: true

  alias Malachi.Auth.AclRegistry, as: Reg

  defp grant(state, username, op, resource), do: elem(Reg.apply(state, {:grant, username, op, resource}), 0)

  describe "grant / authorized?, literal" do
    test "a literal grant authorizes exactly that topic and no other" do
      state = grant(Reg.new(), "alice", :produce, {:literal, "orders.eu"})
      assert Reg.authorized?(state, "alice", :produce, "orders.eu")
      refute Reg.authorized?(state, "alice", :produce, "orders.us")
      # wrong operation
      refute Reg.authorized?(state, "alice", :consume, "orders.eu")
      # wrong user
      refute Reg.authorized?(state, "bob", :produce, "orders.eu")
    end
  end

  describe "grant / authorized?, prefix" do
    test "a prefix grant authorizes any topic under the prefix" do
      state = grant(Reg.new(), "alice", :consume, {:prefix, "orders."})
      assert Reg.authorized?(state, "alice", :consume, "orders.eu")
      assert Reg.authorized?(state, "alice", :consume, "orders.us.west")
      # the prefix boundary is a plain string prefix
      refute Reg.authorized?(state, "alice", :consume, "order")
      refute Reg.authorized?(state, "alice", :consume, "payments.eu")
    end

    test "an empty-prefix grant (from *) authorizes every topic" do
      state = grant(Reg.new(), "svc", :produce, {:prefix, ""})
      assert Reg.authorized?(state, "svc", :produce, "anything")
      assert Reg.authorized?(state, "svc", :produce, "")
    end
  end

  describe "revoke" do
    test "revoke removes a single grant, leaving others; empties the user when last is gone" do
      state =
        Reg.new()
        |> grant("alice", :produce, {:literal, "a"})
        |> grant("alice", :consume, {:prefix, "b."})

      state = elem(Reg.apply(state, {:revoke, "alice", :produce, {:literal, "a"}}), 0)
      refute Reg.authorized?(state, "alice", :produce, "a")
      assert Reg.authorized?(state, "alice", :consume, "b.1")

      state = elem(Reg.apply(state, {:revoke, "alice", :consume, {:prefix, "b."}}), 0)
      assert Reg.list_grants(state, "alice") == []
      # the user key is dropped entirely
      refute Map.has_key?(state.grants, "alice")
    end

    test "revoke of an absent grant is a no-op" do
      {state, reply} = Reg.apply(Reg.new(), {:revoke, "ghost", :produce, {:literal, "x"}})
      assert reply == :ok
      assert state == Reg.new()
    end

    test "revoke_user drops every grant for the user only" do
      state =
        Reg.new()
        |> grant("alice", :produce, {:literal, "a"})
        |> grant("bob", :consume, {:literal, "b"})

      state = elem(Reg.apply(state, {:revoke_user, "alice"}), 0)
      assert Reg.list_grants(state, "alice") == []
      assert Reg.authorized?(state, "bob", :consume, "b")
    end
  end

  describe "grant idempotency and listing" do
    test "granting the same grant twice is idempotent" do
      state = Reg.new() |> grant("alice", :produce, {:literal, "a"}) |> grant("alice", :produce, {:literal, "a"})
      assert Reg.list_grants(state, "alice") == [{:produce, {:literal, "a"}}]
    end

    test "list_all returns every grant across users" do
      state = Reg.new() |> grant("alice", :produce, {:literal, "a"}) |> grant("bob", :consume, {:prefix, "b."})

      assert Enum.sort(Reg.list_all(state)) == [
               {"alice", :produce, {:literal, "a"}},
               {"bob", :consume, {:prefix, "b."}}
             ]
    end
  end

  describe "parse_resource / render_resource" do
    test "a trailing * is a prefix, otherwise a literal; round-trips" do
      assert Reg.parse_resource("orders.*") == {:prefix, "orders."}
      assert Reg.parse_resource("*") == {:prefix, ""}
      assert Reg.parse_resource("orders.eu") == {:literal, "orders.eu"}

      assert Reg.render_resource({:prefix, "orders."}) == "orders.*"
      assert Reg.render_resource({:literal, "orders.eu"}) == "orders.eu"
    end
  end

  describe "replication safety" do
    test "the same command log yields the same state (deterministic)" do
      log = [
        {:grant, "a", :produce, {:literal, "t1"}},
        {:grant, "a", :consume, {:prefix, "t."}},
        {:grant, "b", :produce, {:literal, "t2"}},
        {:revoke, "a", :produce, {:literal, "t1"}},
        {:revoke_user, "b"}
      ]

      replay = fn -> Enum.reduce(log, Reg.new(), fn cmd, st -> elem(Reg.apply(st, cmd), 0) end) end
      assert replay.() == replay.()
    end

    test "an unknown command is a no-op error, never a crash" do
      state = Reg.new()
      assert {^state, {:error, :unknown_command}} = Reg.apply(state, {:bogus, "x"})
    end
  end
end
