defmodule Malachi.AclEnforcementTest do
  # async: false. Toggles global :acl_strict and shares the running acceptor.
  use ExUnit.Case, async: false

  alias Malachi.Auth
  alias Malachi.Test.TCPHelper
  alias Malachi.Wire

  @moduletag :security

  setup do
    prior = Application.get_env(:malachi, :acl_strict)
    on_exit(fn -> restore(:acl_strict, prior) end)
    :ok
  end

  defp restore(key, nil), do: Application.delete_env(:malachi, key)
  defp restore(key, value), do: Application.put_env(:malachi, key, value)

  # Creates a user with `perms`, authenticates a wire connection, and returns `{username, socket}`.
  defp connect_as(perms) do
    username = "acl_#{System.unique_integer([:positive])}"
    password = "Acl-Pass-1!"
    Auth.add_user(username, password, perms)
    on_exit(fn -> Auth.remove_user(username) end)

    {:ok, socket} = TCPHelper.connect()
    {:ok, _token} = TCPHelper.authenticate_wire(socket, username, password)
    {username, socket}
  end

  # create_topic is gated by :produce on the topic; returns :ok or {:error, reason_string}.
  defp create_topic(socket, topic) do
    reply(TCPHelper.request(socket, Wire.create_topic_key(), 1, Wire.encode_create_topic_req(topic, 8)))
  end

  # fetch is gated by :consume on the topic; returns :ok or {:error, reason_string}.
  defp fetch(socket, topic) do
    payload = Wire.encode_fetch_req(topic, nil, nil, nil, 100, 0)
    reply(TCPHelper.request(socket, Wire.fetch_key(), 1, payload))
  end

  defp reply({code, payload}) do
    if code == Wire.ok_code(), do: :ok, else: {:error, Wire.decode_auth_resp(payload)}
  end

  describe "non-strict (default), backward compatible" do
    test "a global :produce permission still creates any topic without an ACL" do
      Application.put_env(:malachi, :acl_strict, false)
      {_user, socket} = connect_as([:produce, :consume])
      assert :ok = create_topic(socket, "acl_topic_#{System.unique_integer([:positive])}")
    end
  end

  describe "strict mode, deny by default" do
    test "a :produce user cannot create a topic without an ACL grant" do
      Application.put_env(:malachi, :acl_strict, true)
      {_user, socket} = connect_as([:produce, :consume])
      assert {:error, "permission_denied"} = create_topic(socket, "acl_topic_#{System.unique_integer([:positive])}")
    end

    test "granting a matching ACL lets the produce through" do
      Application.put_env(:malachi, :acl_strict, true)
      {username, socket} = connect_as([:produce])
      topic = "orders_#{System.unique_integer([:positive])}"

      assert {:error, "permission_denied"} = create_topic(socket, topic)
      assert :ok = Auth.grant_acl(username, :produce, topic)
      assert :ok = create_topic(socket, topic)
    end

    test "a prefix grant authorizes every topic under the prefix" do
      Application.put_env(:malachi, :acl_strict, true)
      {username, socket} = connect_as([:produce])
      prefix = "team_#{System.unique_integer([:positive])}_"

      assert :ok = Auth.grant_acl(username, :produce, prefix <> "*")
      assert :ok = create_topic(socket, prefix <> "a")
      assert :ok = create_topic(socket, prefix <> "b")
    end

    test "a consume ACL gates fetch independently of produce" do
      Application.put_env(:malachi, :acl_strict, true)
      {username, socket} = connect_as([:produce, :consume])
      topic = "feed_#{System.unique_integer([:positive])}"

      # no consume grant yet
      assert {:error, "permission_denied"} = fetch(socket, topic)
      assert :ok = Auth.grant_acl(username, :consume, topic)
      assert :ok = fetch(socket, topic)
    end

    test "admin bypasses ACLs entirely" do
      Application.put_env(:malachi, :acl_strict, true)
      {_user, socket} = connect_as([:admin])
      assert :ok = create_topic(socket, "admin_topic_#{System.unique_integer([:positive])}")
    end
  end

  describe "Auth ACL management API" do
    test "grant/list/revoke round-trip; invalid inputs rejected" do
      username = "acladm_#{System.unique_integer([:positive])}"
      Auth.add_user(username, "Acl-Pass-1!", [:produce])
      on_exit(fn -> Auth.remove_user(username) end)

      assert :ok = Auth.grant_acl(username, :produce, "orders.*")
      assert [%{operation: :produce, resource: "orders.*"}] = Auth.list_acls(username)

      assert {:error, :invalid_acl} = Auth.grant_acl(username, :admin, "x")
      assert {:error, :invalid_acl} = Auth.grant_acl(username, :produce, :not_a_string)

      assert :ok = Auth.revoke_acl(username, :produce, "orders.*")
      assert [] = Auth.list_acls(username)
    end

    test "removing a user revokes its ACL grants" do
      username = "acldel_#{System.unique_integer([:positive])}"
      Auth.add_user(username, "Acl-Pass-1!", [:produce])
      assert :ok = Auth.grant_acl(username, :produce, "orders.*")

      :ok = Auth.remove_user(username)
      assert [] = Auth.list_acls(username)
    end
  end
end
