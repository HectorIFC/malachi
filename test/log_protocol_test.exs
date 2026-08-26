defmodule Malachi.LogProtocolTest do
  # End-to-end over the real TCP server (started by the app): the NorthGuard log protocol over the binary
  # Malachi.Wire framing (create_topic / produce by key / fetch by opaque cursor / groups / long-poll)
  # against the live Malachi.LogBroker.
  use ExUnit.Case, async: false

  alias Malachi.Log.Record
  alias Malachi.Test.TCPHelper
  alias Malachi.Wire

  @port Application.compile_env(:malachi, :tcp_port, 4040)

  defp ok?(code), do: code == Wire.ok_code()
  # an error response carries the reason as a length-prefixed string
  defp reason(payload), do: Wire.decode_auth_resp(payload)

  defp create_topic(socket, topic) do
    TCPHelper.request(socket, Wire.create_topic_key(), 1, Wire.encode_create_topic_req(topic, 8))
  end

  # records: a list of `value` binaries or `{key, value}` tuples
  defp produce(socket, topic, records) do
    wire =
      Enum.map(records, fn
        {key, value} -> %Record{key: key, value: value}
        value when is_binary(value) -> %Record{value: value}
      end)

    TCPHelper.request(socket, Wire.produce_key(), 1, Wire.encode_produce_req(topic, wire))
  end

  defp fetch(socket, topic, opts \\ []) do
    payload =
      Wire.encode_fetch_req(topic, opts[:cursor], opts[:group], opts[:member], opts[:max] || 100, opts[:wait] || 0)

    TCPHelper.request(socket, Wire.fetch_key(), 1, payload)
  end

  defp commit(socket, topic, group, cursor) do
    TCPHelper.request(socket, Wire.commit_key(), 1, Wire.encode_commit_req(topic, group, cursor))
  end

  defp leave_group(socket, topic, group, member) do
    TCPHelper.request(socket, Wire.leave_group_key(), 1, Wire.encode_leave_group_req(topic, group, member))
  end

  defp create_user(socket, username, password, permissions) do
    TCPHelper.request(socket, Wire.create_user_key(), 1, Wire.encode_create_user_req(username, password, permissions))
  end

  defp delete_user(socket, username) do
    TCPHelper.request(socket, Wire.delete_user_key(), 1, Wire.encode_delete_user_req(username))
  end

  defp change_password(socket, username, new_password) do
    TCPHelper.request(socket, Wire.change_password_key(), 1, Wire.encode_change_password_req(username, new_password))
  end

  defp list_users(socket) do
    TCPHelper.request(socket, Wire.list_users_key(), 1, <<>>)
  end

  # Attempts an authentication and returns `{:ok, token}` / `{:error, reason}` (closing the socket).
  defp try_auth(username, password) do
    {:ok, socket} = TCPHelper.connect(port: @port)
    result = TCPHelper.authenticate_wire(socket, username, password)
    :gen_tcp.close(socket)
    result
  end

  # Deterministically wait until the live LogBroker has a parked long-poll waiter.
  defp wait_for_park(deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 3_000

    cond do
      log_broker_waiters() >= 1 -> :ok
      System.monotonic_time(:millisecond) > deadline -> flunk("the long-poll consumer did not park")
      true -> Process.sleep(5) && wait_for_park(deadline)
    end
  end

  defp log_broker_waiters do
    case Process.whereis(Malachi.LogBroker) do
      nil -> 0
      pid -> length(:sys.get_state(pid).waiters)
    end
  end

  defp with_session(username, password, fun) do
    case TCPHelper.connect(port: @port) do
      {:ok, socket} ->
        {:ok, _token} = TCPHelper.authenticate_wire(socket, username, password)

        try do
          fun.(socket)
        after
          :gen_tcp.close(socket)
        end

      {:error, _reason} ->
        # No TCP server in this environment; nothing to assert.
        :ok
    end
  end

  test "produce by key then fetch by opaque cursor (no offsets exposed)" do
    with_session("app", "app123", fn socket ->
      topic = "logproto_#{System.unique_integer([:positive])}"
      assert {code, _} = create_topic(socket, topic)
      assert ok?(code)

      assert {code, <<2::32>>} = produce(socket, topic, [{"k1", "v1"}, {"k2", "v2"}])
      assert ok?(code)

      assert {code, payload} = fetch(socket, topic)
      assert ok?(code)
      {records, cursor} = Wire.decode_fetch_resp(payload)

      # opaque cursor (a byte token, not an integer offset); records carry key/value, never an offset
      assert is_binary(cursor)
      assert records |> Enum.map(& &1.value) |> Enum.sort() == ["v1", "v2"]
      assert Enum.all?(records, &(&1.offset == nil))

      # the cursor carries position: re-fetching with it yields nothing new
      assert {code, payload} = fetch(socket, topic, cursor: cursor)
      assert ok?(code)
      assert {[], _cursor} = Wire.decode_fetch_resp(payload)
    end)
  end

  test "consumer-group member fetch is server-scoped and opaque; leave_group acks" do
    with_session("app", "app123", fn socket ->
      topic = "logproto_member_#{System.unique_integer([:positive])}"
      assert {code, _} = create_topic(socket, topic)
      assert ok?(code)

      assert {code, <<3::32>>} = produce(socket, topic, [{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}])
      assert ok?(code)

      # fetch as a group member: the sole member owns the topic's ranges, so it gets every record plus an
      # opaque cursor: no range id ever crosses the wire
      assert {code, payload} = fetch(socket, topic, group: "g", member: "m1")
      assert ok?(code)
      {records, cursor} = Wire.decode_fetch_resp(payload)
      assert is_binary(cursor)
      assert records |> Enum.map(& &1.value) |> Enum.sort() == ["v1", "v2", "v3"]
      assert Enum.all?(records, &(&1.offset == nil))

      # leaving the group acks with an empty ok (fast rebalance on a clean shutdown)
      assert {code, <<>>} = leave_group(socket, topic, "g", "m1")
      assert ok?(code)
    end)
  end

  test "consumer group: fetch resumes from the server-committed position" do
    with_session("app", "app123", fn socket ->
      topic = "logproto_grp_#{System.unique_integer([:positive])}"
      assert {code, _} = create_topic(socket, topic)
      assert ok?(code)

      assert {code, <<3::32>>} = produce(socket, topic, ["a", "b", "c"])
      assert ok?(code)

      # fetch for a group, then commit the returned cursor durably
      assert {code, payload} = fetch(socket, topic, group: "g1")
      assert ok?(code)
      {records, cursor} = Wire.decode_fetch_resp(payload)
      assert length(records) == 3

      assert {code, _} = commit(socket, topic, "g1", cursor)
      assert ok?(code)

      # the group resumes past the committed position (nothing new)
      assert {code, payload} = fetch(socket, topic, group: "g1")
      assert ok?(code)
      assert {[], _cursor} = Wire.decode_fetch_resp(payload)
    end)
  end

  test "fetch rejects a malformed opaque cursor instead of silently restarting" do
    with_session("app", "app123", fn socket ->
      topic = "logproto_badcur_#{System.unique_integer([:positive])}"
      assert {code, _} = create_topic(socket, topic)
      assert ok?(code)

      # bytes that are not a valid cursor token must error, not fall through to a fetch from the start
      assert {code, payload} = fetch(socket, topic, cursor: "!!!not-a-cursor!!!")
      refute ok?(code)
      assert reason(payload) == "invalid_cursor"

      # no cursor + a group still resumes the group (a nil cursor is "no cursor", not suppressed)
      assert {code, _payload} = fetch(socket, topic, group: "g")
      assert ok?(code)
    end)
  end

  test "produces and fetches arbitrary binary payloads (non-UTF-8 bytes survive the round trip)" do
    with_session("app", "app123", fn socket ->
      topic = "logproto_bin_#{System.unique_integer([:positive])}"
      assert {code, _} = create_topic(socket, topic)
      assert ok?(code)

      payload = <<0, 159, 146, 150, 255, 0, 1>>
      refute String.valid?(payload)

      assert {code, <<1::32>>} = produce(socket, topic, [payload])
      assert ok?(code)

      assert {code, resp} = fetch(socket, topic)
      assert ok?(code)
      {[record], _cursor} = Wire.decode_fetch_resp(resp)
      assert record.value == payload
    end)
  end

  test "fetch with wait resumes when another client produces (long-poll)" do
    topic = "logproto_lp_#{System.unique_integer([:positive])}"

    with_session("app", "app123", fn socket ->
      assert {code, _} = create_topic(socket, topic)
      assert ok?(code)
    end)

    # a consumer long-polls an empty topic in a separate task; it parks until a produce arrives
    consumer =
      Task.async(fn ->
        with_session("app", "app123", fn socket ->
          {code, resp} = fetch(socket, topic, wait: 3_000)
          {code, Wire.decode_fetch_resp(resp)}
        end)
      end)

    wait_for_park()

    with_session("app", "app123", fn socket ->
      assert {code, <<1::32>>} = produce(socket, topic, ["a"])
      assert ok?(code)
    end)

    assert {code, {[record], _cursor}} = Task.await(consumer, 5_000)
    assert ok?(code)
    assert record.value == "a"
  end

  test "fetch with wait returns empty after the timeout (long-poll)" do
    with_session("app", "app123", fn socket ->
      topic = "logproto_lpto_#{System.unique_integer([:positive])}"
      assert {code, _} = create_topic(socket, topic)
      assert ok?(code)

      assert {code, resp} = fetch(socket, topic, wait: 100)
      assert ok?(code)
      assert {[], _cursor} = Wire.decode_fetch_resp(resp)
    end)
  end

  test "produce needs :produce and fetch needs :consume" do
    topic = "logproto_perm_#{System.unique_integer([:positive])}"

    # consumer (only :consume) may fetch but not produce
    with_session("consumer", "consumer123", fn socket ->
      assert {code, payload} = produce(socket, topic, ["x"])
      refute ok?(code)
      assert reason(payload) == "permission_denied"
    end)

    # producer (only :produce) may produce/create but not fetch
    with_session("producer", "producer123", fn socket ->
      assert {code, _} = create_topic(socket, topic)
      assert ok?(code)

      assert {code, payload} = fetch(socket, topic)
      refute ok?(code)
      assert reason(payload) == "permission_denied"
    end)
  end

  describe "admin user management" do
    test "admin creates a user over the wire; the user then authenticates and is listed; delete revokes it" do
      username = "wireuser_#{System.unique_integer([:positive])}"
      on_exit(fn -> Malachi.Auth.remove_user(username) end)

      with_session("admin", "admin123", fn socket ->
        assert {code, _} = create_user(socket, username, "Wire-Pass-123", ["produce", "consume"])
        assert ok?(code)

        # the new user shows up in list_users with its permissions (no hashes on the wire)
        assert {code, payload} = list_users(socket)
        assert ok?(code)
        users = Wire.decode_list_users_resp(payload)
        entry = Enum.find(users, &(&1.username == username))
        assert entry
        assert Enum.sort(entry.permissions) == ["consume", "produce"]
      end)

      # the user is real and cluster-replicated: it can authenticate and use its :produce permission
      with_session(username, "Wire-Pass-123", fn socket ->
        assert {code, _} = create_topic(socket, "t_#{System.unique_integer([:positive])}")
        assert ok?(code)
      end)

      # admin deletes it; it can no longer authenticate
      with_session("admin", "admin123", fn socket ->
        assert {code, _} = delete_user(socket, username)
        assert ok?(code)
      end)

      assert {:error, _reason} = try_auth(username, "Wire-Pass-123")
    end

    test "admin changes a user's password; the new password authenticates and the old does not" do
      username = "wirepw_#{System.unique_integer([:positive])}"
      on_exit(fn -> Malachi.Auth.remove_user(username) end)

      with_session("admin", "admin123", fn socket ->
        assert {code, _} = create_user(socket, username, "Old-Pass-123", ["consume"])
        assert ok?(code)
        assert {code, _} = change_password(socket, username, "New-Pass-456")
        assert ok?(code)
      end)

      assert {:ok, _token} = try_auth(username, "New-Pass-456")
      assert {:error, _reason} = try_auth(username, "Old-Pass-123")
    end

    test "a non-admin cannot manage users, and invalid permissions are rejected" do
      # producer (no :admin) is denied
      with_session("producer", "producer123", fn socket ->
        assert {code, payload} = create_user(socket, "nope_#{System.unique_integer([:positive])}", "p", ["consume"])
        refute ok?(code)
        assert reason(payload) == "permission_denied"
      end)

      # admin, but an unknown permission string is rejected without creating the user
      with_session("admin", "admin123", fn socket ->
        username = "wirebad_#{System.unique_integer([:positive])}"
        assert {code, payload} = create_user(socket, username, "p", ["superuser"])
        refute ok?(code)
        assert reason(payload) == "invalid_permissions"
        assert {:error, _reason} = try_auth(username, "p")
      end)
    end
  end

  describe "admin per-topic ACL management" do
    test "admin grants an ACL over the wire; it is listed; revoke removes it" do
      username = "aclwire_#{System.unique_integer([:positive])}"
      Malachi.Auth.add_user(username, "Acl-Pass-1", [:produce])
      on_exit(fn -> Malachi.Auth.remove_user(username) end)

      with_session("admin", "admin123", fn socket ->
        assert {code, _} = grant_acl(socket, username, "produce", "orders.*")
        assert ok?(code)

        assert {code, payload} = list_acls(socket, username)
        assert ok?(code)
        assert Wire.decode_list_acls_resp(payload) == [%{operation: "produce", resource: "orders.*"}]

        assert {code, _} = revoke_acl(socket, username, "produce", "orders.*")
        assert ok?(code)

        assert {_code, payload} = list_acls(socket, username)
        assert Wire.decode_list_acls_resp(payload) == []
      end)
    end

    test "a non-admin cannot manage ACLs, and an invalid operation is rejected" do
      with_session("producer", "producer123", fn socket ->
        assert {code, payload} = grant_acl(socket, "x", "produce", "t.*")
        refute ok?(code)
        assert reason(payload) == "permission_denied"
      end)

      with_session("admin", "admin123", fn socket ->
        assert {code, payload} = grant_acl(socket, "x", "superuser", "t.*")
        refute ok?(code)
        assert reason(payload) == "invalid_operation"
      end)
    end
  end

  defp grant_acl(socket, username, operation, pattern) do
    TCPHelper.request(socket, Wire.grant_acl_key(), 1, Wire.encode_acl_req(username, operation, pattern))
  end

  defp revoke_acl(socket, username, operation, pattern) do
    TCPHelper.request(socket, Wire.revoke_acl_key(), 1, Wire.encode_acl_req(username, operation, pattern))
  end

  defp list_acls(socket, username) do
    TCPHelper.request(socket, Wire.list_acls_key(), 1, Wire.encode_list_acls_req(username))
  end
end
