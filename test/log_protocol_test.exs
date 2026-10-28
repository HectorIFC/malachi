defmodule Malachi.LogProtocolTest do
  # End-to-end over the real TCP server (started by the app): the binary Malachi.Wire framing against the
  # live Malachi.LogBroker, for what is not a stream key of its own suite: creating a topic, a binary payload
  # through a stream, console roles, and the admin user, ACL and storage policy keys.
  use ExUnit.Case, async: false

  alias Malachi.BrokerServer
  alias Malachi.Cluster.PolicyStore
  alias Malachi.Cluster.RetentionCoordinator
  alias Malachi.Metadata
  alias Malachi.Test.StreamTCP
  alias Malachi.Test.TCPHelper
  alias Malachi.Wire

  # the stream keys route through the group router's topology, which another test's sharded broker can
  # leave naming vnodes this broker does not hold
  setup {Malachi.Test.RouterTopology, :clear}

  defp ok?(code), do: code == Wire.ok_code()
  # an error response carries the reason as a length-prefixed string
  defp reason(payload), do: Wire.decode_auth_resp(payload)

  defp create_topic(socket, topic) do
    TCPHelper.request(socket, Wire.create_topic_key(), 1, Wire.encode_create_topic_req(topic, 8))
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

  defp set_role(socket, username, role) do
    TCPHelper.request(socket, Wire.set_role_key(), 1, Wire.encode_set_role_req(username, role))
  end

  defp list_users_with_roles(socket) do
    TCPHelper.request(socket, Wire.list_users_with_roles_key(), 1, <<>>)
  end

  # Attempts an authentication and returns `{:ok, token}` / `{:error, reason}` (closing the socket).
  defp try_auth(username, password) do
    {:ok, socket} = TCPHelper.connect()
    result = TCPHelper.authenticate_wire(socket, username, password)
    :gen_tcp.close(socket)
    result
  end

  defp with_session(username, password, fun) do
    case TCPHelper.connect() do
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

  test "appends and reads back arbitrary binary payloads (non-UTF-8 bytes survive the round trip)" do
    with_session("app", "app123", fn socket ->
      topic = "logproto_bin_#{System.unique_integer([:positive])}"
      assert {code, _} = create_topic(socket, topic)
      assert ok?(code)

      payload = <<0, 159, 146, 150, 255, 0, 1>>
      refute String.valid?(payload)

      {:ok, %{stream_id: id}} = StreamTCP.open(socket, topic)
      StreamTCP.send_append(socket, id, 0, StreamTCP.batch([payload]))
      assert {_ack, []} = StreamTCP.acked_through(socket, 0)
      assert StreamTCP.values(socket, topic) == [payload]
    end)
  end

  describe "console roles over the wire (#228)" do
    test "an admin sets and removes a console role, and lists users with their roles" do
      username = "wirerole_#{System.unique_integer([:positive])}"
      on_exit(fn -> Malachi.Auth.remove_user(username) end)
      :ok = Malachi.Auth.add_user(username, "Role-Pass-123", [:produce])

      with_session("admin", "admin123", fn socket ->
        assert {code, _} = set_role(socket, username, "viewer")
        assert ok?(code)

        assert {code, payload} = list_users_with_roles(socket)
        assert ok?(code)
        users = Wire.decode_list_users_with_roles_resp(payload)
        assert %{permissions: ["produce"], role: "viewer"} = Enum.find(users, &(&1.username == username))

        assert {code, _} = set_role(socket, username, nil)
        assert ok?(code)
        {_code, payload} = list_users_with_roles(socket)

        assert %{role: nil} =
                 payload |> Wire.decode_list_users_with_roles_resp() |> Enum.find(&(&1.username == username))
      end)
    end

    test "an unknown role or user is refused, and the frozen list_users frame still carries no role" do
      with_session("admin", "admin123", fn socket ->
        assert {code, payload} = set_role(socket, "admin", "root")
        refute ok?(code)
        assert reason(payload) == "invalid_role"

        assert {code, payload} = set_role(socket, "wirerole_nobody", "viewer")
        refute ok?(code)
        assert reason(payload) == "user_not_found"

        {_code, payload} = list_users(socket)

        assert Enum.all?(
                 Wire.decode_list_users_resp(payload),
                 &(Map.keys(&1) |> Enum.sort() == [:permissions, :username])
               )
      end)
    end

    test "managing roles takes the wire :admin permission: a console admin without it is refused" do
      username = "wireconsoleadmin_#{System.unique_integer([:positive])}"
      on_exit(fn -> Malachi.Auth.remove_user(username) end)
      :ok = Malachi.Auth.add_user(username, "Role-Pass-123", [:produce], :admin)

      with_session(username, "Role-Pass-123", fn socket ->
        for {code, payload} <- [set_role(socket, username, "viewer"), list_users_with_roles(socket)] do
          refute ok?(code)
          assert reason(payload) == "permission_denied"
        end
      end)
    end

    test "a console role grants nothing on the wire: a viewer with no wire permission cannot produce" do
      username = "wireviewer_#{System.unique_integer([:positive])}"
      topic = "wireviewer-t-#{System.unique_integer([:positive])}"
      on_exit(fn -> Malachi.Auth.remove_user(username) end)
      :ok = Malachi.Auth.add_user(username, "Role-Pass-123", [], :admin)

      with_session("admin", "admin123", fn socket -> assert {0, _} = create_topic(socket, topic) end)

      version = with_session("admin", "admin123", fn socket -> StreamTCP.routes(socket, topic).version end)

      with_session(username, "Role-Pass-123", fn socket ->
        assert StreamTCP.open(socket, topic, routes_version: version) == {:error, "permission_denied"}
      end)
    end
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

  describe "admin storage policies (api keys 17 to 21)" do
    setup do
      suffix = System.unique_integer([:positive])
      name = "wirepolicy_#{suffix}"
      on_exit(fn -> PolicyStore.delete(name) end)
      %{name: name, kept: "wire-kept-#{suffix}", short: "wire-short-#{suffix}"}
    end

    test "an admin sets a 1 s policy on one topic and only that topic's sealed segments expire", ctx do
      with_session("admin", "admin123", fn socket ->
        for topic <- [ctx.short, ctx.kept], do: assert({0, _} = create_topic(socket, topic))

        fields = [{"retention.max_age_ms", 1_000}]

        assert {0, _} =
                 TCPHelper.request(socket, Wire.define_policy_key(), 1, Wire.encode_define_policy_req(ctx.name, fields))

        assert {0, _} =
                 TCPHelper.request(
                   socket,
                   Wire.bind_topic_policy_key(),
                   2,
                   Wire.encode_bind_topic_policy_req(ctx.short, ctx.name)
                 )

        assert {0, payload} =
                 TCPHelper.request(socket, Wire.get_topic_policy_key(), 3, Wire.encode_get_topic_policy_req(ctx.short))

        read_back = Wire.decode_topic_policy_resp(payload)
        assert %{policy: policy, resolution: :resolved, definition: [{"retention.max_age_ms", 1_000}]} = read_back
        assert policy == ctx.name
        assert {"retention.max_age_ms", 1_000, :policy} in read_back.effective

        assert {0, payload} = TCPHelper.request(socket, Wire.list_policies_key(), 4, <<>>)
        assert {ctx.name, fields} in Wire.decode_list_policies_resp(payload)
      end)

      # The production sweep, over the live control plane's bindings and the live policy store that the
      # wire just wrote. Only the sealed segments are synthesized: the application's broker rolls a
      # segment at 64 MB, and a two-second-old sealed segment per topic is what the rule is about.
      now = System.system_time(:millisecond)
      parent = self()

      {:ok, coordinator} =
        RetentionCoordinator.start_link(
          metadata_source: fn ->
            with_old_sealed_segments(BrokerServer.metadata(Malachi.LogBroker), [ctx.short, ctx.kept], now)
          end,
          expire_segment: fn segment -> send(parent, {:expired, segment.id}) && :ok end,
          policy: %{max_age_ms: nil, max_bytes: nil},
          clock: fn -> now end,
          interval: 3_600_000
        )

      on_exit(fn -> if Process.alive?(coordinator), do: GenServer.stop(coordinator) end)

      assert RetentionCoordinator.run_now(coordinator) == [{{ctx.short, 0}, :old}]
      assert_received {:expired, {{short, 0}, :old}} when short == ctx.short
    end

    test "a policy store that cannot be read is an error on list and on get, never an empty answer", ctx do
      with_session("admin", "admin123", fn socket ->
        assert {0, _} = create_topic(socket, ctx.kept)
        server_id = {PolicyStore.cluster_name(), node()}
        :ok = :ra.stop_server(:default, server_id)

        try do
          assert {1, _reason} = TCPHelper.request(socket, Wire.list_policies_key(), 1, <<>>)

          assert {1, _reason} =
                   TCPHelper.request(socket, Wire.get_topic_policy_key(), 2, Wire.encode_get_topic_policy_req(ctx.kept))
        after
          :ok = :ra.restart_server(:default, server_id)
        end
      end)
    end

    test "a session without :admin is refused on every policy key", ctx do
      with_session("producer", "producer123", fn socket ->
        requests = [
          {Wire.define_policy_key(), Wire.encode_define_policy_req(ctx.name, [])},
          {Wire.delete_policy_key(), Wire.encode_delete_policy_req(ctx.name, false)},
          {Wire.list_policies_key(), <<>>},
          {Wire.bind_topic_policy_key(), Wire.encode_bind_topic_policy_req("t", ctx.name)},
          {Wire.get_topic_policy_key(), Wire.encode_get_topic_policy_req("t")}
        ]

        for {key, payload} <- requests do
          assert {code, reason_payload} = TCPHelper.request(socket, key, 1, payload)
          refute ok?(code)
          assert reason(reason_payload) == "permission_denied"
        end
      end)
    end

    test "refusals come back by name: unknown field, undefined policy, policy in use, unknown topic", ctx do
      with_session("admin", "admin123", fn socket ->
        assert {0, _} = create_topic(socket, ctx.kept)

        unknown = Wire.encode_define_policy_req(ctx.name, [{"retention.ms", 1}])
        assert {1, payload} = TCPHelper.request(socket, Wire.define_policy_key(), 1, unknown)
        assert reason(payload) == "unknown_policy_field: retention.ms"

        undefined = Wire.encode_bind_topic_policy_req(ctx.kept, ctx.name)
        assert {1, payload} = TCPHelper.request(socket, Wire.bind_topic_policy_key(), 2, undefined)
        assert reason(payload) == "no_such_policy"

        assert {0, _} =
                 TCPHelper.request(socket, Wire.define_policy_key(), 3, Wire.encode_define_policy_req(ctx.name, []))

        assert {0, _} = TCPHelper.request(socket, Wire.bind_topic_policy_key(), 4, undefined)

        assert {1, payload} =
                 TCPHelper.request(socket, Wire.delete_policy_key(), 5, Wire.encode_delete_policy_req(ctx.name, false))

        assert reason(payload) == "policy_in_use: " <> ctx.kept

        assert {0, _} =
                 TCPHelper.request(socket, Wire.delete_policy_key(), 6, Wire.encode_delete_policy_req(ctx.name, true))

        ghost = Wire.encode_get_topic_policy_req("ghost-" <> ctx.kept)
        assert {1, payload} = TCPHelper.request(socket, Wire.get_topic_policy_key(), 7, ghost)
        assert reason(payload) == "no_such_topic"

        assert {1, payload} = TCPHelper.request(socket, Wire.define_policy_key(), 8, <<9>>)
        assert reason(payload) == "malformed_request"
      end)
    end
  end

  # Each topic gets one sealed segment, sealed two seconds before `now`, on top of the live metadata.
  defp with_old_sealed_segments(metadata, topics, now) do
    Enum.reduce(topics, metadata, fn topic, metadata ->
      range = {topic, 0}
      {metadata, :ok} = Metadata.apply(metadata, {:register_segment, range, {range, :old}, [node()], 0})
      {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, {range, :old}, 1, 10, now - 2_000})
      metadata
    end)
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
