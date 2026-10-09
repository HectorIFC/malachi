defmodule Malachi.Wire do
  @moduledoc """
  The binary wire protocol for the NorthGuard log client: a length-prefixed, request/response
  framing that replaces the JSON+base64 line protocol (measured ~29% fewer bytes and 9-17x less
  serialization CPU in `benchmark/protocol_bench.exs`).

      Frame:     <<len::32, body::binary-size(len)>>
      Request:   <<api_key::16, correlation_id::32, payload::binary>>
      Response:  <<correlation_id::32, error_code::16, payload::binary>>

  `correlation_id` lets a client pipeline (match each response to its request). A record on the wire
  (`encode_record/1`) carries no offset, so it is a distinct encoding from `Malachi.Log.Record.encode/1`
  (the on-disk frame, which includes the offset); a consume batch puts each record's position beside it
  (`Malachi.Wire.Batch`). Keys and cursors are
  length-prefixed byte strings with a presence flag (`nil` vs empty are distinct). Pure, this module
  only encodes/decodes binaries; the socket wiring is B1b.

  `decode_frame/1` is tolerant (returns `:incomplete` for a partial frame), but the payload decoders
  (`decode_request/1`, `decode_produce_req/1`, …) assume a **well-formed** body and raise on a malformed
  one. A frame body comes from an untrusted client, so B1b must decode inside a `try` and answer an error
  (or close) on a raise: keeping the malformed-input handling at the connection boundary, not in the codec.

  ## Stability and compatibility

  This framing is the compatibility contract with every client: the Node CLI, the Elixir client, and any
  future SDK. Two things are **stable** and must stay so: the byte layout of each frame above, and the
  `api_key` numbers (currently 0..34, `@auth` through `@commit_offsets`). Clients are compiled against them, so
  a running cluster and its clients agree on the wire only as long as both hold.

  A change is **breaking** (every deployed client must update in lockstep, so it cannot ship in a normal
  release) when it:

    * changes the layout or meaning of an existing `api_key`'s request or response payload,
    * reuses or renumbers an `api_key` that already shipped, or
    * changes how `error_code` or its reason string is encoded.

  Evolve the protocol **additively** instead: every new operation, and every extension of an existing one,
  takes the next free `api_key` number. Appending a field to an existing payload is **not** compatible here,
  because the decoders match a payload to its exact end (`{value, <<>>} = take_str(rest)`): trailing bytes
  raise a `MatchError` rather than being ignored, so an old peer cannot skip a field a newer one appended. A
  shipped frame is therefore frozen; a change means a new key. This mirrors the discipline the Apache Iggy
  project keeps around its own binary protocol: extend, do not rewrite.

  ## Streams, routing and groups (keys 24 to 34, #275)

  The data path NorthGuard describes (transcript 541 to 569): a client bootstraps the cluster's minimal
  state from any node (`cluster_state`, key 24), resolves a topic to its ranges and each range's active
  segment and primary (`topic_routes`, 25), then talks to that primary directly.

    * **Produce** is a stream (`open_stream`, 26): a handshake binds it to a range's active segment and
      grants a window; `append` (27) carries a sequence number and one batch, pipelined up to the window;
      the server pushes `append_ack` with the sequence below which every append has been answered (0
      before the first answer), per sequence errors and the current window, and `moved` when the segment
      seals (a roll or a failover), the range splits or merges, or the broker restarts; the appends in flight
      when it moves are still answered after the `moved`. `close_stream` (28) ends it.
    * **Consume** is the mirror (`open_consume`, 29): pushes of `records` carry the offset of every record,
      and `consume_ack` (30) says what was read and can move the window. `fetch_range` (31) is the unary
      read of one range.
    * **Groups** live beside the data path, not in it: `join_group` (32) and `group_heartbeat` (33) answer
      a member's assignment and generation, `commit_offsets` (34) checkpoints its positions.

  A server push reuses the response envelope with the correlation id of the request that opened the
  stream, as a subscribe push always has; its payload starts with a kind byte (`append_ack`, `moved`,
  `records`) and the stream id. Records travel in a batch (`Malachi.Wire.Batch`): a codec byte, the record
  count, the inflated size, and the payload, compressed or not. A range travels as its sequence number
  within the topic, a segment likewise, a position as the index of its source in the range's history (an
  ancestor before a split, or the range itself) and an offset in that source. A range's history lists its
  sources oldest first, and a split only extends it, so a child's history starts with its parent's: a
  position read in the parent names the same record in the child. A merge does not keep that: the merged
  range lists both buddies, so a position read in one of them does not carry over to it. That ordering is part
  of the contract. A consumer of a range a split or merge just made starts from the start of its history,
  so it reads its share of the ancestors' records again: at least once, never lost.

  A window is counted in appends and bytes on a producer stream and in records on a consume stream. The
  window an `open_stream` or `open_consume` asks for is an integer from 1 to 2^32 - 1 in each count, since a
  stream that may never send anything is no stream: the encoders and decoders here raise on anything
  else. In an `append_ack` the window is the
  one granted now, and 0 holds further appends until a later ack grants more. In a `consume_ack` the
  window is a change the consumer asks for, and 0 asks for none, so a consumer pauses by not acking.
  """

  alias Malachi.Log.Record
  alias Malachi.Wire.Batch

  # api keys (request operations)
  @auth 0
  @create_topic 1
  @produce 2
  @fetch 3
  @commit 4
  @subscribe 5
  @stream_ack 6
  @leave_group 7
  # admin user management (require the :admin permission, see Malachi.TCPProtocol)
  @create_user 8
  @delete_user 9
  @change_password 10
  @list_users 11
  # mTLS-identity auth: empty payload; the server authenticates from the verified peer certificate.
  @mtls_auth 12
  # OIDC/JWT auth: payload is a signed JWT; the server validates it and maps a claim to a user.
  @token_auth 13
  # admin per-topic ACL management: grant/revoke a {username, operation, pattern}, list a user's ACLs.
  @grant_acl 14
  @revoke_acl 15
  @list_acls 16
  # admin storage policies (#194): define/delete/list named policies, bind a topic to one, and read back a
  # topic's effective retention. Fields travel as a counted list of self-describing `{name, value}` pairs
  # (see "Policy fields" below), so a field added later (#199, #200, #201, #206) needs no new key.
  @define_policy 17
  @delete_policy 18
  @list_policies 19
  @bind_topic_policy 20
  @get_topic_policy 21
  # admin console roles (#228): set or remove a user's console role, and list users with their roles. The
  # list is a new key rather than a field on `@list_users`, whose response is frozen like every shipped
  # frame. Both require the wire :admin permission.
  @set_role 22
  @list_users_with_roles 23
  # streams, routing and groups (#275, see "Streams, routing and groups" above)
  @cluster_state 24
  @topic_routes 25
  @open_stream 26
  @append 27
  @close_stream 28
  @open_consume 29
  @consume_ack 30
  @fetch_range 31
  @join_group 32
  @group_heartbeat 33
  @commit_offsets 34

  # error codes (responses): 0 = ok, 1 = error with the reason as a string payload
  @ok 0
  @error 1

  @type api_key :: 0..34
  @type error_code :: non_neg_integer()

  @spec auth_key() :: api_key()
  def auth_key, do: @auth
  def create_topic_key, do: @create_topic
  def produce_key, do: @produce
  def fetch_key, do: @fetch
  def commit_key, do: @commit
  def subscribe_key, do: @subscribe
  def stream_ack_key, do: @stream_ack
  def leave_group_key, do: @leave_group
  def create_user_key, do: @create_user
  def delete_user_key, do: @delete_user
  def change_password_key, do: @change_password
  def list_users_key, do: @list_users
  def mtls_auth_key, do: @mtls_auth
  def token_auth_key, do: @token_auth
  def grant_acl_key, do: @grant_acl
  def revoke_acl_key, do: @revoke_acl
  def list_acls_key, do: @list_acls
  def define_policy_key, do: @define_policy
  def delete_policy_key, do: @delete_policy
  def list_policies_key, do: @list_policies
  def bind_topic_policy_key, do: @bind_topic_policy
  def get_topic_policy_key, do: @get_topic_policy
  def set_role_key, do: @set_role
  def list_users_with_roles_key, do: @list_users_with_roles
  def cluster_state_key, do: @cluster_state
  def topic_routes_key, do: @topic_routes
  def open_stream_key, do: @open_stream
  def append_key, do: @append
  def close_stream_key, do: @close_stream
  def open_consume_key, do: @open_consume
  def consume_ack_key, do: @consume_ack
  def fetch_range_key, do: @fetch_range
  def join_group_key, do: @join_group
  def group_heartbeat_key, do: @group_heartbeat
  def commit_offsets_key, do: @commit_offsets
  def ok_code, do: @ok
  def error_code, do: @error

  @doc "A success response frame (error_code 0) for `correlation_id` carrying `payload`."
  @spec encode_ok(non_neg_integer(), binary()) :: binary()
  def encode_ok(correlation_id, payload), do: encode_response(correlation_id, @ok, payload)

  @doc "An error response frame (error_code 1) whose payload is `reason` as a string."
  @spec encode_error(non_neg_integer(), term()) :: binary()
  def encode_error(correlation_id, reason) do
    encode_response(correlation_id, @error, put_str(to_string(reason)))
  end

  # ---- frame ----

  @doc "Wraps a body in a length-prefixed frame."
  @spec encode_frame(binary()) :: binary()
  def encode_frame(body) when is_binary(body), do: <<byte_size(body)::32, body::binary>>

  @doc "Peels one frame off a buffer: `{:ok, body, rest}` or `:incomplete` if the frame is not all here."
  @spec decode_frame(binary()) :: {:ok, binary(), binary()} | :incomplete
  def decode_frame(<<len::32, body::binary-size(len), rest::binary>>), do: {:ok, body, rest}
  def decode_frame(_partial), do: :incomplete

  @doc """
  Like `decode_frame/1` but bounds the frame: as soon as the 4-byte length prefix is readable, a declared
  length over `max_size` is rejected with `{:error, :frame_too_large}`, **before** the body is buffered:
  so a hostile length prefix cannot force the server to accumulate unbounded memory.
  """
  @spec decode_frame(binary(), non_neg_integer()) ::
          {:ok, binary(), binary()} | :incomplete | {:error, :frame_too_large}
  def decode_frame(<<len::32, _::binary>>, max_size) when len > max_size, do: {:error, :frame_too_large}
  def decode_frame(buffer, _max_size), do: decode_frame(buffer)

  # ---- request / response envelope ----

  @spec encode_request(api_key(), non_neg_integer(), binary()) :: binary()
  def encode_request(api_key, correlation_id, payload) do
    encode_frame(<<api_key::16, correlation_id::32, payload::binary>>)
  end

  @doc """
  `encode_request/3` for a payload given as iodata, returned as iodata: the frame header and the payload
  side by side, so a sender writing it to a socket never copies the payload into a new binary. The bytes
  are exactly those of `encode_request/3` over the same payload.
  """
  @spec encode_request_iodata(api_key(), non_neg_integer(), iodata()) :: iodata()
  def encode_request_iodata(api_key, correlation_id, payload) do
    [<<IO.iodata_length(payload) + 6::32, api_key::16, correlation_id::32>>, payload]
  end

  @spec decode_request(binary()) :: {api_key(), non_neg_integer(), binary()}
  def decode_request(<<api_key::16, correlation_id::32, payload::binary>>), do: {api_key, correlation_id, payload}

  @spec encode_response(non_neg_integer(), error_code(), binary()) :: binary()
  def encode_response(correlation_id, error_code, payload) do
    encode_frame(<<correlation_id::32, error_code::16, payload::binary>>)
  end

  @spec decode_response(binary()) :: {non_neg_integer(), error_code(), binary()}
  def decode_response(<<correlation_id::32, error_code::16, payload::binary>>),
    do: {correlation_id, error_code, payload}

  # ---- operation payloads (round-trip: encode_*_req/decode_*_req) ----

  def encode_auth_req(username, password), do: <<put_str(username)::binary, put_str(password)::binary>>

  def decode_auth_req(payload) do
    {username, rest} = take_str(payload)
    {password, <<>>} = take_str(rest)
    {username, password}
  end

  # An mTLS-auth request carries no payload: the server derives the identity from the verified peer
  # certificate. The response reuses the auth response (a session token).
  def encode_mtls_auth_req, do: <<>>

  # A token-auth request carries the signed JWT as its payload; the response reuses the auth response.
  def encode_token_auth_req(jwt), do: put_str(jwt)

  def decode_token_auth_req(payload) do
    {jwt, <<>>} = take_str(payload)
    jwt
  end

  def encode_auth_resp(token), do: put_str(token)

  def decode_auth_resp(payload) do
    {token, <<>>} = take_str(payload)
    token
  end

  @doc "Decodes an error response payload (see `encode_error/2`) back to its reason string."
  @spec decode_error_reason(binary()) :: String.t() | nil
  def decode_error_reason(payload) do
    {reason, <<>>} = take_str(payload)
    reason
  end

  def encode_create_topic_req(topic, keyspace_bits), do: <<put_str(topic)::binary, keyspace_bits::8>>

  def decode_create_topic_req(payload) do
    {topic, <<keyspace_bits::8>>} = take_str(payload)
    {topic, keyspace_bits}
  end

  def encode_produce_req(topic, records),
    do: <<put_str(topic)::binary, encode_produce_records(records)::binary>>

  @doc """
  The records half of a produce request (the count, then each record), which does not depend on the
  topic. A sender that reuses one batch across topics encodes it once with this and joins it to each
  topic with `encode_produce_req_with/2`; `encode_produce_req(topic, records)` is the same bytes as one
  binary.
  """
  @spec encode_produce_records([Record.t()]) :: binary()
  def encode_produce_records(records), do: <<length(records)::32, encode_records(records)::binary>>

  @doc """
  A produce request for `topic` from records already encoded by `encode_produce_records/1`, as iodata, so
  the records are referenced rather than copied; `encode_request_iodata/3` frames it the same way.
  """
  @spec encode_produce_req_with(String.t(), binary()) :: iodata()
  def encode_produce_req_with(topic, encoded_records), do: [put_str(topic), encoded_records]

  def decode_produce_req(payload) do
    {topic, <<count::32, rest::binary>>} = take_str(payload)
    {records, <<>>} = take_records(rest, count, [])
    {topic, records}
  end

  # cursor is an opaque byte string (nil = start), group is an optional consumer group (nil = none),
  # member is an optional consumer-group member id (nil = whole-group / single consumer); max and wait_ms
  # are the fetch bounds. Precedence: a `member` (grouped, server-scoped to its ranges) wins, then an
  # explicit `cursor` (client-managed paging), then a `group` resume. No range/offset ever crosses the
  # wire: the response is always records + an opaque cursor.
  def encode_fetch_req(topic, cursor, group, member, max, wait_ms) do
    <<put_str(topic)::binary, put_str(cursor)::binary, put_str(group)::binary, put_str(member)::binary, max::32,
      wait_ms::32>>
  end

  def decode_fetch_req(payload) do
    {topic, rest} = take_str(payload)
    {cursor, rest} = take_str(rest)
    {group, rest} = take_str(rest)
    {member, <<max::32, wait_ms::32>>} = take_str(rest)
    {topic, cursor, group, member, max, wait_ms}
  end

  def encode_fetch_resp(records, next_cursor) do
    <<length(records)::32, encode_records(records)::binary, put_str(next_cursor)::binary>>
  end

  def decode_fetch_resp(payload) do
    <<count::32, rest::binary>> = payload
    {records, rest} = take_records(rest, count, [])
    {next_cursor, <<>>} = take_str(rest)
    {records, next_cursor}
  end

  def encode_commit_req(topic, group, cursor) do
    <<put_str(topic)::binary, put_str(group)::binary, put_str(cursor)::binary>>
  end

  def decode_commit_req(payload) do
    {topic, rest} = take_str(payload)
    {group, rest} = take_str(rest)
    {cursor, <<>>} = take_str(rest)
    {topic, group, cursor}
  end

  # Streaming: subscribe opens a server-push stream for a consumer `group`, bounded by a credit
  # `window` (max in-flight records) and a per-push `max` batch size. The server then pushes records as
  # ordinary success responses tagged with the subscribe's correlation id, each carrying an
  # `encode_fetch_resp/2` payload (records + the next opaque cursor).
  # member is an optional consumer-group member id (nil = whole-group subscription); with it set the
  # server scopes the push stream to the member's ranges (opaque: the push is still records + cursor).
  def encode_subscribe_req(topic, group, member, window, max) do
    <<put_str(topic)::binary, put_str(group)::binary, put_str(member)::binary, window::32, max::32>>
  end

  def decode_subscribe_req(payload) do
    {topic, rest} = take_str(payload)
    {group, rest} = take_str(rest)
    {member, <<window::32, max::32>>} = take_str(rest)
    {topic, group, member, window, max}
  end

  # stream_ack durably commits `group`'s position at `cursor` and returns `count` records of window
  # credit (unblocking further pushes). Fire-and-forget: the server sends no response, the credit shows
  # up as more pushes.
  def encode_stream_ack_req(topic, group, member, cursor, count) do
    <<put_str(topic)::binary, put_str(group)::binary, put_str(member)::binary, put_str(cursor)::binary, count::32>>
  end

  def decode_stream_ack_req(payload) do
    {topic, rest} = take_str(payload)
    {group, rest} = take_str(rest)
    {member, rest} = take_str(rest)
    {cursor, <<count::32>>} = take_str(rest)
    {topic, group, member, cursor, count}
  end

  # leave_group removes a member from a consumer group for a fast rebalance on a clean shutdown (otherwise
  # the coordinator evicts it on session timeout). Fire-and-forget-ish: the server acks with an empty ok.
  def encode_leave_group_req(topic, group, member) do
    <<put_str(topic)::binary, put_str(group)::binary, put_str(member)::binary>>
  end

  def decode_leave_group_req(payload) do
    {topic, rest} = take_str(payload)
    {group, rest} = take_str(rest)
    {member, <<>>} = take_str(rest)
    {topic, group, member}
  end

  # ---- admin user management (permissions are byte strings on the wire: "admin"/"produce"/"consume") ----

  def encode_create_user_req(username, password, permissions) do
    <<put_str(username)::binary, put_str(password)::binary, put_perms(permissions)::binary>>
  end

  def decode_create_user_req(payload) do
    {username, rest} = take_str(payload)
    {password, rest} = take_str(rest)
    {permissions, <<>>} = take_perms(rest)
    {username, password, permissions}
  end

  def encode_delete_user_req(username), do: put_str(username)

  def decode_delete_user_req(payload) do
    {username, <<>>} = take_str(payload)
    username
  end

  def encode_change_password_req(username, new_password) do
    <<put_str(username)::binary, put_str(new_password)::binary>>
  end

  def decode_change_password_req(payload) do
    {username, rest} = take_str(payload)
    {new_password, <<>>} = take_str(rest)
    {username, new_password}
  end

  # list_users request has an empty payload; the response carries `[{username, [permission_string]}]`.
  def encode_list_users_resp(users) do
    body =
      for %{username: u, permissions: perms} <- users, into: <<>> do
        <<put_str(u)::binary, put_perms(perms)::binary>>
      end

    <<length(users)::32, body::binary>>
  end

  def decode_list_users_resp(<<count::32, rest::binary>>) do
    {users, <<>>} = take_users(rest, count, [])
    users
  end

  # ---- admin console roles. A role is a byte string ("viewer"/"editor"/"admin") or absent for no role. ----

  def encode_set_role_req(username, role), do: <<put_str(username)::binary, put_str(role_str(role))::binary>>

  def decode_set_role_req(payload) do
    {username, rest} = take_str(payload)
    {role, <<>>} = take_str(rest)
    {username, role}
  end

  # list_users_with_roles request has an empty payload; the response carries
  # `[{username, [permission_string], role_string | absent}]`.
  def encode_list_users_with_roles_resp(users) do
    body =
      for %{username: u, permissions: perms, role: role} <- users, into: <<>> do
        <<put_str(u)::binary, put_perms(perms)::binary, put_str(role_str(role))::binary>>
      end

    <<length(users)::32, body::binary>>
  end

  def decode_list_users_with_roles_resp(<<count::32, rest::binary>>) do
    {users, <<>>} = take_users_with_roles(rest, count, [])
    users
  end

  defp role_str(nil), do: nil
  defp role_str(role), do: to_string(role)

  defp take_users_with_roles(rest, 0, acc), do: {Enum.reverse(acc), rest}

  defp take_users_with_roles(rest, n, acc) do
    {username, rest} = take_str(rest)
    {perms, rest} = take_perms(rest)
    {role, rest} = take_str(rest)
    take_users_with_roles(rest, n - 1, [%{username: username, permissions: perms, role: role} | acc])
  end

  # ---- admin per-topic ACL management. operation is a byte string ("produce"/"consume"); resource is a
  # pattern ("orders.eu" literal, or "orders.*" prefix). grant and revoke share the request shape. ----

  def encode_acl_req(username, operation, pattern) do
    <<put_str(username)::binary, put_str(to_string(operation))::binary, put_str(pattern)::binary>>
  end

  def decode_acl_req(payload) do
    {username, rest} = take_str(payload)
    {operation, rest} = take_str(rest)
    {pattern, <<>>} = take_str(rest)
    {username, operation, pattern}
  end

  def encode_list_acls_req(username), do: put_str(username)

  def decode_list_acls_req(payload) do
    {username, <<>>} = take_str(payload)
    username
  end

  # list_acls response carries `[%{operation, resource}]` (both byte strings).
  def encode_list_acls_resp(acls) do
    body =
      for %{operation: operation, resource: resource} <- acls, into: <<>> do
        <<put_str(to_string(operation))::binary, put_str(resource)::binary>>
      end

    <<length(acls)::32, body::binary>>
  end

  def decode_list_acls_resp(<<count::32, rest::binary>>) do
    {acls, <<>>} = take_acls(rest, count, [])
    acls
  end

  # ---- admin storage policies ----
  #
  # Policy fields: <<count::16, field*>>, field = <<put_str(name), value>>, and a value says its own type:
  #
  #     <<0::8>>                          nil (the rule is off)
  #     <<1::8, integer::64>>             a non-negative integer bound
  #     <<2::8, len::32, bytes::binary>>  a string
  #
  # A field left out of the list is absent from the policy: it inherits the global value. So "inherit",
  # "off" and `0` stay three different things. Self-describing values mean a decoder never needs the field
  # table: an older client still parses a response carrying a field it has never heard of, and the server
  # refuses an unknown field BY NAME (`Malachi.Cluster.Policy.from_pairs/2`) instead of failing to parse.

  @typedoc "A policy field as the wire carries it."
  @type policy_value :: non_neg_integer() | String.t() | nil

  @doc "define_policy (17): `<<put_str(name), fields>>`."
  @spec encode_define_policy_req(String.t(), [{String.t(), policy_value()}]) :: binary()
  def encode_define_policy_req(name, fields), do: <<put_str(name)::binary, put_fields(fields)::binary>>

  @spec decode_define_policy_req(binary()) :: {String.t() | nil, [{String.t(), policy_value()}]}
  def decode_define_policy_req(payload) do
    {name, rest} = take_str(payload)
    {fields, <<>>} = take_fields(rest)
    {name, fields}
  end

  @doc "delete_policy (18): `<<put_str(name), force::8>>`, force 1 deletes a policy topics are bound to."
  @spec encode_delete_policy_req(String.t(), boolean()) :: binary()
  def encode_delete_policy_req(name, force), do: <<put_str(name)::binary, if(force, do: 1, else: 0)::8>>

  @spec decode_delete_policy_req(binary()) :: {String.t() | nil, boolean()}
  def decode_delete_policy_req(payload) do
    {name, <<force::8>>} = take_str(payload)
    true = force in [0, 1]
    {name, force == 1}
  end

  @doc "list_policies (19) takes an empty request."
  @spec decode_list_policies_req(binary()) :: :ok
  def decode_list_policies_req(<<>>), do: :ok

  @doc "list_policies response: `<<count::32, (put_str(name), fields)*>>`, in the order given."
  @spec encode_list_policies_resp([{String.t(), [{String.t(), policy_value()}]}]) :: binary()
  def encode_list_policies_resp(policies) do
    body = for {name, fields} <- policies, into: <<>>, do: <<put_str(name)::binary, put_fields(fields)::binary>>
    <<length(policies)::32, body::binary>>
  end

  @spec decode_list_policies_resp(binary()) :: [{String.t(), [{String.t(), policy_value()}]}]
  def decode_list_policies_resp(<<count::32, rest::binary>>) do
    {policies, <<>>} = take_policies(rest, count, [])
    policies
  end

  @doc "bind_topic_policy (20): `<<put_str(topic), put_str(name)>>`, a nil name detaches the topic."
  @spec encode_bind_topic_policy_req(String.t(), String.t() | nil) :: binary()
  def encode_bind_topic_policy_req(topic, name), do: <<put_str(topic)::binary, put_str(name)::binary>>

  @spec decode_bind_topic_policy_req(binary()) :: {String.t() | nil, String.t() | nil}
  def decode_bind_topic_policy_req(payload) do
    {topic, rest} = take_str(payload)
    {name, <<>>} = take_str(rest)
    {topic, name}
  end

  @doc "get_topic_policy (21): `put_str(topic)`."
  @spec encode_get_topic_policy_req(String.t()) :: binary()
  def encode_get_topic_policy_req(topic), do: put_str(topic)

  @spec decode_get_topic_policy_req(binary()) :: String.t() | nil
  def decode_get_topic_policy_req(payload) do
    {topic, <<>>} = take_str(payload)
    topic
  end

  @resolutions %{none: 0, resolved: 1, unresolved: 2}
  @origins %{global: 0, policy: 1, unresolved_backstop: 2}

  @typedoc "A topic's policy as get_topic_policy answers it."
  @type topic_policy_resp :: %{
          topic: String.t(),
          policy: String.t() | nil,
          resolution: :none | :resolved | :unresolved,
          definition: [{String.t(), policy_value()}] | nil,
          effective: [{String.t(), policy_value(), :global | :policy | :unresolved_backstop}]
        }

  @doc """
  get_topic_policy response: `<<put_str(topic), put_str(policy), resolution::8, has_definition::8,
  fields?, count::16, (put_str(name), value, origin::8)*>>`. Resolution is 0 none, 1 resolved, 2
  unresolved; origin is 0 global, 1 policy, 2 unresolved_backstop. The definition's fields follow only when
  `has_definition` is 1.
  """
  @spec encode_topic_policy_resp(topic_policy_resp()) :: binary()
  def encode_topic_policy_resp(%{topic: topic, policy: policy, resolution: resolution} = resp) do
    definition =
      case resp.definition do
        nil -> <<0::8>>
        fields -> <<1::8, put_fields(fields)::binary>>
      end

    effective =
      for {name, value, origin} <- resp.effective, into: <<>> do
        <<put_str(name)::binary, put_value(value)::binary, Map.fetch!(@origins, origin)::8>>
      end

    <<put_str(topic)::binary, put_str(policy)::binary, Map.fetch!(@resolutions, resolution)::8, definition::binary,
      length(resp.effective)::16, effective::binary>>
  end

  @spec decode_topic_policy_resp(binary()) :: topic_policy_resp()
  def decode_topic_policy_resp(payload) do
    {topic, rest} = take_str(payload)
    {policy, <<resolution::8, rest::binary>>} = take_str(rest)

    {definition, <<count::16, rest::binary>>} =
      case rest do
        <<0::8, rest::binary>> -> {nil, rest}
        <<1::8, rest::binary>> -> take_fields(rest)
      end

    {effective, <<>>} = take_effective(rest, count, [])

    %{
      topic: topic,
      policy: policy,
      resolution: key_of(@resolutions, resolution),
      definition: definition,
      effective: effective
    }
  end

  defp key_of(map, code),
    do: Enum.find_value(map, fn {key, value} -> if value == code, do: key end) || raise(ArgumentError)

  # ---- streams, routing and groups (keys 24 to 34) ----

  @broker_statuses %{alive: 0, suspect: 1, dead: 2}
  @range_states %{active: 0, sealed: 1}
  @push_kinds %{append_ack: 0, moved: 1, records: 2}

  @typedoc "Where a range read starts or resumes: `{source_index, offset}` in the range's history."
  @type position :: {non_neg_integer(), non_neg_integer()}

  @typedoc "A range's active segment and its primary, or `nil` while the range has none yet."
  @type route_segment :: %{segment: non_neg_integer(), primary: String.t()} | nil

  @typedoc "Where a consume begins: the oldest record, the next one produced, a position, or a group's checkpoint."
  @type consume_start :: :earliest | :latest | {:position, position()} | {:committed, String.t()}

  @doc "`cluster_state` (24) asks for nothing."
  @spec encode_cluster_state_req() :: binary()
  def encode_cluster_state_req, do: <<>>

  @spec decode_cluster_state_req(binary()) :: :ok
  def decode_cluster_state_req(<<>>), do: :ok

  @doc """
  The cluster's minimal state (transcript 609 to 613): the brokers with their status and the address each
  advertises to clients (`nil` host and port 0 for one that advertises none), the vnodes that exist, and
  whether the stream keys are enabled. `version` changes exactly when the rest of the answer does, so a
  client compares two for equality (`Malachi.Routing`).
  """
  @spec encode_cluster_state_resp(map()) :: binary()
  def encode_cluster_state_resp(%{version: version, streams_enabled: enabled, brokers: brokers, vnodes: vnodes}) do
    <<version::64, bool(enabled)::8, put_list(brokers, &put_broker/1)::binary, put_list(vnodes, &<<&1::32>>)::binary>>
  end

  @spec decode_cluster_state_resp(binary()) :: map()
  def decode_cluster_state_resp(<<version::64, enabled::8, rest::binary>>) do
    {brokers, rest} = take_list(rest, &take_broker/1)
    {vnodes, <<>>} = take_list(rest, fn <<vnode::32, rest::binary>> -> {vnode, rest} end)
    %{version: version, streams_enabled: from_bool(enabled), brokers: brokers, vnodes: vnodes}
  end

  @doc "`topic_routes` (25): the routes of one topic."
  @spec encode_topic_routes_req(String.t()) :: binary()
  def encode_topic_routes_req(topic), do: put_str(topic)

  @spec decode_topic_routes_req(binary()) :: String.t()
  def decode_topic_routes_req(payload) do
    {topic, <<>>} = take_str(payload)
    topic
  end

  @doc """
  A topic's routes: its keyspace (2^`keyspace_bits` positions, a key's position being
  `Malachi.Keyspace.position_of/2`), and each range with its slice `[key_start, key_end)`, its state and
  its active segment and primary. `version` changes exactly when any of that does, the same on every
  node, so a client compares two for equality (`Malachi.Routing`).
  """
  @spec encode_topic_routes_resp(map()) :: binary()
  def encode_topic_routes_resp(%{topic: topic, version: version, keyspace_bits: bits, ranges: ranges}) do
    <<put_str(topic)::binary, version::64, bits::8, put_list(ranges, &put_route/1)::binary>>
  end

  @spec decode_topic_routes_resp(binary()) :: map()
  def decode_topic_routes_resp(payload) do
    {topic, <<version::64, bits::8, rest::binary>>} = take_str(payload)
    {ranges, <<>>} = take_list(rest, &take_route/1)
    %{topic: topic, version: version, keyspace_bits: bits, ranges: ranges}
  end

  @doc """
  `open_stream` (26): a producer stream on one range, for the routes `routes_version` describes. `codec` is
  the codec the appends will carry; `window_appends` and `window_bytes` are what the client asks for, and
  the server grants at most that. `producer_id` is reserved for idempotent produce (#168) and is `nil` for
  now; `label` names the client in the operator interfaces. The window the server grants can later change
  in any `append_ack`. The stream opens on the node that leads the range's active segment: another node
  answers the error `moved`, and routes that differ from the vnode's answer `stale_routes`; either way the
  client reads `topic_routes` again and opens where they say (`Malachi.ProducerStreams`).
  """
  @spec encode_open_stream_req(map()) :: binary()
  def encode_open_stream_req(%{} = req) do
    open_window!([req.window_appends, req.window_bytes])

    <<put_str(req.topic)::binary, req.range::32, req.routes_version::64, Batch.codec_code(req.codec)::8,
      req.window_appends::32, req.window_bytes::32, put_str(req.producer_id)::binary, put_str(req.label)::binary>>
  end

  @spec decode_open_stream_req(binary()) :: map()
  def decode_open_stream_req(payload) do
    {topic, <<range::32, version::64, codec::8, appends::32, bytes::32, rest::binary>>} = take_str(payload)
    open_window!([appends, bytes])
    {producer_id, rest} = take_str(rest)
    {label, <<>>} = take_str(rest)

    %{
      topic: topic,
      range: range,
      routes_version: version,
      codec: Batch.codec_of(codec),
      window_appends: appends,
      window_bytes: bytes,
      producer_id: producer_id,
      label: label
    }
  end

  @doc "The stream the server opened: its id, the segment it is bound to, the granted window and the routes version."
  @spec encode_open_stream_resp(map()) :: binary()
  def encode_open_stream_resp(%{} = resp) do
    <<resp.stream_id::32, resp.segment::32, resp.window_appends::32, resp.window_bytes::32, resp.routes_version::64>>
  end

  @spec decode_open_stream_resp(binary()) :: map()
  def decode_open_stream_resp(<<stream_id::32, segment::32, appends::32, bytes::32, version::64>>) do
    %{stream_id: stream_id, segment: segment, window_appends: appends, window_bytes: bytes, routes_version: version}
  end

  @doc """
  `append` (27): one batch (`Malachi.Wire.Batch`) under `sequence`, which starts at 0 and grows by one per
  append. The server answers with `append_ack` pushes on the stream, not with a response to this request.
  `batch` is the batch already encoded, so a sender encodes it once.
  """
  @spec encode_append_req(non_neg_integer(), non_neg_integer(), iodata()) :: iodata()
  def encode_append_req(stream_id, sequence, batch), do: [<<stream_id::32, sequence::64>>, batch]

  @doc "Splits an append into its stream id, sequence and batch, without opening the batch (see `Malachi.Wire.Batch.decode/2`)."
  @spec decode_append_req(binary()) :: {non_neg_integer(), non_neg_integer(), binary()}
  def decode_append_req(<<stream_id::32, sequence::64, batch::binary>>) do
    {_header, <<>>} = Batch.split(batch)
    {stream_id, sequence, batch}
  end

  @doc "`close_stream` (28): ends a producer or consumer stream. Answered with an empty ok."
  @spec encode_close_stream_req(non_neg_integer()) :: binary()
  def encode_close_stream_req(stream_id), do: <<stream_id::32>>

  @spec decode_close_stream_req(binary()) :: non_neg_integer()
  def decode_close_stream_req(<<stream_id::32>>), do: stream_id

  @doc """
  `open_consume` (29): a push stream of one range, from `start`, with a credit `window` in records, at most
  `max` records and about `max_bytes` bytes per push, in one of the codecs the client `accept`s.
  """
  @spec encode_open_consume_req(map()) :: binary()
  def encode_open_consume_req(%{} = req) do
    open_window!([req.window])

    <<put_str(req.topic)::binary, req.range::32, req.routes_version::64, put_start(req.start)::binary, req.window::32,
      req.max::32, req.max_bytes::32, put_accept(req.accept)::8>>
  end

  @spec decode_open_consume_req(binary()) :: map()
  def decode_open_consume_req(payload) do
    {topic, <<range::32, version::64, rest::binary>>} = take_str(payload)
    {start, <<window::32, max::32, max_bytes::32, accept::8>>} = take_start(rest)
    open_window!([window])

    %{
      topic: topic,
      range: range,
      routes_version: version,
      start: start,
      window: window,
      max: max,
      max_bytes: max_bytes,
      accept: take_accept(accept)
    }
  end

  @doc "The consumer stream the server opened, and the position its first push starts at."
  @spec encode_open_consume_resp(map()) :: binary()
  def encode_open_consume_resp(%{stream_id: stream_id, position: position}),
    do: <<stream_id::32, put_position(position)::binary>>

  @spec decode_open_consume_resp(binary()) :: map()
  def decode_open_consume_resp(<<stream_id::32, rest::binary>>) do
    {position, <<>>} = take_position(rest)
    %{stream_id: stream_id, position: position}
  end

  @doc """
  `consume_ack` (30): the consumer read everything before `position` on the stream (the read ack, transcript
  569), and moves its window to `window` records (0 leaves it as it is: see the moduledoc). No response: the ack shows up as pushes.
  It returns credit only; checkpointing a group's position is `commit_offsets`.
  """
  @spec encode_consume_ack_req(map()) :: binary()
  def encode_consume_ack_req(%{stream_id: stream_id, position: position, window: window}),
    do: <<stream_id::32, put_position(position)::binary, window::32>>

  @spec decode_consume_ack_req(binary()) :: map()
  def decode_consume_ack_req(<<stream_id::32, rest::binary>>) do
    {position, <<window::32>>} = take_position(rest)
    %{stream_id: stream_id, position: position, window: window}
  end

  @doc "`fetch_range` (31): one page of one range, waiting up to `wait_ms` for records when there are none yet."
  @spec encode_fetch_range_req(map()) :: binary()
  def encode_fetch_range_req(%{} = req) do
    <<put_str(req.topic)::binary, req.range::32, req.routes_version::64, put_start(req.start)::binary, req.max::32,
      req.max_bytes::32, req.wait_ms::32, put_accept(req.accept)::8>>
  end

  @spec decode_fetch_range_req(binary()) :: map()
  def decode_fetch_range_req(payload) do
    {topic, <<range::32, version::64, rest::binary>>} = take_str(payload)
    {start, <<max::32, max_bytes::32, wait_ms::32, accept::8>>} = take_start(rest)

    %{
      topic: topic,
      range: range,
      routes_version: version,
      start: start,
      max: max,
      max_bytes: max_bytes,
      wait_ms: wait_ms,
      accept: take_accept(accept)
    }
  end

  @doc """
  A page of one range, as `fetch_range` answers it and as a `records` push carries it: where the next page
  starts, how many leading records of the batch the reader already had (`skip`, for a position inside a
  stored batch), how many records the range still holds past this page (`backlog`), the records retention
  removed before this page (`expired`, exact or an upper bound), and the batch of positioned records.
  """
  @spec encode_page(map()) :: iodata()
  def encode_page(%{next: next, skip: skip, backlog: backlog, expired: expired, expired_exact: exact, batch: batch}) do
    [<<put_position(next)::binary, skip::32, backlog::64, expired::64, bool(exact)::8>>, batch]
  end

  @spec decode_page(binary()) :: map()
  def decode_page(payload) do
    {next, <<skip::32, backlog::64, expired::64, exact::8, batch::binary>>} = take_position(payload)
    {_header, <<>>} = Batch.split(batch)
    %{next: next, skip: skip, backlog: backlog, expired: expired, expired_exact: from_bool(exact), batch: batch}
  end

  @doc """
  A server push on a stream: `append_ack` (`acked_sequence`, the sequence below which every append has been
  answered, 0 before the first answer; the sequences that failed with their reason; the window granted now,
  where 0 holds further appends until a later ack), `moved` (the stream takes no more appends: where its
  range's data goes now, after a seal, a split or merge, or a broker restart; the appends in flight are
  still answered) or `records` (a page, see `encode_page/1`).
  """
  @spec encode_push(:append_ack | :moved | :records, map()) :: iodata()
  def encode_push(:append_ack, %{} = ack) do
    errors = put_list(ack.errors, fn %{sequence: seq, reason: reason} -> <<seq::64, put_str(reason)::binary>> end)

    <<@push_kinds.append_ack::8, ack.stream_id::32, ack.acked_sequence::64, ack.window_appends::32,
      ack.window_bytes::32, errors::binary>>
  end

  def encode_push(:moved, %{} = moved) do
    <<@push_kinds.moved::8, moved.stream_id::32, put_str(moved.reason)::binary, moved.routes_version::64,
      put_list(moved.targets, &put_target/1)::binary>>
  end

  def encode_push(:records, %{stream_id: stream_id} = page),
    do: [<<@push_kinds.records::8, stream_id::32>>, encode_page(page)]

  @spec decode_push(binary()) :: {:append_ack | :moved | :records, map()}
  def decode_push(<<0::8, stream_id::32, acked::64, appends::32, bytes::32, rest::binary>>) do
    {errors, <<>>} =
      take_list(rest, fn <<seq::64, rest::binary>> ->
        {reason, rest} = take_str(rest)
        {%{sequence: seq, reason: reason}, rest}
      end)

    {:append_ack,
     %{stream_id: stream_id, acked_sequence: acked, window_appends: appends, window_bytes: bytes, errors: errors}}
  end

  def decode_push(<<1::8, stream_id::32, rest::binary>>) do
    {reason, <<version::64, rest::binary>>} = take_str(rest)
    {targets, <<>>} = take_list(rest, &take_target/1)
    {:moved, %{stream_id: stream_id, reason: reason, routes_version: version, targets: targets}}
  end

  def decode_push(<<2::8, stream_id::32, rest::binary>>),
    do: {:records, Map.put(decode_page(rest), :stream_id, stream_id)}

  @doc "`join_group` (32): a member joins `group` for `topic`."
  @spec encode_join_group_req(map()) :: binary()
  def encode_join_group_req(%{topic: topic, group: group, member: member}),
    do: <<put_str(topic)::binary, put_str(group)::binary, put_str(member)::binary>>

  @spec decode_join_group_req(binary()) :: map()
  def decode_join_group_req(payload) do
    {topic, rest} = take_str(payload)
    {group, rest} = take_str(rest)
    {member, <<>>} = take_str(rest)
    %{topic: topic, group: group, member: member}
  end

  @doc "`group_heartbeat` (33): a member of `generation` stays in its group."
  @spec encode_group_heartbeat_req(map()) :: binary()
  def encode_group_heartbeat_req(%{generation: generation} = req),
    do: <<encode_join_group_req(req)::binary, generation::64>>

  @spec decode_group_heartbeat_req(binary()) :: map()
  def decode_group_heartbeat_req(payload) do
    {topic, rest} = take_str(payload)
    {group, rest} = take_str(rest)
    {member, <<generation::64>>} = take_str(rest)
    %{topic: topic, group: group, member: member, generation: generation}
  end

  @doc """
  A member's assignment, as `join_group` and `group_heartbeat` answer it: the generation, the session it
  must heartbeat within, and the ranges it consumes.
  """
  @spec encode_assignment_resp(map()) :: binary()
  def encode_assignment_resp(%{generation: generation, session_ms: session_ms, ranges: ranges}),
    do: <<generation::64, session_ms::32, put_list(ranges, &<<&1::32>>)::binary>>

  @spec decode_assignment_resp(binary()) :: map()
  def decode_assignment_resp(<<generation::64, session_ms::32, rest::binary>>) do
    {ranges, <<>>} = take_list(rest, fn <<range::32, rest::binary>> -> {range, rest} end)
    %{generation: generation, session_ms: session_ms, ranges: ranges}
  end

  @doc "`commit_offsets` (34): checkpoints a member's position in each of its ranges. Answered with an empty ok."
  @spec encode_commit_offsets_req(map()) :: binary()
  def encode_commit_offsets_req(%{positions: positions, generation: generation} = req) do
    positions =
      put_list(positions, fn %{range: range, position: position} -> <<range::32, put_position(position)::binary>> end)

    <<encode_join_group_req(req)::binary, generation::64, positions::binary>>
  end

  @spec decode_commit_offsets_req(binary()) :: map()
  def decode_commit_offsets_req(payload) do
    {topic, rest} = take_str(payload)
    {group, rest} = take_str(rest)
    {member, <<generation::64, rest::binary>>} = take_str(rest)

    {positions, <<>>} =
      take_list(rest, fn <<range::32, rest::binary>> ->
        {position, rest} = take_position(rest)
        {%{range: range, position: position}, rest}
      end)

    %{topic: topic, group: group, member: member, generation: generation, positions: positions}
  end

  defp put_broker(%{id: id, host: host, port: port, status: status}),
    do: <<put_str(id)::binary, put_str(host)::binary, port::16, Map.fetch!(@broker_statuses, status)::8>>

  defp take_broker(binary) do
    {id, rest} = take_str(binary)
    {host, <<port::16, status::8, rest::binary>>} = take_str(rest)
    {%{id: id, host: host, port: port, status: key_of(@broker_statuses, status)}, rest}
  end

  defp put_route(%{range: range, key_start: key_start, key_end: key_end, state: state, segment: segment}),
    do:
      <<range::32, key_start::64, key_end::64, Map.fetch!(@range_states, state)::8, put_route_segment(segment)::binary>>

  defp take_route(<<range::32, key_start::64, key_end::64, state::8, rest::binary>>) do
    {segment, rest} = take_route_segment(rest)

    {%{range: range, key_start: key_start, key_end: key_end, state: key_of(@range_states, state), segment: segment},
     rest}
  end

  defp put_route_segment(nil), do: <<0::8>>
  defp put_route_segment(%{segment: segment, primary: primary}), do: <<1::8, segment::32, put_str(primary)::binary>>

  defp take_route_segment(<<0::8, rest::binary>>), do: {nil, rest}

  defp take_route_segment(<<1::8, segment::32, rest::binary>>) do
    {primary, rest} = take_str(rest)
    {%{segment: segment, primary: primary}, rest}
  end

  defp put_target(%{range: range, segment: segment}), do: <<range::32, put_route_segment(segment)::binary>>

  defp take_target(<<range::32, rest::binary>>) do
    {segment, rest} = take_route_segment(rest)
    {%{range: range, segment: segment}, rest}
  end

  defp put_position({source, offset}), do: <<source::16, offset::64>>
  defp take_position(<<source::16, offset::64, rest::binary>>), do: {{source, offset}, rest}

  defp put_start(:earliest), do: <<0::8>>
  defp put_start(:latest), do: <<1::8>>
  defp put_start({:position, position}), do: <<2::8, put_position(position)::binary>>
  defp put_start({:committed, group}), do: <<3::8, put_str(group)::binary>>

  defp take_start(<<0::8, rest::binary>>), do: {:earliest, rest}
  defp take_start(<<1::8, rest::binary>>), do: {:latest, rest}

  defp take_start(<<2::8, rest::binary>>) do
    {position, rest} = take_position(rest)
    {{:position, position}, rest}
  end

  defp take_start(<<3::8, rest::binary>>) do
    {group, rest} = take_str(rest)
    {{:committed, group}, rest}
  end

  # The codecs a reader accepts, as a bitmask of their codes (bit n for code n).
  defp put_accept(codecs),
    do: codecs |> Enum.map(&Bitwise.bsl(1, Batch.codec_code(&1))) |> Enum.reduce(0, &Bitwise.bor/2)

  defp take_accept(mask),
    do: for(codec <- Batch.codecs(), Bitwise.band(mask, Bitwise.bsl(1, Batch.codec_code(codec))) != 0, do: codec)

  defp bool(true), do: 1
  defp bool(false), do: 0
  defp from_bool(0), do: false
  defp from_bool(1), do: true

  @max_list 65_535

  # The window a stream opens with: an integer of at least 1 in each count, and one a u32 can hold (see
  # the moduledoc).
  defp open_window!(counts) do
    if Enum.all?(counts, &(is_integer(&1) and &1 >= 1 and &1 <= 0xFFFFFFFF)),
      do: :ok,
      else: raise(ArgumentError, "a stream opens with a window from 1 to 4294967295, not #{inspect(counts)}")
  end

  # A counted list: <<count::16, item*>>. A longer list raises rather than wrap the count, which would send
  # a frame the peer reads as malformed.
  defp put_list(items, put) do
    count = length(items)
    if count > @max_list, do: raise(ArgumentError, "a list of #{count} items, past the wire's #{@max_list}")
    <<count::16, items |> Enum.map(put) |> IO.iodata_to_binary()::binary>>
  end

  defp take_list(<<count::16, rest::binary>>, take), do: take_n(rest, count, take, [])

  defp take_n(rest, 0, _take, acc), do: {Enum.reverse(acc), rest}

  defp take_n(rest, n, take, acc) do
    {item, rest} = take.(rest)
    take_n(rest, n - 1, take, [item | acc])
  end

  # ---- wire record (no offset; key/value/headers/timestamp only) ----

  @doc """
  Encodes a record for the wire, with no offset: a consume batch puts the position beside the record
  (`Malachi.Wire.Batch`).
  """
  @spec encode_record(Record.t()) :: binary()
  def encode_record(%Record{key: key, value: value, timestamp: ts, headers: headers}) do
    <<put_str(key)::binary, byte_size(value)::32, value::binary, ts::64, encode_headers(headers)::binary>>
  end

  @spec decode_record(binary()) :: {Record.t(), binary()}
  def decode_record(binary) do
    {key, <<value_len::32, value::binary-size(value_len), ts::64, rest::binary>>} = take_str(binary)
    {headers, rest} = take_headers(rest)
    {%Record{key: key, value: value, timestamp: ts, headers: headers, offset: nil}, rest}
  end

  # ---- internals ----

  # length-prefixed byte string with a presence flag: 0 => nil, 1 => len+bytes
  defp put_str(nil), do: <<0::8>>
  defp put_str(s) when is_binary(s), do: <<1::8, byte_size(s)::32, s::binary>>

  defp take_str(<<0::8, rest::binary>>), do: {nil, rest}
  defp take_str(<<1::8, len::32, s::binary-size(len), rest::binary>>), do: {s, rest}

  # permission list: <<count::32, put_str(perm)*>>. Encoding accepts atoms or strings (each -> byte string);
  # decoding yields strings, which the handler maps back to the allowed permission atoms.
  defp put_perms(perms) do
    body = for p <- perms, into: <<>>, do: put_str(to_string(p))
    <<length(perms)::32, body::binary>>
  end

  defp take_perms(<<count::32, rest::binary>>), do: take_perms(rest, count, [])
  defp take_perms(rest, 0, acc), do: {Enum.reverse(acc), rest}

  defp take_perms(rest, n, acc) do
    {perm, rest} = take_str(rest)
    take_perms(rest, n - 1, [perm | acc])
  end

  defp take_users(rest, 0, acc), do: {Enum.reverse(acc), rest}

  defp take_users(rest, n, acc) do
    {username, rest} = take_str(rest)
    {perms, rest} = take_perms(rest)
    take_users(rest, n - 1, [%{username: username, permissions: perms} | acc])
  end

  defp take_acls(rest, 0, acc), do: {Enum.reverse(acc), rest}

  defp take_acls(rest, n, acc) do
    {operation, rest} = take_str(rest)
    {resource, rest} = take_str(rest)
    take_acls(rest, n - 1, [%{operation: operation, resource: resource} | acc])
  end

  defp put_fields(fields) do
    body = for {name, value} <- fields, into: <<>>, do: <<put_str(name)::binary, put_value(value)::binary>>
    <<length(fields)::16, body::binary>>
  end

  defp put_value(nil), do: <<0::8>>
  # At most 2^64 - 1: a larger integer would be cut to its low 64 bits without an error, and read back as a
  # smaller bound than the one that applies. A policy cannot hold one (`Malachi.Cluster.Policy`), and the
  # global limits are refused at boot (`Malachi.Config.retention_bound/2`), so reaching this is a bug.
  defp put_value(value) when is_integer(value) and value >= 0 and value <= 0xFFFF_FFFF_FFFF_FFFF,
    do: <<1::8, value::64>>

  defp put_value(value) when is_binary(value), do: <<2::8, byte_size(value)::32, value::binary>>

  defp take_fields(<<count::16, rest::binary>>), do: take_fields(rest, count, [])
  defp take_fields(rest, 0, acc), do: {Enum.reverse(acc), rest}

  defp take_fields(rest, n, acc) do
    {name, rest} = take_str(rest)
    {value, rest} = take_value(rest)
    take_fields(rest, n - 1, [{name, value} | acc])
  end

  defp take_value(<<0::8, rest::binary>>), do: {nil, rest}
  defp take_value(<<1::8, value::64, rest::binary>>), do: {value, rest}
  defp take_value(<<2::8, len::32, value::binary-size(len), rest::binary>>), do: {value, rest}

  defp take_policies(rest, 0, acc), do: {Enum.reverse(acc), rest}

  defp take_policies(rest, n, acc) do
    {name, rest} = take_str(rest)
    {fields, rest} = take_fields(rest)
    take_policies(rest, n - 1, [{name, fields} | acc])
  end

  defp take_effective(rest, 0, acc), do: {Enum.reverse(acc), rest}

  defp take_effective(rest, n, acc) do
    {name, rest} = take_str(rest)
    {value, <<origin::8, rest::binary>>} = take_value(rest)
    take_effective(rest, n - 1, [{name, value, key_of(@origins, origin)} | acc])
  end

  defp encode_records(records), do: records |> Enum.map(&encode_record/1) |> IO.iodata_to_binary()

  defp take_records(rest, 0, acc), do: {Enum.reverse(acc), rest}

  defp take_records(rest, n, acc) do
    {record, rest} = decode_record(rest)
    take_records(rest, n - 1, [record | acc])
  end

  defp encode_headers(headers) do
    body = for {k, v} <- headers, into: <<>>, do: <<put_str(k)::binary, put_str(v)::binary>>
    <<length(headers)::32, body::binary>>
  end

  defp take_headers(<<count::32, rest::binary>>), do: take_headers(rest, count, [])
  defp take_headers(rest, 0, acc), do: {Enum.reverse(acc), rest}

  defp take_headers(rest, n, acc) do
    {k, rest} = take_str(rest)
    {v, rest} = take_str(rest)
    take_headers(rest, n - 1, [{k, v} | acc])
  end
end
