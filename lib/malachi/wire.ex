defmodule Malachi.Wire do
  @moduledoc """
  The binary wire protocol for the NorthGuard log client: a length-prefixed, request/response
  framing that replaces the JSON+base64 line protocol (measured ~29% fewer bytes and 9-17x less
  serialization CPU in `benchmark/protocol_bench.exs`).

      Frame:     <<len::32, body::binary-size(len)>>
      Request:   <<api_key::16, correlation_id::32, payload::binary>>
      Response:  <<correlation_id::32, error_code::16, payload::binary>>

  `correlation_id` lets a client pipeline (match each response to its request). Records on the wire carry
  **no offset**: the client never sees one; the opaque cursor carries position - so this is a distinct
  encoding from `Malachi.Log.Record.encode/1` (the on-disk frame, which includes the offset). Keys and cursors are
  length-prefixed byte strings with a presence flag (`nil` vs empty are distinct). Pure, this module
  only encodes/decodes binaries; the socket wiring is B1b.

  `decode_frame/1` is tolerant (returns `:incomplete` for a partial frame), but the payload decoders
  (`decode_request/1`, `decode_produce_req/1`, …) assume a **well-formed** body and raise on a malformed
  one. A frame body comes from an untrusted client, so B1b must decode inside a `try` and answer an error
  (or close) on a raise: keeping the malformed-input handling at the connection boundary, not in the codec.

  ## Stability and compatibility

  This framing is the compatibility contract with every client: the Node CLI, the Elixir client, and any
  future SDK. Two things are **stable** and must stay so: the byte layout of each frame above, and the
  `api_key` numbers (currently 0..21, `@auth` through `@get_topic_policy`). Clients are compiled against them, so
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
  """

  alias Malachi.Log.Record

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

  # error codes (responses): 0 = ok, 1 = error with the reason as a string payload
  @ok 0
  @error 1

  @type api_key :: 0..21
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

  def encode_produce_req(topic, records) do
    <<put_str(topic)::binary, length(records)::32, encode_records(records)::binary>>
  end

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

  # ---- wire record (no offset; key/value/headers/timestamp only) ----

  @doc "Encodes a record for the wire (no offset: the client never sees one)."
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
  defp put_value(value) when is_integer(value) and value >= 0, do: <<1::8, value::64>>
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
