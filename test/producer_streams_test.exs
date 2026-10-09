defmodule Malachi.ProducerStreamsTest do
  # Producer streams end to end over the real TCP server (`Malachi.Wire` keys 26 to 28): open against the
  # routes, pipelined appends acknowledged in order, the sequence and window rules, the refusals, and the
  # move a split sends.
  #
  # async: false. The `producer_streams` flag is the application's own cache (`ClusterFlagsPause`), and
  # the routing reads the topology the consumer-group router publishes, cleared here for each test.
  use ExUnit.Case, async: false

  alias Malachi.BrokerServer
  alias Malachi.Cluster.ClusterFlagsCache
  alias Malachi.Log.Record
  alias Malachi.Routing
  alias Malachi.Test.ClusterFlagsPause
  alias Malachi.Test.TCPHelper
  alias Malachi.Wire
  alias Malachi.Wire.Batch

  @router_topology {Malachi.Consumer.CoordinatorRouter, :topology}

  setup do
    :ok = ClusterFlagsPause.pause()
    ClusterFlagsCache.put([Routing.flag()])

    topology = :persistent_term.get(@router_topology, nil)
    :persistent_term.erase(@router_topology)

    on_exit(fn ->
      if topology,
        do: :persistent_term.put(@router_topology, topology),
        else: :persistent_term.erase(@router_topology)
    end)

    {:ok, socket} = TCPHelper.connect()
    {:ok, _token} = TCPHelper.authenticate_wire(socket, "app", "app123")
    on_exit(fn -> :gen_tcp.close(socket) end)

    topic = "pstream_#{System.unique_integer([:positive])}"
    {code, _} = TCPHelper.request(socket, Wire.create_topic_key(), 2, Wire.encode_create_topic_req(topic, 4))
    assert code == Wire.ok_code()

    %{socket: socket, topic: topic}
  end

  defp routes(socket, topic) do
    {code, payload} = TCPHelper.request(socket, Wire.topic_routes_key(), 3, Wire.encode_topic_routes_req(topic))
    assert code == Wire.ok_code()
    Wire.decode_topic_routes_resp(payload)
  end

  defp open(socket, topic, opts \\ []) do
    routes = routes(socket, topic)

    req = %{
      topic: topic,
      range: Keyword.get(opts, :range, 0),
      routes_version: Keyword.get(opts, :routes_version, routes.version),
      codec: :none,
      window_appends: Keyword.get(opts, :window_appends, 16),
      window_bytes: Keyword.get(opts, :window_bytes, 1_048_576),
      producer_id: nil,
      label: "test"
    }

    case TCPHelper.request(socket, Wire.open_stream_key(), 10, Wire.encode_open_stream_req(req)) do
      {0, payload} -> {:ok, Wire.decode_open_stream_resp(payload)}
      {1, payload} -> {:error, Wire.decode_error_reason(payload)}
    end
  end

  defp batch(values, opts \\ []) do
    tombstone = Keyword.get(opts, :tombstone, false)

    Batch.encode(
      Enum.map(values, &{Record.new(&1, key: Keyword.get(opts, :key)), tombstone}),
      Keyword.get(opts, :codec, :none)
    )
  end

  defp send_append(socket, stream_id, sequence, batch),
    do:
      :ok =
        :gen_tcp.send(
          socket,
          Wire.encode_request(
            Wire.append_key(),
            11,
            IO.iodata_to_binary(Wire.encode_append_req(stream_id, sequence, batch))
          )
        )

  # The next push on the stream opened with correlation id 10.
  defp push(socket) do
    {:ok, body} = TCPHelper.recv_frame(socket)
    {10, 0, payload} = Wire.decode_response(body)
    Wire.decode_push(payload)
  end

  # Reads pushes until every append up to `sequence` is answered (an ack's `acked_sequence` is the next one
  # not yet answered), returning every error seen on the way.
  defp acked_through(socket, sequence, errors \\ []) do
    {:append_ack, ack} = push(socket)
    errors = errors ++ ack.errors
    if ack.acked_sequence > sequence, do: {ack, errors}, else: acked_through(socket, sequence, errors)
  end

  defp values(socket, topic) do
    {code, payload} =
      TCPHelper.request(socket, Wire.fetch_key(), 4, Wire.encode_fetch_req(topic, nil, nil, nil, 1000, 0))

    assert code == Wire.ok_code()
    {records, _cursor} = Wire.decode_fetch_resp(payload)
    Enum.map(records, & &1.value)
  end

  describe "opening" do
    test "a stream opens on the range against the current routes, with the window the broker grants", ctx do
      assert {:ok, resp} = open(ctx.socket, ctx.topic, window_appends: 1_000_000, window_bytes: 1_000_000_000)
      assert resp.stream_id >= 1
      assert resp.routes_version == routes(ctx.socket, ctx.topic).version
      # capped by the broker's limits
      assert resp.window_appends == Application.get_env(:malachi, :stream_max_window_appends, 64)
      assert resp.window_bytes == Application.get_env(:malachi, :stream_max_window_bytes, 16_777_216)
      # opening placed the range's first segment, so the routes now name it
      assert [%{segment: %{segment: segment}}] = routes(ctx.socket, ctx.topic).ranges
      assert segment == resp.segment
    end

    test "routes that differ from the vnode's are stale_routes", ctx do
      assert open(ctx.socket, ctx.topic, routes_version: 12_345) == {:error, "stale_routes"}
    end

    test "a range the topic does not have is refused", ctx do
      assert {:error, "no_such_range"} = open(ctx.socket, ctx.topic, range: 99)
    end

    test "with the flag off the key is unsupported", ctx do
      ClusterFlagsCache.put([])

      req = %{
        topic: ctx.topic,
        range: 0,
        routes_version: 0,
        codec: :none,
        window_appends: 1,
        window_bytes: 1,
        producer_id: nil,
        label: nil
      }

      {1, payload} = TCPHelper.request(ctx.socket, Wire.open_stream_key(), 10, Wire.encode_open_stream_req(req))
      assert Wire.decode_error_reason(payload) == "unsupported"
    end

    test "a session that may not produce on the topic is refused", ctx do
      {:ok, socket} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate_wire(socket, "consumer", "consumer123")
      on_exit(fn -> :gen_tcp.close(socket) end)

      req = %{
        topic: ctx.topic,
        range: 0,
        routes_version: routes(ctx.socket, ctx.topic).version,
        codec: :none,
        window_appends: 1,
        window_bytes: 1,
        producer_id: nil,
        label: nil
      }

      {1, payload} = TCPHelper.request(socket, Wire.open_stream_key(), 10, Wire.encode_open_stream_req(req))
      assert Wire.decode_error_reason(payload) == "permission_denied"
    end
  end

  describe "appends" do
    test "pipelined appends are acknowledged in order and land in the log in order", ctx do
      {:ok, %{stream_id: id}} = open(ctx.socket, ctx.topic)

      for seq <- 0..9,
          do:
            send_append(
              ctx.socket,
              id,
              seq,
              batch(["v#{seq}a", "v#{seq}b"], codec: Enum.at([:none, :zstd], rem(seq, 2)))
            )

      {ack, errors} = acked_through(ctx.socket, 9)
      assert ack.acked_sequence == 10 and errors == []
      assert values(ctx.socket, ctx.topic) == Enum.flat_map(0..9, &["v#{&1}a", "v#{&1}b"])
    end

    test "a gap ends the stream with an error at the sequence that broke it", ctx do
      {:ok, %{stream_id: id}} = open(ctx.socket, ctx.topic)
      send_append(ctx.socket, id, 0, batch(["a"]))
      {_ack, []} = acked_through(ctx.socket, 0)

      send_append(ctx.socket, id, 2, batch(["b"]))
      assert {:append_ack, %{errors: [%{sequence: 2, reason: "sequence_gap"}]}} = push(ctx.socket)

      send_append(ctx.socket, id, 1, batch(["c"]))
      {:ok, body} = TCPHelper.recv_frame(ctx.socket)
      assert {11, 1, payload} = Wire.decode_response(body)
      assert Wire.decode_error_reason(payload) == "unknown_stream"
      assert values(ctx.socket, ctx.topic) == ["a"]
    end

    test "an append on a stream a gap stopped is unknown_stream, while the one before it is still answered", ctx do
      {:ok, %{stream_id: id}} = open(ctx.socket, ctx.topic)
      # Whether the connection reads the gap before or after the broker answers 0, the stream is stopped
      # when 3 arrives and `ProducerStreams.topic/2` no longer names it: 3 is unknown_stream. The stopped
      # stream still answering what it had in flight is pinned by `Malachi.ProducerStreamsUnitTest`.
      frames =
        for seq <- [0, 2, 3],
            do:
              Wire.encode_request(
                Wire.append_key(),
                11,
                IO.iodata_to_binary(Wire.encode_append_req(id, seq, batch(["v#{seq}"])))
              )

      :ok = :gen_tcp.send(ctx.socket, frames)

      responses = for _ <- 1..3, do: Wire.decode_response(elem(TCPHelper.recv_frame(ctx.socket), 1))
      assert {11, 1, payload} = Enum.find(responses, &match?({11, _, _}, &1))
      assert Wire.decode_error_reason(payload) == "unknown_stream"

      pushes = for {10, 0, push} <- responses, do: Wire.decode_push(push)

      assert {:append_ack, %{errors: [%{sequence: 2, reason: "sequence_gap"}]}} =
               Enum.find(pushes, &match?({:append_ack, %{errors: [_]}}, &1))

      assert Enum.any?(pushes, &match?({:append_ack, %{acked_sequence: 1, errors: []}}, &1))
    end

    test "a repeat ends the stream too", ctx do
      {:ok, %{stream_id: id}} = open(ctx.socket, ctx.topic)
      send_append(ctx.socket, id, 0, batch(["a"]))
      {_ack, []} = acked_through(ctx.socket, 0)
      send_append(ctx.socket, id, 0, batch(["a again"]))
      assert {:append_ack, %{errors: [%{sequence: 0, reason: "sequence_repeat"}]}} = push(ctx.socket)
    end

    test "an append past the window is refused at its sequence, and the stream goes on", ctx do
      {:ok, %{stream_id: id}} = open(ctx.socket, ctx.topic, window_appends: 1)
      # both frames in one write, so the connection reads the second before the first is answered
      frames =
        for seq <- 0..1,
            do:
              Wire.encode_request(
                Wire.append_key(),
                11,
                IO.iodata_to_binary(Wire.encode_append_req(id, seq, batch(["x#{seq}"])))
              )

      :ok = :gen_tcp.send(ctx.socket, frames)

      {ack, errors} = acked_through(ctx.socket, 1)
      assert ack.acked_sequence == 2
      assert errors == [%{sequence: 1, reason: "window_exceeded"}]

      send_append(ctx.socket, id, 2, batch(["y"]))
      assert {_ack, []} = acked_through(ctx.socket, 2)
      assert values(ctx.socket, ctx.topic) == ["x0", "y"]
    end

    test "a tombstone, or a batch that does not decode, is refused at its sequence", ctx do
      {:ok, %{stream_id: id}} = open(ctx.socket, ctx.topic)
      send_append(ctx.socket, id, 0, batch(["gone"], tombstone: true, key: "k"))
      send_append(ctx.socket, id, 1, <<0::8, 1::32, 5::32, 5::32, "short">>)

      {_ack, errors} = acked_through(ctx.socket, 1)
      assert errors == [%{sequence: 0, reason: "tombstone_unsupported"}, %{sequence: 1, reason: "malformed_batch"}]
      assert values(ctx.socket, ctx.topic) == []
    end

    test "a key outside the stream's range is refused whole", ctx do
      broker = Malachi.DataPlaneRouter.shard_for(ctx.topic)
      {:ok, _left, _right} = BrokerServer.split_range(broker, {ctx.topic, 0})
      [_parent, left, _right] = routes(ctx.socket, ctx.topic).ranges
      {:ok, %{stream_id: id}} = open(ctx.socket, ctx.topic, range: left.range)

      size = 16
      outside = Enum.find(1..10_000, &(:erlang.phash2("k#{&1}", size) >= left.key_end))

      send_append(ctx.socket, id, 0, batch(["v"], key: "k#{outside}"))
      assert {_ack, [%{sequence: 0, reason: "key_outside_range"}]} = acked_through(ctx.socket, 0)
    end

    test "an append to a stream the connection never opened is unknown_stream", ctx do
      send_append(ctx.socket, 77, 0, batch(["a"]))
      {:ok, body} = TCPHelper.recv_frame(ctx.socket)
      assert {11, 1, payload} = Wire.decode_response(body)
      assert Wire.decode_error_reason(payload) == "unknown_stream"
    end
  end

  describe "on a connection holding streams" do
    # The response to a request frame with correlation id 11.
    defp reply_to(socket) do
      {:ok, body} = TCPHelper.recv_frame(socket)
      {11, code, payload} = Wire.decode_response(body)
      if code == Wire.ok_code(), do: {:ok, payload}, else: {:error, Wire.decode_error_reason(payload)}
    end

    test "an append after the flag is turned off is unsupported", ctx do
      {:ok, %{stream_id: id}} = open(ctx.socket, ctx.topic)
      ClusterFlagsCache.put([])
      send_append(ctx.socket, id, 0, batch(["a"]))
      assert reply_to(ctx.socket) == {:error, "unsupported"}
      assert values(ctx.socket, ctx.topic) == []
    end

    test "an append after the topic's grant is revoked is refused, under strict ACLs", ctx do
      user = "pstream_acl_#{System.unique_integer([:positive])}"
      :ok = Malachi.Auth.add_user(user, "secret-pass-123", [])
      on_exit(fn -> Malachi.Auth.remove_user(user) end)
      :ok = Malachi.Auth.grant_acl(user, :produce, ctx.topic)
      acl_strict = Application.get_env(:malachi, :acl_strict)
      Application.put_env(:malachi, :acl_strict, true)

      on_exit(fn ->
        if acl_strict == nil,
          do: Application.delete_env(:malachi, :acl_strict),
          else: Application.put_env(:malachi, :acl_strict, acl_strict)
      end)

      {:ok, socket} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate_wire(socket, user, "secret-pass-123")
      on_exit(fn -> :gen_tcp.close(socket) end)
      {:ok, %{stream_id: id}} = open(socket, ctx.topic)

      :ok = Malachi.Auth.revoke_acl(user, :produce, ctx.topic)
      send_append(socket, id, 0, batch(["a"]))
      assert reply_to(socket) == {:error, "permission_denied"}
    end

    test "a subscribe is refused: one connection does not push records and take appends at once", ctx do
      {:ok, _resp} = open(ctx.socket, ctx.topic)

      :ok =
        :gen_tcp.send(
          ctx.socket,
          Wire.encode_request(Wire.subscribe_key(), 11, Wire.encode_subscribe_req(ctx.topic, "g", nil, 10, 10))
        )

      assert reply_to(ctx.socket) == {:error, "unexpected_frame"}
    end

    test "a malformed frame is answered as one, and the connection goes on", ctx do
      {:ok, %{stream_id: id}} = open(ctx.socket, ctx.topic)
      :ok = :gen_tcp.send(ctx.socket, Wire.encode_request(Wire.append_key(), 11, <<1, 2, 3>>))
      assert reply_to(ctx.socket) == {:error, "malformed_request"}

      send_append(ctx.socket, id, 0, batch(["ok"]))
      assert {_ack, []} = acked_through(ctx.socket, 0)
    end

    test "several streams share a connection, each with its own sequence and acks", ctx do
      {:ok, _left, _right} = BrokerServer.split_range(Malachi.DataPlaneRouter.shard_for(ctx.topic), {ctx.topic, 0})
      [_parent, left, right] = routes(ctx.socket, ctx.topic).ranges
      {:ok, %{stream_id: a}} = open(ctx.socket, ctx.topic, range: left.range)
      {:ok, %{stream_id: b}} = open(ctx.socket, ctx.topic, range: right.range)

      key_in = fn range ->
        "k#{Enum.find(1..10_000, &(:erlang.phash2("k#{&1}", 16) in range.key_start..(range.key_end - 1)))}"
      end

      send_append(ctx.socket, a, 0, batch(["a0"], key: key_in.(left)))
      send_append(ctx.socket, b, 0, batch(["b0"], key: key_in.(right)))
      send_append(ctx.socket, a, 1, batch(["a1"], key: key_in.(left)))

      acks = for _ <- 1..3, do: elem(push(ctx.socket), 1)
      assert acks |> Enum.filter(&(&1.stream_id == a)) |> List.last() |> Map.get(:acked_sequence) == 2
      assert acks |> Enum.filter(&(&1.stream_id == b)) |> List.last() |> Map.get(:acked_sequence) == 1
      assert Enum.sort(values(ctx.socket, ctx.topic)) == ["a0", "a1", "b0"]
    end

    test "a connection with nothing in flight closes after the idle timeout", ctx do
      timeout = Application.get_env(:malachi, :tcp_recv_timeout)
      Application.put_env(:malachi, :tcp_recv_timeout, 300)
      on_exit(fn -> Application.put_env(:malachi, :tcp_recv_timeout, timeout) end)

      {:ok, _resp} = open(ctx.socket, ctx.topic)
      assert {:error, :closed} = :gen_tcp.recv(ctx.socket, 0, 2_000)
    end
  end

  describe "closing and moving" do
    test "a closed stream takes no more appends", ctx do
      {:ok, %{stream_id: id}} = open(ctx.socket, ctx.topic)
      assert {0, <<>>} = TCPHelper.request(ctx.socket, Wire.close_stream_key(), 12, Wire.encode_close_stream_req(id))
      assert {1, _} = TCPHelper.request(ctx.socket, Wire.close_stream_key(), 12, Wire.encode_close_stream_req(id))
    end

    test "a split moves the stream to the range's children", ctx do
      {:ok, %{stream_id: id}} = open(ctx.socket, ctx.topic)
      send_append(ctx.socket, id, 0, batch(["before"]))
      {_ack, []} = acked_through(ctx.socket, 0)

      {:ok, _left, _right} = BrokerServer.split_range(Malachi.DataPlaneRouter.shard_for(ctx.topic), {ctx.topic, 0})

      assert {:moved, moved} = push(ctx.socket)
      assert moved.stream_id == id
      assert moved.reason == "retired"
      assert [%{range: 1}, %{range: 2}] = moved.targets
      assert moved.routes_version == routes(ctx.socket, ctx.topic).version

      send_append(ctx.socket, id, 1, batch(["after"]))
      {:ok, body} = TCPHelper.recv_frame(ctx.socket)
      assert {11, 1, _payload} = Wire.decode_response(body)
    end
  end
end
