defmodule Malachi.TCPProtocolTest do
  # async: false. Shares the running application's broker.
  #
  # `process_frame/4` and `process_stream_frame/3` are the protocol boundary: everything a client can put
  # on a socket arrives here, including things no well-behaved client sends. The end-to-end tests
  # (test/log_protocol_test.exs, test/log_streaming_test.exs) drive the happy paths through a real socket;
  # what they cannot easily reach are the boundary's REFUSALS, which is what this file covers. They are
  # exercised directly against the two public functions with a fake transport, because a truncated frame
  # or an unexpected api_key is defined at this layer and does not need a socket to be true.
  #
  # Two branches in the module are deliberately NOT covered here, because reaching them means contriving
  # cluster state rather than sending a frame: `subscribe`'s error branch, which a single node only takes
  # on `:not_owner` (stale routing mid-failover), and `normalize/1`'s inspect branch, which needs a tuple
  # reason such as `{:unroutable, key}` and so needs active ranges that do not cover the keyspace
  # (mid-reshard). Both belong to the cluster harnesses, not to a protocol test.
  use ExUnit.Case, async: false

  alias Malachi.Log.Record
  alias Malachi.TCPProtocol
  alias Malachi.Wire

  # A transport is anything answering `send/2`. This one hands the frame back to the test process, so an
  # assertion reads the exact bytes the boundary would have written to a socket.
  defmodule EchoTransport do
    @moduledoc false
    def send(pid, frame) do
      Kernel.send(pid, {:frame, frame})
      :ok
    end
  end

  @session %{username: "protocol_test_user", permissions: [:produce, :consume, :admin]}

  setup do
    prior = Application.get_env(:malachi, :acl_strict)
    on_exit(fn -> restore(:acl_strict, prior) end)
    Application.put_env(:malachi, :acl_strict, false)
    :ok
  end

  defp restore(key, nil), do: Application.delete_env(:malachi, key)
  defp restore(key, value), do: Application.put_env(:malachi, key, value)

  # Runs one request through the boundary and returns the response it emitted, decoded.
  defp process(api_key, correlation_id, payload) do
    :ok =
      TCPProtocol.process_frame(
        self(),
        Wire.encode_request(api_key, correlation_id, payload) |> body(),
        @session,
        EchoTransport
      )

    receive_response()
  end

  # `process_frame/4` takes a frame BODY, not a framed request, so peel the length prefix back off.
  defp body(frame) do
    {:ok, frame_body, <<>>} = Wire.decode_frame(frame)
    frame_body
  end

  defp receive_response do
    receive do
      {:frame, frame} ->
        {:ok, frame_body, <<>>} = Wire.decode_frame(frame)
        Wire.decode_response(frame_body)
    after
      1_000 -> flunk("the protocol boundary sent no response frame")
    end
  end

  defp error?({_corr, code, _payload}), do: code == Wire.error_code()
  defp reason({_corr, _code, payload}), do: Wire.decode_error_reason(payload)
  defp correlation({corr, _code, _payload}), do: corr

  defp new_topic(prefix) do
    topic = "#{prefix}_#{System.unique_integer([:positive])}"
    {_corr, code, _payload} = process(Wire.create_topic_key(), 1, Wire.encode_create_topic_req(topic, 8))
    assert code == Wire.ok_code()
    topic
  end

  describe "malformed input" do
    test "a frame body too short to hold the envelope header is refused, not crashed" do
      # The envelope is api_key::16 + correlation_id::32, so anything under six bytes cannot even name
      # the request it belongs to. The boundary answers under correlation id 0 rather than raising: a
      # client that truncates a frame gets an error back, not a dropped connection.
      for truncated <- [<<>>, <<0>>, <<0, 2>>, <<0, 2, 0, 0, 0>>] do
        :ok = TCPProtocol.process_frame(self(), truncated, @session, EchoTransport)
        response = receive_response()

        assert error?(response)
        assert reason(response) == "malformed_request"
        assert correlation(response) == 0
      end
    end

    test "a well-formed envelope with a garbage payload is refused under its own correlation id" do
      # Here the header IS readable, so the error can be matched to the request that caused it. This is
      # the difference the boundary's two-branch decode exists to make.
      response = process(Wire.produce_key(), 77, <<"not a produce payload">>)

      assert error?(response)
      assert reason(response) == "malformed_request"
      assert correlation(response) == 77
    end

    test "an unknown api_key is named as such rather than treated as malformed" do
      response = process(60_000, 5, <<>>)

      assert error?(response)
      assert reason(response) == "unknown_api_key"
    end
  end

  describe "streaming boundary" do
    test "a non-ack frame on a subscribed connection is refused and the stream continues" do
      # Once subscribed the only frame a client should send is a stream_ack. Anything else is answered
      # and the stream stays open: the connection is not torn down for one confused frame.
      frame = Wire.encode_request(Wire.produce_key(), 12, Wire.encode_produce_req("t", [])) |> body()

      assert :ok = TCPProtocol.process_stream_frame(self(), frame, EchoTransport)

      response = receive_response()
      assert error?(response)
      assert reason(response) == "unexpected_frame"
      assert correlation(response) == 12
    end

    test "a frame too short to decode at all is answered, not fatal" do
      # This one never reaches the ack decoder: `Wire.decode_request/1` itself raises on a body with no
      # envelope. Both failures land in the same rescue, which answers under correlation 0 (the id is not
      # trustworthy here) and returns :ok so the stream survives.
      assert :ok = TCPProtocol.process_stream_frame(self(), <<>>, EchoTransport)

      response = receive_response()
      assert error?(response)
      assert reason(response) == "malformed_request"
      assert correlation(response) == 0
    end

    test "a well-formed ack envelope with an undecodable payload is answered, not fatal" do
      # The other half, and the one a real buggy client actually sends: the envelope parses, so the frame
      # is dispatched as an ack, and `Wire.decode_stream_ack_req/1` is what raises. Kept separate from the
      # case above because they fail in different decoders and only this one exercises the ack path.
      frame = <<Wire.stream_ack_key()::16, 99::32>>

      assert :ok = TCPProtocol.process_stream_frame(self(), frame, EchoTransport)

      response = receive_response()
      assert error?(response)
      assert reason(response) == "malformed_request"
      assert correlation(response) == 0
    end
  end

  describe "broker errors reach the client" do
    test "producing to a topic that was never created answers no_such_topic" do
      payload = Wire.encode_produce_req("never_created_#{System.unique_integer([:positive])}", [%Record{value: "v"}])
      response = process(Wire.produce_key(), 3, payload)

      assert error?(response)
      assert reason(response) == "no_such_topic"
    end

    test "subscribing to a topic that was never created still opens a stream (documented, not asserted as good)" do
      # Pinning current behaviour, not endorsing it: unlike produce, subscribe does NOT check that the
      # topic exists, so the client enters stream mode on a topic that may never receive a record. It is
      # defensible (the topic can be created later and the stream then works) and it is what ships, so it
      # is recorded here rather than left as a surprise for whoever next reads the subscribe path.
      topic = "never_created_#{System.unique_integer([:positive])}"
      frame = Wire.encode_request(Wire.subscribe_key(), 4, Wire.encode_subscribe_req(topic, nil, nil, 10, 10)) |> body()

      assert {:stream, 4} = TCPProtocol.process_frame(self(), frame, @session, EchoTransport)
    end

    test "a commit carrying a cursor the server never issued is refused" do
      # The cursor is opaque and server-minted; a client that invents one gets a named error rather than
      # a silently accepted commit, which would move a consumer group to a position nobody chose.
      topic = new_topic("commit_cursor")
      response = process(Wire.commit_key(), 6, Wire.encode_commit_req(topic, "g", <<"not a cursor">>))

      assert error?(response)
      assert reason(response) == "invalid_cursor"
    end
  end

  describe "client-supplied bounds fall back to their defaults" do
    test "a fetch asking for no records at all gets the default page size, not zero" do
      # `max` is a client-supplied uint32, so 0 is expressible and means nothing sensible. It must fall
      # back to the default page of 100 rather than being honoured into an empty page forever.
      topic = new_topic("bounds_fetch")
      records = for i <- 1..150, do: %Record{value: "v#{i}"}
      {_corr, code, _} = process(Wire.produce_key(), 2, Wire.encode_produce_req(topic, records))
      assert code == Wire.ok_code()

      {_corr, code, payload} = process(Wire.fetch_key(), 3, Wire.encode_fetch_req(topic, nil, nil, nil, 0, 0))
      assert code == Wire.ok_code()

      {fetched, _cursor} = Wire.decode_fetch_resp(payload)
      assert length(fetched) == 100
    end

    test "a subscribe asking for no credit window still opens a stream" do
      # Same shape for the streaming credit: a window of 0 would be a stream that can never push, so the
      # default applies and the connection enters stream mode.
      topic = new_topic("bounds_sub")
      frame = Wire.encode_request(Wire.subscribe_key(), 9, Wire.encode_subscribe_req(topic, nil, nil, 0, 0)) |> body()

      assert {:stream, 9} = TCPProtocol.process_frame(self(), frame, @session, EchoTransport)
    end
  end
end
