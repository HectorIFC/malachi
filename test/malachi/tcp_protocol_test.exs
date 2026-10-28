defmodule Malachi.TCPProtocolTest do
  # async: false. Shares the running application's broker.
  #
  # `process_frame/4` is the protocol boundary: everything a client can put on a socket arrives here,
  # including things no well-behaved client sends. The end-to-end tests (test/producer_streams_test.exs,
  # test/consume_streams_test.exs, test/group_keys_test.exs) drive the happy paths through a real socket;
  # what they cannot easily reach are the boundary's REFUSALS, which is what this file covers. They are
  # exercised directly against the public function with a fake transport, because a truncated frame or an
  # unexpected api_key is defined at this layer and does not need a socket to be true.
  #
  # One branch in the module is deliberately NOT covered here, because reaching it means contriving
  # cluster state rather than sending a frame: `normalize/1`'s inspect branch, which needs a tuple reason
  # such as `{:unroutable, key}` and so needs active ranges that do not cover the keyspace (mid-reshard).
  # It belongs to the cluster harnesses, not to a protocol test.
  use ExUnit.Case, async: false

  alias Malachi.BrokerServer
  alias Malachi.DataPlaneRouter
  alias Malachi.Log.Record
  alias Malachi.TCPProtocol
  alias Malachi.Test.EchoTransport
  alias Malachi.Test.RouterTopology
  alias Malachi.Wire
  alias Malachi.Wire.Batch

  @session %{username: "protocol_test_user", permissions: [:produce, :consume, :admin]}

  setup do
    prior = Application.get_env(:malachi, :acl_strict)
    on_exit(fn -> restore(:acl_strict, prior) end)
    Application.put_env(:malachi, :acl_strict, false)
    # the topic routing the range reads go through reads the consumer-group router's topology
    RouterTopology.clear()
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

  # A fetch_range from the start of range 0 of `topic`, at most `max` records, at the routes' current version.
  defp fetch_range_req(topic, max) do
    version =
      case process(Wire.topic_routes_key(), 2, Wire.encode_topic_routes_req(topic)) do
        {_corr, 0, routes} -> Wire.decode_topic_routes_resp(routes).version
        _refused -> 0
      end

    Wire.encode_fetch_range_req(%{
      topic: topic,
      range: 0,
      routes_version: version,
      start: :earliest,
      max: max,
      max_bytes: 64 * 1_048_576,
      wait_ms: 0,
      accept: [:none]
    })
  end

  defp fetched({_corr, code, payload}) do
    assert code == Wire.ok_code()

    {:ok, entries} =
      Batch.decode(Wire.decode_page(payload).batch, max_inflated_bytes: 64 * 1_048_576, layout: :positioned)

    entries
  end

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
      response = process(Wire.open_stream_key(), 77, <<"not an open_stream payload">>)

      assert error?(response)
      assert reason(response) == "malformed_request"
      assert correlation(response) == 77
    end

    test "an unknown api_key is named as such rather than treated as malformed" do
      response = process(60_000, 5, <<>>)

      assert error?(response)
      assert reason(response) == "unknown_api_key"
    end

    test "the retired data keys 2 to 6 are unknown keys, whatever their payload" do
      for api_key <- 2..6, payload <- [<<>>, <<"anything">>] do
        response = process(api_key, 40 + api_key, payload)

        assert error?(response)
        assert reason(response) == "unknown_api_key"
        assert correlation(response) == 40 + api_key
      end
    end
  end

  describe "broker errors reach the client" do
    test "reading a topic that was never created answers no_such_topic" do
      response =
        process(Wire.fetch_range_key(), 3, fetch_range_req("never_created_#{System.unique_integer([:positive])}", 1))

      assert error?(response)
      assert reason(response) == "no_such_topic"
    end
  end

  describe "client-supplied bounds fall back to their defaults" do
    test "a fetch asking for no records at all gets the default page size, not zero, and one asking for too many the cap" do
      # `max` is a client-supplied uint32, so 0 is expressible and means nothing sensible. It must fall
      # back to the default page of 100 rather than being honoured into an empty page forever; and one past
      # the cap is held to its 1000.
      topic = new_topic("bounds_fetch")
      records = for i <- 1..1_100, do: %Record{value: "v#{i}"}
      {:ok, _placements} = BrokerServer.produce(DataPlaneRouter.shard_for(topic), topic, records)

      assert length(fetched(process(Wire.fetch_range_key(), 3, fetch_range_req(topic, 0)))) == 100
      assert length(fetched(process(Wire.fetch_range_key(), 4, fetch_range_req(topic, 5_000)))) == 1_000
    end
  end
end
