defmodule Malachi.WireStreamFramesTest do
  @moduledoc """
  The stream, consume, group and routing keys (24 to 34) against the golden frames both clients read
  (`test/support/fixtures/wire/{route,stream,consume,group}_frames.json`; the Node side is
  `scripts/lib/wire_streams.selftest.js`). A shipped frame is frozen, so a difference here is a wire
  break, not a fixture to regenerate.
  """
  use ExUnit.Case, async: true

  alias Malachi.Test.WireStreamJson
  alias Malachi.Wire
  alias Malachi.Wire.Batch

  @fixtures ~w(route_frames.json stream_frames.json consume_frames.json group_frames.json)
  for fixture <- @fixtures, do: @external_resource(Path.join("test/support/fixtures/wire", fixture))

  @cases Enum.flat_map(@fixtures, fn fixture ->
           "test/support/fixtures/wire" |> Path.join(fixture) |> File.read!() |> Jason.decode!() |> Map.fetch!("cases")
         end)

  test "the fixtures cover every codec of the new keys, both directions" do
    codecs = @cases |> Enum.map(& &1["codec"]) |> Enum.uniq() |> Enum.sort()

    assert codecs ==
             Enum.sort(~w(cluster_state_req cluster_state_resp topic_routes_req topic_routes_resp open_stream_req
               open_stream_resp append_req close_stream_req push open_consume_req open_consume_resp consume_ack_req
               fetch_range_req page join_group_req group_heartbeat_req assignment_resp commit_offsets_req))

    kinds = for %{"codec" => "push", "value" => [kind, _]} <- @cases, uniq: true, do: kind
    assert Enum.sort(kinds) == ~w(append_ack moved records)

    starts = for %{"codec" => "open_consume_req", "value" => %{"start" => start}} <- @cases, do: start
    assert "earliest" in starts and "latest" in starts
    assert Enum.any?(starts, &match?(%{"position" => _}, &1))
    assert Enum.any?(starts, &match?(%{"committed" => _}, &1))
  end

  for %{"name" => name, "codec" => codec, "value" => value, "hex" => hex} <- @cases do
    test "#{codec}: #{name}" do
      bytes = Base.decode16!(unquote(hex), case: :lower)
      assert WireStreamJson.encode(unquote(codec), unquote(Macro.escape(value))) == bytes
      assert WireStreamJson.decode(unquote(codec), bytes) == unquote(Macro.escape(value))
    end
  end

  test "the api keys are the next free numbers after the console role keys" do
    assert [
             Wire.cluster_state_key(),
             Wire.topic_routes_key(),
             Wire.open_stream_key(),
             Wire.append_key(),
             Wire.close_stream_key(),
             Wire.open_consume_key(),
             Wire.consume_ack_key(),
             Wire.fetch_range_key(),
             Wire.join_group_key(),
             Wire.group_heartbeat_key(),
             Wire.commit_offsets_key()
           ] == Enum.to_list(24..34)
  end

  # The connection decodes inside a `try` and answers any raise as malformed_request
  # (`Malachi.TCPProtocol.process_frame/4`), so what matters is that a malformed payload raises at all.
  defp assert_malformed(decode) do
    outcome =
      try do
        decode.()
      rescue
        _exception -> :raised
      end

    assert outcome == :raised, "decoded a malformed payload as #{inspect(outcome)}"
  end

  describe "a malformed payload raises, which the connection answers as malformed_request" do
    test "trailing bytes" do
      req = %{topic: "t", group: "g", member: "m", generation: 1}
      assert_malformed(fn -> Wire.decode_group_heartbeat_req(Wire.encode_group_heartbeat_req(req) <> <<0>>) end)
      assert_malformed(fn -> Wire.decode_close_stream_req(<<5::32, 0>>) end)
      assert_malformed(fn -> Wire.decode_topic_routes_req(Wire.encode_topic_routes_req("t") <> <<0>>) end)
    end

    test "an append whose batch is cut short, or followed by more bytes" do
      batch = Batch.encode([{Malachi.Log.Record.new("v"), false}], :none)
      append = IO.iodata_to_binary(Wire.encode_append_req(1, 0, batch))
      assert_malformed(fn -> Wire.decode_append_req(binary_part(append, 0, byte_size(append) - 1)) end)
      assert_malformed(fn -> Wire.decode_append_req(append <> <<0>>) end)
    end

    test "an unknown push kind, start kind, codec, broker status or range state" do
      assert_malformed(fn -> Wire.decode_push(<<3::8, 1::32>>) end)
      assert_malformed(fn -> Batch.codec_of(9) end)

      req = %{
        topic: "t",
        range: 0,
        routes_version: 0,
        start: :earliest,
        window: 1,
        max: 1,
        max_bytes: 1,
        accept: [:none]
      }

      <<head::binary-size(18), _start::8, tail::binary>> = Wire.encode_open_consume_req(req)
      assert_malformed(fn -> Wire.decode_open_consume_req(<<head::binary, 9::8, tail::binary>>) end)

      state = %{
        version: 0,
        streams_enabled: true,
        brokers: [%{id: "b", host: nil, port: 0, status: :alive}],
        vnodes: []
      }

      bytes = Wire.encode_cluster_state_resp(state)
      bad_status = binary_part(bytes, 0, byte_size(bytes) - 3) <> <<7::8, 0::16>>
      assert_malformed(fn -> Wire.decode_cluster_state_resp(bad_status) end)
    end
  end

  test "a stream cannot open with a window of 0, from either end" do
    stream = %{
      topic: "t",
      range: 0,
      routes_version: 0,
      codec: :none,
      window_appends: 1,
      window_bytes: 1,
      producer_id: nil,
      label: nil
    }

    consume = %{
      topic: "t",
      range: 0,
      routes_version: 0,
      start: :earliest,
      window: 1,
      max: 1,
      max_bytes: 1,
      accept: [:none]
    }

    for bad <- [0, -1, 1.5, nil, 4_294_967_296] do
      for req <- [%{stream | window_appends: bad}, %{stream | window_bytes: bad}] do
        assert_raise ArgumentError, ~r/from 1 to 4294967295/, fn -> Wire.encode_open_stream_req(req) end
      end

      assert_raise ArgumentError, ~r/from 1 to 4294967295/, fn ->
        Wire.encode_open_consume_req(%{consume | window: bad})
      end
    end

    # What a client that skipped the check would send: one window field zeroed in a valid request. The
    # fields sit after the topic ("t": a presence byte, a u32 length, one byte), the u32 range, the u64
    # routes version and, in an open_stream, the codec byte; in an open_consume, the one-byte :earliest start.
    head = 1 + 4 + 1 + 4 + 8
    stream_bytes = Wire.encode_open_stream_req(%{stream | window_appends: 300, window_bytes: 70_000})

    for at <- [head + 1, head + 1 + 4] do
      <<before::binary-size(at), window::32, rest::binary>> = stream_bytes
      assert window in [300, 70_000]
      assert_malformed(fn -> Wire.decode_open_stream_req(<<before::binary, 0::32, rest::binary>>) end)
    end

    <<before::binary-size(head + 1), 300::32, rest::binary>> = Wire.encode_open_consume_req(%{consume | window: 300})
    assert_malformed(fn -> Wire.decode_open_consume_req(<<before::binary, 0::32, rest::binary>>) end)
  end

  test "a list past what its u16 count can say raises at the sender, rather than wrap" do
    at_most = %{generation: 1, session_ms: 1, ranges: Enum.to_list(1..65_535)}
    assert Wire.decode_assignment_resp(Wire.encode_assignment_resp(at_most)) == at_most

    assert_raise ArgumentError, ~r/65536 items/, fn ->
      Wire.encode_assignment_resp(%{at_most | ranges: Enum.to_list(0..65_535)})
    end
  end

  test "a request takes the batch already encoded, so a sender encodes it once" do
    batch = Batch.encode([{Malachi.Log.Record.new("v", key: "k"), false}], :none)
    assert [<<1::32, 9::64>>, ^batch] = Wire.encode_append_req(1, 9, batch)
  end
end
