defmodule Malachi.WireTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Log.Record
  alias Malachi.Wire

  describe "frame" do
    test "round-trips a body" do
      assert {:ok, "hello", ""} = Wire.decode_frame(Wire.encode_frame("hello"))
    end

    test "reports :incomplete when the whole frame is not present yet" do
      frame = Wire.encode_frame("hello")
      # every strict prefix is incomplete
      for cut <- 0..(byte_size(frame) - 1) do
        assert Wire.decode_frame(binary_part(frame, 0, cut)) == :incomplete
      end
    end

    test "peels one frame and returns the rest (framing over a stream)" do
      buffer = Wire.encode_frame("aaa") <> Wire.encode_frame("bb")
      assert {:ok, "aaa", rest} = Wire.decode_frame(buffer)
      assert {:ok, "bb", ""} = Wire.decode_frame(rest)
    end
  end

  describe "envelope" do
    test "request round-trip carries api_key, correlation_id and payload" do
      {:ok, body, ""} = Wire.decode_frame(Wire.encode_request(Wire.produce_key(), 42, "PL"))
      assert Wire.decode_request(body) == {Wire.produce_key(), 42, "PL"}
    end

    test "response round-trip carries correlation_id, error_code and payload" do
      {:ok, body, ""} = Wire.decode_frame(Wire.encode_response(42, 7, "PL"))
      assert Wire.decode_response(body) == {42, 7, "PL"}
    end
  end

  describe "wire record (no offset)" do
    test "round-trips key/value/headers/timestamp" do
      record = %Record{key: "k1", value: "v1", timestamp: 1_700_000_000_000, headers: [{"h", "x"}], offset: nil}
      assert {^record, ""} = Wire.decode_record(Wire.encode_record(record))
    end

    test "a nil key is distinct from an empty key" do
      nil_key = %Record{key: nil, value: "v", timestamp: 1, headers: [], offset: nil}
      empty_key = %Record{key: "", value: "v", timestamp: 1, headers: [], offset: nil}

      assert {%Record{key: nil}, ""} = Wire.decode_record(Wire.encode_record(nil_key))
      assert {%Record{key: ""}, ""} = Wire.decode_record(Wire.encode_record(empty_key))
      refute Wire.encode_record(nil_key) == Wire.encode_record(empty_key)
    end

    test "carries arbitrary (non-UTF-8) value bytes" do
      record = %Record{key: "k", value: <<0, 255, 1, 254>>, timestamp: 1, headers: [], offset: nil}
      assert {%Record{value: <<0, 255, 1, 254>>}, ""} = Wire.decode_record(Wire.encode_record(record))
    end

    property "round-trips any key/value/headers" do
      check all(
              key <- one_of([constant(nil), binary()]),
              value <- binary(),
              headers <- list_of({binary(min_length: 1), binary()}, max_length: 4),
              ts <- integer(0..2_000_000_000_000)
            ) do
        record = %Record{key: key, value: value, timestamp: ts, headers: headers, offset: nil}
        assert {^record, ""} = Wire.decode_record(Wire.encode_record(record))
      end
    end
  end

  describe "operation payloads" do
    test "create_topic req round-trip" do
      assert Wire.decode_create_topic_req(Wire.encode_create_topic_req("events", 8)) == {"events", 8}
    end

    test "produce req round-trip (multiple records)" do
      records = for i <- 1..3, do: %Record{key: "k#{i}", value: "v#{i}", timestamp: i, headers: [], offset: nil}
      assert Wire.decode_produce_req(Wire.encode_produce_req("t", records)) == {"t", records}
    end

    test "fetch req round-trip: cursor, group, member, and :start (all nil)" do
      assert Wire.decode_fetch_req(Wire.encode_fetch_req("t", <<1, 2, 3>>, nil, nil, 500, 30_000)) ==
               {"t", <<1, 2, 3>>, nil, nil, 500, 30_000}

      assert Wire.decode_fetch_req(Wire.encode_fetch_req("t", nil, "grp", nil, 100, 0)) ==
               {"t", nil, "grp", nil, 100, 0}

      # a group member id rides alongside the group (server scopes the fetch to its ranges)
      assert Wire.decode_fetch_req(Wire.encode_fetch_req("t", nil, "grp", "m1", 100, 0)) ==
               {"t", nil, "grp", "m1", 100, 0}

      assert Wire.decode_fetch_req(Wire.encode_fetch_req("t", nil, nil, nil, 100, 0)) ==
               {"t", nil, nil, nil, 100, 0}
    end

    test "leave_group req round-trip" do
      assert Wire.decode_leave_group_req(Wire.encode_leave_group_req("t", "grp", "m1")) == {"t", "grp", "m1"}
    end

    test "subscribe req round-trip carries an optional member id" do
      assert Wire.decode_subscribe_req(Wire.encode_subscribe_req("t", "g", "m1", 10, 100)) == {"t", "g", "m1", 10, 100}
      assert Wire.decode_subscribe_req(Wire.encode_subscribe_req("t", "g", nil, 10, 100)) == {"t", "g", nil, 10, 100}
    end

    test "stream_ack req round-trip carries an optional member id" do
      assert Wire.decode_stream_ack_req(Wire.encode_stream_ack_req("t", "g", "m1", <<1, 2>>, 5)) ==
               {"t", "g", "m1", <<1, 2>>, 5}

      assert Wire.decode_stream_ack_req(Wire.encode_stream_ack_req("t", "g", nil, nil, 0)) ==
               {"t", "g", nil, nil, 0}
    end

    test "auth req/resp round-trip" do
      assert Wire.decode_auth_req(Wire.encode_auth_req("user", "pass")) == {"user", "pass"}
      assert Wire.decode_auth_resp(Wire.encode_auth_resp("token123")) == "token123"
    end

    test "encode_ok / encode_error produce framed responses with the right code" do
      {:ok, ok_body, ""} = Wire.decode_frame(Wire.encode_ok(7, "PL"))
      assert Wire.decode_response(ok_body) == {7, Wire.ok_code(), "PL"}

      {:ok, err_body, ""} = Wire.decode_frame(Wire.encode_error(7, :not_found))
      assert {7, error_code, payload} = Wire.decode_response(err_body)
      assert error_code == Wire.error_code()
      # the reason travels as a length-prefixed string in the payload
      assert Wire.decode_auth_resp(payload) == "not_found"
    end

    test "fetch resp round-trip (records + next cursor)" do
      records = [%Record{key: nil, value: "v", timestamp: 1, headers: [], offset: nil}]
      assert Wire.decode_fetch_resp(Wire.encode_fetch_resp(records, <<9, 9>>)) == {records, <<9, 9>>}
    end

    test "commit req round-trip" do
      assert Wire.decode_commit_req(Wire.encode_commit_req("t", "grp", <<4, 5>>)) == {"t", "grp", <<4, 5>>}
    end
  end
end
