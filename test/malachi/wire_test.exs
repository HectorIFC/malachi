defmodule Malachi.WireTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Log.Record
  alias Malachi.Wire
  alias Malachi.Wire.Batch

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
    test "a request sent as iodata is the bytes of the encoded request, whatever shape the payload has" do
      batch = Batch.encode([{%Record{key: "k", value: "v", timestamp: 7, headers: [{"h", "x"}]}, false}], :none)
      append = Wire.encode_append_req(5, 0, batch)
      expected = Wire.encode_request(Wire.append_key(), 9, IO.iodata_to_binary(append))

      for payload <- [append, IO.iodata_to_binary(append), [IO.iodata_to_binary(append)]] do
        assert payload |> then(&Wire.encode_request_iodata(Wire.append_key(), 9, &1)) |> IO.iodata_to_binary() ==
                 expected
      end

      assert Wire.encode_request_iodata(Wire.append_key(), 1, []) |> IO.iodata_to_binary() ==
               Wire.encode_request(Wire.append_key(), 1, "")
    end

    test "request round-trip carries api_key, correlation_id and payload" do
      {:ok, body, ""} = Wire.decode_frame(Wire.encode_request(Wire.append_key(), 42, "PL"))
      assert Wire.decode_request(body) == {Wire.append_key(), 42, "PL"}
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

    test "leave_group req round-trip" do
      assert Wire.decode_leave_group_req(Wire.encode_leave_group_req("t", "grp", "m1")) == {"t", "grp", "m1"}
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
  end
end
