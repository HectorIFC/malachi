defmodule Malachi.Log.RecordTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Log.Record

  describe "encoded_size/1" do
    test "matches the encoded frame size across key/value/header shapes" do
      records = [
        Record.new("value", key: "key"),
        # nil key (absent) vs empty-binary key: same length here, but exercise both flags
        Record.new("value", key: nil),
        Record.new("value", key: ""),
        Record.new("", key: "k"),
        Record.new("value", key: "k", headers: [{"h1", "v1"}, {"h2", ""}])
      ]

      for record <- records do
        with_offset = %{record | offset: 42}
        assert Record.encoded_size(record) == byte_size(Record.encode(with_offset))
      end
    end

    property "equals byte_size(encode/1) for any record" do
      check all(
              value <- binary(),
              key <- one_of([constant(nil), binary()]),
              headers <- list_of({binary(), binary()}, max_length: 4),
              offset <- integer(0..1_000_000)
            ) do
        record = %{Record.new(value, key: key, headers: headers) | offset: offset}
        assert Record.encoded_size(record) == byte_size(Record.encode(record))
      end
    end
  end
end
