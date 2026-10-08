defmodule WireStreamsJsTest do
  # The Node side of the stream, consume, group and routing keys: its codecs against the golden frames the
  # Elixir codec is tested with, its keyspace hash against the broker's, and zstd batches across the two
  # runtimes, which have no golden bytes (each compressor writes its own) but must carry the same records.
  use ExUnit.Case, async: true

  alias Malachi.Log.Record
  alias Malachi.Wire.Batch

  @selftest Path.expand("../../scripts/lib/wire_streams.selftest.js", __DIR__)

  setup_all do
    # A missing runtime fails loudly instead of skipping: a skipped client test reads as a passing one.
    %{node: System.find_executable("node") || flunk("node is required; install Node 22.15+ (zstd in zlib)")}
  end

  test "the Node codecs and keyspace hash agree with every golden frame and vector", ctx do
    assert {output, 0} = System.cmd(ctx.node, [@selftest], stderr_to_stdout: true)
    assert output =~ "passed 32 golden frames and 426 keyspace positions"
  end

  test "a zstd batch Node compressed reads back here record for record", ctx do
    assert {hex, 0} = System.cmd(ctx.node, [@selftest, "--encode-zstd"])
    batch = hex |> String.trim() |> Base.decode16!(case: :lower)
    assert {%{codec: :zstd, count: 2}, <<>>} = Batch.split(batch)

    assert {:ok, [{first, false}, {second, true}]} = Batch.decode(batch, max_inflated_bytes: 1_048_576)
    assert %Record{key: "k1", value: value, timestamp: 1_767_225_600_000, headers: []} = first
    assert value == String.duplicate("v", 200)
    assert %Record{key: nil, timestamp: 1_767_225_600_001, headers: [{"trace", "abc"}]} = second
  end

  test "a zstd batch compressed here reads back in Node record for record", ctx do
    records = [
      {%Record{key: "k1", value: String.duplicate("x", 300), timestamp: 42, headers: [{"h", "1"}]}, false},
      {%Record{key: "k1", value: "", timestamp: 43, headers: []}, true}
    ]

    hex = records |> Batch.encode(:zstd) |> Base.encode16(case: :lower)
    assert {json, 0} = System.cmd(ctx.node, [@selftest, "--decode", hex])

    assert Jason.decode!(json) == %{
             "codec" => "zstd",
             "layout" => "plain",
             "records" => [
               %{
                 "tombstone" => false,
                 "record" => %{
                   "key" => "k1",
                   "value" => String.duplicate("x", 300),
                   "timestamp" => 42,
                   "headers" => [["h", "1"]]
                 }
               },
               %{"tombstone" => true, "record" => %{"key" => "k1", "value" => "", "timestamp" => 43, "headers" => []}}
             ]
           }
  end
end
