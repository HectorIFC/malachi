defmodule Malachi.Cluster.ScrubberTracingTest do
  # The scrub's local scan is deliberately untraced: it has no parent trace (nothing requested it),
  # so a span per verified segment would be a root span of background housekeeping. The REPAIR is
  # different: it verifies a peer over the network, changes the replica order through the control
  # plane and copies a whole segment between nodes, which is exactly the distributed latency a trace
  # is for. async: false: it sets the global span exporter.
  use ExUnit.Case, async: false
  require Record

  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.Scrubber
  alias Malachi.Log.Record, as: LogRecord
  alias Malachi.Metadata
  alias Malachi.Storage.Layout

  @span_fields Record.extract(:span, from_lib: "opentelemetry/include/otel_span.hrl")
  Record.defrecordp(:span, @span_fields)

  @segment {{"events", 0}, 0}

  setup do
    :otel_simple_processor.set_exporter(:otel_exporter_pid, self())
    :ok
  end

  defp await_span(name, timeout \\ 2_000) do
    receive do
      {:span, s} -> if span(s, :name) == name, do: s, else: await_span(name, timeout)
    after
      timeout -> flunk("no span named #{name} was exported")
    end
  end

  defp attrs(s), do: :otel_attributes.map(span(s, :attributes))

  defp start_replica do
    name = :"scrub_trace_repl_#{System.unique_integer([:positive])}"
    directory = Path.join(System.tmp_dir!(), "malachi_scrub_trace_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    start_supervised!({ReplicationServer, [name: name, directory: directory]}, id: name)
    {{name, node()}, directory}
  end

  defp start_scrubber(opts) do
    opts = Keyword.merge([apply_command: fn _c -> :ok end, on_result: fn _r -> :ok end, interval: 60_000], opts)
    start_supervised!({Scrubber, opts}, id: {:scrubber, System.unique_integer([:positive])})
  end

  test "a repair is traced, a clean pass is not" do
    values = ["a", "b", "c"]
    records = for value <- values, do: LogRecord.new(value, key: value)
    {damaged, damaged_directory} = start_replica()
    {peer, peer_directory} = start_replica()

    for replica <- [damaged, peer] do
      {:ok, _last} = ReplicationServer.follow(replica, @segment, 0, records)
    end

    {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})
    {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, @segment, [damaged, peer], 0})
    bytes = ReplicationServer.stored_bytes(peer, @segment)
    {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, @segment, length(values), bytes, 0})

    peer_scrubber = start_scrubber(metadata_source: fn -> metadata end, local_ref: peer, directory: peer_directory)

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged,
        directory: damaged_directory,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    # a clean pass exports nothing
    assert %{damaged: []} = Scrubber.scrub_now(scrubber)
    refute_receive {:span, _clean_pass_span}, 200

    # now rot the local copy and let the repair run
    [log_file] = damaged_directory |> Layout.segment_directory(@segment) |> Path.join("*.log") |> Path.wildcard()
    {pairs, _valid} = LogRecord.decode_all(File.read!(log_file))
    {_record, position} = Enum.at(pairs, 1)
    flip_at = position + 12
    <<head::binary-size(flip_at), byte, tail::binary>> = File.read!(log_file)
    File.write!(log_file, <<head::binary, Bitwise.bxor(byte, 0xFF), tail::binary>>)

    assert %{repaired: [@segment]} = Scrubber.scrub_now(scrubber)

    repair = await_span("malachi.scrub.repair")
    attributes = attrs(repair)
    assert attributes["malachi.segment"] == inspect(@segment)
    assert attributes["malachi.replicas"] == 2
    assert attributes["malachi.repair"] == "ok"
  end
end
