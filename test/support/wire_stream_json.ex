defmodule Malachi.Test.WireStreamJson do
  @moduledoc """
  The golden frames of the stream, consume, group and routing keys (`Malachi.Wire`, keys 24 to 34): how a
  value is written in their JSON fixtures (`test/support/fixtures/wire/{route,stream,consume,group}_frames.json`)
  and back, and the cases they were first written from.

  The fixtures are read by `test/malachi/wire_stream_frames_test.exs` and
  `scripts/lib/wire_streams.selftest.js`. Bytes are frozen once shipped: never regenerate a fixture, add a
  case. `write!/3` exists to write a new fixture file, once.

  In JSON, a position is `[source_index, offset]`, a record `{key, value, timestamp, headers}` with
  `headers` as `[[name, value], ...]`, and a batch `{codec, layout, records}`, its entries
  `{tombstone, record}`, or `{position, tombstone, record}` in a consume batch.
  """

  alias Malachi.Log.Record
  alias Malachi.Wire
  alias Malachi.Wire.Batch

  @dir "test/support/fixtures/wire"

  @doc "Encodes `value` (as decoded from JSON) with `codec`."
  @spec encode(String.t(), term()) :: binary()
  def encode("cluster_state_req", nil), do: Wire.encode_cluster_state_req()
  def encode("cluster_state_resp", v), do: Wire.encode_cluster_state_resp(cluster_state(v))
  def encode("topic_routes_req", v), do: Wire.encode_topic_routes_req(v)
  def encode("topic_routes_resp", v), do: Wire.encode_topic_routes_resp(routes(v))
  def encode("open_stream_req", v), do: Wire.encode_open_stream_req(open_stream(v))
  def encode("open_stream_resp", v), do: Wire.encode_open_stream_resp(atomize(v))
  def encode("append_req", v), do: bin(Wire.encode_append_req(v["stream_id"], v["sequence"], batch(v["batch"])))
  def encode("close_stream_req", v), do: Wire.encode_close_stream_req(v)
  def encode("push", [kind, v]), do: bin(Wire.encode_push(String.to_existing_atom(kind), push(kind, v)))
  def encode("open_consume_req", v), do: Wire.encode_open_consume_req(consume_req(v))

  def encode("open_consume_resp", v),
    do: Wire.encode_open_consume_resp(%{stream_id: v["stream_id"], position: pos(v["position"])})

  def encode("consume_ack_req", v), do: Wire.encode_consume_ack_req(%{atomize(v) | position: pos(v["position"])})
  def encode("fetch_range_req", v), do: Wire.encode_fetch_range_req(consume_req(v))
  def encode("page", v), do: bin(Wire.encode_page(page(v)))
  def encode("join_group_req", v), do: Wire.encode_join_group_req(atomize(v))
  def encode("group_heartbeat_req", v), do: Wire.encode_group_heartbeat_req(atomize(v))
  def encode("assignment_resp", v), do: Wire.encode_assignment_resp(atomize(v))
  def encode("commit_offsets_req", v), do: Wire.encode_commit_offsets_req(commit(v))

  @doc "Decodes `bytes` with `codec` into the value as it is written in JSON."
  @spec decode(String.t(), binary()) :: term()
  def decode("cluster_state_req", <<>>), do: nil
  def decode("cluster_state_resp", b), do: b |> Wire.decode_cluster_state_resp() |> json()
  def decode("topic_routes_req", b), do: Wire.decode_topic_routes_req(b)
  def decode("topic_routes_resp", b), do: b |> Wire.decode_topic_routes_resp() |> json()
  def decode("open_stream_req", b), do: b |> Wire.decode_open_stream_req() |> json()
  def decode("open_stream_resp", b), do: b |> Wire.decode_open_stream_resp() |> json()

  def decode("append_req", b) do
    {stream_id, sequence, batch} = Wire.decode_append_req(b)
    %{"stream_id" => stream_id, "sequence" => sequence, "batch" => batch_json(batch, :plain)}
  end

  def decode("close_stream_req", b), do: Wire.decode_close_stream_req(b)

  def decode("push", b) do
    {kind, push} = Wire.decode_push(b)
    [Atom.to_string(kind), push_json(kind, push)]
  end

  def decode("open_consume_req", b), do: b |> Wire.decode_open_consume_req() |> json()
  def decode("open_consume_resp", b), do: b |> Wire.decode_open_consume_resp() |> json()
  def decode("consume_ack_req", b), do: b |> Wire.decode_consume_ack_req() |> json()
  def decode("fetch_range_req", b), do: b |> Wire.decode_fetch_range_req() |> json()
  def decode("page", b), do: b |> Wire.decode_page() |> page_json()
  def decode("join_group_req", b), do: b |> Wire.decode_join_group_req() |> json()
  def decode("group_heartbeat_req", b), do: b |> Wire.decode_group_heartbeat_req() |> json()
  def decode("assignment_resp", b), do: b |> Wire.decode_assignment_resp() |> json()
  def decode("commit_offsets_req", b), do: b |> Wire.decode_commit_offsets_req() |> json()

  @doc "A batch from its JSON value."
  @spec batch(map()) :: binary()
  def batch(%{"codec" => codec, "layout" => layout, "records" => records}) do
    layout = String.to_existing_atom(layout)
    Batch.encode(Enum.map(records, &entry(&1, layout)), String.to_existing_atom(codec), layout)
  end

  @doc "The JSON value of an encoded batch (the codec as encoded, the records inflated)."
  @spec batch_json(binary(), Batch.layout()) :: map()
  def batch_json(batch, layout) do
    {%{codec: codec}, <<>>} = Batch.split(batch)
    {:ok, entries} = Batch.decode(batch, max_inflated_bytes: 16_777_216, layout: layout)

    %{
      "codec" => Atom.to_string(codec),
      "layout" => Atom.to_string(layout),
      "records" => Enum.map(entries, &entry_json/1)
    }
  end

  @doc """
  Writes `cases` (`[{codec, name, value}]`) as the fixture `file`, each with the bytes the Elixir codec
  encodes it to. For a new fixture file only: a shipped one is frozen.
  """
  @spec write!(String.t(), String.t(), [{String.t(), String.t(), term()}]) :: :ok
  def write!(file, about, cases) do
    path = Path.join(@dir, file)
    if File.exists?(path), do: raise("#{path} exists: shipped fixtures are frozen, add a case instead")

    cases =
      for {codec, name, value} <- cases,
          do: %{
            "codec" => codec,
            "name" => name,
            "value" => value,
            "hex" => Base.encode16(encode(codec, value), case: :lower)
          }

    File.write!(path, Jason.encode!(%{"about" => about, "cases" => cases}, pretty: true) <> "\n")
  end

  defp cluster_state(v) do
    %{
      version: v["version"],
      streams_enabled: v["streams_enabled"],
      brokers: Enum.map(v["brokers"], &%{atomize(&1) | status: String.to_existing_atom(&1["status"])}),
      vnodes: v["vnodes"]
    }
  end

  defp routes(v) do
    ranges =
      Enum.map(v["ranges"], fn r ->
        %{atomize(r) | state: String.to_existing_atom(r["state"]), segment: r["segment"] && atomize(r["segment"])}
      end)

    %{topic: v["topic"], version: v["version"], keyspace_bits: v["keyspace_bits"], ranges: ranges}
  end

  defp open_stream(v), do: %{atomize(v) | codec: String.to_existing_atom(v["codec"])}

  defp consume_req(v) do
    %{atomize(v) | start: start(v["start"]), accept: Enum.map(v["accept"], &String.to_existing_atom/1)}
  end

  defp start("earliest"), do: :earliest
  defp start("latest"), do: :latest
  defp start(%{"position" => position}), do: {:position, pos(position)}
  defp start(%{"committed" => group}), do: {:committed, group}

  defp push("append_ack", v), do: %{atomize(v) | errors: Enum.map(v["errors"], &atomize/1)}

  defp push("moved", v) do
    %{
      atomize(v)
      | targets: Enum.map(v["targets"], &%{range: &1["range"], segment: &1["segment"] && atomize(&1["segment"])})
    }
  end

  defp push("records", v), do: Map.put(page(v), :stream_id, v["stream_id"])

  defp page(v) do
    %{
      next: pos(v["next"]),
      skip: v["skip"],
      backlog: v["backlog"],
      expired: v["expired"],
      expired_exact: v["expired_exact"],
      batch: batch(v["batch"])
    }
  end

  defp commit(v) do
    %{atomize(v) | positions: Enum.map(v["positions"], &%{range: &1["range"], position: pos(&1["position"])})}
  end

  defp entry(%{"tombstone" => t, "record" => r}, :plain), do: {record(r), t}
  defp entry(%{"position" => p, "tombstone" => t, "record" => r}, :positioned), do: {pos(p), record(r), t}

  defp record(%{"key" => key, "value" => value, "timestamp" => ts, "headers" => headers}),
    do: %Record{key: key, value: value, timestamp: ts, headers: Enum.map(headers, &List.to_tuple/1)}

  defp entry_json({record, tombstone}), do: %{"tombstone" => tombstone, "record" => record_json(record)}

  defp entry_json({position, record, tombstone}),
    do: %{"position" => Tuple.to_list(position), "tombstone" => tombstone, "record" => record_json(record)}

  defp record_json(%Record{key: key, value: value, timestamp: ts, headers: headers}),
    do: %{"key" => key, "value" => value, "timestamp" => ts, "headers" => Enum.map(headers, &Tuple.to_list/1)}

  defp push_json(:records, push), do: push |> page_json() |> Map.put("stream_id", push.stream_id)
  defp push_json(_kind, push), do: json(push)

  defp page_json(page) do
    %{
      "next" => Tuple.to_list(page.next),
      "skip" => page.skip,
      "backlog" => page.backlog,
      "expired" => page.expired,
      "expired_exact" => page.expired_exact,
      "batch" => batch_json(page.batch, :positioned)
    }
  end

  defp pos([source, offset]), do: {source, offset}

  defp atomize(map), do: Map.new(map, fn {key, value} -> {String.to_existing_atom(key), value} end)

  # A decoded term as it is written in JSON: atoms other than booleans and nil as strings, tuples as lists
  # (positions), {:position, p} and {:committed, g} starts as objects.
  defp json(%{} = map), do: Map.new(map, fn {key, value} -> {to_string(key), json(value)} end)
  defp json(list) when is_list(list), do: Enum.map(list, &json/1)
  defp json({:position, position}), do: %{"position" => json(position)}
  defp json({:committed, group}), do: %{"committed" => group}
  defp json(tuple) when is_tuple(tuple), do: tuple |> Tuple.to_list() |> json()
  defp json(value) when is_boolean(value) or is_nil(value), do: value
  defp json(atom) when is_atom(atom), do: Atom.to_string(atom)
  defp json(value), do: value

  defp bin(iodata), do: IO.iodata_to_binary(iodata)
end
