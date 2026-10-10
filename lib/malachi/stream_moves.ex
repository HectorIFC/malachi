defmodule Malachi.StreamMoves do
  @moduledoc """
  What a connection's producer streams (`Malachi.ProducerStreams`) and consume streams
  (`Malachi.ConsumeStreams`) share about moving: the broker processes they watch, and the `moved` push that
  tells a client where a stream's range is served now.

  A stream is held in the index of the broker process that answered its open, which tells it when its
  range moves on. That index goes with the process, so the connection watches each such process once, and
  a `:DOWN` from one moves every stream it held (reason `restarted`, to its own range): the client opens it
  again where the routes say.
  """

  alias Malachi.Metadata
  alias Malachi.Wire

  @typedoc "The broker processes a connection watches, by monitor ref."
  @type brokers :: %{reference() => pid()}

  @doc "Watches `broker_pid` unless it is watched already: once per process, whatever streams it holds."
  @spec watch(brokers(), pid()) :: brokers()
  def watch(brokers, broker_pid) do
    if broker_pid in Map.values(brokers),
      do: brokers,
      else: Map.put(brokers, Process.monitor(broker_pid), broker_pid)
  end

  @doc "The watched broker a `:DOWN` with `ref` is about, and the rest; `:error` for another monitor."
  @spec down(brokers(), reference()) :: {pid(), brokers()} | :error
  def down(brokers, ref) do
    case Map.pop(brokers, ref) do
      {nil, _brokers} -> :error
      {broker_pid, brokers} -> {broker_pid, brokers}
    end
  end

  @doc """
  The `moved` push for stream `stream_id`, opened with correlation id `corr` on `topic`: the reason, the
  routes version the client compares its own copy with, and each range the stream's data goes to now with
  its active segment and primary when it has one.
  """
  @spec moved_frame(pos_integer(), non_neg_integer(), String.t(), atom(), list()) :: binary()
  def moved_frame(stream_id, corr, topic, reason, targets) do
    push = %{
      stream_id: stream_id,
      reason: Atom.to_string(reason),
      routes_version: routes_version(topic),
      targets: Enum.map(targets, &target/1)
    }

    Wire.encode_ok(corr, IO.iodata_to_binary(Wire.encode_push(:moved, push)))
  end

  defp target({{_topic, seq}, nil}), do: %{range: seq, segment: nil}

  defp target({{_topic, seq}, {segment_id, primary}}),
    do: %{
      range: seq,
      segment: %{segment: Metadata.segment_seq(segment_id), primary: Metadata.broker_ref_string(primary)}
    }

  # The version of the routes a moved client reads next, so it can tell whether its own copy is current; 0
  # when the routes cannot be read right now (the client reads them again either way).
  defp routes_version(topic) do
    case Malachi.Routing.read_topic_routes(topic) do
      {:ok, %{version: version}} -> version
      {:error, _reason} -> 0
    end
  end
end
