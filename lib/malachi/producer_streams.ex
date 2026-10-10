defmodule Malachi.ProducerStreams do
  @moduledoc """
  The producer streams one connection holds (`Malachi.Wire` keys 26 to 28), as NorthGuard's producers
  stream to the broker that leads a range's active segment (meetup transcript, 551 to 560).

  A stream is bound to one range and opened on the node that leads the range's active segment
  (`Malachi.BrokerServer.open_stream/3`). Each `append` carries the next sequence number, starting at 0;
  one out of order, a gap or a repeat, ends the stream with an error at that sequence. An append is sent
  to the broker without waiting (`:gen_server.send_request/4`), so a producer pipelines up to its window,
  and each answer becomes an `append_ack` push: the sequence below which every append has been answered
  (0 before the first answer), the errors of the ones that failed, and the window the range's load leaves
  (`Malachi.StreamWindow`). When the broker reports the range moved on (its segment sealed, by a roll or a
  failover, or a split or merge retired it) or the broker itself went down, the stream is answered with a
  `moved` push naming where its range's data goes now, and closed: it takes no more appends, but the
  appends already in flight are still answered, so a producer learns which of them landed before it
  resends anything.

  The connection reads its socket only while every open stream has room in its window, so a producer that
  outruns its grant is held back by TCP rather than buffered here.

  Pure but for the broker calls: frames and broker answers in, pushes and the next state out.
  """

  alias Malachi.BrokerServer
  alias Malachi.Log.Record
  alias Malachi.Metadata
  alias Malachi.StreamMoves
  alias Malachi.StreamWindow
  alias Malachi.Wire
  alias Malachi.Wire.Batch

  defstruct streams: %{}, by_token: %{}, reqids: nil, brokers: %{}

  @typedoc "One open stream."
  @type stream :: %{
          id: pos_integer(),
          corr: non_neg_integer(),
          broker: GenServer.server(),
          topic: String.t(),
          range_id: Metadata.range_id(),
          token: reference(),
          broker_pid: pid(),
          granted: StreamWindow.t(),
          window: StreamWindow.t(),
          next_seq: non_neg_integer(),
          inflight: %{non_neg_integer() => non_neg_integer()},
          inflight_bytes: non_neg_integer(),
          answered: %{non_neg_integer() => :ok | {:error, String.t()}},
          acked: non_neg_integer(),
          open?: boolean()
        }

  @type t :: %__MODULE__{
          streams: %{pos_integer() => stream()},
          by_token: %{reference() => pos_integer()},
          reqids: :gen_server.request_id_collection() | nil,
          brokers: StreamMoves.brokers()
        }

  @typedoc """
  Charges an append's inflated records and bytes to the connection's quotas: `:ok`, or the error the append
  is answered with at its sequence.
  """
  @type charge :: (non_neg_integer(), non_neg_integer() -> :ok | {:error, atom()})

  @typedoc "What a step leaves for the connection to write: frames, each already a whole response frame."
  @type frames :: [binary()]

  @doc "No stream open yet."
  @spec new() :: t()
  def new, do: %__MODULE__{reqids: :gen_server.reqids_new()}

  @doc """
  Records a stream the broker opened (`token` and `broker_pid`, from `Malachi.BrokerServer.open_stream/3`)
  under `id`, which the connection gives every stream it holds, of either kind
  (`Malachi.ConnectionStreams`). `granted` is the window the handshake granted.
  """
  @spec open(t(), map()) :: t()
  def open(%__MODULE__{} = state, %{
        id: id,
        corr: corr,
        broker: broker,
        broker_pid: broker_pid,
        topic: topic,
        range_id: range_id,
        token: token,
        granted: granted
      }) do
    stream = %{
      id: id,
      corr: corr,
      broker: broker,
      topic: topic,
      range_id: range_id,
      token: token,
      broker_pid: broker_pid,
      granted: granted,
      window: granted,
      next_seq: 0,
      inflight: %{},
      inflight_bytes: 0,
      answered: %{},
      acked: 0,
      open?: true
    }

    # The process watched is the one that answered the open, whose index holds the stream: one restarted
    # under the same name since is another process, and one already gone answers with `:DOWN` at once.
    %{
      state
      | streams: Map.put(state.streams, id, stream),
        by_token: Map.put(state.by_token, token, id),
        brokers: StreamMoves.watch(state.brokers, broker_pid)
    }
  end

  @doc """
  Takes one append: checks its sequence and window, decodes its batch within `max_inflated_bytes`, charges
  its inflated records and bytes to `charge` (the connection's publish quotas), and sends it to the broker.
  An append that cannot be sent is answered at once with an error at its sequence; one out of sequence
  ends the stream. `{:unknown, stream_id}` for a stream this connection does not hold.
  """
  @spec append(t(), pos_integer(), non_neg_integer(), binary(), pos_integer(), charge()) ::
          {t(), frames()} | {:unknown, pos_integer()}
  def append(%__MODULE__{} = state, stream_id, sequence, batch, max_inflated_bytes, charge) do
    case Map.fetch(state.streams, stream_id) do
      :error -> {:unknown, stream_id}
      {:ok, %{open?: false}} -> {:unknown, stream_id}
      {:ok, stream} when sequence != stream.next_seq -> out_of_sequence(state, stream, sequence)
      {:ok, stream} -> in_sequence(state, stream, sequence, batch, max_inflated_bytes, charge)
    end
  end

  @doc """
  Folds a message the connection received into the streams: a broker answer to an append, or a move. `:no`
  for a message that belongs to neither.
  """
  @spec handle_message(t(), term()) :: {t(), frames()} | :no
  def handle_message(%__MODULE__{} = state, {:stream_moved, token, reason, targets}) do
    case Map.fetch(state.by_token, token) do
      :error ->
        :no

      {:ok, id} ->
        stream = Map.fetch!(state.streams, id)
        {stop(state, stream), [moved_frame(stream, reason, targets)]}
    end
  end

  # A broker this connection streams to went down, and with it the index that would tell its streams they
  # moved: each is told now, to its own range, and the producer opens it again where the routes say.
  def handle_message(%__MODULE__{brokers: brokers} = state, {:DOWN, ref, :process, _pid, _reason})
      when is_map_key(brokers, ref) do
    {broker_pid, brokers} = StreamMoves.down(brokers, ref)
    gone = for {_id, %{broker_pid: ^broker_pid, open?: true} = stream} <- state.streams, do: stream
    state = Enum.reduce(gone, %{state | brokers: brokers}, &stop(&2, &1))
    {state, Enum.map(gone, &moved_frame(&1, :restarted, [{&1.range_id, nil}]))}
  end

  def handle_message(%__MODULE__{reqids: reqids} = state, message) do
    case :gen_server.check_response(message, reqids, true) do
      {{:reply, {reply, scale}}, {stream_id, sequence}, reqids} ->
        answered(%{state | reqids: reqids}, stream_id, sequence, reply, scale)

      {{:error, {reason, _server}}, {stream_id, sequence}, reqids} ->
        answered(%{state | reqids: reqids}, stream_id, sequence, {:error, reason}, 1.0)

      :no_request ->
        :no

      :no_reply ->
        :no
    end
  end

  @doc "Closes a stream the client is done with. `:unknown` for one this connection does not hold."
  @spec close(t(), pos_integer()) :: {:ok, t()} | :unknown
  def close(%__MODULE__{} = state, stream_id) do
    case Map.fetch(state.streams, stream_id) do
      {:ok, %{open?: true} = stream} ->
        :ok = BrokerServer.close_stream(stream.broker, stream.token)
        {:ok, stop(state, stream)}

      _closed_or_unknown ->
        :unknown
    end
  end

  # Ends a stream for new appends. One with appends still in flight stays until they are answered, so each
  # answer still reaches the producer as an ack; one with none is dropped now.
  defp stop(state, stream) do
    state = %{state | by_token: Map.delete(state.by_token, stream.token)}

    if stream.inflight == %{},
      do: %{state | streams: Map.delete(state.streams, stream.id)},
      else: put_stream(state, %{stream | open?: false})
  end

  @doc "The topic of open stream `stream_id`, or nil for one this connection does not hold or has stopped."
  @spec topic(t(), pos_integer()) :: String.t() | nil
  def topic(%__MODULE__{streams: streams}, stream_id) do
    case Map.fetch(streams, stream_id) do
      {:ok, %{open?: true} = stream} -> stream.topic
      _stopped_or_unknown -> nil
    end
  end

  @doc "Whether every open stream has room for another append: the connection reads its socket only then."
  @spec room?(t()) :: boolean()
  def room?(%__MODULE__{streams: streams}),
    do: Enum.all?(streams, fn {_id, stream} -> not stream.open? or stream_room?(stream) end)

  @doc "Whether anything is still in flight, so the connection is not idle."
  @spec busy?(t()) :: boolean()
  def busy?(%__MODULE__{streams: streams}),
    do: Enum.any?(streams, fn {_id, stream} -> map_size(stream.inflight) > 0 end)

  defp stream_room?(%{inflight: inflight, inflight_bytes: bytes, window: window}),
    do: map_size(inflight) < window.appends and bytes < window.bytes

  # A gap or a repeat: the producer and the broker no longer agree on what was sent, so the stream ends
  # with an error at the sequence that broke it and takes no later append (the ones in flight are still
  # answered).
  defp out_of_sequence(state, stream, sequence) do
    reason = if sequence < stream.next_seq, do: "sequence_repeat", else: "sequence_gap"
    :ok = BrokerServer.close_stream(stream.broker, stream.token)
    {stop(state, stream), [ack_frame(stream, stream.acked, [%{sequence: sequence, reason: reason}])]}
  end

  defp in_sequence(state, stream, sequence, batch, max_inflated_bytes, charge) do
    stream = %{stream | next_seq: sequence + 1}

    with {:ok, entries, size} <- decode(batch, max_inflated_bytes),
         :ok <- within_window(stream, size),
         {:ok, records} <- records(entries),
         # charged last, so an append refused for any other reason spends nothing
         :ok <- charge.(length(records), size) do
      label = {stream.id, sequence}
      reqids = BrokerServer.send_stream_produce(stream.broker, stream.range_id, records, label, state.reqids)

      stream = %{
        stream
        | inflight: Map.put(stream.inflight, sequence, size),
          inflight_bytes: stream.inflight_bytes + size
      }

      {%{state | reqids: reqids, streams: Map.put(state.streams, stream.id, stream)}, []}
    else
      {:error, reason} -> answered(put_stream(state, stream), stream.id, sequence, {:error, reason}, nil)
    end
  end

  defp decode(batch, max_inflated_bytes) do
    with {:ok, entries} <- Batch.decode(batch, max_inflated_bytes: max_inflated_bytes) do
      {%{inflated_size: size}, <<>>} = Batch.split(batch)
      {:ok, entries, size}
    end
  end

  # An append past the window the broker granted is refused, not buffered: the connection stops reading
  # once a window is full, so only a producer that ignored its acks gets here.
  defp within_window(stream, size) do
    if map_size(stream.inflight) < stream.window.appends and stream.inflight_bytes + size <= stream.window.bytes,
      do: :ok,
      else: {:error, :window_exceeded}
  end

  # The log does not store tombstones yet (#206): an append that carries one is refused whole.
  defp records(entries) do
    if Enum.any?(entries, fn {_record, tombstone} -> tombstone end),
      do: {:error, :tombstone_unsupported},
      else: {:ok, Enum.map(entries, fn {%Record{} = record, false} -> record end)}
  end

  defp put_stream(state, stream), do: %{state | streams: Map.put(state.streams, stream.id, stream)}

  # One append answered, by the broker or here before it was sent (`scale` nil: the window is unchanged).
  # The ack names the sequence below which every append has been answered, so a sequence that failed is
  # named in the errors of the ack that first passes it. A stream already stopped is dropped once its last
  # append in flight is answered.
  defp answered(state, stream_id, sequence, reply, scale) do
    case Map.fetch(state.streams, stream_id) do
      :error ->
        {state, []}

      {:ok, stream} ->
        {size, inflight} = Map.pop(stream.inflight, sequence, 0)

        stream = %{
          stream
          | inflight: inflight,
            inflight_bytes: stream.inflight_bytes - size,
            answered: Map.put(stream.answered, sequence, outcome(reply))
        }

        {acked, answered, errors} = advance(stream.acked, stream.answered, [])
        window = if scale, do: StreamWindow.current(stream.granted, scale, inflight == %{}), else: stream.window
        stream = %{stream | acked: acked, answered: answered, window: window}
        frame = ack_frame(stream, acked, Enum.reverse(errors))

        if not stream.open? and inflight == %{},
          do: {%{state | streams: Map.delete(state.streams, stream.id)}, [frame]},
          else: {put_stream(state, stream), [frame]}
    end
  end

  defp outcome({:ok, _placements}), do: :ok
  defp outcome({:error, reason}), do: {:error, reason_string(reason)}

  defp reason_string(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp reason_string({reason, _detail}) when is_atom(reason), do: Atom.to_string(reason)
  defp reason_string(reason), do: inspect(reason)

  # `acked` is the next sequence not yet answered: every one below it has been.
  defp advance(acked, answered, errors) do
    case Map.pop(answered, acked) do
      {nil, _answered} -> {acked, answered, errors}
      {:ok, answered} -> advance(acked + 1, answered, errors)
      {{:error, reason}, answered} -> advance(acked + 1, answered, [%{sequence: acked, reason: reason} | errors])
    end
  end

  defp ack_frame(stream, acked, errors) do
    push = %{
      stream_id: stream.id,
      # every sequence below this one has been answered; 0 before the first answer
      acked_sequence: acked,
      window_appends: stream.window.appends,
      window_bytes: stream.window.bytes,
      errors: errors
    }

    Wire.encode_ok(stream.corr, IO.iodata_to_binary(Wire.encode_push(:append_ack, push)))
  end

  defp moved_frame(stream, reason, targets),
    do: StreamMoves.moved_frame(stream.id, stream.corr, stream.topic, reason, targets)
end
