defmodule Malachi.ConnectionStreams do
  @moduledoc """
  Every stream one connection holds: producer streams (`Malachi.ProducerStreams`) and consume streams
  (`Malachi.ConsumeStreams`), under one id space, since `close_stream` names either kind by its id. A
  client holds one connection per broker and opens on it the streams of every range that broker serves,
  of both kinds, as NorthGuard's clients do (meetup transcript, 542 to 569).

  The connection reads its socket only while every producer stream has room in its window
  (`Malachi.ProducerStreams.room?/1`); a consume stream only pushes, so it never holds the socket back. It
  is never idle while a consume stream is open, an append is in flight or a `fetch_range` waits for its
  answer.
  """

  alias Malachi.ConsumeStreams
  alias Malachi.ProducerStreams

  defstruct producers: nil, consumers: nil, next_id: 1, fetches: nil

  @type t :: %__MODULE__{
          producers: ProducerStreams.t(),
          consumers: ConsumeStreams.t(),
          next_id: pos_integer(),
          fetches: :gen_server.request_id_collection()
        }

  @doc "No stream open yet."
  @spec new() :: t()
  def new,
    do: %__MODULE__{
      producers: ProducerStreams.new(),
      consumers: ConsumeStreams.new(),
      fetches: :gen_server.reqids_new()
    }

  @doc """
  Sends `request` to `broker` without waiting (a `fetch_range`, whose wait would otherwise hold the
  connection's streams back), and answers it when the broker replies: `finish` turns the reply into the
  frame the client gets (`handle_message/2`). A broker that goes down meanwhile is answered as
  `{:error, :broker_down}`.
  """
  @spec fetch(t(), GenServer.server(), term(), (term() -> binary())) :: t()
  def fetch(%__MODULE__{} = state, broker, request, finish),
    do: %{state | fetches: :gen_server.send_request(broker, request, finish, state.fetches)}

  @doc "Records a producer stream the broker opened and returns the id it is given."
  @spec open_producer(t(), map()) :: {t(), pos_integer()}
  def open_producer(%__MODULE__{} = state, opened) do
    id = state.next_id
    {%{state | producers: ProducerStreams.open(state.producers, Map.put(opened, :id, id)), next_id: id + 1}, id}
  end

  @doc "Records a consume stream the broker opened and returns the id it is given."
  @spec open_consumer(t(), map()) :: {t(), pos_integer()}
  def open_consumer(%__MODULE__{} = state, opened) do
    id = state.next_id
    {%{state | consumers: ConsumeStreams.open(state.consumers, Map.put(opened, :id, id)), next_id: id + 1}, id}
  end

  @doc "Closes the stream `stream_id` names, of either kind. `:unknown` for one this connection does not hold."
  @spec close(t(), pos_integer()) :: {:ok, t()} | :unknown
  def close(%__MODULE__{} = state, stream_id) do
    if ConsumeStreams.holds?(state.consumers, stream_id) do
      {:ok, consumers} = ConsumeStreams.close(state.consumers, stream_id)
      {:ok, %{state | consumers: consumers}}
    else
      with {:ok, producers} <- ProducerStreams.close(state.producers, stream_id),
           do: {:ok, %{state | producers: producers}}
    end
  end

  @doc """
  Folds a message the connection received into whatever it is about: the answer to a pending fetch (or the
  `:DOWN` of the broker it was asked of, answered as `{:error, :broker_down}`), or a message for one kind
  of stream. `:no` for one that belongs to none. A broker that went down is told to both kinds of stream,
  since each watches the brokers its own streams were opened on.
  """
  @spec handle_message(t(), term()) :: {t(), [binary()]} | :no
  def handle_message(%__MODULE__{} = state, message) do
    case :gen_server.check_response(message, state.fetches, true) do
      {{:reply, reply}, finish, fetches} ->
        {%{state | fetches: fetches}, [finish.(reply)]}

      {{:error, {_reason, _server}}, finish, fetches} ->
        {%{state | fetches: fetches}, [finish.({:error, :broker_down})]}

      _not_a_fetch ->
        streams_message(state, message)
    end
  end

  defp streams_message(state, {:DOWN, _ref, :process, _pid, _reason} = down) do
    {producers, from_producers} = taken(ProducerStreams.handle_message(state.producers, down), state.producers)
    {consumers, from_consumers} = taken(ConsumeStreams.handle_message(state.consumers, down), state.consumers)

    if from_producers == :no and from_consumers == :no,
      do: :no,
      else: {%{state | producers: producers, consumers: consumers}, frames(from_producers) ++ frames(from_consumers)}
  end

  defp streams_message(state, message) do
    case ConsumeStreams.handle_message(state.consumers, message) do
      {consumers, frames} ->
        {%{state | consumers: consumers}, frames}

      :no ->
        case ProducerStreams.handle_message(state.producers, message) do
          {producers, frames} -> {%{state | producers: producers}, frames}
          :no -> :no
        end
    end
  end

  defp taken({streams, frames}, _unchanged), do: {streams, frames}
  defp taken(:no, unchanged), do: {unchanged, :no}

  defp frames(:no), do: []
  defp frames(frames), do: frames

  @doc "Whether every producer stream has room for another append: the connection reads its socket only then."
  @spec room?(t()) :: boolean()
  def room?(%__MODULE__{producers: producers}), do: ProducerStreams.room?(producers)

  @doc "Whether the connection has work outstanding: an append in flight, a consume stream open, or a fetch waiting."
  @spec busy?(t()) :: boolean()
  def busy?(%__MODULE__{} = state),
    do:
      ProducerStreams.busy?(state.producers) or ConsumeStreams.open?(state.consumers) or
        :gen_server.reqids_size(state.fetches) > 0
end
