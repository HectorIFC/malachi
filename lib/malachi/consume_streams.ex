defmodule Malachi.ConsumeStreams do
  @moduledoc """
  The consume streams one connection holds (`Malachi.Wire` keys 29 and 30), as NorthGuard's broker pushes
  records to a consumer on a stream, each with its offset, and the consumer's read ack says what it has read
  and can move its window (meetup transcript, 562 to 569).

  A stream is bound to one range and opened where the range is read (`Malachi.BrokerServer.open_consume/4`:
  where its active segment is led, or anywhere while it has none), from a position in the range's history.
  The broker never reads for it: it hands the connection a view of the range (`Malachi.Broker.ReadView`)
  when the stream opens and each time the range's durable records grow past what the stream read, and the
  connection reads through it and pushes `records` pages, each carrying the position of every record and of
  the page's end. A view reaches only as far as a replication quorum acknowledged
  (`Malachi.Broker.durable_end/2`), so a consumer never reads a record its producer was not told is stored.
  A page holds at most `max` records and about `max_bytes` bytes, and always one record however large, so
  the byte limit is a soft one; both are capped by the server. One page is read per message, so each goes
  out before the next is read.

  The window is credit in records: the stream holds at most `window` records pushed and not yet acked.
  A `consume_ack` names the position the consumer read up to, which frees every page whose last record ends
  at or before it (the position after that record, or the page's end, which can lie further on), and may
  set a new window. A stream caught up on its view asks the broker to wake it once the range's durable
  records end past that view (`Malachi.BrokerServer.arm_consume/3`); a stream without credit waits for an
  ack, and reads its view again then. Before each page the flag and the consume permission are checked
  again: a stream that lost either is answered with the refusal and closed.

  When the range moves on (a split or merge retires it, or its next active segment opens on another node)
  or the broker that served it goes down, the stream is answered with a `moved` push and closed; a roll
  that keeps the range here does not move it. Positions carry over to a split's children, so the consumer
  opens there from the position it acked (`Malachi.Wire`).

  Pure but for the reads, the broker calls and the skip reports: messages in, pushes and the next state out.
  """

  alias Malachi.Broker
  alias Malachi.Broker.ReadView
  alias Malachi.Broker.Skip
  alias Malachi.BrokerServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Metadata
  alias Malachi.Retention.SkipReporter
  alias Malachi.StreamMoves
  alias Malachi.Wire
  alias Malachi.Wire.Batch

  # A read that failed (an unreachable segment primary, a storage error) is tried again after this long,
  # from a fresh view, rather than at once and in a loop.
  @retry_ms 200

  defstruct streams: %{}, by_token: %{}, brokers: %{}

  @typedoc "A position in a range's history, `{source_index, offset}` (see `Malachi.Wire`)."
  @type position :: {non_neg_integer(), non_neg_integer()}

  @typedoc "One open stream."
  @type stream :: %{
          id: pos_integer(),
          corr: non_neg_integer(),
          broker: GenServer.server(),
          broker_pid: pid(),
          topic: String.t(),
          range_id: Metadata.range_id(),
          token: reference(),
          position: position(),
          window: pos_integer(),
          max: pos_integer(),
          max_bytes: pos_integer(),
          in_flight: non_neg_integer(),
          pages: :queue.queue({position(), pos_integer()}),
          view: ReadView.t() | nil,
          reporter: atom() | pid() | nil,
          allowed: (-> :ok | {:error, atom()})
        }

  @type t :: %__MODULE__{
          streams: %{pos_integer() => stream()},
          by_token: %{reference() => pos_integer()},
          brokers: StreamMoves.brokers()
        }

  @typedoc "What a step leaves for the connection to write: frames, each already a whole response frame."
  @type frames :: [binary()]

  @doc "No stream open yet."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  Records a stream the broker opened under `id` (`token`, `broker_pid` and the resolved `position`, from
  `Malachi.BrokerServer.open_consume/4`). Its first view arrives as a message (`handle_message/2`).
  """
  @spec open(t(), map()) :: t()
  def open(%__MODULE__{} = state, %{id: id, token: token, broker_pid: broker_pid} = opened) do
    stream =
      opened
      |> Map.take([
        :id,
        :corr,
        :broker,
        :broker_pid,
        :topic,
        :range_id,
        :token,
        :position,
        :window,
        :max,
        :max_bytes,
        :allowed
      ])
      |> Map.merge(%{in_flight: 0, pages: :queue.new(), view: nil, reporter: nil})

    %{
      state
      | streams: Map.put(state.streams, id, stream),
        by_token: Map.put(state.by_token, token, id),
        brokers: StreamMoves.watch(state.brokers, broker_pid)
    }
  end

  @doc """
  A `consume_ack`: the consumer read everything before `position`, which frees the pages that end at or
  before it, and asks for `window` records of credit (0 leaves the window as it is). The stream then reads
  again with the credit it has. `{:unknown, stream_id}` for a stream this connection does not hold.
  """
  @spec ack(t(), pos_integer(), position(), non_neg_integer()) :: {t(), frames()} | {:unknown, pos_integer()}
  def ack(%__MODULE__{} = state, stream_id, position, window) do
    case Map.fetch(state.streams, stream_id) do
      {:ok, stream} ->
        {pages, freed} = free_pages(stream.pages, position, 0)
        window = if window > 0, do: window, else: stream.window
        read(state, %{stream | pages: pages, in_flight: stream.in_flight - freed, window: window})

      :error ->
        {:unknown, stream_id}
    end
  end

  # Pages leave in the order they were pushed, so the oldest is the first to check. A page is freed by an
  # ack at or past its last record's end (the position right after it), so a consumer that acks what it read
  # frees it as one that acks the page's end does: the end can lie further on, past records of a sibling's
  # slice or at the start of the next source.
  defp free_pages(pages, position, freed) do
    case :queue.peek(pages) do
      {:value, {records_end, count}} when records_end <= position ->
        free_pages(:queue.drop(pages), position, freed + count)

      _later_or_empty ->
        {pages, freed}
    end
  end

  @doc """
  Folds a message the connection received into the streams: a view to read from, a move, a broker that went
  down, or a read to try again. `:no` for a message that belongs to none of them.
  """
  @spec handle_message(t(), term()) :: {t(), frames()} | :no
  def handle_message(%__MODULE__{} = state, {:consume_wake, token, view, reporter}) do
    case Map.fetch(state.by_token, token) do
      {:ok, id} -> read(state, %{Map.fetch!(state.streams, id) | view: view, reporter: reporter})
      :error -> :no
    end
  end

  def handle_message(%__MODULE__{} = state, {:stream_moved, token, reason, targets}) do
    case Map.fetch(state.by_token, token) do
      {:ok, id} ->
        stream = Map.fetch!(state.streams, id)
        {drop(state, stream), [StreamMoves.moved_frame(stream.id, stream.corr, stream.topic, reason, targets)]}

      :error ->
        :no
    end
  end

  def handle_message(%__MODULE__{brokers: brokers} = state, {:DOWN, ref, :process, _pid, _reason})
      when is_map_key(brokers, ref) do
    {broker_pid, brokers} = StreamMoves.down(brokers, ref)
    gone = for {_id, %{broker_pid: ^broker_pid} = stream} <- state.streams, do: stream
    state = Enum.reduce(gone, %{state | brokers: brokers}, &drop(&2, &1))

    {state, Enum.map(gone, &StreamMoves.moved_frame(&1.id, &1.corr, &1.topic, :restarted, [{&1.range_id, nil}]))}
  end

  # The next page of a stream that had credit left after its last one: one page per message, so each page
  # is written before the next is read, and the connection's other messages and frames come in between.
  def handle_message(%__MODULE__{} = state, {:consume_continue, id}) do
    case Map.fetch(state.streams, id) do
      {:ok, stream} -> read(state, stream)
      :error -> {state, []}
    end
  end

  def handle_message(%__MODULE__{} = state, {:consume_retry, id}) do
    case Map.fetch(state.streams, id) do
      {:ok, stream} ->
        BrokerServer.refresh_consume(stream.broker, stream.token)
        {state, []}

      :error ->
        {state, []}
    end
  end

  def handle_message(%__MODULE__{}, _other), do: :no

  @doc "Closes a stream the client is done with. `:unknown` for one this connection does not hold."
  @spec close(t(), pos_integer()) :: {:ok, t()} | :unknown
  def close(%__MODULE__{} = state, stream_id) do
    case Map.fetch(state.streams, stream_id) do
      {:ok, stream} ->
        :ok = BrokerServer.close_stream(stream.broker, stream.token)
        {:ok, drop(state, stream)}

      :error ->
        :unknown
    end
  end

  @doc "Whether this connection holds stream `stream_id`."
  @spec holds?(t(), pos_integer()) :: boolean()
  def holds?(%__MODULE__{streams: streams}, stream_id), do: is_map_key(streams, stream_id)

  @doc "Whether any stream is open: a connection that consumes is never idle while it waits for records."
  @spec open?(t()) :: boolean()
  def open?(%__MODULE__{streams: streams}), do: streams != %{}

  @doc """
  One page of `range_id` from `position`, read through `view` (what a `records` push and a `fetch_range`
  answer carry, see `Malachi.Wire.encode_page/1`, and `records_end`, the position right after its last
  record): at most `max` records and about `max_bytes` bytes, always one record when the range holds one. `{:ok, page, count}` with `count` records, the skips are
  reported to `reporter` (`Malachi.Retention.SkipReporter`), or `{:error, reason}` for a read that failed.
  """
  @spec read_page(ReadView.t(), Metadata.range_id(), position(), pos_integer(), pos_integer(), term()) ::
          {:ok, map(), non_neg_integer()} | {:error, term()}
  def read_page(view, {topic, _seq} = range_id, position, max, max_bytes, reporter) do
    with {:ok, positioned, next, skips} <- read_positioned(view, range_id, position, max),
         {:ok, kept, next, skips} <- within_bytes(view, range_id, position, {positioned, next, skips}, max_bytes) do
      SkipReporter.report(reporter, topic, nil, skips)
      entries = Enum.map(kept, fn {record_position, record} -> {record_position, record, false} end)
      {expired, exact?} = expired(skips)

      page = %{
        next: next,
        skip: 0,
        backlog: Broker.consume_backlog(view, range_id, next),
        expired: expired,
        expired_exact: exact?,
        batch: Batch.encode(entries, :none, :positioned),
        records_end: records_end(kept)
      }

      {:ok, page, length(kept)}
    end
  end

  defp read_positioned(view, range_id, position, max),
    do: Broker.read_consume_positioned(view, range_id, position, max, &ReplicationServer.read/4)

  # The records of a page up to `max_bytes` encoded, and always the first one. A page cut short is read
  # again for just the records it keeps, so it ends right after its last record and reports only the data
  # it moved past on the way to them: the rest is the next page's to report.
  defp within_bytes(view, range_id, position, {positioned, next, skips}, max_bytes) do
    kept =
      positioned
      |> Enum.reduce_while({0, 0}, fn {_position, record}, {count, bytes} ->
        bytes = bytes + Record.encoded_size(record)
        if count == 0 or bytes <= max_bytes, do: {:cont, {count + 1, bytes}}, else: {:halt, {count, bytes}}
      end)
      |> elem(0)

    if kept == length(positioned),
      do: {:ok, positioned, next, skips},
      else: read_positioned(view, range_id, position, kept)
  end

  # The records retention removed before a page, and whether that count is exact.
  defp expired(skips) do
    Enum.reduce(skips, {0, true}, fn skip, {total, exact?} ->
      case {skip.offsets, Skip.span(skip)} do
        {offsets, :exact} when is_integer(offsets) -> {total + offsets, exact?}
        {offsets, _bound} when is_integer(offsets) -> {total + offsets, false}
        {:unknown, _span} -> {total, false}
      end
    end)
  end

  # Reads and pushes one page when the stream has credit and its view holds records past its position, and
  # asks for the next page by a message to itself (`{:consume_continue, id}`). A stream caught up on its view
  # asks to be woken once the range's durable records pass it; one out of credit waits for an ack and keeps
  # its view to read again then. Before each read the stream's access is checked again (the flag and the
  # consume permission, which an operator can revoke while it is open): a stream that lost it is answered
  # with the refusal and closed.
  defp read(state, %{view: nil} = stream), do: {put(state, stream), []}

  defp read(state, stream) do
    budget = min(stream.max, stream.window - stream.in_flight)

    cond do
      budget <= 0 -> {put(state, stream), []}
      (refusal = stream.allowed.()) != :ok -> refuse(state, stream, refusal)
      true -> read_once(state, stream, budget)
    end
  end

  defp read_once(state, stream, budget) do
    case read_page(stream.view, stream.range_id, stream.position, budget, stream.max_bytes, stream.reporter) do
      {:ok, page, 0} ->
        # caught up on this view; a page with no records still goes out when it moved past expired data, so
        # the consumer learns what it lost
        BrokerServer.arm_consume(stream.broker, stream.token, view_end(stream))
        frames = if page.expired > 0, do: [records_frame(stream, page)], else: []
        {put(state, %{stream | position: page.next, view: nil}), frames}

      {:ok, page, count} ->
        send(self(), {:consume_continue, stream.id})

        stream = %{
          stream
          | position: page.next,
            in_flight: stream.in_flight + count,
            pages: :queue.in({page.records_end, count}, stream.pages)
        }

        {put(state, stream), [records_frame(stream, page)]}

      {:error, _reason} ->
        Process.send_after(self(), {:consume_retry, stream.id}, @retry_ms)
        {put(state, %{stream | view: nil}), []}
    end
  end

  defp refuse(state, stream, {:error, reason}) do
    :ok = BrokerServer.close_stream(stream.broker, stream.token)
    {drop(state, stream), [Wire.encode_error(stream.corr, reason)]}
  end

  defp records_frame(stream, page),
    do:
      Wire.encode_ok(stream.corr, IO.iodata_to_binary(Wire.encode_push(:records, Map.put(page, :stream_id, stream.id))))

  # The position right after a page's last record, nil for a page without one.
  defp records_end([]), do: nil

  defp records_end(kept) do
    {{index, offset}, _record} = List.last(kept)
    {index, offset + 1}
  end

  defp view_end(%{view: view, range_id: range_id}) do
    case ReadView.fetch_end(view, range_id) do
      {:ok, range_end} -> range_end
      :error -> 0
    end
  end

  defp put(state, stream), do: %{state | streams: Map.put(state.streams, stream.id, stream)}

  defp drop(state, stream),
    do: %{state | streams: Map.delete(state.streams, stream.id), by_token: Map.delete(state.by_token, stream.token)}
end
