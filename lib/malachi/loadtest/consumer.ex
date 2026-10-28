defmodule Malachi.Loadtest.Consumer do
  @moduledoc """
  A consuming client as NorthGuard's (meetup transcript, 562 to 569): one consume stream per range
  (`Malachi.Wire` keys 29 and 30), opened where the range is read, pushing `records` pages with the position
  of every record, and a read ack after each page that names the position read and returns the credit, so
  the broker keeps at most `window` records pushed and unread. It reads every active range of a topic, from
  the start of each range's history (a load generator worker reads all of its topic, as a consumer in a
  group of its own does).

  Event driven, as `Malachi.Loadtest.Producer` is: the connections are activated, their socket messages go
  to `handle_message/3`, and what comes back are events:

    * `{:records, range_key, count}`: a page of `count` records new to this consumer (a page's leading
      records it already had, `skip`, are not counted);
    * `{:failed, range_key, reason}`: an open, or a stream, the broker refused for a reason that is not a
      redirect; the range opens again after a backoff, or, with `on_refusal: :stop`, is not read again, nor
      is a range a split makes of it, or a merge back into exactly its block; a merge that takes it in with other keys is a new range, and is opened.

  When a range moves on, its stream is answered `moved` and closed, and the consumer opens the ranges the
  routes now name: one whose block a range it read held (a split's child, or a merge back into a block read
  before its split) from the position read there, which names the same record in it, anything else (a
  merge of two ranges it read, a range it never read) from the start of its history, so a record is read at
  least once and never lost (`Malachi.Loadtest.ReadLanes`, `Malachi.Wire`). Opens refused `moved`,
  `stale_routes`, `not_owner` or `migrating` are redirects, not failures: the routes are read again,
  backing off from the fifth in a row (`Malachi.Loadtest.Links.redirected/4`). Routes that name a range's
  primary with no address are a redirect too, read again only once a backoff ends, the first time
  included (`Malachi.Loadtest.Links.unadvertised/4`). Connections, routes and their backoffs go through
  `Malachi.Loadtest.Links`.
  """

  import Malachi.Loadtest.Links,
    only: [
      address_of_message: 2,
      backoff_until: 1,
      bump: 2,
      connection: 2,
      ensure_routes: 3,
      forget: 2,
      is_redirect: 1,
      on?: 2,
      put_conn: 3
    ]

  alias Malachi.Loadtest.Conn
  alias Malachi.Loadtest.Links
  alias Malachi.Loadtest.Options
  alias Malachi.Loadtest.ReadLanes
  alias Malachi.Loadtest.Router
  alias Malachi.Wire
  alias Malachi.Wire.Batch

  @type range_key :: {String.t(), non_neg_integer()}
  @type event :: {:records, range_key(), non_neg_integer()} | {:failed, range_key(), String.t()}

  @max_bytes 1_048_576
  @first_corr 100

  @enforce_keys [:router, :conn_opts]
  defstruct [
    :router,
    :conn_opts,
    control: nil,
    window: 100,
    max: 100,
    label: "malachi-loadtest",
    on_refusal: :retry,
    # the topics read, each once
    topics: [],
    unroutable: %{},
    conns: %{},
    sockets: %{},
    lanes: %{},
    # range key => the lane of a range the routes retired, kept for the position the ranges made of it open
    # from
    retired: %{},
    streams: %{},
    opening: %{},
    next_corr: @first_corr,
    stats: %{moved: 0, redirects: 0, refreshes: 0, dropped: 0, connect_failures: 0}
  ]

  @type t :: %__MODULE__{}

  @doc """
  A consumer routing by `router` and connecting with `conn_opts`. Options: `:window`, the records a stream
  keeps pushed and unread at most (100); `:max`, the records per page at most (100); and `:on_refusal`,
  what a `{:failed, ...}` range does next: `:retry` (the default) opens it again after a backoff, `:stop`
  leaves it unread from then on, and the ranges a split makes of it too, so each block is reported failed
  once (`Malachi.Loadtest.ReadLanes`: a merge that takes it in with other keys is a new range, and is
  opened).
  """
  @spec new(Router.t(), keyword(), keyword()) :: t()
  def new(%Router{} = router, conn_opts, opts \\ []) do
    on_refusal =
      case Keyword.get(opts, :on_refusal, :retry) do
        value when value in [:retry, :stop] -> value
        value -> raise ArgumentError, "on_refusal must be :retry or :stop, got: #{inspect(value)}"
      end

    %__MODULE__{
      router: router,
      conn_opts: conn_opts,
      window: Options.positive!(opts, :window, 100),
      max: Options.positive!(opts, :max, 100),
      on_refusal: on_refusal
    }
  end

  @doc """
  Connects, authenticated, to the broker of every active range of `topic`, opening no stream: what a
  reader does before it starts (inside a connect ramp, say), so `subscribe/3` pays no connect later and
  reads nothing early. `{:error, reason, consumer}` with the refusal, `"connect_failed"` or `"unadvertised"`.
  """
  @spec prepare(t(), String.t()) :: {:ok, t()} | {:error, String.t(), t()}
  def prepare(%__MODULE__{} = c, topic), do: Links.connect_ranges(c, topic)

  @doc """
  Starts reading `topic`, waiting for nothing: an open goes out on every active range the routes name, from
  the start of its history, and what comes back comes as messages, to `handle_message/3`. The events of
  the pass (an open that failed at once).
  """
  @spec start(t(), String.t(), integer()) :: {t(), [event()]}
  def start(%__MODULE__{} = c, topic, now), do: flush(%{c | topics: Enum.uniq([topic | c.topics])}, now)

  @doc """
  Starts reading `topic` and waits until a stream is open on every active range, within `timeout_ms`, with
  the events of the pages that already came while the streams opened (a range opened first can push before
  the last one answers). `{:error, reason, consumer}` with the refusal that kept a range closed, or
  `"open_timeout"`.
  """
  @spec subscribe(t(), String.t(), non_neg_integer()) :: {:ok, t(), [event()]} | {:error, String.t(), t()}
  def subscribe(%__MODULE__{} = c, topic, timeout_ms) do
    deadline = mono_ms() + timeout_ms
    {c, events} = start(c, topic, mono_ms())
    subscribe_until(c, topic, deadline, refusal_in(events, nil), events)
  end

  defp refusal_in(events, earlier), do: List.first(for({:failed, _key, reason} <- events, do: reason), earlier)

  defp subscribe_until(c, topic, deadline, last_refusal, seen) do
    now = mono_ms()

    case ensure_routes(c, topic, now) do
      {:error, reason, c} ->
        {:error, reason, c}

      {:ok, c} ->
        {c, events} = flush(c, now)
        seen = seen ++ events
        refusal = refusal_in(events, last_refusal)

        cond do
          all_open?(c, topic) ->
            {:ok, c, seen}

          # a range that will not be read again (`on_refusal: :stop`) will not open either
          Enum.any?(c.lanes, &match?({{^topic, _range}, %{stream: :stopped}}, &1)) ->
            {:error, refusal, c}

          now >= deadline ->
            {:error, refusal || "open_timeout", c}

          true ->
            {c, more, _} = await(c, max(0, deadline - mono_ms()), &(&1.opening == %{}))
            refusal = refusal_in(more, refusal)
            backoffs = for {{^topic, _}, %{retry_at: at}} <- c.lanes, is_integer(at), do: at
            if backoffs != [], do: Process.sleep(max(0, min(Enum.max(backoffs), deadline) - mono_ms()))
            subscribe_until(c, topic, deadline, refusal, seen ++ more)
        end
    end
  end

  defp all_open?(c, topic) do
    case Router.routes(c.router, topic) do
      nil -> false
      %{ranges: ranges} -> Enum.all?(ranges, &match?(%{stream: {:open, _}}, Map.get(c.lanes, {topic, &1.range})))
    end
  end

  @doc "Takes a message from one of the consumer's connections: `{:ok, consumer, events}`, or `:no`."
  @spec handle_message(t(), term(), integer()) :: {:ok, t(), [event()]} | :no
  def handle_message(%__MODULE__{} = c, message, now) do
    with {:ok, address} <- address_of_message(c, message),
         %{conn: conn} <- Map.get(c.conns, address) do
      case Conn.message(conn, message) do
        {:frames, bodies, conn} ->
          c = put_conn(c, address, conn)
          {c, events} = Enum.reduce(bodies, {c, []}, &frame(&2, address, &1, now))

          case Conn.activate(conn) do
            :ok -> ok(c, now, events)
            {:error, _closed} -> ok(dropped(c, address, now), now, events)
          end

        {:closed, _reason} ->
          ok(dropped(c, address, now), now, [])

        :no ->
          :no
      end
    else
      _not_ours -> :no
    end
  end

  defp ok(c, now, events) do
    {c, more} = flush(c, now)
    {:ok, c, events ++ more}
  end

  @doc "Opens again what a backoff held until `now`."
  @spec tick(t(), integer()) :: {t(), [event()]}
  def tick(%__MODULE__{} = c, now), do: flush(c, now)

  @doc """
  Milliseconds from `now` until `tick/2` has something to try again, or `:infinity`: a range to open again
  once its backoff ends, or the routes of a topic read once theirs does.
  """
  @spec next_timeout(t(), integer()) :: timeout()
  def next_timeout(%__MODULE__{} = c, now) do
    lanes = for {_key, %{stream: nil, retry_at: at}} <- c.lanes, is_integer(at), do: at
    routes = for topic <- c.topics, {until, _reason} <- [Map.get(c.unroutable, topic)], do: until

    (lanes ++ routes)
    |> Enum.min(fn -> :infinity end)
    |> case do
      :infinity -> :infinity
      deadline -> max(0, deadline - now)
    end
  end

  @doc """
  Waits until `until` holds of the consumer, up to `timeout_ms`, taking its socket messages as they come:
  the events on the way, and `:idle` once `until` holds or `:timeout`.
  """
  @spec await(t(), non_neg_integer(), (t() -> boolean())) :: {t(), [event()], :idle | :timeout}
  def await(%__MODULE__{} = c, timeout_ms, until), do: await_until(c, mono_ms() + timeout_ms, until, [])

  defp await_until(c, deadline, until, events) do
    now = mono_ms()

    cond do
      until.(c) ->
        {c, events, :idle}

      now >= deadline ->
        {c, events, :timeout}

      true ->
        sockets = c.sockets

        receive do
          {tag, socket, _data} = message
          when tag in [:tcp, :ssl, :tcp_error, :ssl_error] and is_map_key(sockets, socket) ->
            {:ok, c, more} = handle_message(c, message, mono_ms())
            await_until(c, deadline, until, events ++ more)

          {tag, socket} = message when tag in [:tcp_closed, :ssl_closed] and is_map_key(sockets, socket) ->
            {:ok, c, more} = handle_message(c, message, mono_ms())
            await_until(c, deadline, until, events ++ more)
        after
          min(deadline - now, next_timeout(c, now)) ->
            {c, more} = tick(c, mono_ms())
            await_until(c, deadline, until, events ++ more)
        end
    end
  end

  @doc "What routing did so far."
  @spec stats(t()) :: map()
  def stats(%__MODULE__{stats: stats}), do: stats

  @doc "Closes every connection; the brokers drop its streams with them."
  @spec close(t()) :: :ok
  def close(%__MODULE__{} = c), do: Links.close_all(c)

  # --- lanes: one per range read ---

  # Every active range of every topic read has a lane; a lane whose range the routes no longer hold, and
  # whose stream is gone, hands its position to the ranges made of it and goes.
  defp flush(c, now) do
    Enum.reduce(c.topics, {c, []}, fn topic, {c, events} ->
      case ensure_routes(c, topic, now) do
        {:ok, c} ->
          {c, more} = flush_topic(c, topic, now)
          {c, events ++ more}

        {:error, _reason, c} ->
          {hold(c, topic), events}
      end
    end)
  end

  # Routes that cannot be read hold every range of the topic waiting to open until their backoff ends:
  # `tick/2` reads them then, and not before, so a range whose own backoff is over does not spin on them.
  defp hold(c, topic) do
    {until, _reason} = Map.fetch!(c.unroutable, topic)

    lanes =
      Map.new(c.lanes, fn
        {{^topic, _range} = key, %{stream: nil} = lane} -> {key, %{lane | retry_at: until}}
        other -> other
      end)

    %{c | lanes: lanes}
  end

  defp flush_topic(c, topic, now) do
    %{ranges: ranges} = Router.routes(c.router, topic)
    # a lane whose stream is still open keeps reading its range until it is told it moved
    {lanes, retired} =
      ReadLanes.reconcile(c.lanes, c.retired, topic, ranges, &(&1.stream in [nil, :stopped]), &new_lane/2)

    c = %{c | lanes: lanes, retired: retired}

    Enum.reduce(ranges, {c, []}, fn %{range: range}, {c, events} ->
      key = {topic, range}
      {c, more} = open_lane(c, key, Map.fetch!(c.lanes, key), now)
      {c, events ++ more}
    end)
  end

  # A range a split made of a stopped one is stopped too: its start is the parent's, `:stopped`.
  defp new_lane(block, :stopped), do: %{block: block, start: :stopped, stream: :stopped, retry_at: nil, redirects: 0}
  defp new_lane(block, start), do: %{block: block, start: start, stream: nil, retry_at: nil, redirects: 0}

  defp open_lane(c, key, %{stream: nil} = lane, now) do
    if is_integer(lane.retry_at) and lane.retry_at > now, do: {c, []}, else: open(c, key, %{lane | retry_at: nil}, now)
  end

  defp open_lane(c, _key, _opening_or_open, _now), do: {c, []}

  defp open(c, {topic, range} = key, lane, now) do
    case Router.address_of(c.router, topic, range) do
      {:ok, address} ->
        open_at(c, key, lane, address, now)

      # routes that name no address for the primary are behind the cluster's, as a redirect says
      {:error, :unadvertised} ->
        {Links.unadvertised(c, key, lane, now), []}

      _unknown ->
        {put_lane(c, key, %{lane | retry_at: backoff_until(now)}) |> forget(topic), []}
    end
  end

  defp open_at(c, {topic, range} = key, lane, address, now) do
    case connection(c, address) do
      {:ok, c, conn} ->
        corr = c.next_corr

        req = %{
          topic: topic,
          range: range,
          routes_version: Router.version(c.router, topic),
          start: lane.start,
          window: c.window,
          max: c.max,
          max_bytes: @max_bytes,
          accept: [:none, :zstd]
        }

        case Conn.send_frame(conn, Wire.open_consume_key(), corr, Wire.encode_open_consume_req(req)) do
          :ok ->
            lane = %{lane | stream: {:opening, {address, corr}}}
            {%{c | next_corr: corr + 1, opening: Map.put(c.opening, {address, corr}, key)} |> put_lane(key, lane), []}

          {:error, _closed} ->
            c = c |> put_lane(key, %{lane | retry_at: backoff_until(now)}) |> forget(topic)
            {dropped(c, address, now), []}
        end

      {:error, c} ->
        {put_lane(c, key, %{lane | retry_at: backoff_until(now)}) |> forget(topic), []}
    end
  end

  # --- frames from a broker ---

  defp frame({c, events}, address, body, now) do
    {corr, code, payload} = Wire.decode_response(body)

    case Map.pop(c.opening, {address, corr}) do
      {nil, _opening} when code == 0 -> push(c, events, address, Wire.decode_push(payload))
      {nil, _opening} -> refused(c, events, address, corr, Wire.decode_error_reason(payload), now)
      {key, opening} -> opened(%{c | opening: opening}, events, key, address, corr, code, payload, now)
    end
  end

  defp opened(c, events, key, address, corr, 0, payload, _now) do
    %{stream_id: id} = Wire.decode_open_consume_resp(payload)
    lane = %{Map.fetch!(c.lanes, key) | stream: {:open, {address, id}}, redirects: 0}
    {%{c | streams: Map.put(c.streams, {address, id}, %{lane: key, corr: corr})} |> put_lane(key, lane), events}
  end

  defp opened(c, events, key, _address, _corr, _code, payload, now) do
    reason = Wire.decode_error_reason(payload)
    lane = %{Map.fetch!(c.lanes, key) | stream: nil}

    if is_redirect(reason) do
      {Links.redirected(c, key, lane, now), events}
    else
      failed(c, events, key, lane, reason, now)
    end
  end

  # A range that failed for a reason that is not a redirect: reported, and opened again after a backoff, or
  # with `on_refusal: :stop` never again.
  defp failed(c, events, key, lane, reason, now) do
    lane =
      if c.on_refusal == :stop,
        do: %{lane | stream: :stopped, start: :stopped},
        else: %{lane | stream: nil, retry_at: backoff_until(now)}

    {put_lane(c, key, lane), events ++ [{:failed, key, reason}]}
  end

  # A page: counted, its end acked at once (the credit it took comes back, the window as it is), and the
  # range's position moved past it.
  defp push(c, events, address, {:records, %{stream_id: id} = page}) do
    case Map.fetch(c.streams, {address, id}) do
      {:ok, %{lane: key, corr: corr}} ->
        {%{count: count}, _rest} = Batch.split(page.batch)
        %{conn: conn} = Map.fetch!(c.conns, address)
        ack = Wire.encode_consume_ack_req(%{stream_id: id, position: page.next, window: 0})
        _ = Conn.send_frame(conn, Wire.consume_ack_key(), corr, ack)
        lane = %{Map.fetch!(c.lanes, key) | start: {:position, page.next}}
        {put_lane(c, key, lane), events ++ [{:records, key, max(0, count - page.skip)}]}

      :error ->
        {c, events}
    end
  end

  # The range moved on: its stream is closed; the routes are read again on the next flush, which opens the
  # ranges they now name.
  defp push(c, events, address, {:moved, %{stream_id: id}}) do
    case Map.pop(c.streams, {address, id}) do
      {%{lane: {topic, _range} = key}, streams} ->
        lane = Map.fetch!(c.lanes, key)
        {%{c | streams: streams} |> bump(:moved) |> forget(topic) |> put_lane(key, %{lane | stream: nil}), events}

      {nil, _streams} ->
        {c, events}
    end
  end

  defp push(c, events, _address, _ack_or_unknown), do: {c, events}

  # An error frame under a stream's correlation id: the broker closed the stream with that refusal (a
  # permission or the flag withdrawn while it read). The range fails (`failed/6`), keeping where it read.
  defp refused(c, events, address, corr, reason, now) do
    case Enum.find(c.streams, fn {{stream_address, _id}, s} -> stream_address == address and s.corr == corr end) do
      {stream_key, %{lane: key}} ->
        failed(%{c | streams: Map.delete(c.streams, stream_key)}, events, key, Map.fetch!(c.lanes, key), reason, now)

      nil ->
        {c, events}
    end
  end

  # A connection dropped: its streams and opens are gone, and the ranges that read through it open again,
  # from where they read, once a backoff ends and the routes are read again.
  defp dropped(c, address, now) do
    streams = Map.reject(c.streams, fn {{stream_address, _id}, _s} -> stream_address == address end)
    opening = Map.reject(c.opening, fn {{open_address, _corr}, _key} -> open_address == address end)

    {lanes, topics} =
      Enum.reduce(c.lanes, {%{}, MapSet.new()}, fn {{topic, _range} = key, lane}, {lanes, topics} ->
        if on?(lane.stream, address),
          do: {Map.put(lanes, key, %{lane | stream: nil, retry_at: backoff_until(now)}), MapSet.put(topics, topic)},
          else: {Map.put(lanes, key, lane), topics}
      end)

    c = %{c | streams: streams, opening: opening, lanes: lanes} |> Links.drop_conn(address) |> bump(:dropped)
    Enum.reduce(topics, c, &forget(&2, &1))
  end

  defp put_lane(c, key, lane), do: %{c | lanes: Map.put(c.lanes, key, lane)}

  defp mono_ms, do: System.monotonic_time(:millisecond)
end
