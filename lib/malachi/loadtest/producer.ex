defmodule Malachi.Loadtest.Producer do
  @moduledoc """
  A producing client as NorthGuard's (meetup transcript, 551 to 560 and 636 to 638): records go to the
  range their key hashes into, each range through one producer stream (`Malachi.Wire` keys 26 to 28)
  opened on the broker that leads the range's active segment, one connection per broker, and every
  append sent within the window the broker grants and answered by an `append_ack` push.

  Event driven, so one process can stream to several brokers at once: the connections are activated
  (`Malachi.Loadtest.Conn.activate/1`), their socket messages go to `handle_message/3`, and what comes back
  are events about the records handed to `produce/4`, each record carried with a tag of the caller's:

    * `{:acked, entries, latency_us}`: one append's records, acknowledged, `latency_us` after it was sent
      (the round trip a legacy produce measures; time spent lingering is not in it);
    * `{:failed, entries, reason}`: one append's records, refused with `reason` as the broker named it,
      or `"connection_lost"` for an append in flight when its connection dropped, which may or may not
      have landed.

  A record is never retried here: the caller sees every outcome and decides (the chaos checker resends,
  the load generator counts). What is retried is routing. A stream told `moved` (its segment sealed by a
  roll or a failover, or its range split or merged) takes no more appends; the appends it had in flight
  are still answered on it, and the records not yet sent go to the stream the routes now name, read again
  over a metadata connection opened only when it is first needed. An open refused `moved`, `stale_routes`,
  `not_owner` or `migrating` reads the routes again and opens where they say, backing off once it has
  been redirected 5 times in a row.

  Lingering, per range: each `produce/4` call's share of a range waits in the range's buffer as one unit,
  until the buffer holds `linger_bytes` (counted by `record_bytes/2`) or its oldest unit has waited
  `linger_ms`; then as many whole units as fit in `linger_bytes` go out as one append, never splitting
  one (a unit larger than `linger_bytes` goes alone). `linger_ms: 0` sends as soon as the window has room.
  `room?/1` is false while some buffer holds `linger_bytes`, which is how a closed loop is held back by
  the window rather than buffering without bound.
  """

  alias Malachi.Loadtest.Conn
  alias Malachi.Loadtest.Router
  alias Malachi.Log.Record
  alias Malachi.Wire
  alias Malachi.Wire.Batch

  @typedoc "A record and the caller's tag for it."
  @type entry :: {term(), Record.t()}

  @type event :: {:acked, [entry()], non_neg_integer()} | {:failed, [entry()], String.t()}

  @typedoc "Counts of what routing did, for a report."
  @type stats :: %{
          moved: non_neg_integer(),
          redirects: non_neg_integer(),
          refreshes: non_neg_integer(),
          dropped: non_neg_integer(),
          connect_failures: non_neg_integer()
        }

  # The refusals of an open that mean the routes it went by are behind the cluster's.
  @redirects ["moved", "stale_routes", "not_owner", "migrating"]
  # Redirects in a row after which a range backs off before opening again, and the most it waits.
  @redirects_before_backoff 5
  @max_backoff_ms 1_000
  # How long a range waits before opening again after a refusal that is not a redirect, or a broker it
  # could not connect to.
  @retry_ms 200
  # The inflated bytes a stream asks the broker for; the broker caps it (`MALACHI_STREAM_MAX_WINDOW_BYTES`).
  @window_bytes 16_777_216
  # The first correlation id a stream's open takes; pushes on the stream come back under it.
  @first_corr 100

  @enforce_keys [:router, :conn_opts]
  defstruct [
    :router,
    :conn_opts,
    control: nil,
    label: "malachi-loadtest",
    max_inflight: 1,
    linger_ms: 0,
    linger_bytes: 1,
    stopped: false,
    # topic => {monotonic ms until which its routes are not read again, the refusal they last got}
    unroutable: %{},
    conns: %{},
    sockets: %{},
    lanes: %{},
    streams: %{},
    opening: %{},
    next_corr: @first_corr,
    stats: %{moved: 0, redirects: 0, refreshes: 0, dropped: 0, connect_failures: 0}
  ]

  @type t :: %__MODULE__{}

  @doc """
  A producer routing by `router` (bootstrapped already, `Malachi.Loadtest.Router.bootstrap/2`) and
  connecting with `conn_opts` (credentials and TLS, as `Malachi.Loadtest.Conn.connect/1` takes them; the
  host and port come from the routes). Options: `:max_inflight`, the appends a stream keeps in flight at
  most, further capped by the window the broker grants (1); `:linger_ms` (0) and `:linger_bytes` (1); and
  `:label`, how the operator interfaces name this client.
  """
  @spec new(Router.t(), keyword(), keyword()) :: t()
  def new(%Router{} = router, conn_opts, opts \\ []) do
    %__MODULE__{
      router: router,
      conn_opts: conn_opts,
      label: Keyword.get(opts, :label, "malachi-loadtest"),
      max_inflight: positive!(opts, :max_inflight, 1),
      linger_ms: non_negative!(opts, :linger_ms, 0),
      linger_bytes: positive!(opts, :linger_bytes, 1)
    }
  end

  @doc """
  Hands `entries` to their ranges' buffers and sends what can go now. Events can come back at once: for
  records that cannot be routed (the topic's routes cannot be read, or no range covers them), for a range
  whose primary has no address, and for appends in flight on a connection a send just found closed.
  """
  @spec produce(t(), String.t(), [entry()], integer()) :: {t(), [event()]}
  def produce(%__MODULE__{} = p, _topic, [], now), do: flush(p, now, [])

  def produce(%__MODULE__{} = p, topic, entries, now) do
    {p, failed} = route(p, topic, entries, now)
    flush(p, now, failed)
  end

  @doc """
  Takes a message from one of the producer's connections: `{:ok, producer, events}`, or `:no` for a
  message that is not one.
  """
  @spec handle_message(t(), term(), integer()) :: {:ok, t(), [event()]} | :no
  def handle_message(%__MODULE__{} = p, message, now) do
    with {:ok, address} <- address_of_message(p, message),
         %{conn: conn} <- Map.get(p.conns, address) do
      case Conn.message(conn, message) do
        {:frames, bodies, conn} ->
          p = put_conn(p, address, conn)
          {p, events} = Enum.reduce(bodies, {p, []}, &frame(&2, address, &1, now))

          case Conn.activate(conn) do
            :ok -> ok(flush(p, now, events))
            {:error, reason} -> ok(flush_after(dropped(p, address, reason, now), now, events))
          end

        {:closed, reason} ->
          ok(flush_after(dropped(p, address, reason, now), now, []))

        :no ->
          :no
      end
    else
      _not_ours -> :no
    end
  end

  defp ok({p, events}), do: {:ok, p, events}

  defp flush_after({p, events}, now, earlier), do: flush(p, now, earlier ++ events)

  @doc "Sends what lingering or a backoff held until `now`."
  @spec tick(t(), integer()) :: {t(), [event()]}
  def tick(%__MODULE__{} = p, now), do: flush(p, now, [])

  @doc """
  Milliseconds from `now` until `tick/2` has something to send: the oldest lingering record's deadline,
  or the end of a range's backoff. `:infinity` when nothing waits on time.
  """
  @spec next_timeout(t(), integer()) :: timeout()
  def next_timeout(%__MODULE__{} = p, now) do
    p.lanes
    |> Enum.flat_map(fn {_key, lane} -> lane_deadlines(p, lane) end)
    |> Enum.min(fn -> :infinity end)
    |> case do
      :infinity -> :infinity
      deadline -> max(0, deadline - now)
    end
  end

  defp lane_deadlines(_p, %{buffer_bytes: 0}), do: []
  defp lane_deadlines(_p, %{retry_at: retry_at}) when is_integer(retry_at), do: [retry_at]
  # Lingering only holds a lane whose window has room: one the window holds waits for an ack, not a clock.
  defp lane_deadlines(%{linger_ms: linger} = p, %{since: since, stream: {:open, stream_key}} = lane) when linger > 0 do
    stream = Map.fetch!(p.streams, stream_key)
    {_chunk, _rest, chunk_bytes} = take_chunk(lane.buffer, p.linger_bytes)
    if window_room?(p, stream) and not over_window?(stream, chunk_bytes), do: [since + linger], else: []
  end

  defp lane_deadlines(_p, _lane), do: []

  @doc "Whether the caller may hand over more records: false while some range's buffer is full."
  @spec room?(t()) :: boolean()
  def room?(%__MODULE__{lanes: lanes, linger_bytes: linger_bytes}),
    do: Enum.all?(lanes, fn {_key, lane} -> lane.buffer_bytes < linger_bytes end)

  @doc "Whether every record handed over has had its outcome: nothing buffered, opening or in flight."
  @spec idle?(t()) :: boolean()
  def idle?(%__MODULE__{} = p) do
    p.opening == %{} and Enum.all?(p.lanes, fn {_key, lane} -> lane.buffer_bytes == 0 end) and
      Enum.all?(p.streams, fn {_key, stream} -> stream.inflight == %{} end)
  end

  @doc """
  Waits until `until` holds of the producer (`idle?/1` unless given; `room?/1` for a closed loop), up to
  `timeout_ms`, taking this producer's socket messages (and only those) as they come: the events on the
  way, and `:idle` once `until` holds or `:timeout`.
  """
  @spec await(t(), non_neg_integer(), (t() -> boolean())) :: {t(), [event()], :idle | :timeout}
  def await(%__MODULE__{} = p, timeout_ms, until \\ &idle?/1), do: await(p, mono_ms() + timeout_ms, until, [])

  defp await(p, deadline, until, events) do
    now = mono_ms()

    cond do
      until.(p) ->
        {p, events, :idle}

      now >= deadline ->
        {p, events, :timeout}

      true ->
        wait = min(deadline - now, next_timeout(p, now))
        sockets = p.sockets

        receive do
          {tag, socket, _data} = message
          when tag in [:tcp, :ssl, :tcp_error, :ssl_error] and is_map_key(sockets, socket) ->
            await_message(p, message, deadline, until, events)

          {tag, socket} = message when tag in [:tcp_closed, :ssl_closed] and is_map_key(sockets, socket) ->
            await_message(p, message, deadline, until, events)
        after
          wait ->
            {p, more} = tick(p, mono_ms())
            await(p, deadline, until, events ++ more)
        end
    end
  end

  defp await_message(p, message, deadline, until, events) do
    {:ok, p, more} = handle_message(p, message, mono_ms())
    await(p, deadline, until, events ++ more)
  end

  @doc """
  Opens a stream on every active range of `topic` before any record is handed over, within `timeout_ms`:
  a client that connects and opens up front (inside a connect ramp, say) pays none of it later. A range
  whose open is redirected is opened again where the routes then say. `{:error, reason, producer}` with
  the refusal that kept a range closed, or `"open_timeout"`.
  """
  @spec prepare(t(), String.t(), non_neg_integer()) :: {:ok, t()} | {:error, String.t(), t()}
  def prepare(%__MODULE__{} = p, topic, timeout_ms), do: prepare(p, topic, mono_ms() + timeout_ms, nil)

  defp prepare(p, topic, deadline, last_refusal) do
    now = mono_ms()

    with {:ok, p} <- ensure_routes(p, topic, now) do
      ranges = for %{range: range} <- Router.routes(p.router, topic).ranges, do: {topic, range}

      case Enum.reject(ranges, &open?(p, &1)) do
        [] ->
          {:ok, p}

        _closed when now >= deadline ->
          {:error, last_refusal || "open_timeout", p}

        closed ->
          {p, refusals} = Enum.reduce(closed, {p, []}, &open_closed(&1, &2, now))
          {p, events, _idle_or_timeout} = await(p, max(0, deadline - mono_ms()), &(&1.opening == %{}))
          refusal = List.first(for({:failed, _entries, reason} <- refusals ++ events, do: reason), last_refusal)
          backoffs = for {_key, %{retry_at: at}} <- Map.take(p.lanes, closed), is_integer(at), do: at
          wait_out(backoffs, deadline)
          prepare(p, topic, deadline, refusal)
      end
    end
  end

  defp open?(p, key), do: match?(%{stream: {:open, _}}, Map.get(p.lanes, key))

  defp open_closed(key, {p, refusals}, now) do
    lane = Map.get(p.lanes, key, new_lane())

    case lane.stream do
      nil ->
        {p, more} = open(p, key, %{lane | retry_at: nil}, now)
        {p, refusals ++ more}

      {:opening, _} ->
        {p, refusals}
    end
  end

  # Sleeps until the latest backoff among `retry_ats` ends, or the deadline, whichever is first.
  defp wait_out([], _deadline), do: :ok
  defp wait_out(retry_ats, deadline), do: Process.sleep(max(0, min(Enum.max(retry_ats), deadline) - mono_ms()))

  @doc """
  Drops every record not yet sent (buffered, or waiting on an open), with no event for them, and sends
  nothing from now on: for a caller that is done handing records over and wants only the outcomes of the
  appends already in flight (`in_flight?/1`, `await/3`). An append answered `unknown_stream` after this
  never reached the broker and is dropped the same way, rather than sent again.
  """
  @spec stop_sending(t()) :: t()
  def stop_sending(%__MODULE__{} = p) do
    lanes = Map.new(p.lanes, fn {key, lane} -> {key, %{lane | buffer: :queue.new(), buffer_bytes: 0}} end)
    %{p | lanes: lanes, stopped: true}
  end

  @doc "Whether an append is still waiting for its answer."
  @spec in_flight?(t()) :: boolean()
  def in_flight?(%__MODULE__{streams: streams}), do: Enum.any?(streams, fn {_key, stream} -> stream.inflight != %{} end)

  @doc "What routing did so far."
  @spec stats(t()) :: stats()
  def stats(%__MODULE__{stats: stats}), do: stats

  @doc "Closes every connection; the brokers drop its streams with them."
  @spec close(t()) :: :ok
  def close(%__MODULE__{} = p) do
    Enum.each(p.conns, fn {_address, %{conn: conn}} -> Conn.close(conn) end)
    if p.control, do: Conn.close(p.control)
    :ok
  end

  # --- routing records to ranges ---

  defp route(p, topic, entries, now) do
    case ensure_routes(p, topic, now) do
      {:ok, p} -> route_known(p, topic, entries, now, true)
      {:error, reason, p} -> {p, [{:failed, entries, reason}]}
    end
  end

  # Each entry into the buffer of the range its key hashes into. Routes caught mid-split may have no range
  # for a position: they are read again once, and an entry still without a range then fails.
  defp route_known(p, topic, entries, now, retry?, where \\ :back) do
    {routed, unrouted} =
      Enum.reduce(entries, {%{}, []}, fn {_tag, record} = entry, {routed, unrouted} ->
        case Router.range_of(p.router, topic, record.key) do
          {:ok, range} -> {Map.update(routed, range, [entry], &[entry | &1]), unrouted}
          :no_range -> {routed, [entry | unrouted]}
        end
      end)

    p = Enum.reduce(routed, p, fn {range, ranged}, p -> buffer(p, {topic, range}, Enum.reverse(ranged), now, where) end)

    case {Enum.reverse(unrouted), retry?} do
      {[], _} ->
        {p, []}

      {unrouted, true} ->
        case refresh(p, topic) do
          {:ok, p} -> route_known(p, topic, unrouted, now, false, where)
          {:error, reason, p} -> {p, [{:failed, unrouted, reason}]}
        end

      {unrouted, false} ->
        {p, [{:failed, unrouted, "no_range"}]}
    end
  end

  # A range's buffer holds hand-overs whole: each `produce/4` call's share of the range is one unit, so an
  # append never splits one (see `take_chunk/2`).
  defp buffer(p, key, entries, now, where) do
    lane = Map.get(p.lanes, key, new_lane())
    bytes = entries_bytes(entries)
    since = if lane.buffer_bytes == 0, do: now, else: lane.since

    queue =
      if where == :front, do: :queue.in_r({entries, bytes}, lane.buffer), else: :queue.in({entries, bytes}, lane.buffer)

    %{p | lanes: Map.put(p.lanes, key, %{lane | buffer: queue, buffer_bytes: lane.buffer_bytes + bytes, since: since})}
  end

  @doc """
  The bytes a record with a `key_bytes` long key and no headers takes in a batch, as the broker counts an
  append against its window (`Malachi.Wire.Batch`): its value and key, and 22 bytes of framing. What a
  buffer holds is counted the same way, so lingering and the window agree; a caller sizes `:linger_bytes`
  with it (the load generator's default is one batch's worth).
  """
  @spec record_bytes(non_neg_integer(), non_neg_integer()) :: pos_integer()
  def record_bytes(value_bytes, key_bytes), do: value_bytes + key_bytes + 22

  defp entries_bytes(entries), do: Enum.reduce(entries, 0, fn {_tag, record}, acc -> acc + entry_bytes(record) end)

  defp buffered_entries(queue), do: queue |> :queue.to_list() |> Enum.flat_map(&elem(&1, 0))

  # `retry_at` is nil, or the monotonic instant a backoff ends (monotonic time can be negative, so no
  # number stands for "none").
  defp new_lane, do: %{buffer: :queue.new(), buffer_bytes: 0, since: 0, stream: nil, retry_at: nil, redirects: 0}

  # Exactly what the record takes in a plain batch: a tombstone flag (1), the key as a present-string (1, or
  # 5 and its bytes), the value's length (4) and bytes, the timestamp (8), the header count (4) and each
  # header's key and value as present-strings.
  defp entry_bytes(%Record{value: value, key: key, headers: headers}) do
    headers = headers || []
    header_bytes = Enum.reduce(headers, 0, fn {k, v}, acc -> acc + str_bytes(k) + str_bytes(v) end)
    1 + str_bytes(key) + 4 + byte_size(value) + 8 + 4 + header_bytes
  end

  defp str_bytes(nil), do: 1
  defp str_bytes(string) when is_binary(string), do: 5 + byte_size(string)

  # --- sending ---

  defp flush(%{stopped: true} = p, _now, events), do: {p, events}

  # A range the routes no longer hold goes first: its records are rerouted in front of what the ranges
  # made of it hold before any of them sends (`flush_open_lane/4`).
  defp flush(p, now, events) do
    keys = Enum.sort_by(Map.keys(p.lanes), &if(retired?(p, &1), do: 0, else: 1))

    Enum.reduce(keys, {p, events}, fn key, {p, events} ->
      {p, more} = flush_lane(p, key, now)
      {p, events ++ more}
    end)
  end

  # A lane retired while the flush ran (a reroute deletes its own, and the flush it runs can delete
  # another) is skipped: its records went to the lanes the routes name now.
  defp flush_lane(p, key, now) do
    case Map.fetch(p.lanes, key) do
      {:ok, lane} -> flush_open_lane(p, key, lane, now)
      :error -> {p, []}
    end
  end

  defp flush_open_lane(p, key, lane, now) do
    cond do
      lane.buffer_bytes == 0 ->
        {p, []}

      # a range a split or merge retired opens nothing, so its backoff is moot: its records go where the
      # routes send them now, ahead of what was routed there since
      lane.stream == nil and retired?(p, key) ->
        reroute(p, key, lane, now)

      # nothing of the topic goes out while it changes shape (`held?/2`); it resumes on a message (an
      # answer, a move), not a clock, so a backoff that ends meanwhile is moot
      held?(p, elem(key, 0)) ->
        {put_lane(p, key, %{lane | retry_at: nil}), []}

      is_integer(lane.retry_at) and lane.retry_at > now ->
        {p, []}

      lane.stream == nil ->
        open(p, key, %{lane | retry_at: nil}, now)

      match?({:opening, _}, lane.stream) ->
        {p, []}

      true ->
        send_appends(p, key, lane, now)
    end
  end

  defp open(p, {topic, range} = key, lane, now) do
    case Router.address_of(p.router, topic, range) do
      {:ok, address} ->
        open_at(p, key, lane, address, now)

      :unknown_topic ->
        case ensure_routes(p, topic, now) do
          {:ok, p} -> open(p, key, lane, now)
          {:error, reason, p} -> fail_buffer(p, key, lane, reason, now)
        end

      # The range was retired (split or merged) since its records were buffered: they go where the routes
      # read now send them.
      {:error, :unknown_range} ->
        reroute(p, key, lane, now)

      {:error, :unadvertised} ->
        fail_buffer(p, key, lane, "unadvertised", now)
    end
  end

  defp reroute(p, {topic, _range} = key, lane, now) do
    entries = buffered_entries(lane.buffer)
    p = %{p | lanes: Map.delete(p.lanes, key)}

    # the last routes read no longer hold the range; the ones it is routed by now are read again if they
    # were forgotten since; a record they have no range for is read again once, as any record's is (they
    # may have been read mid-split); the records are older than anything the ranges they go to hold: in
    # front of it
    case ensure_routes(p, topic, now) do
      {:ok, p} ->
        {p, failed} = route_known(p, topic, entries, now, true, :front)
        {p, more} = flush(p, now, [])
        {p, failed ++ more}

      {:error, reason, p} ->
        {p, [{:failed, entries, reason}]}
    end
  end

  defp open_at(p, {topic, range} = key, lane, address, now) do
    case connection(p, address) do
      {:ok, p, conn} ->
        corr = p.next_corr

        req = %{
          topic: topic,
          range: range,
          routes_version: Router.version(p.router, topic),
          codec: :none,
          window_appends: p.max_inflight,
          window_bytes: @window_bytes,
          producer_id: nil,
          label: p.label
        }

        case Conn.send_frame(conn, Wire.open_stream_key(), corr, Wire.encode_open_stream_req(req)) do
          :ok ->
            lane = %{lane | stream: {:opening, {address, corr}}}

            {%{
               p
               | next_corr: corr + 1,
                 opening: Map.put(p.opening, {address, corr}, key),
                 lanes: Map.put(p.lanes, key, lane)
             }, []}

          # the connection is gone: the range waits out a backoff and reads its routes again, as after a
          # broker it could not connect to (`dropped/4` sees no stream of this lane's on the connection)
          {:error, reason} ->
            p |> put_lane(key, %{lane | retry_at: backoff_until(now)}) |> forget(topic) |> dropped(address, reason, now)
        end

      {:error, p} ->
        # A broker that does not take connections may be gone: the routes are read again before the next open.
        {put_lane(p, key, %{lane | retry_at: backoff_until(now)}) |> forget(topic), []}
    end
  end

  defp send_appends(p, key, lane, now) do
    {:open, stream_key} = lane.stream
    stream = Map.fetch!(p.streams, stream_key)

    if lane.buffer_bytes > 0 and window_room?(p, stream) and due?(p, lane, now) do
      {_chunk, _rest, chunk_bytes} = taken = take_chunk(lane.buffer, p.linger_bytes)
      # the estimate first, so a lane the byte window holds does not encode a batch only to drop it
      if over_window?(stream, chunk_bytes),
        do: {p, []},
        else: encode_append(p, key, lane, stream_key, stream, taken, now)
    else
      {p, []}
    end
  end

  defp encode_append(p, key, lane, stream_key, stream, {chunk, rest, chunk_bytes}, now) do
    batch = Batch.encode(Enum.map(chunk, fn {_tag, record} -> {record, false} end), :none)
    {%{inflated_size: size}, <<>>} = Batch.split(batch)

    if over_window?(stream, size) do
      {p, []}
    else
      lane = %{lane | buffer: rest, buffer_bytes: lane.buffer_bytes - chunk_bytes}
      send_append(p, key, lane, stream_key, stream, %{entries: chunk, batch: batch, size: size}, now)
    end
  end

  # Room for one more append in flight: under the window the broker granted, and under the pipeline.
  defp window_room?(p, stream), do: map_size(stream.inflight) < min(stream.window_appends, p.max_inflight)

  # An append of `size` bytes would go past the byte window. One with nothing in flight goes anyway: the
  # broker refuses it if it must, rather than the range waiting forever.
  defp over_window?(stream, size), do: stream.inflight != %{} and stream.inflight_bytes + size > stream.window_bytes

  defp send_append(p, key, lane, {address, id} = stream_key, stream, append, now) do
    %{conn: conn} = Map.fetch!(p.conns, address)
    seq = stream.next_seq

    case Conn.send_frame(conn, Wire.append_key(), stream.corr, Wire.encode_append_req(id, seq, append.batch)) do
      :ok ->
        stream = %{
          stream
          | next_seq: seq + 1,
            inflight: Map.put(stream.inflight, seq, %{entries: append.entries, size: append.size, sent_at: mono_us()}),
            inflight_bytes: stream.inflight_bytes + append.size
        }

        lane = if lane.buffer_bytes == 0, do: lane, else: %{lane | since: now}
        p = %{p | streams: Map.put(p.streams, stream_key, stream)} |> put_lane(key, lane)
        send_appends(p, key, lane, now)

      {:error, reason} ->
        # the chunk never left: back to the front of the buffer, for the stream opened after the drop
        p |> put_lane(key, lane) |> unsend(key, append.entries) |> dropped(address, reason, now)
    end
  end

  defp due?(%{linger_ms: 0}, _lane, _now), do: true

  defp due?(p, lane, now), do: lane.buffer_bytes >= p.linger_bytes or now - lane.since >= p.linger_ms

  # The oldest hand-overs, as many whole ones as fit in `limit` bytes (at least one, however large): the
  # entries, the rest of the queue and their bytes.
  defp take_chunk(queue, limit), do: take_chunk(queue, limit, [], 0)

  defp take_chunk(queue, limit, taken, bytes) do
    case :queue.peek(queue) do
      {:value, {entries, unit_bytes}} when taken == [] or bytes + unit_bytes <= limit ->
        take_chunk(:queue.drop(queue), limit, [entries | taken], bytes + unit_bytes)

      _empty_or_too_big ->
        {taken |> Enum.reverse() |> Enum.concat(), queue, bytes}
    end
  end

  defp fail_buffer(p, key, lane, reason, now) do
    entries = buffered_entries(lane.buffer)
    lane = %{lane | buffer: :queue.new(), buffer_bytes: 0, stream: nil, retry_at: backoff_until(now)}
    {put_lane(p, key, lane), [{:failed, entries, reason}]}
  end

  # --- frames from a broker ---

  defp frame({p, events}, address, body, now) do
    {corr, code, payload} = Wire.decode_response(body)

    case Map.pop(p.opening, {address, corr}) do
      {nil, _opening} when code == 0 -> push(p, events, address, Wire.decode_push(payload))
      {nil, _opening} -> refused(p, events, address, corr, Wire.decode_error_reason(payload))
      {key, opening} -> opened(%{p | opening: opening}, events, key, address, corr, code, payload, now)
    end
  end

  defp opened(p, events, key, address, corr, 0, payload, _now) do
    resp = Wire.decode_open_stream_resp(payload)

    stream = %{
      lane: key,
      corr: corr,
      next_seq: 0,
      inflight: %{},
      inflight_bytes: 0,
      window_appends: resp.window_appends,
      window_bytes: resp.window_bytes,
      open?: true,
      # appends answered `unknown_stream`, not yet told apart from the ones still to be acknowledged
      rejected: 0
    }

    lane = %{Map.fetch!(p.lanes, key) | stream: {:open, {address, resp.stream_id}}, redirects: 0}
    {%{p | streams: Map.put(p.streams, {address, resp.stream_id}, stream)} |> put_lane(key, lane), events}
  end

  defp opened(p, events, {topic, _range} = key, _address, _corr, _code, payload, now) do
    reason = Wire.decode_error_reason(payload)
    lane = %{Map.fetch!(p.lanes, key) | stream: nil}

    if reason in @redirects do
      redirects = lane.redirects + 1
      retry_at = if redirects >= @redirects_before_backoff, do: now + backoff(redirects), else: nil
      p = p |> bump(:redirects) |> forget(topic) |> put_lane(key, %{lane | redirects: redirects, retry_at: retry_at})
      {p, events}
    else
      {p, failed} = fail_buffer(p, key, lane, reason, now)
      {p, events ++ failed}
    end
  end

  defp backoff(redirects), do: min(@max_backoff_ms, 50 * (redirects - @redirects_before_backoff + 1))

  defp push(p, events, address, {:append_ack, ack}) do
    case Map.fetch(p.streams, {address, ack.stream_id}) do
      {:ok, stream} -> acked(p, events, {address, ack.stream_id}, stream, ack)
      :error -> {p, events}
    end
  end

  # The stream is told its range moved on: no more appends on it, and the records still buffered go where
  # the routes read next say. Its appends in flight are still answered on it.
  defp push(p, events, address, {:moved, %{stream_id: id}}) do
    case Map.fetch(p.streams, {address, id}) do
      {:ok, %{lane: {topic, _range} = key} = stream} ->
        p = p |> bump(:moved) |> forget(topic) |> retire(key, {address, id}, stream)
        {p, events}

      :error ->
        {p, events}
    end
  end

  defp push(p, events, _address, _records_or_unknown), do: {p, events}

  # An error frame under a stream's correlation id answers one append the connection did not take.
  #
  # `unknown_stream`: the broker stopped the stream for good (a move, or a sequence it refused), and every
  # append that reaches it after that is answered so. Those appends never reached the broker, and they are
  # always the last ones sent; but their answers can come after the acknowledgements of the ones before
  # them, or not, so which appends they are is known only once every append on the stream is answered:
  # then the ones still in flight are exactly the refused ones (`settle/3`). Until then the range opens no
  # new stream (`draining?/2`), so they go out again before anything handed over after them, as the
  # broker's contract asks: the appends in flight are answered first, so a producer learns which landed
  # before it resends anything. That holds across a split or merge too: nothing of the topic goes out while
  # the retired stream drains (`held?/2`), and what it held is rerouted in front of what the ranges made of
  # it got since.
  #
  # Any other refusal (`unsupported`, `permission_denied`) uses up no sequence and does not stop the stream,
  # so it names no append the client can tell apart, and the appends after it break the sequence. The
  # stream is retired and every append it has in flight fails with the reason: some of them may have
  # landed, as an append in flight on a dropped connection may.
  defp refused(p, events, address, corr, reason) do
    case Enum.find(p.streams, fn {{stream_address, _id}, stream} ->
           stream_address == address and stream.corr == corr
         end) do
      {stream_key, %{lane: {topic, _range} = key, inflight: inflight} = stream} when inflight != %{} ->
        p = forget(p, topic)

        if reason == "unknown_stream" do
          stream = %{stream | rejected: stream.rejected + 1, open?: false}
          {p |> retire(key, stream_key, stream) |> settle(stream_key), events}
        else
          failed = for {_seq, sent} <- Enum.sort_by(inflight, &elem(&1, 0)), do: {:failed, sent.entries, reason}
          {retire(p, key, stream_key, %{stream | inflight: %{}, inflight_bytes: 0, open?: false}), events ++ failed}
        end

      _no_stream_or_nothing_in_flight ->
        {p, events}
    end
  end

  # Once a stopped stream's refusals account for every append it still has in flight, those appends are
  # the refused ones: their records go back to the front of the range's buffer, in the order they were
  # sent, and the stream is dropped, which lets the range open its next one.
  defp settle(p, stream_key) do
    case Map.fetch(p.streams, stream_key) do
      {:ok, %{open?: false, rejected: rejected, inflight: inflight, lane: key}}
      when rejected > 0 and map_size(inflight) == rejected ->
        p = %{p | streams: Map.delete(p.streams, stream_key)}
        if p.stopped, do: p, else: unsend_all(p, key, inflight)

      _open_or_still_answering ->
        p
    end
  end

  defp unsend_all(p, key, inflight) do
    inflight
    |> Enum.sort_by(&elem(&1, 0), :desc)
    |> Enum.reduce(p, fn {_seq, sent}, p -> unsend(p, key, sent.entries) end)
  end

  # Whether `topic` is changing shape, so none of its ranges opens a stream or sends an append:
  #
  #   * a stream retired on it still has appends in flight (`draining?/2`): its refused appends go out
  #     again before anything handed over after them, on its own range or on the ranges a split or merge
  #     made of it;
  #   * the routes no longer hold a range that still has a stream, open or opening: a merge or split whose
  #     move has not reached that stream yet (the broker tells each parent apart, a reconcile apart when
  #     they are on different nodes). Nothing more goes out on it, and the ranges that take its keys wait
  #     for its move, so what it held is rerouted in front of what they got since (`reroute/4`).
  #
  # Either ends on a message (an answer, a move), which flushes again. The hold covers the whole topic,
  # ranges the split or merge did not touch included, for as long as the late parent's broker takes to
  # tell it it moved: its next reconcile (about a second), or, should that broker not see the change, the
  # idle close of the connection (`MALACHI_TCP_RECV_TIMEOUT`). Order is worth that pause; moves are rare.
  defp held?(p, topic) do
    draining?(p, topic) or
      Enum.any?(p.lanes, fn {{lane_topic, _range} = key, lane} ->
        lane_topic == topic and lane.stream != nil and retired?(p, key)
      end)
  end

  defp draining?(p, topic) do
    Enum.any?(p.streams, fn {_key, stream} ->
      not stream.open? and elem(stream.lane, 0) == topic and stream.inflight != %{}
    end)
  end

  # Whether the last routes read no longer hold `key`'s range (a split or merge retired it). Judged by the
  # last ones read even after they are forgotten (`Malachi.Loadtest.Router.last_read/2`): within one life
  # of the topic a retired range never comes back, so an older view cannot call a live range retired, and
  # forgetting the routes (on any move, refusal or drop) does not let `held?/2` go before the move it waits
  # for. A topic created again numbers its ranges anew, but a record is routed only after a read, which
  # replaces the last one first.
  defp retired?(p, {topic, range}) do
    case Router.last_read(p.router, topic) do
      nil -> false
      %{ranges: ranges} -> not Enum.any?(ranges, &(&1.range == range))
    end
  end

  # Puts `entries`, which never left, back at the front of `key`'s buffer.
  defp unsend(p, key, entries) do
    lane = Map.get(p.lanes, key, new_lane())
    bytes = entries_bytes(entries)

    put_lane(p, key, %{
      lane
      | buffer: :queue.in_r({entries, bytes}, lane.buffer),
        buffer_bytes: lane.buffer_bytes + bytes
    })
  end

  defp retire(p, key, stream_key, stream) do
    p =
      case Map.fetch(p.lanes, key) do
        {:ok, %{stream: {:open, ^stream_key}} = lane} -> put_lane(p, key, %{lane | stream: nil})
        _other -> p
      end

    if stream.inflight == %{},
      do: %{p | streams: Map.delete(p.streams, stream_key)},
      else: %{p | streams: Map.put(p.streams, stream_key, %{stream | open?: false})}
  end

  # Every append below `acked_sequence` has been answered: the ones named in `errors` failed with their
  # reason, the rest landed.
  defp acked(p, events, stream_key, stream, ack) do
    errors = Map.new(ack.errors, &{&1.sequence, &1.reason})
    {answered, inflight} = Map.split_with(stream.inflight, fn {seq, _sent} -> seq < ack.acked_sequence end)
    now_us = mono_us()

    new_events =
      answered
      |> Enum.sort_by(fn {seq, _sent} -> seq end)
      |> Enum.map(fn {seq, sent} ->
        case Map.fetch(errors, seq) do
          {:ok, reason} -> {:failed, sent.entries, reason}
          :error -> {:acked, sent.entries, now_us - sent.sent_at}
        end
      end)

    stream = %{
      stream
      | inflight: inflight,
        inflight_bytes: stream.inflight_bytes - Enum.reduce(answered, 0, fn {_seq, sent}, acc -> acc + sent.size end),
        window_appends: ack.window_appends,
        window_bytes: ack.window_bytes
    }

    streams =
      if not stream.open? and inflight == %{},
        do: Map.delete(p.streams, stream_key),
        else: Map.put(p.streams, stream_key, stream)

    {settle(%{p | streams: streams}, stream_key), events ++ new_events}
  end

  # A connection to a broker dropped: its appends in flight may or may not have landed, its streams and
  # opens are gone, and the ranges that streamed through it read their routes again before opening.
  defp dropped(p, address, _reason, now) do
    {lost, streams} = Map.split_with(p.streams, fn {{stream_address, _id}, _stream} -> stream_address == address end)
    {_opens, opening} = Map.split_with(p.opening, fn {{open_address, _corr}, _key} -> open_address == address end)

    events =
      for {_key, stream} <- lost,
          {_seq, sent} <- Enum.sort_by(stream.inflight, &elem(&1, 0)),
          do: {:failed, sent.entries, "connection_lost"}

    lanes =
      Map.new(p.lanes, fn {key, lane} ->
        if on?(lane.stream, address), do: {key, %{lane | stream: nil, retry_at: backoff_until(now)}}, else: {key, lane}
      end)

    {conn, conns} = Map.pop(p.conns, address)
    if conn, do: Conn.close(conn.conn)
    sockets = if conn, do: Map.delete(p.sockets, conn.conn.socket), else: p.sockets
    topics = for {{topic, _range}, lane} <- p.lanes, on?(lane.stream, address), uniq: true, do: topic
    p = %{p | streams: streams, opening: opening, lanes: lanes, conns: conns, sockets: sockets} |> bump(:dropped)
    {Enum.reduce(topics, p, &forget(&2, &1)), events}
  end

  defp on?({_open_or_opening, {address, _id}}, address), do: true
  defp on?(_stream, _address), do: false

  # --- connections ---

  defp connection(p, address) do
    case Map.fetch(p.conns, address) do
      {:ok, %{conn: conn}} ->
        {:ok, p, conn}

      :error ->
        with {:ok, conn} <- connect(p, address), :ok <- Conn.activate(conn) do
          {:ok,
           %{p | conns: Map.put(p.conns, address, %{conn: conn}), sockets: Map.put(p.sockets, conn.socket, address)},
           conn}
        else
          _refused -> {:error, bump(p, :connect_failures)}
        end
    end
  end

  defp connect(p, {host, port}) do
    opts = Keyword.merge(p.conn_opts, host: host, port: port)

    with {:ok, conn} <- Conn.connect(opts) do
      case Conn.authenticate(conn, opts) do
        {:ok, conn} ->
          {:ok, conn}

        {:error, _} = refused ->
          Conn.close(conn)
          refused
      end
    end
  end

  defp put_conn(p, address, conn), do: %{p | conns: Map.put(p.conns, address, %{conn: conn})}

  defp address_of_message(p, message) when is_tuple(message) and tuple_size(message) in [2, 3] do
    case Map.fetch(p.sockets, elem(message, 1)) do
      {:ok, address} -> {:ok, address}
      :error -> :no
    end
  end

  defp address_of_message(_p, _message), do: :no

  # --- routes ---

  # When a fixed backoff ends, counted from when the call gave up, not from when it was asked: a topic's
  # routes that could not be read (`ensure_routes/3`); a range whose connection failed, on connect or on a
  # send (`open_at/5`), or dropped (`dropped/4`); and a range whose records failed (`fail_buffer/5`), for an
  # open the broker refused for a reason that is not a redirect, routes that could not be read, or a
  # primary with no address. A call that blocked (a node that does not answer takes the connect timeout per
  # address) would otherwise leave one already over, and the next attempt would not wait at all. `now` is
  # the caller's instant, which a test may set ahead of the clock.
  #
  # The one backoff that does not use it is the redirect branch of `opened/8`: it grows with the redirects
  # in a row, and is counted from `now`, which follows an answer read from the socket with no blocking call
  # before it, so `now` is when that answer came.
  defp backoff_until(now), do: max(now, mono_ms()) + @retry_ms

  # The routes of `topic`, read again if they were forgotten. A topic whose routes could not be read is not
  # asked again until its backoff ends, whichever path asks (a hand-over, an open, a reroute, a prepare):
  # until then the answer is the refusal the routes last got, with no round trip. One read is not held
  # back: `route_known/6`'s second look at routes that left a record without a range (read mid-split),
  # since those routes stay known and that look happens once per hand-over or reroute the gap catches,
  # failing or not.
  defp ensure_routes(p, topic, now) do
    cond do
      Router.routes(p.router, topic) ->
        {:ok, p}

      match?({until, _reason} when until > now, Map.get(p.unroutable, topic)) ->
        {:error, elem(Map.fetch!(p.unroutable, topic), 1), p}

      true ->
        case refresh(p, topic) do
          {:ok, p} ->
            {:ok, %{p | unroutable: Map.delete(p.unroutable, topic)}}

          {:error, reason, p} ->
            {:error, reason, %{p | unroutable: Map.put(p.unroutable, topic, {backoff_until(now), reason})}}
        end
    end
  end

  # Reads the cluster state and `topic`'s routes over the metadata connection, opened on first use to the
  # bootstrap address or, should that not answer, to each broker the cluster state lists in turn.
  defp refresh(p, topic), do: p |> bump(:refreshes) |> read_routes(topic, p.control != nil)

  # A metadata connection kept from an earlier read may have been closed by the server since (an idle one
  # is, after `MALACHI_TCP_RECV_TIMEOUT`): one that fails is replaced and the read made again, once.
  defp read_routes(p, topic, reused?) do
    case control(p) do
      {:ok, p, conn} ->
        case Router.refresh(p.router, conn, topic) do
          {:ok, router, conn} ->
            {:ok, %{p | router: router, control: conn}}

          {:error, reason, conn} when is_binary(reason) ->
            {:error, reason, %{p | control: conn}}

          {:error, _transport} ->
            Conn.close(conn)
            p = %{p | control: nil}
            if reused?, do: read_routes(p, topic, false), else: {:error, "metadata_unavailable", p}
        end

      {:error, p} ->
        {:error, "metadata_unavailable", p}
    end
  end

  defp control(%{control: nil} = p) do
    p.router
    |> control_addresses()
    |> Enum.find_value({:error, p}, fn address ->
      case connect(p, address) do
        {:ok, conn} -> {:ok, %{p | control: conn}, conn}
        {:error, _} -> nil
      end
    end)
  end

  defp control(%{control: conn} = p), do: {:ok, p, conn}

  defp control_addresses(%Router{bootstrap: bootstrap, brokers: brokers}) do
    advertised = for {_id, %{host: host, port: port}} <- brokers, is_binary(host) and port > 0, do: {host, port}
    Enum.uniq([bootstrap | advertised])
  end

  defp forget(p, topic), do: %{p | router: Router.forget(p.router, topic)}

  defp put_lane(p, key, lane), do: %{p | lanes: Map.put(p.lanes, key, lane)}

  defp bump(p, stat), do: %{p | stats: Map.update!(p.stats, stat, &(&1 + 1))}

  defp positive!(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> value
      value -> raise ArgumentError, "#{key} must be a positive integer, got: #{inspect(value)}"
    end
  end

  defp non_negative!(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value >= 0 -> value
      value -> raise ArgumentError, "#{key} must be a non-negative integer, got: #{inspect(value)}"
    end
  end

  defp mono_ms, do: System.monotonic_time(:millisecond)
  defp mono_us, do: System.monotonic_time(:microsecond)
end
