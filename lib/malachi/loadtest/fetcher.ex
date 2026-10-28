defmodule Malachi.Loadtest.Fetcher do
  @moduledoc """
  A fetching client: one `fetch_range` (`Malachi.Wire` key 31) at a time, a closed loop, over every active
  range of a topic in turn, each request to the broker the routes say serves the range. Where a range's
  next fetch starts moves past each page it reads. What a page that brings nothing new does is the
  `:at_tail` option: `:restart` (the default, the load generator's) sends that range back to the start of
  its history, so a reader keeps pulling a backlog; `:wait` asks again from where the page says the next one
  starts, so a reader that wants every record once (the chaos checker) asks from the end it reached. An
  empty page can still move on: from a position in an ancestor's history that held nothing of the range's
  slice, it names the range's own records next. Where a range with no ancestor read starts is the
  `:start_from` option: `:earliest` (the default), or a group's checkpoint, `{:committed, group}`. Which
  ranges it reads is the `:ranges` option: every active one (`nil`, the default), or only those a group
  member was handed (their sequence numbers). As the Node fetcher (`scripts/lib/fetcher.js`) does all three.

  The connections are passive (a request, then its answer); connections, routes and their backoffs go
  through `Malachi.Loadtest.Links`, and lanes and what a split hands its children through
  `Malachi.Loadtest.ReadLanes`. A fetch is answered as one of:

    * `{:ok, count, latency_us}` from `fetch/3`: a page, with `count` records new to this reader (a page's
      leading records it already had, `skip`, are not counted), and the round trip of its `fetch_range`
      alone: reading the routes or connecting before it is preparation, timed by nobody;
    * `{:page, page}` from `fetch_page/3`: the same page with its records' values (`fetched/0`);
    * `{:error, reason}`: a refusal that is not a redirect, routes that could not be read, or a broker that
      took no connection (`"connect_failed"`: it is connected to again on a later fetch);
    * `{:held, ms}`: the topic's routes are held back, for `ms` more milliseconds, after a read that
      failed or that named no address for a primary; nothing was asked of anyone, and the caller is
      expected to wait it out rather than ask again;
    * `:redirected`: the routes it went by were behind the cluster's (`moved`, `stale_routes`, `not_owner`,
      `migrating`, or a primary they name with no address), read again for the next fetch, which is not an
      op (after a backoff, for a primary with no address);
    * `:dropped`: the connection failed; the range's broker is connected to again on a later fetch;
    * `:unassigned`: `:ranges` holds none of the topic's active ranges; nothing was asked of anyone.
  """

  import Malachi.Loadtest.Links, only: [bump: 2, ensure_routes: 3, forget: 2, is_redirect: 1, put_conn: 3]

  alias Malachi.Loadtest.Conn
  alias Malachi.Loadtest.Links
  alias Malachi.Loadtest.Options
  alias Malachi.Loadtest.ReadLanes
  alias Malachi.Loadtest.Router
  alias Malachi.Wire
  alias Malachi.Wire.Batch

  @typedoc """
  A page as `fetch_page/3` gives it: the range it came from (`range`, its number, and `block`, its keyspace
  block in a keyspace of `size` keys), the topic's active ranges by the routes it was fetched by (`active`),
  where it was asked from (`from`: `:earliest`, `{:committed, group}` or `{:position, p}`) and where the range's next page starts
  (`to`), and the values of the records new to this reader, in the order the range holds them.
  """
  @type fetched :: %{
          range: non_neg_integer(),
          active: [non_neg_integer()],
          block: {non_neg_integer(), pos_integer()},
          size: pos_integer(),
          from: term(),
          to: Wire.position(),
          values: [binary()]
        }

  @type result ::
          {:ok, non_neg_integer(), non_neg_integer()}
          | {:page, fetched()}
          | {:error, String.t()}
          | {:held, pos_integer()}
          | :redirected
          | :dropped
          | :unassigned

  @max_bytes 1_048_576
  # What a page's batch may inflate to: the broker sends at least one whole batch per page, so a page can
  # pass `@max_bytes`; the cap is the broker's own for a batch (`MALACHI_MAX_INFLATED_BATCH_BYTES`).
  @max_inflated_bytes 16_777_216

  @enforce_keys [:router, :conn_opts]
  defstruct [
    :router,
    :conn_opts,
    control: nil,
    max: 100,
    at_tail: :restart,
    start_from: :earliest,
    ranges: nil,
    unroutable: %{},
    conns: %{},
    sockets: %{},
    lanes: %{},
    retired: %{},
    turn: 0,
    next_corr: 1,
    stats: %{moved: 0, redirects: 0, refreshes: 0, dropped: 0, connect_failures: 0}
  ]

  @type t :: %__MODULE__{}

  @doc """
  A fetcher routing by `router` and connecting with `conn_opts`. Options: `:max`, records per page at most
  (100); `:at_tail`, `:restart` (the default) or `:wait`; `:start_from`, `:earliest` (the default) or
  `{:committed, group}`; and `:ranges`, `nil` (the default) or the sequence numbers of the ranges to read
  (see the moduledoc).
  """
  @spec new(Router.t(), keyword(), keyword()) :: t()
  def new(%Router{} = router, conn_opts, opts \\ []) do
    at_tail =
      case Keyword.get(opts, :at_tail, :restart) do
        value when value in [:restart, :wait] -> value
        value -> raise ArgumentError, "at_tail must be :restart or :wait, got: #{inspect(value)}"
      end

    start_from =
      case Keyword.get(opts, :start_from, :earliest) do
        :earliest -> :earliest
        {:committed, group} = value when is_binary(group) -> value
        value -> raise ArgumentError, "start_from must be :earliest or {:committed, group}, got: #{inspect(value)}"
      end

    ranges =
      case Keyword.get(opts, :ranges) do
        nil -> nil
        list when is_list(list) -> MapSet.new(list)
        value -> raise ArgumentError, "ranges must be nil or a list of range numbers, got: #{inspect(value)}"
      end

    %__MODULE__{
      router: router,
      conn_opts: conn_opts,
      max: Options.positive!(opts, :max, 100),
      at_tail: at_tail,
      start_from: start_from,
      ranges: ranges
    }
  end

  @doc """
  Connects to the broker of every active range of `topic` before the first fetch, so a reader that connects
  up front (inside a connect ramp, say) pays none of it later. `{:error, reason, fetcher}` with the refusal.
  """
  @spec prepare(t(), String.t()) :: {:ok, t()} | {:error, String.t(), t()}
  def prepare(%__MODULE__{} = f, topic) do
    with {:ok, f} <- Links.connect_ranges(f, topic, active: false), do: {:ok, reconcile(f, topic)}
  end

  @doc "One fetch of the next range of `topic`, in turn: the fetcher and how it was answered, a page as a count."
  @spec fetch(t(), String.t(), integer()) :: {t(), result()}
  def fetch(%__MODULE__{} = f, topic, now) do
    case next(f, topic, now) do
      # counted, not decoded: a load generator measures the broker, not its own parsing of what came back
      {f, {:fetched, key, lane, _shape, page, latency_us}} ->
        {%{count: count}, _rest} = Batch.split(page.batch)
        new = max(0, count - page.skip)
        {advance(f, key, lane, page, new > 0), {:ok, new, latency_us}}

      answered ->
        answered
    end
  end

  @doc "One fetch of the next range of `topic`, in turn, as `fetch/3`, a page with its values (`fetched/0`)."
  @spec fetch_page(t(), String.t(), integer()) :: {t(), result()}
  def fetch_page(%__MODULE__{} = f, topic, now) do
    case next(f, topic, now) do
      {f, {:fetched, {_topic, range} = key, lane, shape, page, _latency_us}} ->
        {:ok, entries} = Batch.decode(page.batch, max_inflated_bytes: @max_inflated_bytes, layout: :positioned)
        values = entries |> Enum.drop(page.skip) |> Enum.map(fn {_position, record, _tombstone} -> record.value end)

        fetched = %{
          range: range,
          active: shape.active,
          block: lane.block,
          size: shape.size,
          from: lane.start,
          to: page.next,
          values: values
        }

        {advance(f, key, lane, page, values != []), {:page, fetched}}

      answered ->
        answered
    end
  end

  defp next(f, topic, now) do
    case {Router.routes(f.router, topic), Map.get(f.unroutable, topic)} do
      {nil, {until, _reason}} when until > now -> {f, {:held, until - now}}
      _known_or_due -> fetch_known(f, topic, now)
    end
  end

  defp fetch_known(f, topic, now) do
    case ensure_routes(f, topic, now) do
      {:error, reason, f} ->
        {f, {:error, reason}}

      {:ok, f} ->
        f = reconcile(f, topic)

        case readable(f, topic) do
          [] ->
            {f, :unassigned}

          ranges ->
            %{range: range} = Enum.at(ranges, rem(f.turn, length(ranges)))
            fetch_range(%{f | turn: f.turn + 1}, {topic, range}, now)
        end
    end
  end

  # The active ranges of `topic` this fetcher reads: every one, or those `:ranges` holds.
  defp readable(%__MODULE__{ranges: nil} = f, topic), do: Router.routes(f.router, topic).ranges
  defp readable(f, topic), do: Enum.filter(Router.routes(f.router, topic).ranges, &MapSet.member?(f.ranges, &1.range))

  # Over every active range, read or not: a range `:ranges` leaves out is still one the routes hold, not
  # one gone that hands its position on.
  defp reconcile(f, topic) do
    %{ranges: ranges} = Router.routes(f.router, topic)
    # a range no ancestor of which was read starts where the fetcher was told to; one an ancestor was read
    # of starts where that read reached
    new_lane = fn block, start -> %{block: block, start: if(start == :earliest, do: f.start_from, else: start)} end
    {lanes, retired} = ReadLanes.reconcile(f.lanes, f.retired, topic, ranges, fn _lane -> true end, new_lane)
    %{f | lanes: lanes, retired: retired}
  end

  defp fetch_range(f, {topic, range} = key, now) do
    with {:ok, address} <- Router.address_of(f.router, topic, range),
         {:ok, f, conn} <- Links.connection(f, address, active: false) do
      lane = Map.fetch!(f.lanes, key)

      req = %{
        topic: topic,
        range: range,
        routes_version: Router.version(f.router, topic),
        start: lane.start,
        max: f.max,
        max_bytes: @max_bytes,
        wait_ms: 0,
        accept: [:none, :zstd]
      }

      corr = f.next_corr
      f = %{f | next_corr: corr + 1}

      t0 = System.monotonic_time(:microsecond)

      case Conn.request(conn, Wire.fetch_range_key(), corr, Wire.encode_fetch_range_req(req)) do
        {:ok, 0, payload, conn} ->
          latency_us = System.monotonic_time(:microsecond) - t0
          %{size: size, ranges: ranges} = Router.routes(f.router, topic)
          shape = %{size: size, active: Enum.map(ranges, & &1.range)}
          {put_conn(f, address, conn), {:fetched, key, lane, shape, Wire.decode_page(payload), latency_us}}

        {:ok, _code, refusal, conn} ->
          refused(put_conn(f, address, conn), topic, Wire.decode_error_reason(refusal))

        {:error, _transport} ->
          {f |> Links.drop_conn(address) |> bump(:dropped) |> forget(topic), :dropped}
      end
    else
      # the routes were just read and the range taken from them, so its address is known or unadvertised;
      # routes that name no address for it are read again, but only once a backoff ends
      {:error, %__MODULE__{} = f} ->
        {forget(f, topic), {:error, "connect_failed"}}

      {:error, :unadvertised} ->
        {f |> bump(:redirects) |> hold_routes(topic, now), :redirected}
    end
  end

  # Where a range's next fetch starts after a page: past it; or, when it brought nothing new, back at the
  # start of its history (`at_tail: :restart`) or where the page says the next one starts (`:wait`), which
  # is past an ancestor's history the range's slice held nothing of.
  defp advance(f, key, lane, page, brought_any?) do
    start = if brought_any? or f.at_tail == :wait, do: {:position, page.next}, else: :earliest

    %{f | lanes: Map.put(f.lanes, key, %{lane | start: start})}
  end

  # Routes that name no address for a range's primary are behind the cluster's: read again, but not before a
  # backoff, so a fetcher that has only that range to fetch waits (`{:held, ms}`) rather than spin on reads.
  defp hold_routes(f, topic, now) do
    f = forget(f, topic)
    %{f | unroutable: Map.put(f.unroutable, topic, {Links.backoff_until(now), "unadvertised"})}
  end

  defp refused(f, topic, reason) when is_redirect(reason), do: {f |> bump(:redirects) |> forget(topic), :redirected}
  defp refused(f, _topic, reason), do: {f, {:error, reason}}

  @doc "What routing did so far."
  @spec stats(t()) :: map()
  def stats(%__MODULE__{stats: stats}), do: stats

  @doc "Closes every connection."
  @spec close(t()) :: :ok
  def close(%__MODULE__{} = f), do: Links.close_all(f)
end
