defmodule Malachi.Loadtest.Fetcher do
  @moduledoc """
  A fetching client of the load generator: one `fetch_range` (`Malachi.Wire` key 31) at a time, a closed
  loop, over every active range of a topic in turn, each request to the broker the routes say serves the
  range. Where a range's next fetch starts moves past each page it reads; a page that brings nothing new
  sends that range back to the start of its history, so a reader keeps pulling a backlog the way a legacy
  fetch does at the tail of its cursor.

  The connections are passive (a request, then its answer); connections, routes and their backoffs go
  through `Malachi.Loadtest.Links`, and lanes and what a split hands its children through
  `Malachi.Loadtest.ReadLanes`. A fetch is answered as one of:

    * `{:ok, count, latency_us}`: a page, with `count` records new to this reader (a page's leading records
      it already had, `skip`, are not counted), and the round trip of its `fetch_range` alone: reading the
      routes or connecting before it is preparation, timed by nobody, as a legacy reconnect is not;
    * `{:error, reason}`: a refusal that is not a redirect, routes that could not be read, or a broker that
      took no connection (`"connect_failed"`: it is connected to again on a later fetch);
    * `{:held, ms}`: the topic's routes are held back, for `ms` more milliseconds, after a read that
      failed or that named no address for a primary; nothing was asked of anyone, and the caller is
      expected to wait it out rather than ask again;
    * `:redirected`: the routes it went by were behind the cluster's (`moved`, `stale_routes`, `not_owner`,
      `migrating`, or a primary they name with no address), read again for the next fetch, which is not an
      op (after a backoff, for a primary with no address);
    * `:dropped`: the connection failed; the range's broker is connected to again on a later fetch.
  """

  import Malachi.Loadtest.Links, only: [bump: 2, ensure_routes: 3, forget: 2, is_redirect: 1, put_conn: 3]

  alias Malachi.Loadtest.Conn
  alias Malachi.Loadtest.Links
  alias Malachi.Loadtest.Options
  alias Malachi.Loadtest.ReadLanes
  alias Malachi.Loadtest.Router
  alias Malachi.Wire
  alias Malachi.Wire.Batch

  @type result ::
          {:ok, non_neg_integer(), non_neg_integer()}
          | {:error, String.t()}
          | {:held, pos_integer()}
          | :redirected
          | :dropped

  @max_bytes 1_048_576

  @enforce_keys [:router, :conn_opts]
  defstruct [
    :router,
    :conn_opts,
    control: nil,
    max: 100,
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

  @doc "A fetcher routing by `router` and connecting with `conn_opts`. Option: `:max`, records per page at most (100)."
  @spec new(Router.t(), keyword(), keyword()) :: t()
  def new(%Router{} = router, conn_opts, opts \\ []) do
    %__MODULE__{router: router, conn_opts: conn_opts, max: Options.positive!(opts, :max, 100)}
  end

  @doc """
  Connects to the broker of every active range of `topic` before the first fetch, so a reader that connects
  up front (inside a connect ramp, say) pays none of it later. `{:error, reason, fetcher}` with the refusal.
  """
  @spec prepare(t(), String.t()) :: {:ok, t()} | {:error, String.t(), t()}
  def prepare(%__MODULE__{} = f, topic) do
    with {:ok, f} <- Links.connect_ranges(f, topic, active: false), do: {:ok, reconcile(f, topic)}
  end

  @doc "One fetch of the next range of `topic`, in turn: the fetcher and how it was answered."
  @spec fetch(t(), String.t(), integer()) :: {t(), result()}
  def fetch(%__MODULE__{} = f, topic, now) do
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
        %{ranges: ranges} = Router.routes(f.router, topic)
        %{range: range} = Enum.at(ranges, rem(f.turn, length(ranges)))
        fetch_range(%{f | turn: f.turn + 1}, {topic, range}, now)
    end
  end

  defp reconcile(f, topic) do
    %{ranges: ranges} = Router.routes(f.router, topic)
    new_lane = fn block, start -> %{block: block, start: start} end
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
          page(put_conn(f, address, conn), key, lane, Wire.decode_page(payload), latency_us)

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

  # What a page brought, and where the range's next fetch starts: past it, or back at the start of its
  # history when it brought nothing new.
  defp page(f, key, lane, page, latency_us) do
    {%{count: count}, _rest} = Batch.split(page.batch)
    new = max(0, count - page.skip)
    start = if new == 0, do: :earliest, else: {:position, page.next}
    {%{f | lanes: Map.put(f.lanes, key, %{lane | start: start})}, {:ok, new, latency_us}}
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
