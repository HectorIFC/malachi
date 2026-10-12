defmodule Malachi.Loadtest.Links do
  @moduledoc """
  What every routing client of the load generator keeps between a topic's routes and the brokers that serve
  them: one connection per broker address, a metadata connection opened on first use, the routes read again
  when they were forgotten, with a per-topic backoff once a read fails, and counts of what routing did. The
  producer (`Malachi.Loadtest.Producer`), the consumer (`Malachi.Loadtest.Consumer`) and the fetcher
  (`Malachi.Loadtest.Fetcher`) all hold these fields in their own struct and route through here:

    * `:router` (`Malachi.Loadtest.Router`), `:conn_opts` (credentials and TLS, as
      `Malachi.Loadtest.Conn.connect/1` takes them; host and port come from the routes);
    * `:control`, the metadata connection or nil; `:conns`, address to `%{conn: conn}`; `:sockets`, socket
      to address, for the messages an activated connection sends;
    * `:unroutable`, topic to `{until, refusal}` while its routes back off; `:stats`, with at least
      `:refreshes` and `:connect_failures`.
  """

  alias Malachi.Loadtest.Conn
  alias Malachi.Loadtest.Router

  # How long a backoff lasts: a topic whose routes could not be read, a range whose open failed.
  @retry_ms 200
  # Redirects in a row after which a range backs off before opening again, and the most it waits.
  @redirects_before_backoff 5
  @max_backoff_ms 1_000

  @doc """
  Whether `reason`, a refusal, means the routes the call went by are behind the cluster's: `moved`,
  `stale_routes`, `not_owner` or `migrating`. Every client reads its routes again on one, rather than count
  it as an error.
  """
  defguard is_redirect(reason) when reason in ["moved", "stale_routes", "not_owner", "migrating"]

  @doc """
  When a range redirected `redirects` times in a row opens again: at once (nil) for the first four, then
  after a backoff that grows by 50 ms a redirect up to a second, counted from `now`.
  """
  @spec redirect_retry_at(pos_integer(), integer()) :: integer() | nil
  def redirect_retry_at(redirects, now) when redirects >= @redirects_before_backoff,
    do: now + min(@max_backoff_ms, 50 * (redirects - @redirects_before_backoff + 1))

  def redirect_retry_at(_redirects, _now), do: nil

  @doc "Whether a lane's stream, opening or open, is on the connection to `address`."
  @spec on?(term(), Router.address()) :: boolean()
  def on?({_open_or_opening, {address, _id}}, address), do: true
  def on?(_stream, _address), do: false

  @doc """
  The connection to `address`, opened, authenticated and (unless `active: false`) activated on first use:
  `{:ok, client, conn}`, or `{:error, client}` with `:connect_failures` counted.
  """
  @spec connection(map(), Router.address(), keyword()) :: {:ok, map(), Conn.t()} | {:error, map()}
  def connection(client, address, opts \\ []) do
    case Map.fetch(client.conns, address) do
      {:ok, %{conn: conn}} ->
        {:ok, client, conn}

      :error ->
        with {:ok, conn} <- connect(client, address), :ok <- activate(conn, Keyword.get(opts, :active, true)) do
          conns = Map.put(client.conns, address, %{conn: conn})
          {:ok, %{client | conns: conns, sockets: Map.put(client.sockets, conn.socket, address)}, conn}
        else
          _refused -> {:error, bump(client, :connect_failures)}
        end
    end
  end

  defp activate(conn, true), do: Conn.activate(conn)
  defp activate(_conn, false), do: :ok

  @doc "A new authenticated connection to `address`, closed again if its authentication is refused."
  @spec connect(map(), Router.address()) :: {:ok, Conn.t()} | {:error, term()}
  def connect(client, {host, port}) do
    opts = Keyword.merge(client.conn_opts, host: host, port: port)

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

  @doc """
  Connects, authenticated, to the broker of every active range of `topic`, opening nothing on them: what a
  reader does before it starts (inside a connect ramp, say), so its reading pays no connect later.
  `{:error, reason, client}` with the refusal the routes got, `"connect_failed"` or `"unadvertised"`. Option:
  `active: false` keeps the connections passive (`connection/3`).
  """
  @spec connect_ranges(map(), String.t(), keyword()) :: {:ok, map()} | {:error, String.t(), map()}
  def connect_ranges(client, topic, opts \\ []) do
    with {:ok, client} <- ensure_routes(client, topic, mono_ms()) do
      Enum.reduce_while(Router.routes(client.router, topic).ranges, {:ok, client}, fn %{range: range}, {:ok, client} ->
        with {:ok, address} <- Router.address_of(client.router, topic, range),
             {:ok, client, _conn} <- connection(client, address, opts) do
          {:cont, {:ok, client}}
        else
          {:error, :unadvertised} -> {:halt, {:error, "unadvertised", client}}
          {:error, client} -> {:halt, {:error, "connect_failed", client}}
        end
      end)
    end
  end

  @doc "Keeps `conn` (with its buffer as it is now) as the connection to `address`."
  @spec put_conn(map(), Router.address(), Conn.t()) :: map()
  def put_conn(client, address, conn), do: %{client | conns: Map.put(client.conns, address, %{conn: conn})}

  @doc "Closes and forgets the connection to `address`, if there is one."
  @spec drop_conn(map(), Router.address()) :: map()
  def drop_conn(client, address) do
    case Map.pop(client.conns, address) do
      {nil, _conns} ->
        client

      {%{conn: conn}, conns} ->
        Conn.close(conn)
        %{client | conns: conns, sockets: Map.delete(client.sockets, conn.socket)}
    end
  end

  @doc "Closes every connection, the metadata one included."
  @spec close_all(map()) :: :ok
  def close_all(client) do
    Enum.each(client.conns, fn {_address, %{conn: conn}} -> Conn.close(conn) end)
    if client.control, do: Conn.close(client.control)
    :ok
  end

  @doc "The address a socket message is about: `{:ok, address}`, or `:no` for a message about no socket of ours."
  @spec address_of_message(map(), term()) :: {:ok, Router.address()} | :no
  def address_of_message(client, message) when is_tuple(message) and tuple_size(message) in [2, 3] do
    case Map.fetch(client.sockets, elem(message, 1)) do
      {:ok, address} -> {:ok, address}
      :error -> :no
    end
  end

  def address_of_message(_client, _message), do: :no

  @doc """
  When a fixed backoff ends, counted from when the call gave up, not from when it was asked: a call that
  blocked (a node that does not answer takes the connect timeout per address) would otherwise leave one
  already over, and the next attempt would not wait at all. `now` is the caller's instant, which a test may
  set ahead of the clock.
  """
  @spec backoff_until(integer()) :: integer()
  def backoff_until(now), do: max(now, mono_ms()) + @retry_ms

  @doc """
  The routes of `topic`, read again if they were forgotten. A topic whose routes could not be read is not
  asked again until its backoff ends, whichever path asks: until then the answer is the refusal the routes
  last got, with no round trip.
  """
  @spec ensure_routes(map(), String.t(), integer()) :: {:ok, map()} | {:error, String.t(), map()}
  def ensure_routes(client, topic, now) do
    cond do
      Router.routes(client.router, topic) ->
        {:ok, client}

      match?({until, _reason} when until > now, Map.get(client.unroutable, topic)) ->
        {:error, elem(Map.fetch!(client.unroutable, topic), 1), client}

      true ->
        case refresh(client, topic) do
          {:ok, client} ->
            {:ok, %{client | unroutable: Map.delete(client.unroutable, topic)}}

          {:error, reason, client} ->
            unroutable = Map.put(client.unroutable, topic, {backoff_until(now), reason})
            {:error, reason, %{client | unroutable: unroutable}}
        end
    end
  end

  @doc """
  Reads the cluster state and `topic`'s routes over the metadata connection, opened on first use to the
  bootstrap address or, should that not answer, to each broker the cluster state lists in turn. Not held
  back by the topic's backoff: `ensure_routes/3` is what honours it.
  """
  @spec refresh(map(), String.t()) :: {:ok, map()} | {:error, String.t(), map()}
  def refresh(client, topic), do: client |> bump(:refreshes) |> read_routes(topic, client.control != nil)

  # A metadata connection kept from an earlier read may have been closed by the server since (an idle one
  # is, after `MALACHI_TCP_RECV_TIMEOUT`): one that fails is replaced and the read made again, once.
  defp read_routes(client, topic, reused?) do
    case control(client) do
      {:ok, client, conn} ->
        case Router.refresh(client.router, conn, topic) do
          {:ok, router, conn} ->
            {:ok, %{client | router: router, control: conn}}

          {:error, reason, conn} when is_binary(reason) ->
            {:error, reason, %{client | control: conn}}

          {:error, _transport} ->
            Conn.close(conn)
            client = %{client | control: nil}
            if reused?, do: read_routes(client, topic, false), else: {:error, "metadata_unavailable", client}
        end

      {:error, client} ->
        {:error, "metadata_unavailable", client}
    end
  end

  defp control(%{control: nil} = client) do
    client.router
    |> control_addresses()
    |> Enum.find_value({:error, client}, fn address ->
      case connect(client, address) do
        {:ok, conn} -> {:ok, %{client | control: conn}, conn}
        {:error, _} -> nil
      end
    end)
  end

  defp control(%{control: conn} = client), do: {:ok, client, conn}

  defp control_addresses(%Router{bootstrap: bootstrap, brokers: brokers}) do
    advertised = for {_id, %{host: host, port: port}} <- brokers, is_binary(host) and port > 0, do: {host, port}
    Enum.uniq([bootstrap | advertised])
  end

  @doc """
  A range whose open the broker refused as a redirect (`is_redirect/1`): the routes the open went by are
  behind the cluster's. Not a failure: counted in `:redirects`, the topic's routes forgotten so the next
  look reads them again, and the lane (`key` in the client's `:lanes`, with `:stream`, `:redirects` and
  `:retry_at`) due again at `now` for the first four in a row, after a growing backoff from the fifth
  (`redirect_retry_at/2`). Due, not left undated: the lane wakes the client by its own `retry_at`, whatever
  else does or does not come, rather than wait for some later pass to notice it closed.
  """
  @spec redirected(map(), {String.t(), non_neg_integer()}, map(), integer()) :: map()
  def redirected(client, key, lane, now), do: redirect(client, key, lane, &(redirect_retry_at(&1, now) || now))

  @doc """
  `redirected/4` for routes that name a range's primary with no address: the same redirect, but never due
  before a fixed backoff (`backoff_until/1`), not even the first in a row. An address takes gossip rounds to
  spread, not microseconds, and every lane of the topic whose primary is that broker reads the routes again
  on its own, so reading them at once would only add metadata reads to the cluster being measured.
  """
  @spec unadvertised(map(), {String.t(), non_neg_integer()}, map(), integer()) :: map()
  def unadvertised(client, key, lane, now),
    do: redirect(client, key, lane, &max(redirect_retry_at(&1, now) || now, backoff_until(now)))

  defp redirect(client, {topic, _range} = key, lane, retry_at) do
    redirects = lane.redirects + 1
    lane = %{lane | stream: nil, redirects: redirects, retry_at: retry_at.(redirects)}
    %{bump(client, :redirects) | lanes: Map.put(client.lanes, key, lane)} |> forget(topic)
  end

  @doc "Forgets `topic`'s routes, so the next look reads them again."
  @spec forget(map(), String.t()) :: map()
  def forget(client, topic), do: %{client | router: Router.forget(client.router, topic)}

  @doc "Counts one more `stat`."
  @spec bump(map(), atom()) :: map()
  def bump(client, stat), do: %{client | stats: Map.update!(client.stats, stat, &(&1 + 1))}

  defp mono_ms, do: System.monotonic_time(:millisecond)
end
