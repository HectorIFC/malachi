defmodule Malachi.Loadtest.Router do
  @moduledoc """
  Where a record goes, as a NorthGuard client knows it (meetup transcript, 609 to 613 and 885): the
  minimal cluster state any node answers (`cluster_state`, `Malachi.Wire` key 24), and per topic its
  routes (`topic_routes`, key 25), cached here and read again only when the cluster says they changed
  (a `moved`, `stale_routes`, `not_owner` or `migrating` answer), never on a timer.

  A record's key is hashed into the topic's keyspace exactly as the server does (`Malachi.Keyspace`), the
  active range holding that position is the range it goes to, and the broker leading that range's active
  segment is where the range's stream is opened. A range with no active segment (`unassigned`, before its
  first record or after a failover sealed its segment) is opened at a live broker, which places the segment
  and answers `moved` towards its primary (`address_of/3`).

  Pure but for `bootstrap/1` and `refresh/3`, which ask over a passive `Malachi.Loadtest.Conn`.
  """

  alias Malachi.Keyspace
  alias Malachi.Loadtest.Conn
  alias Malachi.Wire

  @enforce_keys [:bootstrap]
  # `topics` holds the routes a record is routed by, dropped by `forget/2` so the next one reads them again;
  # `last_read` keeps the last routes read for each topic even then, for `last_read/2`.
  defstruct [:bootstrap, brokers: %{}, streams_enabled: false, topics: %{}, last_read: %{}]

  @typedoc "A broker's client address."
  @type address :: {String.t(), :inet.port_number()}

  @typedoc "One topic's routes: its version, keyspace size and active ranges with their primaries."
  @type routes :: %{
          version: term(),
          size: pos_integer(),
          ranges: [
            %{
              range: non_neg_integer(),
              key_start: non_neg_integer(),
              key_end: non_neg_integer(),
              primary: String.t() | nil
            }
          ]
        }

  @type t :: %__MODULE__{
          bootstrap: address(),
          brokers: %{String.t() => %{host: String.t() | nil, port: non_neg_integer(), status: atom()}},
          streams_enabled: boolean(),
          topics: %{String.t() => routes()}
        }

  @doc "A router that knows only the address it bootstraps from."
  @spec new(address()) :: t()
  def new({host, port} = bootstrap) when is_binary(host) and is_integer(port), do: %__MODULE__{bootstrap: bootstrap}

  @doc """
  Reads the cluster state through `conn`, an authenticated connection to the router's bootstrap address.
  `{:legacy, :no_cluster_state, conn}` from a node too old to answer it (any refusal of the key), and
  `{:legacy, :malformed_cluster_state, conn}` from one whose answer is not a cluster state, so the client
  keeps to the legacy keys; otherwise the router, which says in `streams_enabled?/1` whether the
  stream keys are served.
  """
  @spec bootstrap(t(), Conn.t()) ::
          {:ok, t(), Conn.t()} | {:legacy, :no_cluster_state | :malformed_cluster_state, Conn.t()} | {:error, term()}
  def bootstrap(%__MODULE__{} = router, conn) do
    case Conn.request(conn, Wire.cluster_state_key(), 24, Wire.encode_cluster_state_req()) do
      {:ok, 0, payload, conn} -> decoded(router, payload, conn)
      {:ok, _code, _refusal, conn} -> {:legacy, :no_cluster_state, conn}
      {:error, _reason} = failed -> failed
    end
  end

  # An answer that is not a cluster state (a server that answers every key with an empty ok) is a server
  # that does not speak the stream keys either.
  defp decoded(router, payload, conn) do
    {:ok, put_cluster_state(router, Wire.decode_cluster_state_resp(payload)), conn}
  rescue
    _malformed in [FunctionClauseError, MatchError, CaseClauseError, ArgumentError] ->
      {:legacy, :malformed_cluster_state, conn}
  end

  @doc """
  Reads the cluster state and `topic`'s routes again through `conn`: after a move, or a connection to a
  primary that failed, either may have changed. `{:error, reason, conn}` when the server refused either
  (the topic does not exist, the stream keys were turned off), with `reason` as it answered.
  """
  @spec refresh(t(), Conn.t(), String.t()) ::
          {:ok, t(), Conn.t()} | {:error, String.t(), Conn.t()} | {:error, term()}
  def refresh(%__MODULE__{} = router, conn, topic) do
    with {:ok, router, conn} <- refresh_cluster(router, conn) do
      case Conn.request(conn, Wire.topic_routes_key(), 25, Wire.encode_topic_routes_req(topic)) do
        {:ok, 0, payload, conn} -> {:ok, put_routes(router, Wire.decode_topic_routes_resp(payload)), conn}
        {:ok, _code, refusal, conn} -> {:error, Wire.decode_error_reason(refusal), conn}
        {:error, _reason} = failed -> failed
      end
    end
  end

  defp refresh_cluster(router, conn) do
    case bootstrap(router, conn) do
      {:legacy, reason, conn} -> {:error, Atom.to_string(reason), conn}
      other -> other
    end
  end

  @doc "Takes a decoded `cluster_state` answer."
  @spec put_cluster_state(t(), map()) :: t()
  def put_cluster_state(%__MODULE__{} = router, %{brokers: brokers, streams_enabled: enabled}) do
    %{
      router
      | brokers: Map.new(brokers, fn broker -> {broker.id, Map.take(broker, [:host, :port, :status])} end),
        streams_enabled: enabled
    }
  end

  @doc "Takes a decoded `topic_routes` answer: only the active ranges route records."
  @spec put_routes(t(), map()) :: t()
  def put_routes(%__MODULE__{} = router, %{topic: topic, version: version, keyspace_bits: bits, ranges: ranges}) do
    active =
      for %{state: :active} = range <- ranges do
        %{
          range: range.range,
          key_start: range.key_start,
          key_end: range.key_end,
          primary: range.segment && range.segment.primary
        }
      end

    routes = %{version: version, size: Bitwise.bsl(1, bits), ranges: active}
    %{router | topics: Map.put(router.topics, topic, routes), last_read: Map.put(router.last_read, topic, routes)}
  end

  @doc "Forgets `topic`'s routes, so the next record for it reads them again."
  @spec forget(t(), String.t()) :: t()
  def forget(%__MODULE__{} = router, topic), do: %{router | topics: Map.delete(router.topics, topic)}

  @doc "Whether the cluster serves the stream keys (the `producer_streams` flag)."
  @spec streams_enabled?(t()) :: boolean()
  def streams_enabled?(%__MODULE__{streams_enabled: enabled}), do: enabled

  @doc "`topic`'s routes, or nil when they have not been read (or were forgotten)."
  @spec routes(t(), String.t()) :: routes() | nil
  def routes(%__MODULE__{topics: topics}, topic), do: Map.get(topics, topic)

  @doc """
  The last routes read for `topic`, kept when `forget/2` drops them, or nil before any read. Not for
  routing a record, which reads the routes again once they are forgotten; for telling that a range was
  retired, which a later read never undoes within one life of the topic: a range a split or merge retired
  stays retired. A topic deleted and created again numbers its ranges from the start, but its next read
  replaces this before any record is routed by it.
  """
  @spec last_read(t(), String.t()) :: routes() | nil
  def last_read(%__MODULE__{last_read: last_read}, topic), do: Map.get(last_read, topic)

  @doc """
  The range of `topic` a record keyed `key` goes to: the active range whose keyspace block holds the key's
  position. `:unknown_topic` before the topic's routes are read; `:no_range` when no active range covers
  the position, which only routes caught mid-split can say, and which reading them again resolves.
  """
  @spec range_of(t(), String.t(), term()) :: {:ok, non_neg_integer()} | :unknown_topic | :no_range
  def range_of(%__MODULE__{} = router, topic, key) do
    case routes(router, topic) do
      nil ->
        :unknown_topic

      %{size: size, ranges: ranges} ->
        position = Keyspace.position_of(key, size)

        case Enum.find(ranges, &Keyspace.within?(position, &1.key_start, &1.key_end)) do
          nil -> :no_range
          %{range: range} -> {:ok, range}
        end
    end
  end

  @doc """
  The address to open `topic`'s `range` stream at: its primary's advertised address. A range with no
  active segment (never written, or its segment sealed by a failover) is opened at any live broker, which
  places its segment and answers `moved` toward its primary: one of the brokers the cluster state says
  are alive and advertise an address, picked by the range so a topic's ranges spread over the cluster,
  and so not a node that went down once the routes are read again. The bootstrap address when the cluster
  is one broker, or lists none alive. A primary that advertises no address can only be reached when it is
  the bootstrap node itself, the one broker of a single node cluster; in a larger one it is
  `{:error, :unadvertised}`, as is a primary the cluster state does not list.
  """
  @spec address_of(t(), String.t(), non_neg_integer()) ::
          {:ok, address()} | :unknown_topic | {:error, :unknown_range | :unadvertised}
  def address_of(%__MODULE__{} = router, topic, range) do
    case routes(router, topic) do
      nil ->
        :unknown_topic

      %{ranges: ranges} ->
        case Enum.find(ranges, &(&1.range == range)) do
          nil -> {:error, :unknown_range}
          %{primary: nil} -> {:ok, unassigned_address(router, topic, range)}
          %{primary: primary} -> broker_address(router, primary)
        end
    end
  end

  defp unassigned_address(%{brokers: brokers} = router, _topic, _range) when map_size(brokers) <= 1,
    do: router.bootstrap

  defp unassigned_address(router, topic, range) do
    live =
      for {_id, %{status: :alive, host: host, port: port}} <- Enum.sort(router.brokers),
          is_binary(host) and port > 0,
          do: {host, port}

    case live do
      [] -> router.bootstrap
      # hashed as a binary, so the Node client (scripts/lib/router.js) picks the same broker with its port of phash2
      _ -> Enum.at(live, :erlang.phash2("#{topic}/#{range}", length(live)))
    end
  end

  defp broker_address(router, primary) do
    case Map.fetch(router.brokers, primary) do
      {:ok, %{host: host, port: port}} when is_binary(host) and port > 0 -> {:ok, {host, port}}
      {:ok, _unadvertised} when map_size(router.brokers) == 1 -> {:ok, router.bootstrap}
      _unadvertised_or_unknown -> {:error, :unadvertised}
    end
  end

  @doc "The routes version of `topic` a stream is opened against, or nil before they are read."
  @spec version(t(), String.t()) :: term()
  def version(%__MODULE__{} = router, topic) do
    case routes(router, topic) do
      nil -> nil
      %{version: version} -> version
    end
  end
end
