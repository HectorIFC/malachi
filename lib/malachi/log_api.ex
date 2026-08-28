defmodule Malachi.LogApi do
  @moduledoc """
  The client-facing **log** API over a `Malachi.BrokerServer`: the NorthGuard model, not Kafka's.

  A client deals in `topic` + **key** (produce) + an **opaque cursor** (consume). It never sees
  partitions or offsets: those are internal (ranges and per-range offsets) and deliberately hidden,
  so the system can split/merge/restripe underneath without breaking clients. That hiding is the
  point: it is what lets malachi evolve its physical layout where Kafka leaks it to the client.

  The cursor is just a token the client echoes back; today it encodes the consumer's position as
  `%{range_id => next_offset}`, but its contents are not part of the contract. Because it comes from
  an untrusted client, `decode_cursor/1` bounds its size, uses `binary_to_term(_, [:safe])` and
  validates the shape.

  Those three steps answer different attacks, and only the middle one is about code execution. On
  OTP 28 the `:safe` option rejects both a fun and an atom the node has never seen, so a crafted
  token cannot introduce code or exhaust the atom table. What `:safe` does not bound is *size*: the
  Erlang external format spends about 2.7 bytes per list element where the heap spends 16, so a token
  the wire would accept buys several times its own weight in memory. `@max_cursor_bytes` is the guard
  for that, and `valid_positions?/1` then rejects anything that decoded cleanly but is not a cursor.

  `create_topic`, `produce` (by key) and `fetch`/`fetch_group` (by opaque cursor) over a topic's
  ranges, including consumer groups with server-side committed positions, cross-epoch consume across
  ranges that split, and long-poll (`fetch`/`fetch_group` with `wait_ms`). The read orchestration
  itself lives in `Malachi.BrokerServer` (one coherent call that can also park long-poll waiters).
  """

  require OpenTelemetry.Tracer, as: Tracer

  alias Malachi.BrokerServer
  alias Malachi.Consumer.GroupCoordinator
  alias Malachi.Log.Record
  alias Malachi.Metadata
  alias Malachi.Telemetry

  # Keyspace size 2^8 = 256, leaving room for the topic's range to split as it grows. The client
  # does not choose this (no "partition count" leaks to it).
  @default_keyspace_bits 8

  @typedoc "An opaque consumer position token (treat as a string; do not interpret)."
  @type cursor :: String.t()

  @doc "Creates `topic`. The client does not specify partitions: the keyspace is internal."
  @spec create_topic(GenServer.server(), Metadata.topic_name()) :: :ok | {:error, term()}
  def create_topic(server, topic) do
    case BrokerServer.create_topic(server, topic, @default_keyspace_bits) do
      {:ok, _root_id} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Produces `records` (each `%{"value" => binary, optional "key" => binary, optional "headers" =>
  map}`) to `topic`, routed by key. Returns `{:ok, produced_count}`; the client gets no offsets.
  """
  @spec produce(GenServer.server(), Metadata.topic_name(), [map()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def produce(server, topic, records) when is_list(records) do
    case build_records(records) do
      {:ok, built} -> produce_records(server, topic, built)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Appends already-built `Malachi.Log.Record`s (offset unassigned) to `topic`: the binary protocol path,
  which decodes records off the wire directly, skipping the JSON map→record step of `produce/3`. Returns
  `{:ok, count}` or `{:error, reason}`.
  """
  @spec produce_records(GenServer.server(), Metadata.topic_name(), [Record.t()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def produce_records(server, topic, records) when is_list(records) do
    Tracer.with_span "malachi.produce" do
      case BrokerServer.produce(server, topic, records) do
        {:ok, _placements} ->
          count = length(records)
          bytes = value_bytes(records)
          Tracer.set_attributes(%{"malachi.topic" => topic, "malachi.records" => count, "malachi.bytes" => bytes})
          Telemetry.produce(topic, count, bytes)
          {:ok, count}

        {:error, reason} ->
          Tracer.set_attributes(%{"malachi.topic" => topic, "malachi.error" => inspect(reason)})
          {:error, reason}
      end
    end
  end

  @doc """
  Fetches up to `max` records per current range of `topic` from the position in `cursor` (`nil` or
  `:start` begins at the beginning). Returns `{:ok, records, next_cursor}`, advance by passing
  `next_cursor` back. Records carry no client-visible offset.
  """
  @spec fetch(GenServer.server(), Metadata.topic_name(), cursor() | nil | :start, pos_integer(), non_neg_integer()) ::
          {:ok, [Record.t()], cursor()} | {:error, term()}
  def fetch(server, topic, cursor, max, wait_ms \\ 0) when is_integer(max) and max > 0 do
    case decode_cursor(cursor) do
      {:ok, positions} -> do_fetch(server, topic, positions, max, wait_ms)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Fetches for a consumer `group`, resuming from the group's durably committed position (or the
  beginning if it never committed). Returns `{:ok, records, next_cursor}`; the client processes the
  records and then `commit/4`s `next_cursor` to advance the durable position (at-least-once).
  """
  @spec fetch_group(GenServer.server(), Metadata.topic_name(), Metadata.group(), pos_integer(), non_neg_integer()) ::
          {:ok, [Record.t()], cursor()} | {:error, term()}
  def fetch_group(server, topic, group, max, wait_ms \\ 0) when is_integer(max) and max > 0 do
    positions = BrokerServer.committed_offsets(server, group, topic)
    do_fetch(server, topic, positions, max, wait_ms)
  end

  @doc """
  Fetches for `member` of consumer `group` on `topic`: consults the `coordinator` for the member's
  assigned ranges (registering it if new: the fetch is the heartbeat) and consumes **only** those ranges
  from the group's committed positions. Returns `{:ok, records, next_cursor}`; the opaque cursor covers
  only the member's slice, so `commit/4` advances just its ranges (the client never sees a range id).
  Members of the same group thus consume **disjoint** records in parallel.
  """
  @spec fetch_member(
          GenServer.server(),
          GenServer.server(),
          Metadata.topic_name(),
          Metadata.group(),
          term(),
          pos_integer(),
          non_neg_integer()
        ) :: {:ok, [Record.t()], cursor()} | {:error, term()}
  def fetch_member(server, coordinator, topic, group, member, max, wait_ms \\ 0) when is_integer(max) and max > 0 do
    case GroupCoordinator.poll(coordinator, group, topic, member) do
      {:ok, _generation, ranges} ->
        positions = BrokerServer.committed_offsets(server, group, topic)
        do_fetch(server, topic, Map.take(positions, ranges), max, wait_ms, ranges)

      # this node no longer owns the topic's coordination (stale routing during failover), the client
      # re-resolves and retries against the new owner
      {:error, _reason} = error ->
        error
    end
  end

  @doc """
  Durably commits a consumer `group`'s position for `topic` from `cursor` (a token from `fetch`/
  `fetch_group`). Returns `:ok`, or `{:error, :invalid_cursor}` for a bad token.
  """
  @spec commit(GenServer.server(), Metadata.topic_name(), Metadata.group(), cursor()) ::
          :ok | {:error, term()}
  def commit(server, topic, group, cursor) do
    case decode_cursor(cursor) do
      {:ok, positions} -> BrokerServer.commit_offset(server, group, topic, positions)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Opens a streaming subscription: registers the **calling process** as a push subscriber of `topic` for
  consumer `group`, bounded by a credit `window` (max in-flight records) and a `max` per-push batch. The
  broker resumes from the group's committed position, pushes an initial backlog, and then pushes new
  records on produce as `{:log_records, topic, records, positions}` messages to the caller. Returns `:ok`.
  """
  @spec subscribe(GenServer.server(), Metadata.topic_name(), Metadata.group(), pos_integer(), pos_integer()) :: :ok
  def subscribe(server, topic, group, window, max) do
    BrokerServer.subscribe(server, topic, group, window, max)
  end

  @doc """
  Like `subscribe/5` but for a consumer-group **member**: registers the member with the `coordinator`,
  scopes the push stream to its assigned ranges (server-side: the client still only gets records + an
  opaque cursor), and lets the broker leave the group when the calling process exits. The member stays
  alive by acking (`stream_ack_member/7`); the coordinator does not run in the broker, so the broker never
  calls it (deadlock-safe).
  """
  @spec subscribe_member(
          GenServer.server(),
          GenServer.server(),
          Metadata.topic_name(),
          Metadata.group(),
          term(),
          pos_integer(),
          pos_integer()
        ) ::
          :ok | {:error, term()}
  def subscribe_member(server, coordinator, topic, group, member, window, max) do
    case GroupCoordinator.poll(coordinator, group, topic, member) do
      {:ok, _generation, ranges} ->
        BrokerServer.subscribe(server, topic, group, window, max,
          member: member,
          ranges: ranges,
          coordinator: coordinator
        )

      # stale routing during a failover: the client re-resolves and re-subscribes against the new owner
      {:error, _reason} = error ->
        error
    end
  end

  @doc """
  Acks `count` streamed records for `group` at `cursor`: durably commits the position (at-least-once) and
  returns `count` records of window credit, unblocking further pushes. Returns `:ok`, or
  `{:error, :invalid_cursor}` for a bad token.
  """
  @spec stream_ack(GenServer.server(), Metadata.topic_name(), Metadata.group(), cursor(), non_neg_integer()) ::
          :ok | {:error, term()}
  def stream_ack(server, topic, group, cursor, count) do
    case decode_cursor(cursor) do
      {:ok, positions} -> BrokerServer.stream_ack(server, topic, group, positions, count)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Like `stream_ack/5` but for a group **member**: re-polls the `coordinator` (a heartbeat that also
  refreshes the member's ranges, so a rebalance is picked up on the next ack) before acking. Returns `:ok`
  or `{:error, :invalid_cursor}`.
  """
  @spec stream_ack_member(
          GenServer.server(),
          GenServer.server(),
          Metadata.topic_name(),
          Metadata.group(),
          term(),
          cursor(),
          non_neg_integer()
        ) ::
          :ok | {:error, term()}
  def stream_ack_member(server, coordinator, topic, group, member, cursor, count) do
    case decode_cursor(cursor) do
      {:ok, positions} ->
        case GroupCoordinator.poll(coordinator, group, topic, member) do
          # refresh the subscriber's ranges *and* its coordinator ref, so after a leadership change the
          # :DOWN leave (and later acks) target the current owner, not the one captured at subscribe time
          {:ok, _generation, ranges} ->
            BrokerServer.stream_ack(server, topic, group, positions, count, ranges, coordinator)

          # stale routing during a failover: fire-and-forget, the member's next ack re-resolves
          {:error, _reason} = error ->
            error
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Encodes internal consume `positions` into an opaque cursor. Public because the streaming push path
  (the connection forwarding `{:log_records, ...}`) must turn the pushed positions into a client cursor,
  the same token `fetch`/`fetch_group` return.
  """
  @spec encode_cursor(map()) :: cursor()
  def encode_cursor(positions), do: Base.url_encode64(:erlang.term_to_binary(positions))

  defp do_fetch(server, topic, positions, max, wait_ms, ranges \\ nil) do
    Tracer.with_span "malachi.consume" do
      case BrokerServer.consume(server, topic, positions, max, wait_ms, ranges) do
        # The broker cannot see this topic's metadata, so it has nothing true to say. Surfacing the
        # error is the difference between a client retrying and a client concluding the topic is empty.
        {:error, reason} ->
          Tracer.set_attributes(%{"malachi.topic" => topic, "malachi.error" => inspect(reason)})
          {:error, reason}

        {records, next_positions} ->
          count = length(records)
          Tracer.set_attributes(%{"malachi.topic" => topic, "malachi.records" => count})
          Telemetry.consume(topic, count)
          {:ok, records, encode_cursor(next_positions)}
      end
    end
  end

  defp value_bytes(records), do: Enum.reduce(records, 0, fn record, acc -> acc + byte_size(record.value) end)

  # --- records ---

  defp build_records(records) do
    Enum.reduce_while(records, {:ok, []}, fn record, {:ok, acc} ->
      case build_record(record) do
        {:ok, built} -> {:cont, {:ok, [built | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      error -> error
    end
  end

  defp build_record(%{"value" => value} = record) when is_binary(value) do
    {:ok, Record.new(value, key: key(record), headers: headers(record))}
  end

  defp build_record(_record), do: {:error, :invalid_record}

  defp key(%{"key" => key}) when is_binary(key), do: key
  defp key(_record), do: nil

  defp headers(%{"headers" => headers}) when is_map(headers) do
    for {name, value} <- headers, is_binary(name) and is_binary(value), do: {name, value}
  end

  defp headers(_record), do: []

  # --- opaque cursor ---

  defp decode_cursor(nil), do: {:ok, %{}}
  defp decode_cursor(:start), do: {:ok, %{}}

  # The size check comes first, and has to: everything after it decodes, and decoding is the step that
  # turns the client's bytes into our memory. A cursor covers one topic's ranges, so its size is set by
  # the keyspace, 2^8 = 256 ranges (`@default_keyspace_bits`), which no client can raise. Measured, 256
  # ranges encode to about 8KB under an ordinary topic name and about 93KB under a 255-character one,
  # so this ceiling clears the worst legitimate cursor by roughly 2.8x while cutting what an abusive
  # one can allocate from the frame limit's 16MB (`config/config.exs`) down to a couple of megabytes.
  @max_cursor_bytes 262_144

  defp decode_cursor(token) when is_binary(token) do
    with true <- byte_size(token) <= @max_cursor_bytes,
         {:ok, binary} <- Base.url_decode64(token),
         {:ok, positions} <- safe_term(binary),
         true <- valid_positions?(positions) do
      {:ok, positions}
    else
      _ -> {:error, :invalid_cursor}
    end
  end

  defp decode_cursor(_other), do: {:error, :invalid_cursor}

  # Never trust a client-supplied binary: on OTP 28 :safe rejects funs and atoms this node has never
  # seen, so what reaches valid_positions?/1 is plain data. Sobelow flags every binary_to_term on the
  # grounds that :safe still deserializes funs, which its own docs assert but which no longer holds;
  # the size ceiling in decode_cursor/1 covers the memory side the option genuinely does not.
  # sobelow_skip ["Misc.BinToTerm"]
  defp safe_term(binary) do
    {:ok, :erlang.binary_to_term(binary, [:safe])}
  rescue
    ArgumentError -> :error
  end

  defp valid_positions?(positions) when is_map(positions) do
    Enum.all?(positions, fn
      {{topic, seq}, position} ->
        is_binary(topic) and is_integer(seq) and seq >= 0 and valid_position?(position)

      _other ->
        false
    end)
  end

  defp valid_positions?(_other), do: false

  # A per-range consume position (a `Broker.consume_cursor`): the start sentinel or an internal
  # `{source_index, source_offset}` pair, both non-negative.
  defp valid_position?(:start), do: true

  defp valid_position?({source_index, source_offset}),
    do: is_integer(source_index) and source_index >= 0 and is_integer(source_offset) and source_offset >= 0

  defp valid_position?(_other), do: false
end
