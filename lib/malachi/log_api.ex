defmodule Malachi.LogApi do
  @moduledoc """
  The client-facing **log** API over a `Malachi.BrokerServer` — the NorthGuard model, not Kafka's.

  A client deals in `topic` + **key** (produce) + an **opaque cursor** (consume). It never sees
  partitions or offsets: those are internal (ranges and per-range offsets) and deliberately hidden,
  so the system can split/merge/restripe underneath without breaking clients. That hiding is the
  point — it is what lets malachi evolve its physical layout where Kafka leaks it to the client.

  The cursor is just a token the client echoes back; today it encodes the consumer's position as
  `%{range_id => next_offset}`, but its contents are not part of the contract. Because it comes from
  an untrusted client, `decode_cursor/1` uses `binary_to_term(_, [:safe])` and validates the shape.

  This is the first slice: `create_topic`, `produce` (by key), and `fetch` (by cursor) on a topic's
  current ranges. Consumer groups, server-side committed positions, long-poll, and consuming across
  ranges that split are later slices.
  """

  alias Malachi.BrokerServer
  alias Malachi.Log.Record
  alias Malachi.Metadata

  # Keyspace size 2^8 = 256, leaving room for the topic's range to split as it grows. The client
  # does not choose this (no "partition count" leaks to it).
  @default_keyspace_bits 8

  @typedoc "An opaque consumer position token (treat as a string; do not interpret)."
  @type cursor :: String.t()

  @doc "Creates `topic`. The client does not specify partitions — the keyspace is internal."
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
      {:ok, built} ->
        case BrokerServer.produce(server, topic, built) do
          {:ok, _placements} -> {:ok, length(built)}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Fetches up to `max` records per current range of `topic` from the position in `cursor` (`nil` or
  `:start` begins at the beginning). Returns `{:ok, records, next_cursor}` — advance by passing
  `next_cursor` back. Records carry no client-visible offset.
  """
  @spec fetch(GenServer.server(), Metadata.topic_name(), cursor() | nil | :start, pos_integer()) ::
          {:ok, [Record.t()], cursor()} | {:error, term()}
  def fetch(server, topic, cursor, max) when is_integer(max) and max > 0 do
    case decode_cursor(cursor) do
      {:ok, positions} ->
        {records, next_cursor} = do_fetch(server, topic, positions, max)
        {:ok, records, next_cursor}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Fetches for a consumer `group`, resuming from the group's durably committed position (or the
  beginning if it never committed). Returns `{:ok, records, next_cursor}`; the client processes the
  records and then `commit/4`s `next_cursor` to advance the durable position (at-least-once).
  """
  @spec fetch_group(GenServer.server(), Metadata.topic_name(), Metadata.group(), pos_integer()) ::
          {:ok, [Record.t()], cursor()}
  def fetch_group(server, topic, group, max) when is_integer(max) and max > 0 do
    positions = BrokerServer.committed_offsets(server, group, topic)
    {records, next_cursor} = do_fetch(server, topic, positions, max)
    {:ok, records, next_cursor}
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

  defp do_fetch(server, topic, positions, max) do
    ranges = BrokerServer.active_range_ids(server, topic)
    {records, positions} = read_ranges(server, ranges, positions, max)
    {records, encode_cursor(positions)}
  end

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

  # --- reading ---

  defp read_ranges(server, ranges, positions, max) do
    Enum.reduce(ranges, {[], positions}, fn range_id, {acc, positions} ->
      offset = Map.get(positions, range_id, 0)

      case BrokerServer.read(server, range_id, offset, max) do
        {:ok, records} -> {acc ++ records, Map.put(positions, range_id, offset + length(records))}
        _eof_or_error -> {acc, positions}
      end
    end)
  end

  # --- opaque cursor ---

  defp encode_cursor(positions), do: Base.url_encode64(:erlang.term_to_binary(positions))

  defp decode_cursor(nil), do: {:ok, %{}}
  defp decode_cursor(:start), do: {:ok, %{}}

  defp decode_cursor(token) when is_binary(token) do
    with {:ok, binary} <- Base.url_decode64(token),
         {:ok, positions} <- safe_term(binary),
         true <- valid_positions?(positions) do
      {:ok, positions}
    else
      _ -> {:error, :invalid_cursor}
    end
  end

  defp decode_cursor(_other), do: {:error, :invalid_cursor}

  # Never trust a client-supplied binary: :safe forbids new atoms and unsafe terms.
  defp safe_term(binary) do
    {:ok, :erlang.binary_to_term(binary, [:safe])}
  rescue
    ArgumentError -> :error
  end

  defp valid_positions?(positions) when is_map(positions) do
    Enum.all?(positions, fn
      {{topic, seq}, offset} ->
        is_binary(topic) and is_integer(seq) and seq >= 0 and is_integer(offset) and offset >= 0

      _other ->
        false
    end)
  end

  defp valid_positions?(_other), do: false
end
