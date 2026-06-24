defmodule Malachi.Broker do
  @moduledoc """
  The bridge between the control plane and the data plane on a single node.

  `Malachi.Metadata` is the **source of truth** for structure — which topics and ranges
  exist, their keyspace bounds and active/sealed state. The broker pairs that with the data
  plane: one `Malachi.Log` of records per range, keyed by range id. Producing routes a
  record to the active range that owns its key (looked up from the metadata, hashed with
  `Malachi.Keyspace`), then appends to that range's log; splitting/merging go through
  `Metadata` for the structure and seal the affected logs. So the logical decisions live in
  one place (the control plane) instead of being duplicated in the storage layer.

  Within a range the data is divided into **segments** — NorthGuard's unit of replication.
  Each active range has one open segment; the broker registers it in the metadata (choosing a
  replica set via `Malachi.Cluster.Placement`), tallies the records appended to it, and seals
  it once it reaches `:segment_max_bytes`, rolling to a fresh one (a "logical" segment: a span
  of offsets over the range's single log, not a separate file). The cross-node replication of a
  segment's replica set is a later step; for now the placement is recorded but the data is local.

  This supersedes the standalone `Malachi.Topic`/`Range`, whose split/merge/coverage logic
  duplicated the metadata machine. Those modules — and cross-epoch reads on top of the
  broker — are retired/rebuilt in a follow-up.

  Single-node and `Metadata`-backed for now; in a cluster the structure comes from the
  `Malachi.Cluster.DSRSM` (the sharded, replicated control plane). Like the layers it
  composes, the broker is a functional value threaded through calls (no GenServer).
  """

  alias Malachi.Cluster.Placement
  alias Malachi.Keyspace
  alias Malachi.Log
  alias Malachi.Log.Record
  alias Malachi.Metadata

  # 64 MiB: the active segment seals once it reaches this many encoded bytes (soft threshold,
  # checked at produce-batch boundaries — see `bump_segment/4`).
  @default_segment_max_bytes 64 * 1024 * 1024

  @typedoc "The broker's view of the open (unsealed) segment of a range."
  @type active_segment :: %{
          id: Metadata.segment_id(),
          start_offset: non_neg_integer(),
          records: non_neg_integer(),
          bytes: non_neg_integer()
        }

  @type t :: %__MODULE__{
          directory: Path.t(),
          metadata: Metadata.t(),
          logs: %{Metadata.range_id() => Log.t()},
          segment_opts: keyword(),
          brokers: [Metadata.broker()],
          replication_factor: pos_integer(),
          segment_max_bytes: pos_integer(),
          segments: %{Metadata.range_id() => active_segment()},
          segment_seq: %{Metadata.range_id() => non_neg_integer()}
        }

  defstruct directory: nil,
            metadata: nil,
            logs: %{},
            segment_opts: [],
            brokers: nil,
            replication_factor: 1,
            segment_max_bytes: @default_segment_max_bytes,
            segments: %{},
            segment_seq: %{}

  @doc """
  Opens an empty broker rooted at `directory`.

  ## Options
    * `:brokers` - the broker set segment replicas are placed on (default `[node()]`). Must be
      non-empty.
    * `:replication_factor` - replicas per segment, clamped to the broker count (default `1`).
    * `:segment_max_bytes` - the active segment seals once it reaches this many encoded bytes
      (default 64 MiB).
    * any remaining options are forwarded to each range's `Malachi.Log` (e.g. `:max_bytes`,
      `:flush_bytes`, `:index_interval`).
  """
  @spec open(Path.t(), keyword()) :: {:ok, t()}
  def open(directory, opts \\ []) do
    File.mkdir_p!(directory)

    {brokers, opts} = Keyword.pop(opts, :brokers, [node()])
    {replication_factor, opts} = Keyword.pop(opts, :replication_factor, 1)
    {segment_max_bytes, log_opts} = Keyword.pop(opts, :segment_max_bytes, @default_segment_max_bytes)

    validate_policy!(brokers, replication_factor, segment_max_bytes)

    {:ok,
     %__MODULE__{
       directory: directory,
       metadata: Metadata.new(),
       logs: %{},
       segment_opts: log_opts,
       brokers: brokers,
       replication_factor: replication_factor,
       segment_max_bytes: segment_max_bytes,
       segments: %{},
       segment_seq: %{}
     }}
  end

  defp validate_policy!(brokers, replication_factor, segment_max_bytes) do
    cond do
      not (is_list(brokers) and brokers != []) ->
        raise ArgumentError, ":brokers must be a non-empty list of brokers to place replicas on"

      not (is_integer(replication_factor) and replication_factor >= 1) ->
        raise ArgumentError, ":replication_factor must be a positive integer"

      not (is_integer(segment_max_bytes) and segment_max_bytes >= 1) ->
        raise ArgumentError, ":segment_max_bytes must be a positive integer"

      true ->
        :ok
    end
  end

  @doc """
  Creates a topic (and its root range) in the control plane. Returns the updated broker and
  `{:ok, root_range_id}` or a `Metadata` error. No log is created until the first produce.
  """
  @spec create_topic(t(), Metadata.topic_name(), pos_integer()) :: {t(), term()}
  def create_topic(%__MODULE__{} = broker, name, keyspace_bits) do
    {metadata, reply} = Metadata.apply(broker.metadata, {:create_topic, name, keyspace_bits})
    {%{broker | metadata: metadata}, reply}
  end

  @doc """
  Routes each record to the active range that owns its key (per the metadata) and appends it
  to that range's log. Returns `{:ok, broker, placements}` where `placements` maps a
  `range_id` to `{first_offset, last_offset}`, or `{:error, reason}`
  (`:no_such_topic`, or `{:unroutable, key}` if no active range covers a key).
  """
  @spec produce(t(), Metadata.topic_name(), [Malachi.Log.Record.t()]) ::
          {:ok, t(), %{Metadata.range_id() => {non_neg_integer(), non_neg_integer()}}}
          | {:error, term()}
  def produce(%__MODULE__{} = broker, topic, records) when is_list(records) do
    case Metadata.get_topic(broker.metadata, topic) do
      nil -> {:error, :no_such_topic}
      topic_meta -> route_and_append(broker, topic, topic_meta, records)
    end
  end

  @doc "Flushes and fsyncs every open range log."
  @spec sync(t()) :: {:ok, t()}
  def sync(%__MODULE__{} = broker) do
    logs =
      Map.new(broker.logs, fn {range_id, log} ->
        {:ok, log} = Log.sync(log)
        {range_id, log}
      end)

    {:ok, %{broker | logs: logs}}
  end

  @doc """
  Reads up to `max_records` committed records from `range_id`'s log, starting at `offset`.
  Returns `:eof` if the range has no log yet (nothing produced to it).
  """
  @spec read(t(), Metadata.range_id(), non_neg_integer(), pos_integer()) ::
          {:ok, [Malachi.Log.Record.t()]} | :eof | {:error, term()}
  def read(%__MODULE__{} = broker, range_id, offset, max_records) do
    case Map.fetch(broker.logs, range_id) do
      :error -> :eof
      {:ok, log} -> Log.read(log, offset, max_records)
    end
  end

  @doc """
  Splits a range: the control plane seals the parent and creates two children; the data
  plane seals (flushes) the parent's log. Returns `{broker, {:ok, left_id, right_id}}` or a
  `Metadata` error.
  """
  @spec split_range(t(), Metadata.range_id()) :: {t(), term()}
  def split_range(%__MODULE__{} = broker, range_id) do
    case Metadata.apply(broker.metadata, {:split_range, range_id}) do
      {metadata, {:ok, _left, _right} = reply} ->
        broker = seal_active_segment(%{broker | metadata: metadata}, range_id)
        {seal_log(broker, range_id), reply}

      {_metadata, {:error, _reason} = error} ->
        {broker, error}
    end
  end

  @doc """
  Merges two buddy ranges: the control plane seals both and creates a child; the data plane
  seals both logs. Returns `{broker, {:ok, child_id}}` or a `Metadata` error.
  """
  @spec merge_ranges(t(), Metadata.range_id(), Metadata.range_id()) :: {t(), term()}
  def merge_ranges(%__MODULE__{} = broker, range_id_a, range_id_b) do
    case Metadata.apply(broker.metadata, {:merge_ranges, range_id_a, range_id_b}) do
      {metadata, {:ok, _child} = reply} ->
        broker = %{broker | metadata: metadata}
        broker = seal_active_segment(seal_active_segment(broker, range_id_a), range_id_b)
        {seal_log(seal_log(broker, range_id_a), range_id_b), reply}

      {_metadata, {:error, _reason} = error} ->
        {broker, error}
    end
  end

  @doc "The ids of a topic's active ranges (those that currently tile the keyspace)."
  @spec active_range_ids(t(), Metadata.topic_name()) :: [Metadata.range_id()]
  def active_range_ids(%__MODULE__{} = broker, topic) do
    broker.metadata |> Metadata.active_ranges_of_topic(topic) |> Enum.map(& &1.id)
  end

  @doc "Whether any open range log has buffered records not yet flushed."
  @spec pending?(t()) :: boolean()
  def pending?(%__MODULE__{} = broker) do
    Enum.any?(broker.logs, fn {_range_id, log} -> Log.pending?(log) end)
  end

  @typedoc "Opaque cursor for `stream_history/4`: `:start`, an internal position, or `:done`."
  @type history_cursor :: :start | {non_neg_integer(), non_neg_integer()} | :done

  @doc """
  Streams one bounded page of a range's **cross-epoch** history: records its sealed
  ancestors hold for this range's keyspace slice (oldest first, in happens-before order),
  then the range's own records. The lineage comes from the control plane
  (`Metadata` `parents`); ancestor records are filtered to the range's slice via `Keyspace`.

  Returns `{:ok, records, next_cursor}`; call again with `next_cursor` until it is `:done`.
  A page may be empty while `next_cursor` is not `:done`. `{:error, :no_such_range}` if the
  range is unknown.
  """
  @spec stream_history(t(), Metadata.range_id(), history_cursor(), pos_integer()) ::
          {:ok, [Malachi.Log.Record.t()], history_cursor()} | {:error, term()}
  def stream_history(broker, range_id, cursor \\ :start, max_records \\ 1000)

  def stream_history(%__MODULE__{}, _range_id, :done, _max_records), do: {:ok, [], :done}

  def stream_history(%__MODULE__{} = broker, range_id, cursor, max_records)
      when is_integer(max_records) and max_records > 0 do
    case Metadata.get_range(broker.metadata, range_id) do
      nil ->
        {:error, :no_such_range}

      range ->
        sources = history_sources(range_id, range)
        {source_index, source_offset} = normalize_cursor(cursor)
        read_history_page(broker, sources, source_index, source_offset, max_records)
    end
  end

  @doc """
  Convenience that pages `stream_history/4` to the end and returns every record as one
  ordered list. Loads the whole history into memory — for bounded/administrative use.
  """
  @spec read_history(t(), Metadata.range_id()) :: {:ok, [Malachi.Log.Record.t()]} | {:error, term()}
  def read_history(%__MODULE__{} = broker, range_id) do
    drain_history(broker, range_id, :start, [])
  end

  @doc "Closes every open range log's file handle."
  @spec close(t()) :: :ok
  def close(%__MODULE__{} = broker) do
    Enum.each(broker.logs, fn {_range_id, log} -> Log.close(log) end)
    :ok
  end

  # --- internals ---

  defp route_and_append(broker, topic, topic_meta, records) do
    active_ranges = Metadata.active_ranges_of_topic(broker.metadata, topic)

    case group_by_owning_range(records, active_ranges, topic_meta.keyspace_size) do
      {:error, _reason} = error -> error
      {:ok, grouped} -> append_groups(broker, grouped)
    end
  end

  # Groups records by the active range that owns each key, or errors if any key is uncovered.
  defp group_by_owning_range(records, active_ranges, keyspace_size) do
    Enum.reduce_while(records, {:ok, %{}}, fn record, {:ok, groups} ->
      case owning_range_id(active_ranges, keyspace_size, record.key) do
        nil -> {:halt, {:error, {:unroutable, record.key}}}
        range_id -> {:cont, {:ok, Map.update(groups, range_id, [record], &[record | &1])}}
      end
    end)
  end

  defp owning_range_id(active_ranges, keyspace_size, key) do
    position = Keyspace.position_of(key, keyspace_size)

    Enum.find_value(active_ranges, fn range ->
      if Keyspace.within?(position, range.key_start, range.key_end), do: range.id
    end)
  end

  defp append_groups(broker, grouped) do
    {broker, placements} =
      Enum.reduce(grouped, {broker, %{}}, fn {range_id, reversed_records}, {broker, placements} ->
        records = Enum.reverse(reversed_records)
        {broker, log} = fetch_or_open_log(broker, range_id)
        {:ok, log, first_offset, last_offset} = Log.append(log, records)
        broker = %{broker | logs: Map.put(broker.logs, range_id, log)}
        broker = account_segment(broker, range_id, records, first_offset, last_offset)
        {broker, Map.put(placements, range_id, {first_offset, last_offset})}
      end)

    {:ok, broker, placements}
  end

  # --- segment lifecycle (logical spans over the per-range log) ---

  # Records the just-appended records against the range's active segment, opening one if needed,
  # then seals it if it crossed the size threshold.
  defp account_segment(broker, range_id, records, first_offset, last_offset) do
    broker =
      case Map.has_key?(broker.segments, range_id) do
        true -> broker
        false -> open_segment(broker, range_id, first_offset)
      end

    bytes = Enum.reduce(records, 0, fn record, acc -> acc + Record.encoded_size(record) end)
    count = last_offset - first_offset + 1
    bump_segment(broker, range_id, count, bytes)
  end

  # Registers a new segment for `range_id` starting at `start_offset`, choosing its replica set
  # via the placement policy. The id `{range_id, seq}` is globally unique (range ids are) and the
  # per-range `seq` counter persists across seals, so a sealed segment's id is never reused.
  defp open_segment(broker, range_id, start_offset) do
    seq = Map.get(broker.segment_seq, range_id, 0)
    segment_id = {range_id, seq}
    {:ok, replica_set} = Placement.place(segment_id, broker.brokers, broker.replication_factor)

    {metadata, :ok} =
      Metadata.apply(broker.metadata, {:register_segment, range_id, segment_id, replica_set, start_offset})

    active = %{id: segment_id, start_offset: start_offset, records: 0, bytes: 0}

    %{
      broker
      | metadata: metadata,
        segments: Map.put(broker.segments, range_id, active),
        segment_seq: Map.put(broker.segment_seq, range_id, seq + 1)
    }
  end

  # Adds the appended records to the active segment's tallies, sealing it (soft threshold checked
  # at the batch boundary, so a segment may overshoot by at most one batch) once it reaches the
  # byte limit. The next produce to this range opens a fresh segment.
  defp bump_segment(broker, range_id, count, bytes) do
    active = Map.fetch!(broker.segments, range_id)
    active = %{active | records: active.records + count, bytes: active.bytes + bytes}
    broker = %{broker | segments: Map.put(broker.segments, range_id, active)}

    if active.bytes >= broker.segment_max_bytes do
      seal_active_segment(broker, range_id)
    else
      broker
    end
  end

  # Seals the range's active segment (recording its record count as the length) and forgets it,
  # so the next append opens a new one. No-op if the range has no open segment.
  defp seal_active_segment(broker, range_id) do
    case Map.fetch(broker.segments, range_id) do
      :error ->
        broker

      {:ok, active} ->
        {metadata, _reply} = Metadata.apply(broker.metadata, {:seal_segment, active.id, active.records})
        %{broker | metadata: metadata, segments: Map.delete(broker.segments, range_id)}
    end
  end

  defp fetch_or_open_log(broker, range_id) do
    case Map.fetch(broker.logs, range_id) do
      {:ok, log} ->
        {broker, log}

      :error ->
        {:ok, log} = Log.open(log_directory(broker, range_id), broker.segment_opts)
        {%{broker | logs: Map.put(broker.logs, range_id, log)}, log}
    end
  end

  # Flushes a range's log if one is open (the range is sealed in the metadata, so no further
  # produce routes to it; its records stay put — split/merge are logical).
  defp seal_log(broker, range_id) do
    case Map.fetch(broker.logs, range_id) do
      :error ->
        broker

      {:ok, log} ->
        {:ok, log} = Log.sync(log)
        %{broker | logs: Map.put(broker.logs, range_id, log)}
    end
  end

  defp log_directory(broker, {topic, seq}), do: Path.join(broker.directory, "#{topic}-#{seq}")

  # --- cross-epoch history ---

  # The ordered sources for a range's history: each sealed ancestor (its records filtered to
  # the target range's slice), then the range itself (no filter).
  defp history_sources(range_id, range) do
    ancestors = Enum.map(range.parents, fn ancestor_id -> {ancestor_id, range} end)
    ancestors ++ [{range_id, nil}]
  end

  defp normalize_cursor(:start), do: {0, 0}
  defp normalize_cursor({source_index, source_offset}), do: {source_index, source_offset}

  defp read_history_page(broker, sources, source_index, source_offset, max_records) do
    case Enum.at(sources, source_index) do
      nil ->
        {:ok, [], :done}

      {source_range_id, filter_range} ->
        case read_source(broker, source_range_id, source_offset, max_records) do
          :eof ->
            read_history_page(broker, sources, source_index + 1, 0, max_records)

          {:ok, records} ->
            filtered = filter_records(records, filter_range)
            {:ok, filtered, {source_index, source_offset + length(records)}}
        end
    end
  end

  defp read_source(broker, range_id, offset, max_records) do
    case Map.fetch(broker.logs, range_id) do
      :error -> :eof
      {:ok, log} -> Log.read(log, offset, max_records)
    end
  end

  defp filter_records(records, nil), do: records

  defp filter_records(records, range) do
    Enum.filter(records, fn record ->
      Keyspace.within?(
        Keyspace.position_of(record.key, range.keyspace_size),
        range.key_start,
        range.key_end
      )
    end)
  end

  defp drain_history(broker, range_id, cursor, pages) do
    case stream_history(broker, range_id, cursor, 1000) do
      {:ok, records, :done} -> {:ok, [records | pages] |> Enum.reverse() |> List.flatten()}
      {:ok, records, next_cursor} -> drain_history(broker, range_id, next_cursor, [records | pages])
      {:error, _reason} = error -> error
    end
  end
end
