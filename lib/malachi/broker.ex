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

  This supersedes the standalone `Malachi.Topic`/`Range`, whose split/merge/coverage logic
  duplicated the metadata machine. Those modules — and cross-epoch reads on top of the
  broker — are retired/rebuilt in a follow-up.

  Single-node and `Metadata`-backed for now; in a cluster the structure comes from the
  `Malachi.Cluster.DSRSM` (the sharded, replicated control plane). Like the layers it
  composes, the broker is a functional value threaded through calls (no GenServer).
  """

  alias Malachi.Keyspace
  alias Malachi.Log
  alias Malachi.Metadata

  @type t :: %__MODULE__{
          directory: Path.t(),
          metadata: Metadata.t(),
          logs: %{Metadata.range_id() => Log.t()}
        }

  defstruct directory: nil, metadata: nil, logs: %{}

  @doc "Opens an empty broker rooted at `directory`."
  @spec open(Path.t()) :: {:ok, t()}
  def open(directory) do
    File.mkdir_p!(directory)
    {:ok, %__MODULE__{directory: directory, metadata: Metadata.new(), logs: %{}}}
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
        {seal_log(%{broker | metadata: metadata}, range_id), reply}

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
        {seal_log(seal_log(broker, range_id_a), range_id_b), reply}

      {_metadata, {:error, _reason} = error} ->
        {broker, error}
    end
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
        {broker, Map.put(placements, range_id, {first_offset, last_offset})}
      end)

    {:ok, broker, placements}
  end

  defp fetch_or_open_log(broker, range_id) do
    case Map.fetch(broker.logs, range_id) do
      {:ok, log} ->
        {broker, log}

      :error ->
        {:ok, log} = Log.open(log_directory(broker, range_id))
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
end
