defmodule Malachi.Broker do
  @moduledoc """
  The control-plane **router** on a single node: it owns `Malachi.Metadata` (the source of truth
  for topics, ranges and segments) and decides *where* each record goes, but it no longer stores
  anything itself. Storage and replication of a segment's records live in
  `Malachi.Cluster.ReplicationServer`; the broker drives them through **injected effect functions**
  so its routing/lifecycle logic stays pure and testable with in-memory fakes.

  `Malachi.Metadata` is the source of truth for structure — which topics and ranges exist, their
  keyspace bounds and active/sealed state. Producing routes each record to the active range that
  owns its key (hashed with `Malachi.Keyspace`). Within a range the data is divided into
  **segments** — NorthGuard's unit of replication: each active range has one open segment, whose
  ordered `replica_set` is chosen by `Malachi.Cluster.Placement`. The broker registers segments,
  tallies bytes, and seals/rolls the active one once it reaches `:segment_max_bytes`. Offsets are
  contiguous *per range* (a segment is a window `[start_offset, ...)` of its range).

  Effects are injected, never performed here:

    * `replicate_fun.(primary, segment_id, replica_set, base_offset, records)` →
      `{:ok, last_offset} | {:error, reason}` — appends/replicates a batch (e.g.
      `&Malachi.Cluster.ReplicationServer.replicate/5`).
    * `read_fun.(ref, segment_id, offset, max_records)` →
      `{:ok, records} | :eof | {:error, reason}` — reads one segment (e.g.
      `&Malachi.Cluster.ReplicationServer.read/4`).

  The broker is a functional value threaded through calls (no GenServer); `Malachi.BrokerServer`
  wires the real `ReplicationServer`-backed effects and serializes access.
  """

  alias Malachi.Cluster.Placement
  alias Malachi.Keyspace
  alias Malachi.Log.Record
  alias Malachi.Metadata

  # 64 MiB: the active segment seals once it reaches this many encoded bytes (soft threshold,
  # checked at produce-batch boundaries — see `commit_batch/4`).
  @default_segment_max_bytes 64 * 1024 * 1024

  @typedoc "The broker's view of the open (unsealed) segment of a range."
  @type active_segment :: %{
          id: Metadata.segment_id(),
          start_offset: non_neg_integer(),
          records: non_neg_integer(),
          bytes: non_neg_integer(),
          replica_set: [Metadata.broker()]
        }

  @typedoc "Maps a range id to the `{first_offset, last_offset}` a produce placed there."
  @type placements :: %{Metadata.range_id() => {non_neg_integer(), non_neg_integer()}}

  @typedoc "Appends/replicates a batch to a segment, returning the last offset stored."
  @type replicate_fun ::
          (Metadata.broker(), Metadata.segment_id(), [Metadata.broker()], non_neg_integer(), [Record.t()] ->
             {:ok, non_neg_integer()} | {:error, term()})

  @typedoc "Reads up to `max_records` of a segment from `offset`."
  @type read_fun ::
          (Metadata.broker(), Metadata.segment_id(), non_neg_integer(), pos_integer() ->
             {:ok, [Record.t()]} | :eof | {:error, term()})

  @typedoc """
  Applies a metadata command, returning `{metadata, reply}` — the same shape as
  `Malachi.Metadata.apply/2` (the default), or a Raft-backed variant
  (`Malachi.Cluster.ReplicatedMetadata.apply_command/3`) that makes the metadata authoritative.
  """
  @type command_fun :: (Metadata.t(), Metadata.command() -> {Metadata.t(), term()})

  @type t :: %__MODULE__{
          metadata: Metadata.t(),
          command_fun: command_fun(),
          brokers: [Metadata.broker()],
          replication_factor: pos_integer(),
          segment_max_bytes: pos_integer(),
          segments: %{Metadata.range_id() => active_segment()},
          segment_seq: %{Metadata.range_id() => non_neg_integer()},
          offsets: %{Metadata.range_id() => non_neg_integer()}
        }

  defstruct metadata: nil,
            command_fun: nil,
            brokers: nil,
            replication_factor: 1,
            segment_max_bytes: @default_segment_max_bytes,
            # Rack/DC-aware placement: `spread_by` is the global attribute key to spread replicas over
            # (nil = off; a topic's storage policy can override it), `broker_attributes` maps each
            # broker to its attributes (refreshed from membership).
            spread_by: nil,
            broker_attributes: %{},
            segments: %{},
            segment_seq: %{},
            offsets: %{}

  @doc """
  Opens an empty broker.

  ## Options
    * `:brokers` - the broker set segment replicas are placed on (default `[node()]`). Must be
      non-empty.
    * `:replication_factor` - replicas per segment, clamped to the broker count (default `1`).
    * `:segment_max_bytes` - the active segment seals once it reaches this many encoded bytes
      (default 64 MiB).
    * `:command_fun` - how metadata mutations are applied (default `&Malachi.Metadata.apply/2`, an
      in-memory single node); pass a Raft-backed function to make the metadata authoritative.
    * `:metadata` - the initial metadata view (default empty), e.g. seeded from a replicated cluster.
  """
  @spec open(keyword()) :: {:ok, t()}
  def open(opts \\ []) do
    brokers = Keyword.get(opts, :brokers, [node()])
    replication_factor = Keyword.get(opts, :replication_factor, 1)
    segment_max_bytes = Keyword.get(opts, :segment_max_bytes, @default_segment_max_bytes)

    validate_policy!(brokers, replication_factor, segment_max_bytes)

    {:ok,
     %__MODULE__{
       metadata: Keyword.get(opts, :metadata) || Metadata.new(),
       command_fun: Keyword.get(opts, :command_fun, &Metadata.apply/2),
       brokers: brokers,
       replication_factor: replication_factor,
       segment_max_bytes: segment_max_bytes,
       spread_by: Keyword.get(opts, :spread_by),
       broker_attributes: Keyword.get(opts, :broker_attributes, %{})
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
  `{:ok, root_range_id}` or a `Metadata` error.
  """
  @spec create_topic(t(), Metadata.topic_name(), pos_integer()) :: {t(), term()}
  def create_topic(%__MODULE__{} = broker, name, keyspace_bits) do
    {metadata, reply} = apply_metadata(broker, {:create_topic, name, keyspace_bits})
    {%{broker | metadata: metadata}, reply}
  end

  @doc """
  Routes each record to the active range that owns its key and replicates the batch to that
  range's active segment via `replicate_fun`. Returns `{broker, {:ok, placements}}`, or
  `{broker, {:error, reason}}` (`:no_such_topic`; `{:unroutable, key}`; or a `replicate_fun`
  error). On a `replicate_fun` failure the returned broker reflects the groups that already
  committed (their data is durable); the failing group is not committed.
  """
  @spec produce(t(), Metadata.topic_name(), [Record.t()], replicate_fun()) ::
          {t(), {:ok, placements()} | {:error, term()}}
  def produce(%__MODULE__{} = broker, topic, records, replicate_fun) when is_list(records) do
    case Metadata.get_topic(broker.metadata, topic) do
      nil -> {broker, {:error, :no_such_topic}}
      topic_meta -> route_and_replicate(broker, topic, topic_meta, records, replicate_fun)
    end
  end

  @doc """
  Reads up to `max_records` records from `range_id` starting at `offset`, from the owning
  segment's primary via `read_fun`. Returns `:eof` past the range's end (or if nothing was
  produced to it).
  """
  @spec read(t(), Metadata.range_id(), non_neg_integer(), pos_integer(), read_fun()) ::
          {:ok, [Record.t()]} | :eof | {:error, term()}
  def read(%__MODULE__{} = broker, range_id, offset, max_records, read_fun) do
    case locate_segment(broker, range_id, offset) do
      :eof -> :eof
      {:ok, segment} -> read_fun.(primary(segment), segment.id, offset, max_records)
    end
  end

  @doc """
  Splits a range: the control plane seals the parent and creates two children; the parent's
  active segment is sealed. Returns `{broker, {:ok, left_id, right_id}}` or a `Metadata` error.
  """
  @spec split_range(t(), Metadata.range_id()) :: {t(), term()}
  def split_range(%__MODULE__{} = broker, range_id) do
    case apply_metadata(broker, {:split_range, range_id}) do
      {metadata, {:ok, _left, _right} = reply} ->
        {seal_active_segment(%{broker | metadata: metadata}, range_id), reply}

      {_metadata, {:error, _reason} = error} ->
        {broker, error}
    end
  end

  @doc """
  Merges two buddy ranges: the control plane seals both and creates a child; both parents'
  active segments are sealed. Returns `{broker, {:ok, child_id}}` or a `Metadata` error.
  """
  @spec merge_ranges(t(), Metadata.range_id(), Metadata.range_id()) :: {t(), term()}
  def merge_ranges(%__MODULE__{} = broker, range_id_a, range_id_b) do
    case apply_metadata(broker, {:merge_ranges, range_id_a, range_id_b}) do
      {metadata, {:ok, _child} = reply} ->
        broker = %{broker | metadata: metadata}
        {seal_active_segment(seal_active_segment(broker, range_id_a), range_id_b), reply}

      {_metadata, {:error, _reason} = error} ->
        {broker, error}
    end
  end

  @doc "The ids of a topic's active ranges (those that currently tile the keyspace)."
  @spec active_range_ids(t(), Metadata.topic_name()) :: [Metadata.range_id()]
  def active_range_ids(%__MODULE__{} = broker, topic) do
    broker.metadata |> Metadata.active_ranges_of_topic(topic) |> Enum.map(& &1.id)
  end

  @doc """
  Applies control-plane `:set_segment_replicas` `commands` (from `Malachi.Cluster.SelfHealing`
  healing sealed segments, or `Malachi.Cluster.Failover` promoting an active segment's primary).
  Each command updates the metadata; when it targets a range's **active** segment, the broker's
  active-segment cache is updated too, so the next produce routes to the new replica set/primary.
  """
  @spec apply_heal(t(), [Metadata.command()]) :: t()
  def apply_heal(%__MODULE__{} = broker, commands) do
    Enum.reduce(commands, broker, &apply_replica_command/2)
  end

  defp apply_replica_command({:set_segment_replicas, segment_id, replica_set} = command, broker) do
    {metadata, _reply} = apply_metadata(broker, command)
    broker = %{broker | metadata: metadata}
    update_active_replica_set(broker, segment_id, replica_set)
  end

  defp apply_replica_command(command, broker) do
    {metadata, _reply} = apply_metadata(broker, command)
    %{broker | metadata: metadata}
  end

  defp update_active_replica_set(broker, {range_id, _seq} = segment_id, replica_set) do
    case Map.get(broker.segments, range_id) do
      %{id: ^segment_id} = active ->
        %{broker | segments: Map.put(broker.segments, range_id, %{active | replica_set: replica_set})}

      _other ->
        broker
    end
  end

  @doc "The current metadata (the control-plane source of truth held by this broker)."
  @spec metadata(t()) :: Metadata.t()
  def metadata(%__MODULE__{} = broker), do: broker.metadata

  @doc """
  Durably records a consumer `group`'s committed position (`offsets`, per range) for `topic`,
  through the control plane (Raft-backed when configured). Returns `{broker, reply}`.
  """
  @spec commit_offset(t(), Metadata.group(), Metadata.topic_name(), Metadata.offsets()) :: {t(), term()}
  def commit_offset(%__MODULE__{} = broker, group, topic, offsets) do
    {metadata, reply} = apply_metadata(broker, {:commit_offset, group, topic, offsets})
    {%{broker | metadata: metadata}, reply}
  end

  @doc """
  Removes a sealed `segment_id` from the control plane (retention), through the control plane
  (Raft-backed when configured). Returns `{broker, reply}` (`:ok`, or a `Metadata` error such as
  `:segment_active`/`:no_such_segment`).
  """
  @spec delete_segment(t(), Metadata.segment_id()) :: {t(), term()}
  def delete_segment(%__MODULE__{} = broker, segment_id) do
    {metadata, reply} = apply_metadata(broker, {:delete_segment, segment_id})
    {%{broker | metadata: metadata}, reply}
  end

  @doc "A consumer group's committed offsets for `topic` (empty if it never committed)."
  @spec committed_offsets(t(), Metadata.group(), Metadata.topic_name()) :: Metadata.offsets()
  def committed_offsets(%__MODULE__{} = broker, group, topic) do
    Metadata.committed_offsets(broker.metadata, group, topic)
  end

  @doc """
  Replaces the broker set new segments are placed on (e.g. refreshed from live membership), so
  freshly opened segments land on currently-alive brokers. Must be non-empty.
  """
  @spec set_brokers(t(), [Metadata.broker()]) :: t()
  def set_brokers(%__MODULE__{} = broker, [_ | _] = brokers), do: %{broker | brokers: brokers}

  @doc """
  Replaces the per-broker attributes used for rack/DC-aware placement (refreshed from membership).
  New segments spread over `spread_by` using these; a broker absent here has no attributes.
  """
  @spec set_broker_attributes(t(), %{Metadata.broker() => map()}) :: t()
  def set_broker_attributes(%__MODULE__{} = broker, attributes) when is_map(attributes) do
    %{broker | broker_attributes: attributes}
  end

  @typedoc "Opaque cursor for `stream_history/5`: `:start`, an internal position, or `:done`."
  @type history_cursor :: :start | {non_neg_integer(), non_neg_integer()} | :done

  @typedoc """
  Opaque per-range consume position for `read_consume/5`: `:start`, or an internal
  `{source_index, source_offset}`. Unlike `history_cursor`, it has no `:done`: the active range is
  tailed, so consumption never terminates.
  """
  @type consume_cursor :: :start | {non_neg_integer(), non_neg_integer()}

  @doc """
  Streams one bounded page of a range's **cross-epoch** history: records its sealed ancestors
  hold for this range's keyspace slice (oldest first, in happens-before order), then the range's
  own records. The lineage comes from the control plane (`Metadata` `parents`); ancestor records
  are filtered to the range's slice via `Keyspace`, and segments are read through `read_fun`.

  Returns `{:ok, records, next_cursor}`; call again with `next_cursor` until it is `:done`. A page
  may be empty while `next_cursor` is not `:done`. `{:error, :no_such_range}` if the range is
  unknown.
  """
  @spec stream_history(t(), Metadata.range_id(), history_cursor(), pos_integer(), read_fun()) ::
          {:ok, [Record.t()], history_cursor()} | {:error, term()}
  def stream_history(broker, range_id, cursor, max_records, read_fun)

  def stream_history(%__MODULE__{}, _range_id, :done, _max_records, _read_fun), do: {:ok, [], :done}

  def stream_history(%__MODULE__{} = broker, range_id, cursor, max_records, read_fun)
      when is_integer(max_records) and max_records > 0 do
    case Metadata.get_range(broker.metadata, range_id) do
      nil ->
        {:error, :no_such_range}

      range ->
        sources = history_sources(range_id, range)
        {source_index, source_offset} = normalize_cursor(cursor)
        read_history_page(broker, sources, source_index, source_offset, max_records, read_fun)
    end
  end

  @doc """
  Convenience that pages `stream_history/5` to the end and returns every record as one ordered
  list. Loads the whole history into memory — for bounded/administrative use.
  """
  @spec read_history(t(), Metadata.range_id(), read_fun()) ::
          {:ok, [Record.t()]} | {:error, term()}
  def read_history(%__MODULE__{} = broker, range_id, read_fun) do
    drain_history(broker, range_id, :start, [], read_fun)
  end

  @doc """
  Reads up to `max_records` of `range_id`'s **cross-epoch** stream for live consumption: first the
  records its sealed ancestors hold for this range's keyspace slice (oldest first, in
  happens-before order), then the range's own records — and it **tails** the active range. Unlike
  `stream_history/5`, the self source never terminates: when the range is caught up it returns an
  empty page whose cursor stays on the self source, so records produced later are delivered on a
  later call. This is what lets a consumer drain a range's full history across splits/merges (the
  pre-split records live in the now-sealed parent's segments) without ever seeing partition/offset.

  Returns `{:ok, records, next_cursor}` (call again with `next_cursor`), or `{:error,
  :no_such_range}` if the range is unknown.
  """
  @spec read_consume(t(), Metadata.range_id(), consume_cursor(), pos_integer(), read_fun()) ::
          {:ok, [Record.t()], consume_cursor()} | {:error, term()}
  def read_consume(%__MODULE__{} = broker, range_id, cursor, max_records, read_fun)
      when is_integer(max_records) and max_records > 0 do
    case Metadata.get_range(broker.metadata, range_id) do
      nil ->
        {:error, :no_such_range}

      range ->
        sources = history_sources(range_id, range)
        {index, offset} = normalize_cursor(cursor)
        consume_page(broker, sources, index, offset, max_records, [], 0, read_fun)
    end
  end

  # --- routing & replication ---

  defp route_and_replicate(broker, topic, topic_meta, records, replicate_fun) do
    active_ranges = Metadata.active_ranges_of_topic(broker.metadata, topic)

    case group_by_owning_range(records, active_ranges, topic_meta.keyspace_size) do
      {:error, _reason} = error -> {broker, error}
      {:ok, grouped} -> replicate_groups(broker, grouped, replicate_fun)
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

  defp replicate_groups(broker, grouped, replicate_fun) do
    result =
      Enum.reduce_while(grouped, {broker, %{}}, fn {range_id, reversed_records}, {broker, placements} ->
        records = Enum.reverse(reversed_records)
        replicate_group(broker, range_id, records, placements, replicate_fun)
      end)

    case result do
      {:error, reason, broker} -> {broker, {:error, reason}}
      {broker, placements} -> {broker, {:ok, placements}}
    end
  end

  defp replicate_group(broker, range_id, records, placements, replicate_fun) do
    case ensure_segment(broker, range_id) do
      # Opening the segment (its register_segment command) failed — abort this group, keeping the
      # pre-open broker (no phantom segment); a retry re-places.
      {:error, reason} ->
        {:halt, {:error, reason, broker}}

      {:ok, opened, segment} ->
        replicate_to_segment(broker, opened, segment, range_id, records, placements, replicate_fun)
    end
  end

  defp replicate_to_segment(broker, opened, segment, range_id, records, placements, replicate_fun) do
    first = next_offset(opened, range_id)
    count = length(records)
    last = first + count - 1

    case replicate_fun.(primary(segment), segment.id, segment.replica_set, segment.start_offset, records) do
      {:ok, ^last} ->
        committed = commit_batch(opened, range_id, count, batch_bytes(records))
        {:cont, {committed, Map.put(placements, range_id, {first, last})}}

      # On failure, discard the just-opened segment by returning the pre-open broker (immutable
      # value = free rollback), so a failed produce leaves no phantom segment and a retry re-places.
      {:ok, other} ->
        {:halt, {:error, {:offset_mismatch, expected: last, got: other}, broker}}

      {:error, reason} ->
        {:halt, {:error, reason, broker}}
    end
  end

  # --- segment lifecycle (logical spans over per-segment replicated storage) ---

  defp ensure_segment(broker, range_id) do
    case Map.fetch(broker.segments, range_id) do
      {:ok, segment} ->
        {:ok, broker, segment}

      :error ->
        case open_segment(broker, range_id, next_offset(broker, range_id)) do
          {:ok, broker} -> {:ok, broker, Map.fetch!(broker.segments, range_id)}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  # Registers a new segment for `range_id` starting at `start_offset`, choosing its replica set
  # via the placement policy. The id `{range_id, seq}` is globally unique (range ids are) and the
  # per-range `seq` counter persists across seals, so a sealed segment's id is never reused.
  defp open_segment(broker, range_id, start_offset) do
    seq = Map.get(broker.segment_seq, range_id, 0)
    segment_id = {range_id, seq}

    {:ok, replica_set} =
      Placement.place(segment_id, broker.brokers, broker.replication_factor, place_opts(broker, range_id))

    # The register command can fail when the metadata is Raft-backed (e.g. an ra timeout); surface
    # it so the produce aborts cleanly instead of crashing. The cache/seq are advanced only on :ok.
    case apply_metadata(broker, {:register_segment, range_id, segment_id, replica_set, start_offset}) do
      {metadata, :ok} ->
        active = %{id: segment_id, start_offset: start_offset, records: 0, bytes: 0, replica_set: replica_set}

        broker = %{
          broker
          | metadata: metadata,
            segments: Map.put(broker.segments, range_id, active),
            segment_seq: Map.put(broker.segment_seq, range_id, seq + 1)
        }

        {:ok, broker}

      {_metadata, {:error, reason}} ->
        {:error, reason}

      {_metadata, other} ->
        {:error, {:unexpected_register_reply, other}}
    end
  end

  # Advances the range's offset and the active segment's tallies, sealing the segment (soft
  # threshold checked at the batch boundary, so it may overshoot by at most one batch) once it
  # reaches the byte limit. The next produce to this range opens a fresh segment.
  defp commit_batch(broker, range_id, count, bytes) do
    active = Map.fetch!(broker.segments, range_id)
    active = %{active | records: active.records + count, bytes: active.bytes + bytes}

    broker = %{
      broker
      | offsets: Map.put(broker.offsets, range_id, next_offset(broker, range_id) + count),
        segments: Map.put(broker.segments, range_id, active)
    }

    if active.bytes >= broker.segment_max_bytes do
      seal_active_segment(broker, range_id)
    else
      broker
    end
  end

  # Seals the range's active segment (recording its record count as the length) and forgets it,
  # so the next produce opens a new one. No-op if the range has no open segment.
  defp seal_active_segment(broker, range_id) do
    case Map.fetch(broker.segments, range_id) do
      :error ->
        broker

      {:ok, active} ->
        # sealed_at is generated here (like Record timestamps) and carried in the command, so every
        # replica applies the same value deterministically. Retention uses byte_size + sealed_at.
        sealed_at = System.system_time(:millisecond)
        command = {:seal_segment, active.id, active.records, active.bytes, sealed_at}
        {metadata, _reply} = apply_metadata(broker, command)
        %{broker | metadata: metadata, segments: Map.delete(broker.segments, range_id)}
    end
  end

  # Applies a metadata mutation via the configured command function (in-memory by default, or
  # Raft-backed), threading the cache so multiple mutations in one operation see each other.
  defp apply_metadata(broker, command), do: broker.command_fun.(broker.metadata, command)

  # Placement options for a new segment of `range_id`: spread it over the effective attribute using
  # the current broker attributes, else none (plain rendezvous ranking).
  defp place_opts(broker, range_id) do
    case effective_spread_by(broker, range_id) do
      nil -> []
      key -> [spread: {key, broker.broker_attributes}]
    end
  end

  # The spread attribute for `range_id`: its topic policy's `spread_by` when the policy sets that key
  # (an explicit nil opts the topic out of spreading), overriding the global; otherwise the broker's
  # global `spread_by`. Mirrors the per-topic retention resolution (a set key wins, nil included).
  defp effective_spread_by(broker, range_id) do
    case Metadata.topic_policy(broker.metadata, elem(range_id, 0)) do
      %{spread_by: spread_by} -> spread_by
      _no_policy_spread_by -> broker.spread_by
    end
  end

  defp next_offset(broker, range_id), do: Map.get(broker.offsets, range_id, 0)

  # The earliest offset still stored for a range: the smallest segment start_offset (0 if none).
  # Retention deletes the oldest segments — a contiguous prefix — so a consumer positioned below this
  # has had its data expired; read callers clamp up to it to skip transparently to what still exists.
  defp earliest_offset(broker, range_id) do
    broker.metadata
    |> Metadata.segments_of_range(range_id)
    |> Enum.map(& &1.start_offset)
    |> Enum.min(fn -> 0 end)
  end

  defp primary(%{replica_set: [primary | _]}), do: primary

  defp batch_bytes(records), do: Enum.reduce(records, 0, fn record, acc -> acc + Record.encoded_size(record) end)

  # The segment of `range_id` containing `offset` (range-relative), or `:eof` past the range's
  # end. Segments partition the offset space contiguously, so the owning segment is the last one
  # whose `start_offset` is at or below `offset`.
  defp locate_segment(broker, range_id, offset) do
    if offset < 0 or offset >= next_offset(broker, range_id) do
      :eof
    else
      segment =
        broker.metadata
        |> Metadata.segments_of_range(range_id)
        |> Enum.sort_by(& &1.start_offset, :desc)
        |> Enum.find(&(&1.start_offset <= offset))

      if segment, do: {:ok, segment}, else: :eof
    end
  end

  # --- cross-epoch history ---

  # The ordered sources for a range's history: each sealed ancestor (its records filtered to the
  # target range's slice), then the range itself (no filter).
  defp history_sources(range_id, range) do
    ancestors = Enum.map(range.parents, fn ancestor_id -> {ancestor_id, range} end)
    ancestors ++ [{range_id, nil}]
  end

  defp normalize_cursor(:start), do: {0, 0}
  defp normalize_cursor({source_index, source_offset}), do: {source_index, source_offset}

  defp read_history_page(broker, sources, source_index, source_offset, max_records, read_fun) do
    case Enum.at(sources, source_index) do
      nil ->
        {:ok, [], :done}

      {source_range_id, filter_range} ->
        source_offset = max(source_offset, earliest_offset(broker, source_range_id))

        case read(broker, source_range_id, source_offset, max_records, read_fun) do
          :eof ->
            read_history_page(broker, sources, source_index + 1, 0, max_records, read_fun)

          {:ok, records} ->
            filtered = filter_records(records, filter_range)
            {:ok, filtered, {source_index, source_offset + length(records)}}

          {:error, _reason} = error ->
            error
        end
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

  # Like `read_history_page`, but accumulates across sources up to `max_records` and tails the self
  # source (the last one): when self yields :eof it pauses there (no `:done`), so a later produce is
  # picked up on the next call. Ancestors (filtered to the target slice) are drained then skipped.
  # A cursor's source_index past the end (e.g. a forged/stale client cursor) has nothing to read:
  # pause here rather than crash, mirroring how an out-of-range offset yields :eof gracefully.
  defp consume_page(_broker, sources, index, offset, _max_records, acc, _count, _read_fun)
       when index >= length(sources) do
    {:ok, Enum.reverse(acc), {index, offset}}
  end

  defp consume_page(broker, sources, index, offset, max_records, acc, count, read_fun) do
    last_index = length(sources) - 1
    {source_range_id, filter_range} = Enum.at(sources, index)
    # Skip data retention expired: never read below the range's earliest available offset, so a
    # consumer whose position was deleted advances to the earliest data still stored (at-least-once).
    offset = max(offset, earliest_offset(broker, source_range_id))

    case read(broker, source_range_id, offset, max_records, read_fun) do
      {:ok, [_ | _] = records} ->
        kept = filter_records(records, filter_range)
        acc = Enum.reverse(kept) ++ acc
        count = count + length(kept)
        next_offset = offset + length(records)

        if count >= max_records do
          {:ok, Enum.reverse(acc), {index, next_offset}}
        else
          consume_page(broker, sources, index, next_offset, max_records, acc, count, read_fun)
        end

      {:error, _reason} = error ->
        error

      # :eof, or a defensive empty page: this source is drained. Advance to the next ancestor, or
      # pause on the self source so records produced later tail in on a subsequent call.
      _eof_or_empty ->
        if index < last_index do
          consume_page(broker, sources, index + 1, 0, max_records, acc, count, read_fun)
        else
          {:ok, Enum.reverse(acc), {index, offset}}
        end
    end
  end

  defp drain_history(broker, range_id, cursor, pages, read_fun) do
    case stream_history(broker, range_id, cursor, 1000, read_fun) do
      {:ok, records, :done} -> {:ok, [records | pages] |> Enum.reverse() |> List.flatten()}
      {:ok, records, next_cursor} -> drain_history(broker, range_id, next_cursor, [records | pages], read_fun)
      {:error, _reason} = error -> error
    end
  end
end
