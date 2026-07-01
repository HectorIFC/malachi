defmodule Malachi.Metadata do
  @moduledoc """
  The deterministic state machine behind a NorthGuard vnode/coordinator: the durable,
  replicated source of truth for **metadata** about topics, ranges and segments.

  Unlike the data-plane storage (`Malachi.Broker`/`Log`, which hold open file handles),
  this holds only metadata — it is pure data and a pure transition function. All
  mutations go through `apply/2` (`command -> {new_state, reply}`), exactly the contract a
  Raft machine's `apply` needs, so the `ra` integration in Phase 1b replicates this state
  without changes. This is what makes topic structure durable (the item deferred from
  Phase 0).

  ## Determinism

  `apply/2` must be deterministic so every replica reaches the same state from the same
  command log: no wall-clock time and no random/process-unique values are generated inside
  it. New range ids come from a counter in the state (`next_range_id`); anything else
  non-deterministic (timestamps, broker-chosen segment ids) is supplied *in the command* by
  the proposer. Bad input returns an error tuple rather than raising (a raise would crash a
  replica).

  Reads are plain functions over the state (`get_topic/2`, `ranges_of_topic/2`, …), not
  commands.
  """

  # We define apply/2 (the Raft-style transition function), which shadows Kernel.apply/2.
  import Kernel, except: [apply: 2]

  alias Malachi.Keyspace

  @type topic_name :: String.t()
  # Range ids are {topic, seq}: globally unique (topic names are cluster-unique), so a range
  # can migrate between vnodes (vnode split) without its id colliding with another topic's.
  @type range_id :: {topic_name(), non_neg_integer()}
  @type segment_id :: term()
  @type broker :: term()

  @type topic_meta :: %{
          name: topic_name(),
          keyspace_size: pos_integer(),
          state: :active | :sealed,
          next_range_seq: non_neg_integer()
        }

  @type range_meta :: %{
          id: range_id(),
          topic: topic_name(),
          key_start: non_neg_integer(),
          key_end: non_neg_integer(),
          keyspace_size: pos_integer(),
          state: :active | :sealed,
          parents: [range_id()]
        }

  @type segment_meta :: %{
          id: segment_id(),
          range_id: range_id(),
          replica_set: [broker()],
          state: :active | :sealed,
          start_offset: non_neg_integer(),
          length: non_neg_integer() | nil,
          # Epoch ms the segment was sealed (`nil` while active). Set from the seal command, so it is
          # deterministic across replicas; used by retention to expire sealed segments by age.
          sealed_at: non_neg_integer() | nil
        }

  @typedoc "A consumer group's name."
  @type group :: String.t()

  @typedoc """
  A consumer's per-range stream position. Opaque to the metadata (it only stores/returns it); set
  by the log layer as a `Malachi.Broker.consume_cursor` — `:start` or `{source_index, source_offset}`.
  """
  @type position :: :start | {non_neg_integer(), non_neg_integer()}

  @typedoc "A consumer's position across the ranges of a topic."
  @type offsets :: %{range_id() => position()}

  @typedoc "Identifies a consumer group's position for a topic."
  @type group_topic :: {group(), topic_name()}

  @type t :: %__MODULE__{
          topics: %{topic_name() => topic_meta()},
          ranges: %{range_id() => range_meta()},
          segments: %{segment_id() => segment_meta()},
          committed_offsets: %{group_topic() => offsets()}
        }

  @typedoc "A topic's complete metadata, extracted for migration between vnodes."
  @type topic_export :: %{
          topic: topic_meta(),
          ranges: %{range_id() => range_meta()},
          segments: %{segment_id() => segment_meta()}
        }

  @type command ::
          {:create_topic, topic_name(), pos_integer()}
          | {:seal_topic, topic_name()}
          | {:delete_topic, topic_name()}
          | {:split_range, range_id()}
          | {:merge_ranges, range_id(), range_id()}
          | {:register_segment, range_id(), segment_id(), [broker()], non_neg_integer()}
          | {:seal_segment, segment_id(), non_neg_integer(), non_neg_integer()}
          | {:delete_segment, segment_id()}
          | {:set_segment_replicas, segment_id(), [broker()]}
          | {:commit_offset, group(), topic_name(), offsets()}

  defstruct topics: %{}, ranges: %{}, segments: %{}, committed_offsets: %{}

  @doc "An empty metadata state."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  Applies a command, returning `{new_state, reply}`. Deterministic: the same command on the
  same state always yields the same result on every replica. On failure the state is
  returned unchanged with an `{:error, reason}` reply.

  `register_segment` requires the `segment_id` to be **globally unique** across the cluster
  (the broker-assigned contract). Within a vnode this is checked (`:segment_exists`), but
  uniqueness across vnodes is the caller's responsibility — it is what keeps a topic's
  segments safe when it migrates to another vnode (see `insert_topic/2`).
  """
  @spec apply(t(), command()) :: {t(), term()}
  def apply(%__MODULE__{} = state, {:create_topic, name, keyspace_bits}) do
    cond do
      not valid_topic_name?(name) -> {state, {:error, :invalid_topic_name}}
      Map.has_key?(state.topics, name) -> {state, {:error, :already_exists}}
      true -> create_topic_with_bits(state, name, keyspace_bits)
    end
  end

  def apply(%__MODULE__{} = state, {:seal_topic, name}) do
    case Map.fetch(state.topics, name) do
      :error ->
        {state, {:error, :no_such_topic}}

      {:ok, topic} ->
        topics = Map.put(state.topics, name, %{topic | state: :sealed})
        ranges = seal_ranges_of_topic(state.ranges, name)
        {%{state | topics: topics, ranges: ranges}, :ok}
    end
  end

  def apply(%__MODULE__{} = state, {:delete_topic, name}) do
    case Map.fetch(state.topics, name) do
      :error ->
        {state, {:error, :no_such_topic}}

      {:ok, _topic} ->
        doomed_range_ids = range_ids_of_topic(state.ranges, name)

        ranges = Map.drop(state.ranges, doomed_range_ids)
        segments = drop_segments_in(state.segments, doomed_range_ids)
        topics = Map.delete(state.topics, name)
        {%{state | topics: topics, ranges: ranges, segments: segments}, :ok}
    end
  end

  def apply(%__MODULE__{} = state, {:split_range, range_id}) do
    case fetch_active_range(state, range_id) do
      {:error, _reason} = error ->
        {state, error}

      {:ok, range} ->
        if Keyspace.splittable?(range.key_start, range.key_end) do
          do_split_range(state, range)
        else
          {state, {:error, :cannot_split}}
        end
    end
  end

  def apply(%__MODULE__{} = state, {:merge_ranges, range_id_a, range_id_b}) do
    with {:ok, range_a} <- fetch_active_range(state, range_id_a),
         {:ok, range_b} <- fetch_active_range(state, range_id_b),
         :ok <- check_mergeable(range_a, range_b) do
      do_merge_ranges(state, range_a, range_b)
    else
      {:error, _reason} = error -> {state, error}
    end
  end

  def apply(%__MODULE__{} = state, {:register_segment, range_id, segment_id, replica_set, start_offset}) do
    if Map.has_key?(state.segments, segment_id) do
      {state, {:error, :segment_exists}}
    else
      case fetch_active_range(state, range_id) do
        {:error, _reason} = error ->
          {state, error}

        {:ok, _range} ->
          segment = %{
            id: segment_id,
            range_id: range_id,
            replica_set: replica_set,
            state: :active,
            start_offset: start_offset,
            length: nil,
            sealed_at: nil
          }

          {%{state | segments: Map.put(state.segments, segment_id, segment)}, :ok}
      end
    end
  end

  def apply(%__MODULE__{} = state, {:seal_segment, segment_id, length, sealed_at}) do
    update_segment(state, segment_id, fn segment ->
      %{segment | state: :sealed, length: length, sealed_at: sealed_at}
    end)
  end

  def apply(%__MODULE__{} = state, {:delete_segment, segment_id}) do
    # Retention removes an expired segment from the control plane. Only sealed segments are
    # deletable — the active segment is still being written and must never be dropped.
    case Map.fetch(state.segments, segment_id) do
      :error -> {state, {:error, :no_such_segment}}
      {:ok, %{state: :active}} -> {state, {:error, :segment_active}}
      {:ok, _sealed} -> {%{state | segments: Map.delete(state.segments, segment_id)}, :ok}
    end
  end

  def apply(%__MODULE__{} = state, {:set_segment_replicas, segment_id, replica_set}) do
    update_segment(state, segment_id, fn segment -> %{segment | replica_set: replica_set} end)
  end

  def apply(%__MODULE__{} = state, {:commit_offset, group, topic, offsets}) do
    # A consumer group's durable position. Last commit wins (the client owns its position); no
    # topic-existence check, so an offset can be committed before/independently of routing.
    committed = Map.put(state.committed_offsets, {group, topic}, offsets)
    {%{state | committed_offsets: committed}, :ok}
  end

  # Defensive catch-all: an unknown command must NOT crash the machine. Once this RSM is
  # replicated by Raft, a command that raises in `apply` would crash every replica
  # deterministically (and again on replay) — e.g. an older replica seeing a newer
  # command during a rolling upgrade. Keep the replica alive and surface the problem.
  def apply(%__MODULE__{} = state, _unknown_command), do: {state, {:error, :unknown_command}}

  # --- queries ---

  @doc "The topic metadata, or `nil`."
  @spec get_topic(t(), topic_name()) :: topic_meta() | nil
  def get_topic(%__MODULE__{} = state, name), do: Map.get(state.topics, name)

  @doc "The range metadata, or `nil`."
  @spec get_range(t(), range_id()) :: range_meta() | nil
  def get_range(%__MODULE__{} = state, range_id), do: Map.get(state.ranges, range_id)

  @doc "The segment metadata, or `nil`."
  @spec get_segment(t(), segment_id()) :: segment_meta() | nil
  def get_segment(%__MODULE__{} = state, segment_id), do: Map.get(state.segments, segment_id)

  @doc "All ranges of a topic (any state)."
  @spec ranges_of_topic(t(), topic_name()) :: [range_meta()]
  def ranges_of_topic(%__MODULE__{} = state, name) do
    state.ranges |> Map.values() |> Enum.filter(&(&1.topic == name))
  end

  @doc "The active ranges of a topic (the ones that currently tile the keyspace)."
  @spec active_ranges_of_topic(t(), topic_name()) :: [range_meta()]
  def active_ranges_of_topic(%__MODULE__{} = state, name) do
    state |> ranges_of_topic(name) |> Enum.filter(&(&1.state == :active))
  end

  @doc "All segments of a range."
  @spec segments_of_range(t(), range_id()) :: [segment_meta()]
  def segments_of_range(%__MODULE__{} = state, range_id) do
    state.segments |> Map.values() |> Enum.filter(&(&1.range_id == range_id))
  end

  @doc "A consumer group's committed offsets for a topic, or `%{}` if it has never committed."
  @spec committed_offsets(t(), group(), topic_name()) :: offsets()
  def committed_offsets(%__MODULE__{} = state, group, topic) do
    Map.get(state.committed_offsets, {group, topic}, %{})
  end

  # --- migration (vnode split) ---

  @doc """
  Removes a topic and all its ranges/segments from `state`, returning
  `{state_without_topic, export}` (or `{state, nil}` if the topic is absent). The export
  can be re-inserted on another vnode with `insert_topic/2` — this is how a vnode split
  migrates a topic's metadata to a new vnode.
  """
  @spec extract_topic(t(), topic_name()) :: {t(), topic_export() | nil}
  def extract_topic(%__MODULE__{} = state, name) do
    case Map.fetch(state.topics, name) do
      :error ->
        {state, nil}

      {:ok, topic} ->
        range_ids = for {id, range} <- state.ranges, range.topic == name, into: MapSet.new(), do: id
        ranges = Map.take(state.ranges, MapSet.to_list(range_ids))
        segments = Map.filter(state.segments, fn {_id, seg} -> MapSet.member?(range_ids, seg.range_id) end)

        remaining = %{
          state
          | topics: Map.delete(state.topics, name),
            ranges: Map.drop(state.ranges, MapSet.to_list(range_ids)),
            segments: Map.drop(state.segments, Map.keys(segments))
        }

        {remaining, %{topic: topic, ranges: ranges, segments: segments}}
    end
  end

  @doc """
  Inserts a topic `export` (from `extract_topic/2`) into `state`. Range ids are globally
  unique (`{topic, seq}`), so merged ranges never collide with another topic's.

  Segment ids, however, are caller-supplied and independent of range ids: this merges them
  by id, so a segment id that already exists in `state` is **overwritten**. Migration is
  therefore safe only if segment ids are globally unique across the cluster (the
  broker-assigned contract — see `register_segment` in `apply/2`).
  """
  @spec insert_topic(t(), topic_export()) :: t()
  def insert_topic(%__MODULE__{} = state, export) do
    %{
      state
      | topics: Map.put(state.topics, export.topic.name, export.topic),
        ranges: Map.merge(state.ranges, export.ranges),
        segments: Map.merge(state.segments, export.segments)
    }
  end

  # --- internals: topic ---

  # Topic names are used in file paths (range log directories), so restrict them to a safe
  # allowlist and reject "."/".." — preventing path traversal at the data plane.
  defp valid_topic_name?(name) do
    is_binary(name) and name not in ["", ".", ".."] and name =~ ~r/\A[A-Za-z0-9._-]+\z/
  end

  defp create_topic_with_bits(state, name, keyspace_bits) do
    case Keyspace.size_for_bits(keyspace_bits) do
      {:error, :out_of_range} -> {state, {:error, :invalid_keyspace_bits}}
      {:ok, keyspace_size} -> create_topic(state, name, keyspace_size)
    end
  end

  defp create_topic(state, name, keyspace_size) do
    root_id = {name, 0}
    # next_range_seq is the per-topic counter; the root takes seq 0, so the next is 1.
    topic = %{name: name, keyspace_size: keyspace_size, state: :active, next_range_seq: 1}

    root_range = %{
      id: root_id,
      topic: name,
      key_start: 0,
      key_end: keyspace_size,
      keyspace_size: keyspace_size,
      state: :active,
      parents: []
    }

    state = %{
      state
      | topics: Map.put(state.topics, name, topic),
        ranges: Map.put(state.ranges, root_id, root_range)
    }

    {state, {:ok, root_id}}
  end

  defp seal_ranges_of_topic(ranges, name) do
    Map.new(ranges, fn {id, range} ->
      if range.topic == name, do: {id, %{range | state: :sealed}}, else: {id, range}
    end)
  end

  defp range_ids_of_topic(ranges, name) do
    for {id, range} <- ranges, range.topic == name, do: id
  end

  defp drop_segments_in(segments, range_ids) do
    doomed = MapSet.new(range_ids)
    Map.filter(segments, fn {_id, segment} -> not MapSet.member?(doomed, segment.range_id) end)
  end

  # --- internals: range split/merge ---

  defp do_split_range(state, range) do
    {state, [left_id, right_id]} = allocate_range_ids(state, range.topic, 2)
    midpoint = Keyspace.split_point(range.key_start, range.key_end)
    parents = range.parents ++ [range.id]

    left = child_range(left_id, range, range.key_start, midpoint, parents)
    right = child_range(right_id, range, midpoint, range.key_end, parents)

    ranges =
      state.ranges
      |> Map.put(range.id, %{range | state: :sealed})
      |> Map.put(left_id, left)
      |> Map.put(right_id, right)

    {%{state | ranges: ranges}, {:ok, left_id, right_id}}
  end

  defp do_merge_ranges(state, range_a, range_b) do
    {state, [child_id]} = allocate_range_ids(state, range_a.topic, 1)
    union_start = min(range_a.key_start, range_b.key_start)
    union_end = max(range_a.key_end, range_b.key_end)
    parents = Enum.uniq(range_a.parents ++ range_b.parents ++ [range_a.id, range_b.id])

    child = child_range(child_id, range_a, union_start, union_end, parents)

    ranges =
      state.ranges
      |> Map.put(range_a.id, %{range_a | state: :sealed})
      |> Map.put(range_b.id, %{range_b | state: :sealed})
      |> Map.put(child_id, child)

    {%{state | ranges: ranges}, {:ok, child_id}}
  end

  # Allocates `count` fresh range ids `{topic, seq}` from the topic's per-topic counter,
  # returning the bumped state and the ids in order.
  defp allocate_range_ids(state, topic_name, count) do
    topic = Map.fetch!(state.topics, topic_name)
    first_seq = topic.next_range_seq
    ids = for seq <- first_seq..(first_seq + count - 1)//1, do: {topic_name, seq}
    topics = Map.put(state.topics, topic_name, %{topic | next_range_seq: first_seq + count})
    {%{state | topics: topics}, ids}
  end

  defp child_range(id, parent_range, key_start, key_end, parents) do
    %{
      id: id,
      topic: parent_range.topic,
      key_start: key_start,
      key_end: key_end,
      keyspace_size: parent_range.keyspace_size,
      state: :active,
      parents: parents
    }
  end

  defp check_mergeable(range_a, range_b) do
    cond do
      range_a.topic != range_b.topic -> {:error, :different_topic}
      not buddies?(range_a, range_b) -> {:error, :not_buddies}
      true -> :ok
    end
  end

  defp buddies?(range_a, range_b) do
    range_a.keyspace_size == range_b.keyspace_size and
      Keyspace.buddies?(range_a.key_start, range_a.key_end, range_b.key_start, range_b.key_end)
  end

  # --- internals: lookups / segment update ---

  defp fetch_active_range(state, range_id) do
    case Map.fetch(state.ranges, range_id) do
      :error -> {:error, :no_such_range}
      {:ok, %{state: :sealed}} -> {:error, :sealed}
      {:ok, range} -> {:ok, range}
    end
  end

  defp update_segment(state, segment_id, fun) do
    case Map.fetch(state.segments, segment_id) do
      :error -> {state, {:error, :no_such_segment}}
      {:ok, segment} -> {%{state | segments: Map.put(state.segments, segment_id, fun.(segment))}, :ok}
    end
  end
end
