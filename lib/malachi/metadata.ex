defmodule Malachi.Metadata do
  @moduledoc """
  The deterministic state machine behind a NorthGuard vnode/coordinator: the durable,
  replicated source of truth for **metadata** about topics, ranges and segments.

  Unlike the data-plane storage (`Malachi.Broker`/`Log`, which hold open file handles),
  this holds only metadata: it is pure data and a pure transition function. All
  mutations go through `apply/2` (`command -> {new_state, reply}`), exactly the contract a
  Raft machine's `apply` needs, so the `ra` integration replicates this state without
  changing it. This is what makes topic structure durable.

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
          next_range_seq: non_neg_integer(),
          # The name of the storage policy governing this topic's retention/placement (nil = globals).
          policy: policy_name() | nil
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
          # Encoded byte size of the segment (`nil` while active). Set from the seal command, so it is
          # deterministic across replicas; used by retention to expire by total size per range.
          byte_size: non_neg_integer() | nil,
          # Epoch ms the segment was sealed (`nil` while active). Set from the seal command, so it is
          # deterministic across replicas; used by retention to expire sealed segments by age.
          sealed_at: non_neg_integer() | nil
        }

  @typedoc "A consumer group's name."
  @type group :: String.t()

  @typedoc """
  A consumer's per-range stream position. Opaque to the metadata (it only stores/returns it); set
  by the log layer as a `Malachi.Broker.consume_cursor`, `:start` or `{source_index, source_offset}`.
  """
  @type position :: :start | {non_neg_integer(), non_neg_integer()}

  @typedoc "A consumer's position across the ranges of a topic."
  @type offsets :: %{range_id() => position()}

  @typedoc "Identifies a consumer group's position for a topic."
  @type group_topic :: {group(), topic_name()}

  @type policy_name :: String.t()

  @typedoc """
  A named storage policy: per-topic retention overrides and a placement spread attribute. Both keys
  are optional; a policy applies only the ones it sets, falling back to the global defaults otherwise.
  """
  @type policy :: %{
          optional(:retention) => %{
            optional(:max_age_ms) => non_neg_integer() | nil,
            optional(:max_bytes) => non_neg_integer() | nil
          },
          optional(:spread_by) => term() | nil
        }

  @type t :: %__MODULE__{
          topics: %{topic_name() => topic_meta()},
          ranges: %{range_id() => range_meta()},
          segments: %{segment_id() => segment_meta()},
          committed_offsets: %{group_topic() => offsets()},
          policies: %{policy_name() => policy()},
          # Secondary (reverse) indexes kept in lockstep with `ranges`/`segments` so
          # `ranges_of_topic`/`segments_of_range` can be O(1)+O(k) lookups instead of full scans. Stored
          # as MapSets (O(1) add/remove, dedup); an entry exists iff the key has ≥1 member.
          topic_ranges: %{topic_name() => MapSet.t(range_id())},
          range_segments: %{range_id() => MapSet.t(segment_id())},
          # Topics currently **fenced for migration** (a vnode split copying them to another vnode), a set
          # kept as a `%{name => true}` map: every mutating command targeting one is rejected with
          # `{:error, :migrating}`, so the split's snapshot is not raced. Cleared on extract/`:end_migration`.
          migrating: %{topic_name() => true}
        }

  @typedoc "A topic's complete metadata, extracted for migration between vnodes."
  @type topic_export :: %{
          topic: topic_meta(),
          ranges: %{range_id() => range_meta()},
          segments: %{segment_id() => segment_meta()},
          offsets: %{group() => offsets()}
        }

  @type command ::
          {:create_topic, topic_name(), pos_integer()}
          | {:seal_topic, topic_name()}
          | {:delete_topic, topic_name()}
          | {:split_range, range_id()}
          | {:merge_ranges, range_id(), range_id()}
          | {:register_segment, range_id(), segment_id(), [broker()], non_neg_integer()}
          | {:seal_segment, segment_id(), non_neg_integer(), non_neg_integer(), non_neg_integer()}
          | {:delete_segment, segment_id()}
          | {:set_segment_replicas, segment_id(), [broker()]}
          | {:commit_offset, group(), topic_name(), offsets()}
          | {:define_policy, policy_name(), policy()}
          | {:set_topic_policy, topic_name(), policy_name() | nil}
          | {:extract_topic, topic_name()}
          | {:insert_topic, topic_export()}
          | {:begin_migration, topic_name()}
          | {:end_migration, topic_name()}

  defstruct topics: %{},
            ranges: %{},
            segments: %{},
            committed_offsets: %{},
            policies: %{},
            topic_ranges: %{},
            range_segments: %{},
            migrating: %{}

  # Shared empty MapSet returned for an absent index key (no allocation on the miss path).
  @empty_set MapSet.new()

  @doc "An empty metadata state."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  The topic a command belongs to, for **routing** it to the owning vnode, or `nil` if it names none.

  Structural, no state: a topic command names its topic; a `range_id` is `{topic, seq}` and a
  `segment_id` is `{range_id, seq}`, so the topic is `elem/2` away. The broker uses this to dispatch.
  """
  @spec command_target_topic(command()) :: topic_name() | nil
  def command_target_topic({:create_topic, name, _bits}), do: name
  def command_target_topic({:seal_topic, name}), do: name
  def command_target_topic({:delete_topic, name}), do: name
  def command_target_topic({:set_topic_policy, name, _policy}), do: name
  def command_target_topic({:commit_offset, _group, topic, _offsets}), do: topic
  def command_target_topic(command), do: List.first(routed_range_topics(command))

  @doc """
  The topics a range/segment command's ids belong to (structurally), or `[]` for any other command.

  This is the narrower half of `command_target_topic/1`, and the one the routing guard uses: only a
  command carrying a `range_id`/`segment_id` can be pointed at a co-located topic's range, so only these
  can mismatch the topic they were routed by (`:range_topic_mismatch`, in `Malachi.Cluster.DSRSM`). A
  command that names its topic directly (create/seal/delete/commit) targets exactly that topic and is not
  guarded. `merge_ranges` carries **two** ids, so it yields two topics and both must match where it was
  routed. An id of an unrecognized shape drops out (no topic to check) rather than raising.

  Distinct from the private `command_topic/2`, which resolves a range's **stored** topic from the state
  for the migration fence: that one needs the range to exist; this reads the id itself, catching a
  mismatch before any lookup.
  """
  @spec routed_range_topics(command()) :: [topic_name()]
  def routed_range_topics({:split_range, range_id}), do: topics([range_id_topic(range_id)])
  def routed_range_topics({:merge_ranges, a, b}), do: topics([range_id_topic(a), range_id_topic(b)])
  def routed_range_topics({:register_segment, range_id, _seg, _replicas, _off}), do: topics([range_id_topic(range_id)])
  def routed_range_topics({:seal_segment, segment_id, _len, _bytes, _at}), do: topics([segment_id_topic(segment_id)])
  def routed_range_topics({:delete_segment, segment_id}), do: topics([segment_id_topic(segment_id)])
  def routed_range_topics({:set_segment_replicas, segment_id, _replicas}), do: topics([segment_id_topic(segment_id)])
  def routed_range_topics(_not_range_scoped), do: []

  defp topics(list), do: Enum.reject(list, &is_nil/1)

  @doc """
  Whether a command carries a range/segment id belonging to a topic other than `topic_name`, the routing
  layer's mismatch check. True when any recognized id's topic differs; false for a command that names its
  topic directly or whose ids all match. Shared by `Malachi.Cluster.DSRSM` and its replicated counterpart.
  """
  @spec routed_to_foreign_topic?(command(), topic_name()) :: boolean()
  def routed_to_foreign_topic?(command, topic_name) do
    Enum.any?(routed_range_topics(command), &(&1 != topic_name))
  end

  # A range id is `{topic, seq}` and a segment id is `{range_id, seq}`. Match those shapes rather than
  # `elem/2` so an id of any other shape yields `nil` (no topic to check) instead of raising: the guard
  # runs on every command, and it must never crash on an id it does not recognize.
  defp range_id_topic({topic, _seq}), do: topic
  defp range_id_topic(_), do: nil
  defp segment_id_topic({{topic, _range_seq}, _seg_seq}), do: topic
  defp segment_id_topic(_), do: nil

  @doc """
  Applies a command, returning `{new_state, reply}`. Deterministic: the same command on the
  same state always yields the same result on every replica. On failure the state is
  returned unchanged with an `{:error, reason}` reply.

  `register_segment` requires the `segment_id` to be **globally unique** across the cluster
  (the broker-assigned contract). Within a vnode this is checked (`:segment_exists`), but
  uniqueness across vnodes is the caller's responsibility: it is what keeps a topic's
  segments safe when it migrates to another vnode (see `insert_topic/2`).
  """
  @spec apply(t(), command()) :: {t(), term()}
  def apply(%__MODULE__{} = state, command) do
    # migration fence (seal-first, à la NorthGuard's range split): a mutating command targeting a topic
    # that is being migrated to another vnode is rejected, so the split's snapshot is never raced.
    case command_topic(state, command) do
      topic when is_binary(topic) ->
        if Map.has_key?(state.migrating, topic), do: {state, {:error, :migrating}}, else: do_apply(state, command)

      nil ->
        do_apply(state, command)
    end
  end

  # The topic a mutating command would change, or `nil` when the command is not topic-scoped or is a
  # migration/read command that must never be fenced (create_topic, define_policy, begin/end_migration,
  # extract/insert_topic). Range/segment commands resolve to their owning topic via the state.
  defp command_topic(_state, {:seal_topic, name}), do: name
  defp command_topic(_state, {:delete_topic, name}), do: name
  defp command_topic(_state, {:set_topic_policy, name, _policy}), do: name
  defp command_topic(_state, {:commit_offset, _group, topic, _offsets}), do: topic
  defp command_topic(state, {:split_range, range_id}), do: range_topic(state, range_id)
  defp command_topic(state, {:merge_ranges, range_id_a, _range_id_b}), do: range_topic(state, range_id_a)
  defp command_topic(state, {:register_segment, range_id, _seg, _replicas, _off}), do: range_topic(state, range_id)
  defp command_topic(state, {:seal_segment, segment_id, _len, _bytes, _at}), do: segment_topic(state, segment_id)
  defp command_topic(state, {:delete_segment, segment_id}), do: segment_topic(state, segment_id)
  defp command_topic(state, {:set_segment_replicas, segment_id, _replicas}), do: segment_topic(state, segment_id)
  defp command_topic(_state, _unfenced), do: nil

  defp range_topic(state, range_id) do
    case Map.fetch(state.ranges, range_id) do
      {:ok, range} -> range.topic
      :error -> nil
    end
  end

  defp segment_topic(state, segment_id) do
    case Map.fetch(state.segments, segment_id) do
      {:ok, segment} -> range_topic(state, segment.range_id)
      :error -> nil
    end
  end

  defp do_apply(%__MODULE__{} = state, {:create_topic, name, keyspace_bits}) do
    cond do
      not valid_topic_name?(name) -> {state, {:error, :invalid_topic_name}}
      Map.has_key?(state.topics, name) -> {state, {:error, :already_exists}}
      true -> create_topic_with_bits(state, name, keyspace_bits)
    end
  end

  defp do_apply(%__MODULE__{} = state, {:seal_topic, name}) do
    case Map.fetch(state.topics, name) do
      :error ->
        {state, {:error, :no_such_topic}}

      {:ok, topic} ->
        topics = Map.put(state.topics, name, %{topic | state: :sealed})
        ranges = seal_topic_ranges(state, name)
        {%{state | topics: topics, ranges: ranges}, :ok}
    end
  end

  defp do_apply(%__MODULE__{} = state, {:delete_topic, name}) do
    case Map.fetch(state.topics, name) do
      :error ->
        {state, {:error, :no_such_topic}}

      {:ok, _topic} ->
        doomed_range_ids = range_ids_of_topic(state, name)
        doomed_segment_ids = Enum.flat_map(doomed_range_ids, &segment_ids_of_range(state, &1))

        ranges = Map.drop(state.ranges, doomed_range_ids)
        segments = Map.drop(state.segments, doomed_segment_ids)
        topics = Map.delete(state.topics, name)

        state =
          %{state | topics: topics, ranges: ranges, segments: segments}
          |> index_forget_topic(name, doomed_range_ids)

        {state, :ok}
    end
  end

  defp do_apply(%__MODULE__{} = state, {:split_range, range_id}) do
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

  defp do_apply(%__MODULE__{} = state, {:merge_ranges, range_id_a, range_id_b}) do
    with {:ok, range_a} <- fetch_active_range(state, range_id_a),
         {:ok, range_b} <- fetch_active_range(state, range_id_b),
         :ok <- check_mergeable(range_a, range_b) do
      do_merge_ranges(state, range_a, range_b)
    else
      {:error, _reason} = error -> {state, error}
    end
  end

  defp do_apply(%__MODULE__{} = state, {:register_segment, range_id, segment_id, replica_set, start_offset}) do
    cond do
      Map.has_key?(state.segments, segment_id) ->
        {state, {:error, :segment_exists}}

      start_offset < range_end(state, range_id) ->
        # A segment starting below where the range already ends would claim offsets another segment
        # owns, and two segments handing out the same offsets is how one acknowledged record quietly
        # replaces another. The caller derives this offset from its own view, which can lag behind a
        # seal applied elsewhere, so the control plane checks it rather than trusting it: a frontend
        # that is behind gets an error it can retry after refreshing, instead of silently overlapping.
        {state, {:error, :segment_overlap}}

      true ->
        register_new_segment(state, range_id, segment_id, replica_set, start_offset)
    end
  end

  defp do_apply(%__MODULE__{} = state, {:seal_segment, segment_id, length, byte_size, sealed_at}) do
    update_segment(state, segment_id, fn segment ->
      %{segment | state: :sealed, length: length, byte_size: byte_size, sealed_at: sealed_at}
    end)
  end

  defp do_apply(%__MODULE__{} = state, {:delete_segment, segment_id}) do
    # Retention removes an expired segment from the control plane. Only sealed segments are
    # deletable: the active segment is still being written and must never be dropped.
    case Map.fetch(state.segments, segment_id) do
      :error ->
        {state, {:error, :no_such_segment}}

      {:ok, %{state: :active}} ->
        {state, {:error, :segment_active}}

      {:ok, sealed} ->
        state = %{state | segments: Map.delete(state.segments, segment_id)}
        {index_forget_segment(state, sealed.range_id, segment_id), :ok}
    end
  end

  defp do_apply(%__MODULE__{} = state, {:set_segment_replicas, segment_id, replica_set}) do
    update_segment(state, segment_id, fn segment -> %{segment | replica_set: replica_set} end)
  end

  defp do_apply(%__MODULE__{} = state, {:commit_offset, group, topic, offsets}) do
    # A consumer group's durable position, **merged per range** (last commit wins for each range) rather
    # than replaced: with a partitioned group, each member commits only the ranges it owns, and must not
    # clobber the positions of ranges owned by other members. Then **prune** offsets of ranges that are no
    # longer active: a split/merge retires a range whose committed offset can never be consumed again (the
    # active children resume from `:start`), so without this the map would grow one dead key per split.
    # Pruning is skipped when the topic is not routed yet (an offset committed before `create_topic`).
    merged = Map.merge(committed_offsets(state, group, topic), offsets)
    committed = Map.put(state.committed_offsets, {group, topic}, prune_offsets(state, topic, merged))
    {%{state | committed_offsets: committed}, :ok}
  end

  defp do_apply(%__MODULE__{} = state, {:define_policy, name, policy}) do
    if is_binary(name) and name != "" and is_map(policy) do
      {%{state | policies: Map.put(state.policies, name, policy)}, :ok}
    else
      {state, {:error, :invalid_policy}}
    end
  end

  defp do_apply(%__MODULE__{} = state, {:set_topic_policy, topic, policy_name}) do
    cond do
      not Map.has_key?(state.topics, topic) ->
        {state, {:error, :no_such_topic}}

      policy_name != nil and not Map.has_key?(state.policies, policy_name) ->
        {state, {:error, :no_such_policy}}

      true ->
        topics = Map.update!(state.topics, topic, fn topic -> %{topic | policy: policy_name} end)
        {%{state | topics: topics}, :ok}
    end
  end

  # Vnode-split migration (driven through each vnode's Raft log): `:extract_topic` removes a topic's full
  # metadata from the source vnode and returns it as an `export` (the reply), `:insert_topic` restores that
  # export on the destination vnode. Together they relocate a topic, with its ranges, segments and
  # committed offsets: from one vnode to another, keeping the move deterministic and replay-safe.
  defp do_apply(%__MODULE__{} = state, {:extract_topic, name}) do
    extract_topic(state, name)
  end

  defp do_apply(%__MODULE__{} = state, {:insert_topic, export}) do
    {insert_topic(state, export), :ok}
  end

  # Fence a topic for migration (a vnode split copying it away): further mutating commands on it are
  # rejected until `:end_migration` or `:extract_topic`. Idempotent; errors on an absent topic.
  defp do_apply(%__MODULE__{} = state, {:begin_migration, name}) do
    if Map.has_key?(state.topics, name) do
      {%{state | migrating: Map.put(state.migrating, name, true)}, :ok}
    else
      {state, {:error, :no_such_topic}}
    end
  end

  defp do_apply(%__MODULE__{} = state, {:end_migration, name}) do
    {%{state | migrating: Map.delete(state.migrating, name)}, :ok}
  end

  # Defensive catch-all: an unknown command must NOT crash the machine. Once this RSM is
  # replicated by Raft, a command that raises in `apply` would crash every replica
  # deterministically (and again on replay), e.g. an older replica seeing a newer
  # command during a rolling upgrade. Keep the replica alive and surface the problem.
  defp do_apply(%__MODULE__{} = state, _unknown_command), do: {state, {:error, :unknown_command}}

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

  @doc "All ranges of a topic (any state). O(k) via the `topic_ranges` index, see `apply/2`."
  @spec ranges_of_topic(t(), topic_name()) :: [range_meta()]
  def ranges_of_topic(%__MODULE__{} = state, name) do
    state.topic_ranges
    |> Map.get(name, @empty_set)
    |> Enum.map(&Map.fetch!(state.ranges, &1))
  end

  @doc "The active ranges of a topic (the ones that currently tile the keyspace)."
  @spec active_ranges_of_topic(t(), topic_name()) :: [range_meta()]
  def active_ranges_of_topic(%__MODULE__{} = state, name) do
    state |> ranges_of_topic(name) |> Enum.filter(&(&1.state == :active))
  end

  @doc "All segments of a range. O(k) via the `range_segments` index, see `apply/2`."
  @spec segments_of_range(t(), range_id()) :: [segment_meta()]
  def segments_of_range(%__MODULE__{} = state, range_id) do
    state.range_segments
    |> Map.get(range_id, @empty_set)
    |> Enum.map(&Map.fetch!(state.segments, &1))
  end

  @doc "A consumer group's committed offsets for a topic, or `%{}` if it has never committed."
  @spec committed_offsets(t(), group(), topic_name()) :: offsets()
  def committed_offsets(%__MODULE__{} = state, group, topic) do
    Map.get(state.committed_offsets, {group, topic}, %{})
  end

  @doc """
  A read-only, JSON-serializable **summary** of the whole log stack for the ops dashboard: one entry per
  topic (sorted by name) with state, keyspace, policy, range/segment counts, total bytes, and the
  consumer groups that have committed a position. Pure. This is the light payload the dashboard streams
  every tick; the per-range/segment drill-down is fetched on demand via `topic_detail/2`.
  """
  @spec overview(t()) :: [map()]
  def overview(%__MODULE__{} = state) do
    state.topics
    |> Map.values()
    |> Enum.sort_by(& &1.name)
    |> Enum.map(&topic_summary(state, &1))
  end

  defp topic_summary(state, topic) do
    ranges = ranges_of_topic(state, topic.name)
    segments = Enum.flat_map(ranges, &segments_of_range(state, &1.id))

    %{
      name: topic.name,
      state: topic.state,
      keyspace_size: topic.keyspace_size,
      policy: topic.policy,
      range_count: length(ranges),
      active_range_count: Enum.count(ranges, &(&1.state == :active)),
      segment_count: length(segments),
      active_segment_count: Enum.count(segments, &(&1.state == :active)),
      total_bytes: segments |> Enum.map(&(&1.byte_size || 0)) |> Enum.sum(),
      groups: groups_of_topic(state, topic.name)
    }
  end

  @doc """
  The JSON-serializable drill-down for a single `name`: its ranges (sorted by seq), each with its
  segments (sorted by start_offset). `nil` if the topic does not exist. Pure: the on-demand counterpart
  to `overview/1`, so the per-second stream stays light and segment detail is fetched only when a topic
  is expanded. `range_id`/`segment_id` tuples are flattened to display fields (`seq`, `start_offset`).
  """
  @spec topic_detail(t(), topic_name()) :: map() | nil
  def topic_detail(%__MODULE__{} = state, name) do
    case Map.get(state.topics, name) do
      nil ->
        nil

      topic ->
        ranges =
          state
          |> ranges_of_topic(name)
          |> Enum.sort_by(&elem(&1.id, 1))
          |> Enum.map(&range_detail(state, &1))

        %{name: topic.name, ranges: ranges}
    end
  end

  defp range_detail(state, range) do
    segments =
      state
      |> segments_of_range(range.id)
      |> Enum.sort_by(& &1.start_offset)
      |> Enum.map(&segment_detail/1)

    %{
      seq: elem(range.id, 1),
      key_start: range.key_start,
      key_end: range.key_end,
      state: range.state,
      parents: length(range.parents),
      segments: segments
    }
  end

  defp segment_detail(segment) do
    segment
    |> Map.take([:state, :start_offset, :length, :byte_size, :sealed_at])
    |> Map.put(:seq, segment_seq(segment.id))
    |> Map.put(:primary, segment.replica_set |> List.first() |> broker_ref_string())
    |> Map.put(:replica_set, Enum.map(segment.replica_set, &broker_ref_string/1))
  end

  # The segment's sequence within its range (the broker's `{range_id, seq}` id shape), which with the
  # range's seq names the segment's on-disk directory; `nil` for foreign id shapes.
  defp segment_seq({_range_id, seq}) when is_integer(seq), do: seq
  defp segment_seq(_id), do: nil

  # JSON-safe display form of a replica reference. The primary is the replica set's head (the same
  # convention the broker and replication servers use), so exposing the set exposes ownership: an
  # operator (or the chaos harness) can see which node serves a segment. `{name, node}` collapses to
  # the node, which is what identifies a broker to an operator; pids and bare atoms are single-node
  # shapes where ownership is trivially local.
  defp broker_ref_string(nil), do: nil
  defp broker_ref_string({_name, node}) when is_atom(node), do: Atom.to_string(node)
  defp broker_ref_string(ref) when is_atom(ref), do: Atom.to_string(ref)
  defp broker_ref_string(ref), do: inspect(ref)

  defp groups_of_topic(state, topic_name) do
    state.committed_offsets
    |> Map.keys()
    |> Enum.filter(fn {_group, topic} -> topic == topic_name end)
    |> Enum.map(fn {group, _topic} -> group end)
    |> Enum.sort()
  end

  @doc "The policy named `name`, or `nil` if undefined."
  @spec get_policy(t(), policy_name()) :: policy() | nil
  def get_policy(%__MODULE__{} = state, name), do: Map.get(state.policies, name)

  @doc "The policy governing `topic` (its associated policy), or `nil` if none/unknown (use globals)."
  @spec topic_policy(t(), topic_name()) :: policy() | nil
  def topic_policy(%__MODULE__{} = state, topic) do
    case Map.get(state.topics, topic) do
      %{policy: name} when is_binary(name) -> Map.get(state.policies, name)
      _topic_without_policy_or_unknown -> nil
    end
  end

  # --- migration (vnode split) ---

  @doc """
  A **read-only** snapshot of a topic's full metadata: topic + all its ranges/segments + its consumer
  groups' committed offsets (keyed by group, the topic implied): as a `topic_export`, or `nil` if the
  topic is absent. This is what a **copy-first** vnode split reads and `insert_topic/2`s into the new
  vnode *before* `extract_topic/2` removes it from the source, so a failed migration never loses a topic.
  """
  @spec export_topic(t(), topic_name()) :: topic_export() | nil
  def export_topic(%__MODULE__{} = state, name) do
    case Map.fetch(state.topics, name) do
      :error ->
        nil

      {:ok, topic} ->
        range_ids = for {id, range} <- state.ranges, range.topic == name, into: MapSet.new(), do: id
        ranges = Map.take(state.ranges, MapSet.to_list(range_ids))
        segments = Map.filter(state.segments, fn {_id, seg} -> MapSet.member?(range_ids, seg.range_id) end)
        offsets = for {{group, ^name}, offs} <- state.committed_offsets, into: %{}, do: {group, offs}
        %{topic: topic, ranges: ranges, segments: segments, offsets: offsets}
    end
  end

  @doc """
  Removes a topic and all its ranges/segments, and its consumer groups' committed offsets:
  from `state`, returning `{state_without_topic, export}` (or `{state, nil}` if the topic is
  absent). The export can be re-inserted on another vnode with `insert_topic/2`: this is how
  a vnode split migrates a topic's metadata to a new vnode. Offsets ride along (keyed by group,
  the topic implied) so a consumer group keeps its committed position across a split.
  """
  @spec extract_topic(t(), topic_name()) :: {t(), topic_export() | nil}
  def extract_topic(%__MODULE__{} = state, name) do
    case export_topic(state, name) do
      nil ->
        {state, nil}

      export ->
        range_ids = Map.keys(export.ranges)

        remaining =
          %{
            state
            | topics: Map.delete(state.topics, name),
              ranges: Map.drop(state.ranges, range_ids),
              segments: Map.drop(state.segments, Map.keys(export.segments)),
              committed_offsets: Map.reject(state.committed_offsets, fn {{_g, t}, _offs} -> t == name end),
              # the topic is gone, so drop any migration fence it held
              migrating: Map.delete(state.migrating, name)
          }
          |> index_forget_topic(name, range_ids)

        {remaining, export}
    end
  end

  @doc """
  Inserts a topic `export` (from `extract_topic/2`) into `state`. Range ids are globally
  unique (`{topic, seq}`), so merged ranges never collide with another topic's.

  Segment ids, however, are caller-supplied and independent of range ids: this merges them
  by id, so a segment id that already exists in `state` is **overwritten**. Migration is
  therefore safe only if segment ids are globally unique across the cluster (the
  broker-assigned contract, see `register_segment` in `apply/2`).
  """
  @spec insert_topic(t(), topic_export()) :: t()
  def insert_topic(%__MODULE__{} = state, export) do
    name = export.topic.name
    # re-key the exported per-group offsets back to {group, topic}; default `%{}` tolerates an older
    # export (e.g. an :insert_topic command logged before offsets rode along)
    offsets = for {group, offs} <- Map.get(export, :offsets, %{}), into: %{}, do: {{group, name}, offs}

    state = %{
      state
      | topics: Map.put(state.topics, name, export.topic),
        ranges: Map.merge(state.ranges, export.ranges),
        segments: Map.merge(state.segments, export.segments),
        committed_offsets: Map.merge(state.committed_offsets, offsets)
    }

    state = Enum.reduce(export.ranges, state, fn {id, range}, s -> index_add_range(s, range.topic, id) end)
    Enum.reduce(export.segments, state, fn {id, seg}, s -> index_add_segment(s, seg.range_id, id) end)
  end

  # --- internals: topic ---

  # Topic names are used in file paths (range log directories), so restrict them to a safe
  # allowlist and reject "."/"..": preventing path traversal at the data plane.
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
    topic = %{name: name, keyspace_size: keyspace_size, state: :active, next_range_seq: 1, policy: nil}

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

    {index_add_range(state, name, root_id), {:ok, root_id}}
  end

  # Seals only the topic's ranges (O(k) via the index), returning the updated `ranges` map.
  defp seal_topic_ranges(state, name) do
    state
    |> range_ids_of_topic(name)
    |> Enum.reduce(state.ranges, fn id, ranges -> Map.update!(ranges, id, &%{&1 | state: :sealed}) end)
  end

  # The range ids of a topic / the segment ids of a range, straight from the reverse index (O(1)+O(k)).
  defp range_ids_of_topic(state, name), do: state.topic_ranges |> Map.get(name, @empty_set) |> MapSet.to_list()

  # Drops committed offsets whose range is no longer active (a retired split/merge parent). Skipped when
  # the topic has no ranges index yet (committed before routing) so those offsets are kept as-is.
  defp prune_offsets(state, topic, offsets) do
    if Map.has_key?(state.topic_ranges, topic) do
      active = active_range_id_set(state, topic)
      Map.filter(offsets, fn {range_id, _offset} -> MapSet.member?(active, range_id) end)
    else
      offsets
    end
  end

  defp active_range_id_set(state, topic) do
    state
    |> range_ids_of_topic(topic)
    |> Enum.filter(fn range_id -> Map.fetch!(state.ranges, range_id).state == :active end)
    |> MapSet.new()
  end

  defp segment_ids_of_range(state, range_id),
    do: state.range_segments |> Map.get(range_id, @empty_set) |> MapSet.to_list()

  # --- internals: secondary index maintenance ---
  # Every membership change to `ranges`/`segments` mirrors into the reverse indexes here, so they stay
  # exactly equal to what a scan would return. An entry is dropped once its set empties, so a missing key
  # reads as "no members" (matching the scan, which returns []).

  defp index_add_range(state, topic, range_id) do
    topic_ranges = Map.update(state.topic_ranges, topic, MapSet.new([range_id]), &MapSet.put(&1, range_id))
    %{state | topic_ranges: topic_ranges}
  end

  defp index_add_segment(state, range_id, segment_id) do
    range_segments = Map.update(state.range_segments, range_id, MapSet.new([segment_id]), &MapSet.put(&1, segment_id))
    %{state | range_segments: range_segments}
  end

  defp index_forget_segment(state, range_id, segment_id) do
    %{state | range_segments: drop_from_index(state.range_segments, range_id, segment_id)}
  end

  # Forgets a whole topic: its `topic_ranges` entry and the `range_segments` entries of all its ranges.
  defp index_forget_topic(state, topic, range_ids) do
    %{
      state
      | topic_ranges: Map.delete(state.topic_ranges, topic),
        range_segments: Map.drop(state.range_segments, range_ids)
    }
  end

  # Removes `member` from the set at `key`, dropping the key entirely once its set is empty.
  defp drop_from_index(index, key, member) do
    case Map.fetch(index, key) do
      :error ->
        index

      {:ok, set} ->
        set = MapSet.delete(set, member)
        if MapSet.size(set) == 0, do: Map.delete(index, key), else: Map.put(index, key, set)
    end
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

    state =
      %{state | ranges: ranges}
      |> index_add_range(range.topic, left_id)
      |> index_add_range(range.topic, right_id)

    {state, {:ok, left_id, right_id}}
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

    state = index_add_range(%{state | ranges: ranges}, range_a.topic, child_id)
    {state, {:ok, child_id}}
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

  # Where a range's segments end today: a sealed one ends at its start plus its length, and an active
  # one is only known to reach its start, which is enough to catch a registration below it.
  defp range_end(state, range_id) do
    state
    |> segments_of_range(range_id)
    |> Enum.map(fn
      %{start_offset: start, length: length} when is_integer(length) -> start + length
      %{start_offset: start} -> start
    end)
    |> Enum.max(fn -> 0 end)
  end

  defp register_new_segment(state, range_id, segment_id, replica_set, start_offset) do
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
          byte_size: nil,
          sealed_at: nil
        }

        state = %{state | segments: Map.put(state.segments, segment_id, segment)}
        {index_add_segment(state, range_id, segment_id), :ok}
    end
  end

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
