defmodule Malachi.Cluster.DSRSM do
  @moduledoc """
  The Dynamically-Sharded Replicated State Machine: a `Malachi.Cluster.HashRing` plus one
  `Malachi.Metadata` state machine per vnode. Metadata commands and queries are routed by
  consistent hashing to the vnode that owns them, so the cluster's metadata is sharded
  across vnodes (and different topics land on different vnodes, avoiding hotspots).

  ## Sharding granularity

  A topic's metadata: the topic plus **all** its ranges and segments - is co-located on a
  single vnode, routed by **topic name**. Every command and query therefore carries the
  topic name. This keeps each operation single-vnode and keeps range ids unique within the
  vnode that owns them.

  NorthGuard additionally shards a topic's ranges/segments by *range id* across vnodes (so a
  single hot topic spreads out). That requires cross-vnode operations and a
  globally-unique range-id scheme, so it, and **vnode split** (rebalancing, which migrates
  metadata between vnodes): are deferred to a later increment. See `docs/ARCHITECTURE.md`.

  ## Caller contract: `(topic_name, range_id/segment_id)` must match

  Because the topic name is used only for *routing*, the target `range_id`/`segment_id` of a
  command or query must actually belong to `topic_name`. A mismatched pair is not rejected:
  since several co-located topics share a vnode (and range ids are only unique within a
  vnode), a command naming topic A but targeting a range of co-located topic B would
  silently act on B's range. Callers (the coordinator) must pass matching pairs.

  > TODO: this validation gap closes with range-id sharding, where range/segment
  > operations route by range id and the range's own vnode is authoritative, the topic
  > param drops out for those ops, so a mismatch becomes impossible rather than relying on
  > the caller.

  Like `Metadata`, this is a pure, deterministic value: `command/3` returns
  `{new_dsrsm, reply}`. Backing each vnode with its own `ra` group leaves this routing layer
  unchanged; see `Malachi.Cluster.ReplicatedDSRSM`, which does exactly that.
  """

  alias Malachi.Cluster.HashRing
  alias Malachi.Metadata

  @type vnode_id :: HashRing.vnode_id()

  @type t :: %__MODULE__{
          ring: HashRing.t(),
          vnodes: %{vnode_id() => Metadata.t()}
        }

  defstruct ring: nil, vnodes: %{}

  @doc """
  Builds an empty DS-RSM with no vnodes. Options are forwarded to `HashRing.new/1`
  (e.g. `:ring_bits`).
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []), do: %__MODULE__{ring: HashRing.new(opts), vnodes: %{}}

  @doc """
  A DS-RSM with a single vnode holding `metadata`: the trivial, unsharded shape. Seeds a one-vnode
  control plane (the current single-cluster runtime and tests) before real multi-vnode sharding: with
  one vnode every topic routes to it, so behavior matches a plain `Malachi.Metadata`.
  """
  @spec single(Metadata.t()) :: t()
  def single(metadata \\ Metadata.new()) do
    {:ok, ring} = HashRing.add_vnode(HashRing.new(), :vnode_0, 0)
    %__MODULE__{ring: ring, vnodes: %{vnode_0: metadata}}
  end

  @doc """
  A fresh cache with `vnode_ids`' entries taken from `previous` instead: what a reader does with the
  vnodes a snapshot could not reach. Their entry in `fresh` is an empty placeholder, and installing
  that placeholder would erase topics whose records are durable on disk, turning every read of them
  into a successful empty page. Keeping the last view this reader held is stale at worst, and the
  reader already tolerates staleness between refreshes; it is `nil` metadata that has no honest
  reading. A vnode absent from `previous` (never yet reached) keeps the placeholder, since there is no
  older view to keep.
  """
  @spec retain_vnodes(t(), t(), [vnode_id()]) :: t()
  def retain_vnodes(%__MODULE__{} = fresh, %__MODULE__{} = previous, vnode_ids) do
    %{fresh | vnodes: Map.merge(fresh.vnodes, Map.take(previous.vnodes, vnode_ids))}
  end

  @doc """
  A DS-RSM over `ring` whose vnodes already hold `metadata_by_vnode` (`%{vnode_id => Metadata}`):
  used to seed a local read cache that mirrors an authoritative `Malachi.Cluster.ReplicatedDSRSM`
  sharing the same ring: reads are served from this cache, writes routed back through the vnodes' ra
  clusters. `metadata_by_vnode` must key exactly the ring's vnodes.
  """
  @spec seed(HashRing.t(), %{vnode_id() => Metadata.t()}) :: t()
  def seed(%HashRing{} = ring, metadata_by_vnode) when is_map(metadata_by_vnode) do
    %__MODULE__{ring: ring, vnodes: metadata_by_vnode}
  end

  @doc """
  Adds a vnode at `token`, migrating any displaced topics to it: the general "grow the
  ring" entry point. Delegates to `split_vnode/3`, so it is safe whether or not topics
  already exist (migration is a no-op on an empty ring). Propagates `HashRing` placement
  errors (`:token_out_of_range`, `:token_taken`, `:already_present`).
  """
  @spec add_vnode(t(), vnode_id(), HashRing.token()) :: {:ok, t()} | {:error, atom()}
  def add_vnode(%__MODULE__{} = dsrsm, vnode_id, token), do: split_vnode(dsrsm, vnode_id, token)

  @doc """
  Adds a new vnode at `token` and **migrates** to it every topic that now routes there:
  the "dynamically sharded" part of DS-RSM (rebalancing). Adding a vnode only steals an arc
  from one existing vnode, so only that vnode's affected topics move; their full metadata
  (topic + ranges + segments) is relocated. Range ids stay valid because they are globally
  unique (`{topic, seq}`); migration is likewise safe for segments only if segment ids are
  globally unique (the broker-assigned contract, see `Malachi.Metadata`). Propagates
  `HashRing` placement errors.
  """
  @spec split_vnode(t(), vnode_id(), HashRing.token()) :: {:ok, t()} | {:error, atom()}
  def split_vnode(%__MODULE__{} = dsrsm, new_vnode_id, token) do
    case HashRing.add_vnode(dsrsm.ring, new_vnode_id, token) do
      {:ok, ring} ->
        dsrsm = %{dsrsm | ring: ring, vnodes: Map.put(dsrsm.vnodes, new_vnode_id, Metadata.new())}
        {:ok, migrate_displaced_topics(dsrsm, new_vnode_id)}

      {:error, _reason} = error ->
        error
    end
  end

  @doc """
  Routes a `Malachi.Metadata` command to the vnode owning `topic_name` and applies it there,
  returning `{new_dsrsm, reply}`. `reply` is whatever `Metadata.apply/2` returns, or
  `{:error, :no_vnode}` if the ring is empty.

  For range/segment commands, the targeted id **must** belong to `topic_name`, see the
  caller contract in the module docs (a mismatch is not rejected and may act on a
  co-located topic's metadata).
  """
  @spec command(t(), Metadata.topic_name(), Metadata.command()) :: {t(), term()}
  def command(%__MODULE__{} = dsrsm, topic_name, command) do
    update_vnode(dsrsm, topic_name, &Metadata.apply(&1, command))
  end

  @doc """
  Routes `topic_name` to its vnode and updates that vnode's `Metadata` with `update_fun`, the
  general single-vnode mutation combinator. `update_fun` receives the vnode's `Metadata` and returns
  `{new_metadata, reply}` (the `Malachi.Metadata.apply/2` shape); the new metadata replaces the
  vnode's and `reply` is returned as-is. `{dsrsm, {:error, :no_vnode}}` if the ring is empty.

  `command/3` is this with `&Malachi.Metadata.apply(&1, command)` (pure). A Raft-backed control plane
  injects an authoritative apply here instead (see `Malachi.BrokerServer`), so purity/determinism
  hold only when `update_fun` is itself pure.
  """
  @spec update_vnode(t(), Metadata.topic_name(), (Metadata.t() -> {Metadata.t(), reply})) ::
          {t(), reply | {:error, :no_vnode}}
        when reply: term()
  def update_vnode(%__MODULE__{} = dsrsm, topic_name, update_fun) do
    case HashRing.route(dsrsm.ring, topic_name) do
      {:error, :empty} ->
        {dsrsm, {:error, :no_vnode}}

      {:ok, vnode_id} ->
        {metadata, reply} = update_fun.(Map.fetch!(dsrsm.vnodes, vnode_id))
        {%{dsrsm | vnodes: Map.put(dsrsm.vnodes, vnode_id, metadata)}, reply}
    end
  end

  @doc "The vnode id that owns `topic_name`, or `{:error, :empty}` if the ring has no vnodes."
  @spec vnode_for(t(), Metadata.topic_name()) :: {:ok, vnode_id()} | {:error, :empty}
  def vnode_for(%__MODULE__{} = dsrsm, topic_name), do: HashRing.route(dsrsm.ring, topic_name)

  @doc "The ids of the vnodes."
  @spec vnode_ids(t()) :: [vnode_id()]
  def vnode_ids(%__MODULE__{} = dsrsm), do: HashRing.vnode_ids(dsrsm.ring)

  # --- routed queries (all by topic name) ---

  @doc "The topic metadata, or `nil`."
  @spec get_topic(t(), Metadata.topic_name()) :: Metadata.topic_meta() | nil
  def get_topic(%__MODULE__{} = dsrsm, topic_name) do
    query(dsrsm, topic_name, &Metadata.get_topic(&1, topic_name))
  end

  @doc "All ranges of a topic."
  @spec ranges_of_topic(t(), Metadata.topic_name()) :: [Metadata.range_meta()]
  def ranges_of_topic(%__MODULE__{} = dsrsm, topic_name) do
    query(dsrsm, topic_name, &Metadata.ranges_of_topic(&1, topic_name)) || []
  end

  @doc "The active ranges of a topic."
  @spec active_ranges_of_topic(t(), Metadata.topic_name()) :: [Metadata.range_meta()]
  def active_ranges_of_topic(%__MODULE__{} = dsrsm, topic_name) do
    query(dsrsm, topic_name, &Metadata.active_ranges_of_topic(&1, topic_name)) || []
  end

  @doc "A range of `topic_name`, or `nil`. `range_id` must belong to `topic_name` (see caller contract)."
  @spec get_range(t(), Metadata.topic_name(), Metadata.range_id()) :: Metadata.range_meta() | nil
  def get_range(%__MODULE__{} = dsrsm, topic_name, range_id) do
    query(dsrsm, topic_name, &Metadata.get_range(&1, range_id))
  end

  @doc "A segment of `topic_name`, or `nil`. `segment_id` must belong to `topic_name` (see caller contract)."
  @spec get_segment(t(), Metadata.topic_name(), Metadata.segment_id()) ::
          Metadata.segment_meta() | nil
  def get_segment(%__MODULE__{} = dsrsm, topic_name, segment_id) do
    query(dsrsm, topic_name, &Metadata.get_segment(&1, segment_id))
  end

  @doc "All segments of a range under `topic_name`. `range_id` must belong to `topic_name` (see caller contract)."
  @spec segments_of_range(t(), Metadata.topic_name(), Metadata.range_id()) ::
          [Metadata.segment_meta()]
  def segments_of_range(%__MODULE__{} = dsrsm, topic_name, range_id) do
    query(dsrsm, topic_name, &Metadata.segments_of_range(&1, range_id)) || []
  end

  @doc "A consumer group's committed offsets for `topic_name` (empty if it never committed)."
  @spec committed_offsets(t(), Metadata.group(), Metadata.topic_name()) :: Metadata.offsets()
  def committed_offsets(%__MODULE__{} = dsrsm, group, topic_name) do
    query(dsrsm, topic_name, &Metadata.committed_offsets(&1, group, topic_name)) || %{}
  end

  @doc "The storage policy governing `topic_name`, or `nil` if none/unknown (use the globals)."
  @spec topic_policy(t(), Metadata.topic_name()) :: Metadata.policy() | nil
  def topic_policy(%__MODULE__{} = dsrsm, topic_name) do
    query(dsrsm, topic_name, &Metadata.topic_policy(&1, topic_name))
  end

  @doc """
  The union of every vnode's `Metadata`: a single flat view for whole-cluster consumers (retention,
  healing) that iterate all segments. Topics/ranges/segments/offsets are disjoint across vnodes (each
  topic lives on one vnode), so the union is unambiguous; with one vnode it is that vnode's metadata.
  """
  @spec merged_metadata(t()) :: Metadata.t()
  def merged_metadata(%__MODULE__{vnodes: vnodes}) do
    case Map.values(vnodes) do
      [single] -> single
      many -> Enum.reduce(many, Metadata.new(), &merge_metadata/2)
    end
  end

  # --- internals ---

  # Unions one vnode's metadata into the accumulator. Shards are disjoint by topic, so the per-field
  # `Map.merge` never collides; folded from an empty `Metadata`, so one vnode yields exactly it.
  defp merge_metadata(from, acc) do
    %Metadata{
      topics: Map.merge(acc.topics, from.topics),
      ranges: Map.merge(acc.ranges, from.ranges),
      segments: Map.merge(acc.segments, from.segments),
      committed_offsets: Map.merge(acc.committed_offsets, from.committed_offsets),
      policies: Map.merge(acc.policies, from.policies),
      # Shards are disjoint by topic (and range), so the index keys never collide: a plain merge keeps
      # the reverse indexes exact on the merged view too.
      topic_ranges: Map.merge(acc.topic_ranges, from.topic_ranges),
      range_segments: Map.merge(acc.range_segments, from.range_segments)
    }
  end

  # Routes `topic_name` to its vnode and runs `fun` over that vnode's metadata. Returns nil
  # when the ring is empty (the caller turns that into nil/[] per its return contract).
  defp query(dsrsm, topic_name, fun) do
    case HashRing.route(dsrsm.ring, topic_name) do
      {:error, :empty} -> nil
      {:ok, vnode_id} -> fun.(Map.fetch!(dsrsm.vnodes, vnode_id))
    end
  end

  # Moves every topic that now routes to `new_vnode_id` out of whatever vnode currently
  # holds it and into the new one.
  defp migrate_displaced_topics(dsrsm, new_vnode_id) do
    Enum.reduce(dsrsm.vnodes, dsrsm, fn {source_id, _metadata}, acc ->
      if source_id == new_vnode_id do
        acc
      else
        acc
        |> displaced_topics(source_id, new_vnode_id)
        |> Enum.reduce(acc, &migrate_topic(&2, &1, source_id, new_vnode_id))
      end
    end)
  end

  defp displaced_topics(dsrsm, source_id, new_vnode_id) do
    source_metadata = Map.fetch!(dsrsm.vnodes, source_id)

    for name <- Map.keys(source_metadata.topics),
        HashRing.route(dsrsm.ring, name) == {:ok, new_vnode_id},
        do: name
  end

  defp migrate_topic(dsrsm, name, source_id, new_vnode_id) do
    {source_metadata, export} = Metadata.extract_topic(Map.fetch!(dsrsm.vnodes, source_id), name)

    case export do
      nil ->
        dsrsm

      export ->
        new_metadata = Metadata.insert_topic(Map.fetch!(dsrsm.vnodes, new_vnode_id), export)

        vnodes =
          dsrsm.vnodes
          |> Map.put(source_id, source_metadata)
          |> Map.put(new_vnode_id, new_metadata)

        %{dsrsm | vnodes: vnodes}
    end
  end
end
