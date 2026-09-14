defmodule Malachi.Broker do
  @moduledoc """
  The control-plane **router** on a single node: it owns `Malachi.Metadata` (the source of truth
  for topics, ranges and segments) and decides *where* each record goes, but it no longer stores
  anything itself. Storage and replication of a segment's records live in
  `Malachi.Cluster.ReplicationServer`; the broker drives them through **injected effect functions**
  so its routing/lifecycle logic stays pure and testable with in-memory fakes.

  `Malachi.Metadata` is the source of truth for structure, which topics and ranges exist, their
  keyspace bounds and active/sealed state. Producing routes each record to the active range that
  owns its key (hashed with `Malachi.Keyspace`). Within a range the data is divided into
  **segments**. NorthGuard's unit of replication: each active range has one open segment, whose
  ordered `replica_set` is chosen by `Malachi.Cluster.Placement`. The broker registers segments,
  tallies bytes, and REQUESTS a roll of the active one once it reaches `:segment_max_bytes`. Offsets are
  contiguous *per range* (a segment is a window `[start_offset, ...)` of its range).

  Sealing is the one lifecycle step this module cannot finish on its own, because a sealed length must be
  what closing the segment ANSWERS rather than a number this frontend measured beside a log that is still
  growing. So a threshold crossing only records a `roll` here (`due_rolls/1`); the caller
  (`Malachi.BrokerServer`) fences the segment's store, learns its true end, and hands it back through
  `record_seal/5`, which is what emits the `:seal_segment` command.

  Effects are injected, never performed here:

    * `replicate_fun.(primary, segment_id, replica_set, base_offset, records)` →
      `{:ok, last_offset} | {:error, reason}`, appends/replicates a batch (e.g.
      `&Malachi.Cluster.ReplicationServer.replicate/5`).
    * `read_fun.(ref, segment_id, offset, max_records)` →
      `{:ok, records} | :eof | {:error, reason}`, reads one segment (e.g.
      `&Malachi.Cluster.ReplicationServer.read/4`).

  The broker is a functional value threaded through calls (no GenServer); `Malachi.BrokerServer`
  wires the real `ReplicationServer`-backed effects and serializes access.
  """

  alias Malachi.Cluster.DSRSM
  alias Malachi.Cluster.Placement
  alias Malachi.Keyspace
  alias Malachi.Log.Record
  alias Malachi.Metadata

  # 64 MiB: the active segment asks to roll once it reaches this many encoded bytes (soft threshold,
  # checked at produce-batch boundaries, see `tally_bytes/3`).
  @default_segment_max_bytes 64 * 1024 * 1024

  @typedoc "The broker's view of the open (unsealed) segment of a range."
  @type active_segment :: %{
          id: Metadata.segment_id(),
          start_offset: non_neg_integer(),
          bytes: non_neg_integer(),
          replica_set: [Metadata.broker()]
        }

  @typedoc "Maps a range id to the `{first_offset, last_offset}` a produce placed there."
  @type placements :: %{Metadata.range_id() => {non_neg_integer(), non_neg_integer()}}

  @typedoc """
  A segment whose fence the caller still owes before the control plane can seal it.

  A map of rolls rather than a set of range ids, because `record_seal/5` can fail (an ra timeout) after
  the cached segment is already gone, and the retry needs the segment id, its primary and its start
  offset. A set would lose them. `fence_sent_at` is when its fence was last sent (`fences_to_send/3`), in
  monotonic milliseconds, or `nil` before the first send.
  """
  @type roll :: %{
          range_id: Metadata.range_id(),
          segment_id: Metadata.segment_id(),
          primary: Metadata.broker(),
          start_offset: non_neg_integer(),
          fence_sent_at: integer() | nil
        }

  @typedoc "Appends/replicates a batch to a segment, returning the last offset stored."
  @type replicate_fun ::
          (Metadata.broker(), Metadata.segment_id(), [Metadata.broker()], non_neg_integer(), [Record.t()] ->
             {:ok, non_neg_integer()} | {:error, term()})

  @typedoc "Reads up to `max_records` of a segment from `offset`."
  @type read_fun ::
          (Metadata.broker(), Metadata.segment_id(), non_neg_integer(), pos_integer() ->
             {:ok, [Record.t()]} | :eof | {:error, term()})

  @typedoc """
  Routes a metadata command to the vnode owning `topic_name` and applies it there, returning
  `{dsrsm, reply}`: the `Malachi.Cluster.DSRSM.command/3` shape. The default `&DSRSM.command/3`
  applies in memory; a Raft-backed variant (see `Malachi.BrokerServer`) injects an authoritative
  apply through `Malachi.Cluster.ReplicatedMetadata` into the routed vnode.
  """
  @type command_fun :: (DSRSM.t(), Metadata.topic_name(), Metadata.command() -> {DSRSM.t(), term()})

  @type t :: %__MODULE__{
          dsrsm: DSRSM.t(),
          command_fun: command_fun(),
          brokers: [Metadata.broker()],
          replication_factor: pos_integer(),
          segment_max_bytes: pos_integer(),
          segments: %{Metadata.range_id() => active_segment()},
          segment_seq: %{Metadata.range_id() => non_neg_integer()},
          offsets: %{Metadata.range_id() => non_neg_integer()},
          rolling: %{Metadata.range_id() => roll()},
          fencing: %{Metadata.range_id() => %{segment_id: Metadata.segment_id(), sent_at: integer()}}
        }

  defstruct dsrsm: nil,
            command_fun: nil,
            brokers: nil,
            replication_factor: 1,
            segment_max_bytes: @default_segment_max_bytes,
            # Rack/DC-aware placement: `spread_by` is the global attribute key to spread replicas over
            # (nil = off; a topic's storage policy can override it), `broker_attributes` maps each
            # broker to its attributes (refreshed from membership).
            spread_by: nil,
            broker_attributes: %{},
            # Failure-domain guarantee: `min_domains` distinct `spread_by` values a segment's replica set
            # must span; `placement_policy` :hard rejects a segment that cannot reach it (produce fails
            # fast), :soft (default) places best-effort. nil min_domains = no requirement.
            min_domains: nil,
            placement_policy: :soft,
            segments: %{},
            segment_seq: %{},
            offsets: %{},
            # Ranges whose active segment crossed `:segment_max_bytes` and still owes a fence. A REQUEST,
            # not a barrier: the range keeps taking writes, and whatever lands meanwhile is inside the end
            # the fence eventually reports. See `due_rolls/1` and `record_seal/5`.
            rolling: %{},
            # Fences this frontend has SENT and not yet seen answered, by range. Apart from `rolling` on
            # purpose: a refusal clears the roll (`forget_sealed/4`), and it is exactly then that a produce
            # must still be recognized as racing this frontend's own fence. See `awaiting_fence?/5`.
            fencing: %{}

  @doc """
  Opens an empty broker.

  ## Options
    * `:brokers` - the broker set segment replicas are placed on (default `[node()]`). Must be
      non-empty.
    * `:replication_factor` - replicas per segment, clamped to the broker count (default `1`).
    * `:segment_max_bytes` - the active segment seals once it reaches this many encoded bytes
      (default 64 MiB).
    * `:command_fun` - how metadata mutations are routed and applied (default `&DSRSM.command/3`, an
      in-memory single vnode); pass a Raft-backed function to make the metadata authoritative.
    * `:dsrsm` - the initial sharded metadata view (default a single-vnode `DSRSM.single/1`), e.g.
      seeded from a replicated cluster.
  """
  @spec open(keyword()) :: {:ok, t()}
  def open(opts \\ []) do
    brokers = Keyword.get(opts, :brokers, [node()])
    replication_factor = Keyword.get(opts, :replication_factor, 1)
    segment_max_bytes = Keyword.get(opts, :segment_max_bytes, @default_segment_max_bytes)

    validate_policy!(brokers, replication_factor, segment_max_bytes)

    {:ok,
     %__MODULE__{
       dsrsm: Keyword.get(opts, :dsrsm) || DSRSM.single(),
       command_fun: Keyword.get(opts, :command_fun, &DSRSM.command/3),
       brokers: brokers,
       replication_factor: replication_factor,
       segment_max_bytes: segment_max_bytes,
       spread_by: Keyword.get(opts, :spread_by),
       broker_attributes: Keyword.get(opts, :broker_attributes, %{}),
       min_domains: Keyword.get(opts, :min_domains),
       placement_policy: Keyword.get(opts, :placement_policy, :soft)
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
    {dsrsm, reply} = apply_metadata(broker, {:create_topic, name, keyspace_bits})
    {%{broker | dsrsm: dsrsm}, reply}
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
    case DSRSM.get_topic(broker.dsrsm, topic) do
      nil -> {broker, {:error, :no_such_topic}}
      topic_meta -> route_and_replicate(broker, topic, topic_meta, records, replicate_fun)
    end
  end

  @typedoc """
  One replication call a planned produce still owes: everything `replicate_fun` would get, plus the
  range and expected offsets.
  """
  @type dispatch :: %{
          range_id: Metadata.range_id(),
          primary: term(),
          segment_id: term(),
          replica_set: [Metadata.broker()],
          base_offset: non_neg_integer(),
          records: [Record.t()],
          first: non_neg_integer(),
          last: non_neg_integer(),
          count: pos_integer()
        }

  @doc """
  Like `produce/4`, but PLANS the replication instead of executing it: routes each record to its range,
  opens segments as needed, commits the offsets optimistically, and returns the replication dispatches
  for the caller to execute (typically asynchronously, so a broker frontend never blocks its loop on
  replication). Returns `{broker, {:ok, placements, dispatches}}` or `{broker, {:error, reason}}`; on
  error NOTHING was dispatched, so the original broker is returned untouched (stronger atomicity than
  the executing variant, which may have committed earlier groups).

  Committing before durability is what makes the frontend non-blocking, and it is safe because clients
  never see offsets (positions travel in opaque cursors): a dispatch that later fails burns its
  offsets, the client gets the error and retries, and the local counters stay in lockstep with the
  primary's log, which appended the batch even when its quorum did not close.
  """
  @spec produce_plan(t(), Metadata.topic_name(), [Record.t()]) ::
          {t(), {:ok, placements(), [dispatch()]} | {:error, term()}}
  def produce_plan(%__MODULE__{} = broker, topic, records) when is_list(records) do
    case DSRSM.get_topic(broker.dsrsm, topic) do
      nil ->
        {broker, {:error, :no_such_topic}}

      topic_meta ->
        active_ranges = DSRSM.active_ranges_of_topic(broker.dsrsm, topic)

        case group_by_owning_range(records, active_ranges, topic_meta.keyspace_size) do
          {:error, _reason} = error -> {broker, error}
          {:ok, grouped} -> plan_groups(broker, grouped)
        end
    end
  end

  defp plan_groups(original, grouped) do
    result =
      Enum.reduce_while(grouped, {original, %{}, []}, fn {range_id, reversed_records},
                                                         {broker, placements, dispatches} ->
        records = Enum.reverse(reversed_records)

        case plan_group(broker, range_id, records, placements, dispatches) do
          {:ok, broker, placements, dispatches} -> {:cont, {broker, placements, dispatches}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)

    case result do
      # Nothing was executed yet, so a failed plan rolls the WHOLE produce back to the original broker.
      {:error, reason} -> {original, {:error, reason}}
      {broker, placements, dispatches} -> {broker, {:ok, placements, Enum.reverse(dispatches)}}
    end
  end

  @doc """
  Seeds a range's recovered bookkeeping after a restart: the next offset to hand out and the floor for
  the segment sequence counter. The in-memory `offsets`/`segment_seq` maps start empty on boot, and
  without seeding every read of pre-restart data would clamp to `:eof` at offset 0 even though the
  records are durable on disk and the metadata survived (the failure the chaos harness caught).
  Both merges are monotone (`max`), so re-seeding never rewinds live state.
  """
  @spec seed_range_state(t(), Metadata.range_id(), non_neg_integer(), non_neg_integer()) :: t()
  def seed_range_state(%__MODULE__{} = broker, range_id, next_offset, min_seq) do
    %{
      broker
      | offsets: Map.update(broker.offsets, range_id, next_offset, &max(&1, next_offset)),
        segment_seq: Map.update(broker.segment_seq, range_id, min_seq, &max(&1, min_seq))
    }
  end

  @doc """
  Adopts the primary-assigned end offset of a dispatched batch into the local bookkeeping, when the
  dispatch still describes the range's write head. The range's primary serializes appends and assigns
  the REAL offsets (the NorthGuard invariant), so when several broker frontends produce to the same
  range their interleaving makes a frontend's precomputed offsets diverge from what the primary
  assigned; the frontend then adopts the primary's truth instead of failing. The counter only moves
  forward (`max`), so this frontend's own in-flight batches keep their reservations; a later collision
  just adopts again.

  Guarded on the segment id: an answer that arrives after the range's head was closed describes a
  segment that no longer owns the range's end, and raising the counter past the fenced edge would make
  the next `register_segment` fail with `:segment_overlap` and block the range.
  """
  @spec adopt_offsets(t(), Metadata.range_id(), Metadata.segment_id(), non_neg_integer()) :: t()
  def adopt_offsets(%__MODULE__{} = broker, range_id, segment_id, actual_last) do
    case Map.get(broker.segments, range_id) do
      %{id: ^segment_id} ->
        %{broker | offsets: Map.update(broker.offsets, range_id, actual_last + 1, &max(&1, actual_last + 1))}

      _closed_or_rolled ->
        broker
    end
  end

  defp plan_group(broker, range_id, records, placements, dispatches) do
    case ensure_segment(broker, range_id) do
      {:error, reason} ->
        {:error, reason}

      {:ok, opened, segment} ->
        first = next_offset(opened, range_id)
        count = length(records)
        last = first + count - 1

        # Offsets first, then bytes and the roll threshold: the reservation must be taken from the
        # counter before a roll request can be recorded against the segment it lands in.
        committed =
          opened
          |> reserve_offsets(range_id, count)
          |> tally_bytes(range_id, batch_bytes(records))

        dispatch = %{
          range_id: range_id,
          primary: primary(segment),
          segment_id: segment.id,
          replica_set: segment.replica_set,
          base_offset: segment.start_offset,
          records: records,
          first: first,
          last: last,
          count: count
        }

        {:ok, committed, Map.put(placements, range_id, {first, last}), [dispatch | dispatches]}
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
      :eof ->
        :eof

      {:ok, segment} ->
        # Never below the segment's base: `locate_segment/3` steps up when the requested offset falls
        # in a hole, and asking a segment for an offset it never held is not a question it can answer.
        start = max(offset, segment.start_offset)

        case read_budget(segment, start, max_records) do
          :eof -> :eof
          budget -> read_fun.(primary(segment), segment.id, start, budget)
        end
    end
  end

  # How many records a read starting at `start` may take from `segment`.
  #
  # An active segment is the write head: nothing above its start belongs to anyone else, so the
  # caller's budget stands. A sealed segment's length is the control plane's truth about it, and the
  # store can hold more: a stale frontend keeps appending through a primary that never heard of the
  # seal, so the log grows past the offset where the NEXT segment was opened. Serving that surplus
  # hands out offsets the next segment owns, and since the consume cursor moves to the last offset
  # served plus one, it lands past the next segment's head without ever delivering it. The surplus
  # itself sits at the top of the page, where the cursor never comes back for it. So the read is
  # capped at the sealed edge: records a store holds beyond the sealed length are, by contract, not
  # part of the log, and the fence on the write side is what keeps them from being acknowledged.
  defp read_budget(%{length: length} = segment, start, max_records) when is_integer(length) do
    remaining = sealed_end(segment) - start

    # A non-positive remainder means the read starts at or above the sealed edge, which for a segment
    # `locate_segment/3` handed back can only be a zero-length seal reached through
    # `next_segment_above/2` (which steps over a hole without asking `serves?/2`). An ordinary outcome:
    # a split or a failover can close a segment that was registered and never written. Kept as :eof
    # rather than a zero or negative budget, because a read_fun given one would answer with whatever it
    # holds and undo the cap.
    if remaining > 0, do: min(max_records, remaining), else: :eof
  end

  defp read_budget(_active_segment, _start, max_records), do: max_records

  @doc """
  Splits a range: the control plane seals the parent and creates two children. Returns
  `{broker, {:ok, left_id, right_id}}` or a `Metadata` error.

  Metadata only. The caller must FENCE the parent's active segment and record its seal (through
  `active_roll/2` and `record_seal/5`) BEFORE calling, because a segment's sealed length has to be what
  closing it answered: sealing it here, from this frontend's counter, is the guess this design removes.
  Fencing first is also what closes the write half of the split gap, since a node that has not seen the
  split can no longer get a record into the parent once its store is fenced.
  """
  @spec split_range(t(), Metadata.range_id()) :: {t(), term()}
  def split_range(%__MODULE__{} = broker, range_id) do
    case apply_metadata(broker, {:split_range, range_id}) do
      {dsrsm, {:ok, _left, _right} = reply} ->
        {%{broker | dsrsm: dsrsm}, reply}

      {_dsrsm, {:error, _reason} = error} ->
        {broker, error}
    end
  end

  @doc """
  Merges two buddy ranges: the control plane seals both and creates a child. Returns
  `{broker, {:ok, child_id}}` or a `Metadata` error.

  Metadata only, for the same reason as `split_range/2`: the caller fences and records both parents'
  active segments first.
  """
  @spec merge_ranges(t(), Metadata.range_id(), Metadata.range_id()) :: {t(), term()}
  def merge_ranges(%__MODULE__{} = broker, range_id_a, range_id_b) do
    case apply_metadata(broker, {:merge_ranges, range_id_a, range_id_b}) do
      {dsrsm, {:ok, _child} = reply} ->
        {%{broker | dsrsm: dsrsm}, reply}

      {_dsrsm, {:error, _reason} = error} ->
        {broker, error}
    end
  end

  @doc "The ids of a topic's active ranges (those that currently tile the keyspace)."
  @spec active_range_ids(t(), Metadata.topic_name()) :: [Metadata.range_id()]
  def active_range_ids(%__MODULE__{} = broker, topic) do
    broker.dsrsm |> DSRSM.active_ranges_of_topic(topic) |> Enum.map(& &1.id)
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
    {dsrsm, _reply} = apply_metadata(broker, command)
    broker = %{broker | dsrsm: dsrsm}
    update_active_replica_set(broker, segment_id, replica_set)
  end

  # Failover seals an active segment whose primary died (`Malachi.Cluster.Failover`), so the cached
  # entry must go with it: the store refuses an append to a fenced segment, and a broker still holding
  # it in `segments` would keep routing produces at a segment that can no longer take them.
  #
  # The counter is seated from the length the metadata ended up with (not from the command's, which a
  # conflicting re-seal can reject), and any roll this frontend owed for the same segment is cleared.
  # That is how a failover seal performed on another node unblocks a frontend caught mid-roll: without
  # it the frontend keeps re-fencing a segment somebody else already closed.
  defp apply_replica_command({:seal_segment, segment_id, _length, _bytes, _at} = command, broker) do
    {dsrsm, _reply} = apply_metadata(broker, command)
    broker = %{broker | dsrsm: dsrsm}
    {range_id, _seq} = segment_id

    case DSRSM.get_segment(broker.dsrsm, topic_of_segment(segment_id), segment_id) do
      %{state: :sealed, start_offset: start_offset, length: length} when is_integer(length) ->
        forget_sealed(broker, range_id, segment_id, start_offset + length)

      # The segment is gone or the seal did not land: there is no edge to seat the counter at, so only
      # the cache eviction (which is safe on its own) happens.
      _no_sealed_extent ->
        forget_active_segment(broker, segment_id)
    end
  end

  defp apply_replica_command(command, broker) do
    {dsrsm, _reply} = apply_metadata(broker, command)
    %{broker | dsrsm: dsrsm}
  end

  # Drops the range's cached active segment when the seal targets exactly it. Guarded on the id, so a
  # seal of some older segment of the same range (a late command, a retry) cannot evict the segment
  # that is currently open and take writes down with it.
  defp forget_active_segment(broker, {range_id, _seq} = segment_id) do
    case Map.get(broker.segments, range_id) do
      %{id: ^segment_id} -> %{broker | segments: Map.delete(broker.segments, range_id)}
      _other -> broker
    end
  end

  defp update_active_replica_set(broker, {range_id, _seq} = segment_id, replica_set) do
    case Map.get(broker.segments, range_id) do
      %{id: ^segment_id} = active ->
        %{broker | segments: Map.put(broker.segments, range_id, %{active | replica_set: replica_set})}

      _other ->
        broker
    end
  end

  @doc """
  The fences this broker owes: ranges whose active segment crossed `:segment_max_bytes`.

  The caller fences each one's primary and hands the answer back through `record_seal/5`. Until it
  does, the range stays writable and the segment simply overshoots its soft threshold.
  """
  @spec due_rolls(t()) :: [roll()]
  def due_rolls(%__MODULE__{rolling: rolling}), do: Map.values(rolling)

  @doc """
  The owed rolls whose fence the caller should send now, marked as sent at `now_ms`: each roll whose fence
  has not been sent yet, and each whose last send is at least `retry_after_ms` old (a fence that went
  unanswered, from a primary that is slow, mute or gone). Pure; the caller sends the fences and hands each
  answer to `record_fence/5`.

  The mark is what keeps a busy range from fanning out a fence per produce while one is in flight, and
  the window is what keeps a lost fence from leaving the roll owed forever.

  A roll is never sealed without its fence. It used to be, from this frontend's counter, and a frontend
  that had not yet seen that seal kept appending to the segment through a primary that knew nothing of it:
  acknowledged records landed above the recorded edge, stored and unreachable.
  """
  @spec fences_to_send(t(), integer(), non_neg_integer()) :: {t(), [roll()]}
  def fences_to_send(%__MODULE__{} = broker, now_ms, retry_after_ms) do
    {broker, due} =
      Enum.reduce(broker.rolling, {broker, []}, fn {range_id, roll}, {acc, due} ->
        if roll.fence_sent_at == nil or now_ms - roll.fence_sent_at >= retry_after_ms do
          sent = %{roll | fence_sent_at: now_ms}
          fencing = Map.put(acc.fencing, range_id, %{segment_id: roll.segment_id, sent_at: now_ms})
          {%{acc | rolling: Map.put(acc.rolling, range_id, sent), fencing: fencing}, [sent | due]}
        else
          {acc, due}
        end
      end)

    {broker, Enum.reverse(due)}
  end

  @doc """
  Records the answer to a produce roll's fence: `end_offset` and `byte_size` are what closing the segment
  answered, so they become its sealed length (`record_seal/5`).

  Applied while the control plane still calls the segment ACTIVE, and applied then even if this frontend
  no longer owes the roll. That second half matters: a produce this frontend sent after the fence is
  refused with `{:sealed, end_offset}`, which clears the roll (`forget_sealed/4`) before the fence's own
  answer arrives, and dropping the answer then would leave the store fenced under a segment the metadata
  still calls active, refusing every write to the range until a heal pass reconciled it. It is safe for the
  same reason it is needed: while the segment is active its range has no successor, so nothing can have
  been written above the end this seats the counter at.

  Once the segment is sealed (a failover, or another frontend's roll, closed it first), its length is not
  this answer's to say, and the roll is only cleared.
  """
  @spec record_fence(t(), roll(), non_neg_integer(), non_neg_integer(), non_neg_integer()) ::
          {t(), :ok | {:error, term()}}
  def record_fence(%__MODULE__{} = broker, roll, end_offset, byte_size, sealed_at) do
    case DSRSM.get_segment(broker.dsrsm, topic_of_segment(roll.segment_id), roll.segment_id) do
      %{state: :active} ->
        record_seal(broker, roll, end_offset, byte_size, sealed_at)

      _sealed_or_gone ->
        {broker |> clear_roll(roll.range_id, roll.segment_id) |> clear_fencing(roll.range_id, roll.segment_id), :ok}
    end
  end

  @doc """
  Stops awaiting the fence of `roll` after it FAILED. The roll stays owed (`fences_to_send/3` resends it),
  but a produce refused meanwhile is no longer taken for this frontend's own roll overtaking it.
  """
  @spec forget_fence(t(), roll()) :: t()
  def forget_fence(%__MODULE__{} = broker, roll), do: clear_fencing(broker, roll.range_id, roll.segment_id)

  @doc """
  Whether this frontend sent the fence of `segment_id`, the write head of `range_id`, less than `window_ms`
  ago and has not recorded its answer yet.

  A `{:sealed, _}` refusal for that segment is then this frontend's own roll overtaking one of its
  produces: the primary applied the fence (the refusal proves it) and the answer is on its way. The caller
  can hold the produce until that answer opens the successor instead of failing it. The window bounds the
  wait for an answer that was lost.
  """
  @spec awaiting_fence?(t(), Metadata.range_id(), Metadata.segment_id(), integer(), non_neg_integer()) :: boolean()
  def awaiting_fence?(%__MODULE__{fencing: fencing}, range_id, segment_id, now_ms, window_ms) do
    case Map.get(fencing, range_id) do
      %{segment_id: ^segment_id, sent_at: sent_at} -> now_ms - sent_at < window_ms
      _other -> false
    end
  end

  @doc """
  A range of `topic` whose fence this frontend is still waiting on (see `awaiting_fence?/5`), or `nil`: for a
  caller that learns only that a produce to `topic` was refused, not which of its segments refused it. The
  lowest such range, so the answer is deterministic.
  """
  @spec range_awaiting_fence(t(), Metadata.topic_name(), integer(), non_neg_integer()) ::
          Metadata.range_id() | nil
  def range_awaiting_fence(%__MODULE__{fencing: fencing}, topic, now_ms, window_ms) do
    fencing
    |> Enum.filter(fn {range_id, %{sent_at: sent_at}} ->
      topic_of_range(range_id) == topic and now_ms - sent_at < window_ms
    end)
    |> Enum.map(&elem(&1, 0))
    |> Enum.min(fn -> nil end)
  end

  @doc """
  The roll for one range's write head, on demand (a split or a merge retiring the range), or `:none`
  when the range has none. Does not mutate: the caller fences and records in one step, so nothing is
  owed if it never gets that far.

  The head comes from the CONTROL PLANE, and falls back to this frontend's cache only when the metadata
  has nothing to say. Reading it from the cache instead was issue #41: a split is performed on whichever
  node the operator reached, which is usually NOT the node producing to the range, so a fence
  conditional on the caller having written there fences nothing on exactly the node where it matters,
  and reports success. What that leaves behind is not a bounded window: the split seals the RANGE and
  not its segments, and nothing else ever closes an active segment on a sealed range (failover only
  seals segments whose primary is dead, retention and healing only touch sealed ones), so every
  frontend still caching it keeps appending to the parent forever, and a child reads those records
  ahead of its own.
  """
  @spec active_roll(t(), Metadata.range_id()) :: roll() | :none
  def active_roll(%__MODULE__{} = broker, range_id) do
    case registered_active_segment(broker, range_id) || Map.get(broker.segments, range_id) do
      nil -> :none
      active -> roll_of(range_id, active)
    end
  end

  # The range's write head as the metadata records it. A segment with an empty replica set is skipped
  # rather than returned: there is no primary to fence, and `primary/1` would raise inside the broker
  # loop. Shared with `adopt_active_segment/2` so the fence and the adopt cannot disagree about which
  # segment a range's writes belong to.
  @spec registered_active_segment(t(), Metadata.range_id()) :: map() | nil
  defp registered_active_segment(%__MODULE__{} = broker, range_id) do
    broker.dsrsm
    |> DSRSM.segments_of_range(topic_of_range(range_id), range_id)
    |> Enum.find(&match?(%{state: :active, replica_set: [_ | _]}, &1))
  end

  @doc """
  Records a fenced seal. `end_offset` and `byte_size` are what the fence ANSWERED, so the sealed length
  is a consequence of closing the segment rather than a measurement racing it.

  All or nothing: on `:ok` the metadata command landed, the cached active segment is dropped, the roll
  is cleared, and the range's next offset is SET to exactly `end_offset`; on any other reply the broker
  is returned untouched and the roll stays owed, so the next pass re-fences (idempotent, the same
  numbers) and re-issues the same command.

  Set, not `max`. A batch the fence refused, or one that never reached the primary at all, burned
  reserved offsets, so the local counter can sit ABOVE the fence's end; opening the next segment there
  would be rejected by the tiling rule and the range would wedge. The rewind is invisible to clients,
  because positions travel in opaque cursors and every burned offset belonged to a produce that
  returned an error. It is safe precisely because the fence is a latch: after it, nothing can land in
  this segment.

  A `{:already_sealed, existing}` conflict (a roll fence racing a failover seal) converges on the
  winner rather than wedging: this broker adopts `existing` as the length and reports `:ok`.
  """
  @spec record_seal(t(), roll(), non_neg_integer(), non_neg_integer(), non_neg_integer()) ::
          {t(), :ok | {:error, term()}}
  def record_seal(%__MODULE__{} = broker, roll, end_offset, byte_size, sealed_at) do
    start_offset = recorded_start_offset(broker, roll)
    segment = %{id: roll.segment_id, start_offset: start_offset}

    case apply_metadata(broker, Metadata.seal_command(segment, end_offset, byte_size, sealed_at)) do
      {dsrsm, :ok} ->
        {settle_roll(%{broker | dsrsm: dsrsm}, roll, end_offset), :ok}

      {dsrsm, {:error, {:already_sealed, existing}}} ->
        {settle_roll(%{broker | dsrsm: dsrsm}, roll, start_offset + existing), :ok}

      {_dsrsm, {:error, _reason} = error} ->
        {broker, error}

      {_dsrsm, other} ->
        {broker, {:error, {:unexpected_seal_reply, other}}}
    end
  end

  # The control plane's own start offset for the segment, which is what the successor's registration
  # will be checked against. `roll.start_offset` is the fallback for a segment the metadata no longer
  # carries (retention dropped it between the fence and the command).
  defp recorded_start_offset(broker, roll) do
    case DSRSM.get_segment(broker.dsrsm, topic_of_segment(roll.segment_id), roll.segment_id) do
      %{start_offset: start_offset} -> start_offset
      nil -> roll.start_offset
    end
  end

  # Drops the cached segment (guarded on the id, so a late seal cannot evict a newer one), clears the
  # roll and seats the range at the sealed edge.
  defp settle_roll(broker, roll, end_offset) do
    broker
    |> forget_active_segment(roll.segment_id)
    |> clear_roll(roll.range_id, roll.segment_id)
    |> clear_fencing(roll.range_id, roll.segment_id)
    |> put_offset(roll.range_id, end_offset)
  end

  @doc """
  Seats this frontend behind a seal somebody else performed (a `{:sealed, end_offset}` refusal): drops
  the cached active segment for `segment_id`, clears any roll naming it, and moves the range's next
  offset to `end_offset`, so the retry opens or adopts the successor exactly where this segment closed
  instead of racing `:segment_overlap`.

  Guarded on the id, like the seal command's own cache eviction: a refusal naming an older segment must
  not evict the one currently open.
  """
  @spec forget_sealed(t(), Metadata.range_id(), Metadata.segment_id(), non_neg_integer()) :: t()
  def forget_sealed(%__MODULE__{} = broker, range_id, segment_id, end_offset) do
    if names_range_head?(broker, range_id, segment_id) do
      broker
      |> forget_active_segment(segment_id)
      |> clear_roll(range_id, segment_id)
      |> put_offset(range_id, end_offset)
    else
      broker
    end
  end

  @doc """
  Drops cached active segments the control plane has already sealed, seating each range's next offset
  at the sealed edge. Called on every metadata refresh, right after `put_cache/3`.

  Only `apply_heal/2`, on the node running the heal coordinator, used to evict such a segment, so every
  other frontend kept routing produces at a segment the metadata had sealed and only learned otherwise
  when the store refused a batch. This makes the convergence level-triggered on every node instead,
  bounded by the reconcile interval.
  """
  @spec drop_stale_active_segments(t()) :: t()
  def drop_stale_active_segments(%__MODULE__{} = broker) do
    Enum.reduce(broker.segments, broker, fn {range_id, active}, acc ->
      case DSRSM.get_segment(acc.dsrsm, topic_of_segment(active.id), active.id) do
        %{state: :sealed, start_offset: start_offset, length: length} when is_integer(length) ->
          forget_sealed(acc, range_id, active.id, start_offset + length)

        _still_active_or_unknown ->
          drop_if_range_retired(acc, range_id)
      end
    end)
  end

  # Defence in depth for a fence that never happened: a segment the metadata still calls ACTIVE whose
  # RANGE has been retired by a split or a merge. That state should be unreachable, since the control
  # plane now refuses to retire a range with a write head, but if it is ever reached the segment is
  # never closed by anything (failover only seals segments whose primary is dead, retention and healing
  # only touch sealed ones), so a frontend caching it would append to a range that is already history
  # for as long as it lived. Dropping the cache bounds that to one refresh: the next produce routes to
  # the children, and any attempt to register on the retired range is refused authoritatively.
  #
  # A plain eviction, NOT `forget_sealed/4`: that segment's end is precisely what nobody knows here, and
  # moving the range's offset counter to a guess is the class of mistake the sealed length exists to
  # avoid.
  defp drop_if_range_retired(broker, range_id) do
    case DSRSM.get_range(broker.dsrsm, topic_of_range(range_id), range_id) do
      %{state: :sealed} -> %{broker | segments: Map.delete(broker.segments, range_id)}
      _active_or_unknown -> broker
    end
  end

  # Whether `segment_id` is what this frontend currently treats as the range's write head: the cached
  # active segment, or the segment a pending roll names when the cache is already gone.
  defp names_range_head?(broker, range_id, segment_id) do
    case {Map.get(broker.segments, range_id), Map.get(broker.rolling, range_id)} do
      {%{id: ^segment_id}, _roll} -> true
      {_cached, %{segment_id: ^segment_id}} -> true
      _other -> false
    end
  end

  defp clear_roll(broker, range_id, segment_id) do
    case Map.get(broker.rolling, range_id) do
      %{segment_id: ^segment_id} -> %{broker | rolling: Map.delete(broker.rolling, range_id)}
      _other -> broker
    end
  end

  # Guarded on the id like `clear_roll/3`: an answer for an older segment must not forget a newer fence.
  defp clear_fencing(broker, range_id, segment_id) do
    case Map.get(broker.fencing, range_id) do
      %{segment_id: ^segment_id} -> %{broker | fencing: Map.delete(broker.fencing, range_id)}
      _other -> broker
    end
  end

  defp put_offset(broker, range_id, offset), do: %{broker | offsets: Map.put(broker.offsets, range_id, offset)}

  @doc """
  The current metadata as one flat view: the union of the sharded vnodes (see
  `Malachi.Cluster.DSRSM.merged_metadata/1`), for whole-cluster consumers like retention and healing.
  """
  @spec metadata(t()) :: Metadata.t()
  def metadata(%__MODULE__{} = broker), do: DSRSM.merged_metadata(broker.dsrsm)

  @doc """
  Per-topic count of segments whose replica set spans fewer than `min_domains` distinct `spread_by`
  domains: the failure-domain diversity violations (`Malachi.Cluster.Placement.domain_violations/4`),
  keyed by topic. Empty when `spread_by` or `min_domains` is unset (nothing to check). This surfaces the
  HA degradation a `:soft` policy allows (under-diversified placements are kept, not rejected), for
  metrics/alerting.
  """
  @spec domain_violations(t()) :: %{String.t() => non_neg_integer()}
  def domain_violations(%__MODULE__{} = broker), do: domain_violations(broker, metadata(broker))

  @doc "Like `domain_violations/1` but over an already-computed metadata view (avoids a second merge)."
  @spec domain_violations(t(), Metadata.t()) :: %{String.t() => non_neg_integer()}
  def domain_violations(%__MODULE__{spread_by: key, min_domains: min} = broker, %Metadata{} = metadata)
      when not is_nil(key) and not is_nil(min) do
    metadata
    |> Placement.domain_violations(key, broker.broker_attributes, min)
    |> Enum.frequencies_by(&topic_of_segment/1)
  end

  def domain_violations(%__MODULE__{}, %Metadata{}), do: %{}

  @doc """
  Durably records a consumer `group`'s committed position (`offsets`, per range) for `topic`,
  through the control plane (Raft-backed when configured). Returns `{broker, reply}`.
  """
  @spec commit_offset(t(), Metadata.group(), Metadata.topic_name(), Metadata.offsets()) :: {t(), term()}
  def commit_offset(%__MODULE__{} = broker, group, topic, offsets) do
    {dsrsm, reply} = apply_metadata(broker, {:commit_offset, group, topic, offsets})
    {%{broker | dsrsm: dsrsm}, reply}
  end

  @doc """
  Removes a sealed `segment_id` from the control plane (retention), through the control plane
  (Raft-backed when configured). Returns `{broker, reply}` (`:ok`, or a `Metadata` error such as
  `:segment_active`/`:no_such_segment`).
  """
  @spec delete_segment(t(), Metadata.segment_id()) :: {t(), term()}
  def delete_segment(%__MODULE__{} = broker, segment_id) do
    {dsrsm, reply} = apply_metadata(broker, {:delete_segment, segment_id})
    {%{broker | dsrsm: dsrsm}, reply}
  end

  @doc "A consumer group's committed offsets for `topic` (empty if it never committed)."
  @spec committed_offsets(t(), Metadata.group(), Metadata.topic_name()) :: Metadata.offsets()
  def committed_offsets(%__MODULE__{} = broker, group, topic) do
    DSRSM.committed_offsets(broker.dsrsm, group, topic)
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

  @doc """
  Replaces the broker's local metadata cache, e.g. re-seeded from the authoritative ra clusters by a
  periodic refresh, which fills in vnodes not yet ready at boot and picks up writes made through other
  nodes. The ra log is the source of truth, so a refresh only ever moves the cache forward.

  `unreachable` names the vnodes the refresh could not read; those keep whatever view this broker
  already held, because the refresh represents them with an empty `Metadata` and installing it would
  delete live topics from the cache. See `Malachi.Cluster.DSRSM.retain_vnodes/3`.
  """
  @spec put_cache(t(), DSRSM.t(), [DSRSM.vnode_id()]) :: t()
  def put_cache(%__MODULE__{} = broker, %DSRSM{} = dsrsm, unreachable \\ []) do
    %{broker | dsrsm: DSRSM.retain_vnodes(dsrsm, broker.dsrsm, unreachable)}
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
    case DSRSM.get_range(broker.dsrsm, topic_of_range(range_id), range_id) do
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
  list. Loads the whole history into memory, for bounded/administrative use.
  """
  @spec read_history(t(), Metadata.range_id(), read_fun()) ::
          {:ok, [Record.t()]} | {:error, term()}
  def read_history(%__MODULE__{} = broker, range_id, read_fun) do
    drain_history(broker, range_id, :start, [], read_fun)
  end

  @doc """
  Reads up to `max_records` of `range_id`'s **cross-epoch** stream for live consumption: first the
  records its sealed ancestors hold for this range's keyspace slice (oldest first, in
  happens-before order), then the range's own records, and it **tails** the active range. Unlike
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
    case DSRSM.get_range(broker.dsrsm, topic_of_range(range_id), range_id) do
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
    active_ranges = DSRSM.active_ranges_of_topic(broker.dsrsm, topic)

    case group_by_owning_range(records, active_ranges, topic_meta.keyspace_size) do
      {:error, _reason} = error -> {broker, error}
      {:ok, grouped} -> replicate_groups(broker, grouped, replicate_fun)
    end
  end

  # Groups records by the active range that owns each key, or errors if any key is uncovered.
  #
  # Fast path: a topic with exactly ONE active range covering the whole keyspace (every topic starts
  # this way, and it is the dominant shape under load) needs no per-record work at all: every key lands
  # in that range, so the hash + linear range scan + map update per record are skipped. Under a
  # produce-heavy profile that per-record routing was about a third of the broker's CPU (eprof:
  # owning_range_id, Keyspace.position_of, find_value). The group's records are stored reversed, the
  # same shape the scanning path builds and the callers re-reverse.
  # An empty produce groups to nothing (and so places nothing), on every path.
  defp group_by_owning_range([], _active_ranges, _keyspace_size), do: {:ok, %{}}

  defp group_by_owning_range(records, [range] = active_ranges, keyspace_size) do
    if range.key_start == 0 and range.key_end == keyspace_size do
      {:ok, %{range.id => Enum.reverse(records)}}
    else
      scan_by_owning_range(records, active_ranges, keyspace_size)
    end
  end

  defp group_by_owning_range(records, active_ranges, keyspace_size) do
    scan_by_owning_range(records, active_ranges, keyspace_size)
  end

  # The linear per-record range scan below is DELIBERATE, not an oversight. Measured (compiled):
  # 33/48/56/72/105 ns per record for 1/2/4/8/16 active ranges, under 1 percent of one core at the
  # current peak rates, and the single-range fast path above already skips it for the dominant shape.
  # An O(1) buddy-block router was prototyped (a `{block_size, block_start} => range_id` map probed
  # once per distinct block size; buddy partitions have few sizes) and measured 67.5 ns per record:
  # it only beats the scan from R >= 16 active ranges. Revisit if split-heavy topics ever run with
  # R >= 16 under high produce load; until then the scan is simpler and just as fast.
  defp scan_by_owning_range(records, active_ranges, keyspace_size) do
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
      # Opening the segment (its register_segment command) failed, abort this group, keeping the
      # pre-open broker (no phantom segment); a retry re-places.
      {:error, reason} ->
        {:halt, {:error, reason, broker}}

      {:ok, opened, segment} ->
        replicate_to_segment(broker, opened, segment, range_id, records, placements, replicate_fun)
    end
  end

  defp replicate_to_segment(broker, opened, segment, range_id, records, placements, replicate_fun) do
    count = length(records)

    case replicate_fun.(primary(segment), segment.id, segment.replica_set, segment.start_offset, records) do
      # The primary serializes appends and assigns the REAL offsets (the NorthGuard invariant), so the
      # counter follows its answer rather than a local reservation. A matching answer and an interleaved
      # one were always the same case: adopting an answer that agrees is a no-op.
      {:ok, actual} ->
        committed =
          opened
          |> adopt_offsets(range_id, segment.id, actual)
          |> tally_bytes(range_id, batch_bytes(records))

        {:cont, {committed, Map.put(placements, range_id, {actual - count + 1, actual})}}

      # The write head moved under us (another frontend, or a failover, closed this segment). Seat this
      # frontend at the fenced end so the retry opens the successor instead of racing :segment_overlap.
      {:error, {:sealed, end_offset}} ->
        {:halt, {:error, {:sealed, end_offset}, forget_sealed(broker, range_id, segment.id, end_offset)}}

      # On failure, discard the just-opened segment by returning the pre-open broker (immutable
      # value = free rollback), so a failed produce leaves no phantom segment and a retry re-places.
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
        # Another frontend may have already registered this range's active segment in the shared
        # metadata: adopt it instead of racing a duplicate registration (which the metadata rejects
        # with :segment_exists, and which used to fail every produce from the losing frontends).
        case adopt_active_segment(broker, range_id) do
          {:ok, broker, segment} ->
            {:ok, broker, segment}

          :none ->
            case open_segment(broker, range_id, next_offset(broker, range_id)) do
              {:ok, broker} -> {:ok, broker, Map.fetch!(broker.segments, range_id)}
              {:error, reason} -> {:error, reason}
            end
        end
    end
  end

  # Adopts the range's active segment from the (shared) metadata into the local cache: id, replica set
  # and start offset come from the registrant; the byte tally restarts at zero (it only steers this
  # frontend's roll pressure, never the sealed length, which is the fence's answer). The seq counter
  # jumps past the adopted id so a later local roll never reuses it, and the offset counter jumps to at
  # least the segment's start (the primary-assigned results correct it further on the first produce).
  defp adopt_active_segment(broker, range_id) do
    case registered_active_segment(broker, range_id) do
      nil ->
        :none

      meta ->
        active = %{id: meta.id, start_offset: meta.start_offset, bytes: 0, replica_set: meta.replica_set}
        {_range, seq} = meta.id

        broker = %{
          broker
          | segments: Map.put(broker.segments, range_id, active),
            segment_seq: Map.update(broker.segment_seq, range_id, seq + 1, &max(&1, seq + 1)),
            offsets: Map.update(broker.offsets, range_id, meta.start_offset, &max(&1, meta.start_offset))
        }

        {:ok, broker, active}
    end
  end

  # Registers a new segment for `range_id` starting at `start_offset`, choosing its replica set
  # via the placement policy. The id `{range_id, seq}` is globally unique (range ids are) and the
  # per-range `seq` counter persists across seals, so a sealed segment's id is never reused.
  defp open_segment(broker, range_id, start_offset) do
    seq = Map.get(broker.segment_seq, range_id, 0)
    segment_id = {range_id, seq}

    # Under a :hard placement policy, a segment that cannot span :min_domains failure domains is rejected;
    # surface it so the produce fails fast instead of silently placing a non-HA replica set.
    case Placement.place(segment_id, broker.brokers, broker.replication_factor, place_opts(broker, range_id)) do
      {:ok, replica_set} -> register_segment(broker, range_id, segment_id, replica_set, start_offset, seq)
      {:error, reason} -> {:error, reason}
    end
  end

  defp register_segment(broker, range_id, segment_id, replica_set, start_offset, seq) do
    # The register command can fail when the metadata is Raft-backed (e.g. an ra timeout); surface
    # it so the produce aborts cleanly instead of crashing. The cache/seq are advanced only on :ok.
    case apply_metadata(broker, {:register_segment, range_id, segment_id, replica_set, start_offset}) do
      {dsrsm, :ok} ->
        active = %{id: segment_id, start_offset: start_offset, bytes: 0, replica_set: replica_set}

        broker = %{
          broker
          | dsrsm: dsrsm,
            segments: Map.put(broker.segments, range_id, active),
            segment_seq: Map.put(broker.segment_seq, range_id, seq + 1)
        }

        {:ok, broker}

      {dsrsm, {:error, reason}} when reason in [:segment_exists, :segment_overlap, :active_segment_exists] ->
        # Lost the registration race to another frontend: by id (`:segment_exists`), by offset
        # (`:segment_overlap`, this frontend derived a start below where the range already ends,
        # typically because a failover sealed the previous segment elsewhere), or because the range
        # already has a write head (`:active_segment_exists`, the rival registered first and its
        # segment is the one to write to). All three say the same thing, this view is stale, and all
        # three have the same remedy. The returned metadata may already carry the winner's segment:
        # adopt it and carry on; when it is still stale, surface the error and let the next produce
        # adopt after the periodic metadata refresh.
        broker = %{broker | dsrsm: dsrsm}

        case adopt_active_segment(broker, range_id) do
          {:ok, broker, _segment} -> {:ok, broker}
          :none -> {:error, reason}
        end

      {_dsrsm, {:error, reason}} ->
        {:error, reason}

      {_dsrsm, other} ->
        {:error, {:unexpected_register_reply, other}}
    end
  end

  # Reserves `count` offsets for a batch. Optimistic on the plan path (`produce_plan/3` needs a first
  # offset for the next group before the primary has answered); on the executing path the primary's
  # answer is adopted instead and this is not used, which is why the two are separate steps.
  defp reserve_offsets(broker, range_id, count) do
    %{broker | offsets: Map.put(broker.offsets, range_id, next_offset(broker, range_id) + count)}
  end

  # Advances the active segment's byte tally and REQUESTS a roll once it crosses the soft threshold.
  # The request does not close the segment: the range keeps taking writes, and whatever lands before
  # the fence answers is inside the end the fence reports. Sealing on the decision instead would have
  # to guess a length, which is the defect this design exists to remove.
  defp tally_bytes(broker, range_id, bytes) do
    active = Map.fetch!(broker.segments, range_id)
    active = %{active | bytes: active.bytes + bytes}
    broker = %{broker | segments: Map.put(broker.segments, range_id, active)}

    if active.bytes >= broker.segment_max_bytes, do: request_roll(broker, range_id), else: broker
  end

  # `Map.put_new`: a range whose fence has not answered yet must keep the roll it already owes, not a
  # fresh one built from a segment that may since have been rolled out of the cache.
  defp request_roll(broker, range_id) do
    case Map.fetch(broker.segments, range_id) do
      :error -> broker
      {:ok, active} -> %{broker | rolling: Map.put_new(broker.rolling, range_id, roll_of(range_id, active))}
    end
  end

  defp roll_of(range_id, active) do
    %{
      range_id: range_id,
      segment_id: active.id,
      primary: primary(active),
      start_offset: active.start_offset,
      fence_sent_at: nil
    }
  end

  # Applies a metadata mutation via the configured command function (in-memory by default, or
  # Raft-backed), routing it to the vnode owning the command's topic and threading the sharded cache
  # so multiple mutations in one operation see each other. Returns `{dsrsm, reply}`.
  # Routes each control-plane command to the vnode owning its topic, which every such command names
  # directly or embeds in its range id (`{topic, seq}`) or segment id (`{range_id, seq}`). The routing
  # layer rejects a command whose target topic disagrees with where it was routed (`:range_topic_mismatch`).
  defp apply_metadata(broker, command) do
    broker.command_fun.(broker.dsrsm, Metadata.command_target_topic(command), command)
  end

  # Query routing: a range/segment id embeds its topic, which is how a read is dispatched to the owning
  # vnode. The command path uses `Metadata.command_target_topic/1` instead (it takes a whole command).
  defp topic_of_range(range_id), do: elem(range_id, 0)
  defp topic_of_segment(segment_id), do: topic_of_range(elem(segment_id, 0))

  # Placement options for a new segment of `range_id`: spread it over the effective attribute using
  # the current broker attributes, else none (plain rendezvous ranking).
  defp place_opts(broker, range_id) do
    spread_opts =
      case effective_spread_by(broker, range_id) do
        nil -> []
        key -> [spread: {key, broker.broker_attributes}]
      end

    case broker.min_domains do
      nil -> spread_opts
      min -> spread_opts ++ [min_domains: min, policy: broker.placement_policy]
    end
  end

  # The spread attribute for `range_id`: its topic policy's `spread_by` when the policy sets that key
  # (an explicit nil opts the topic out of spreading), overriding the global; otherwise the broker's
  # global `spread_by`. Mirrors the per-topic retention resolution (a set key wins, nil included).
  defp effective_spread_by(broker, range_id) do
    case DSRSM.topic_policy(broker.dsrsm, topic_of_range(range_id)) do
      %{spread_by: spread_by} -> spread_by
      _no_policy_spread_by -> broker.spread_by
    end
  end

  defp next_offset(broker, range_id), do: Map.get(broker.offsets, range_id, 0)

  # The earliest offset still stored for a range: the smallest segment start_offset (0 if none).
  # Retention deletes the oldest segments: a contiguous prefix - so a consumer positioned below this
  # has had its data expired; read callers clamp up to it to skip transparently to what still exists.
  defp earliest_offset(broker, range_id) do
    broker.dsrsm
    |> DSRSM.segments_of_range(topic_of_range(range_id), range_id)
    |> Enum.map(& &1.start_offset)
    |> Enum.min(fn -> 0 end)
  end

  defp primary(%{replica_set: [primary | _]}), do: primary

  defp batch_bytes(records), do: Enum.reduce(records, 0, fn record, acc -> acc + Record.encoded_size(record) end)

  # The segment of `range_id` serving `offset` (range-relative), or `:eof` past the range's end.
  #
  # Segments are meant to tile the offset space, and the control plane enforces it on registration, so
  # normally this is just the last segment starting at or below `offset`. It does not assume it,
  # because a hole can appear after the fact: retention expires by `sealed_at`, and clocks that
  # disagree across nodes can expire a segment while its neighbours on both sides survive; an operator
  # can delete one directly. Landing in a hole and answering with the segment BEFORE it would return
  # :eof, and the consume cursor stops on :eof rather than advancing, so a single hole wedges every
  # consumer of the range permanently. Stepping up to the next segment instead loses only what is
  # already gone. Callers read the records' own offsets rather than counting from the one they asked
  # for, so a step forward stays consistent.
  defp locate_segment(broker, range_id, offset) do
    if offset < 0 or offset >= next_offset(broker, range_id) do
      :eof
    else
      segments = DSRSM.segments_of_range(broker.dsrsm, topic_of_range(range_id), range_id)

      below =
        segments
        |> Enum.filter(&(&1.start_offset <= offset))
        |> Enum.sort_by(& &1.start_offset, :desc)

      # Among the segments starting at or below `offset`, the one that actually SERVES it. Checking only
      # the greatest start misses a zero-length seal sharing a start with its successor: the zero-length
      # one can win the sort, serves nothing, and `next_segment_above/2` (strictly greater) cannot see
      # the successor, so the read answers :eof and the range's consumers wedge there permanently.
      case Enum.find(below, &serves?(&1, offset)) do
        nil -> next_segment_above(segments, offset)
        segment -> {:ok, segment}
      end
    end
  end

  # One offset past the last one a sealed segment owns. The read budget and `serves?/2` both fence
  # on this edge, so they share the arithmetic rather than each encoding it.
  defp sealed_end(%{start_offset: start, length: length}) when is_integer(length), do: start + length

  # Whether a segment's own extent covers `offset`. An active segment has no recorded length: it is
  # the write head, so everything from its start upward is its to serve.
  defp serves?(%{length: length} = segment, offset) when is_integer(length) do
    offset < sealed_end(segment)
  end

  defp serves?(_active_segment, _offset), do: true

  # The last record's assigned offset, falling back to counting from `requested` when the store did
  # not assign them (the fake stores in tests, and any reader that predates the assignment). The
  # fallback is the old arithmetic, so a store that assigns nothing behaves exactly as before.
  defp last_offset(records, requested) do
    case List.last(records) do
      %{offset: offset} when is_integer(offset) -> offset
      _unassigned -> requested + length(records) - 1
    end
  end

  # The nearest segment starting above `offset`, for a read that landed in a hole (or below the
  # earliest segment still stored, after retention dropped the front of the range).
  defp next_segment_above(segments, offset) do
    case segments |> Enum.filter(&(&1.start_offset > offset)) |> Enum.min_by(& &1.start_offset, fn -> nil end) do
      nil -> :eof
      segment -> {:ok, segment}
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
          {:ok, [_ | _] = records} ->
            # From the records' own offsets, not from the offset asked for, mirroring `consume_page/8`.
            # `read/5` can start ABOVE the request when `locate_segment/3` steps over a hole (a segment
            # dropped by retention or by an operator), and counting from the request puts the cursor
            # back inside that hole, so the same page is delivered again on every call.
            {:ok, filter_records(records, filter_range), {source_index, last_offset(records, source_offset) + 1}}

          # Before the catch-all, or a read error is silently taken for the end of a source: a failed
          # read is not an empty log, which is the mistake this whole area has already made once.
          {:error, _reason} = error ->
            error

          # `:eof` and an empty page are the same thing here: this source has nothing more to give at
          # this position. The empty case used to fall into the first clause, which returned the cursor
          # unchanged, and `drain_history/5` then asked the identical question forever. The trade is
          # explicit: an administrative history read now ends early on a transient empty page rather
          # than hanging, which is the same choice `:eof` already made beside it.
          _eof_or_empty ->
            read_history_page(broker, sources, source_index + 1, 0, max_records, read_fun)
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
        # From the records themselves, not from the offset that was asked for. The read can start
        # ABOVE the request when it steps over a hole left by retention or an operator, and counting
        # from the request would put the cursor back inside that hole, re-delivering what was already
        # returned and never getting past it.
        next_offset = last_offset(records, offset) + 1

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
