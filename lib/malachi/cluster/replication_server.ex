defmodule Malachi.Cluster.ReplicationServer do
  @moduledoc """
  The transport for segment replication: a `GenServer`, one per broker, that ships an active
  segment's records from the **primary** to its **followers** and acknowledges a write once a
  **quorum** has durably stored it.

  A broker is identified by this server's process reference (a registered name locally, or a
  `{name, node}` tuple across nodes — `GenServer.call/3` accepts both, so the same code path runs
  in-process for tests and over distributed Erlang in production). A segment's `replica_set` (from
  `Malachi.Cluster.Placement`) is a list of those references; the first is the primary.

  On `replicate/5` the primary appends the batch to its local copy, fans out to the followers
  concurrently, and feeds each ack into a `Malachi.Cluster.ReplicaTracker` to compute the commit
  offset. A segment's log opens at the segment's `base_offset` (its first range-relative offset),
  so the offsets of a range's segments are contiguous rather than restarting at zero per segment. The call returns `{:ok, last_offset}` once a quorum (the primary plus enough followers)
  has the batch — tolerating up to ⌊(N-1)/2⌋ slow or unreachable followers — or `{:error,
  :no_quorum}` otherwise. Both the primary and the followers `fsync` before counting toward the
  quorum, so "committed" means "durable on a majority".

  Scope (first slice): the active segment's happy path with quorum tolerance. A follower that has
  fallen behind replies `{:error, :out_of_sync}` and simply does not count toward the quorum;
  catch-up/backfill, sealed-segment replication, and primary failover are later slices.
  """

  use GenServer

  alias Malachi.Cluster.ReplicaTracker
  alias Malachi.Log

  @follow_timeout 5_000

  @typep state :: %{
           ref: term(),
           directory: Path.t(),
           log_opts: keyword(),
           logs: %{term() => Log.t()},
           trackers: %{term() => ReplicaTracker.t()}
         }

  @doc """
  Starts a replication server.

  ## Options
    * `:name` (optional) - the broker reference this server is registered under and known by in
      replica sets. When omitted, the server is unregistered and its reference is its pid.
    * `:directory` (required) - where replicated segment logs are stored.
    * any remaining options are forwarded to each segment's `Malachi.Log`.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    gen_server_opts =
      case Keyword.fetch(opts, :name) do
        {:ok, name} -> [name: name]
        :error -> []
      end

    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @doc """
  Replicates `records` for `segment_id` across `replica_set`, called on the primary (the first
  broker of the set). `base_offset` is the segment's first offset; it is used only when the
  segment's log is opened for the first time, so a segment's offsets continue its range
  (`start_offset, start_offset + 1, ...`) rather than restarting at zero.

  Returns `{:ok, last_offset}` once a quorum has the batch durably, `{:error, :no_quorum}` if too
  few replicas acked, `{:error, :not_primary}` if this server is not the set's primary, or
  `{:error, :empty}` for an empty batch.
  """
  @spec replicate(term(), term(), [term()], non_neg_integer(), [Malachi.Log.Record.t()]) ::
          {:ok, non_neg_integer()}
          | {:error, :no_quorum | :not_primary | :empty | :empty_replica_set}
  def replicate(primary, segment_id, replica_set, base_offset, records) do
    GenServer.call(
      primary,
      {:replicate, segment_id, replica_set, base_offset, records},
      @follow_timeout + 10_000
    )
  end

  @doc "Reads up to `max_records` records of `segment_id` stored on this server, from `offset`."
  @spec read(term(), term(), non_neg_integer(), pos_integer()) ::
          {:ok, [Malachi.Log.Record.t()]} | :eof | {:error, term()}
  def read(ref, segment_id, offset, max_records) do
    GenServer.call(ref, {:read, segment_id, offset, max_records})
  end

  # --- GenServer ---

  @impl true
  def init(opts) do
    # When unregistered, the server's reference is its own pid (set in replica sets by the caller).
    ref = Keyword.get(opts, :name) || self()
    directory = Keyword.fetch!(opts, :directory)
    File.mkdir_p!(directory)

    state = %{
      ref: ref,
      directory: directory,
      log_opts: Keyword.drop(opts, [:name, :directory]),
      logs: %{},
      trackers: %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:replicate, _segment_id, _replica_set, _base_offset, []}, _from, state) do
    {:reply, {:error, :empty}, state}
  end

  def handle_call({:replicate, _segment_id, [], _base_offset, _records}, _from, state) do
    {:reply, {:error, :empty_replica_set}, state}
  end

  def handle_call({:replicate, segment_id, replica_set, base_offset, records}, _from, state) do
    if hd(replica_set) == state.ref do
      do_replicate(state, segment_id, replica_set, base_offset, records)
    else
      {:reply, {:error, :not_primary}, state}
    end
  end

  def handle_call({:follow, segment_id, expected_first, records}, _from, state) do
    # A new follower segment opens at the batch's first offset, so its offsets match the primary's.
    {state, log} = fetch_or_open(state, segment_id, expected_first)

    if log.next_offset == expected_first do
      {log, _first, last} = append_durably(log, records)
      {:reply, {:ok, last}, put_log(state, segment_id, log)}
    else
      # The follower is behind (missed an earlier batch); catch-up is a later slice.
      {:reply, {:error, :out_of_sync}, state}
    end
  end

  def handle_call({:read, segment_id, offset, max_records}, _from, state) do
    case Map.fetch(state.logs, segment_id) do
      :error -> {:reply, :eof, state}
      {:ok, log} -> {:reply, Log.read(log, offset, max_records), state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.logs, fn {_segment_id, log} -> Log.close(log) end)
    :ok
  end

  # --- internals ---

  @spec do_replicate(state(), term(), [term()], non_neg_integer(), [Malachi.Log.Record.t()]) ::
          {:reply, {:ok, non_neg_integer()} | {:error, :no_quorum}, state()}
  defp do_replicate(state, segment_id, replica_set, base_offset, records) do
    # Distinct replicas, and never the primary itself among the followers — otherwise the server
    # would synchronously call itself (deadlock). The ack math also assumes distinct replicas.
    replica_set = Enum.uniq(replica_set)
    followers = replica_set -- [state.ref]

    {state, log} = fetch_or_open(state, segment_id, base_offset)
    {log, first, last} = append_durably(log, records)
    state = put_log(state, segment_id, log)

    tracker = tracker_for(state, segment_id, replica_set)
    {:ok, tracker} = ReplicaTracker.ack(tracker, state.ref, last)

    tracker =
      followers
      |> gather_acks(segment_id, first, records)
      |> Enum.reduce(tracker, fn {follower, offset}, acc ->
        {:ok, acc} = ReplicaTracker.ack(acc, follower, offset)
        acc
      end)

    state = put_in(state.trackers[segment_id], tracker)
    reply = if ReplicaTracker.committed?(tracker, last), do: {:ok, last}, else: {:error, :no_quorum}
    {:reply, reply, state}
  end

  # Reuses the segment's tracker (so acks accumulate across batches), rebuilding it if the replica
  # set changed.
  defp tracker_for(state, segment_id, replica_set) do
    case Map.get(state.trackers, segment_id) do
      %ReplicaTracker{replica_set: ^replica_set} = tracker -> tracker
      _ -> ReplicaTracker.new(replica_set)
    end
  end

  # Calls each follower concurrently; keeps only those that durably stored the batch. A slow,
  # unreachable, or out-of-sync follower is simply dropped (it does not count toward the quorum).
  defp gather_acks(followers, segment_id, expected_first, records) do
    followers
    |> Task.async_stream(
      fn follower ->
        try do
          {follower, GenServer.call(follower, {:follow, segment_id, expected_first, records}, @follow_timeout)}
        catch
          :exit, _ -> {follower, {:error, :unreachable}}
        end
      end,
      timeout: @follow_timeout + 1_000,
      on_timeout: :kill_task
    )
    |> Enum.flat_map(fn
      {:ok, {follower, {:ok, offset}}} -> [{follower, offset}]
      _ -> []
    end)
  end

  # `base_offset` is used only when the segment's log does not exist yet, so a fresh segment starts
  # at its range-relative first offset.
  defp fetch_or_open(state, segment_id, base_offset) do
    case Map.fetch(state.logs, segment_id) do
      {:ok, log} ->
        {state, log}

      :error ->
        opts = [base_offset: base_offset] ++ state.log_opts
        {:ok, log} = Log.open(segment_directory(state.directory, segment_id), opts)
        {put_log(state, segment_id, log), log}
    end
  end

  defp append_durably(log, records) do
    {:ok, log, first, last} = Log.append(log, records)
    {:ok, log} = Log.sync(log)
    {log, first, last}
  end

  defp put_log(state, segment_id, log), do: put_in(state.logs[segment_id], log)

  # A readable directory for the broker's {{topic, range_seq}, seg_seq} ids, with a safe,
  # collision-free fallback for any other term.
  defp segment_directory(base, {{topic, range_seq}, seg_seq}) do
    Path.join(base, "#{topic}-r#{range_seq}-s#{seg_seq}")
  end

  defp segment_directory(base, segment_id) do
    Path.join(base, Base.url_encode64(:erlang.term_to_binary(segment_id), padding: false))
  end
end
