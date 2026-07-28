defmodule Malachi.Cluster.ReplicationServer do
  @moduledoc """
  The transport for segment replication: a `GenServer`, one per broker, that ships an active
  segment's records from the **primary** to its **followers** and acknowledges a write once a
  **quorum** has durably stored it.

  A broker is identified by this server's process reference (a registered name locally, or a
  `{name, node}` tuple across nodes: `GenServer.call/3` accepts both, so the same code path runs
  in-process for tests and over distributed Erlang in production). A segment's `replica_set` (from
  `Malachi.Cluster.Placement`) is a list of those references; the first is the primary.

  On `replicate/5` the primary appends the batch to its local copy, fans out to the followers
  concurrently, and feeds each ack into a `Malachi.Cluster.ReplicaTracker` to compute the commit
  offset. A segment's log opens at the segment's `base_offset` (its first range-relative offset),
  so the offsets of a range's segments are contiguous rather than restarting at zero per segment.
  The call returns `{:ok, last_offset}` once a quorum (the primary plus enough followers) has the
  batch. Tolerating up to ⌊(N-1)/2⌋ slow or unreachable followers - or `{:error, :no_quorum}`
  otherwise. Both the primary and the followers `fsync` before counting toward the quorum, so
  "committed" means "durable on a majority".

  Scope: the active segment's happy path with quorum tolerance, plus **automatic catch-up** of a
  follower that is behind: when the primary's fan-out reaches a follower whose end is below the
  batch's offset, the follower kicks off a background pull from the primary (`Malachi.Cluster.Catchup`)
  and rejoins the quorum on a later batch. This covers both a follower that missed some batches and
  a **brand-new replica** that joins an active segment: it opens at the segment's `base`, sees the
  gap, backfills, and converges on the moving head as later fan-outs re-trigger. Sealed-segment
  re-replication (see `Malachi.Cluster.SelfHealing`) and primary failover are later/other slices.
  """

  use GenServer

  require OpenTelemetry.Tracer, as: Tracer

  alias Malachi.Cluster.Catchup
  alias Malachi.Cluster.ReplicaTracker
  alias Malachi.Log
  alias Malachi.Telemetry
  alias OpenTelemetry.Ctx

  @follow_timeout 5_000

  @typep state :: %{
           ref: term(),
           directory: Path.t(),
           log_opts: keyword(),
           logs: %{term() => Log.t()},
           trackers: %{term() => ReplicaTracker.t()},
           catching_up: MapSet.t(),
           catchup_monitors: %{reference() => term()}
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
    # Carry the caller's trace context (the broker produce span, possibly on another node) so the quorum
    # replication becomes a child span: distributed tracing across the produce -> replication hop (O5b).
    GenServer.call(
      primary,
      {:replicate, segment_id, replica_set, base_offset, records, Ctx.get_current()},
      @follow_timeout + 10_000
    )
  catch
    # A dead/unreachable primary must not crash the caller; surface it as an error to handle.
    :exit, _reason -> {:error, :unreachable}
  end

  @doc "Reads up to `max_records` records of `segment_id` stored on this server, from `offset`."
  @spec read(term(), term(), non_neg_integer(), pos_integer()) ::
          {:ok, [Malachi.Log.Record.t()]} | :eof | {:error, term()}
  def read(ref, segment_id, offset, max_records) do
    GenServer.call(ref, {:read, segment_id, offset, max_records})
  end

  @doc """
  Deletes `segment_id`'s stored data from this server (used by retention once the control plane has
  dropped the segment). Idempotent. Deleting an unknown or already-removed segment is `:ok`, and it
  also clears any on-disk files left after a restart when the log was not reopened.
  """
  @spec delete(term(), term()) :: :ok
  def delete(ref, segment_id) do
    GenServer.call(ref, {:delete, segment_id})
  catch
    # An unreachable replica must not crash the caller (e.g. retention's periodic sweep): the files
    # are left in place, harmless without control-plane metadata, and cleaned up on a later sweep.
    :exit, _reason -> :ok
  end

  @doc """
  Appends a replicated batch of `segment_id` to this server (the follower side). `expected_first`
  is the offset the batch must start at: it must equal this server's current end for the segment
  (or the segment's base when it is opened here for the first time). Returns `{:ok, last_offset}`
  or `{:error, :out_of_sync}` if this server is behind. Used by the primary's fan-out and by
  `Malachi.Cluster.Catchup`.
  """
  @spec follow(term(), term(), non_neg_integer(), [Malachi.Log.Record.t()]) ::
          {:ok, non_neg_integer()} | {:error, :out_of_sync}
  def follow(ref, segment_id, expected_first, records) do
    # No source: a plain replica append (used by Catchup); never re-triggers a catch-up. A fresh
    # log opens exactly where this batch starts.
    GenServer.call(ref, {:follow, segment_id, expected_first, expected_first, records, nil})
  end

  @doc "This server's next offset for `segment_id`, or `:empty` if it stores none of it yet."
  @spec end_offset(term(), term()) :: non_neg_integer() | :empty
  def end_offset(ref, segment_id) do
    GenServer.call(ref, {:end_offset, segment_id})
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
      trackers: %{},
      catching_up: MapSet.new(),
      catchup_monitors: %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:replicate, _segment_id, _replica_set, _base_offset, [], _ctx}, _from, state) do
    {:reply, {:error, :empty}, state}
  end

  def handle_call({:replicate, _segment_id, [], _base_offset, _records, _ctx}, _from, state) do
    {:reply, {:error, :empty_replica_set}, state}
  end

  def handle_call({:replicate, segment_id, replica_set, base_offset, records, ctx}, _from, state) do
    token = Ctx.attach(ctx)

    try do
      Tracer.with_span "malachi.replication.commit" do
        if hd(replica_set) == state.ref do
          do_replicate(state, segment_id, replica_set, base_offset, records)
        else
          {:reply, {:error, :not_primary}, state}
        end
      end
    after
      Ctx.detach(token)
    end
  end

  def handle_call({:follow, segment_id, base, expected_first, records, source}, _from, state) do
    # Open a fresh segment at its `base` (not the batch's offset), so a brand-new replica that
    # joins mid-segment sees the start gap and backfills it, rather than silently starting late.
    {state, log} = fetch_or_open(state, segment_id, base)

    cond do
      log.next_offset == expected_first ->
        {log, _first, last} = append_durably(log, records)
        {:reply, {:ok, last}, put_log(state, segment_id, log)}

      source != nil and log.next_offset < expected_first ->
        # Behind on the active segment (a new replica from base, or one that missed batches): kick
        # off a background catch-up from the primary and skip this batch (it commits via the
        # up-to-date replicas). We rejoin on a later batch as the catch-up converges on the head.
        {:reply, {:error, :out_of_sync}, trigger_catchup(state, segment_id, source)}

      true ->
        {:reply, {:error, :out_of_sync}, state}
    end
  end

  def handle_call({:read, segment_id, offset, max_records}, _from, state) do
    case Map.fetch(state.logs, segment_id) do
      :error -> {:reply, :eof, state}
      {:ok, log} -> {:reply, Log.read(log, offset, max_records), state}
    end
  end

  def handle_call({:delete, segment_id}, _from, state) do
    case Map.pop(state.logs, segment_id) do
      # Open here: close and drop its whole directory.
      {%Log{} = log, logs} ->
        :ok = Log.delete(log)
        {:reply, :ok, %{state | logs: logs}}

      # Not open (never replicated here, or not reopened after a restart): clear any files on disk.
      {nil, logs} ->
        _ = File.rm_rf(segment_directory(state.directory, segment_id))
        {:reply, :ok, %{state | logs: logs}}
    end
  end

  def handle_call({:end_offset, segment_id}, _from, state) do
    reply =
      case Map.fetch(state.logs, segment_id) do
        {:ok, log} -> log.next_offset
        :error -> :empty
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, _pid, _reason}, state) do
    case Map.pop(state.catchup_monitors, monitor_ref) do
      {nil, _monitors} ->
        {:noreply, state}

      {segment_id, monitors} ->
        {:noreply, %{state | catching_up: MapSet.delete(state.catching_up, segment_id), catchup_monitors: monitors}}
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
    # Distinct replicas, and never the primary itself among the followers, otherwise the server
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
      |> gather_acks(segment_id, base_offset, first, records, state.ref)
      |> Enum.reduce(tracker, fn {follower, offset}, acc ->
        {:ok, acc} = ReplicaTracker.ack(acc, follower, offset)
        acc
      end)

    state = put_in(state.trackers[segment_id], tracker)
    reply = if ReplicaTracker.committed?(tracker, last), do: {:ok, last}, else: {:error, :no_quorum}
    Telemetry.replication_commit(length(records), if(match?({:ok, _}, reply), do: :ok, else: :no_quorum))
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

  # Calls each follower concurrently, passing `primary` as the catch-up source so a behind follower
  # can self-heal. Keeps only those that durably stored the batch; a slow, unreachable, or
  # out-of-sync follower is dropped (it does not count toward the quorum).
  defp gather_acks(followers, segment_id, base, expected_first, records, primary) do
    followers
    |> Task.async_stream(
      fn follower ->
        try do
          {follower,
           GenServer.call(follower, {:follow, segment_id, base, expected_first, records, primary}, @follow_timeout)}
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

  # Starts one background catch-up per segment (deduped via `catching_up`): it pulls everything the
  # source has past our current end. Monitored so the in-progress flag is cleared on completion or
  # crash; the offset check in `follow` keeps concurrent appends safe, so a racing produce just
  # makes the catch-up abort and the next gap re-trigger.
  defp trigger_catchup(state, segment_id, source) do
    if MapSet.member?(state.catching_up, segment_id) do
      state
    else
      target = state.ref
      {_pid, monitor_ref} = spawn_monitor(fn -> run_catchup(target, source, segment_id) end)

      %{
        state
        | catching_up: MapSet.put(state.catching_up, segment_id),
          catchup_monitors: Map.put(state.catchup_monitors, monitor_ref, segment_id)
      }
    end
  end

  defp run_catchup(target, source, segment_id) do
    from = current_end(target, segment_id)
    to = current_end(source, segment_id)
    if to > from, do: Catchup.run(target, source, segment_id, from, to)
  end

  defp current_end(ref, segment_id) do
    case end_offset(ref, segment_id) do
      :empty -> 0
      offset -> offset
    end
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

  # A readable directory for the broker's {{topic, range_seq}, seg_seq} ids, with a safe, collision-free
  # fallback for any other term. segment_ids arrive over inter-node replication, so the topic is not
  # trusted here even though Metadata.valid_topic_name?/1 screens locally created topics: a path-unsafe
  # topic (or a non-integer seq) falls through to the Base64 encoding, keeping the directory inside `base`.
  defp segment_directory(base, {{topic, range_seq}, seg_seq} = segment_id)
       when is_integer(range_seq) and is_integer(seg_seq) do
    if safe_path_segment?(topic) do
      Path.join(base, "#{topic}-r#{range_seq}-s#{seg_seq}")
    else
      encoded_segment_directory(base, segment_id)
    end
  end

  defp segment_directory(base, segment_id), do: encoded_segment_directory(base, segment_id)

  defp encoded_segment_directory(base, segment_id) do
    Path.join(base, Base.url_encode64(:erlang.term_to_binary(segment_id), padding: false))
  end

  # Mirrors the allowlist in Metadata.valid_topic_name?/1 as a defense-in-depth check where the path is
  # built, since segment_ids can arrive from other nodes.
  defp safe_path_segment?(topic) do
    is_binary(topic) and topic not in ["", ".", ".."] and topic =~ ~r/\A[A-Za-z0-9._-]+\z/
  end
end
