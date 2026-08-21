defmodule Malachi.Cluster.Scrubber do
  @moduledoc """
  Background verification of data at rest (a "scrub"), and repair of what it finds.

  A sealed segment is immutable, and nothing re-reads it: `Malachi.Log.recover/2` trusts sealed
  files by name, so a checksum is only ever confirmed when a consumer happens to read that exact
  frame. Bit rot there is therefore silent, and worse than an error: the read bound comes from the
  control plane's record count rather than from the file, so a read past the damaged frame returns
  no records, which the broker reads as "this source is drained". The consumer then stalls at that
  offset or skips the rest of the range, with nothing logged anywhere.

  This worker closes that by walking the node's own sealed segments on a slow cadence, verifying
  every frame's checksum (`Malachi.Log.verify/2`), and repairing a damaged copy from an intact
  replica. NorthGuard's rule that you are on the clock to re-replicate a lost replica applies here
  too: a copy that will not decode is a lost replica, whatever the metadata says about it.

  ## Why it runs on every node, not just the leader

  Unlike the healing and retention coordinators, this one is deliberately **not** leader-gated:
  each node is the only one that can read its own disk, and corruption is a per-copy fact, not a
  cluster-wide one. Every node scrubs what it stores.

  ## Repair, and its one safety rule

  Damage is repaired by fetching the segment again from a replica that still has it intact, which
  means deleting the local copy first. The rule is that **the local copy is never destroyed before
  a peer has confirmed it holds the whole segment, intact**: swapping a partially readable copy for
  a shorter one, or for no copy at all, would be worse than the damage. Both halves of that
  sentence carry weight, and the second is the one easy to lose: a peer whose checksums all pass can
  still be missing whole frames at the end, and repairing from it deletes the local copy and then
  catches up to an offset the source cannot reach. So a peer qualifies only when its scan matches
  what the control plane recorded at seal time, which is the same test `cross_check/4` applies to
  this node's own copy. Peers are asked through their own `Scrubber` (never the replication server),
  so the verification scan stays off the replication hot loop.

  When the damaged copy is the segment's primary, the repair first submits a
  `:set_segment_replicas` that moves this node to the end of the replica set. Reads follow the head
  of that set, so they move to an intact replica immediately instead of hitting the hole (or the
  gap left while the copy is being refetched).

  Every pass returns its outcome; logging is only the default `:on_result` callback, so tests
  assert on data rather than on log lines.

  ## Options

    * `:metadata_source` - `(-> Malachi.Metadata.t())`, the current metadata (required);
    * `:local_ref` - this node's replication server reference, or a `(-> ref)` resolved per pass
      (required). A single-node broker owns an unnamed replication server, so its reference is a pid
      that a broker restart replaces: passing a function keeps the scrub from silently matching
      nothing after such a restart;
    * `:directory` - the data directory whose segments are scrubbed (required);
    * `:apply_command` - `(Metadata.command() -> any)`, applies the demote command (required);
    * `:peer_scrubber` - `(node() -> GenServer.server())`, how to reach a peer's scrubber. Defaults
      to this process's own registered name on that node, since every node runs the same worker
      under the same name;
    * `:interval` - ms between ticks (default 60s). A value that is not a positive integer is
      refused with a warning and the default used instead, since it arrives from the environment;
    * `:segments_per_tick` - segments verified per tick (default 1). With the default 64MB segment
      size a full cycle takes `segments * interval / segments_per_tick`, so a node holding 10k
      sealed segments revisits each one about weekly;
    * `:on_result` - `(result -> any)` for each pass (default: log damage and repairs).
  """

  use GenServer

  require Logger
  require OpenTelemetry.Tracer, as: Tracer

  alias Malachi.Cluster.Catchup
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log
  alias Malachi.Metadata
  alias Malachi.Storage.Layout
  alias Malachi.Telemetry

  @default_interval 60_000
  @default_segments_per_tick 1
  # Bounds a peer's verification scan. Generous next to the other cross-node probes because the peer
  # is reading a whole segment from disk (24 to 86ms for 64MB warm, more from cold storage), but
  # still bounded so one stuck peer cannot hold up the pass.
  @peer_timeout 30_000

  @type verdict :: :ok | map()
  @type result :: %{
          verified: [Metadata.segment_id()],
          damaged: [{Metadata.segment_id(), verdict()}],
          repaired: [Metadata.segment_id()],
          unrepairable: [{Metadata.segment_id(), term()}]
        }

  @doc "Starts the scrubber. See the module doc for options."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_server_opts, opts} = Keyword.split(opts, [:name])
    # The registered name is ALSO handed to init: a peer's scrubber is addressed by the same name on
    # its node, so deriving that from the name this node registered under keeps the two from
    # drifting. Hardcoding the module name here instead is what made the first cluster run fail to
    # repair: the supervisor registers it as Malachi.LogScrubber, every peer lookup hit a dead name,
    # and every repair concluded there was no intact copy anywhere.
    GenServer.start_link(__MODULE__, Keyword.merge(opts, gen_server_opts), gen_server_opts)
  end

  @doc "Runs one pass synchronously and returns its result, for tests and manual triggers."
  @spec scrub_now(GenServer.server()) :: result()
  def scrub_now(server), do: GenServer.call(server, :scrub_now, :infinity)

  @doc """
  Verifies `segment_id` on this node's disk, without repairing anything. This is what a peer calls
  during another node's repair, and the reason peers are asked through the scrubber rather than the
  replication server: the scan reads a whole segment, which must not run inside the replication
  loop.
  """
  @spec verify_segment(GenServer.server(), Metadata.segment_id(), timeout()) ::
          {:ok, map()} | {:error, term()}
  def verify_segment(server, segment_id, timeout \\ @peer_timeout) do
    GenServer.call(server, {:verify_segment, segment_id}, timeout)
  end

  @doc "The segments this node currently knows to be damaged (a gauge for operators)."
  @spec damaged(GenServer.server()) :: [Metadata.segment_id()]
  def damaged(server), do: GenServer.call(server, :damaged)

  # --- server ---

  @impl true
  def init(opts) do
    registered_name = Keyword.get(opts, :name, __MODULE__)

    state = %{
      metadata_source: Keyword.fetch!(opts, :metadata_source),
      local_ref: Keyword.fetch!(opts, :local_ref),
      directory: Keyword.fetch!(opts, :directory),
      apply_command: Keyword.fetch!(opts, :apply_command),
      peer_scrubber: Keyword.get(opts, :peer_scrubber, &{registered_name, &1}),
      interval: usable_interval(Keyword.get(opts, :interval, @default_interval)),
      segments_per_tick: max(1, Keyword.get(opts, :segments_per_tick, @default_segments_per_tick)),
      on_result: Keyword.get(opts, :on_result, &log_result/1),
      # Segments still to visit in this cycle; refilled from the metadata when it empties, so every
      # sealed copy is revisited on a fixed period instead of the walk restarting from the top.
      pending: [],
      damaged: MapSet.new(),
      # The reference resolved for the pass currently running (see `:local_ref`).
      resolved_ref: nil
    }

    schedule(state)
    {:ok, state}
  end

  @impl true
  def handle_call(:scrub_now, _from, state) do
    {result, state} = run(state)
    {:reply, result, state}
  end

  def handle_call({:verify_segment, segment_id}, _from, state) do
    {:reply, verify(state, segment_id), state}
  end

  def handle_call(:damaged, _from, state), do: {:reply, MapSet.to_list(state.damaged), state}

  @impl true
  def handle_info(:tick, state) do
    {_result, state} = run(state)
    schedule(state)
    {:noreply, state}
  end

  # A worker meant to run for the lifetime of the node must not die on a message it did not plan for:
  # the crash costs the cycle position and the damaged set, so the restarted walk begins again at the
  # first segment and the tail of the rotation is never reached on a busy node. Nothing here is known
  # to send one (a reply arriving after `verify_segment/3` times out is dropped by the runtime, which
  # deactivates the call's alias on timeout), and that is the point: an unexpected message is a fact
  # worth surfacing, not a reason to take the process down.
  def handle_info(message, state) do
    # `printable_limit: 0` is the point of these options, not `limit`: it replaces the CONTENT of
    # every string in the term with an ellipsis while leaving the term's shape intact, so this prints
    # something like `{#Reference<...>, {:ok, ...}}`. Whatever reaches this clause is by definition
    # not a message we planned for, so it may carry record values, and a log line is the one place
    # user data must not end up in by accident. Truncating the text is not enough for that: a bounded
    # prefix of a payload is still a payload. The shape is what makes the line worth having, since it
    # is what identifies the sender.
    Logger.warning("scrubber ignoring unexpected message: #{inspect(message, limit: 3, printable_limit: 0)}")

    {:noreply, state}
  end

  # --- internals ---

  defp schedule(state), do: Process.send_after(self(), :tick, state.interval)

  # The interval reaches here straight from MALACHI_SCRUB_INTERVAL_MS, which parses any integer: zero
  # would turn the cadence into a busy loop that scans the disk as fast as it can, and a negative one
  # would crash `Process.send_after/3` at the first schedule. Neither is what an operator meant to ask
  # for, and neither is worth refusing to boot over, so the value is rejected and said out loud. This
  # is the same shape as the `max(1, ...)` on `:segments_per_tick`, which has the same origin.
  defp usable_interval(interval) when is_integer(interval) and interval > 0, do: interval

  defp usable_interval(interval) do
    Logger.warning(
      "scrub interval #{inspect(interval)} is not a positive number of milliseconds, " <>
        "using the default of #{@default_interval}ms"
    )

    @default_interval
  end

  defp resolve_ref(local_ref) when is_function(local_ref, 0), do: local_ref.()
  defp resolve_ref(local_ref), do: local_ref

  defp run(state) do
    state = %{state | resolved_ref: resolve_ref(state.local_ref)}
    {segments, state} = take_segments(state)

    {result, state} =
      Enum.reduce(segments, {empty_result(), state}, fn segment, {acc, acc_state} ->
        scrub_segment(segment, acc, acc_state)
      end)

    result = finalize(result)
    emit_progress(result)
    state.on_result.(result)
    {result, state}
  end

  # Pops the next batch off the cycle, refilling from the current metadata when the cycle is done.
  # Refilling reads the metadata fresh, so segments sealed since the last cycle join the rotation
  # and deleted ones drop out of it.
  defp take_segments(%{pending: []} = state) do
    case sealed_segments_here(state) do
      [] -> {[], state}
      segments -> take_segments(%{state | pending: segments})
    end
  end

  defp take_segments(state) do
    {batch, rest} = Enum.split(state.pending, state.segments_per_tick)
    {batch, %{state | pending: rest}}
  end

  defp sealed_segments_here(state) do
    state.metadata_source.().segments
    |> Map.values()
    |> Enum.filter(&(&1.state == :sealed and state.resolved_ref in &1.replica_set))
    |> Enum.sort_by(& &1.id)
  end

  defp scrub_segment(segment, acc, state) do
    case verify(state, segment.id) do
      {:ok, counts} ->
        cross_check(segment, counts, acc, state)

      # Retention removed the segment mid-cycle, or this node never stored it: not damage.
      {:error, :enoent} ->
        {acc, forget_damage(state, segment.id)}

      {:error, verdict} ->
        report_damage(segment, verdict, acc, state)
    end
  end

  # The checksums pass, but the control plane recorded what the segment held when it was sealed, so
  # a copy with fewer records (or bytes) than that is damaged in a way no checksum can see: whole
  # frames are missing from the end.
  defp cross_check(segment, counts, acc, state) do
    if short_copy?(segment, counts) do
      verdict = %{reason: :short_copy, position: counts.bytes, unreadable_bytes: 0, sealed?: true}
      report_damage(segment, verdict, acc, state)
    else
      {%{acc | verified: [segment.id | acc.verified]}, forget_damage(state, segment.id)}
    end
  end

  # Applied to this node's copy to classify it, and to a peer's copy to decide whether it can be a
  # repair source. One predicate for both on purpose: a peer we would call damaged if it were ours is
  # not a copy worth deleting ours for. `length` and `byte_size` are only set at seal time, so an
  # unset one simply does not constrain (`register_segment` leaves them nil until the seal lands).
  defp short_copy?(segment, counts) do
    (is_integer(segment.length) and counts.records < segment.length) or
      (is_integer(segment.byte_size) and counts.bytes < segment.byte_size)
  end

  defp report_damage(segment, verdict, acc, state) do
    # The scrub only ever walks segments the control plane sealed, so damage found here is always
    # damage to an immutable copy, whatever the local file's seal marker says (a replica's marker is
    # written only when its own log rolls, so it is usually absent).
    verdict = Map.put(verdict, :sealed?, true)
    Telemetry.storage_integrity(verdict, segment.id, :scrub)
    state = %{state | damaged: MapSet.put(state.damaged, segment.id)}
    acc = %{acc | damaged: [{segment.id, verdict} | acc.damaged]}

    repair(segment, verdict, acc, state)
  end

  # A damaged sparse index is repaired without leaving the node and without touching the segment: the
  # index is DERIVED from the records, so nothing original was lost and no replica can offer anything
  # this node does not already hold. That makes it the one damage shape with no peer, no demotion and
  # no deletion, which is also why it must never be confused with damage to the segment itself.
  defp repair(segment, %{reason: :bad_index}, acc, state), do: rebuild_index(segment, acc, state)
  defp repair(segment, _segment_damage, acc, state), do: repair_from_peer(segment, acc, state)

  defp rebuild_index(segment, acc, state) do
    directory = Layout.segment_directory(state.directory, segment.id)

    case Log.rebuild_index(directory) do
      :ok ->
        {%{acc | repaired: [segment.id | acc.repaired]}, forget_damage(state, segment.id)}

      {:error, reason} ->
        {%{acc | unrepairable: [{segment.id, {:rebuild_index, reason}} | acc.unrepairable]}, state}
    end
  end

  # The repair is a distributed operation (a peer scan, a control-plane command and a cross-node
  # copy), which is why it gets a span while the local scan does not: this is where the latency and
  # the failure modes are worth tracing.
  defp repair_from_peer(segment, acc, state) do
    Tracer.with_span "malachi.scrub.repair" do
      Tracer.set_attributes(%{
        "malachi.segment" => inspect(segment.id),
        "malachi.replicas" => length(segment.replica_set)
      })

      case intact_peer(segment, state) do
        nil ->
          Tracer.set_attributes(%{"malachi.repair" => "no_intact_copy"})
          {%{acc | unrepairable: [{segment.id, :no_intact_copy} | acc.unrepairable]}, state}

        peer ->
          run_repair(segment, peer, acc, state)
      end
    end
  end

  # The safety rule: a peer must confirm it holds the WHOLE segment, intact, BEFORE the local copy is
  # deleted. An unreachable, damaged or short peer is simply not a source.
  defp intact_peer(segment, state) do
    segment.replica_set
    |> Enum.reject(&(&1 == state.resolved_ref))
    |> Enum.find(fn replica ->
      case peer_verify(state, replica, segment.id) do
        {:ok, counts} -> not short_copy?(segment, counts)
        {:error, _reason} -> false
      end
    end)
  end

  defp peer_verify(state, {_name, node} = _replica, segment_id) do
    verify_segment(state.peer_scrubber.(node), segment_id)
  catch
    :exit, _reason -> {:error, :unreachable}
  end

  defp peer_verify(_state, _replica, _segment_id), do: {:error, :not_addressable}

  defp run_repair(segment, peer, acc, state) do
    state = demote_if_primary(segment, state)
    to_offset = segment.start_offset + (segment.length || 0)

    # The point of no return, and it cannot fail (`delete/2` answers `:ok` even for a segment it does
    # not hold): past this line the damaged bytes are gone and only the peer can put them back. That
    # is what makes `intact_peer/2` strict about what counts as a source, and why a failure after it
    # is reported apart from one decided before it, which left the copy untouched.
    :ok = ReplicationServer.delete(state.resolved_ref, segment.id)

    # Reaching `to_offset` is itself the completeness check: the catchup copied every offset the seal
    # recorded, so the verify only has to confirm the frames landed readable.
    with {:ok, ^to_offset} <- Catchup.run(state.resolved_ref, peer, segment.id, segment.start_offset, to_offset),
         {:ok, _counts} <- verify(state, segment.id) do
      Tracer.set_attributes(%{"malachi.repair" => "ok"})
      {%{acc | repaired: [segment.id | acc.repaired]}, forget_damage(state, segment.id)}
    else
      other ->
        Tracer.set_attributes(%{"malachi.repair" => "failed"})
        {%{acc | unrepairable: [{segment.id, {:refetch, other}} | acc.unrepairable]}, state}
    end
  end

  # Reads follow the head of the replica set, so moving this node to the end takes the damaged copy
  # out of the read path before it is deleted and refetched. The set itself is unchanged, so the
  # segment is never under-replicated by the repair.
  defp demote_if_primary(segment, state) do
    others = Enum.reject(segment.replica_set, &(&1 == state.resolved_ref))

    case segment.replica_set do
      # Only worth a control-plane command when there is somewhere else for reads to go: at rf=1 the
      # demoted set would be the same single-element list.
      [primary | _rest] when primary == state.resolved_ref and others != [] ->
        _ = state.apply_command.({:set_segment_replicas, segment.id, others ++ [state.resolved_ref]})
        state

      _not_primary_or_alone ->
        state
    end
  end

  defp verify(state, segment_id) do
    state.directory
    |> Layout.segment_directory(segment_id)
    |> Log.verify()
  end

  defp forget_damage(state, segment_id), do: %{state | damaged: MapSet.delete(state.damaged, segment_id)}

  defp empty_result, do: %{verified: [], damaged: [], repaired: [], unrepairable: []}

  defp finalize(result) do
    %{
      verified: Enum.reverse(result.verified),
      damaged: Enum.reverse(result.damaged),
      repaired: Enum.reverse(result.repaired),
      unrepairable: Enum.reverse(result.unrepairable)
    }
  end

  defp emit_progress(result) do
    Telemetry.scrub_pass(
      length(result.verified),
      length(result.damaged),
      length(result.repaired),
      length(result.unrepairable)
    )
  end

  defp log_result(%{damaged: [], unrepairable: []}), do: :ok

  defp log_result(result) do
    for {segment_id, verdict} <- result.damaged do
      Logger.warning(
        "scrub found #{inspect(segment_id)} damaged (#{verdict.reason} at byte #{verdict.position})" <>
          repair_outcome(result, segment_id)
      )
    end

    :ok
  end

  defp repair_outcome(result, segment_id) do
    cond do
      segment_id in result.repaired ->
        ": repaired from an intact replica"

      entry = List.keyfind(result.unrepairable, segment_id, 0) ->
        not_repaired(elem(entry, 1))

      true ->
        ""
    end
  end

  # The two failures need different operator action, so they do not share a message: everything up to
  # the refetch leaves the damaged bytes on disk to salvage by hand, while a failure during it leaves
  # a copy that is merely incomplete, which a later pass finds as short and repairs again on its own.
  defp not_repaired({:refetch, reason}) do
    ": repair FAILED mid-refetch (#{inspect(reason)}), this copy is incomplete until a later pass finishes it"
  end

  defp not_repaired(reason) do
    ": NOT repaired (#{inspect(reason)}), this copy stays damaged and its bytes are still on disk"
  end
end
