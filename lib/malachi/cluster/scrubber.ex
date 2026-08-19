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
  a peer has confirmed its own copy verifies**: swapping a partially readable copy for no copy at
  all would be worse than the damage. Peers are asked through their own `Scrubber` (never the
  replication server), so the verification scan stays off the replication hot loop.

  When the damaged copy is the segment's primary, the repair first submits a
  `:set_segment_replicas` that moves this node to the end of the replica set. Reads follow the head
  of that set, so they move to an intact replica immediately instead of hitting the hole (or the
  gap left while the copy is being refetched).

  Every pass returns its outcome; logging is only the default `:on_result` callback, so tests
  assert on data rather than on log lines.

  ## Options

    * `:metadata_source` - `(-> Malachi.Metadata.t())`, the current metadata (required);
    * `:local_ref` - this node's replication server reference (required);
    * `:directory` - the data directory whose segments are scrubbed (required);
    * `:apply_command` - `(Metadata.command() -> any)`, applies the demote command (required);
    * `:peer_scrubber` - `(node() -> GenServer.server())`, how to reach a peer's scrubber
      (default `{__MODULE__, node}`);
    * `:interval` - ms between ticks (default 60s);
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
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
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
    state = %{
      metadata_source: Keyword.fetch!(opts, :metadata_source),
      local_ref: Keyword.fetch!(opts, :local_ref),
      directory: Keyword.fetch!(opts, :directory),
      apply_command: Keyword.fetch!(opts, :apply_command),
      peer_scrubber: Keyword.get(opts, :peer_scrubber, &{__MODULE__, &1}),
      interval: Keyword.get(opts, :interval, @default_interval),
      segments_per_tick: max(1, Keyword.get(opts, :segments_per_tick, @default_segments_per_tick)),
      on_result: Keyword.get(opts, :on_result, &log_result/1),
      # Segments still to visit in this cycle; refilled from the metadata when it empties, so every
      # sealed copy is revisited on a fixed period instead of the walk restarting from the top.
      pending: [],
      damaged: MapSet.new()
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

  # --- internals ---

  defp schedule(state), do: Process.send_after(self(), :tick, state.interval)

  defp run(state) do
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
    |> Enum.filter(&(&1.state == :sealed and state.local_ref in &1.replica_set))
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
    cond do
      is_integer(segment.length) and counts.records < segment.length ->
        verdict = %{reason: :short_copy, position: counts.bytes, unreadable_bytes: 0, sealed?: true}
        report_damage(segment, verdict, acc, state)

      is_integer(segment.byte_size) and counts.bytes < segment.byte_size ->
        verdict = %{reason: :short_copy, position: counts.bytes, unreadable_bytes: 0, sealed?: true}
        report_damage(segment, verdict, acc, state)

      true ->
        {%{acc | verified: [segment.id | acc.verified]}, forget_damage(state, segment.id)}
    end
  end

  defp report_damage(segment, verdict, acc, state) do
    # The scrub only ever walks segments the control plane sealed, so damage found here is always
    # damage to an immutable copy, whatever the local file's seal marker says (a replica's marker is
    # written only when its own log rolls, so it is usually absent).
    verdict = Map.put(verdict, :sealed?, true)
    Telemetry.storage_integrity(verdict, segment.id, :scrub)
    state = %{state | damaged: MapSet.put(state.damaged, segment.id)}
    acc = %{acc | damaged: [{segment.id, verdict} | acc.damaged]}

    repair(segment, acc, state)
  end

  # The repair is a distributed operation (a peer scan, a control-plane command and a cross-node
  # copy), which is why it gets a span while the local scan does not: this is where the latency and
  # the failure modes are worth tracing.
  defp repair(segment, acc, state) do
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

  # The safety rule: a peer must confirm its own copy verifies BEFORE the local one is deleted.
  # An unreachable or damaged peer is simply not a source.
  defp intact_peer(segment, state) do
    segment.replica_set
    |> Enum.reject(&(&1 == state.local_ref))
    |> Enum.find(fn replica -> match?({:ok, _counts}, peer_verify(state, replica, segment.id)) end)
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

    with :ok <- ReplicationServer.delete(state.local_ref, segment.id),
         {:ok, ^to_offset} <- Catchup.run(state.local_ref, peer, segment.id, segment.start_offset, to_offset),
         {:ok, _counts} <- verify(state, segment.id) do
      Tracer.set_attributes(%{"malachi.repair" => "ok"})
      {%{acc | repaired: [segment.id | acc.repaired]}, forget_damage(state, segment.id)}
    else
      other ->
        Tracer.set_attributes(%{"malachi.repair" => "failed"})
        {%{acc | unrepairable: [{segment.id, other} | acc.unrepairable]}, state}
    end
  end

  # Reads follow the head of the replica set, so moving this node to the end takes the damaged copy
  # out of the read path before it is deleted and refetched. The set itself is unchanged, so the
  # segment is never under-replicated by the repair.
  defp demote_if_primary(segment, state) do
    case segment.replica_set do
      [primary | _rest] when primary == state.local_ref ->
        others = Enum.reject(segment.replica_set, &(&1 == state.local_ref))
        _ = state.apply_command.({:set_segment_replicas, segment.id, others ++ [state.local_ref]})
        state

      _not_primary ->
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

      reason = List.keyfind(result.unrepairable, segment_id, 0) ->
        ": NOT repaired (#{inspect(elem(reason, 1))}), this copy stays damaged"

      true ->
        ""
    end
  end
end
