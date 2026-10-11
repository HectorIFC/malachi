defmodule Malachi.Consumer.GroupCoordinator do
  @moduledoc """
  Tracks the members of consumer groups and assigns each topic's ranges across them (via
  `Malachi.Consumer.Assignment`), so members consume in parallel: one range per member.

  A group is keyed by `{group, topic}`. Members `join`, then `heartbeat` to stay alive; a member that
  stops heartbeating for `session_ms` is evicted (a dead consumer). Rebalancing is **eager**: any
  membership change: join, leave, eviction - or a change in the topic's ranges recomputes the whole
  assignment and bumps a **generation** counter; a member re-reads its assignment on the next heartbeat and
  sees the new generation, its signal to take over its (possibly changed) ranges. Because the assignment is
  deterministic and sticky, a rebalance moves few ranges.

  This is the coordinator **logic** as a single GenServer, reached through seams (`:clock`, `:ranges_fun`)
  so it is testable without a cluster; which node coordinates which group (or replicating the membership)
  is a separate wiring concern. Member state is soft: a coordinator restart just makes members re-join.

  ## Generations and the commit fence

  What the group keys hand a member (`join_group/4`, `heartbeat_group/4`) is a generation: the group's
  **epoch** in its high 32 bits and its rebalance count in its low 32. The epoch comes from the control
  plane the first time a coordinator serves the group keys for a group (`:open_fun`, which is
  `Malachi.Broker.open_group/3` once wired), and every opening is above every earlier one. The opening
  runs beside the coordinator, which goes on serving the other groups, and a call for the group waits for
  it (at most `:open_deadline_ms`, then `timeout`); nothing about the group changes before it has its
  epoch, so a refused `join_group` registers no one. A commit at an epoch another coordinator has since
  superseded is refused by the control plane, and `drop_epoch/4` makes this coordinator open the group
  again.

  A commit (`validate_commit/6`) is fenced **per range**, so a rebalance is as incremental for checkpoints
  as NorthGuard's consumer group management (its Conductor) makes it for consumers: each range remembers the count at which it was handed to its current owner, and a member
  may commit a range it still owns at any generation from that one on. A rebalance that moves other
  members' ranges, or adds a member that takes none, leaves its checkpoints alone. The control plane
  orders every commit and refuses one below a generation a range was already committed at
  (`Malachi.Metadata`'s group fence), which closes the gap between this check and the write.

  The legacy entry points (`join/4`, `poll/4`, `heartbeat/4`) predate the epoch and hand out the bare
  rebalance count; they never wait for an opening.

  ## Options
    * `:ranges_fun` - `(topic -> [range_id])`, the topic's current ranges (default `fn _ -> [] end`)
    * `:open_fun`   - `(group, topic -> {:ok, epoch} | {:error, reason})`, opens a group's next epoch
      (default `fn _, _ -> {:ok, 0} end`)
    * `:open_deadline_ms` - how long an opening may take before its callers are answered `timeout`
      (default 2_000)
    * `:clock`      - `(-> integer_ms)` monotonic clock (default `System.monotonic_time(:millisecond)`)
    * `:session_ms` - evict a member silent this long (default 30_000)
    * `:tick_ms`    - reconcile interval: evict + rebalance (default a third of the session, at most
      10_000, so a member silent past its session is evicted within a third of it more)
    * `:name`       - optional registered name
  """

  use GenServer

  alias Malachi.Consumer.Assignment

  @default_session_ms 30_000
  @default_tick_ms 10_000
  # How long an epoch opening may take before the calls waiting for it are answered `timeout`: well inside
  # the 5 s a caller waits on this coordinator.
  @default_open_deadline_ms 2_000

  @type member :: term()
  @type reply :: {:ok, non_neg_integer(), [term()]} | {:error, :unknown_member | :not_owner}
  @type group_reply ::
          {:ok, %{generation: non_neg_integer(), ranges: [term()], session_ms: pos_integer()}}
          | {:error, term()}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_server_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @doc """
  Joins `member` to `{group, topic}`, triggering a rebalance. Returns `{:ok, generation, ranges}`, or
  `{:error, :not_owner}` if this node no longer owns the topic's coordination (the caller re-resolves).
  """
  @spec join(GenServer.server(), term(), term(), member()) :: reply()
  def join(server, group, topic, member), do: GenServer.call(server, {:join, group, topic, member})

  @doc """
  The fetch-path entry point (implicit membership): registers `member` if new (a rebalance) or just
  refreshes its session if known (no rebalance), and returns its `{:ok, generation, ranges}`. Called on
  every `fetch`, so a member stays alive by fetching and needs no separate heartbeat. Returns
  `{:error, :not_owner}` if this node no longer owns the topic's coordination (the caller re-resolves).
  """
  @spec poll(GenServer.server(), term(), term(), member()) :: reply()
  def poll(server, group, topic, member), do: GenServer.call(server, {:poll, group, topic, member})

  @doc """
  Refreshes `member`'s session and returns its current `{:ok, generation, ranges}`, or
  `{:error, :unknown_member}` if it was evicted (the caller must `join/4` again), or `{:error, :not_owner}`
  if this node no longer owns the topic's coordination.
  """
  @spec heartbeat(GenServer.server(), term(), term(), member()) :: reply()
  def heartbeat(server, group, topic, member), do: GenServer.call(server, {:heartbeat, group, topic, member})

  @doc """
  `join/4` as the group keys answer it (`Malachi.Wire` 32): the generation, the member's ranges, and the
  session it must heartbeat within. Opens the group's epoch first if this coordinator has none for it yet,
  and answers the control plane's error if it cannot (the member asks again).
  """
  @spec join_group(GenServer.server(), term(), term(), member()) :: group_reply()
  def join_group(server, group, topic, member), do: GenServer.call(server, {:join_group, group, topic, member})

  @doc """
  `heartbeat/4` as the group keys answer it (`Malachi.Wire` 33): the current generation and ranges, whatever
  generation the member holds, so a member that missed a rebalance learns it here; `{:error,
  :unknown_member}` for one evicted (it joins again).
  """
  @spec heartbeat_group(GenServer.server(), term(), term(), member()) :: group_reply()
  def heartbeat_group(server, group, topic, member),
    do: GenServer.call(server, {:heartbeat_group, group, topic, member})

  @doc """
  Whether `member` of `{group, topic}` may commit positions of `range_ids` at `generation`: `:ok` when it is a
  member, the generation is of the group's epoch and not past its count, and every range is the member's
  and was handed to it at or before that generation (the commit also refreshes its session). Otherwise
  `{:error, :unknown_member}` (it joins again), `{:error, :not_assigned}` (a range that is not its own),
  `{:error, :stale_generation}` (a generation of an earlier epoch, or one older than the hand-over of a
  range it holds), or `{:error, :not_owner}` (this node no longer owns the topic's coordination).
  """
  @spec validate_commit(GenServer.server(), term(), term(), member(), non_neg_integer(), [term()]) ::
          :ok | {:error, :unknown_member | :stale_generation | :not_assigned | :not_owner}
  def validate_commit(server, group, topic, member, generation, range_ids),
    do: GenServer.call(server, {:validate_commit, group, topic, member, generation, range_ids})

  @doc """
  Forgets the epoch this coordinator holds for `{group, topic}` when it is `epoch`: the control plane
  refused a commit at it because another coordinator opened the group since (`:stale_epoch`). The next
  group key call opens the group again, above that one, and its members learn the new generation on their
  next heartbeat.
  """
  @spec drop_epoch(GenServer.server(), term(), term(), non_neg_integer()) :: :ok
  def drop_epoch(server, group, topic, epoch), do: GenServer.cast(server, {:drop_epoch, group, topic, epoch})

  @doc "Removes `member` from `{group, topic}`, triggering a rebalance."
  @spec leave(GenServer.server(), term(), term(), member()) :: :ok
  def leave(server, group, topic, member), do: GenServer.call(server, {:leave, group, topic, member})

  @doc "Reads `member`'s current assignment without refreshing its session."
  @spec assignment(GenServer.server(), term(), term(), member()) :: reply()
  def assignment(server, group, topic, member), do: GenServer.call(server, {:assignment, group, topic, member})

  @doc "Runs one reconcile (evict timed-out members + rebalance) immediately. Also fired on the timer."
  @spec reconcile_now(GenServer.server()) :: :ok
  def reconcile_now(server), do: GenServer.call(server, :reconcile_now)

  @impl true
  def init(opts) do
    session_ms = Keyword.get(opts, :session_ms, @default_session_ms)

    state = %{
      groups: %{},
      ranges_fun: Keyword.get(opts, :ranges_fun, fn _topic -> [] end),
      # Whether this node owns a topic's coordination. A member routed here by a stale leadership view
      # (during a vnode failover) is rejected with `{:error, :not_owner}` so it re-resolves to the new
      # owner, rather than registering a phantom assignment here. Default `true`: single-instance / single
      # -node (and tests) always own.
      owns_fun: Keyword.get(opts, :owns_fun, fn _topic -> true end),
      clock: Keyword.get(opts, :clock, fn -> System.monotonic_time(:millisecond) end),
      session_ms: session_ms,
      tick_ms: Keyword.get_lazy(opts, :tick_ms, fn -> tick_ms(session_ms) end),
      open_fun: Keyword.get(opts, :open_fun, fn _group, _topic -> {:ok, 0} end),
      open_deadline_ms: Keyword.get(opts, :open_deadline_ms, @default_open_deadline_ms),
      # each group's epoch, once the control plane opened it, and the openings under way, with the group
      # key calls that wait for them
      epochs: %{},
      opening: %{},
      opening_keys: %{}
    }

    schedule(state)
    {:ok, state}
  end

  @impl true
  def handle_call({:join, group, topic, member}, _from, state) do
    if state.owns_fun.(topic) do
      {state, group_state} = join_member(state, {group, topic}, topic, member)
      {:reply, {:ok, group_state.generation, ranges_of(group_state, member)}, state}
    else
      {:reply, {:error, :not_owner}, state}
    end
  end

  def handle_call({:poll, group, topic, member}, _from, state) do
    if state.owns_fun.(topic) do
      key = {group, topic}

      case Map.get(state.groups, key) do
        # known member: refresh its session only (no rebalance)
        %{members: members} = group_state when is_map_key(members, member) ->
          group_state = %{group_state | members: Map.put(members, member, state.clock.())}
          state = put_in(state.groups[key], group_state)
          {:reply, {:ok, group_state.generation, ranges_of(group_state, member)}, state}

        # new (or evicted) member: register it, which rebalances
        _new ->
          {state, group_state} = join_member(state, key, topic, member)
          {:reply, {:ok, group_state.generation, ranges_of(group_state, member)}, state}
      end
    else
      {:reply, {:error, :not_owner}, state}
    end
  end

  def handle_call({:heartbeat, group, topic, member}, _from, state) do
    key = {group, topic}

    case {state.owns_fun.(topic), Map.get(state.groups, key)} do
      {false, _group_state} ->
        {:reply, {:error, :not_owner}, state}

      {true, %{members: members} = group_state} when is_map_key(members, member) ->
        group_state = %{group_state | members: Map.put(members, member, state.clock.())}
        state = put_in(state.groups[key], group_state)
        {:reply, {:ok, group_state.generation, ranges_of(group_state, member)}, state}

      {true, _evicted_or_unknown} ->
        {:reply, {:error, :unknown_member}, state}
    end
  end

  def handle_call({:leave, group, topic, member}, _from, state) do
    key = {group, topic}

    state =
      case Map.get(state.groups, key) do
        nil ->
          state

        group_state ->
          members = Map.delete(group_state.members, member)
          groups = settle(state.groups, key, %{group_state | members: members}, topic, state.ranges_fun)
          # a group the leave emptied drops its epoch with it
          epochs = if is_map_key(groups, key), do: state.epochs, else: Map.delete(state.epochs, key)
          %{state | groups: groups, epochs: epochs}
      end

    {:reply, :ok, state}
  end

  def handle_call({:assignment, group, topic, member}, _from, state) do
    case Map.get(state.groups, {group, topic}) do
      %{members: members} = group_state when is_map_key(members, member) ->
        {:reply, {:ok, group_state.generation, ranges_of(group_state, member)}, state}

      _evicted_or_unknown ->
        {:reply, {:error, :unknown_member}, state}
    end
  end

  # The group key calls need the group's epoch. One the control plane has not opened yet for this
  # coordinator is opened beside it, in a process of its own, and the calls wait for it there: the
  # coordinator goes on serving every other group meanwhile, and calls for a group whose opening is under
  # way wait for that one rather than open another.
  def handle_call({:join_group, _group, topic, _member} = request, from, state),
    do: with_epoch(request, topic, from, state)

  def handle_call({:heartbeat_group, _group, topic, _member} = request, from, state),
    do: with_epoch(request, topic, from, state)

  def handle_call({:validate_commit, _group, topic, _member, _generation, _range_ids} = request, from, state),
    do: with_epoch(request, topic, from, state)

  def handle_call(:reconcile_now, _from, state), do: {:reply, :ok, reconcile(state)}

  @impl true
  def handle_cast({:drop_epoch, group, topic, epoch}, state) do
    key = {group, topic}
    epochs = if Map.get(state.epochs, key) == epoch, do: Map.delete(state.epochs, key), else: state.epochs
    {:noreply, %{state | epochs: epochs}}
  end

  defp with_epoch(request, topic, from, state) do
    key = request_key(request)

    cond do
      not state.owns_fun.(topic) ->
        {:reply, {:error, :not_owner}, state}

      is_map_key(state.epochs, key) ->
        serve(request, state)

      # a member this coordinator does not know has nothing an epoch would serve: none is opened for it
      elem(request, 0) != :join_group and not member?(state, key, elem(request, 3)) ->
        {:reply, {:error, :unknown_member}, state}

      true ->
        {:noreply, await_epoch(state, key, {from, request})}
    end
  end

  defp member?(state, key, member), do: match?(%{members: %{^member => _}}, Map.get(state.groups, key))

  defp request_key(request), do: {elem(request, 1), elem(request, 2)}

  # Parks `waiter` on the group's opening, starting one if none is under way.
  defp await_epoch(state, key, waiter) do
    case Map.fetch(state.opening, key) do
      {:ok, opening} ->
        put_in(state.opening[key], %{opening | waiting: [waiter | opening.waiting]})

      :error ->
        {group, topic} = key
        open_fun = state.open_fun
        {pid, ref} = spawn_monitor(fn -> exit({:shutdown, {:opened, open_fun.(group, topic)}}) end)
        timer = Process.send_after(self(), {:open_deadline, key, ref}, state.open_deadline_ms)
        state = put_in(state.opening_keys[ref], key)
        put_in(state.opening[key], %{pid: pid, ref: ref, timer: timer, waiting: [waiter]})
    end
  end

  # The opening of `key` ended with `result`: its waiters are served in the order they came, with the
  # epoch, or answered the error, and the next call opens again.
  defp opened(state, key, result) do
    {opening, state} = pop_in(state.opening[key])
    state = %{state | opening_keys: Map.delete(state.opening_keys, opening.ref)}

    state =
      case result do
        {:ok, epoch} -> put_in(state.epochs[key], epoch)
        {:error, _reason} -> state
      end

    opening.waiting
    |> Enum.reverse()
    |> Enum.reduce(state, fn {from, request}, state ->
      {:reply, reply, state} = if match?({:ok, _}, result), do: serve(request, state), else: {:reply, result, state}
      GenServer.reply(from, reply)
      state
    end)
  end

  defp opening_result({:shutdown, {:opened, {:ok, epoch}}})
       when is_integer(epoch) and epoch >= 0 and epoch <= 0xFFFFFFFF,
       do: {:ok, epoch}

  defp opening_result({:shutdown, {:opened, {:error, _reason} = error}}), do: error
  defp opening_result({:shutdown, {:opened, other}}), do: {:error, {:open_group, other}}
  defp opening_result(_crashed), do: {:error, :timeout}

  # A group key call, with the group's epoch at hand. The ownership is asked again, as a call may have
  # waited for the epoch.
  defp serve({_kind, group, topic, member} = request, state) when elem(request, 0) in [:join_group, :heartbeat_group] do
    key = {group, topic}

    cond do
      not state.owns_fun.(topic) ->
        {:reply, {:error, :not_owner}, state}

      elem(request, 0) == :join_group ->
        {state, group_state} = join_member(state, key, topic, member)
        {:reply, {:ok, assignment_reply(state, key, group_state, member)}, state}

      true ->
        case Map.get(state.groups, key) do
          %{members: members} = group_state when is_map_key(members, member) ->
            group_state = %{group_state | members: Map.put(members, member, state.clock.())}
            {:reply, {:ok, assignment_reply(state, key, group_state, member)}, put_in(state.groups[key], group_state)}

          _evicted_or_unknown ->
            {:reply, {:error, :unknown_member}, state}
        end
    end
  end

  defp serve({:validate_commit, group, topic, member, generation, range_ids}, state) do
    key = {group, topic}

    with true <- state.owns_fun.(topic) || {:error, :not_owner},
         %{members: members} = group_state when is_map_key(members, member) <-
           Map.get(state.groups, key, {:error, :unknown_member}),
         :ok <- commit_allowed(state.epochs[key], group_state, member, generation, range_ids) do
      # a commit is a member alive as much as a heartbeat is
      group_state = %{group_state | members: Map.put(members, member, state.clock.())}
      {:reply, :ok, put_in(state.groups[key], group_state)}
    else
      {:error, _reason} = error -> {:reply, error, state}
      _evicted_or_unknown -> {:reply, {:error, :unknown_member}, state}
    end
  end

  defp assignment_reply(state, key, group_state, member),
    do: %{
      generation: generation(state, key, group_state),
      ranges: ranges_of(group_state, member),
      session_ms: state.session_ms
    }

  # The per-range fence: the generation is of this epoch and not past its count, every range is the
  # member's, and each was handed to it at or before the generation it commits at.
  defp commit_allowed(epoch, group_state, member, generation, range_ids) do
    count = Bitwise.band(generation, 0xFFFFFFFF)
    owned = Map.new(ranges_of(group_state, member), &{&1, true})

    cond do
      Bitwise.bsr(generation, 32) != epoch or count > group_state.generation ->
        {:error, :stale_generation}

      not Enum.all?(range_ids, &is_map_key(owned, &1)) ->
        {:error, :not_assigned}

      Enum.any?(range_ids, &(Map.fetch!(group_state.assigned_at, &1) > count)) ->
        {:error, :stale_generation}

      true ->
        :ok
    end
  end

  # What a group key hands a member: the group's epoch above its rebalance count (see the moduledoc).
  defp generation(state, key, group_state), do: Bitwise.bsl(Map.fetch!(state.epochs, key), 32) + group_state.generation

  defp tick_ms(session_ms), do: min(@default_tick_ms, max(div(session_ms, 3), 1))

  @impl true
  def handle_info(:tick, state) do
    state = reconcile(state)
    schedule(state)
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.fetch(state.opening_keys, ref) do
      {:ok, key} ->
        Process.cancel_timer(state.opening[key].timer)
        {:noreply, opened(state, key, opening_result(reason))}

      :error ->
        {:noreply, state}
    end
  end

  # An opening past its deadline: its waiters are answered `timeout` and its process is let go; a late
  # epoch it brings back is not taken, and the next call opens again.
  def handle_info({:open_deadline, key, ref}, state) do
    case Map.fetch(state.opening, key) do
      {:ok, %{ref: ^ref, pid: pid}} ->
        Process.demonitor(ref, [:flush])
        Process.exit(pid, :kill)
        {:noreply, opened(state, key, {:error, :timeout})}

      _done ->
        {:noreply, state}
    end
  end

  # Evict members silent past the session window, then rebalance every group; a group with no live members
  # is dropped so state does not leak.
  defp reconcile(state) do
    cutoff = state.clock.() - state.session_ms

    groups =
      Enum.reduce(state.groups, %{}, fn {{_group, topic} = key, group_state}, acc ->
        live = for {member, hb} <- group_state.members, hb >= cutoff, into: %{}, do: {member, hb}
        settle(acc, key, %{group_state | members: live}, topic, state.ranges_fun)
      end)

    prune_epochs(%{state | groups: groups})
  end

  # A group dropped for having no member drops its epoch too; the next coordinator call for it opens one.
  defp prune_epochs(state), do: %{state | epochs: Map.take(state.epochs, Map.keys(state.groups))}

  # Adds `member` to the group (creating it if new) and rebalances; returns the updated state + group.
  defp join_member(state, key, topic, member) do
    group_state =
      Map.get(state.groups, key, %{members: %{}, assignment: %{}, generation: 0, assigned_at: %{}})

    members = Map.put(group_state.members, member, state.clock.())
    group_state = rebalance(%{group_state | members: members}, topic, state.ranges_fun)
    {put_in(state.groups[key], group_state), group_state}
  end

  # Drop the group when it has no members, else rebalance it and keep it.
  defp settle(groups, key, group_state, topic, ranges_fun) do
    if map_size(group_state.members) == 0 do
      Map.delete(groups, key)
    else
      Map.put(groups, key, rebalance(group_state, topic, ranges_fun))
    end
  end

  # Recompute the assignment over the current members and ranges; bump the generation only if it changed
  # (level-triggered, safe to run every tick). A range that stays with its owner keeps the count it was
  # handed over at; one that changes hands, or is new, is handed over at the new count.
  defp rebalance(group_state, topic, ranges_fun) do
    assignment = Assignment.assign(ranges_fun.(topic), Map.keys(group_state.members))

    if assignment == group_state.assignment do
      group_state
    else
      generation = group_state.generation + 1
      owners = owners(group_state.assignment)

      assigned_at =
        for {member, ranges} <- assignment, range <- ranges, into: %{} do
          if owners[range] == member,
            do: {range, Map.fetch!(group_state.assigned_at, range)},
            else: {range, generation}
        end

      %{group_state | assignment: assignment, generation: generation, assigned_at: assigned_at}
    end
  end

  defp owners(assignment), do: for({member, ranges} <- assignment, range <- ranges, into: %{}, do: {range, member})

  defp ranges_of(group_state, member), do: Map.get(group_state.assignment, member, [])

  defp schedule(state), do: Process.send_after(self(), :tick, state.tick_ms)
end
