defmodule Malachi.Consumer.GroupCoordinator do
  @moduledoc """
  Tracks the members of consumer groups and assigns each topic's ranges across them (via
  `Malachi.Consumer.Assignment`), so members consume in parallel: one range per member.

  A group is keyed by `{group, topic}`. Members `join`, then `heartbeat` to stay alive; a member that
  stops heartbeating for `session_ms` is evicted (a dead consumer). Rebalancing is **eager**: any
  membership change: join, leave, eviction - or a change in the topic's ranges recomputes the whole
  assignment and bumps a **generation** counter; a member re-reads its assignment on the next heartbeat and
  sees the new generation, its signal to take over its (possibly changed) ranges. Because the assignment is
  deterministic and sticky (S1), a rebalance moves few ranges.

  This is the coordinator **logic** as a single GenServer, reached through seams (`:clock`, `:ranges_fun`)
  so it is testable without a cluster; which node coordinates which group (or replicating the membership)
  is a separate wiring concern. Member state is soft: a coordinator restart just makes members re-join.

  ## Options
    * `:ranges_fun` - `(topic -> [range_id])`, the topic's current ranges (default `fn _ -> [] end`)
    * `:clock`      - `(-> integer_ms)` monotonic clock (default `System.monotonic_time(:millisecond)`)
    * `:session_ms` - evict a member silent this long (default 30_000)
    * `:tick_ms`    - reconcile interval: evict + rebalance (default 10_000)
    * `:name`       - optional registered name
  """

  use GenServer

  alias Malachi.Consumer.Assignment

  @default_session_ms 30_000
  @default_tick_ms 10_000

  @type member :: term()
  @type reply :: {:ok, non_neg_integer(), [term()]} | {:error, :unknown_member | :not_owner}

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
  `{:error, :unknown_member}` if it was evicted (the caller must `join/4` again).
  """
  @spec heartbeat(GenServer.server(), term(), term(), member()) :: reply()
  def heartbeat(server, group, topic, member), do: GenServer.call(server, {:heartbeat, group, topic, member})

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
    state = %{
      groups: %{},
      ranges_fun: Keyword.get(opts, :ranges_fun, fn _topic -> [] end),
      # Whether this node owns a topic's coordination. A member routed here by a stale leadership view
      # (during a vnode failover) is rejected with `{:error, :not_owner}` so it re-resolves to the new
      # owner, rather than registering a phantom assignment here. Default `true`: single-instance / single
      # -node (and tests) always own.
      owns_fun: Keyword.get(opts, :owns_fun, fn _topic -> true end),
      clock: Keyword.get(opts, :clock, fn -> System.monotonic_time(:millisecond) end),
      session_ms: Keyword.get(opts, :session_ms, @default_session_ms),
      tick_ms: Keyword.get(opts, :tick_ms, @default_tick_ms)
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

    case Map.get(state.groups, key) do
      %{members: members} = group_state when is_map_key(members, member) ->
        group_state = %{group_state | members: Map.put(members, member, state.clock.())}
        state = put_in(state.groups[key], group_state)
        {:reply, {:ok, group_state.generation, ranges_of(group_state, member)}, state}

      _evicted_or_unknown ->
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
          %{state | groups: settle(state.groups, key, %{group_state | members: members}, topic, state.ranges_fun)}
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

  def handle_call(:reconcile_now, _from, state), do: {:reply, :ok, reconcile(state)}

  @impl true
  def handle_info(:tick, state) do
    state = reconcile(state)
    schedule(state)
    {:noreply, state}
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

    %{state | groups: groups}
  end

  # Adds `member` to the group (creating it if new) and rebalances; returns the updated state + group.
  defp join_member(state, key, topic, member) do
    group_state = Map.get(state.groups, key, %{members: %{}, assignment: %{}, generation: 0})
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
  # (level-triggered, safe to run every tick).
  defp rebalance(group_state, topic, ranges_fun) do
    assignment = Assignment.assign(ranges_fun.(topic), Map.keys(group_state.members))

    if assignment == group_state.assignment do
      group_state
    else
      %{group_state | assignment: assignment, generation: group_state.generation + 1}
    end
  end

  defp ranges_of(group_state, member), do: Map.get(group_state.assignment, member, [])

  defp schedule(state), do: Process.send_after(self(), :tick, state.tick_ms)
end
