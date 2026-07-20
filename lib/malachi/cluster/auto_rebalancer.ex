defmodule Malachi.Cluster.AutoRebalancer do
  @moduledoc """
  Level-triggered **automatic** rebalancing (opt-in). A periodic reconcile that, on the lease holder,
  commits the current rebalancing plan once it has stayed the **same for `stabilization` consecutive
  ticks**, so a transient SWIM membership flap (a node briefly suspected, then alive again) never
  triggers a vnode move. This is the k8s-controller pattern: SWIM does the fast, event-driven *detection*;
  the *decision* to move vnodes reconciles desired-vs-actual and converges.

  It is **policy** on top of the `Malachi.Cluster.RebalanceCoordinator` *mechanism* (which stays
  manual-by-default): the seams `:plan_fun` / `:commit_fun` / `:leader?` wire to the coordinator and the
  lease holder, so the timing logic is testable by driving one reconcile with the timer out of the way.
  Off by default: nothing moves unless it is enabled and this node holds the lease.

  ## Options
    * `:plan_fun`   - `(-> [Rebalance.change()])`, the current plan (wired to `RebalanceCoordinator.plan/1`)
    * `:commit_fun` - `(-> commit_result)`, commit under the lease (wired to `RebalanceCoordinator.commit/1`)
    * `:leader?`    - `(-> boolean())`, whether this node holds the lease (only the leader drives)
    * `:interval`   - ms between reconciles (default 30_000)
    * `:stabilization` - consecutive identical, non-empty plans required before committing (default 3)
    * `:on_result`  - `(commit_result -> any)`, called with each commit result (default: logs)
    * `:name`       - optional registered name
  """

  use GenServer
  require Logger

  @default_interval 30_000
  @default_stabilization 3

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_server_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @doc "Runs one reconcile immediately (bypasses the timer). Useful in tests and to force a check."
  @spec reconcile_now(GenServer.server()) :: {plan :: list(), stable_count :: non_neg_integer()}
  def reconcile_now(server), do: GenServer.call(server, :reconcile_now, :infinity)

  @impl true
  def init(opts) do
    state = %{
      plan_fun: Keyword.fetch!(opts, :plan_fun),
      commit_fun: Keyword.fetch!(opts, :commit_fun),
      leader?: Keyword.fetch!(opts, :leader?),
      interval: Keyword.get(opts, :interval, @default_interval),
      # at least 1: a 0 would defeat the flap protection (commit on the very first non-empty plan)
      stabilization: max(1, Keyword.get(opts, :stabilization, @default_stabilization)),
      on_result: Keyword.get(opts, :on_result, &log_result/1),
      last_plan: [],
      stable_count: 0
    }

    schedule(state)
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    state = reconcile(state)
    schedule(state)
    {:noreply, state}
  end

  @impl true
  def handle_call(:reconcile_now, _from, state) do
    state = reconcile(state)
    {:reply, {state.last_plan, state.stable_count}, state}
  end

  # Only the lease holder drives; a non-leader (or a node that just lost the lease) resets its counter so
  # stabilization restarts cleanly if it becomes leader again.
  defp reconcile(%{leader?: leader?} = state) do
    if leader?.(), do: on_leader(state), else: %{state | last_plan: [], stable_count: 0}
  end

  defp on_leader(%{last_plan: last} = state) do
    case state.plan_fun.() do
      # nothing to do: reset so a later change starts its stabilization window fresh
      [] -> %{state | last_plan: [], stable_count: 0}
      # unchanged plan (non-empty; the empty case is handled above): one more stable tick
      ^last -> maybe_commit(state, last, state.stable_count + 1)
      # a new/changed plan restarts the window
      plan -> maybe_commit(state, plan, 1)
    end
  end

  defp maybe_commit(state, plan, count) do
    if count >= state.stabilization do
      # commit re-checks leadership itself and stops if the lease was lost mid-commit; reset afterwards so
      # the next tick recomputes from the (now-changed) actual state rather than re-committing a stale plan.
      state.on_result.(state.commit_fun.())
      %{state | last_plan: [], stable_count: 0}
    else
      %{state | last_plan: plan, stable_count: count}
    end
  end

  defp schedule(state), do: Process.send_after(self(), :tick, state.interval)

  defp log_result({:ok, []}), do: :ok
  defp log_result({:ok, applied}), do: Logger.info("auto-rebalance committed: #{inspect(applied)}")
  defp log_result({:error, :not_leader}), do: :ok

  defp log_result({:error, {applied, failure}}) do
    Logger.warning("auto-rebalance partial: applied=#{inspect(applied)} failure=#{inspect(failure)}")
  end
end
