defmodule Malachi.Cluster.RebalanceCoordinator do
  @moduledoc """
  The manual **plan/commit** coordinator for dynamic rebalancing (R3-b). An operator calls `plan/1` to
  see the changes that would move each vnode to the placement desired over the live membership, then
  `commit/1` to apply them. Commit is **never automatic** — nothing moves until the operator asks.

  It is a GenServer so commits serialize (one at a time) and it holds the wiring; the world is reached
  through seams, so it is testable without ra or a real lease:

    * `:plan_fun` - `(-> plan)`, the current rebalancing plan (wired to `Malachi.Application.live_rebalance_plan/5`);
    * `:add_member` / `:remove_member` - the `Malachi.Cluster.Rebalance` member ops (wired to `ra_*`);
    * `:leader?` - `(-> boolean())`, whether this node currently holds the lease (wired to the
      `LeaseHolder`); `commit/1` refuses unless it holds the lease, and passes this same check to
      `Rebalance.apply_plan/4` so a lease lost mid-commit stops the run.
    * `:name` - optional registered name.

  `commit/1` returns `Rebalance.apply_plan/4`'s result (`{:ok, applied}` / `{:error, {applied, failure}}`)
  or `{:error, :not_leader}` when this node does not hold the lease.
  """

  use GenServer

  alias Malachi.Cluster.Rebalance

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_server_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @doc "The rebalancing plan for the live cluster (computes, does not apply)."
  @spec plan(GenServer.server()) :: [Rebalance.change()]
  def plan(server), do: GenServer.call(server, :plan)

  @doc """
  Commits the current plan under the lease: refuses with `{:error, :not_leader}` unless this node holds
  the lease, otherwise runs `Rebalance.apply_plan/4` fail-fast. May take a while (it moves vnodes), so it
  uses an infinite call timeout and serializes with other commits.
  """
  @spec commit(GenServer.server()) ::
          {:ok, [atom()]} | {:error, {[atom()], Rebalance.failure()}} | {:error, :not_leader}
  def commit(server), do: GenServer.call(server, :commit, :infinity)

  @impl true
  def init(opts) do
    state = %{
      plan_fun: Keyword.fetch!(opts, :plan_fun),
      add_member: Keyword.fetch!(opts, :add_member),
      remove_member: Keyword.fetch!(opts, :remove_member),
      leader?: Keyword.fetch!(opts, :leader?)
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:plan, _from, state) do
    {:reply, state.plan_fun.(), state}
  end

  @impl true
  def handle_call(:commit, _from, state) do
    reply =
      if state.leader?.() do
        Rebalance.apply_plan(state.plan_fun.(), state.add_member, state.remove_member, state.leader?)
      else
        {:error, :not_leader}
      end

    {:reply, reply, state}
  end
end
