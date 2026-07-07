defmodule Malachi.Cluster.LeaseHolder do
  @moduledoc """
  The lease **client** (R0-b): a GenServer that keeps trying to hold a `Malachi.Cluster.Lease` and runs
  the leader-election timer triangle (Kubernetes-style), so exactly one node runs the fenced work.

  On each `:retry_period_ms` tick it calls the injected `renew` seam (acquire-or-renew):

    * a **follower** that acquires becomes **leader** and calls `on_acquired` with the fencing token;
    * a **leader** that renews stays leader (recording the local time of the successful renewal);
    * a leader told the lease is **held by another** drops leadership immediately (`on_lost`);
    * a leader that **cannot reach** the lease keeps trying until `:renew_deadline_ms` has passed since
      its last successful renewal, then **proactively drops** leadership (`on_lost`) — the k8s
      *OnStoppedLeading*: give up before the lease could expire and be stolen, so two nodes never both
      believe they lead.

  The triangle must satisfy `lease duration > renew_deadline_ms > retry_period_ms`: the holder gives up
  (deadline) before the lease expires (duration), leaving a safety margin for another node to take over;
  it retries several times (retry period) within the deadline. Elapsed time is measured on the **local**
  clock (`:clock`), tolerating clock skew as k8s does (assumes NTP). On normal shutdown a leader releases
  the lease, so the next holder can take over without waiting for expiry.

  Seams (all injected, so the timing logic is testable without ra):

    * `:renew` - `(-> {:ok, fence} | {:error, :held} | {:error, reason})` (`:held` means another node
      holds the lease; any other error means it could not be reached/renewed this tick);
    * `:release` - `(fence -> any)`, best-effort release on shutdown;
    * `:on_acquired` - `(fence -> any)`, leadership gained;
    * `:on_lost` - `(-> any)`, leadership lost/dropped;
    * `:retry_period_ms` (default 2_000), `:renew_deadline_ms` (default 10_000);
    * `:clock` - `(-> integer())` monotonic ms (default `System.monotonic_time/1`);
    * `:name` - optional registered name.
  """

  use GenServer

  @default_retry_period_ms 2_000
  @default_renew_deadline_ms 10_000

  @doc "Starts the holder. See the module doc for required seams."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_server_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @doc "Runs one election tick synchronously; returns `{role, fence}` (a manual trigger, e.g. for tests)."
  @spec tick_now(GenServer.server()) :: {:follower | :leader, non_neg_integer() | nil}
  def tick_now(server), do: GenServer.call(server, :tick_now)

  @doc "Whether this node currently holds the lease. A plain read — does not force a tick."
  @spec leader?(GenServer.server()) :: boolean()
  def leader?(server), do: GenServer.call(server, :leader?)

  @impl true
  def init(opts) do
    state = %{
      renew: Keyword.fetch!(opts, :renew),
      release: Keyword.get(opts, :release, fn _fence -> :ok end),
      on_acquired: Keyword.get(opts, :on_acquired, fn _fence -> :ok end),
      on_lost: Keyword.get(opts, :on_lost, fn -> :ok end),
      retry_period_ms: Keyword.get(opts, :retry_period_ms, @default_retry_period_ms),
      renew_deadline_ms: Keyword.get(opts, :renew_deadline_ms, @default_renew_deadline_ms),
      clock: Keyword.get(opts, :clock, fn -> System.monotonic_time(:millisecond) end),
      role: :follower,
      fence: nil,
      last_renew_at: nil
    }

    schedule(state)
    {:ok, state}
  end

  @impl true
  def handle_call(:tick_now, _from, state) do
    state = tick(state)
    {:reply, {state.role, state.fence}, state}
  end

  def handle_call(:leader?, _from, state) do
    {:reply, state.role == :leader, state}
  end

  @impl true
  def handle_info(:tick, state) do
    state = tick(state)
    schedule(state)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{role: :leader, fence: fence} = state) do
    # best-effort: hand the lease back so the next holder need not wait for expiry
    state.release.(fence)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  defp schedule(state), do: Process.send_after(self(), :tick, state.retry_period_ms)

  defp tick(%{role: :follower} = state) do
    case state.renew.() do
      {:ok, fence} -> become_leader(state, fence)
      _held_or_unavailable -> state
    end
  end

  defp tick(%{role: :leader} = state) do
    case state.renew.() do
      {:ok, fence} when fence == state.fence ->
        # normal renewal: keep leadership, mark the successful renewal on the local clock
        %{state | last_renew_at: state.clock.()}

      {:ok, fence} ->
        # the fence advanced: we lost and re-acquired the lease (a gap in leadership). Signal the old
        # work to stop, then start fresh under the new token.
        state |> lose_leadership() |> become_leader(fence)

      {:error, :held} ->
        # someone else holds it now — we have definitively lost
        lose_leadership(state)

      {:error, _unreachable} ->
        # could not renew (unreachable/timeout/any non-held error): give up only once the renew deadline
        # has passed since the last successful renewal (OnStoppedLeading)
        maybe_give_up(state)
    end
  end

  defp become_leader(state, fence) do
    state = %{state | role: :leader, fence: fence, last_renew_at: state.clock.()}
    state.on_acquired.(fence)
    state
  end

  defp lose_leadership(state) do
    state.on_lost.()
    %{state | role: :follower, fence: nil, last_renew_at: nil}
  end

  defp maybe_give_up(state) do
    if state.clock.() - state.last_renew_at >= state.renew_deadline_ms do
      lose_leadership(state)
    else
      state
    end
  end
end
