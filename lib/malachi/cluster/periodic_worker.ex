defmodule Malachi.Cluster.PeriodicWorker do
  @moduledoc """
  The shape every background worker in this cluster shares: a tick on a fixed period, an interval that
  arrived from the environment and may be nonsense, and the rule that an unknown message is counted and
  survived rather than fatal (`Malachi.UnexpectedMessage`).

  Four servers grew that shape independently: `Malachi.Cluster.Scrubber`,
  `Malachi.Cluster.RetentionCoordinator`, `Malachi.Cluster.HealCoordinator` and
  `Malachi.Cluster.AutoRebalancer`. Each carried its own `schedule/1`, its own `drop_unexpected/3` and
  its own opinion on a bad interval, and the newest of them predates `Malachi.UnexpectedMessage` and had
  no catch-alls at all. This module owns that shape once, so a fifth worker inherits the decisions
  instead of re-making them.

  ## Plain functions, not a `use` macro

  Deliberate. A macro that injected `handle_call/3` catch-alls would append them after the host's own
  clauses, which is the "clauses with the same name and arity should be grouped together" warning, and
  this build compiles with `--warnings-as-errors`. It would also hide from a reader of the host which
  callbacks that host actually implements. The host writes four one-line clauses and they all delegate
  here, which keeps the behaviour in one place without hiding it.

  ## The state this module owns

  `new/3` builds the three keys read here, which the host merges into its own state:

    * `:worker` - the label unknown messages are reported under (see `Malachi.UnexpectedMessage`);
    * `:interval` - the validated tick period in ms;
    * `:unexpected_shapes` - the message shapes already logged.

  ## What a host writes

      @impl true
      def init(opts) do
        state = Map.merge(PeriodicWorker.new(opts, :scrubber, @default_interval), %{...seams...})
        PeriodicWorker.schedule(state)
        {:ok, state}
      end

      @impl true
      def handle_call(:scrub_now, _from, state), do: ...
      def handle_call(message, _from, state), do: PeriodicWorker.unknown_call(state, message)

      @impl true
      def handle_cast(message, state), do: PeriodicWorker.unknown_cast(state, message)

      @impl true
      def handle_info(:tick, state), do: PeriodicWorker.tick(state, &run/1)
      def handle_info(message, state), do: PeriodicWorker.unknown_info(state, message)

  The work runs **before** the next tick is scheduled, as all four did: a pass that takes longer than
  the interval then paces itself instead of queueing ticks behind itself. A leader gate, where the
  worker has one, stays inside the host's own function, because the synchronous trigger each of them
  exposes (`scrub_now/1`, `run_now/1`, `heal_now/1`, `reconcile_now/1`) deliberately ignores that gate.
  """

  alias Malachi.Config
  alias Malachi.UnexpectedMessage

  @typedoc """
  The keys this module owns inside a host's state, alongside whatever else that host keeps.

  `:unexpected_shapes` is `t:Malachi.UnexpectedMessage.seen/0`, left loose here on purpose: it is an
  opaque set built by that module, and naming it in a map this module hands back to a host that merges
  its own keys in would make every one of those merges an opaqueness violation to dialyzer.
  """
  @type t :: %{
          required(:worker) => UnexpectedMessage.server(),
          required(:interval) => pos_integer(),
          required(:unexpected_shapes) => term(),
          optional(any()) => any()
        }

  @doc """
  The state keys this module owns, for the host to merge into its own.

  `worker` is the `Malachi.UnexpectedMessage` label, `default_interval` the period used when `opts` has
  no `:interval` or carries one that cannot be a period. The setting name in the warning is derived from
  `worker`, so an operator reading it knows which of the workers refused its value.
  """
  @spec new(keyword(), UnexpectedMessage.server(), pos_integer()) :: t()
  def new(opts, worker, default_interval) do
    interval =
      opts
      |> Keyword.get(:interval, default_interval)
      |> Config.checked(:"#{worker}_interval", default_interval, &(is_integer(&1) and &1 > 0))

    %{worker: worker, interval: interval, unexpected_shapes: MapSet.new()}
  end

  @doc "Schedules the next `:tick` for this worker's interval."
  @spec schedule(t()) :: reference()
  def schedule(state), do: Process.send_after(self(), :tick, state.interval)

  @doc """
  Runs one pass and schedules the next tick: the whole body of a host's `handle_info(:tick, state)`.

  `run` takes the state and returns the state, so a host whose pass also returns a result adapts it
  here rather than this module guessing which element of a tuple is the state.
  """
  @spec tick(t(), (t() -> t())) :: {:noreply, t()}
  def tick(state, run) do
    state = run.(state)
    schedule(state)
    {:noreply, state}
  end

  @doc "The reply and state for a call this worker does not implement."
  @spec unknown_call(t(), term()) :: {:reply, term(), t()}
  def unknown_call(state, message) do
    {:reply, UnexpectedMessage.unknown_call_reply(), drop(state, :call, message)}
  end

  @doc "The state for a cast this worker does not implement. Nothing casts to any of them, which is the point."
  @spec unknown_cast(t(), term()) :: {:noreply, t()}
  def unknown_cast(state, message), do: {:noreply, drop(state, :cast, message)}

  @doc """
  The state for a message this worker did not plan for.

  A worker meant to run for the lifetime of the node must not die on one: the crash costs whatever the
  pass had accumulated, and an unexpected message is a fact worth surfacing rather than a reason to take
  the process down.
  """
  @spec unknown_info(t(), term()) :: {:noreply, t()}
  def unknown_info(state, message), do: {:noreply, drop(state, :info, message)}

  defp drop(state, kind, message) do
    %{state | unexpected_shapes: UnexpectedMessage.drop(state.unexpected_shapes, state.worker, kind, message)}
  end
end
