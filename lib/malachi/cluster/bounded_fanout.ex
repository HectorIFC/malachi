defmodule Malachi.Cluster.BoundedFanout do
  @moduledoc """
  Runs one bounded remote call per item, concurrently, keeping every answer paired with its item.

  Two callers need exactly this, for the same reason: the reconcile reads every metadata vnode
  (`Malachi.Cluster.ReplicatedDSRSM.snapshot/2`) and, on the orchestrator, asks every vnode whether its
  cluster is formed (`Malachi.BrokerServer`). Each call can cost its whole timeout, so run in sequence a
  pass over `n` silent vnodes costs `n` times it. That is what used to be paid inside the broker's own
  loop while it was supposed to be serving clients (#178), and holding the pattern here is what keeps
  the next such pass from being written in sequence again.

  Two details are the reason this is a module rather than a line repeated twice:

    * the answers are zipped back against the input, because the `:exit` result of a killed task does
      not carry the item it was working on, and a caller that cannot tell WHICH item failed cannot
      report it either;
    * the stream carries a bound of its own, looser than the per-call one, so the inner call is
      normally what decides. It is not redundant. `ra` follows a `{redirect, Leader}` reply by calling
      that leader with a FRESH full timeout (`ra_server_proc:statem_call/3`), so two members that name
      each other cost unbounded time without any single call ever timing out, and only a bound on the
      whole fan-out ends it.
  """

  # How much longer the stream waits than the call it wraps. Only the redirect-chasing case above can
  # reach it, so it is slack, not a second policy.
  @stream_slack_ms 1_000

  @doc """
  Applies `fun` to every item concurrently, bounded by `timeout` milliseconds per item, and returns
  the results **in the input's order**.

  An item whose task overran that bound takes `on_failure.(item)` in place of a result, so a call that
  never came back is a value the caller reads rather than an exception it has to catch. `timeout` is a
  count of milliseconds: `:infinity` has no meaning here, since the whole point is that the fan-out
  ends.

  A `fun` that RAISES is a different case and is deliberately not contained: the tasks are linked, so
  the exception propagates to the caller, exactly as it did when these passes were written as a loop.
  That is the right shape for both callers. In the reconcile task it is caught as a crashed pass,
  logged and counted; at boot it fails `init/1` and the supervisor restarts the broker, which is the
  honest answer for a node that cannot complete its first pass. Containing it here would turn either
  into a pass that quietly reports nothing.
  """
  @spec map([item], pos_integer(), (item -> result), (item -> result)) :: [result]
        when item: term(), result: term()
  def map([], _timeout, _fun, _on_failure), do: []

  def map(items, timeout, fun, on_failure)
      when is_list(items) and is_integer(timeout) and timeout > 0 and
             is_function(fun, 1) and is_function(on_failure, 1) do
    items
    |> Task.async_stream(fun,
      max_concurrency: length(items),
      timeout: timeout + @stream_slack_ms,
      on_timeout: :kill_task
    )
    |> Enum.zip(items)
    |> Enum.map(fn
      {{:ok, result}, _item} -> result
      {{:exit, _reason}, item} -> on_failure.(item)
    end)
  end
end
