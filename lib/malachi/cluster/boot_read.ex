defmodule Malachi.Cluster.BootRead do
  @moduledoc """
  The one retry a boot-time read of a replicated store needs, shared by every store a node has to read
  before it may serve.

  The retry exists for the ordinary shape of a full-cluster restart: the first node up cannot reach a
  quorum until a second one joins, and both are in boot at the time. Its Raft server votes as soon as
  distribution is up, well before the supervision tree finishes, so the wait is normally short.

  Two stores read this way, for the same reason and with the same consequence if they read a guess
  instead: `Malachi.Cluster.RingBoot`, because a node that cannot see the ring does not know which vnode
  owns which arc, and `Malachi.Cluster.ClusterFlags`, because a node that cannot see the flags does not
  know whether it may serve at all.

  A definite answer returns immediately; only `{:error, _}` is retried, and the **last** error is what
  comes back on timeout, so the caller can name what actually went wrong.
  """

  @default_timeout_ms 60_000
  @default_retry_interval_ms 500

  @doc "The wait a store gets by default before the caller has to decide what an unreadable store means."
  @spec default_timeout_ms() :: pos_integer()
  def default_timeout_ms, do: @default_timeout_ms

  @doc """
  Calls `read` until it answers something other than `{:error, _}`, or until `:timeout_ms` elapses.

  Options: `:timeout_ms` (default `#{@default_timeout_ms}`), `:retry_interval_ms`
  (default `#{@default_retry_interval_ms}`), and the `:sleep` / `:elapsed_ms` seams tests use to keep
  the clock out of it.
  """
  @spec until_answered((-> answer), keyword()) :: answer when answer: term()
  def until_answered(read, opts \\ []) do
    timeout_ms = Keyword.get(opts, :timeout_ms, @default_timeout_ms)
    interval_ms = Keyword.get(opts, :retry_interval_ms, @default_retry_interval_ms)
    sleep = Keyword.get(opts, :sleep, &Process.sleep/1)
    started = System.monotonic_time(:millisecond)
    elapsed = Keyword.get(opts, :elapsed_ms, fn -> System.monotonic_time(:millisecond) - started end)

    retry(read, timeout_ms, interval_ms, sleep, elapsed)
  end

  defp retry(read, timeout_ms, interval_ms, sleep, elapsed) do
    case read.() do
      {:error, _reason} = error ->
        if elapsed.() >= timeout_ms do
          error
        else
          sleep.(interval_ms)
          retry(read, timeout_ms, interval_ms, sleep, elapsed)
        end

      answered ->
        answered
    end
  end
end
