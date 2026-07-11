defmodule Malachi.Shutdown do
  @moduledoc """
  Graceful shutdown orchestration, run from `Malachi.Application.prep_stop/1` on SIGTERM (or
  `bin/malachi stop`). Three ordered steps, so a rolling upgrade does not cut in-flight work:

    1. **quiesce** — stop accepting new client connections by terminating the TCP acceptor pool (so the
       app supervisor does not restart it). Already-accepted connections are separate spawned processes
       and keep serving.
    2. **drain** — wait a bounded window (`:shutdown_grace_ms`, default 5s) for in-flight requests to
       finish. Bounded on purpose: streaming connections stay open indefinitely, so draining until zero
       connections would never converge.
    3. **close** — close the remaining connections.

  The lease is released separately by `Malachi.Cluster.LeaseHolder.terminate/2` during the
  supervision-tree teardown that follows (fast failover instead of waiting for expiry), and `ra` persists
  to disk so a restarted node rejoins as the same member.

  The steps are seams so the orchestration (their order and the drain window) is unit-testable without
  stopping the running application.
  """

  @acceptor_pool Malachi.TCPAcceptorPool
  @root_supervisor Malachi.Supervisor

  @doc """
  Runs the ordered graceful-shutdown steps. Options (all defaulted to the real effects) let a test drive
  the orchestration with spies:

    * `:quiesce`  - `(-> any)`, stop accepting new connections
    * `:drain_ms` - the bounded drain window (default `:shutdown_grace_ms` config, else 5000)
    * `:sleep`    - `(ms -> any)`, the drain wait (default `Process.sleep/1`)
    * `:close`    - `(-> any)`, close remaining connections
  """
  @spec graceful(keyword()) :: :ok
  def graceful(opts \\ []) do
    quiesce = Keyword.get(opts, :quiesce, &quiesce_acceptors/0)
    sleep = Keyword.get(opts, :sleep, &Process.sleep/1)
    close = Keyword.get(opts, :close, &close_connections/0)
    drain_ms = Keyword.get(opts, :drain_ms, Application.get_env(:malachi, :shutdown_grace_ms, 5_000))

    quiesce.()
    if drain_ms > 0, do: sleep.(drain_ms)
    close.()
    :ok
  end

  # Terminate (not restart) the acceptor pool so no new connections are accepted and the supervisor does
  # not bring it back. Best-effort: a missing/already-stopped child is fine.
  defp quiesce_acceptors do
    _ = Supervisor.terminate_child(@root_supervisor, @acceptor_pool)
    :ok
  end

  defp close_connections do
    Malachi.ConnectionRegistry.close_all()
  rescue
    ArgumentError -> :ok
  catch
    _, _ -> :ok
  end
end
