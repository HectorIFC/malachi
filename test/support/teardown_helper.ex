defmodule Malachi.Test.TeardownHelper do
  @moduledoc """
  Best-effort teardown for processes a test started with `start_link`.

  When a test finishes, ExUnit terminates the test process with reason `:shutdown`, and every process
  the test `start_link`ed receives that `:shutdown` through the link CONCURRENTLY with the `on_exit`
  callbacks, which run in their own process. A callback that checks `Process.alive?/1` and then calls
  `GenServer.stop/1` can therefore lose the race three ways: the process is already gone (`:noproc`),
  it is going down with `:shutdown` while `stop` asked for `:normal` (a reason mismatch, which
  `GenServer.stop/3` turns into an exit of the caller), or it dies mid `:sys.terminate`. None of those
  is a failure of the test that just passed, but each one fails the suite if it escapes: exactly that
  turned a green `Malachi.LogApiTest` run red on main (issue #88).

  `stop_quietly/1` is the one hardened teardown for all of them: aliveness check, a plain
  `GenServer.stop/1`, and any `:exit` swallowed as `:ok`.
  """

  @doc "Stops a test-linked process, treating every teardown exit reason as success."
  @spec stop_quietly(pid()) :: :ok
  def stop_quietly(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
    :ok
  catch
    :exit, _ -> :ok
  end
end
