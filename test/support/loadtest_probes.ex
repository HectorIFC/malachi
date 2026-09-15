defmodule Malachi.Test.LoadtestProbes do
  @moduledoc """
  Observes a load generator from outside it, for the tests of both generators.

  The questions those tests ask are about ORDER and COUNT on the server: did the measure marker appear
  only after every connection had authenticated, and how many authentications did a run perform. Neither
  generator can be trusted to report either about itself, since each is the thing under test, so the
  answers come from the server's own `[:malachi, :auth]` telemetry and from polling the file system.
  """

  @poll_ms 5

  @doc """
  Starts forwarding every authentication the server performs to `pid` as
  `{:auth, result, monotonic_ms}`. Returns the handler id for `stop_auth/1`.
  """
  @spec watch_auth(pid()) :: String.t()
  def watch_auth(pid \\ self()) do
    id = "loadtest-probe-auth-#{System.unique_integer([:positive])}"
    :ok = :telemetry.attach(id, [:malachi, :auth], &__MODULE__.forward_auth/4, pid)
    id
  end

  @doc "Stops the forwarding started by `watch_auth/1`."
  @spec stop_auth(String.t()) :: :ok | {:error, :not_found}
  def stop_auth(id), do: :telemetry.detach(id)

  @doc false
  def forward_auth(_event, _measurements, %{result: result}, pid) do
    send(pid, {:auth, result, System.monotonic_time(:millisecond)})
  end

  @doc """
  Drains the `{:auth, :ok, ms}` messages already in the mailbox, oldest first. Failed authentications are
  drained too but not returned: the tests count connections that were admitted.
  """
  @spec successful_auths() :: [integer()]
  def successful_auths, do: drain_auths([])

  defp drain_auths(acc) do
    receive do
      {:auth, :ok, ms} -> drain_auths([ms | acc])
      {:auth, _failed, _ms} -> drain_auths(acc)
    after
      0 -> Enum.reverse(acc)
    end
  end

  @doc """
  Polls for `path` until it exists, then sends `{:file_appeared, path, monotonic_ms}` to `pid`. The
  poller is linked to the caller, so it ends with the test.
  """
  @spec watch_file(Path.t(), pid()) :: pid()
  def watch_file(path, pid \\ self()), do: spawn_link(fn -> poll_file(path, pid) end)

  defp poll_file(path, pid) do
    if File.exists?(path) do
      send(pid, {:file_appeared, path, System.monotonic_time(:millisecond)})
    else
      Process.sleep(@poll_ms)
      poll_file(path, pid)
    end
  end

  @doc "The resolution of `watch_file/2`, for tolerances in assertions about when a file appeared."
  @spec poll_ms() :: pos_integer()
  def poll_ms, do: @poll_ms
end
