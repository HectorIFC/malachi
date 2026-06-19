defmodule Malachi.TestHelpers do
  @moduledoc """
  Helper functions for tests to avoid flaky timing issues.
  Replaces Process.sleep with deterministic polling.
  """

  @doc """
  Repeatedly calls a function until it returns a truthy value or times out.

  ## Examples

      assert eventually(fn -> 
        stats = Malachi.Queue.get_stats(queue_name)
        stats.consumers == 1
      end)
      
      # With custom timeout and interval
      assert eventually(fn -> Process.whereis(MyWorker) != nil end, 
        timeout: 5_000, 
        interval: 50
      )
  """
  def eventually(fun, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 2_000)
    interval = Keyword.get(opts, :interval, 50)
    deadline = System.monotonic_time(:millisecond) + timeout

    do_eventually(fun, deadline, interval)
  end

  defp do_eventually(fun, deadline, interval) do
    case fun.() do
      truthy when truthy not in [nil, false] ->
        truthy

      _ ->
        now = System.monotonic_time(:millisecond)

        if now >= deadline do
          # Timeout reached - return last result for better error messages
          fun.()
        else
          Process.sleep(interval)
          do_eventually(fun, deadline, interval)
        end
    end
  end

  @doc """
  Waits until a GenServer is registered with a given name.

  ## Examples

      wait_for_process(Malachi.Metrics)
      wait_for_process({:via, Registry, {Malachi.QueueRegistry, {queue_name, 0}}})
  """
  def wait_for_process(name, timeout \\ 2_000) do
    eventually(
      fn ->
        case GenServer.whereis(name) do
          nil -> false
          pid when is_pid(pid) -> pid
        end
      end,
      timeout: timeout
    )
  end

  @doc """
  Generates a unique queue name for test isolation.
  """
  def unique_queue_name(prefix \\ "test") do
    "#{prefix}_#{:erlang.unique_integer([:positive])}"
  end

  @doc """
  Generates a unique channel name for test isolation.
  """
  def unique_channel_name(prefix \\ "test") do
    "#{prefix}_#{:erlang.unique_integer([:positive])}"
  end

  @doc """
  Waits for a TCP connection to be accepted and ready.
  Useful for tests that connect to the TCP server.
  """
  def wait_for_tcp_ready(socket, timeout \\ 1_000) do
    eventually(
      fn ->
        case :inet.getopts(socket, [:active]) do
          {:ok, _} -> true
          {:error, _} -> false
        end
      end,
      timeout: timeout,
      interval: 10
    )
  end
end
