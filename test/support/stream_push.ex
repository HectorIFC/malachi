defmodule Malachi.Test.StreamPush do
  @moduledoc """
  The subscriber's side of a streaming push, for a test process that subscribes itself.

  A broker no longer reads for its subscribers: it sends a subscriber `{:log_read, plan}` and the
  subscriber runs the plan (`Malachi.LogApi.execute_push/1`), which reads in the subscriber's process
  and reports back to the broker. A connection does that in its stream loop; a test process has no loop,
  so it runs the plans it received through `recv/1` and gets back what a connection would have written
  to its socket, in the shape the broker used to send: `{:log_records, topic, records, positions}`.

  A plan that reads nothing to deliver (the subscriber is caught up, or the read only moved past data
  that is gone) is still run, so the broker learns the read ended, and `recv/1` keeps waiting.
  """

  alias Malachi.LogApi

  @doc """
  Runs the plans that arrive within `timeout` ms until one delivers records, and returns them as
  `{:log_records, topic, records, positions}`, or `:timeout` if none did.
  """
  @spec recv(non_neg_integer()) :: {:log_records, term(), [term()], map()} | :timeout
  def recv(timeout \\ 1_000), do: recv_until(System.monotonic_time(:millisecond) + timeout)

  @doc "Runs every plan that arrives within `timeout` ms and returns the pushes they delivered, oldest first."
  @spec drain(non_neg_integer()) :: [{:log_records, term(), [term()], map()}]
  def drain(timeout \\ 100), do: drain_until(System.monotonic_time(:millisecond) + timeout, [])

  defp recv_until(deadline) do
    receive do
      {:log_read, plan} ->
        case LogApi.execute_push(plan) do
          {:ok, topic, records, positions} -> {:log_records, topic, records, positions}
          :nothing -> recv_until(deadline)
        end
    after
      remaining(deadline) -> :timeout
    end
  end

  defp drain_until(deadline, pushes) do
    case recv_until(deadline) do
      :timeout -> Enum.reverse(pushes)
      push -> drain_until(deadline, [push | pushes])
    end
  end

  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)
end
