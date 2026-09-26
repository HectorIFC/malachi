defmodule Malachi.ParallelProbeTest do
  # Run only by Malachi.ParallelRunTest, in a second `mix test` started while the suite holds its own node
  # name and listeners. Reaching this test at all means the second run got past naming its node and
  # starting the application; the assertions say it did so on its own ports.
  use ExUnit.Case, async: false

  @moduletag :parallel_probe

  test "this run bound its own listener ports" do
    assert Application.get_env(:malachi, :tcp_port) == 0
    assert Application.get_env(:malachi, :dashboard_port) == 0

    tcp = Malachi.TCPAcceptorPool.port()
    dashboard = Malachi.Dashboard.port()
    assert tcp > 0 and dashboard > 0

    refute tcp == String.to_integer(System.fetch_env!("MALACHI_PARENT_TCP_PORT"))
    refute dashboard == String.to_integer(System.fetch_env!("MALACHI_PARENT_DASHBOARD_PORT"))
  end
end
