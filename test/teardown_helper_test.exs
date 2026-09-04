defmodule Malachi.Test.TeardownHelperTest do
  use ExUnit.Case, async: true

  import Malachi.Test.TeardownHelper

  # A well-behaved server: stops cleanly with the :normal reason stop_quietly asks for.
  defmodule Compliant do
    use GenServer
    def init(:ok), do: {:ok, nil}
  end

  # Reproduces the issue #88 race deterministically: the server dies with :shutdown while the stop
  # asked for :normal, which is the reason mismatch GenServer.stop/1 turns into an exit of the caller.
  defmodule ShutdownOnStop do
    use GenServer
    def init(:ok), do: {:ok, nil}
    def terminate(_reason, _state), do: exit(:shutdown)
  end

  test "stops a live, compliant server and reports :ok" do
    # start (not start_link): the subject under test is the teardown, not ExUnit's link cleanup.
    {:ok, pid} = GenServer.start(Compliant, :ok)
    ref = Process.monitor(pid)

    assert stop_quietly(pid) == :ok
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
  end

  test "an already-dead process is :ok, not a :noproc exit" do
    pid = spawn(fn -> :ok end)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}

    assert stop_quietly(pid) == :ok
  end

  test "a server that dies with :shutdown instead of :normal is :ok, not a mismatched-reason exit" do
    {:ok, pid} = GenServer.start(ShutdownOnStop, :ok)
    ref = Process.monitor(pid)

    # Bare GenServer.stop(pid) here would exit the caller with :shutdown; the helper must swallow it.
    assert stop_quietly(pid) == :ok
    assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}
  end
end
