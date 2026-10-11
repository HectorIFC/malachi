defmodule Malachi.ProducerDirectoryTest do
  # `Malachi.ProducerDirectory` with stand-in connections: what it keeps per connection, what a listing
  # collects, and how it treats a connection that does not answer or goes away while asked. Each test
  # looks only at its own stand-ins: the directory is the node's, and connections of earlier suites may
  # still be on their way out.
  #
  # async: false. The async suites open producer streams of their own.
  use ExUnit.Case, async: false

  alias Malachi.ProducerDirectory

  # A connection holding `count` producer streams, answering listings with `streams` (or never, `:silent`).
  defp connection(count, streams) do
    parent = self()

    pid =
      spawn(fn ->
        for _ <- 1..count//1, do: ProducerDirectory.track()
        send(parent, {:tracked, self()})
        loop(streams)
      end)

    assert_receive {:tracked, ^pid}
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp loop(streams) do
    receive do
      {:describe_producer_streams, from, ref} when streams != :silent ->
        send(from, {ref, self(), streams})
        loop(streams)

      {:untrack, from} ->
        ProducerDirectory.untrack()
        send(from, :untracked)
        loop(streams)
    end
  end

  defp held(pid) do
    case Registry.lookup(ProducerDirectory, pid) do
      [{^pid, count}] -> count
      [] -> 0
    end
  end

  defp untrack(pid) do
    send(pid, {:untrack, self()})
    assert_receive :untracked
  end

  defp stream(id, opened_at), do: %{stream_id: id, opened_at: opened_at}

  defp own(streams, pids) do
    names = Enum.map(pids, &inspect/1)
    Enum.filter(streams, &(&1.connection in names))
  end

  test "a connection is one entry, counted up and down, gone at its last stream or when it exits" do
    one = connection(2, [])
    two = connection(1, [])
    assert {held(one), held(two)} == {2, 1}
    assert ProducerDirectory.open_count() >= 3

    untrack(one)
    assert held(one) == 1
    untrack(one)
    assert Registry.lookup(ProducerDirectory, one) == []

    ref = Process.monitor(two)
    Process.exit(two, :kill)
    assert_receive {:DOWN, ^ref, :process, ^two, :killed}
    # the registry clears an exited process's entry as it learns of the exit
    assert wait_until(fn -> Registry.lookup(ProducerDirectory, two) == [] end)
  end

  test "the open count sums what every connection holds, and is 0 while the directory is not running" do
    connection(3, [])
    connection(2, [])
    assert ProducerDirectory.open_count() >= 5

    registry = Process.whereis(ProducerDirectory)
    true = Process.unregister(ProducerDirectory)
    on_exit(fn -> if Process.whereis(ProducerDirectory) == nil, do: Process.register(registry, ProducerDirectory) end)
    assert ProducerDirectory.open_count() == 0
    true = Process.register(registry, ProducerDirectory)
  end

  test "a listing collects every connection's streams, named by their connection, in their order" do
    one = connection(2, [stream(1, 30), stream(2, 10)])
    two = connection(1, [stream(1, 20)])

    {elapsed_us, %{streams: streams}} = :timer.tc(fn -> ProducerDirectory.list(5_000) end)
    own = own(streams, [one, two])
    assert Enum.map(own, &{&1.opened_at, &1.connection}) == [{10, inspect(one)}, {20, inspect(two)}, {30, inspect(one)}]
    # every connection answered: the listing does not wait out its bound
    assert elapsed_us < 2_000_000
  end

  test "a connection that does not answer in time is counted, and one that goes away is not" do
    answering = connection(1, [stream(1, 1)])
    connection(1, :silent)
    gone = connection(1, :silent)

    # this one exits as it is asked: its streams are gone, nothing is waited for
    spawn(fn ->
      Process.sleep(20)
      Process.exit(gone, :kill)
    end)

    {elapsed_us, %{streams: streams, unanswered: unanswered}} = :timer.tc(fn -> ProducerDirectory.list(300) end)
    # only the silent one: the one that went away was not waited for, nor counted
    assert unanswered == 1
    assert [%{opened_at: 1}] = own(streams, [answering])
    assert elapsed_us < 1_000_000
  end

  test "untracking on a connection that holds nothing changes nothing" do
    pid = connection(0, [])
    untrack(pid)
    assert Registry.lookup(ProducerDirectory, pid) == []
  end

  defp wait_until(check, tries \\ 100) do
    cond do
      check.() -> true
      tries == 0 -> false
      true -> Process.sleep(10) && wait_until(check, tries - 1)
    end
  end
end
