defmodule Malachi.Cluster.ReplicatedMetadataTest do
  # async: false: ra is global/stateful (one data dir, on-disk Raft logs).
  use ExUnit.Case, async: false

  alias Malachi.Cluster.ReplicatedMetadata
  alias Malachi.Metadata
  alias Malachi.Test.SilentRaMember

  setup_all do
    :ok
  end

  defp start do
    {:ok, replicated} = ReplicatedMetadata.start(:"rm_#{System.unique_integer([:positive])}")
    on_exit(fn -> ReplicatedMetadata.delete(replicated) end)
    replicated
  end

  test "a committed command updates the local cache (read-your-writes)" do
    replicated = start()

    {reply, replicated} = ReplicatedMetadata.command(replicated, {:create_topic, "events", 4})
    assert {:ok, root_id} = reply

    # the cache reflects the committed command without any further query
    topic = ReplicatedMetadata.metadata(replicated) |> Metadata.get_topic("events")
    assert topic.name == "events"

    {{:ok, left, right}, replicated} = ReplicatedMetadata.command(replicated, {:split_range, root_id})
    active = ReplicatedMetadata.metadata(replicated) |> Metadata.active_ranges_of_topic("events")
    assert Enum.sort(Enum.map(active, & &1.id)) == Enum.sort([left, right])
  end

  test "a rejected command leaves the cache unchanged and surfaces the machine error" do
    replicated = start()
    {{:ok, _root}, replicated} = ReplicatedMetadata.command(replicated, {:create_topic, "events", 4})

    {reply, replicated} = ReplicatedMetadata.command(replicated, {:create_topic, "events", 4})
    assert reply == {:error, :already_exists}

    # still exactly one topic in the cache
    assert ReplicatedMetadata.metadata(replicated).topics |> map_size() == 1
  end

  test "the cache equals the replicated state (refresh is a no-op for the sole writer)" do
    replicated = start()
    {{:ok, root_id}, replicated} = ReplicatedMetadata.command(replicated, {:create_topic, "events", 4})
    {{:ok, _l, _r}, replicated} = ReplicatedMetadata.command(replicated, {:split_range, root_id})

    {:ok, refreshed} = ReplicatedMetadata.refresh(replicated)
    assert refreshed.cache == replicated.cache
  end

  describe "a command the group's machine version refuses" do
    # One member pinned below the version that introduced the command plays a group caught mid rolling
    # upgrade: ra refuses the command on every member, and the cache must not apply it either.
    setup do
      previous = Application.fetch_env(:malachi, :ra_machine_version_pin)
      Application.put_env(:malachi, :ra_machine_version_pin, 3)

      on_exit(fn ->
        case previous do
          {:ok, pin} -> Application.put_env(:malachi, :ra_machine_version_pin, pin)
          :error -> Application.delete_env(:malachi, :ra_machine_version_pin)
        end
      end)
    end

    test "leaves the cache as the replicated state holds it, and surfaces the refusal" do
      replicated = start()
      {{:ok, _root}, replicated} = ReplicatedMetadata.command(replicated, {:create_topic, "events", 4})

      {reply, replicated} = ReplicatedMetadata.command(replicated, {:bind_topic_policy, "events", "short"})

      assert reply == {:error, {:unsupported_command, {:bind_topic_policy, 3}, 4, 3}}
      assert Metadata.topic_policy_name(ReplicatedMetadata.metadata(replicated), "events") == nil

      {:ok, refreshed} = ReplicatedMetadata.refresh(replicated)
      assert refreshed.cache == replicated.cache
    end
  end

  test "a binding waits for its commit at most 2 s, well inside the default, on a vnode that cannot commit" do
    # The broker applies commands inside its own loop, so a produce on the shard waits behind this.
    silent = :"rm_mute_#{System.unique_integer([:positive])}"
    {:ok, _pid} = SilentRaMember.start_link(silent)
    on_exit(fn -> SilentRaMember.stop(silent) end)

    cache = Metadata.new()

    {elapsed_us, result} =
      :timer.tc(fn -> ReplicatedMetadata.apply_command({silent, node()}, cache, {:bind_topic_policy, "t", "p"}) end)

    assert result == {cache, {:error, :timeout}}
    assert elapsed_us < 3_000_000, "waited #{div(elapsed_us, 1000)} ms"
  end

  test "the 2 s are a deadline for the whole bind, not for each hop of a redirect" do
    # ra follows a redirect with a fresh full timeout, so members that keep redirecting (an election in
    # progress) would hold the broker's loop for as many hops as they make. Only a deadline on the whole
    # command bounds it.
    suffix = System.unique_integer([:positive])
    ping = :"rm_ping_#{suffix}"
    pong = :"rm_pong_#{suffix}"
    {:ok, _pid} = SilentRaMember.start_redirecting(ping, {pong, node()}, 20)
    on_exit(fn -> SilentRaMember.stop(ping) end)
    {:ok, _pid} = SilentRaMember.start_redirecting(pong, {ping, node()}, 20)
    on_exit(fn -> SilentRaMember.stop(pong) end)

    cache = Metadata.new()

    {elapsed_us, result} =
      :timer.tc(fn -> ReplicatedMetadata.apply_command({ping, node()}, cache, {:bind_topic_policy, "t", "p"}) end)

    assert result == {cache, {:error, :timeout}}
    assert elapsed_us < 3_000_000, "waited #{div(elapsed_us, 1000)} ms"
    refute_received {_tag, _late_reply}
  end

  test "a bind whose command process crashes answers the crash, and the cache stays as it was" do
    cache = Metadata.new()

    assert {^cache, {:error, {:command_crashed, _reason}}} =
             ReplicatedMetadata.apply_command({1, 2}, cache, {:bind_topic_policy, "t", "p"})
  end

  describe "run_within/2 (the deadline on an operator's command)" do
    test "answers what the function returned, and leaves nothing behind" do
      assert ReplicatedMetadata.run_within(fn -> :answered end, 1_000) == :answered
      refute_received _anything
    end

    test "a function past the deadline is dead by the time the answer is given, and its result never arrives" do
      # The contract, not a regression guard: the kill is asynchronous, and answering before the process
      # is gone would let a result it sends in that window reach the caller's handlers as an unexpected
      # message. That window is a few instructions wide and was never hit in about 12,000 probe runs, so an
      # implementation that skips waiting for the DOWN still passes here almost every time.
      parent = self()

      result =
        ReplicatedMetadata.run_within(
          fn ->
            send(parent, {:running, self()})
            Process.sleep(200)
            :too_late
          end,
          50
        )

      assert result == {:error, :timeout}
      assert_received {:running, pid}
      refute Process.alive?(pid)
      Process.sleep(250)
      refute_received {_tag, :too_late}
      refute_received {:DOWN, _ref, :process, _pid, _reason}
    end

    test "a function that crashes answers the crash" do
      assert {:error, {:command_crashed, {%RuntimeError{message: "boom"}, _stack}}} =
               ReplicatedMetadata.run_within(fn -> raise "boom" end, 1_000)
    end
  end
end
