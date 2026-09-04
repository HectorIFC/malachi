defmodule Malachi.Cluster.VnodeCoordinatorManagerTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.VnodeCoordinatorManager, as: Manager

  # Starts a manager whose "led vnodes" come from an Agent (so a test can change leadership between
  # reconciles) and whose spawn/stop report to the test as {:spawn, vnode_id, pid} / {:stop, pid}.
  # spawn returns a real, monitorable pid (the manager monitors it); stop kills it. The interval
  # defaults long so the scheduled tick never fires mid-test and reconcile_now/1 drives it; the death
  # tests pass a short interval so the tick drives the respawn.
  defp start_manager(leading_agent, opts \\ []) do
    test_pid = self()

    # The fake coordinators below are unlinked sleepers, so nothing reaps them when the test ends and
    # they would pile up in the VM across runs. Collect every spawned pid (post-death replacements
    # included) and kill them in on_exit. The collector is unlinked so it outlives the manager (which
    # is linked to the test pid and so already dead by the time on_exit runs).
    {:ok, spawned} = Agent.start(fn -> [] end)

    on_exit(fn ->
      spawned |> Agent.get(& &1) |> Enum.each(&Process.exit(&1, :kill))
      Agent.stop(spawned)
    end)

    {:ok, manager} =
      Manager.start_link(
        leading: fn -> Agent.get(leading_agent, & &1) end,
        spawn: fn vnode_id ->
          pid = spawn(fn -> Process.sleep(:infinity) end)
          Agent.update(spawned, &[pid | &1])
          send(test_pid, {:spawn, vnode_id, pid})
          pid
        end,
        stop: fn pid ->
          send(test_pid, {:stop, pid})
          Process.exit(pid, :kill)
        end,
        interval: Keyword.get(opts, :interval, 60_000)
      )

    manager
  end

  test "starts a coordinator handle for each led vnode on startup" do
    {:ok, leading} = Agent.start_link(fn -> [:a, :b] end)
    manager = start_manager(leading)

    assert_receive {:spawn, :a, _}
    assert_receive {:spawn, :b, _}
    assert Enum.sort(Manager.reconcile_now(manager)) == [:a, :b]
  end

  test "stops vnodes it no longer leads and starts newly-led ones, leaving unchanged ones alone" do
    {:ok, leading} = Agent.start_link(fn -> [:a, :b] end)
    manager = start_manager(leading)
    assert_receive {:spawn, :a, pid_a}
    assert_receive {:spawn, :b, pid_b}

    Agent.update(leading, fn _ -> [:b, :c] end)
    assert Enum.sort(Manager.reconcile_now(manager)) == [:b, :c]

    assert_receive {:stop, ^pid_a}
    assert_receive {:spawn, :c, _}
    # b was already running and is still led: it must not be stopped or respawned
    refute_receive {:stop, ^pid_b}
    refute_receive {:spawn, :b, _}
  end

  test "is idempotent while leadership is stable" do
    {:ok, leading} = Agent.start_link(fn -> [:a] end)
    manager = start_manager(leading)
    assert_receive {:spawn, :a, _}

    assert Manager.reconcile_now(manager) == [:a]
    refute_receive {:spawn, :a, _}
    refute_receive {:stop, _}
  end

  test "stops every coordinator once this node leads no vnodes" do
    {:ok, leading} = Agent.start_link(fn -> [:a, :b] end)
    manager = start_manager(leading)
    assert_receive {:spawn, :a, pid_a}
    assert_receive {:spawn, :b, pid_b}

    Agent.update(leading, fn _ -> [] end)
    assert Manager.reconcile_now(manager) == []
    assert_receive {:stop, ^pid_a}
    assert_receive {:stop, ^pid_b}
  end

  @tag :capture_log
  test "restarts a vnode whose coordinator tree dies while this node still leads it" do
    {:ok, leading} = Agent.start_link(fn -> [:a] end)
    manager = start_manager(leading, interval: 50)
    assert_receive {:spawn, :a, pid1}

    # The coordinator tree dies on its own (its supervisor exhausted its restart intensity), not via a
    # deliberate stop. The manager must notice and start a fresh one while :a is still led. On main
    # nothing monitors the handle, so no replacement is ever spawned and this assertion times out.
    Process.exit(pid1, :kill)

    assert_receive {:spawn, :a, pid2}, 1_000
    assert pid2 != pid1
    assert Manager.reconcile_now(manager) == [:a]
  end

  test "a deliberate stop does not trigger a respawn" do
    {:ok, leading} = Agent.start_link(fn -> [:a] end)
    manager = start_manager(leading, interval: 50)
    assert_receive {:spawn, :a, pid1}

    # Stop :a deliberately by dropping leadership. The stop kills pid1, but because stop_vnodes
    # demonitors first, that death must not be delivered as a :DOWN and respawn :a.
    Agent.update(leading, fn _ -> [] end)
    assert Manager.reconcile_now(manager) == []
    assert_receive {:stop, ^pid1}

    refute_receive {:spawn, :a, _}, 300
  end
end
