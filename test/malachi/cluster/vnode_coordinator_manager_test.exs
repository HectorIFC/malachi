defmodule Malachi.Cluster.VnodeCoordinatorManagerTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.VnodeCoordinatorManager, as: Manager

  # Starts a manager whose "led vnodes" come from an Agent (so a test can change leadership between
  # reconciles) and whose spawn/stop report to the test as {:spawn, vnode_id} / {:stop, handle}. The
  # interval is long so the scheduled tick never fires mid-test; reconcile_now/1 drives it instead.
  defp start_manager(leading_agent) do
    test_pid = self()

    {:ok, manager} =
      Manager.start_link(
        leading: fn -> Agent.get(leading_agent, & &1) end,
        spawn: fn vnode_id ->
          send(test_pid, {:spawn, vnode_id})
          {:handle, vnode_id}
        end,
        stop: fn handle -> send(test_pid, {:stop, handle}) end,
        interval: 60_000
      )

    manager
  end

  test "starts a coordinator handle for each led vnode on startup" do
    {:ok, leading} = Agent.start_link(fn -> [:a, :b] end)
    manager = start_manager(leading)

    assert_receive {:spawn, :a}
    assert_receive {:spawn, :b}
    assert Enum.sort(Manager.reconcile_now(manager)) == [:a, :b]
  end

  test "stops vnodes it no longer leads and starts newly-led ones, leaving unchanged ones alone" do
    {:ok, leading} = Agent.start_link(fn -> [:a, :b] end)
    manager = start_manager(leading)
    assert_receive {:spawn, :a}
    assert_receive {:spawn, :b}

    Agent.update(leading, fn _ -> [:b, :c] end)
    assert Enum.sort(Manager.reconcile_now(manager)) == [:b, :c]

    assert_receive {:stop, {:handle, :a}}
    assert_receive {:spawn, :c}
    # b was already running and is still led: it must not be stopped or respawned
    refute_receive {:stop, {:handle, :b}}
    refute_receive {:spawn, :b}
  end

  test "is idempotent while leadership is stable" do
    {:ok, leading} = Agent.start_link(fn -> [:a] end)
    manager = start_manager(leading)
    assert_receive {:spawn, :a}

    assert Manager.reconcile_now(manager) == [:a]
    refute_receive {:spawn, :a}
    refute_receive {:stop, _}
  end

  test "stops every coordinator once this node leads no vnodes" do
    {:ok, leading} = Agent.start_link(fn -> [:a, :b] end)
    manager = start_manager(leading)
    assert_receive {:spawn, :a}
    assert_receive {:spawn, :b}

    Agent.update(leading, fn _ -> [] end)
    assert Manager.reconcile_now(manager) == []
    assert_receive {:stop, {:handle, :a}}
    assert_receive {:stop, {:handle, :b}}
  end
end
