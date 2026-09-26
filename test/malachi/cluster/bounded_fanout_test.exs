defmodule Malachi.Cluster.BoundedFanoutTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.BoundedFanout

  test "no items means no tasks and no answer" do
    # `max_concurrency: 0` would raise, so the empty case is its own clause rather than a lucky path.
    assert BoundedFanout.map([], 50, fn _ -> flunk("must not run") end, fn _ -> flunk("must not run") end) == []
  end

  test "results come back in the input's order, whatever order they finish in" do
    # The caller pairs each answer with the thing it asked about, so the order is part of the contract,
    # not an accident of scheduling. The sleeps invert completion order against input order.
    items = [30, 20, 10]

    assert BoundedFanout.map(items, 1_000, fn ms -> Process.sleep(ms) && ms end, fn item -> {:failed, item} end) ==
             items
  end

  test "the calls overlap rather than queue behind each other" do
    # The whole point. Ten calls of 200ms cost 2s in sequence and about 200ms concurrently.
    items = Enum.to_list(1..10)

    {elapsed_us, results} =
      :timer.tc(fn ->
        BoundedFanout.map(items, 2_000, fn i -> Process.sleep(200) && i end, fn i -> {:failed, i} end)
      end)

    assert results == items
    assert elapsed_us < 1_500_000, "ten 200ms calls took #{div(elapsed_us, 1000)}ms; they ran in sequence"
  end

  test "an item whose call overruns the bound takes its failure value, and the rest still answer" do
    # A call that outlives its own timeout is what a redirect chase looks like from here: the item has
    # to be named in the result, which is why the answers are zipped back against the input.
    items = [:fast, :wedged, :also_fast]

    results =
      BoundedFanout.map(
        items,
        100,
        fn
          :wedged -> Process.sleep(60_000)
          other -> other
        end,
        fn item -> {:failed, item} end
      )

    assert results == [:fast, {:failed, :wedged}, :also_fast]
  end

  test "a call that raises propagates instead of being reported as a failed item" do
    # `on_failure` covers a call that did not come back, not one that blew up. The tasks are linked, so
    # an exception takes the CALLER down with it: in the reconcile task that is a crashed pass, logged
    # and counted, and at boot it fails init and the supervisor restarts the broker. Run in a monitored
    # process of its own, because the whole point is that it would otherwise kill this test.
    {pid, ref} =
      spawn_monitor(fn ->
        BoundedFanout.map(
          [:ok_one, :boom],
          1_000,
          fn
            :boom -> raise "boom"
            other -> other
          end,
          fn item -> {:failed, item} end
        )
      end)

    assert_receive {:DOWN, ^ref, :process, ^pid, reason}, 5_000
    assert {%RuntimeError{message: "boom"}, _stacktrace} = reason
  end
end
