defmodule ChaosCheckerTest do
  # The chaos harness's checker is a script, not a library, so it is loaded here rather than compiled
  # into the app. Its drain policy is what turned a visibility lag into a reported durability failure
  # (issue #75), so that policy is pinned here with an injected fetch, clock and sleep: no cluster, no
  # containers, no wall-clock waiting.
  use ExUnit.Case, async: true

  setup_all do
    # Safe to require: the script only runs a mode outside :test, which is the whole reason that guard
    # is written against Mix.env rather than a flag someone has to remember to set.
    Code.require_file("scripts/chaos_checker.exs")
    :ok
  end

  # A fetch that replays a script of pages. Each element is the values of one page; `[]` models a poll
  # that found nothing right now. Pages are consumed in order regardless of cursor, which is enough to
  # exercise the policy (the cursor's own correctness belongs to the server's fetch).
  defp scripted(pages) do
    {:ok, agent} = Agent.start_link(fn -> pages end)

    fetch = fn conn, _topic, _cursor ->
      case Agent.get_and_update(agent, fn
             [] -> {[], []}
             [page | rest] -> {page, rest}
           end) do
        [] -> {:ok, [], "cursor", conn}
        values -> {:ok, values, "cursor", conn}
      end
    end

    {fetch, agent}
  end

  # A clock the test advances by hand: every reading of `now` moves it by `step_ms`, and `sleep` adds
  # the slept time, so a settle budget is exhausted deterministically without any real delay.
  defp fake_clock(step_ms \\ 0) do
    {:ok, clock} = Agent.start_link(fn -> 0 end)

    now = fn -> Agent.get_and_update(clock, fn t -> {t, t + step_ms} end) end
    sleep = fn ms -> Agent.update(clock, &(&1 + ms)) end

    {now, sleep}
  end

  defp drain(pages, opts) do
    {fetch, _agent} = scripted(pages)
    {now, sleep} = fake_clock()
    opts = Keyword.merge([now: now, sleep: sleep, settle_ms: 1_000, poll_ms: 250], opts)
    {values, :conn} = ChaosChecker.drain(fetch, :conn, "topic", opts)
    values
  end

  test "reads every page until the log is genuinely drained" do
    values = drain([["a", "b"], ["c"], []], [])
    assert Enum.sort(values) == ["a", "b", "c"]
  end

  test "an empty page does not end the scan: values arriving after it are still read" do
    # The exact shape of issue #75. The old scan stopped at the first empty page and reported
    # everything after it as lost, on a cluster that had lost nothing.
    values = drain([["a"], [], [], ["late"], []], [])

    assert "late" in values, "a value that arrived after an empty poll must still be read"
    assert Enum.sort(values) == ["a", "late"]
  end

  test "a value that only becomes visible after several empty polls is still read" do
    values = drain([[], [], [], [], ["slow"], []], [])
    assert Enum.sort(values) == ["slow"]
  end

  test "the scan ends once empty pages exhaust the settle budget" do
    # Nothing but empty pages: the drain has to terminate rather than poll forever.
    assert drain([], []) |> Enum.to_list() == []
  end

  test "progress resets the patience, so a long log drains in one pass" do
    # More empty gaps than the settle budget alone would tolerate, each followed by progress. With the
    # budget reset on every page that carries records, all of them are read.
    pages = Enum.flat_map(1..10, fn i -> [[], ["v#{i}"]] end) ++ [[], [], [], [], []]
    values = drain(pages, settle_ms: 600)

    assert Enum.sort(values) == Enum.sort(for i <- 1..10, do: "v#{i}")
  end

  test "a failed fetch is not an empty log: it is fatal, never a short successful page" do
    # A read that failed used to be indistinguishable from a drained topic, which is a successful wrong
    # answer. `on_error` stands in for the halt so the test can observe it.
    fetch = fn _conn, _topic, _cursor -> {:error, :unreachable} end
    {now, sleep} = fake_clock()

    assert {:halted, {:error, :unreachable}} =
             ChaosChecker.drain(fetch, :conn, "topic",
               now: now,
               sleep: sleep,
               on_error: fn other -> {:halted, other} end
             )
  end

  test "the settle budget is spent in poll-sized steps, not busy-looped" do
    slept = :counters.new(1, [])
    {fetch, _agent} = scripted([])
    {now, sleep} = fake_clock()

    counting_sleep = fn ms ->
      :counters.add(slept, 1, 1)
      sleep.(ms)
    end

    {_values, :conn} =
      ChaosChecker.drain(fetch, :conn, "topic",
        now: now,
        sleep: counting_sleep,
        settle_ms: 1_000,
        poll_ms: 250
      )

    # 1000ms of patience in 250ms steps: four sleeps, not a spin.
    assert :counters.get(slept, 1) == 4
  end
end
