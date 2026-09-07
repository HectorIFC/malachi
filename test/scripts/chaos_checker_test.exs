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
    {values, :conn, _scan} = ChaosChecker.drain(fetch, :conn, "topic", opts)
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

  describe "the caller's deadline" do
    test "ends a scan whose pages keep arriving, so a busy topic cannot hold it open forever" do
      # The settle budget is reset by every page carrying records, which is what lets a long log drain
      # in one pass. Without an absolute ceiling that same reset lets a topic under continuous produce
      # keep the scan running indefinitely, and the revisit loop that calls it stops being bounded.
      endless = fn conn, _topic, _cursor -> {:ok, ["v"], "cursor", conn} end
      {now, sleep} = fake_clock(50)

      {_values, :conn, %{status: :timeout}} =
        ChaosChecker.drain(endless, :conn, "topic",
          now: now,
          sleep: sleep,
          settle_ms: 1_000,
          deadline: 500
        )

      # Returning at all is the assertion: without the ceiling this call does not terminate.
    end

    test "a scan starting near the deadline returns instead of spending a whole settle window" do
      {fetch, _agent} = scripted([[], [], [], []])
      {now, sleep} = fake_clock(10)

      {values, :conn, %{status: :timeout}} =
        ChaosChecker.drain(fetch, :conn, "topic",
          now: now,
          sleep: sleep,
          settle_ms: 10_000,
          poll_ms: 250,
          deadline: 20
        )

      assert Enum.to_list(values) == []
    end

    test "a sleep never overshoots the deadline" do
      {:ok, slept} = Agent.start_link(fn -> [] end)
      {fetch, _agent} = scripted([])
      {now, sleep} = fake_clock()

      recording_sleep = fn ms ->
        Agent.update(slept, &[ms | &1])
        sleep.(ms)
      end

      {_values, :conn, _scan} =
        ChaosChecker.drain(fetch, :conn, "topic",
          now: now,
          sleep: recording_sleep,
          settle_ms: 10_000,
          poll_ms: 250,
          deadline: 600
        )

      # 250 + 250 + 100: the last poll is cut to what is left rather than running past the ceiling.
      assert Agent.get(slept, & &1) |> Enum.reverse() == [250, 250, 100]
    end

    test "no deadline means the settle budget alone decides, as a plain scan expects" do
      values = drain([["a"], [], [], [], []], settle_ms: 1_000, poll_ms: 250)
      assert Enum.to_list(values) == ["a"]
    end
  end

  describe "verdict/2 (a truncated scan is not a durability failure)" do
    test "a value missing from a scan that was cut short is inconclusive, not lost" do
      # The trap this exists to avoid: a scan stopped by its ceiling read only a prefix, so an
      # acknowledged value living past the last fetched cursor is simply unread. Reporting that as
      # data loss would be the same false alarm this checker was rewritten to stop making, with a new
      # cause.
      assert ChaosChecker.verdict(1, :timeout) == :inconclusive
      assert ChaosChecker.verdict(500, :timeout) == :inconclusive
    end

    test "a value missing from a scan that reached the end is worth the alarm" do
      assert ChaosChecker.verdict(1, :settled) == :missing
    end

    test "finding everything is a pass however the scan ended" do
      # A truncated scan that nonetheless read every acknowledged value proved what it had to prove.
      assert ChaosChecker.verdict(0, :timeout) == :ok
      assert ChaosChecker.verdict(0, :settled) == :ok
    end
  end

  describe "the page trace (telling a skipped scan from a slow one)" do
    # A scan that walked the topic in three fetches, the middle one starting well above where the
    # previous ended: the cursor moved over c-6 through c-9 and the scan never returned them.
    defp skipping_pages do
      [
        %{from: nil, to: "a", count: 2, first: "c-1", last: "c-5"},
        %{from: "a", to: "b", count: 2, first: "c-10", last: "c-12"},
        %{from: "b", to: "b", count: 0, first: nil, last: nil}
      ]
    end

    test "names the two pages that bracket the missing values" do
      missing = MapSet.new(["c-6", "c-7", "c-8", "c-9"])

      assert {before, following} = ChaosChecker.skipped_at(skipping_pages(), missing)
      assert before.last == "c-5"
      assert following.first == "c-10"
    end

    test "a scan that simply stopped early has no bracketing boundary" do
      # Everything missing lies ABOVE the last page, which is a scan that ran out of time. Reporting a
      # skip here would be inventing a boundary to fit, and it is the distinction the whole trace
      # exists to draw.
      pages = [%{from: nil, to: "a", count: 2, first: "c-1", last: "c-5"}]

      assert ChaosChecker.skipped_at(pages, MapSet.new(["c-6", "c-7"])) == nil
    end

    test "values that do not carry a sequence number yield no boundary rather than a wrong one" do
      assert ChaosChecker.skipped_at(skipping_pages(), MapSet.new(["surprise"])) == nil
      assert ChaosChecker.skipped_at([], MapSet.new(["c-1"])) == nil
    end

    test "the boundary is found from the LOWEST missing value, not whichever comes first" do
      # A set that is not in order, and one whose lowest member sits in the gap while others sit above
      # the end of the scan: the first gap is the one that explains the scan.
      missing = MapSet.new(["c-99", "c-7", "c-42"])

      assert {before, following} = ChaosChecker.skipped_at(skipping_pages(), missing)
      assert {before.last, following.first} == {"c-5", "c-10"}
    end

    test "two cursors sharing a long prefix get different labels" do
      # The bug this pins: cursors are base64 Erlang terms, so they all open with the same header.
      # Labelling them by their prefix rendered every page identical in the one trace whose whole job
      # is to show when the cursor moved.
      prefix = "g3QAAAABbQAAAA"
      pages = [%{from: prefix <> "aaa", to: prefix <> "zzz", count: 1, first: "c-1", last: "c-1"}]

      [line] = ChaosChecker.page_lines(pages)
      [_, from, to] = Regex.run(~r/from=(\S+) to=(\S+)/, line)

      refute from == to, "a moved cursor must not render as the same label"
    end

    test "a cursor that did not move renders as the same label" do
      same = "g3QAAAABbQAAAAsame"
      pages = [%{from: same, to: same, count: 1, first: "c-1", last: "c-1"}]

      [line] = ChaosChecker.page_lines(pages)
      [_, from, to] = Regex.run(~r/from=(\S+) to=(\S+)/, line)

      assert from == to
    end

    test "renders one line per page, collapsing runs of empty polls" do
      lines = ChaosChecker.page_lines(skipping_pages())

      assert length(lines) == 3
      assert Enum.at(lines, 0) =~ "from=start"
      assert Enum.at(lines, 0) =~ "count=2 first=c-1 last=c-5"
      assert Enum.at(lines, 2) == "PAGE (1 empty, cursor held)"
    end

    test "a stretch of empty polls collapses to a single line carrying its length" do
      empties = for _ <- 1..12, do: %{from: "a", to: "a", count: 0, first: nil, last: nil}

      assert ChaosChecker.page_lines(empties) == ["PAGE (12 empty, cursor held)"]
    end
  end

  describe "skip_confirmed?/3 (a skip is a verdict only once two full reads agree on it)" do
    # The bug this pins: with two segments owning the same offsets, every re-read of the topic returns
    # the same pages and skips the same block, so the revisit could never find the values and ended in
    # INCONCLUSIVE, wording that says the opposite of what happened. A boundary that is there again on
    # a fresh read is structural, and the flow must call it so before spending the revisit budget.
    defp missing_block, do: MapSet.new(["c-6", "c-7", "c-8", "c-9"])

    # The same walk on a fresh read: the cursors are different (a fresh read hands out its own), the
    # sequence numbers at the boundary are not.
    defp same_skip_fresh_cursors do
      [
        %{from: nil, to: "x", count: 2, first: "c-1", last: "c-5"},
        %{from: "x", to: "y", count: 2, first: "c-10", last: "c-12"},
        %{from: "y", to: "y", count: 0, first: nil, last: nil}
      ]
    end

    # A scan that stopped before the missing block: nothing brackets it, which is what a slow read
    # looks like and must never be confirmed as a skip.
    defp stopped_early_pages, do: [%{from: nil, to: "a", count: 2, first: "c-1", last: "c-5"}]

    test "the same boundary in both scans confirms the skip, cursors notwithstanding" do
      # Compared by sequence number, not by cursor: the two scans above share no cursor at all, and a
      # comparison on cursors would never confirm anything.
      assert ChaosChecker.skip_confirmed?(skipping_pages(), same_skip_fresh_cursors(), missing_block()) ==
               true
    end

    test "a boundary that is gone from the second scan is not confirmed" do
      # The second read walked the block contiguously (c-1..c-12 in two pages with no gap), which is
      # what a transient short page looks like on the next try. That is the revisit's territory.
      contiguous = [
        %{from: nil, to: "x", count: 5, first: "c-1", last: "c-5"},
        %{from: "x", to: "y", count: 7, first: "c-6", last: "c-12"},
        %{from: "y", to: "y", count: 0, first: nil, last: nil}
      ]

      assert ChaosChecker.skip_confirmed?(skipping_pages(), contiguous, missing_block()) == false
    end

    test "no boundary in either scan is not confirmed" do
      # Both scans simply stopped early: nothing brackets the missing values, so there is no skip to
      # confirm, and inventing one would be the wrong verdict for a slow scan.
      assert ChaosChecker.skip_confirmed?(stopped_early_pages(), stopped_early_pages(), missing_block()) == false
      assert ChaosChecker.skip_confirmed?([], [], missing_block()) == false
    end

    test "a boundary in the first scan alone is not confirmed" do
      assert ChaosChecker.skip_confirmed?(skipping_pages(), stopped_early_pages(), missing_block()) == false
    end

    test "a different boundary in the second scan is not confirmed" do
      # Both scans skip the block, but not at the same place: the second one ends its first page at
      # c-4 and resumes at c-11. Two different skips are two observations of something moving, not
      # one persistent structure, so the measurement is not repeated and the verdict is not taken.
      other_boundary = [
        %{from: nil, to: "x", count: 4, first: "c-1", last: "c-4"},
        %{from: "x", to: "y", count: 2, first: "c-11", last: "c-12"},
        %{from: "y", to: "y", count: 0, first: nil, last: nil}
      ]

      assert ChaosChecker.skip_confirmed?(skipping_pages(), other_boundary, missing_block()) == false
    end

    test "a boundary whose only difference is one side is still a different boundary" do
      # Same lower edge (c-5), different upper edge (c-11): half a match is no match, since the
      # cursor landed somewhere else the second time.
      upper_moved = [
        %{from: nil, to: "x", count: 2, first: "c-1", last: "c-5"},
        %{from: "x", to: "y", count: 2, first: "c-11", last: "c-12"},
        %{from: "y", to: "y", count: 0, first: nil, last: nil}
      ]

      assert ChaosChecker.skip_confirmed?(skipping_pages(), upper_moved, missing_block()) == false
    end

    test "the mirror of that: the lower edge alone moving is a different boundary too" do
      # Same upper edge (c-10), different lower edge (c-4). The comparison is on the PAIR, so neither
      # side may be the one that carries it: a check that only watched where the cursor resumed would
      # confirm this and call a moving boundary structural.
      lower_moved = [
        %{from: nil, to: "x", count: 4, first: "c-1", last: "c-4"},
        %{from: "x", to: "y", count: 2, first: "c-10", last: "c-12"},
        %{from: "y", to: "y", count: 0, first: nil, last: nil}
      ]

      assert ChaosChecker.skip_confirmed?(skipping_pages(), lower_moved, missing_block()) == false
    end

    test "a missing set mixing sequence numbers with anything else confirms on the lowest NUMBER" do
      # The set is a difference of two files read from disk, not a range the checker built, so a value
      # that does not parse as c-N can be in it. It is dropped when picking the lowest rather than
      # dropping the set that carries it, which would turn a real skip into no boundary at all.
      mixed = MapSet.new(["surprise", "c-6", "c-7", "c-8", "c-9"])

      assert ChaosChecker.skip_confirmed?(skipping_pages(), same_skip_fresh_cursors(), mixed) == true
    end

    test "a second read that found the values leaves nothing to confirm" do
      # The flow recomputes the missing set from both reads before asking. Once it is empty there is
      # no lowest missing value, so no boundary can bracket it in either scan.
      assert ChaosChecker.skip_confirmed?(skipping_pages(), skipping_pages(), MapSet.new()) == false
    end
  end

  describe "confirmed_boundary/3 (only a read that reached the end can confirm anything)" do
    test "a settled second read showing the same boundary confirms it, and hands back the bracket" do
      # Returned rather than recomputed where it is printed: the verdict and the evidence under it
      # come from one decision, so no later change can leave them describing different boundaries.
      assert {:ok, {before, following}} =
               ChaosChecker.confirmed_boundary(
                 skipping_pages(),
                 %{status: :settled, pages: same_skip_fresh_cursors()},
                 missing_block()
               )

      assert {before.last, following.first} == {"c-5", "c-10"}
    end

    test "the very same pages, from a read cut short by its budget, confirm nothing" do
      # The bug this pins: the verdict claims the values are "still missing after two full reads", and
      # a read stopped by its ceiling is not a full read. It saw a prefix of the topic, so it neither
      # agreed nor disagreed, and `verdict/2` refuses to read a truncated scan as loss for that exact
      # reason. `:no` sends the set to the revisit, which ends INCONCLUSIVE: what a read that
      # established nothing is worth.
      assert ChaosChecker.confirmed_boundary(
               skipping_pages(),
               %{status: :timeout, pages: same_skip_fresh_cursors()},
               missing_block()
             ) == :no
    end

    test "a settled second read with no boundary at all is not a confirmation" do
      assert ChaosChecker.confirmed_boundary(
               skipping_pages(),
               %{status: :settled, pages: stopped_early_pages()},
               missing_block()
             ) == :no
    end
  end

  describe "unreachable_line/2 (a verdict that claims only what two reads measured)" do
    test "names the confirmed bracket rather than calling the whole missing set unreachable" do
      # The overstatement this pins: the count is every value still absent after two reads, which can
      # include a tail that merely arrived above the last page. Only the block between the two
      # bracketing pages was measured as unreachable, and the line has to say which is which.
      [before, following, _empty] = skipping_pages()

      assert ChaosChecker.unreachable_line(6, {before, following}) ==
               "VERIFY FAILED: 6 acknowledged values are still missing after two full reads, " <>
                 "of which at least the block between c-5 and c-10 is unreachable " <>
                 "(both reads skipped it at the same page boundary)"
    end

    test "a single missing value keeps the plural, so the harness has one shape to match" do
      # A block of exactly one record behind the boundary is as legitimate an outcome as five hundred,
      # and the count is the size of a set rather than a sentence being conjugated. Pinned because a
      # helpful "1 acknowledged value is" would be a second wording for `chaos_lib.sh` to grep for.
      [before, following, _empty] = skipping_pages()

      assert ChaosChecker.unreachable_line(1, {before, following}) ==
               "VERIFY FAILED: 1 acknowledged values are still missing after two full reads, " <>
                 "of which at least the block between c-5 and c-10 is unreachable " <>
                 "(both reads skipped it at the same page boundary)"
    end

    test "keeps the shape the shell harness matches on" do
      # chaos_lib.sh routes this verdict away from the generic loss branch by grepping
      # `VERIFY FAILED: .* unreachable`, so a rewording that drops either end silently turns an
      # unreachable block back into a reported lost write.
      [before, following, _empty] = skipping_pages()
      line = ChaosChecker.unreachable_line(519, {before, following})

      assert String.starts_with?(line, "VERIFY FAILED: ")
      assert line =~ ~r/^VERIFY FAILED: .* unreachable/
      assert line =~ "519 acknowledged values"
    end
  end

  describe "drain/4 records the pages it walked" do
    test "one entry per fetch, in order, carrying the values it returned" do
      {fetch, _agent} = scripted([["c-1", "c-2"], ["c-3"], []])
      {now, sleep} = fake_clock()

      {_values, :conn, scan} =
        ChaosChecker.drain(fetch, :conn, "topic", now: now, sleep: sleep, settle_ms: 1_000, poll_ms: 250)

      carrying = Enum.filter(scan.pages, &(&1.count > 0))
      assert Enum.map(carrying, & &1.count) == [2, 1]
      assert Enum.map(carrying, & &1.first) == ["c-1", "c-3"]
      assert Enum.map(carrying, & &1.last) == ["c-2", "c-3"]
      assert scan.status == :settled
    end

    test "an empty poll is recorded too, holding the cursor it was asked from" do
      # The trace has to show a poll that found nothing at a position, because a scan that sat at one
      # cursor and a scan that walked past it look identical in the values alone.
      {fetch, _agent} = scripted([[], ["c-1"], []])
      {now, sleep} = fake_clock()

      {_values, :conn, scan} =
        ChaosChecker.drain(fetch, :conn, "topic", now: now, sleep: sleep, settle_ms: 1_000, poll_ms: 250)

      assert [%{count: 0, from: nil, to: nil} | _] = scan.pages
      assert Enum.any?(scan.pages, &(&1.count == 1))
    end
  end

  describe "revisit_budget_ms/1 (how long the deciding scan gets)" do
    test "affords three of the first scan's passes once that scan is slower than the floor" do
      # The case a fixed ceiling gets wrong: on a runner where one pass costs 9s, a 10s budget cannot
      # complete even one, so the verdict is inconclusive no matter what the cluster did.
      assert ChaosChecker.revisit_budget_ms(9_000) == 27_000
    end

    test "a fast first scan still gets the floor, not three times almost nothing" do
      # An empty or barely written topic drains in milliseconds; tripling that would leave no room to
      # observe a late arrival at all, which is the only thing the revisit exists to do.
      assert ChaosChecker.revisit_budget_ms(50) == 10_000
      assert ChaosChecker.revisit_budget_ms(0) == 10_000
    end

    test "the floor and the measurement meet without a step" do
      # At the crossover the two rules agree, so no first-scan duration produces a budget below either.
      assert ChaosChecker.revisit_budget_ms(3_333) == 10_000
      assert ChaosChecker.revisit_budget_ms(3_334) == 10_002
    end
  end

  describe "segment_lines/1 (the map a failing run leaves behind)" do
    test "renders one line per segment, carrying the fields a seal is made of" do
      ranges = [
        %{
          "seq" => 0,
          "segments" => [
            %{
              "seq" => 1,
              "state" => "sealed",
              "start_offset" => 10,
              "length" => 5,
              "byte_size" => 640,
              "primary" => "node2",
              "replica_set" => ["node2", "node1", "node3"]
            }
          ]
        }
      ]

      assert ChaosChecker.segment_lines(ranges) == [
               "SEGMENT range=0 seq=1 state=sealed start=10 length=5 bytes=640 " <>
                 "primary=node2 replicas=node2,node1,node3"
             ]
    end

    test "a topic with no segments renders nothing rather than a placeholder line" do
      assert ChaosChecker.segment_lines([%{"seq" => 0, "segments" => []}]) == []
      assert ChaosChecker.segment_lines([]) == []
    end
  end

  describe "describe/1 (what the missing values look like)" do
    test "an unbroken run reads as one, with its extremes" do
      # The shape that says the scan stopped early rather than that records went missing at random.
      assert ChaosChecker.describe(MapSet.new(["c-8", "c-9", "c-10"])) == "3 values, c-8 to c-10, one unbroken run"
    end

    test "a gap in the middle reads as scattered" do
      assert ChaosChecker.describe(MapSet.new(["c-1", "c-5"])) == "2 values, c-1 to c-5, scattered"
    end

    test "values that do not follow the c-N shape are still reported" do
      assert ChaosChecker.describe(MapSet.new(["surprise"])) =~ "1 values"
    end
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

    {_values, :conn, _scan} =
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
