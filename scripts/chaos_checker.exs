# Jepsen-style acked-durability checker for the chaos certification harness.
#
# produce mode: connects to the cluster (multi-host round-robin), creates the topic, and produces
# sequential values c-1, c-2, ... at a steady rate for the whole window, RETRYING through errors and
# dropped connections (chaos is happening on purpose). A value is appended to the acked file ONLY
# when its produce was confirmed, so the file is the exact set of writes the cluster acknowledged.
#
# verify mode: fetches the whole topic and asserts every acked value is present. The invariant under
# test: an acknowledged write survives any single-node kill, partition, or stall (rf=3 quorum
# durability). Exit 0 on success, 1 with a summary of missing values otherwise.
#
# Draining is deliberately patient, and separates two things a naive scan conflates:
#
#   * an acknowledged write that is GONE (the durability failure this drill exists to catch), and
#   * an acknowledged write not YET VISIBLE on the node being asked.
#
# The second is not a bug. A frontend's read horizon advances for the produces it handles and
# otherwise from a periodic refresh of the vnodes (a second by default), so a write acknowledged
# through one node is legitimately invisible on another for a moment, and the checker produces
# round-robin across hosts while verifying against a single one. This scan used to stop at the FIRST
# empty page of a non-blocking fetch, so one poll landing inside that window declared every unread
# value lost: `acked=2034 read=2033 missing=1` on a healthy cluster (issue #75). Now an empty page
# only ends the scan after it keeps coming back empty for `@drain_settle_ms`, and a missing set is
# re-read for `@revisit_ms` before any verdict, with the outcome naming which of the two it was.
#
# There is a third thing, and it is neither of those: an acknowledged write that EXISTS on disk but
# cannot be reached through the API. Two segments owning the same offsets (a sealed segment whose log
# kept growing under a stale primary, and the next segment starting where the seal said the first one
# ended) make a scan deliver one owner's records for those offsets and hand out a cursor past the
# other's. Re-reading never helps: every pass returns the same pages, so the revisit would spend its
# whole budget and report inconclusive, wording that says the opposite of what happened. That case
# has a signature a lag cannot produce, a contiguous block of missing values strictly between two
# consecutive delivered pages of a scan that reached the end, and it is measured twice, by two reads
# that both reached the end, before it is called: see `confirmed_boundary/3`.
#
# So verify ends in one of these, and the line it prints names which:
#
#   VERIFY OK            every acknowledged value was read (possibly after waiting out a lag).
#   VERIFY INCONCLUSIVE  the scan was cut short by its ceiling, so unread values are unread, not lost.
#   VERIFY FAILED ... unreachable
#                        a settled scan skipped a block at a page boundary and a second read that
#                        also reached the end skipped the same one: the values are behind the API,
#                        not behind a clock.
#   VERIFY FAILED, missing ...
#                        a settled re-read within the budget still could not find them, and no page
#                        boundary explains it: the values are gone.
#
# topology mode: queries the dashboard's /topic drill-down and prints one SEGMENT line per segment
# with its range/seq (which name the on-disk directory), state, primary and replica set. The
# storage-chaos harness uses it to pick a FOLLOWER copy to damage: primary damage is seal-on-failure
# territory (a separate roadmap item), while follower copies must self-repair.
#
# Usage (run inside the cluster network or anywhere that reaches the hosts):
#   mix run --no-start scripts/chaos_checker.exs produce  host1,host2,host3 topic duration_s acked_file
#   mix run --no-start scripts/chaos_checker.exs verify   host1,host2,host3 topic acked_file
#   mix run --no-start scripts/chaos_checker.exs topology host1,host2,host3 topic

defmodule ChaosChecker do
  alias Malachi.Loadtest.Conn
  alias Malachi.Log.Record
  alias Malachi.Wire

  @rate_sleep_ms 20
  @retry_sleep_ms 250

  # How long an empty page has to keep repeating before the topic counts as drained, and how often to
  # ask. Comfortably over the frontend's default one-second metadata refresh, so a scan that arrives
  # inside the visibility window waits it out instead of mistaking it for the end of the log.
  @drain_settle_ms 5_000
  @drain_poll_ms 250

  # After a missing set is computed, how long to keep re-reading before calling it a durability
  # failure. A value that shows up here was never lost, only late to this node, which is a different
  # finding and is reported as one.
  #
  # A floor, not the budget: see `revisit_budget_ms/1` for why the real one is measured.
  @revisit_ms 10_000
  @revisit_poll_ms 500

  # How many complete re-reads of the topic the revisit must be able to afford. One is not enough: the
  # scan that decides the verdict has to both finish AND have waited, so the budget has to cover a pass
  # that reaches the end plus a couple that watch for late arrivals.
  @revisit_scan_passes 3

  # A ceiling on the whole verification scan. The settle budget alone cannot bound it: every page that
  # carries records resets that budget, so a topic still being written to would keep the scan open for
  # as long as it kept producing. Generous next to the drill's few thousand records read 500 at a time.
  @verify_scan_ms 60_000

  def main(["produce", hosts, topic, duration_s, acked_file]) do
    hosts = parse_hosts(hosts)
    deadline = System.monotonic_time(:millisecond) + String.to_integer(duration_s) * 1000
    {:ok, out} = File.open(acked_file, [:write])

    conn = connect_retry(hosts, 0, deadline)
    ensure_topic(conn, topic)
    produce_loop(conn, hosts, topic, out, deadline, 1, 2, 0)
  end

  def main(["verify", hosts, topic, acked_file]) do
    hosts = parse_hosts(hosts)
    acked = acked_file |> File.read!() |> String.split("\n", trim: true) |> MapSet.new()

    conn = connect_retry(hosts, 0, System.monotonic_time(:millisecond) + 30_000)
    scan_deadline = System.monotonic_time(:millisecond) + @verify_scan_ms
    started = System.monotonic_time(:millisecond)
    {read, conn, scan} = drain(&fetch_page/3, conn, topic, deadline: scan_deadline)
    scan_ms = System.monotonic_time(:millisecond) - started
    missing = MapSet.difference(acked, read)

    IO.puts("acked=#{MapSet.size(acked)} read=#{MapSet.size(read)} missing=#{MapSet.size(missing)}")

    case verdict(MapSet.size(missing), scan.status) do
      :ok ->
        IO.puts("VERIFY OK: every acknowledged write survived")
        System.halt(0)

      :inconclusive ->
        report_evidence(scan, missing, hosts, topic)
        report_inconclusive(missing, @verify_scan_ms)

      :missing ->
        # The FIRST scan's trace, not a later one: it is the pass where a skip first happened, and the
        # revisit re-reads from the start, so its own pages describe a topic that has since moved on.
        case skipped_at(scan.pages, missing) do
          nil -> revisit(conn, hosts, topic, acked, read, missing, revisit_budget_ms(scan_ms), scan)
          _boundary -> confirm_skip(conn, hosts, topic, acked, read, scan, scan_ms)
        end
    end
  end

  def main(["topology", hosts, topic]) do
    case topology(parse_hosts(hosts), topic) do
      {:ok, ranges} ->
        Enum.each(segment_lines(ranges), &IO.puts/1)

      {:error, reason} ->
        IO.puts("topology failed: #{inspect(reason)}")
        System.halt(1)
    end
  end

  def main(_argv) do
    IO.puts("usage: chaos_checker.exs produce  <hosts> <topic> <duration_s> <acked_file>")
    IO.puts("       chaos_checker.exs verify   <hosts> <topic> <acked_file>")
    IO.puts("       chaos_checker.exs topology <hosts> <topic>")
    System.halt(2)
  end

  @doc """
  How long the revisit gets, given how long the first full scan took (`scan_ms`).

  Measured rather than fixed, because the revisit re-reads the WHOLE topic on every iteration and all
  of those iterations share this one budget. A flat ceiling therefore encodes a guess about how fast
  the machine is: on a slow runner a single pass already overruns it, so the revisit can never reach
  the end, every run with any visibility lag reports as inconclusive, and the certification gate stops
  meaning anything. The first scan is a direct measurement of what one pass costs on THIS machine, so
  the budget is stated as what it actually needs to be: room for `#{@revisit_scan_passes}` of them.
  The floor keeps a suspiciously fast first scan (an empty or barely-written topic) from producing a
  budget too small to observe anything.
  """
  @spec revisit_budget_ms(non_neg_integer()) :: pos_integer()
  def revisit_budget_ms(scan_ms), do: max(@revisit_ms, @revisit_scan_passes * scan_ms)

  @doc """
  One `SEGMENT ...` line per segment, in the order the topology reports them.

  Shared by the `topology` command and by the failure paths of `verify`, which is the point: a run
  that ends in an unread or missing block is exactly the run whose segment map is worth having, and
  re-reading it from a rerun is not the same evidence, because the cluster has moved on by then.
  """
  @spec segment_lines([map()]) :: [String.t()]
  def segment_lines(ranges) do
    for range <- ranges, segment <- range["segments"] do
      "SEGMENT range=#{range["seq"]} seq=#{segment["seq"]} state=#{segment["state"]} " <>
        "start=#{segment["start_offset"]} length=#{segment["length"]} bytes=#{segment["byte_size"]} " <>
        "primary=#{segment["primary"]} replicas=#{Enum.join(segment["replica_set"], ",")}"
    end
  end

  # The segment map at the moment a verification failed, so the next look at it starts from evidence
  # rather than from theory. Best effort by design: a topology call that fails must not replace the
  # verdict the caller is about to report with an error about the diagnostics.
  defp report_topology(hosts, topic) do
    case topology(hosts, topic) do
      {:ok, ranges} ->
        IO.puts("segment map at the time of the failure:")
        Enum.each(segment_lines(ranges), &IO.puts/1)

      {:error, reason} ->
        IO.puts("segment map unavailable: #{inspect(reason)}")
    end
  end

  @doc """
  What a scan's outcome means, given how many acknowledged values it could not find and whether it
  finished or hit its ceiling.

  The distinction that matters: a scan cut short by its deadline read only a PREFIX of the topic, so
  values it did not reach are unread, not lost. Calling that a durability failure would be the same
  false alarm this checker was rewritten to stop making, only with a different cause, so it is
  reported as an inconclusive verification instead. A scan that settled did reach the end, so anything
  still absent is worth the alarm.

  `:missing` is where the alarm starts, not what it says. A settled scan with values absent is
  followed by one of these verdict lines, and what separates them is what the page trace shows:

    * `VERIFY OK`: the values turned up on a re-read, so they were late to this node, not lost.
    * `VERIFY FAILED ... unreachable`: the first scan skipped a block at a page boundary and a second
      read that also reached the end skipped the same one (`confirmed_boundary/3`). The values are
      acknowledged and cannot be read through the API, and no amount of waiting changes that.
    * `VERIFY INCONCLUSIVE`: only ever a scan that was cut short, either this one or the revisit's
      last pass. Nothing was established either way.
    * `VERIFY FAILED, missing ...`: a settled re-read within the budget still could not find them and
      no page boundary explains it, so the values are gone.
  """
  @spec verdict(non_neg_integer(), :settled | :timeout) :: :ok | :inconclusive | :missing
  def verdict(0, _status), do: :ok
  def verdict(_missing, :timeout), do: :inconclusive
  def verdict(_missing, :settled), do: :missing

  # Only a scan that hit its ceiling ends here, never one that reached the end: a truncated read
  # leaves a suffix unread and proves nothing about it, which is why the wording refuses to call it
  # loss. A settled scan whose values are still absent is reported by `report_unreachable/2` (the
  # trace shows a skip that persisted) or as lost (it does not), never as inconclusive.
  defp report_inconclusive(missing, budget_ms) do
    IO.puts(
      "VERIFY INCONCLUSIVE: the scan hit its #{budget_ms}ms ceiling with #{MapSet.size(missing)} " <>
        "acknowledged values unread, so it reached only a prefix of the topic. This is not evidence " <>
        "of data loss; re-run with a longer ceiling, or check whether the topic is still being " <>
        "produced to."
    )

    IO.puts("missing #{describe(missing)}")
    System.halt(1)
  end

  # The verdict for a skip that survived a second full read. A failure, not an inconclusive: the
  # values were acknowledged, both scans reached the end, and both handed out a cursor past them, so
  # the API cannot return them. Wording chosen so the shell harness can tell it from the lost case,
  # which is a different bug with a different owner.
  defp report_unreachable(missing, boundary) do
    IO.puts(unreachable_line(MapSet.size(missing), boundary))
    IO.puts("missing #{describe(missing)}")
    report_missing_sample(missing)
    System.halt(1)
  end

  @doc """
  The `VERIFY FAILED ... unreachable` line for a `missing` set whose skip was confirmed, given the
  `boundary` (the two bracketing pages `skipped_at/2` returned) the two reads agreed on.

  Built around the bracket rather than around the count, because the two are not the same claim. What
  the second read established is that the block between those pages cannot be reached; a value missing
  ABOVE the last page is in the same set for a duller reason (it landed after the scan walked past
  that position), and putting the whole count behind the word "unreachable" would state more than was
  measured, in the one verdict that has to be trusted on its evidence.
  """
  @spec unreachable_line(non_neg_integer(), {map(), map()}) :: String.t()
  def unreachable_line(missing_count, {before, following}) do
    "VERIFY FAILED: #{missing_count} acknowledged values are still missing after two full reads, " <>
      "of which at least the block between #{before.last} and #{following.first} is unreachable " <>
      "(both reads skipped it at the same page boundary)"
  end

  # The pass that makes the verdict. Re-reads the topic with a deadline of its own, so the revisit
  # that may follow still gets its full budget, and asks one question: is the same boundary there
  # again? Yes means the block is unreachable and re-reading further would only delay saying so. No
  # means the first scan's boundary was a moment (or the second read found the values, or this read
  # never reached the end), and the revisit takes over exactly as if the boundary had never been
  # seen. `read` carries both passes forward either way, since a value delivered once is not missing.
  #
  # A budget of its own is what makes the worst case here the first scan plus TWICE
  # `revisit_budget_ms/1`: one full budget confirming, and, when the boundary turns out to have been
  # transient, another one revisiting. Paid rather than shared, because halving the two would leave
  # each too short to reach the end of the topic, which is the one thing either pass must do to
  # conclude anything, and the transient case is rare by construction: a boundary that gets this far
  # has already been observed once.
  defp confirm_skip(conn, hosts, topic, acked, read, first_scan, scan_ms) do
    IO.puts("scan trace shows a page boundary over the missing values; re-reading once to confirm it")
    budget_ms = revisit_budget_ms(scan_ms)
    started = System.monotonic_time(:millisecond)
    {fresh, conn, second} = drain(&fetch_page/3, conn, topic, deadline: started + budget_ms)
    elapsed_ms = System.monotonic_time(:millisecond) - started
    read = MapSet.union(read, fresh)
    missing = MapSet.difference(acked, read)

    if MapSet.size(missing) == 0 do
      report_ok(acked, elapsed_ms)
    else
      # Decided once and carried into the verdict that prints it. Recomputing the bracket where it
      # gets reported worked only because a confirmed skip guarantees the recomputation finds one, a
      # coupling nothing in the code stated, in the one path that has to be reliable.
      case confirmed_boundary(first_scan.pages, second, missing) do
        {:ok, boundary} ->
          report_evidence(first_scan, missing, hosts, topic)
          report_unreachable(missing, boundary)

        :no ->
          revisit(conn, hosts, topic, acked, read, missing, budget_ms, first_scan)
      end
    end
  end

  # Everything the cluster acknowledged is present, so the durability invariant held. The lag is
  # reported rather than swallowed: it is a real property of reading a write acknowledged through
  # another node, and a growing one would be worth investigating on its own.
  defp report_ok(acked, elapsed_ms) do
    IO.puts(
      "VERIFY OK: every acknowledged write survived " <>
        "(#{MapSet.size(acked)} values, the last of them visible on this node after #{elapsed_ms}ms)"
    )

    System.halt(0)
  end

  @doc """
  The scan's pages rendered one per line, with runs of empty pages collapsed.

  Reads as a walk: each line is one fetch, what it was asked from, what it returned, and the span of
  values it carried. A scan that is merely slow shows pages marching upward and then stopping; a scan
  that skipped shows one page ending at a value and the next starting well above it, which is the
  distinction the verdict alone could never make.
  """
  @spec page_lines([map()]) :: [String.t()]
  def page_lines(pages) do
    pages
    |> Enum.chunk_by(&(&1.count == 0))
    |> Enum.flat_map(fn
      [%{count: 0} | _] = empties -> ["PAGE (#{length(empties)} empty, cursor held)"]
      carrying -> Enum.map(carrying, &page_line/1)
    end)
  end

  defp page_line(page) do
    "PAGE from=#{cursor_label(page.from)} to=#{cursor_label(page.to)} " <>
      "count=#{page.count} first=#{page.first} last=#{page.last}"
  end

  # Cursors are opaque on the wire, so what matters here is telling one from another and seeing when
  # one repeats, not decoding it. Hashed rather than truncated: the cursor is a base64 Erlang term, so
  # every one of them opens with the same header, and showing a prefix rendered every page identical
  # (g3QAAAAB...) precisely in the trace that exists to compare them. A hash of the WHOLE cursor is
  # short, stable, and different exactly when the cursor is.
  defp cursor_label(nil), do: "start"

  defp cursor_label(cursor) do
    cursor |> :erlang.phash2() |> Integer.to_string(16) |> String.pad_leading(8, "0")
  end

  @doc """
  Where a scan walked past values it never returned, given its `pages` and the `missing` set: the two
  consecutive pages that bracket the lowest missing value, or `nil` when no page boundary does.

  This is the finding the page trace exists to produce. A cursor that advances over records makes the
  values above the gap readable while the ones inside it never come back, which looks exactly like a
  slow scan from the outside and is nothing like it: one is a budget to raise, the other is the server
  handing out a position past data it had not made visible yet. `nil` means the trace does not explain
  the missing set, which is worth knowing too rather than inventing a boundary that fits.
  """
  @spec skipped_at([map()], MapSet.t()) :: {map(), map()} | nil
  def skipped_at(pages, missing) do
    carrying = Enum.filter(pages, &(&1.count > 0))

    with lowest when is_integer(lowest) <- lowest_sequence(missing) do
      carrying
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.find(fn [before, following] ->
        brackets?(sequence_number(before.last), sequence_number(following.first), lowest)
      end)
      |> case do
        [before, following] -> {before, following}
        nil -> nil
      end
    else
      _no_sequence -> nil
    end
  end

  defp brackets?(last, first, lowest) when is_integer(last) and is_integer(first) do
    last < lowest and first > lowest
  end

  defp brackets?(_last, _first, _lowest), do: false

  @doc """
  Whether the skip `skipped_at/2` found in a first scan is still there in a second one: the same page
  boundary, a page ending at the same sequence number followed by one starting at the same sequence
  number, brackets the lowest value of `missing` in both `first_pages` and `second_pages`.

  Measured twice on purpose. One scan's boundary could in principle be a single transient page (a
  fetch that came back short for a reason of the moment, with the cursor placed after it), and a
  verdict as strong as "unreachable" must not rest on one observation. A boundary that comes back
  identical from a fresh read started at the beginning of the topic is not a moment: it is how the
  server answers that position, which is what makes the values behind it unreachable rather than
  late. Compared by sequence number and not by cursor, because a fresh read hands out cursors of its
  own, so the two scans never share one even when they walked the same pages.

  This is the question stated over the two page traces alone, its readable form. The flow itself
  calls `confirmed_boundary/3`, which takes the second SCAN instead: whether that read reached the
  end of the topic is half the evidence, and a page trace cannot carry it.
  """
  @spec skip_confirmed?([map()], [map()], MapSet.t()) :: boolean()
  def skip_confirmed?(first_pages, second_pages, missing) do
    confirmed_boundary(first_pages, %{status: :settled, pages: second_pages}, missing) != :no
  end

  @doc """
  The boundary two full reads agreed on: `{:ok, {before, following}}` when the first scan's
  `first_pages` and the `second` scan (as `drain/4` returned it) both skip the lowest value of
  `missing` at the same pair of sequence numbers, `:no` otherwise.

  The bracket handed back is the FIRST scan's, because that is the pass whose trace the failure
  report prints, and it is returned rather than recomputed at the point of printing so that the
  verdict and the evidence behind it cannot drift apart.

  Takes the whole second scan because a truncated read confirms nothing. It stopped somewhere in the
  middle of the topic, so a boundary it did not reach is not a boundary it disagreed with, and one it
  did reach was never followed to the end: it read a PREFIX, which is exactly what `verdict/2`
  refuses to read as loss. `:no` sends the missing set to the revisit instead, where a read that
  established nothing ends INCONCLUSIVE. The first scan needs no such guard: only a settled one gets
  this far (see `verdict/2`).
  """
  @spec confirmed_boundary([map()], %{status: :settled | :timeout, pages: [map()]}, MapSet.t()) ::
          {:ok, {map(), map()}} | :no
  def confirmed_boundary(first_pages, %{status: :settled, pages: second_pages}, missing) do
    case {skipped_at(first_pages, missing), skipped_at(second_pages, missing)} do
      {{before, following}, {again_before, again_following}} ->
        if boundary(before, following) == boundary(again_before, again_following) do
          {:ok, {before, following}}
        else
          :no
        end

      _absent_in_at_least_one ->
        :no
    end
  end

  def confirmed_boundary(_first_pages, %{status: :timeout}, _missing), do: :no

  defp boundary(before, following) do
    {sequence_number(before.last), sequence_number(following.first)}
  end

  defp lowest_sequence(values) do
    values |> Enum.map(&sequence_number/1) |> Enum.reject(&is_nil/1) |> Enum.min(fn -> nil end)
  end

  # The page trace at the point a scan came up short, plus the boundary that explains it when one does.
  defp report_scan(scan, missing) do
    Enum.each(page_lines(scan.pages), &IO.puts/1)

    case skipped_at(scan.pages, missing) do
      {before, following} ->
        IO.puts(
          "SCAN SKIPPED: a page ending at #{before.last} was followed by one starting at " <>
            "#{following.first}, so the cursor advanced past values the scan never returned. " <>
            "This is not a scan that ran out of time."
        )

      nil ->
        IO.puts("scan trace does not show a page boundary over the missing values")
    end
  end

  # Everything a non-OK verdict leaves behind, in one order: the trace of the scan that came up short,
  # then the segment map as it was at that moment. Emitted from here rather than from each verdict so
  # the two FAILED lines cannot end up describing the same run differently, which is the failure mode
  # of evidence assembled at four call sites.
  defp report_evidence(first_scan, missing, hosts, topic) do
    report_scan(first_scan, missing)
    report_topology(hosts, topic)
  end

  # Names under the count. `describe/1` says how many and how far apart, which is the shape of the
  # finding but nothing anyone can go and grep a log for; the first few names are.
  defp report_missing_sample(missing) do
    IO.puts("first missing: #{missing |> Enum.take(10) |> inspect()}")
  end

  @doc """
  A compact description of a set of `c-N` values: how many, the extremes, and whether they form one
  unbroken run. Which values are absent says more than how many: an unbroken run ending at the last
  value produced is a scan that stopped early, while a scattered set points somewhere else entirely.
  """
  @spec describe(MapSet.t()) :: String.t()
  def describe(values) do
    numbers = values |> Enum.map(&sequence_number/1) |> Enum.reject(&is_nil/1) |> Enum.sort()

    case numbers do
      [] ->
        "#{MapSet.size(values)} values: #{values |> Enum.take(5) |> inspect()}"

      _ ->
        first = List.first(numbers)
        last = List.last(numbers)
        shape = if last - first + 1 == length(numbers), do: "one unbroken run", else: "scattered"

        "#{length(numbers)} values, c-#{first} to c-#{last}, #{shape}"
    end
  end

  defp sequence_number("c-" <> digits) do
    case Integer.parse(digits) do
      {number, ""} -> number
      _ -> nil
    end
  end

  defp sequence_number(_other), do: nil

  # A missing set is not yet a verdict. Re-read the whole topic until either the missing values turn up
  # (they were late to this node, not lost) or the budget runs out (they are gone). Reached only once
  # `confirm_skip/7` has ruled out a persistent page boundary, because re-reading cannot find values
  # the API skips on every pass, and spending the budget on them would end in the wrong verdict.
  defp revisit(conn, hosts, topic, acked, read, missing, budget_ms, first_scan) do
    IO.puts("#{MapSet.size(missing)} values not visible yet; re-reading for up to #{budget_ms}ms")
    started = System.monotonic_time(:millisecond)
    {read, elapsed_ms, status} = revisit_loop(conn, topic, acked, read, started + budget_ms, started, :settled)
    missing = MapSet.difference(acked, read)

    cond do
      MapSet.size(missing) > 0 and status == :timeout ->
        report_evidence(first_scan, missing, hosts, topic)
        report_inconclusive(missing, budget_ms)

      MapSet.size(missing) > 0 ->
        IO.puts("VERIFY FAILED, missing #{describe(missing)}")
        report_missing_sample(missing)
        report_evidence(first_scan, missing, hosts, topic)
        System.halt(1)

      true ->
        report_ok(acked, elapsed_ms)
    end
  end

  # `status` carries whether the last re-read reached the end of the topic or was cut off by the
  # budget, because a cut-off read leaves values unread rather than proving them gone.
  defp revisit_loop(conn, topic, acked, read, deadline, started, status) do
    now = System.monotonic_time(:millisecond)

    if MapSet.subset?(acked, read) or now >= deadline do
      # Everything found means the scan did its job whatever the clock says; only an unfinished search
      # inherits the timeout.
      status = if MapSet.subset?(acked, read), do: :settled, else: status
      {read, now - started, status}
    else
      Process.sleep(min(@revisit_poll_ms, deadline - now))
      # The re-read shares THIS budget rather than taking a fresh settle window of its own: otherwise
      # a retry starting just before the deadline could still run a full scan past it, and a topic
      # that keeps producing would reset that scan's patience forever, leaving the bounded revisit
      # unbounded.
      {fresh, conn, scan} = drain(&fetch_page/3, conn, topic, deadline: deadline)
      revisit_loop(conn, topic, acked, MapSet.union(read, fresh), deadline, started, scan.status)
    end
  end

  # --- draining (pure over an injected fetch, so its policy is testable without a cluster) ---

  @doc """
  Reads the topic from the start into a set of values, treating an empty page as "nothing right now"
  rather than "nothing left": the scan only ends once pages keep coming back empty for `:settle_ms`.
  Any page carrying records resets that patience, so a long log still drains in one pass.

  Returns `{values, conn, scan}`, where `scan` is `%{status: :settled | :timeout, pages: [page]}`.
  `:timeout` means the absolute `:deadline` cut the scan short, so `values` is a PREFIX of the topic
  and the caller must not read a missing value as a lost one. `pages` is one entry per fetch, in
  order, each `%{from: cursor, to: cursor, count: n, first: value, last: value}`: see `page_lines/1`
  and `skipped_at/2` for what it answers, which is whether a scan that came up short ran out of time
  or walked past records. The status travels inside the map rather than beside it so a caller cannot
  take one and forget the other, which is the whole reason they are reported together.

  `fetch` is `(conn, topic, cursor -> {:ok, values, next_cursor, conn} | {:error, reason})`, injected
  so this policy can be exercised without a cluster; `:sleep` and `:now` are injected for the same
  reason. Anything but a clean page is fatal: a failed read is not an empty log, and the caller must
  not turn it into one (the mistake this whole module now guards against).
  """
  def drain(fetch, conn, topic, opts \\ []) do
    settle_ms = Keyword.get(opts, :settle_ms, @drain_settle_ms)
    now = Keyword.get(opts, :now, fn -> System.monotonic_time(:millisecond) end)

    config = %{
      settle_ms: settle_ms,
      poll_ms: Keyword.get(opts, :poll_ms, @drain_poll_ms),
      sleep: Keyword.get(opts, :sleep, &Process.sleep/1),
      now: now,
      on_error: Keyword.get(opts, :on_error, &halt_on_fetch_error/1),
      # An absolute ceiling on the whole scan, separate from the settle budget. The settle deadline is
      # reset by every page that carries records, which is what lets a long log drain in one pass, but
      # it also means a topic that keeps producing could hold the scan open indefinitely. A caller
      # working to its own budget (the revisit loop) passes that budget here so the scan cannot outlive
      # it. `:infinity` for a scan that should run until the log is quiet.
      deadline: Keyword.get(opts, :deadline, :infinity)
    }

    drain_loop(fetch, conn, topic, nil, %{values: MapSet.new(), pages: []}, now.() + settle_ms, config)
  end

  defp drain_loop(fetch, conn, topic, cursor, acc, settle_deadline, config) do
    if expired?(config, config.deadline) do
      finish_scan(acc, conn, :timeout)
    else
      drain_page(fetch, conn, topic, cursor, acc, settle_deadline, config)
    end
  end

  defp drain_page(fetch, conn, topic, cursor, acc, settle_deadline, config) do
    case fetch.(conn, topic, cursor) do
      {:ok, [], _next, conn} ->
        acc = record_page(acc, cursor, cursor, [])

        if expired?(config, settle_deadline) do
          # Quiet for a whole settle window: the topic is drained, which is a real end of scan.
          finish_scan(acc, conn, :settled)
        else
          # Never sleep past the caller's ceiling: a poll that would overshoot it is cut to whatever is
          # left, so the scan returns on time instead of one poll late.
          config.sleep.(capped_sleep(config))
          # The same cursor on purpose: an empty page means this position had nothing to give yet, so
          # the scan resumes from it rather than skipping past values it never read.
          drain_loop(fetch, conn, topic, cursor, acc, settle_deadline, config)
        end

      {:ok, values, next_cursor, conn} ->
        # Progress resets the patience: only an uninterrupted stretch of nothing ends the scan. The
        # absolute deadline above is what keeps that reset from running forever.
        acc = record_page(acc, cursor, next_cursor, values)
        acc = %{acc | values: Enum.into(values, acc.values)}
        drain_loop(fetch, conn, topic, next_cursor, acc, config.now.() + config.settle_ms, config)

      other ->
        config.on_error.(other)
    end
  end

  # One entry per fetch, in the order the scan made them. The whole point is to be able to answer
  # WHERE a scan stopped seeing values it should have seen: a page that ends at one sequence number
  # followed by a page that starts well above it is a cursor that moved past records, which is a
  # different finding from a scan that ran out of time, and the two were indistinguishable from the
  # outside. Kept even on a clean scan, since it costs one small map per fetch and the run that turns
  # out to need it is never the one you decided to instrument.
  defp record_page(acc, from, to, values) do
    page = %{from: from, to: to, count: length(values), first: List.first(values), last: List.last(values)}
    %{acc | pages: [page | acc.pages]}
  end

  defp finish_scan(acc, conn, status) do
    {acc.values, conn, %{status: status, pages: Enum.reverse(acc.pages)}}
  end

  defp expired?(_config, :infinity), do: false
  defp expired?(config, deadline), do: config.now.() >= deadline

  defp capped_sleep(%{deadline: :infinity} = config), do: config.poll_ms
  defp capped_sleep(config), do: max(0, min(config.poll_ms, config.deadline - config.now.()))

  defp halt_on_fetch_error(other) do
    IO.puts("fetch failed: #{inspect(other)}")
    System.halt(1)
  end

  # One page over the wire, as the values it carried plus the cursor to continue from.
  defp fetch_page(conn, topic, cursor) do
    payload = Wire.encode_fetch_req(topic, cursor, nil, nil, 500, 0)

    case Conn.request(conn, Wire.fetch_key(), next_corr(), payload) do
      {:ok, 0, resp, conn} ->
        {records, next_cursor} = Wire.decode_fetch_resp(resp)
        {:ok, Enum.map(records, & &1.value), next_cursor, conn}

      other ->
        other
    end
  end

  # The wire needs a fresh correlation id per request; the scan no longer threads one because it can
  # re-read the same cursor any number of times.
  defp next_corr do
    corr = Process.get(:corr, 2)
    Process.put(:corr, corr + 1)
    corr
  end

  defp parse_hosts(hosts), do: hosts |> String.split(",", trim: true) |> Enum.map(&String.trim/1)

  # --- produce ---

  defp produce_loop(conn, hosts, topic, out, deadline, i, corr, host_idx) do
    if System.monotonic_time(:millisecond) >= deadline do
      IO.puts("produced through #{i - 1} attempts; done")
    else
      value = "c-#{i}"
      record = %Record{value: value, key: "k#{i}", timestamp: 0, headers: []}

      case try_produce(conn, topic, record, corr) do
        {:ok, conn} ->
          # Confirmed by the cluster: this write must now survive anything the chaos does.
          IO.write(out, value <> "\n")
          Process.sleep(@rate_sleep_ms)
          produce_loop(conn, hosts, topic, out, deadline, i + 1, corr + 1, host_idx)

        {:retry, _dead} ->
          # Error or dropped connection mid-chaos: reconnect (next host) and RETRY THE SAME value;
          # it was never acked, so it may legitimately be absent or duplicated, both fine.
          Process.sleep(@retry_sleep_ms)
          conn = connect_retry(hosts, host_idx + 1, deadline)
          produce_loop(conn, hosts, topic, out, deadline, i, corr + 1, host_idx + 1)
      end
    end
  end

  defp try_produce(conn, topic, record, corr) do
    case Conn.request(conn, Wire.produce_key(), corr, Wire.encode_produce_req(topic, [record])) do
      {:ok, 0, _resp, conn} -> {:ok, conn}
      {:ok, _code, _resp, conn} -> {:retry, conn}
      {:error, _reason} -> {:retry, conn}
    end
  rescue
    _any -> {:retry, conn}
  end

  # --- verify ---

  # --- topology (dashboard HTTP) ---

  # Logs into the first reachable node's dashboard and fetches the topic drill-down. Plain :httpc
  # (the dashboard speaks HTTP/1.1 on 4041); the Bearer token comes from POST /login, the same
  # credentials the wire connection uses.
  # Starts :inets here rather than at each call site: the dashboard is now read from the verify
  # failure paths too, and an http client that is only started on one of them is a diagnostic that
  # works everywhere except where it is needed. `ensure_all_started` is idempotent.
  defp topology(hosts, topic) do
    {:ok, _apps} = Application.ensure_all_started(:inets)
    request_topology(hosts, topic)
  end

  defp request_topology([], _topic), do: {:error, :no_reachable_dashboard}

  defp request_topology([host | rest], topic) do
    base = "http://#{host}:4041"
    login_body = Jason.encode!(%{username: "admin", password: "admin123"})
    http_opts = [timeout: 5_000]
    opts = [body_format: :binary]

    with {:ok, {{_http, 200, _msg}, _hdrs, login}} <-
           :httpc.request(:post, {~c"#{base}/login", [], ~c"application/json", login_body}, http_opts, opts),
         {:ok, %{"token" => token}} <- Jason.decode(login),
         auth = [{~c"authorization", ~c"Bearer #{token}"}],
         {:ok, {{_http2, 200, _msg2}, _hdrs2, detail}} <-
           :httpc.request(:get, {~c"#{base}/topic?name=#{topic}", auth}, http_opts, opts),
         {:ok, %{"ranges" => ranges}} <- Jason.decode(detail) do
      {:ok, ranges}
    else
      _error when rest != [] -> request_topology(rest, topic)
      error -> {:error, error}
    end
  end

  # --- connection plumbing ---

  defp connect_retry(hosts, idx, deadline) do
    if System.monotonic_time(:millisecond) >= deadline do
      IO.puts("could not connect before the deadline")
      System.halt(1)
    end

    host = Enum.at(hosts, rem(idx, length(hosts)))

    with {:ok, conn} <- Conn.connect(host: host, port: 4040),
         {:ok, conn} <- Conn.authenticate(conn, user: "admin", pass: "admin123") do
      conn
    else
      _err ->
        Process.sleep(@retry_sleep_ms)
        connect_retry(hosts, idx + 1, deadline)
    end
  rescue
    _any ->
      Process.sleep(@retry_sleep_ms)
      connect_retry(hosts, idx + 1, deadline)
  end

  defp ensure_topic(conn, topic) do
    {:ok, _code, _resp, _conn} = Conn.request(conn, Wire.create_topic_key(), 1, Wire.encode_create_topic_req(topic, 8))
    :ok
  end
end

# Running a mode is what this file is for everywhere except a test, which requires it to exercise
# `drain/4` and would otherwise be halted by `main/1` on the way in. The environment already answers
# the question, so no knob is invented for it: the drill runs the checker through `mix run` in the
# loadtest image (MIX_ENV=dev), and only `mix test` is :test.
unless Mix.env() == :test do
  ChaosChecker.main(System.argv())
end
