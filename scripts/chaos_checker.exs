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
  @revisit_ms 10_000
  @revisit_poll_ms 500

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
    {read, conn, status} = drain(&fetch_page/3, conn, topic, deadline: scan_deadline)
    missing = MapSet.difference(acked, read)

    IO.puts("acked=#{MapSet.size(acked)} read=#{MapSet.size(read)} missing=#{MapSet.size(missing)}")

    case verdict(MapSet.size(missing), status) do
      :ok ->
        IO.puts("VERIFY OK: every acknowledged write survived")
        System.halt(0)

      :inconclusive ->
        report_inconclusive(missing, @verify_scan_ms)

      :missing ->
        revisit(conn, topic, acked, read, missing)
    end
  end

  def main(["topology", hosts, topic]) do
    {:ok, _apps} = Application.ensure_all_started(:inets)

    case topology(parse_hosts(hosts), topic) do
      {:ok, ranges} ->
        for range <- ranges, segment <- range["segments"] do
          IO.puts(
            "SEGMENT range=#{range["seq"]} seq=#{segment["seq"]} state=#{segment["state"]} " <>
              "start=#{segment["start_offset"]} primary=#{segment["primary"]} " <>
              "replicas=#{Enum.join(segment["replica_set"], ",")}"
          )
        end

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
  What a scan's outcome means, given how many acknowledged values it could not find and whether it
  finished or hit its ceiling.

  The distinction that matters: a scan cut short by its deadline read only a PREFIX of the topic, so
  values it did not reach are unread, not lost. Calling that a durability failure would be the same
  false alarm this checker was rewritten to stop making, only with a different cause, so it is
  reported as an inconclusive verification instead. A scan that settled did reach the end, so anything
  still absent is worth the alarm.
  """
  @spec verdict(non_neg_integer(), :settled | :timeout) :: :ok | :inconclusive | :missing
  def verdict(0, _status), do: :ok
  def verdict(_missing, :timeout), do: :inconclusive
  def verdict(_missing, :settled), do: :missing

  defp report_inconclusive(missing, budget_ms) do
    IO.puts(
      "VERIFY INCONCLUSIVE: the scan hit its #{budget_ms}ms ceiling with #{MapSet.size(missing)} " <>
        "acknowledged values unread, so it reached only a prefix of the topic. This is not evidence " <>
        "of data loss; re-run with a longer ceiling, or check whether the topic is still being " <>
        "produced to."
    )

    System.halt(1)
  end

  # A missing set is not yet a verdict. Re-read the whole topic until either the missing values turn up
  # (they were late to this node, not lost) or the budget runs out (they are gone).
  defp revisit(conn, topic, acked, read, missing) do
    IO.puts("#{MapSet.size(missing)} values not visible yet; re-reading for up to #{@revisit_ms}ms")
    started = System.monotonic_time(:millisecond)
    {read, elapsed_ms, status} = revisit_loop(conn, topic, acked, read, started + @revisit_ms, started, :settled)
    missing = MapSet.difference(acked, read)

    cond do
      MapSet.size(missing) > 0 and status == :timeout ->
        report_inconclusive(missing, @revisit_ms)

      MapSet.size(missing) > 0 ->
        IO.puts("VERIFY FAILED, first missing: #{missing |> Enum.take(10) |> inspect()}")
        System.halt(1)

      true ->
        # Everything the cluster acknowledged is present, so the durability invariant held. The lag is
        # reported rather than swallowed: it is a real property of reading a write acknowledged through
        # another node, and a growing one would be worth investigating on its own.
        IO.puts(
          "VERIFY OK: every acknowledged write survived " <>
            "(#{MapSet.size(acked)} values, the last of them visible on this node after #{elapsed_ms}ms)"
        )

        System.halt(0)
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
      {fresh, conn, status} = drain(&fetch_page/3, conn, topic, deadline: deadline)
      revisit_loop(conn, topic, acked, MapSet.union(read, fresh), deadline, started, status)
    end
  end

  # --- draining (pure over an injected fetch, so its policy is testable without a cluster) ---

  @doc """
  Reads the topic from the start into a set of values, treating an empty page as "nothing right now"
  rather than "nothing left": the scan only ends once pages keep coming back empty for `:settle_ms`.
  Any page carrying records resets that patience, so a long log still drains in one pass. Returns
  `{values, conn, :settled | :timeout}`: `:timeout` means the absolute `:deadline` cut the scan short,
  so `values` is a PREFIX of the topic and the caller must not read a missing value as a lost one.

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

    drain_loop(fetch, conn, topic, nil, MapSet.new(), now.() + settle_ms, config)
  end

  defp drain_loop(fetch, conn, topic, cursor, acc, settle_deadline, config) do
    if expired?(config, config.deadline) do
      {acc, conn, :timeout}
    else
      drain_page(fetch, conn, topic, cursor, acc, settle_deadline, config)
    end
  end

  defp drain_page(fetch, conn, topic, cursor, acc, settle_deadline, config) do
    case fetch.(conn, topic, cursor) do
      {:ok, [], _next, conn} ->
        if expired?(config, settle_deadline) do
          # Quiet for a whole settle window: the topic is drained, which is a real end of scan.
          {acc, conn, :settled}
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
        drain_loop(fetch, conn, topic, next_cursor, Enum.into(values, acc), config.now.() + config.settle_ms, config)

      other ->
        config.on_error.(other)
    end
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
  defp topology([], _topic), do: {:error, :no_reachable_dashboard}

  defp topology([host | rest], topic) do
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
      _error when rest != [] -> topology(rest, topic)
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
