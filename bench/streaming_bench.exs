# Streaming delivery benchmark: push (1A) vs push+windowing vs pull (1B), sustained over 1M records.
#
# Standalone script (does NOT touch lib/). Models the three delivery mechanisms on top of a single
# serialized GenServer "broker" so the numbers reflect the ARCHITECTURAL difference, not a TCP path:
#
#   push        — broker send/2s every record to every subscriber on produce (1A, no flow control)
#   push+window — broker only sends up to `window` records ahead of each subscriber's acked position;
#                 the subscriber returns credit as it processes (NorthGuard-style sessionized windowing)
#   pull        — subscribers repeatedly ask the broker for the next batch (1B, Kafka-style fetch)
#
# Metrics per run: wall time & throughput, total reductions (CPU), and the PEAK subscriber mailbox
# (message_queue_len) — the memory/backpressure signal that separates windowing from raw push.
#
# Run: mix run bench/streaming_bench.exs

defmodule StreamBench do
  @total 200_000
  @window 1_000

  # ---- broker: holds the "log length" and, for windowed/pull, each subscriber's acked position ----
  defmodule Broker do
    use GenServer
    def start_link, do: GenServer.start_link(__MODULE__, %{len: 0, subs: %{}, acked: %{}})
    def init(s), do: {:ok, s}

    # push: register a plain subscriber pid
    def sub_push(b, pid), do: GenServer.call(b, {:sub_push, pid})
    # windowed: register with a window; broker tracks acked position and sends up to window ahead
    def sub_window(b, pid, w), do: GenServer.call(b, {:sub_window, pid, w})
    def ack(b, pid, n), do: GenServer.cast(b, {:ack, pid, n})
    # pull: fetch the next batch from a position
    def fetch(b, pos, max), do: GenServer.call(b, {:fetch, pos, max})

    def produce_push(b), do: GenServer.call(b, :produce_push, :infinity)
    def produce_window(b), do: GenServer.call(b, :produce_window, :infinity)
    def produce_pull(b), do: GenServer.call(b, :produce_pull)

    def handle_call({:sub_push, pid}, _f, s), do: {:reply, :ok, put_in(s.subs[pid], true)}
    def handle_call({:sub_window, pid, w}, _f, s) do
      {:reply, :ok, %{s | subs: Map.put(s.subs, pid, w), acked: Map.put(s.acked, pid, 0)}}
    end

    def handle_call(:produce_push, _f, s) do
      n = s.len + 1
      for {pid, _} <- s.subs, do: send(pid, {:rec, n})
      {:reply, :ok, %{s | len: n}}
    end

    # windowed produce: advance log, then push to each sub only what fits in its window
    def handle_call(:produce_window, _f, s), do: {:reply, :ok, push_windowed(%{s | len: s.len + 1})}
    def handle_call(:produce_pull, _f, s), do: {:reply, :ok, %{s | len: s.len + 1}}

    def handle_call({:fetch, pos, max}, _f, s) do
      hi = min(s.len, pos + max)
      {:reply, {pos, hi}, s}
    end

    def handle_cast({:ack, pid, n}, s) do
      s = update_in(s.acked[pid], &(&1 + n))
      {:noreply, push_windowed(s)}
    end

    # for each windowed sub, send records in (sent_hi, acked+window] — but we track only acked and a
    # per-sub "sent" high-water in the value tuple {window, sent}
    defp push_windowed(s) do
      subs =
        Map.new(s.subs, fn
          {pid, w} when is_integer(w) -> {pid, {w, 0}}
          {pid, {w, sent}} -> {pid, {w, sent}}
        end)

      {subs, _} =
        Enum.map_reduce(subs, s, fn {pid, {w, sent}}, acc ->
          allowed = min(acc.len, Map.get(acc.acked, pid, 0) + w)
          for n <- (sent + 1)..allowed//1, do: send(pid, {:rec, n})
          {{pid, {w, max(sent, allowed)}}, acc}
        end)

      %{s | subs: Map.new(subs)}
    end
  end

  # ---- consumers ----

  # push/window consumer: receive {:rec, _}; on window mode, ack credit back every `ack_every`
  defp run_push_consumer(parent, broker, total, slow?, ack_every) do
    spawn(fn ->
      loop_push(broker, 0, total, slow?, ack_every, 0)
      send(parent, :done)
    end)
  end

  defp loop_push(_b, got, total, _slow?, _ae, _pending) when got >= total, do: :ok
  defp loop_push(b, got, total, slow?, ack_every, pending) do
    receive do
      {:rec, _n} ->
        if slow?, do: busy(5000)
        pending = pending + 1

        pending =
          if ack_every > 0 and pending >= ack_every do
            Broker.ack(b, self(), pending)
            0
          else
            pending
          end

        loop_push(b, got + 1, total, slow?, ack_every, pending)
    end
  end

  # pull consumer: fetch next batch in a loop until caught up to total
  defp run_pull_consumer(parent, broker, total, slow?, batch) do
    spawn(fn ->
      loop_pull(broker, 0, total, slow?, batch)
      send(parent, :done)
    end)
  end

  defp loop_pull(_b, pos, total, _slow?, _batch) when pos >= total, do: :ok
  defp loop_pull(b, pos, total, slow?, batch) do
    {lo, hi} = Broker.fetch(b, pos, batch)

    if hi > lo do
      if slow?, do: busy(5000)
      loop_pull(b, hi, total, slow?, batch)
    else
      # caught up; tiny yield so we do not hot-spin the GenServer
      Process.sleep(0)
      loop_pull(b, pos, total, slow?, batch)
    end
  end

  defp busy(iters), do: Enum.reduce(1..iters, 0, fn i, a -> a + rem(i * 2_654_435_761, 97) end)

  defp peak_mailbox(pids) do
    pids |> Enum.map(fn p -> if Process.alive?(p), do: Process.info(p, :message_queue_len) |> elem(1), else: 0 end) |> Enum.max(fn -> 0 end)
  end

  # ---- runners: return {wall_ms, throughput_recs_s, reductions, peak_mailbox} ----

  defp measure(fun) do
    {r0, _} = :erlang.statistics(:reductions)
    t0 = System.monotonic_time(:microsecond)
    peak = fun.()
    wall = System.monotonic_time(:microsecond) - t0
    {r1, _} = :erlang.statistics(:reductions)
    {Float.round(wall / 1000, 1), round(@total / (wall / 1_000_000)), r1 - r0, peak}
  end

  def run_push(n, slow?) do
    {:ok, b} = Broker.start_link()
    parent = self()
    measure(fn ->
      pids = for _ <- 1..n, do: run_push_consumer(parent, b, @total, slow?, 0)
      for pid <- pids, do: Broker.sub_push(b, pid)
      spawn(fn -> for _ <- 1..@total, do: Broker.produce_push(b) end)
      peak = sample_peak(pids, parent, n)
      GenServer.stop(b)
      peak
    end)
  end

  def run_window(n, slow?) do
    {:ok, b} = Broker.start_link()
    parent = self()
    measure(fn ->
      pids = for _ <- 1..n do
        pid = run_push_consumer(parent, b, @total, slow?, div(@window, 4))
        pid
      end
      for pid <- pids, do: Broker.sub_window(b, pid, @window)
      spawn(fn -> for _ <- 1..@total, do: Broker.produce_window(b) end)
      peak = sample_peak(pids, parent, n)
      GenServer.stop(b)
      peak
    end)
  end

  def run_pull(n, slow?) do
    {:ok, b} = Broker.start_link()
    parent = self()
    measure(fn ->
      pids = for _ <- 1..n, do: run_pull_consumer(parent, b, @total, slow?, @window)
      spawn(fn -> for _ <- 1..@total, do: Broker.produce_pull(b) end)
      peak = sample_peak(pids, parent, n)
      GenServer.stop(b)
      peak
    end)
  end

  # sample peak mailbox while waiting for all n consumers to finish
  defp sample_peak(pids, parent, n), do: sample_peak(pids, parent, n, 0)
  defp sample_peak(pids, parent, remaining, peak) do
    receive do
      :done -> if remaining <= 1, do: peak, else: sample_peak(pids, parent, remaining - 1, peak)
    after
      5 -> sample_peak(pids, parent, remaining, max(peak, peak_mailbox(pids)))
    end
  end

  def run do
    IO.puts("\nStreaming delivery: #{@total} records, window=#{@window}. wall(ms) | throughput(rec/s) | reductions | peak mailbox\n")

    for {label, n, slow?} <- [{"1 fast consumer", 1, false}, {"10 fast consumers", 10, false}, {"1 SLOW consumer (50-iter/rec)", 1, true}] do
      IO.puts("== #{label} ==")
      {pw, tw, rw, mw} = run_push(n, slow?)
      {pwin, twin, rwin, mwin} = run_window(n, slow?)
      {pp, tp, rp, mp} = run_pull(n, slow?)
      IO.puts("  push         wall=#{pw}  thr=#{tw}  red=#{rw}  peak_mbox=#{mw}")
      IO.puts("  push+window  wall=#{pwin}  thr=#{twin}  red=#{rwin}  peak_mbox=#{mwin}")
      IO.puts("  pull         wall=#{pp}  thr=#{tp}  red=#{rp}  peak_mbox=#{mp}")
      IO.puts("")
    end
  end
end

StreamBench.run()
