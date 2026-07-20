# Long-poll mechanism benchmark: (A) waiters inside the BrokerServer vs (B) Registry pub/sub.
#
# This is a standalone investigation script: it does NOT touch lib/. It models the two mechanisms
# faithfully on top of a single serialized GenServer (the BrokerServer is one process), so the
# numbers reflect the architectural difference, not a full TCP path. The per-wakeup "re-consume"
# work (reading one log page) is the SAME in both, so the signal is the notification mechanism.
#
# Run: mix run bench/long_poll_bench.exs
#
# Metrics, for W ∈ {1, 10, 100, 1000} consumers waiting on one topic:
#   * wakeup_total_us: from the start of a produce until ALL W waiters hold the new data
#                         (fair end-to-end fan-out time: notification + re-consume)
#   * producer_block_us: how long the produce call itself blocks (latency the PRODUCER perceives)

defmodule LongPollBench do
  # Represents reading one log page on wakeup. Identical cost in A and B; small but non-zero so the
  # fan-out work is realistic. Kept pure/CPU so it is deterministic across runs.
  @reconsume_iters 200
  def reconsume do
    Enum.reduce(1..@reconsume_iters, 0, fn i, acc -> acc + rem(i * 2_654_435_761, 97) end)
  end

  # ---- Mechanism A: waiters held inside the GenServer; produce does the fan-out in-process ----
  defmodule ServerA do
    use GenServer

    def start_link, do: GenServer.start_link(__MODULE__, %{offset: 0, waiters: []})
    def init(state), do: {:ok, state}

    def offset(pid), do: GenServer.call(pid, :offset)
    def waiter_count(pid), do: GenServer.call(pid, :waiter_count)
    def wait(pid, pos), do: GenServer.call(pid, {:wait, pos}, :infinity)
    def produce(pid), do: GenServer.call(pid, :produce, :infinity)

    def handle_call(:offset, _from, s), do: {:reply, s.offset, s}
    def handle_call(:waiter_count, _from, s), do: {:reply, length(s.waiters), s}

    def handle_call({:wait, pos}, from, s) do
      if s.offset > pos do
        _ = LongPollBench.reconsume()
        {:reply, {:ok, s.offset}, s}
      else
        {:noreply, %{s | waiters: [{from, pos} | s.waiters]}}
      end
    end

    def handle_call(:produce, _from, s) do
      offset = s.offset + 1
      # Fan-out happens here, on the serialized produce path: re-consume + reply per woken waiter.
      {keep, woken} = Enum.split_with(s.waiters, fn {_from, pos} -> offset <= pos end)

      Enum.each(woken, fn {from, _pos} ->
        _ = LongPollBench.reconsume()
        GenServer.reply(from, {:ok, offset})
      end)

      {:reply, :ok, %{s | offset: offset, waiters: keep}}
    end
  end

  # ---- Mechanism B: Registry pub/sub; produce only dispatches, waiters re-fetch via the GenServer ----
  defmodule ServerB do
    use GenServer

    def start_link, do: GenServer.start_link(__MODULE__, %{offset: 0})
    def init(state), do: {:ok, state}

    def offset(pid), do: GenServer.call(pid, :offset)
    def read(pid, pos), do: GenServer.call(pid, {:read, pos}, :infinity)
    def produce(pid, registry), do: GenServer.call(pid, {:produce, registry}, :infinity)

    def handle_call(:offset, _from, s), do: {:reply, s.offset, s}

    def handle_call({:read, pos}, _from, s) do
      _ = LongPollBench.reconsume()
      reply = if s.offset > pos, do: {:ok, s.offset}, else: :empty
      {:reply, reply, s}
    end

    def handle_call({:produce, registry}, _from, s) do
      offset = s.offset + 1
      # Produce only publishes; the heavy re-fetch happens later, in the consumer processes.
      Registry.dispatch(registry, :topic, fn entries ->
        for {pid, _} <- entries, do: send(pid, :produced)
      end)

      {:reply, :ok, %{s | offset: offset}}
    end
  end

  # ---- harness ----

  defp percentile(sorted, p) do
    idx = max(0, min(length(sorted) - 1, round(p / 100 * (length(sorted) - 1))))
    Enum.at(sorted, idx)
  end

  defp stats(samples) do
    sorted = Enum.sort(samples)
    %{
      p50: percentile(sorted, 50),
      p99: percentile(sorted, 99),
      mean: round(Enum.sum(samples) / length(samples)),
      max: List.last(sorted)
    }
  end

  defp busy_wait_until(fun) do
    if fun.(), do: :ok, else: (Process.sleep(0); busy_wait_until(fun))
  end

  # One trial of mechanism A: spawn W waiters, wait until all parked, time a produce that wakes all.
  defp trial_a(pid, w) do
    parent = self()
    base = ServerA.offset(pid)

    for _ <- 1..w do
      spawn(fn ->
        {:ok, _} = ServerA.wait(pid, base)
        send(parent, :woke)
      end)
    end

    busy_wait_until(fn -> ServerA.waiter_count(pid) == w end)

    t0 = System.monotonic_time(:microsecond)
    ServerA.produce(pid)
    producer_block = System.monotonic_time(:microsecond) - t0
    for _ <- 1..w, do: receive(do: (:woke -> :ok))
    wakeup_total = System.monotonic_time(:microsecond) - t0

    {wakeup_total, producer_block}
  end

  defp trial_b(pid, registry, w) do
    parent = self()
    base = ServerB.offset(pid)

    for _ <- 1..w do
      spawn(fn ->
        {:ok, _} = Registry.register(registry, :topic, nil)
        send(parent, :ready)

        receive do
          :produced ->
            {:ok, _} = ServerB.read(pid, base)
            send(parent, :woke)
        end
      end)
    end

    for _ <- 1..w, do: receive(do: (:ready -> :ok))
    busy_wait_until(fn -> Registry.count(registry) == w end)

    t0 = System.monotonic_time(:microsecond)
    ServerB.produce(pid, registry)
    producer_block = System.monotonic_time(:microsecond) - t0
    for _ <- 1..w, do: receive(do: (:woke -> :ok))
    wakeup_total = System.monotonic_time(:microsecond) - t0

    {wakeup_total, producer_block}
  end

  defp trials_for(w) when w >= 1000, do: 30
  defp trials_for(w) when w >= 100, do: 100
  defp trials_for(_w), do: 300

  def run do
    {:ok, _} = Registry.start_link(keys: :duplicate, name: BenchRegistry)

    IO.puts("\nLong-poll mechanism benchmark (#{@reconsume_iters}-iter re-consume per wakeup)")
    IO.puts("All times in microseconds (µs). N trials per W.\n")

    for w <- [1, 10, 100, 1000] do
      trials = trials_for(w)

      {:ok, a} = ServerA.start_link()
      a_samples = for _ <- 1..trials, do: trial_a(a, w)
      GenServer.stop(a)

      {:ok, b} = ServerB.start_link()
      b_samples = for _ <- 1..trials, do: trial_b(b, BenchRegistry, w)
      GenServer.stop(b)

      a_total = stats(Enum.map(a_samples, &elem(&1, 0)))
      a_block = stats(Enum.map(a_samples, &elem(&1, 1)))
      b_total = stats(Enum.map(b_samples, &elem(&1, 0)))
      b_block = stats(Enum.map(b_samples, &elem(&1, 1)))

      IO.puts("W = #{w}  (#{trials} trials)")
      IO.puts("  wakeup_total   A: p50=#{a_total.p50} p99=#{a_total.p99} mean=#{a_total.mean}   B: p50=#{b_total.p50} p99=#{b_total.p99} mean=#{b_total.mean}")
      IO.puts("  producer_block A: p50=#{a_block.p50} p99=#{a_block.p99} mean=#{a_block.mean}   B: p50=#{b_block.p50} p99=#{b_block.p99} mean=#{b_block.mean}")
      IO.puts("")
    end
  end
end

LongPollBench.run()
