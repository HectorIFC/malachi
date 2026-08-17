# Server-ceiling profiler: classifies the produce ceiling as CPU-bound, network-bound, or
# filesystem-bound, and names the hot functions.
#
# Run with the server env you want to profile, e.g.:
#
#     MALACHI_GROUP_COMMIT=true mix run benchmark/profile_server.exs
#
# The script runs inside the server VM (mix run starts the app). The load client is spawned as a
# SEPARATE OS process (its own BEAM via `mix malachi.loadtest`), so the profiled VM contains only the
# server; the client competes for host cores but not for this VM's schedulers or profile counters.
#
# While the client drives steady-state produce load, it samples:
#   - :msacc (microstate accounting): where scheduler threads spend time (emulator = executing Elixir
#     code, port = socket I/O drivers, gc, sleep = idle/waiting). Dirty IO schedulers busy = file
#     I/O (fsync) pressure.
#   - :scheduler.utilization/1: overall busy fraction.
#   - eprof over the two hot singletons (BrokerServer + ReplicationServer): which functions burn the
#     emulator time (route? encode? store append? flush?).
#
# Reading the result:
#   - emulator high + utilization high        -> CPU-bound (eprof says where)
#   - port high                               -> network-bound (socket driver work dominates)
#   - dirty io busy / sleep high while load waits -> filesystem-bound (fsync latency)
#   - everything low                          -> the ceiling is not in this VM (look at the client)

# :eprof lives in OTP's :tools app, which Elixir does not load by default.
Mix.ensure_application!(:tools)

defmodule ProfileServer do
  @duration_s 10
  @warmup_s 2
  @connections 64
  @batch 100
  @topics 8

  def run do
    port = Application.get_env(:malachi, :tcp_port, 4040)

    IO.puts("""
    server VM: schedulers=#{:erlang.system_info(:schedulers_online)} \
    dirty_io=#{:erlang.system_info(:dirty_io_schedulers)} \
    group_commit=#{Application.get_env(:malachi, :group_commit, false)}
    load: produce #{@connections} conns, batch #{@batch}, #{@topics} topics, #{@warmup_s}+#{@duration_s}s (external client VM)
    """)

    client =
      Task.async(fn ->
        System.cmd(
          "mix",
          ~w(malachi.loadtest --host 127.0.0.1 --port #{port} --scenario produce
             --connections #{@connections} --batch #{@batch} --topics #{@topics}
             --duration #{@duration_s} --warmup #{@warmup_s} --record-size 256 --json),
          stderr_to_stdout: true
        )
      end)

    # Let the client connect and pass warmup, then sample the steady state.
    Process.sleep((@warmup_s + 2) * 1000)

    util_task = Task.async(fn -> :scheduler.utilization(4) end)
    msacc_stats = sample_msacc(4_000)
    eprof = sample_eprof(2_000)
    util = Task.await(util_task, 10_000)

    {out, status} = Task.await(client, (@warmup_s + @duration_s + 60) * 1000)

    # A failed or reportless client means the profile sampled an idle or partially loaded VM: any
    # verdict from it would be garbage, so refuse to classify instead of printing a false conclusion.
    client_json = out |> String.split("\n") |> Enum.find(&String.starts_with?(String.trim(&1), "{"))

    if status != 0 or client_json == nil do
      IO.puts("\nclient failed (exit #{status}); output tail:\n#{String.slice(out, -400, 400)}")
      IO.puts("\n== verdict ==\n  NONE: the load client failed, so the samples do not reflect steady-state load")
      exit({:shutdown, 1})
    end

    print_msacc(msacc_stats)
    print_utilization(util)
    print_eprof(eprof)
    IO.puts("\n== client report ==\n  #{client_json}")
    classify(msacc_stats, util)
  end

  # --- msacc ---

  defp sample_msacc(ms) do
    :msacc.start(ms)
    stats = :msacc.stats()
    :msacc.stop()
    stats
  end

  # Aggregates msacc per thread type into percent-of-time per state.
  defp print_msacc(stats) do
    by_type =
      stats
      |> Enum.group_by(& &1.type)
      |> Enum.map(fn {type, threads} ->
        totals =
          Enum.reduce(threads, %{}, fn thread, acc ->
            Enum.reduce(thread.counters, acc, fn {state, us}, acc2 ->
              Map.update(acc2, state, us, &(&1 + us))
            end)
          end)

        total = totals |> Map.values() |> Enum.sum() |> max(1)
        {type, totals, total}
      end)

    IO.puts("== msacc: % of thread time per state ==")

    for {type, totals, total} <- Enum.sort(by_type), type in [:scheduler, :dirty_io_scheduler, :dirty_cpu_scheduler] do
      line =
        [:emulator, :port, :gc, :other, :aux, :check_io, :sleep]
        |> Enum.map(fn state ->
          pct = Float.round(Map.get(totals, state, 0) / total * 100, 1)
          "#{state}=#{pct}%"
        end)
        |> Enum.join("  ")

      IO.puts("  #{type}: #{line}")
    end
  end

  defp print_utilization(util) do
    total = for({:total, frac, _} <- util, do: frac) |> List.first() || 0.0
    IO.puts("\n== scheduler utilization ==\n  total busy: #{Float.round(total * 100, 1)}%")
  end

  # --- eprof over the hot singletons ---

  defp sample_eprof(ms) do
    pids = for name <- [Malachi.LogBroker, Malachi.LogReplication], pid = Process.whereis(name), do: pid

    if pids == [] do
      :no_processes
    else
      :eprof.start()
      :eprof.start_profiling(pids)
      Process.sleep(ms)
      :eprof.stop_profiling()

      # eprof prints to stdout via analyze; capture it.
      {:group_leader, gl} = Process.info(Process.whereis(:eprof), :group_leader)
      {:ok, capture} = StringIO.open("")
      Process.group_leader(Process.whereis(:eprof), capture)
      :eprof.analyze(:total, filter: [calls: 100])
      Process.group_leader(Process.whereis(:eprof), gl)
      {:ok, {_in, out}} = StringIO.close(capture)
      :eprof.stop()
      out
    end
  end

  defp print_eprof(:no_processes), do: IO.puts("\n== eprof ==\n  (broker processes not found)")

  defp print_eprof(out) do
    IO.puts("\n== eprof: hottest functions in BrokerServer + ReplicationServer (top lines) ==")
    out |> String.split("\n") |> Enum.take(-18) |> Enum.each(&IO.puts("  " <> &1))
  end

  # --- classification ---

  defp classify(stats, util) do
    sched = Enum.filter(stats, &(&1.type == :scheduler))

    {emu, prt, slp, total} =
      Enum.reduce(sched, {0, 0, 0, 0}, fn thread, {e, p, s, t} ->
        e2 = e + Map.get(thread.counters, :emulator, 0)
        p2 = p + Map.get(thread.counters, :port, 0)
        s2 = s + Map.get(thread.counters, :sleep, 0)
        {e2, p2, s2, t + Enum.sum(Map.values(thread.counters))}
      end)

    emu_pct = emu / max(total, 1) * 100
    port_pct = prt / max(total, 1) * 100
    sleep_pct = slp / max(total, 1) * 100
    dirty_io_pct = dirty_io_busy_pct(stats)
    busy = (for({:total, frac, _} <- util, do: frac) |> List.first() || 0.0) * 100

    verdict =
      cond do
        # Fsync pressure lands on the dirty IO schedulers, where regular schedulers can look idle:
        # check it FIRST or a filesystem-bound server reads as "not saturated".
        dirty_io_pct > 50 -> "filesystem-bound: dirty IO schedulers busy (fsync pressure dominates)"
        busy > 75 and emu_pct > port_pct -> "CPU-bound: schedulers busy executing code; see eprof for the hot path"
        busy > 75 -> "network-bound: schedulers busy but mostly in port (socket driver) work"
        port_pct > emu_pct and sleep_pct < 50 -> "network-leaning: port work dominates the non-idle time"
        sleep_pct > 70 -> "not saturated in this VM: mostly idle; the ceiling is latency or the client"
        true -> "mixed: no single state dominates; read the msacc table"
      end

    IO.puts("\n== verdict ==\n  #{verdict}")
  end

  # Fraction of dirty IO scheduler time NOT spent sleeping: the fsync/file-IO pressure signal.
  defp dirty_io_busy_pct(stats) do
    {busy, total} =
      stats
      |> Enum.filter(&(&1.type == :dirty_io_scheduler))
      |> Enum.reduce({0, 0}, fn thread, {b, t} ->
        thread_total = Enum.sum(Map.values(thread.counters))
        {b + thread_total - Map.get(thread.counters, :sleep, 0), t + thread_total}
      end)

    busy / max(total, 1) * 100
  end
end

ProfileServer.run()
