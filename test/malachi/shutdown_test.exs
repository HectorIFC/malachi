defmodule Malachi.ShutdownTest do
  use ExUnit.Case, async: true

  alias Malachi.Shutdown

  # Records the seam calls in order so the orchestration can be asserted without stopping the real app.
  defp recorder do
    {:ok, log} = Agent.start_link(fn -> [] end)
    rec = fn tag -> Agent.update(log, &[tag | &1]) end
    {log, rec}
  end

  defp events(log), do: Agent.get(log, &Enum.reverse(&1))

  test "runs quiesce, then drains for drain_ms, then closes, in that order" do
    {log, rec} = recorder()

    assert :ok =
             Shutdown.graceful(
               quiesce: fn -> rec.(:quiesce) end,
               sleep: fn ms -> rec.({:sleep, ms}) end,
               close: fn -> rec.(:close) end,
               drain_ms: 2500
             )

    assert events(log) == [:quiesce, {:sleep, 2500}, :close]
  end

  test "skips the drain sleep when drain_ms is 0 (still quiesces and closes)" do
    {log, rec} = recorder()

    Shutdown.graceful(
      quiesce: fn -> rec.(:quiesce) end,
      sleep: fn ms -> rec.({:sleep, ms}) end,
      close: fn -> rec.(:close) end,
      drain_ms: 0
    )

    assert events(log) == [:quiesce, :close]
  end

  test "defaults drain_ms from the :shutdown_grace_ms config" do
    {log, rec} = recorder()
    prev = Application.get_env(:malachi, :shutdown_grace_ms)
    Application.put_env(:malachi, :shutdown_grace_ms, 1234)
    on_exit(fn -> restore(:shutdown_grace_ms, prev) end)

    Shutdown.graceful(quiesce: fn -> :ok end, sleep: fn ms -> rec.({:sleep, ms}) end, close: fn -> :ok end)

    assert events(log) == [{:sleep, 1234}]
  end

  defp restore(key, nil), do: Application.delete_env(:malachi, key)
  defp restore(key, value), do: Application.put_env(:malachi, key, value)
end
