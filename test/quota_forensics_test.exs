defmodule Malachi.Test.QuotaForensicsTest do
  # async: false. Toggles MALACHI_QUOTA_FORENSICS and reads the shared limiter table.
  #
  # The forensic helper is what turns the next over-limit admission into a diagnosis (issue #151), so a
  # helper that silently reran, swallowed or under-reported a failure would hide exactly what it exists to
  # show. These pin that it does not.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Malachi.Test.QuotaForensics
  alias Malachi.Test.QuotaForensics.Straddled

  @config %{limit: 1, window_ms: 60_000}
  @fields ~w(limiter_pid time_warp_mode os_minus_system_ms window_start schedulers_online shard_caps counters)

  setup do
    prior = System.get_env("MALACHI_QUOTA_FORENSICS")
    System.delete_env("MALACHI_QUOTA_FORENSICS")

    on_exit(fn ->
      if prior, do: System.put_env("MALACHI_QUOTA_FORENSICS", prior), else: System.delete_env("MALACHI_QUOTA_FORENSICS")
    end)

    :ok
  end

  defp identifier, do: "forensics_#{System.unique_integer([:positive])}"

  defp strict, do: System.put_env("MALACHI_QUOTA_FORENSICS", "strict")

  # Counts calls in the test process, so a rerun is observable.
  defp counting(fun) do
    fn ->
      Process.put(:calls, Process.get(:calls, 0) + 1)
      fun.(Process.get(:calls))
    end
  end

  describe "snapshot/3" do
    test "records every field the report needs, with the explicit config" do
      id = identifier()
      snap = QuotaForensics.snapshot(id, :publish, @config)

      for field <- @fields, do: assert(Map.has_key?(snap, String.to_atom(field)), "missing #{field}")
      assert snap.limiter_pid == Process.whereis(Malachi.RateLimiter)
      assert snap.limit == 1 and snap.window_ms == 60_000
      assert Enum.sum(snap.shard_caps) == 1
      assert snap.counters == []
    end

    test "reads the configured limit when none is given" do
      prior =
        {Application.get_env(:malachi, :publish_rate_limit), Application.get_env(:malachi, :publish_rate_window_ms)}

      Application.put_env(:malachi, :publish_rate_limit, 7)
      Application.put_env(:malachi, :publish_rate_window_ms, 30_000)

      on_exit(fn ->
        {limit, window} = prior
        Application.put_env(:malachi, :publish_rate_limit, limit)
        Application.put_env(:malachi, :publish_rate_window_ms, window)
      end)

      assert %{limit: 7, window_ms: 30_000} = QuotaForensics.snapshot(identifier(), :publish)
    end
  end

  describe "assert_refused/4" do
    test "passes a refusal through untouched" do
      id = identifier()
      snap = QuotaForensics.snapshot(id, :publish, @config)

      assert QuotaForensics.assert_refused({:error, "rate_limited"}, snap, id, :publish) == {:error, "rate_limited"}
    end

    test "an admission in the same window fails with both snapshots and every field named" do
      id = identifier()
      snap = QuotaForensics.snapshot(id, :publish, @config)

      error = assert_raise ExUnit.AssertionError, fn -> QuotaForensics.assert_refused(:ok, snap, id, :publish) end

      assert error.message =~ ~s(expected {:error, "rate_limited"}, got :ok)
      assert error.message =~ "before:"
      assert error.message =~ "now:"
      for field <- @fields, do: assert(error.message =~ "#{field}:", "report does not name #{field}")
    end

    test "names what changed between the snapshots, and says so when nothing did" do
      id = identifier()
      snap = QuotaForensics.snapshot(id, :publish, @config)

      unchanged = QuotaForensics.report(:ok, snap, snap)
      assert unchanged =~ "changed since the quota was first spent:\n  nothing"

      moved = QuotaForensics.report(:ok, snap, %{snap | limiter_pid: self(), system_time_ms: 0})
      assert moved =~ "limiter_pid: #{inspect(snap.limiter_pid)} -> #{inspect(self())}"
      # the two clocks always move, so they are reported as elapsed time rather than as a change
      refute moved =~ "  system_time_ms:"
    end

    test "an admission in a later window raises Straddled, not an assertion failure" do
      id = identifier()
      snap = %{QuotaForensics.snapshot(id, :publish, @config) | window_start: -60_000}

      error = assert_raise Straddled, fn -> QuotaForensics.assert_refused(:ok, snap, id, :publish) end
      assert Exception.message(error) =~ "crossed a window boundary"
      assert Exception.message(error) =~ "window_start: -60000 ->"
    end

    test "a different error is a failure even in a later window: only an admission is a boundary burst" do
      id = identifier()
      snap = %{QuotaForensics.snapshot(id, :publish, @config) | window_start: -60_000}

      error =
        assert_raise ExUnit.AssertionError, fn ->
          QuotaForensics.assert_refused({:error, "overloaded"}, snap, id, :publish)
        end

      assert error.message =~ ~s(got {:error, "overloaded"})
    end
  end

  describe "within_one_window/3" do
    test "returns the result of a run that stayed inside one window, running it once" do
      assert QuotaForensics.within_one_window(60_000, counting(fn calls -> {:ran, calls} end)) == {:ran, 1}
    end

    test "reruns after a Straddled and returns the clean attempt" do
      fun =
        counting(fn
          1 -> raise Straddled, report: "boundary"
          calls -> {:ran, calls}
        end)

      assert QuotaForensics.within_one_window(60_000, fun) == {:ran, 2}
    end

    test "gives up after the given number of attempts" do
      fun = counting(fn _calls -> raise Straddled, report: "boundary" end)

      error = assert_raise ExUnit.AssertionError, fn -> QuotaForensics.within_one_window(60_000, fun, 3) end
      assert error.message =~ "every attempt straddled a 60000ms window boundary"
      assert Process.get(:calls) == 3
    end

    test "reruns a run whose own window changed under it" do
      # A 1ms window turns over during a 2ms sleep, every time.
      fun = counting(fn _calls -> Process.sleep(2) end)

      assert_raise ExUnit.AssertionError, ~r/every attempt straddled a 1ms/, fn ->
        QuotaForensics.within_one_window(1, fun, 2)
      end

      assert Process.get(:calls) == 2
    end

    test "does not rerun other failures" do
      fun = counting(fn _calls -> flunk("a real failure") end)

      assert_raise ExUnit.AssertionError, ~r/a real failure/, fn -> QuotaForensics.within_one_window(60_000, fun) end
      assert Process.get(:calls) == 1
    end

    test "in strict mode a Straddled fails at once with its report" do
      strict()
      fun = counting(fn _calls -> raise Straddled, report: "the report" end)

      error = assert_raise ExUnit.AssertionError, fn -> QuotaForensics.within_one_window(60_000, fun) end
      assert error.message =~ "crossed a window boundary\nthe report"
      assert Process.get(:calls) == 1
    end

    test "in strict mode a straddle that changed no assertion is printed and rerun" do
      strict()
      fun = counting(fn _calls -> Process.sleep(2) end)

      output =
        capture_io(fn ->
          assert_raise ExUnit.AssertionError, fn -> QuotaForensics.within_one_window(1, fun, 2) end
        end)

      assert output =~ "quota-forensics straddle: window_ms=1 started_in="
      assert Process.get(:calls) == 2
    end

    test "strict mode is off unless the variable says strict" do
      refute QuotaForensics.strict?()
      System.put_env("MALACHI_QUOTA_FORENSICS", "yes")
      refute QuotaForensics.strict?()
      strict()
      assert QuotaForensics.strict?()
    end
  end
end
