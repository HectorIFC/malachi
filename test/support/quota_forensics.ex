defmodule Malachi.Test.QuotaForensics do
  @moduledoc """
  Makes a quota test that admits over its limit say why (issue #151).

  The sharded publish/subscribe window (`Malachi.RateLimiter.check_limit_in_caller/3`) has admitted one
  request over the limit twice in CI and never on rerun. A bare `assert {:error, "rate_limited"} = ...`
  only says that it happened. The helpers here take a snapshot of everything that decides between the
  explanations still open, before the requests and again at the moment an over-limit one is admitted:

  - the limiter pid: a restart recreates its table empty and refunds every quota
  - the time warp mode and the OS minus Erlang system time offset: a clock step
  - the current window start by the limiter's own clock: a window boundary crossed between requests
  - the shard count, the caps and every window counter held for the identifier: the arithmetic

  ## Window boundaries

  A fixed window hands out a fresh quota when it rolls over, and that is documented behavior, not the
  defect. `within_one_window/3` reruns a measurement that straddled a boundary, and `assert_refused/4`
  tells it to when an admission coincides with a new window.

  Setting `MALACHI_QUOTA_FORENSICS=strict` (the CI hunt does) turns that rerun into a failure carrying the
  report for an over-limit admission, and prints a line for every straddle that did not change an
  assertion, so a hunt counts boundaries instead of hiding them.
  """

  import ExUnit.Assertions

  alias Malachi.RateLimiter

  @strict_env "MALACHI_QUOTA_FORENSICS"
  @default_attempts 5

  defmodule Straddled do
    @moduledoc false
    # Raised by `assert_refused/4` when the admission it caught happened in a later window than the
    # snapshot it was given, so `within_one_window/3` can rerun the measurement.
    defexception [:report]

    @impl true
    def message(%{report: report}), do: "an admission crossed a window boundary\n" <> report
  end

  @doc "Whether the CI hunt asked for boundary crossings to be reported instead of rerun."
  @spec strict?() :: boolean()
  def strict?, do: System.get_env(@strict_env) == "strict"

  @doc """
  Runs `fun` and returns its result, guaranteeing it ran inside ONE window of `window_ms`, measured by the
  limiter's own clock (`Malachi.RateLimiter.current_window_start/1`).

  A run that straddled a boundary, or whose `assert_refused/4` raised `Straddled`, is rerun up to
  `attempts` times in all. `fun` must therefore build its own fresh state (a new identifier or user) on
  every call, or a rerun would inherit a half-spent quota.
  """
  @spec within_one_window(pos_integer(), (-> result), pos_integer()) :: result when result: term()
  def within_one_window(window_ms, fun, attempts \\ @default_attempts) do
    started_in = RateLimiter.current_window_start(window_ms)

    outcome =
      try do
        {:ok, fun.()}
      rescue
        error in Straddled -> {:straddled, error}
      end

    ended_in = RateLimiter.current_window_start(window_ms)

    case outcome do
      {:ok, result} when started_in == ended_in ->
        result

      {:ok, _result} ->
        note_straddle(window_ms, started_in, ended_in)
        retry(window_ms, fun, attempts)

      {:straddled, error} ->
        if strict?(), do: flunk(Exception.message(error))
        retry(window_ms, fun, attempts)
    end
  end

  defp retry(window_ms, fun, attempts) when attempts > 1,
    do: within_one_window(window_ms, fun, attempts - 1)

  defp retry(window_ms, _fun, _attempts),
    do: flunk("every attempt straddled a #{window_ms}ms window boundary; the measurement never ran clean")

  defp note_straddle(window_ms, started_in, ended_in) do
    if strict?() do
      IO.puts("quota-forensics straddle: window_ms=#{window_ms} started_in=#{started_in} ended_in=#{ended_in}")
    end
  end

  @doc """
  Everything that decides why a quota for `identifier` and `action` would admit over its limit, taken
  now. `action` must have a configured limit (`Malachi.RateLimiter.action_config/1`), or `limit` is
  given explicitly.
  """
  @spec snapshot(term(), atom(), %{limit: pos_integer(), window_ms: pos_integer()} | nil) :: map()
  def snapshot(identifier, action, config \\ nil) do
    %{limit: limit, window_ms: window_ms} = config || RateLimiter.action_config(action)

    %{
      limiter_pid: Process.whereis(RateLimiter),
      time_warp_mode: :erlang.system_info(:time_warp_mode),
      os_minus_system_ms: System.os_time(:millisecond) - System.system_time(:millisecond),
      system_time_ms: System.system_time(:millisecond),
      monotonic_time_ms: System.monotonic_time(:millisecond),
      limit: limit,
      window_ms: window_ms,
      window_start: RateLimiter.current_window_start(window_ms),
      schedulers_online: :erlang.system_info(:schedulers_online),
      shard_caps: RateLimiter.shard_caps(limit),
      counters: RateLimiter.window_counters(identifier, action)
    }
  end

  @doc """
  Asserts that `result` is the refusal a quota over its limit answers, `{:error, "rate_limited"}` from
  the wire. On any other result, the failure carries `before` (a `snapshot/3` taken before the requests
  that spent the quota) next to a snapshot taken now.

  An `:ok` that landed in a later window than `before` is the documented boundary burst: it raises
  `Straddled` so `within_one_window/3` reruns the test (or, in strict mode, reports it).
  """
  @spec assert_refused(term(), map(), term(), atom()) :: term()
  def assert_refused({:error, "rate_limited"} = result, _before, _identifier, _action), do: result

  def assert_refused(result, before, identifier, action) do
    now = snapshot(identifier, action, Map.take(before, [:limit, :window_ms]))
    report = report(result, before, now)

    if result == :ok and now.window_start != before.window_start do
      raise Straddled, report: report
    end

    flunk(report)
  end

  @doc "The failure message for `result`, with the two snapshots and what changed between them."
  @spec report(term(), map(), map()) :: String.t()
  def report(result, before, now) do
    changed =
      for {key, value} <- now, Map.fetch!(before, key) != value, key not in [:system_time_ms, :monotonic_time_ms] do
        "  #{key}: #{inspect(Map.fetch!(before, key))} -> #{inspect(value)}"
      end

    """
    expected {:error, "rate_limited"}, got #{inspect(result)}
    changed since the quota was first spent:
    #{if changed == [], do: "  nothing", else: Enum.join(changed, "\n")}
    elapsed: system #{now.system_time_ms - before.system_time_ms}ms, \
    monotonic #{now.monotonic_time_ms - before.monotonic_time_ms}ms
    before: #{inspect(before, limit: :infinity)}
    now:    #{inspect(now, limit: :infinity)}
    """
  end
end
