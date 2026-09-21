defmodule Malachi.Retention.SkipReporter do
  @moduledoc """
  Turns the skips a consume read reports (`Malachi.Broker.Skip`: data a consumer was moved past because
  it was no longer stored) into telemetry and a rate-limited log line, attributed to the consumer group
  that read them.

  One runs beside each data-plane broker (`name_for/1` derives its name from the broker's). The broker
  itself never learns what a group is on the fetch path: it hands the skips back with the page, and the
  consumer layer (`Malachi.LogApi`, or the push loop, which already serves a group) sends them here by
  `report/4`, a cast. The deduplication and the log's rate limit live in this process's
  `Malachi.Retention.SkipLedger`, off the broker's loop, which serializes every produce and consume.

  Reporting is best effort by design: a cast to a reporter that is restarting is dropped, which loses a
  count, never a record, and never fails the read that found the skip. A message this server has no
  clause for is dropped the way every long-lived server here drops one, through
  `Malachi.UnexpectedMessage`: counted by server and kind, its shape logged once per process, and a call
  answered rather than left to time out.

  Options: `:name`, `:clock` (`(-> integer())` monotonic milliseconds, default
  `System.monotonic_time/1`), `:max` (ledger entries, default `:retention_skip_ledger_max`, 10_000) and
  `:window_ms` (how often one reader may be logged, default `:retention_skip_log_window_ms`, 10 minutes).
  """

  use GenServer

  require Logger

  alias Malachi.Broker.Skip
  alias Malachi.I18n
  alias Malachi.Retention.SkipLedger
  alias Malachi.Telemetry
  alias Malachi.UnexpectedMessage

  @default_max 10_000
  @default_window_ms 600_000

  @doc "Starts a reporter. See the module doc for the options."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_server_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @doc """
  The reporter that serves the broker registered as `broker_name`, or `nil` for a broker with no
  registered name (a pid, or a via tuple), which has no reporter beside it.
  """
  @spec name_for(GenServer.server()) :: atom() | nil
  def name_for(broker_name) when is_atom(broker_name) and not is_nil(broker_name),
    do: Module.concat(broker_name, SkipReporter)

  def name_for(_unnamed_broker), do: nil

  @doc """
  Reports the `skips` a page read on `topic` handed to `group` (`nil` for a fetch outside a group).
  Always `:ok`: nothing is sent for an empty list or a `nil` reporter, and a cast to a reporter that is
  not running is dropped.
  """
  @spec report(atom() | pid() | nil, String.t(), String.t() | nil, [Skip.t()]) :: :ok
  def report(_reporter, _topic, _group, []), do: :ok
  def report(nil, _topic, _group, _skips), do: :ok
  def report(reporter, topic, group, skips), do: GenServer.cast(reporter, {:report, topic, group, skips})

  @impl true
  def init(opts) do
    max =
      Keyword.get_lazy(opts, :max, fn -> Application.get_env(:malachi, :retention_skip_ledger_max, @default_max) end)

    window_ms =
      Keyword.get_lazy(opts, :window_ms, fn ->
        Application.get_env(:malachi, :retention_skip_log_window_ms, @default_window_ms)
      end)

    clock = Keyword.get(opts, :clock, fn -> System.monotonic_time(:millisecond) end)
    # `unexpected_shapes`: the unknown message shapes already logged (see `Malachi.UnexpectedMessage`).
    {:ok, %{ledger: SkipLedger.new(max, window_ms), clock: clock, unexpected_shapes: MapSet.new()}}
  end

  @impl true
  def handle_cast({:report, topic, group, skips}, state) do
    now_ms = state.clock.()
    ledger = Enum.reduce(skips, state.ledger, &observe(&1, &2, topic, group, now_ms))
    {:noreply, %{state | ledger: ledger}}
  end

  def handle_cast(message, state), do: {:noreply, drop_unexpected(state, :cast, message)}

  @impl true
  def handle_info(message, state), do: {:noreply, drop_unexpected(state, :info, message)}

  # Nothing calls this server; without this clause a call from a newer node would wait out its timeout
  # instead of getting an answer it can fall back on.
  @impl true
  def handle_call(message, _from, state) do
    {:reply, UnexpectedMessage.unknown_call_reply(), drop_unexpected(state, :call, message)}
  end

  defp drop_unexpected(state, kind, message) do
    %{state | unexpected_shapes: UnexpectedMessage.drop(state.unexpected_shapes, :skip_reporter, kind, message)}
  end

  # One skip is identified by who read it and where it began; the log is rate limited per reader, a
  # group on a range.
  defp observe(%Skip{} = skip, ledger, topic, group, now_ms) do
    skip_key = {topic, group, skip.range_id, skip.source_range_id, skip.from}
    log_key = {topic, group, skip.range_id}
    {ledger, verdict} = SkipLedger.observe(ledger, skip_key, log_key, now_ms)
    act(verdict, skip, topic, group)
    ledger
  end

  defp act(:duplicate, _skip, _topic, _group), do: :ok
  defp act(:count, skip, topic, group), do: Telemetry.retention_skip(topic, group, skip)

  defp act({:log, held}, skip, topic, group) do
    Telemetry.retention_skip(topic, group, skip)

    Logger.warning(
      I18n.t(:retention_consumer_skipped,
        group: inspect(group),
        topic: topic,
        offsets: skip.offsets,
        range: inspect(skip.range_id),
        source_range: inspect(skip.source_range_id),
        origin: skip.origin,
        span: Skip.span(skip),
        held: held
      )
    )
  end
end
