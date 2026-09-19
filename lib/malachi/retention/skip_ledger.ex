defmodule Malachi.Retention.SkipLedger do
  @moduledoc """
  The pure bookkeeping behind `Malachi.Retention.SkipReporter`: which skips were already reported, and
  when each reader was last logged.

  A consumer group re-reads from its committed position until it commits, so the page that stepped over
  expired data is served, with the same skip, on every poll in between. Counting each of those would
  turn one loss into as many as the group polled. `observe/4` answers `:duplicate` for a skip it has
  already seen, so a skip is counted once however often it is re-read.

  Logging is rate limited per reader (the caller's `log_key`, a group on a range): the first skip of a
  window is logged, the rest of that window only counted, and the next line says how many were held
  back.

  Both tables are bounded by `max` entries and forget the oldest first (insertion order), so a flood of
  distinct skips costs a fixed amount of memory. What is forgotten is only this bookkeeping: a forgotten
  skip read again is counted again, which errs towards an alert rather than towards silence.
  """

  # The two queues are built in `new/2` rather than as struct defaults: a default is a literal fixed at
  # compile time, which breaks `:queue`'s opaque type for dialyzer.
  @enforce_keys [:max, :window_ms, :seen_order, :logged_order]
  defstruct [:max, :window_ms, :seen_order, :logged_order, seen: %{}, logged: %{}]

  @typedoc "A ledger. Build it with `new/2` and read it only through this module."
  @type t :: %__MODULE__{
          max: pos_integer(),
          window_ms: non_neg_integer(),
          seen: %{term() => true},
          seen_order: :queue.queue(term()),
          logged: %{term() => {integer(), non_neg_integer()}},
          logged_order: :queue.queue(term())
        }

  @typedoc """
  What to do with a skip: nothing (`:duplicate`), count it (`:count`), or count it and log it, saying
  how many skips of the same reader were counted without a line since the last one (`{:log, held}`).
  """
  @type verdict :: :duplicate | :count | {:log, non_neg_integer()}

  @doc "An empty ledger holding at most `max` entries per table, logging a reader at most once per `window_ms`."
  @spec new(pos_integer(), non_neg_integer()) :: t()
  def new(max, window_ms) when is_integer(max) and max > 0 and is_integer(window_ms) and window_ms >= 0 do
    %__MODULE__{max: max, window_ms: window_ms, seen_order: :queue.new(), logged_order: :queue.new()}
  end

  @doc """
  Records a sighting of the skip identified by `skip_key`, read by the reader `log_key`, at `now_ms` (a
  monotonic clock), and says what to do with it.
  """
  @spec observe(t(), term(), term(), integer()) :: {t(), verdict()}
  def observe(%__MODULE__{} = ledger, skip_key, log_key, now_ms) do
    if Map.has_key?(ledger.seen, skip_key) do
      {ledger, :duplicate}
    else
      ledger
      |> remember_skip(skip_key)
      |> log_verdict(log_key, now_ms)
    end
  end

  @doc "How many distinct skips the ledger currently remembers."
  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{seen: seen}), do: map_size(seen)

  defp remember_skip(ledger, skip_key) do
    {seen, order} = put_bounded(ledger.seen, ledger.seen_order, skip_key, true, ledger.max)
    %{ledger | seen: seen, seen_order: order}
  end

  defp log_verdict(ledger, log_key, now_ms) do
    case Map.fetch(ledger.logged, log_key) do
      {:ok, {logged_at, held}} when now_ms - logged_at < ledger.window_ms ->
        {put_logged(ledger, log_key, {logged_at, held + 1}), :count}

      {:ok, {_logged_at, held}} ->
        {put_logged(ledger, log_key, {now_ms, 0}), {:log, held}}

      :error ->
        {put_logged(ledger, log_key, {now_ms, 0}), {:log, 0}}
    end
  end

  defp put_logged(ledger, log_key, value) do
    {logged, order} = put_bounded(ledger.logged, ledger.logged_order, log_key, value, ledger.max)
    %{ledger | logged: logged, logged_order: order}
  end

  # Puts `key` in a map whose insertion order is kept in `order`, evicting the oldest key once the map
  # holds more than `max`. Updating a key already present keeps its place in the order.
  defp put_bounded(map, order, key, value, max) do
    if Map.has_key?(map, key) do
      {Map.put(map, key, value), order}
    else
      evict(Map.put(map, key, value), :queue.in(key, order), max)
    end
  end

  defp evict(map, order, max) when map_size(map) > max do
    {{:value, oldest}, order} = :queue.out(order)
    evict(Map.delete(map, oldest), order, max)
  end

  defp evict(map, order, _max), do: {map, order}
end
