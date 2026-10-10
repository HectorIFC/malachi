defmodule Malachi.BrokerServer.ConsumeIndex do
  @moduledoc """
  The consume streams and `fetch_range` calls a `Malachi.BrokerServer` serves, indexed by range, so a
  write to a range costs a look at that range's readers and nobody else's.

  A consume stream is held by the monitor ref of the connection that opened it: its range, and the end of
  the range's durable records it read up to while it waits for more (`armed`), nil while it reads. A
  `fetch_range` call that found nothing past its position waits here, by ref, until its range's durable
  records pass that position or its time runs out.

  Pure: the broker decides what is ready, sends the wakes and replies; this keeps the bookkeeping.
  """

  alias Malachi.Metadata

  defstruct consumers: %{}, waiters: %{}, consumer_ranges: %{}, waiter_ranges: %{}

  @type consumer :: %{pid: pid(), range_id: Metadata.range_id(), armed: integer() | nil}
  @type waiter :: %{from: GenServer.from(), range_id: Metadata.range_id(), position: term(), timer: reference()}

  @type t :: %__MODULE__{
          consumers: %{reference() => consumer()},
          waiters: %{reference() => waiter()},
          consumer_ranges: %{Metadata.range_id() => MapSet.t(reference())},
          waiter_ranges: %{Metadata.range_id() => MapSet.t(reference())}
        }

  @doc "No reader yet."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc "Adds the consume stream `token` on `range_id`, waiting for any durable record (`armed` -1)."
  @spec put_consumer(t(), reference(), pid(), Metadata.range_id()) :: t()
  def put_consumer(%__MODULE__{} = index, token, pid, range_id) do
    %{
      index
      | consumers: Map.put(index.consumers, token, %{pid: pid, range_id: range_id, armed: -1}),
        consumer_ranges: add(index.consumer_ranges, range_id, token)
    }
  end

  @doc "The consume stream `token`, or `:error`."
  @spec fetch_consumer(t(), reference()) :: {:ok, consumer()} | :error
  def fetch_consumer(%__MODULE__{consumers: consumers}, token), do: Map.fetch(consumers, token)

  @doc "Whether `token` names a consume stream here."
  @spec consumer?(t(), reference()) :: boolean()
  def consumer?(%__MODULE__{consumers: consumers}, token), do: is_map_key(consumers, token)

  @doc "Drops the consume stream `token`; a token it does not hold changes nothing."
  @spec drop_consumer(t(), reference()) :: t()
  def drop_consumer(%__MODULE__{} = index, token) do
    case Map.pop(index.consumers, token) do
      {nil, _consumers} ->
        index

      {consumer, consumers} ->
        %{index | consumers: consumers, consumer_ranges: remove(index.consumer_ranges, consumer.range_id, token)}
    end
  end

  @doc "The consume stream `token`, which it holds, read up to `seen_end` and waits for the range to grow past it."
  @spec arm(t(), reference(), integer()) :: t()
  def arm(%__MODULE__{} = index, token, seen_end),
    do: %{index | consumers: Map.update!(index.consumers, token, &%{&1 | armed: seen_end})}

  @doc "Whether any consume stream or waiter is on `range_id`."
  @spec readers?(t(), Metadata.range_id()) :: boolean()
  def readers?(%__MODULE__{} = index, range_id),
    do: is_map_key(index.consumer_ranges, range_id) or is_map_key(index.waiter_ranges, range_id)

  @doc "Adds the `fetch_range` call waiting under `ref`."
  @spec put_waiter(t(), reference(), waiter()) :: t()
  def put_waiter(%__MODULE__{} = index, ref, waiter) do
    %{
      index
      | waiters: Map.put(index.waiters, ref, waiter),
        waiter_ranges: add(index.waiter_ranges, waiter.range_id, ref)
    }
  end

  @doc "Takes the waiter `ref` out, or `:error` for one already answered."
  @spec pop_waiter(t(), reference()) :: {waiter(), t()} | :error
  def pop_waiter(%__MODULE__{} = index, ref) do
    case Map.pop(index.waiters, ref) do
      {nil, _waiters} ->
        :error

      {waiter, waiters} ->
        {waiter, %{index | waiters: waiters, waiter_ranges: remove(index.waiter_ranges, waiter.range_id, ref)}}
    end
  end

  @doc "Every range a reader here is on."
  @spec ranges(t()) :: [Metadata.range_id()]
  def ranges(%__MODULE__{} = index), do: Enum.uniq(Map.keys(index.consumer_ranges) ++ Map.keys(index.waiter_ranges))

  @doc """
  The readers of `range_id` that are ready, taken out of waiting: the armed consume streams whose `seen_end`
  is below `durable_end` (disarmed, since they read until they ask again), and the waiters `ready?` accepts
  (removed). `{consumers, waiters, index}`.
  """
  @spec take_ready(t(), Metadata.range_id(), non_neg_integer(), (waiter() -> boolean())) ::
          {[{reference(), consumer()}], [{reference(), waiter()}], t()}
  def take_ready(%__MODULE__{} = index, range_id, durable_end, ready?) do
    consumers =
      for token <- Map.get(index.consumer_ranges, range_id, MapSet.new()),
          %{armed: seen_end} = consumer = Map.fetch!(index.consumers, token),
          is_integer(seen_end) and durable_end > seen_end,
          do: {token, consumer}

    waiters =
      for ref <- Map.get(index.waiter_ranges, range_id, MapSet.new()),
          waiter = Map.fetch!(index.waiters, ref),
          ready?.(waiter),
          do: {ref, waiter}

    index =
      Enum.reduce(consumers, index, fn {token, consumer}, acc ->
        %{acc | consumers: Map.put(acc.consumers, token, %{consumer | armed: nil})}
      end)

    index = Enum.reduce(waiters, index, fn {ref, _waiter}, acc -> elem(pop_waiter(acc, ref), 1) end)
    {consumers, waiters, index}
  end

  @doc """
  Splits the readers by `keep?` (given the range): the consume streams and waiters of the ranges it keeps
  stay, the rest are taken out and returned. `{consumers, waiters, index}`.
  """
  @spec take_unless(t(), (Metadata.range_id() -> boolean())) ::
          {[{reference(), consumer()}], [{reference(), waiter()}], t()}
  def take_unless(%__MODULE__{} = index, keep?) do
    gone = for range_id <- ranges(index), not keep?.(range_id), do: range_id

    consumers =
      for range_id <- gone, token <- Map.get(index.consumer_ranges, range_id, []), do: {token, index.consumers[token]}

    waiters = for range_id <- gone, ref <- Map.get(index.waiter_ranges, range_id, []), do: {ref, index.waiters[ref]}

    index = Enum.reduce(consumers, index, fn {token, _consumer}, acc -> drop_consumer(acc, token) end)
    index = Enum.reduce(waiters, index, fn {ref, _waiter}, acc -> elem(pop_waiter(acc, ref), 1) end)
    {consumers, waiters, index}
  end

  defp add(by_range, range_id, key), do: Map.update(by_range, range_id, MapSet.new([key]), &MapSet.put(&1, key))

  # `key` is on `range_id`: every caller removes a key it just found there.
  defp remove(by_range, range_id, key) do
    keys = by_range |> Map.fetch!(range_id) |> MapSet.delete(key)
    if MapSet.size(keys) == 0, do: Map.delete(by_range, range_id), else: Map.put(by_range, range_id, keys)
  end
end
