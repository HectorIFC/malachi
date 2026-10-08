defmodule Malachi.BrokerServer.Subscribers do
  @moduledoc """
  The streaming subscribers of one `Malachi.BrokerServer`, and the bookkeeping that decides who is read
  for and when, with no process, no I/O and no read in it.

  `Malachi.BrokerServer` serializes every append, so nothing slow may run in its loop. A push to a
  streaming subscriber reads records, and a read can go to disk or to another node, so the read and the
  socket write run in the subscriber's own process (`Malachi.BrokerServer.execute_push/1`). What stays
  here is the decision: which subscriber is owed a read, how many records it may take, and what its
  position and credit become once the read is done. That split is the transcript's "many event loops"
  exchanging messages (line 786) applied to the one loop that both serialized appends and read for every
  subscriber.

  Each subscriber carries these invariants, which every function here keeps:

    * **One read at a time.** `reading` is set when a read is handed out and cleared by `read_done/3`. A
      wake or an ack that arrives meanwhile sets `wake_pending` instead of handing out a second read, so
      two reads can never start from the same position and push the same records twice, and the records
      of one subscriber reach it in order.
    * **Credit bounds the read.** A read takes at most `min(max, window - in_flight)` records, and
      `in_flight` grows by what was actually pushed, so `in_flight <= window` holds after every step on
      the push side, whatever the acks do.
    * **Every range gets its turn.** A read's budget is shared across the ranges it reads
      (`Malachi.Broker.consume_shared/5`), so when the budget is smaller than the number of ranges only
      the first ones fit. `turn` counts the reads handed out and the caller starts the range list at
      that rotation, so no range is always last.
    * **The index matches the lists.** `by_ref` maps each subscription's monitor ref to its topic, so a
      dead subscriber is removed from one topic's list in one lookup instead of a rebuild of every topic.
      It holds the topic only, never a copy of the subscriber: positions, credit, ranges and the
      coordinator change on every push and ack, and a copy would go stale. Every removal (by ref on
      `:DOWN`, by pid on unsubscribe) keeps the two in step.

  The push order of a topic is kept as a list of refs beside a map of its subscribers, so the order
  survives and a read ending (the most frequent event, one per push) touches one subscriber directly.
  """

  @typedoc "One subscription: a subscriber process streaming one topic for one group."
  @type subscriber :: %{
          required(:pid) => pid(),
          required(:ref) => reference(),
          required(:topic) => term(),
          required(:group) => term(),
          required(:positions) => map(),
          required(:window) => pos_integer(),
          required(:in_flight) => non_neg_integer(),
          required(:max) => pos_integer(),
          required(:member) => term(),
          required(:ranges) => [term()] | nil,
          required(:coordinator) => term(),
          required(:reading) => boolean(),
          required(:wake_pending) => boolean(),
          required(:turn) => non_neg_integer()
        }

  @typedoc "A read handed out: the subscriber as it was when the read started, and how many records it may take."
  @type read :: {subscriber(), pos_integer()}

  @typedoc "How a read ended: the records it pushed and where it left the positions, or a failed read."
  @type outcome :: {:ok, non_neg_integer(), map()} | :error

  @typedoc """
  A topic's subscribers: `order` is the push order (newest first, as subscribe has always added them),
  `by_ref` the subscribers themselves. A read ending touches one subscriber by its ref, so it costs a
  map update rather than a walk of the topic's list.
  """
  @type topic_subs :: %{order: [reference()], by_ref: %{reference() => subscriber()}}

  @type t :: %__MODULE__{by_topic: %{term() => topic_subs()}, by_ref: %{reference() => term()}}

  defstruct by_topic: %{}, by_ref: %{}

  @doc "No subscribers."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  Adds `subscriber` at the head of its topic's push order and hands out its first read when it has
  credit. `reading`, `wake_pending` and `turn` are set here; whatever the caller put in them is ignored.
  """
  @spec add(t(), map()) :: {[read()], t()}
  def add(%__MODULE__{} = subs, %{ref: ref, topic: topic} = subscriber) do
    subscriber = Map.merge(subscriber, %{reading: false, wake_pending: false, turn: 0})
    topic_subs = Map.get(subs.by_topic, topic, %{order: [], by_ref: %{}})

    subs = %{
      subs
      | by_topic:
          Map.put(subs.by_topic, topic, %{
            order: [ref | topic_subs.order],
            by_ref: Map.put(topic_subs.by_ref, ref, subscriber)
          }),
        by_ref: Map.put(subs.by_ref, ref, topic)
    }

    update_one(subs, topic, ref, &try_read/1)
  end

  @doc "The subscribers of `topic`, in push order."
  @spec list(t(), term()) :: [subscriber()]
  def list(%__MODULE__{by_topic: by_topic}, topic) do
    case Map.fetch(by_topic, topic) do
      {:ok, %{order: order, by_ref: by_ref}} -> Enum.map(order, &Map.fetch!(by_ref, &1))
      :error -> []
    end
  end

  @doc "`range_ids` rotated by the subscriber's `turn`: the order its read handed out should read them in."
  @spec rotate([term()], subscriber()) :: [term()]
  def rotate([], _subscriber), do: []

  def rotate(range_ids, %{turn: turn}) do
    {front, back} = Enum.split(range_ids, rem(turn, length(range_ids)))
    back ++ front
  end

  @doc "Every topic with a subscriber list (possibly empty)."
  @spec topics(t()) :: [term()]
  def topics(%__MODULE__{by_topic: by_topic}), do: Map.keys(by_topic)

  @doc """
  A wake on `topic` (a produce landed, or the reconcile tick): hands out a read to every subscriber of the
  topic that has credit and is not reading, and marks the ones reading so they are read for again when
  their read ends.
  """
  @spec wake(t(), term()) :: {[read()], t()}
  def wake(%__MODULE__{} = subs, topic), do: update_where(subs, topic, fn _ -> true end, &try_read/1)

  @doc """
  `pid` acked `count` records of `topic`: returns that much credit (never below zero, a client may ack
  more than it was sent), refreshes the member's `ranges` and `coordinator` when given (nil keeps the
  current ones), and hands out a read as a wake would.
  """
  @spec ack(t(), term(), pid(), non_neg_integer(), [term()] | nil, term()) :: {[read()], t()}
  def ack(%__MODULE__{} = subs, topic, pid, count, ranges, coordinator) do
    update_where(subs, topic, &(&1.pid == pid), fn sub ->
      try_read(%{
        sub
        | in_flight: max(sub.in_flight - count, 0),
          ranges: ranges || sub.ranges,
          coordinator: coordinator || sub.coordinator
      })
    end)
  end

  @doc """
  The read handed out for the subscription `ref` ended with `outcome`. A successful read moves the
  positions and adds what it pushed to `in_flight`; a failed one leaves both, so nothing is skipped, and
  the next wake or ack reads again. Either way the subscription can be read for again, at once when a
  wake arrived during the read. A `ref` no longer subscribed (it died or unsubscribed meanwhile), or one
  with no read out, is a no-op.
  """
  @spec read_done(t(), reference(), outcome()) :: {[read()], t()}
  def read_done(%__MODULE__{} = subs, ref, outcome) do
    case Map.fetch(subs.by_ref, ref) do
      :error ->
        {[], subs}

      {:ok, topic} ->
        update_one(subs, topic, ref, fn
          %{reading: true} = sub ->
            sub = %{sub | reading: false} |> apply_outcome(outcome)
            if sub.wake_pending, do: try_read(%{sub | wake_pending: false}), else: {[], sub}

          idle ->
            {[], idle}
        end)
    end
  end

  @doc "Removes the subscription `ref` (its process died). Returns it, or `nil` if it was not subscribed."
  @spec remove_ref(t(), reference()) :: {subscriber() | nil, t()}
  def remove_ref(%__MODULE__{} = subs, ref) do
    case Map.pop(subs.by_ref, ref) do
      {nil, _by_ref} ->
        {nil, subs}

      {topic, by_ref} ->
        %{order: order, by_ref: topic_by_ref} = Map.fetch!(subs.by_topic, topic)
        {removed, topic_by_ref} = Map.pop!(topic_by_ref, ref)
        topic_subs = %{order: List.delete(order, ref), by_ref: topic_by_ref}
        {removed, %{subs | by_topic: Map.put(subs.by_topic, topic, topic_subs), by_ref: by_ref}}
    end
  end

  @doc "Removes `pid`'s subscriptions to `topic` (an unsubscribe). Returns them."
  @spec remove_pid(t(), term(), pid()) :: {[subscriber()], t()}
  def remove_pid(%__MODULE__{} = subs, topic, pid) do
    removed = subs |> list(topic) |> Enum.filter(&(&1.pid == pid))

    Enum.reduce(removed, {removed, subs}, fn sub, {removed, subs} ->
      {_sub, subs} = remove_ref(subs, sub.ref)
      {removed, subs}
    end)
  end

  # How many records `sub` may be pushed now.
  defp budget(sub), do: min(sub.max, sub.window - sub.in_flight)

  # A read for `sub` if it has credit and none in progress; a pending wake if one is in progress.
  defp try_read(%{reading: true} = sub), do: {[], %{sub | wake_pending: true}}

  defp try_read(sub) do
    case budget(sub) do
      budget when budget > 0 -> {[{sub, budget}], %{sub | reading: true, turn: sub.turn + 1}}
      _no_credit -> {[], sub}
    end
  end

  defp apply_outcome(sub, {:ok, pushed, positions}),
    do: %{sub | positions: positions, in_flight: sub.in_flight + pushed}

  defp apply_outcome(sub, :error), do: sub

  # Applies `fun` (subscriber -> {reads, subscriber}) to the one subscriber `ref` of `topic`.
  defp update_one(subs, topic, ref, fun) do
    %{by_ref: topic_by_ref} = topic_subs = Map.fetch!(subs.by_topic, topic)
    {reads, sub} = fun.(Map.fetch!(topic_by_ref, ref))
    topic_subs = %{topic_subs | by_ref: Map.put(topic_by_ref, ref, sub)}
    {reads, %{subs | by_topic: Map.put(subs.by_topic, topic, topic_subs)}}
  end

  # Applies `fun` to the subscribers of `topic` matching `match?`, in push order, and gathers the reads
  # they hand out in that order.
  defp update_where(subs, topic, match?, fun) do
    case Map.fetch(subs.by_topic, topic) do
      :error ->
        {[], subs}

      {:ok, %{order: order, by_ref: topic_by_ref} = topic_subs} ->
        {reads, topic_by_ref} =
          Enum.reduce(order, {[], topic_by_ref}, fn ref, {reads, topic_by_ref} ->
            sub = Map.fetch!(topic_by_ref, ref)

            if match?.(sub) do
              {sub_reads, sub} = fun.(sub)
              {Enum.reverse(sub_reads, reads), Map.put(topic_by_ref, ref, sub)}
            else
              {reads, topic_by_ref}
            end
          end)

        topic_subs = %{topic_subs | by_ref: topic_by_ref}
        {Enum.reverse(reads), %{subs | by_topic: Map.put(subs.by_topic, topic, topic_subs)}}
    end
  end
end
