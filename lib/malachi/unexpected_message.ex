defmodule Malachi.UnexpectedMessage do
  @moduledoc """
  What a long-lived server does with a message it has no clause for: it logs the message's shape, counts
  it, and keeps running.

  The servers on the cluster path (`Malachi.Cluster.ReplicationServer`, `Malachi.Cluster.MembershipServer`,
  `Malachi.BrokerServer`, `Malachi.Cluster.Scrubber`, `Malachi.Cluster.RetentionCoordinator`,
  `Malachi.Cluster.HealCoordinator` and `Malachi.Retention.SkipReporter`) each end their
  `handle_cast/2`, `handle_info/2` and `handle_call/3`
  with a catch-all that calls `drop/4`. Without it, a message shape a server does not know raises
  `FunctionClauseError`, and during a rolling upgrade a newer node sends exactly that: the first upgraded
  primary that pushed a new replication message would take down the whole data plane of every older
  follower, and keep taking it down after every restart.

  ## What each kind gets

    * a cast or an info message is dropped;
    * a call is answered `{:error, :unknown_call}` (see `unknown_call_reply/0`), so a newer caller gets
      an answer it can fall back on instead of waiting out its timeout. That atom is part of the
      cross-version contract: callers match on it, so it does not change.

  The logged line carries the message's SHAPE and never its data: strings, charlists and numbers are
  elided, so what is left is the tags, the atoms and the nesting that identify the sender.

  Every drop emits `[:malachi, :process, :unexpected_message]` (see `Malachi.Telemetry`), which the
  metrics reporter folds into `malachi_unexpected_messages_total{server, kind}`. Only the first
  occurrence of each `{kind, shape}` per server process is logged, because a newer primary can push
  thousands of batches a second at an older follower, and a warning per message would put the logger in
  synchronous mode and slow down the very process this protects. The count is never deduplicated.

  ## What this does not do

    * **A drop does not make a mixed-version cluster work.** An older follower that drops a new
      replication message sends no ack, so the primary waits for its replication timeout; an older
      member that drops a new SWIM ping sends no ack either, so the newer member suspects it and can
      declare it dead. Not sending a new shape until every member understands it is the capability
      gate's job, not this module's.
    * **Only the outer shape is covered.** A message whose tag and arity a server knows, carrying an
      inner term it does not (a new record struct inside `:replica_append`, a new update tuple inside a
      SWIM `:ping`), still matches the old clause and can still crash inside it. A change to what a
      message carries therefore needs a new tag, not a new inner shape under an old one.
    * **It hides typos.** A clause added later with a mistyped pattern now drops its message instead of
      crashing. The counter is the signal: it must stay at zero on a cluster where every node runs the
      same build, and the test suite fails on any drop it did not ask for.
  """

  require Logger

  alias Malachi.I18n
  alias Malachi.Telemetry

  @servers [
    :replication,
    :membership,
    :broker,
    :scrubber,
    :retention,
    :heal,
    :skip_reporter,
    :rebalance
  ]
  @kinds [:cast, :info, :call]

  # How many distinct {kind, shape} pairs one server process logs. Tags are atoms, so the set is bounded
  # anyway, but a sender cycling through many of them would still turn "first occurrence" into a flood.
  @max_logged_shapes 32

  @typedoc "The label of a server that drops unexpected messages (anything else is counted as `other`)."
  @type server ::
          :replication
          | :membership
          | :broker
          | :scrubber
          | :retention
          | :heal
          | :skip_reporter
          | :rebalance

  @typedoc "How the message arrived."
  @type kind :: :cast | :info | :call

  @typedoc """
  A message's shape: `{tag, arity}` for a tuple led by an atom, `{:tuple, arity}` for any other tuple,
  the atom itself for an atom, and the type otherwise. It identifies the sender without carrying data.
  """
  @type shape :: {atom(), non_neg_integer()} | atom()

  @typedoc "The `{kind, shape}` pairs a server process has already logged (kept in its own state)."
  @type seen :: MapSet.t()

  @doc "The server labels exported as `malachi_unexpected_messages_total` series."
  @spec servers() :: [server()]
  def servers, do: @servers

  @doc "The kinds exported as `malachi_unexpected_messages_total` series."
  @spec kinds() :: [kind()]
  def kinds, do: @kinds

  @doc "The reply a server gives to a call it has no clause for."
  @spec unknown_call_reply() :: {:error, :unknown_call}
  def unknown_call_reply, do: {:error, :unknown_call}

  @doc """
  Counts `message`, which `server` received as `kind` and has no clause for, logs its shape if this
  process has not logged that `{kind, shape}` before, and returns the updated `seen` set to keep in the
  server's state.
  """
  @spec drop(seen(), atom(), kind(), term()) :: seen()
  def drop(seen, server, kind, message) when kind in @kinds do
    shape = shape(message)
    :ok = Telemetry.unexpected_message(server, kind, shape)
    key = {kind, shape}
    logged = MapSet.size(seen)

    cond do
      MapSet.member?(seen, key) ->
        seen

      logged < @max_logged_shapes ->
        log(server, kind, message)
        MapSet.put(seen, key)

      # The one line that says the rest will be counted but not logged, so a quiet log after a burst of
      # warnings is not read as the problem having stopped. The marker takes the set past the limit,
      # which is what keeps this line from being written twice.
      logged == @max_logged_shapes ->
        Logger.warning(I18n.t(:unexpected_messages_log_limit, server: server, limit: @max_logged_shapes))
        MapSet.put(seen, :log_limit_reached)

      true ->
        seen
    end
  end

  @doc "The shape of `message` (see `t:shape/0`)."
  @spec shape(term()) :: shape()
  def shape(message) when is_tuple(message) and tuple_size(message) > 0 and is_atom(elem(message, 0)),
    do: {elem(message, 0), tuple_size(message)}

  def shape(message) when is_tuple(message), do: {:tuple, tuple_size(message)}
  def shape(message) when is_atom(message), do: message
  def shape(message) when is_list(message), do: :list
  def shape(message) when is_map(message), do: :map
  def shape(message) when is_binary(message), do: :binary
  def shape(_message), do: :other

  # `printable_limit: 0` is the point of these options, not `limit`: it replaces the CONTENT of every
  # string in the term with an ellipsis while leaving the term's shape intact, so this prints something
  # like `{:replica_append_v2, {...}, 0, ...}`. Whatever reaches a catch-all is by definition not a
  # message we planned for, so it may carry record values, and a log line is the one place user data must
  # not end up in by accident. Truncating the text is not enough for that: a bounded prefix of a payload
  # is still a payload. The shape is what makes the line worth having, since it is what identifies the
  # sender.
  #
  # Lists and numbers need `redact/2` on top of that. `printable_limit: 0` also makes EVERY list pass the
  # "is this a printable charlist" test (zero elements checked), so inspecting a message that holds a list
  # of maps raised ArgumentError from `List.to_string/1`, inside the very clause meant to keep the server
  # alive. And it elides text only: a number reached the line whole, which for an account number or an id
  # is the entire payload rather than a bounded prefix of one.
  #
  # A reply that arrives after a `GenServer.call/3` timed out does not reach these clauses: on OTP 28 the
  # runtime deactivates the call's alias on timeout and drops the late reply, so an ordinary slow call
  # does not show up here.
  defp log(server, kind, message) do
    printed = inspect(message, limit: 3, printable_limit: 0, inspect_fun: &redact/2)
    Logger.warning(I18n.t(log_key(kind), server: server, message: printed))
  end

  # Called for every term `inspect/2` visits, nested ones included.
  #
  # A number is elided as `_`, the same wildcard a pattern would use, because a number can be the payload
  # itself (an id, an account number) and, unlike a string, there is no prefix of it to bound. Atoms are
  # NOT elided: they come from the sending code rather than from user input, and they are what tells two
  # messages with the same tag apart. An atom built from user input would be an atom-table exhaustion
  # problem long before it was a logging one, which `Malachi.AtomMonitor` watches for.
  #
  # Any other list is printed as a list, never tested for being a charlist; a list of integers may be
  # text, so it is elided like a string (printed as a list, its first elements would show).
  defp redact(number, _opts) when is_number(number), do: Inspect.Algebra.string("_")

  defp redact(list, opts) when is_list(list) do
    if integer_list?(list),
      do: Inspect.Algebra.string(~s(~c"...")),
      else: Inspect.inspect(list, %{opts | charlists: :as_lists})
  end

  defp redact(term, opts), do: Inspect.inspect(term, opts)

  # Safe on improper lists, which a stray message can carry as well as anything else.
  defp integer_list?([head | tail]) when is_integer(head), do: tail == [] or integer_list?(tail)
  defp integer_list?(_other), do: false

  defp log_key(:cast), do: :unexpected_cast
  defp log_key(:info), do: :unexpected_info
  defp log_key(:call), do: :unexpected_call
end
