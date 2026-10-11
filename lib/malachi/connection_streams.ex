defmodule Malachi.ConnectionStreams do
  @moduledoc """
  Every stream one connection holds: producer streams (`Malachi.ProducerStreams`) and consume streams
  (`Malachi.ConsumeStreams`), under one id space, since `close_stream` names either kind by its id. A
  client holds one connection per broker and opens on it the streams of every range that broker serves,
  of both kinds, as NorthGuard's clients do (meetup transcript, 542 to 569).

  The connection reads its socket only while every producer stream has room in its window
  (`Malachi.ProducerStreams.room?/1`) and fewer than 64 jobs run or wait beside it; a consume stream only
  pushes, so it never holds the socket back. It is never idle while a consume stream is open, an append is
  in flight or a job runs beside it (`run/3`: a `fetch_range` waiting for records, a group key waiting for
  its coordinator).
  """

  alias Malachi.ConsumeStreams
  alias Malachi.ProducerStreams

  # The most jobs one connection may have running or waiting: past it the connection stops reading its
  # socket until one ends, as a producer stream's closed window stops it.
  @max_jobs 64

  defstruct producers: nil,
            consumers: nil,
            next_id: 1,
            jobs: %{},
            in_order: nil,
            in_order_pid: nil,
            waiting: nil,
            client_ip: nil

  @type job :: {(-> binary()), binary(), :run | :drop}
  @type t :: %__MODULE__{
          producers: ProducerStreams.t(),
          consumers: ConsumeStreams.t(),
          next_id: pos_integer(),
          jobs: %{reference() => binary()},
          in_order: reference() | nil,
          in_order_pid: pid() | nil,
          client_ip: String.t() | nil,
          waiting: :queue.queue(job())
        }

  @doc "No stream open yet, on a connection from `client_ip`."
  @spec new(String.t() | nil) :: t()
  def new(client_ip \\ nil),
    do: %__MODULE__{
      producers: ProducerStreams.new(),
      consumers: ConsumeStreams.new(),
      waiting: :queue.new(),
      client_ip: client_ip
    }

  @doc "This connection's open producer streams, each with the address of the client that holds it."
  @spec describe_producers(t()) :: [map()]
  def describe_producers(%__MODULE__{} = state),
    do: for(stream <- ProducerStreams.describe(state.producers), do: Map.put(stream, :client_ip, state.client_ip))

  @doc """
  Runs `job` beside the connection, in a process of its own, for a request whose answer waits on another
  process (a `fetch_range` waiting for records, a group key asking its coordinator) and would otherwise hold
  the connection's streams back. `job` returns the frame the client gets, which `handle_message/2` hands
  back once it is done; a job that crashes is answered with `fallback`.
  """
  @spec run(t(), (-> binary()), binary()) :: t()
  def run(%__MODULE__{} = state, job, fallback), do: elem(start(state, job, fallback), 0)

  @doc """
  The connection is closing: the in-order jobs still waiting (a `leave_group` behind a commit) are run all
  the same, in order, by a process of their own once the running one ends, since the client sent them
  before it went, except those queued with `on_close: :drop`. Nobody is left to read their answers.
  """
  @spec hand_off(t()) :: :ok
  def hand_off(%__MODULE__{in_order: nil}), do: :ok

  def hand_off(%__MODULE__{in_order_pid: running} = state) do
    case :queue.to_list(state.waiting) do
      [] ->
        :ok

      waiting ->
        spawn(fn ->
          ref = Process.monitor(running)
          receive(do: ({:DOWN, ^ref, :process, _pid, _reason} -> :ok))
          for {job, _fallback, :run} <- waiting, do: run_detached(job)
        end)

        :ok
    end
  end

  # One waiting job after the connection closed: a job that raises or exits does not keep the next from
  # running.
  defp run_detached(job) do
    job.()
  catch
    _kind, _reason -> :ok
  end

  @doc """
  `run/3` for jobs whose effects must land in the order the client sent them (a member's commits, each of
  which may move the same checkpoint): each starts once the one before it ended. `on_close: :drop` (the
  default is `:run`) leaves the job out of `hand_off/1` if it is still waiting when the connection closes.
  """
  @spec run_in_order(t(), (-> binary()), binary(), keyword()) :: t()
  def run_in_order(state, job, fallback, opts \\ [])

  def run_in_order(%__MODULE__{in_order: nil} = state, job, fallback, _opts) do
    {state, ref, pid} = start(state, job, fallback)
    %{state | in_order: ref, in_order_pid: pid}
  end

  def run_in_order(%__MODULE__{} = state, job, fallback, opts),
    do: %{state | waiting: :queue.in({job, fallback, Keyword.get(opts, :on_close, :run)}, state.waiting)}

  defp start(state, job, fallback) do
    {pid, ref} = spawn_monitor(fn -> exit({:shutdown, {:frame, job.()}}) end)
    {%{state | jobs: Map.put(state.jobs, ref, fallback)}, ref, pid}
  end

  # The in-order job that ended was `ref`: the next one waiting starts.
  defp next_in_order(%__MODULE__{in_order: ref} = state, ref) do
    case :queue.out(state.waiting) do
      {{:value, {job, fallback, _on_close}}, waiting} ->
        run_in_order(%{state | in_order: nil, in_order_pid: nil, waiting: waiting}, job, fallback)

      {:empty, _waiting} ->
        %{state | in_order: nil, in_order_pid: nil}
    end
  end

  defp next_in_order(state, _ref), do: state

  @doc "Records a producer stream the broker opened and returns the id it is given."
  @spec open_producer(t(), map()) :: {t(), pos_integer()}
  def open_producer(%__MODULE__{} = state, opened) do
    id = state.next_id
    {%{state | producers: ProducerStreams.open(state.producers, Map.put(opened, :id, id)), next_id: id + 1}, id}
  end

  @doc "Records a consume stream the broker opened and returns the id it is given."
  @spec open_consumer(t(), map()) :: {t(), pos_integer()}
  def open_consumer(%__MODULE__{} = state, opened) do
    id = state.next_id
    {%{state | consumers: ConsumeStreams.open(state.consumers, Map.put(opened, :id, id)), next_id: id + 1}, id}
  end

  @doc "Closes the stream `stream_id` names, of either kind. `:unknown` for one this connection does not hold."
  @spec close(t(), pos_integer()) :: {:ok, t()} | :unknown
  def close(%__MODULE__{} = state, stream_id) do
    if ConsumeStreams.holds?(state.consumers, stream_id) do
      {:ok, consumers} = ConsumeStreams.close(state.consumers, stream_id)
      {:ok, %{state | consumers: consumers}}
    else
      with {:ok, producers} <- ProducerStreams.close(state.producers, stream_id),
           do: {:ok, %{state | producers: producers}}
    end
  end

  @doc """
  Folds a message the connection received into whatever it is about: the end of a job (`run/3`), with the
  frame it answers, or a message for one kind of stream. `:no` for one that belongs to none. A broker that
  went down is told to both kinds of stream, since each watches the brokers its own streams were opened on.
  """
  @spec handle_message(t(), term()) :: {t(), [binary()]} | :no
  def handle_message(%__MODULE__{jobs: jobs} = state, {:DOWN, ref, :process, _pid, reason})
      when is_map_key(jobs, ref) do
    {fallback, jobs} = Map.pop!(jobs, ref)

    frame =
      case reason do
        {:shutdown, {:frame, frame}} -> frame
        _crashed -> fallback
      end

    {next_in_order(%{state | jobs: jobs}, ref), [frame]}
  end

  def handle_message(%__MODULE__{} = state, message), do: streams_message(state, message)

  defp streams_message(state, {:DOWN, _ref, :process, _pid, _reason} = down) do
    {producers, from_producers} = taken(ProducerStreams.handle_message(state.producers, down), state.producers)
    {consumers, from_consumers} = taken(ConsumeStreams.handle_message(state.consumers, down), state.consumers)

    if from_producers == :no and from_consumers == :no,
      do: :no,
      else: {%{state | producers: producers, consumers: consumers}, frames(from_producers) ++ frames(from_consumers)}
  end

  defp streams_message(state, message) do
    case ConsumeStreams.handle_message(state.consumers, message) do
      {consumers, frames} ->
        {%{state | consumers: consumers}, frames}

      :no ->
        case ProducerStreams.handle_message(state.producers, message) do
          {producers, frames} -> {%{state | producers: producers}, frames}
          :no -> :no
        end
    end
  end

  defp taken({streams, frames}, _unchanged), do: {streams, frames}
  defp taken(:no, unchanged), do: {unchanged, :no}

  defp frames(:no), do: []
  defp frames(frames), do: frames

  @doc """
  Whether every producer stream has room for another append and the connection has room for another job:
  the connection reads its socket only then.
  """
  @spec room?(t()) :: boolean()
  def room?(%__MODULE__{} = state), do: ProducerStreams.room?(state.producers) and job_room?(state)

  @doc """
  Whether the connection may take another job: fewer than `max_jobs/0` run or wait. Asked before each frame
  the connection reads, not only before each read, so one delivery holding many requests stops at the cap
  and the rest wait in the buffer until a job ends.
  """
  @spec job_room?(t()) :: boolean()
  def job_room?(%__MODULE__{} = state), do: map_size(state.jobs) + :queue.len(state.waiting) < @max_jobs

  @doc "The most jobs a connection may have running or waiting before it stops reading its socket."
  @spec max_jobs() :: pos_integer()
  def max_jobs, do: @max_jobs

  @doc "Whether the connection has work outstanding: an append in flight, a consume stream open, or a job running."
  @spec busy?(t()) :: boolean()
  def busy?(%__MODULE__{} = state),
    do: ProducerStreams.busy?(state.producers) or ConsumeStreams.open?(state.consumers) or map_size(state.jobs) > 0
end
