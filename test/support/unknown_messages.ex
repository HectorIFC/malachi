defmodule Malachi.Test.UnknownMessages do
  @moduledoc """
  Unknown messages for the catch-all tests (`Malachi.UnexpectedMessage`), and the suite-wide guard that
  keeps those catch-alls from hiding a bug.

  A catch-all turns a mistyped pattern in a later clause into a dropped message instead of a crash. The
  guard (`start_guard/0`, from `test_helper.exs`) is what keeps that loud: it records every
  `[:malachi, :process, :unexpected_message]` event on this node, and `report_guard/0` fails the run if
  any came from a process no test declared (`expect_from/1`) as one it sends unknown messages to on
  purpose.
  """

  import ExUnit.Assertions
  import ExUnit.CaptureLog

  @owner __MODULE__.Owner
  @expected __MODULE__.Expected
  @violations __MODULE__.Violations
  @event [:malachi, :process, :unexpected_message]

  @tag :malachi_test_unknown
  # Record data that must never reach a log line: the assertions look for fragments of it.
  @secret "s3cret-sentinel-payload"
  @secret_key "user-7"

  @doc "The tag every unknown message built here carries."
  def tag, do: @tag

  @doc "The value planted in `message/0`, which a log line must never contain."
  def secret, do: @secret

  @doc "An unknown message shaped like a record push: `{tag, ref, [record]}`, so its shape is `{tag, 3}`."
  def message, do: {@tag, make_ref(), [%{offset: 7, key: @secret_key, value: String.duplicate(@secret, 50)}]}

  @doc """
  Declares that the test sends unknown messages to `pid` on purpose, so the guard does not count the
  drops it reports.
  """
  def expect_from(pid) when is_pid(pid) do
    true = :ets.insert(@expected, {pid})
    :ok
  end

  @doc """
  Sends `server` an unknown cast, an unknown info message and an unknown call, and asserts that it
  survived all three as a server labelled `label`: the same process is alive and registered, `probe`
  (a function that exercises a real request) still works, each kind was counted with its label, the call
  was answered `{:error, :unknown_call}`, and the log names the shape without any of the payload.

  Returns what `probe` returned.
  """
  def assert_survives_unknown(server, label, probe) when is_function(probe, 0) do
    pid = GenServer.whereis(server)
    assert is_pid(pid), "#{inspect(server)} is not running"
    expect_from(pid)

    handler_id = {__MODULE__, make_ref()}
    :ok = :telemetry.attach(handler_id, @event, &__MODULE__.forward_event/4, %{pid: pid, test: self()})

    try do
      log =
        capture_log(fn ->
          GenServer.cast(server, message())
          send(pid, message())
          # Same sender, so the mailbox keeps the order: this answer comes after the cast and the info
          # message were handled, or the server died on one of them.
          assert GenServer.call(server, message()) == {:error, :unknown_call}
        end)

      for kind <- [:cast, :info, :call] do
        assert_receive {:unexpected_event, ^pid, ^label, ^kind, {@tag, 3}, 1}, 1_000
      end

      assert Process.alive?(pid)
      assert GenServer.whereis(server) == pid, "the server was restarted rather than kept alive"

      for fragment <- ["unexpected cast", "unexpected message", "unexpected call"] do
        assert log =~ fragment, "no #{inspect(fragment)} line in #{inspect(log)}"
      end

      assert log =~ inspect(@tag), "the log line should name the shape"
      refute log =~ @secret
      refute log =~ @secret_key

      probe.()
    after
      :telemetry.detach(handler_id)
    end
  end

  @doc """
  Runs `send_fun`, which sends `server` messages it has no clause for, and returns the drops the server
  reported for them, in order, as `{label, kind, shape}`, together with the log captured meanwhile.
  """
  def drops(server, send_fun) when is_function(send_fun, 0) do
    pid = GenServer.whereis(server)
    expect_from(pid)

    handler_id = {__MODULE__, make_ref()}
    :ok = :telemetry.attach(handler_id, @event, &__MODULE__.forward_event/4, %{pid: pid, test: self()})

    try do
      log =
        capture_log(fn ->
          send_fun.()
          # A system message is handled in mailbox order too, so once this answers, everything sent
          # before it has been handled.
          _ = :sys.get_state(pid)
        end)

      {collect_drops(pid, []), log}
    after
      :telemetry.detach(handler_id)
    end
  end

  defp collect_drops(pid, acc) do
    receive do
      {:unexpected_event, ^pid, label, kind, shape, 1} -> collect_drops(pid, [{label, kind, shape} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  @doc false
  def forward_event(_event, measurements, %{pid: pid} = metadata, %{pid: pid, test: test}) do
    send(test, {:unexpected_event, pid, metadata.server, metadata.kind, metadata.shape, measurements.count})
  end

  def forward_event(_event, _measurements, _metadata, _config), do: :ok

  # --- the suite-wide guard ---

  @doc "Starts the guard: the tables it keeps and the telemetry handler that fills them."
  def start_guard do
    case Process.whereis(@owner) do
      nil ->
        parent = self()
        owner = spawn(fn -> own_tables(parent) end)

        receive do
          {:tables_ready, ^owner} -> :ok
        end

      _owner ->
        :ok
    end

    _ = :telemetry.detach({__MODULE__, :guard})
    :ok = :telemetry.attach({__MODULE__, :guard}, @event, &__MODULE__.guard_event/4, nil)
  end

  defp own_tables(parent) do
    Process.register(self(), @owner)
    :ets.new(@expected, [:named_table, :public, :set])
    :ets.new(@violations, [:named_table, :public, :bag])
    send(parent, {:tables_ready, self()})
    Process.sleep(:infinity)
  end

  @doc false
  def guard_event(_event, _measurements, %{pid: pid} = metadata, _config) do
    # The event is emitted from the server's own loop, and a test declares a server before sending it
    # anything, so a declared pid is always in the table by the time its drops are reported.
    if :ets.member(@expected, pid) do
      :ok
    else
      true = :ets.insert(@violations, {metadata.server, metadata.kind, metadata.shape, pid})
      :ok
    end
  end

  @doc "The drops no test asked for, as `{server, kind, shape, pid}`."
  def violations, do: :ets.tab2list(@violations)

  @doc """
  Called once the suite finished: `:ok` when every drop was expected, otherwise prints them and returns
  `{:error, violations}` for the caller to fail the run with.
  """
  def report_guard do
    case violations() do
      [] ->
        :ok

      violations ->
        IO.puts(:stderr, """

        A server dropped messages no test sent on purpose. A catch-all hides what used to be a crash, so
        this is the failure it would have been: a clause pattern that does not match what is sent.

        #{Enum.map_join(violations, "\n", &inspect/1)}
        """)

        {:error, violations}
    end
  end
end
