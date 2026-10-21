defmodule Malachi.Test.CallTrace do
  @moduledoc """
  Records the calls a piece of code makes to chosen functions, in order, for tests that must prove a
  step happened (and after which other step), not only that the end state looks right. A directory
  fsync is the case in point: it leaves nothing on disk to inspect.

  Call tracing is set per function for the whole VM, so a test module using this must be
  `async: false`.
  """

  @doc """
  Runs `fun` in the calling process with calls to each `{module, function, arity}` in `mfas` traced,
  and answers the calls as `{module, function, args}` in the order they happened. Every module is
  loaded first: a pattern set on a module that is not loaded yet matches nothing and records nothing.
  """
  @spec calls([mfa()], (-> any())) :: [{module(), atom(), [term()]}]
  def calls(mfas, fun), do: calls(mfas, [], fun)

  @doc """
  As `calls/2`, and also traces the processes in `pids:`, such as a server the test started. A process
  that is not the caller can make calls the test needs to see, and they are answered in the same order.
  """
  @spec calls([mfa()], [pids: [pid()]], (-> any())) :: [{module(), atom(), [term()]}]
  def calls(mfas, opts, fun) do
    for {module, _function, _arity} <- mfas, do: Code.ensure_loaded!(module)
    # A separate collector: a process set as its own tracer receives nothing on the OTP this suite
    # runs on.
    collector = spawn_link(fn -> collect([]) end)
    tracees = [self() | Keyword.get(opts, :pids, [])]
    for mfa <- mfas, do: 1 = :erlang.trace_pattern(mfa, true, [:local])
    for tracee <- tracees, do: :erlang.trace(tracee, true, [:call, {:tracer, collector}])

    try do
      fun.()
    after
      for tracee <- tracees, Process.alive?(tracee), do: :erlang.trace(tracee, false, [:call])
      for mfa <- mfas, do: :erlang.trace_pattern(mfa, false, [:local])
    end

    # Trace messages are delivered asynchronously; wait until every one sent so far has arrived, from
    # each traced process.
    for tracee <- tracees do
      ref = :erlang.trace_delivered(tracee)

      receive do
        {:trace_delivered, _pid, ^ref} -> :ok
      end
    end

    send(collector, {:done, self()})

    receive do
      {:calls, calls} -> calls
    end
  end

  defp collect(acc) do
    receive do
      {:trace, _pid, :call, {module, function, args}} -> collect([{module, function, args} | acc])
      {:done, caller} -> send(caller, {:calls, Enum.reverse(acc)})
    end
  end
end
