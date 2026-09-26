defmodule Malachi.Test.SilentRaMember do
  @moduledoc """
  A process registered under an `ra` server's name that answers its calls late, wrongly, or not at all.

  Every `ra` client call ends in `gen_statem:call({Name, Node}, Msg, Timeout)`
  (`ra_server_proc:gen_statem_safe_call/3`), so a plain process registered under `Name` receives the
  `$gen_call` and decides what the caller experiences. `start_link/1` never answers, which makes the
  caller wait out its whole timeout before `ra` reports `{timeout, ServerId}`: the production shape a
  peer that drops packets instead of refusing them produces, and the one #178 is about.

  The alternatives do not reproduce it. A server id at a node that does not exist fails in under 20ms
  (nothing to resolve, the connection is refused at once), and `Malachi.Test.StuckRaMember` produces a
  member that stopped *applying* entries but still answers.

  `start_redirecting/3` covers the other shape a bound has to survive: `ra` follows a `{:redirect,
  leader}` reply by calling the named leader with a **fresh full timeout**
  (`ra_server_proc:statem_call/3`), so two members that redirect to each other cost unbounded time
  without any single call ever timing out.
  """
  use GenServer

  @doc """
  Registers a member under `name` on this node that never answers a call, linked to the caller.

  `on_exit` is the caller's job, as with every other support process here; `stop/1` unregisters it.
  """
  @spec start_link(atom()) :: GenServer.on_start()
  def start_link(name), do: GenServer.start_link(__MODULE__, %{mode: :silent}, name: name)

  @doc """
  Registers a member under `name` that answers every call with `{:redirect, target}` after `delay_ms`.

  Point two of them at each other and `ra` chases the leader between them forever, each hop well
  inside its own timeout. Only a bound on the whole read ends it.
  """
  @spec start_redirecting(atom(), {atom(), node()}, non_neg_integer()) :: GenServer.on_start()
  def start_redirecting(name, target, delay_ms) do
    GenServer.start_link(__MODULE__, %{mode: :redirecting, target: target, delay: delay_ms}, name: name)
  end

  @doc """
  Makes the member answer `reply` from now on, immediately.

  `ra` reads anything it does not understand as a failed call, which is all a test needs: the point of
  releasing is that reads stop costing their timeout, not what they come back with.
  """
  @spec release(atom() | pid(), term()) :: :ok
  def release(server, reply \\ {:error, :released}), do: GenServer.call(server, {:release, reply})

  @doc "Stops the member, which unregisters the name. Safe to call on one that is already gone."
  @spec stop(atom() | pid()) :: :ok
  def stop(server) do
    case GenServer.whereis(server) do
      nil -> :ok
      pid -> GenServer.stop(pid, :normal)
    end
  catch
    :exit, _reason -> :ok
  end

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  # The release is the one call this process always answers, so it is matched before the modes.
  def handle_call({:release, reply}, _from, state) do
    {:reply, :ok, Map.merge(state, %{mode: :released, reply: reply})}
  end

  def handle_call(_message, _from, %{mode: :released} = state), do: {:reply, state.reply, state}

  def handle_call(_message, from, %{mode: :redirecting} = state) do
    Process.send_after(self(), {:redirect, from}, state.delay)
    {:noreply, state}
  end

  # `:silent`: no reply, ever. The caller waits out its own timeout.
  def handle_call(_message, _from, state), do: {:noreply, state}

  @impl true
  def handle_info({:redirect, from}, state) do
    GenServer.reply(from, {:redirect, state.target})
    {:noreply, state}
  end
end
