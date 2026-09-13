defmodule Malachi.Test.UnfenceablePrimary do
  @moduledoc """
  A `Malachi.Cluster.ReplicationServer` proxy that serves every request normally except the write
  fence, which it never answers.

  It exists for one thing the design turns on: what happens when a fence FAILS. The safety property is
  that a failed fence degrades to "not sealed yet", never to "sealed at the wrong place", and that
  cannot be asserted with a dead primary, because a dead primary fails the produce too and the two
  outcomes become indistinguishable.

  Everything else is forwarded to a real replication server started underneath, with the replica set
  rewritten from this proxy's reference to the inner server's, so the inner server still recognizes
  itself as the primary.

  `fence: :hold` makes the asynchronous fence (`seal_async/5`) take effect on the inner server while its
  answer is held back: the store refuses writes past the fence and the caller does not hear so until
  `release_fences/1`, after which held and later answers pass straight through. Never released, it is a
  fence applied and never answered. The default, `:swallow`, never applies the fence at all.
  """

  use GenServer

  alias Malachi.Cluster.ReplicationServer

  @doc "Starts the proxy. `:directory` is where the inner replication server stores segments."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_server_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @doc "Delivers every fence answer held under `fence: :hold`, and lets later ones through unheld."
  @spec release_fences(GenServer.server()) :: :ok
  def release_fences(proxy), do: GenServer.call(proxy, :release_fences)

  @impl true
  def init(opts) do
    directory =
      Keyword.get_lazy(opts, :directory, fn ->
        Path.join(System.tmp_dir!(), "malachi_unfenceable_#{System.unique_integer([:positive])}")
      end)

    {:ok, inner} = ReplicationServer.start_link(directory: directory)
    {:ok, %{inner: inner, directory: directory, fence: Keyword.get(opts, :fence, :swallow), held: []}}
  end

  @impl true
  def handle_call({:seal, _segment_id, _base_offset}, _from, state) do
    # Deliberately no reply: the caller's fence times out, which is the whole point of this double.
    {:noreply, state}
  end

  def handle_call(:release_fences, _from, state) do
    state.held |> Enum.reverse() |> Enum.each(&deliver/1)
    {:reply, :ok, %{state | fence: :released, held: []}}
  end

  def handle_call(request, _from, state) do
    {:reply, GenServer.call(state.inner, rewrite(request, state)), state}
  end

  @impl true
  def handle_cast({:seal_async, segment_id, base_offset, notify}, %{fence: mode} = state)
      when mode in [:hold, :released] do
    # Applied on the inner server, with the answer routed through this proxy (`handle_info/2`).
    GenServer.cast(state.inner, {:seal_async, segment_id, base_offset, {self(), {:proxied, notify}}})
    {:noreply, state}
  end

  def handle_cast({:seal_async, _segment_id, _base_offset, _notify}, state) do
    # The asynchronous fence is swallowed too: forwarding it would fence the inner server, and the double
    # would stop being a primary that never answers a fence.
    {:noreply, state}
  end

  def handle_cast(request, state) do
    GenServer.cast(state.inner, rewrite(request, state))
    {:noreply, state}
  end

  @impl true
  def handle_info({:seal_result, {:proxied, notify}, reply}, %{fence: :released} = state) do
    deliver({notify, reply})
    {:noreply, state}
  end

  def handle_info({:seal_result, {:proxied, notify}, reply}, state) do
    {:noreply, %{state | held: [{notify, reply} | state.held]}}
  end

  defp deliver({{pid, tag}, reply}), do: send(pid, {:seal_result, tag, reply})

  # The broker builds replica sets out of THIS process's reference, and the inner server accepts a
  # write only when it is the set's head, so the head is swapped for the inner server on the way in.
  defp rewrite({:replicate, segment_id, replica_set, base_offset, records, ctx}, state),
    do: {:replicate, segment_id, inner_set(replica_set, state), base_offset, records, ctx}

  defp rewrite({:replicate_async, segment_id, replica_set, base_offset, records, notify, ctx}, state),
    do: {:replicate_async, segment_id, inner_set(replica_set, state), base_offset, records, notify, ctx}

  defp rewrite({:append, segment_id, replica_set, base_offset, records}, state),
    do: {:append, segment_id, inner_set(replica_set, state), base_offset, records}

  defp rewrite(request, _state), do: request

  defp inner_set(replica_set, state) do
    Enum.map(replica_set, fn ref -> if mine?(ref), do: state.inner, else: ref end)
  end

  defp mine?(ref) when is_pid(ref), do: ref == self()
  defp mine?({name, node}) when is_atom(name), do: node == node() and Process.whereis(name) == self()
  defp mine?(name) when is_atom(name), do: Process.whereis(name) == self()
  defp mine?(_ref), do: false
end
