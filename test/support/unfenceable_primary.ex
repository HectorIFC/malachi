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
  """

  use GenServer

  alias Malachi.Cluster.ReplicationServer

  @doc "Starts the proxy. `:directory` is where the inner replication server stores segments."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_server_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @impl true
  def init(opts) do
    directory =
      Keyword.get_lazy(opts, :directory, fn ->
        Path.join(System.tmp_dir!(), "malachi_unfenceable_#{System.unique_integer([:positive])}")
      end)

    {:ok, inner} = ReplicationServer.start_link(directory: directory)
    {:ok, %{inner: inner, directory: directory}}
  end

  @impl true
  def handle_call({:seal, _segment_id, _base_offset}, _from, state) do
    # Deliberately no reply: the caller's fence times out, which is the whole point of this double.
    {:noreply, state}
  end

  def handle_call(request, _from, state) do
    {:reply, GenServer.call(state.inner, rewrite(request, state)), state}
  end

  @impl true
  def handle_cast(request, state) do
    GenServer.cast(state.inner, rewrite(request, state))
    {:noreply, state}
  end

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
