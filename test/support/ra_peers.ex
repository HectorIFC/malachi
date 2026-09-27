defmodule Malachi.Test.RaPeers do
  @moduledoc """
  Peer BEAM nodes running `ra`, which can be stopped and started again on the same name and data
  directory: what a node restart, a rolling upgrade or a rollback looks like to a Raft group.

  A peer's machine version pin (`Malachi.Cluster.MachineVersion`) is set before `ra` starts, which is
  how these tests give one member different "code" from the others while every node loads the same
  code path. The test node must already be distributed (`Malachi.Test.Distribution.ensure_started/0`,
  which `ensure_distribution/0` calls).
  """

  alias Malachi.Test.Distribution

  @system :default

  @type t :: %{peer: pid(), node: node(), name: atom(), data_dir: charlist()}

  @doc "Makes the test node distributed and starts `ra` on it."
  @spec ensure_distribution() :: :ok
  def ensure_distribution do
    :ok = Distribution.ensure_started()
    {:ok, _apps} = Application.ensure_all_started(:ra)
    :ok
  end

  @doc "Starts a fresh peer with its own data directory, pinned to `pin` (nil means no pin)."
  @spec start(non_neg_integer() | nil) :: t()
  def start(pin) do
    name = Distribution.peer_name("mv")
    data_dir = ~c"#{System.tmp_dir!()}/malachi_ra_mv_#{name}"
    boot(name, data_dir, pin)
  end

  @doc """
  Stops `peer`: `ra` first, so its name registry is closed on disk as it is on a clean node shutdown,
  then the node. A peer that is already down is left alone.
  """
  @spec stop(t()) :: :ok
  def stop(%{peer: peer, node: node}) do
    _ = :erpc.call(node, :application, :stop, [:ra], 10_000)
    :peer.stop(peer)
  catch
    _kind, _reason -> :ok
  end

  @doc """
  Starts `peer` again on the same name and data directory, pinned to `pin`, and restarts the local
  members of `clusters` from their persisted logs. Returns the new handle.
  """
  @spec restart(t(), non_neg_integer() | nil, [atom()]) :: t()
  def restart(%{name: name, data_dir: data_dir} = peer, pin, clusters) do
    :ok = stop(peer)
    restarted = boot(name, data_dir, pin)

    for cluster <- clusters do
      :ok = :erpc.call(restarted.node, :ra, :restart_server, [@system, {cluster, restarted.node}])
    end

    restarted
  end

  @doc "The effective machine version `ra` recorded for `cluster`'s member on `node`, or nil."
  @spec effective(node(), atom()) :: non_neg_integer() | nil
  def effective(node, cluster) do
    case :erpc.call(node, :ra_counters, :counters, [{cluster, node}, [:effective_machine_version]]) do
      %{effective_machine_version: effective} -> effective
      _undefined -> nil
    end
  end

  @doc "The state `cluster`'s member on `node` holds right now (no consensus round trip)."
  @spec local_state(node(), atom()) :: term()
  def local_state(node, cluster) do
    {:ok, {_index_term, state}, _leader} = :ra.local_query({cluster, node}, &Function.identity/1)
    state
  end

  @doc "Polls `fun` until it returns a truthy value, for up to `timeout_ms`. Returns that value or false."
  @spec eventually((-> term()), non_neg_integer()) :: term()
  def eventually(fun, timeout_ms \\ 15_000) do
    case fun.() do
      result when result not in [nil, false] ->
        result

      _falsy when timeout_ms <= 0 ->
        false

      _falsy ->
        Process.sleep(100)
        eventually(fun, timeout_ms - 100)
    end
  end

  defp boot(name, data_dir, pin) do
    {:ok, peer, node} = :peer.start(%{name: name, host: ~c"127.0.0.1", longnames: true})
    :ok = :erpc.call(node, :code, :add_paths, [:code.get_path()])
    if pin, do: :ok = :erpc.call(node, :application, :set_env, [:malachi, :ra_machine_version_pin, pin])
    {:ok, _apps} = :erpc.call(node, :application, :ensure_all_started, [:ra])
    {:ok, _pid} = :erpc.call(node, :ra, :start_in, [data_dir])
    %{peer: peer, node: node, name: name, data_dir: data_dir}
  end
end
