defmodule Malachi.Test.StuckRaMember do
  @moduledoc """
  A real, local, single-member `Malachi.Cluster.MetadataMachine` cluster driven into the state a rolled
  back member ends up in: its log holds the `noop` that moved the group to machine version 1, and it
  restarts on code pinned to version 0. `ra` replays that `noop`, cannot honor it, and stops applying
  entries, which is what `Malachi.Cluster.MachineVersion.check/3` has to report.

  The pin is node-wide application env, so tests using this must be `async: false`, and must call
  `cleanup/1` (normally from `on_exit`).
  """

  alias Malachi.Cluster.MetadataMachine
  alias Malachi.Cluster.RaCluster

  @system :default

  @doc "Forms the cluster, lets it reach version 1, then restarts it pinned to 0. Returns the server id."
  @spec start(atom()) :: {atom(), node()}
  def start(cluster_name) do
    {:ok, _member} = RaCluster.start(MetadataMachine, cluster_name, [node()])
    server_id = {cluster_name, node()}
    :ok = await_effective(server_id, 1)

    restart(server_id, 0)
    :ok = await_effective(server_id, 1)
    server_id
  end

  @doc "Restarts the member on unpinned code, which supports version 1 again."
  @spec recover({atom(), node()}) :: :ok
  def recover(server_id) do
    restart(server_id, nil)
    await_effective(server_id, 1)
  end

  @doc "Drops the pin and deletes the cluster."
  @spec cleanup({atom(), node()}) :: :ok
  def cleanup(server_id) do
    Application.delete_env(:malachi, :ra_machine_version_pin)
    _ = RaCluster.delete(server_id)
    :ok
  end

  @doc "The effective machine version `ra` recorded for a local member, or nil when it has no counters."
  @spec effective(atom() | {atom(), node()}) :: non_neg_integer() | nil
  def effective(server_id) do
    case :ra_counters.counters(server_id, [:effective_machine_version]) do
      %{effective_machine_version: effective} -> effective
      _undefined -> nil
    end
  end

  defp restart(server_id, pin) do
    :ok = :ra.stop_server(@system, server_id)

    if pin,
      do: Application.put_env(:malachi, :ra_machine_version_pin, pin),
      else: Application.delete_env(:malachi, :ra_machine_version_pin)

    :ok = :ra.restart_server(@system, server_id)
  end

  defp await_effective(server_id, expected, remaining_ms \\ 5_000) do
    cond do
      effective(server_id) == expected ->
        :ok

      remaining_ms <= 0 ->
        {:error, {:effective_version, effective(server_id)}}

      true ->
        Process.sleep(20)
        await_effective(server_id, expected, remaining_ms - 20)
    end
  end
end
