defmodule Malachi.Test.ClusterFlagsPause do
  @moduledoc """
  Takes the running application's cluster flag cache over for the length of a test: pauses the
  reconciler that refreshes it (`Malachi.LogClusterFlagsReconciler`), so a value the test puts there is
  not overwritten by a tick, and puts back exactly what the cache held afterwards, unread included.

  `Malachi.Cluster.ClusterFlagsCache.enabled/0` answers `[]` both for a cache nobody has read and for
  one that was read and holds nothing, so restoring that list alone would turn the first into the second
  and make a later readiness check pass for a node whose store never answered. Must be called from a test
  or its setup (it registers an `ExUnit.Callbacks.on_exit/1`), in a module that is not async.
  """

  alias Malachi.Cluster.ClusterFlagsCache

  @reconciler Malachi.LogClusterFlagsReconciler

  @doc "Pauses the reconciler and arranges for the cache and the reconciler to be restored. Returns `:ok`."
  @spec pause() :: :ok
  def pause do
    restore = snapshot()
    _ = Supervisor.terminate_child(Malachi.Supervisor, @reconciler)

    ExUnit.Callbacks.on_exit(fn ->
      restore.()
      _ = Supervisor.restart_child(Malachi.Supervisor, @reconciler)
    end)

    :ok
  end

  # What the cache held, as a function that puts it back exactly, unread included.
  defp snapshot do
    if ClusterFlagsCache.read?() do
      published = ClusterFlagsCache.enabled()
      fn -> ClusterFlagsCache.put(published) end
    else
      &ClusterFlagsCache.forget/0
    end
  end
end
