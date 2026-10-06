defmodule Malachi.Test.AppChildren do
  @moduledoc """
  Takes one of the running application's children out of its supervision tree for the length of a test,
  and puts it back after.

  The suite's application is a single node in its default configuration, which is a one-member control
  plane cluster (#273): it runs a membership server, a ring store and their reconcilers under their
  production names. `borrow_ring/0` does the same for the ring store, which is an `ra` cluster rather
  than a child. A test that registers one of those names itself, or that needs the name unclaimed,
  suspends the application's child first. Must be called from a test or its setup (it registers an
  `ExUnit.Callbacks.on_exit/1`).
  """

  alias Malachi.Cluster.RaCluster
  alias Malachi.Cluster.RingMachine
  alias Malachi.Cluster.RingServer
  alias Malachi.Test.PollingHelper

  @supervisor Malachi.Supervisor
  @log_ring Malachi.LogRing

  @doc """
  Terminates the application child `id` and restarts it when the test exits. A child the application
  does not run is left alone. Returns `:ok`.
  """
  @spec suspend(term()) :: :ok
  def suspend(id) do
    case Supervisor.terminate_child(@supervisor, id) do
      :ok -> ExUnit.Callbacks.on_exit(fn -> {:ok, _pid} = Supervisor.restart_child(@supervisor, id) end)
      {:error, :not_found} -> :ok
    end

    :ok
  end

  @doc """
  Takes the application's ring store (`Malachi.LogRing`) away for the length of the test, leaving the
  name to the test from no store at all, and forms an empty one again when the test exits: the state a
  single node boots its ring store in. The reconciler that keeps this node joined to the store is
  suspended with it, so it cannot form the store again mid-test. Returns `:ok`.

  A test that formed and deleted `Malachi.LogRing` without this used to delete the application's own,
  and every later test that reads the topology of record found no store (`:noproc`).
  """
  @spec borrow_ring() :: :ok
  def borrow_ring do
    suspend(Malachi.LogRingReconciler)
    delete_ring!()

    ExUnit.Callbacks.on_exit(fn ->
      PollingHelper.wait_until!(fn -> is_nil(Process.whereis(@log_ring)) end)
      {:ok, _server_id} = RaCluster.start(RingMachine, @log_ring, [node()])
    end)

    :ok
  end

  @doc """
  Forms a `Malachi.LogRing` holding `topology` (or none), the way boot leaves it, deleted when the test
  exits. Call `borrow_ring/0` first, in the test or its setup. Returns the server id.
  """
  @spec start_ring!(term()) :: RingServer.server_id()
  def start_ring!(topology) do
    {:ok, server_id} = RaCluster.start(RingMachine, @log_ring, [node()])
    ExUnit.Callbacks.on_exit(&delete_ring!/0)
    if topology, do: :ok = RingServer.init(server_id, topology)
    server_id
  end

  # Deleting a cluster returns before its server has gone (a leader may even report its own shutdown as
  # the answer), and forming one under the same name before then is refused as already started.
  defp delete_ring! do
    case RaCluster.delete(@log_ring) do
      :ok -> :ok
      {:error, {:shutdown, :delete}} -> :ok
    end

    PollingHelper.wait_until!(fn -> is_nil(Process.whereis(@log_ring)) end)
  end
end
