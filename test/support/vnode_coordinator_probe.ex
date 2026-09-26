defmodule Malachi.Test.VnodeCoordinatorProbe do
  @moduledoc """
  Runs a vnode coordinator manager **on the node it is called from**, with the production placement
  seams and recording `:spawn`/`:stop` seams that report back to a test process.

  It lives in `test/support` rather than in a test module so a multinode test can invoke it on a peer:
  peer nodes share this project's compiled code path, and test modules are not part of it.

  The seams that carry the behaviour under test (`:placement`, `:leading`, `:version_servers`) are the
  production ones, taken from `Malachi.Application.vnode_coordinator_manager_opts/0` and evaluated on
  the node that runs them. Only `:spawn` and `:stop` are replaced, because the real ones would start
  this node's whole heal, retention and consumer-group stack per vnode.
  """

  alias Malachi.Cluster.MembershipServer
  alias Malachi.Cluster.RingTopology
  alias Malachi.Cluster.VnodeCoordinatorManager, as: Manager

  @doc """
  Starts an unlinked membership server under the production name, holding `topology`.

  Unlinked on purpose: started through `:erpc`, a linked server would die with the short-lived worker
  that started it.
  """
  @spec start_membership(RingTopology.t() | nil) :: {:ok, pid()} | {:error, term()}
  def start_membership(topology) do
    opts = [self_ref: {Malachi.LogMembership, node()}] ++ if(topology, do: [topology: topology], else: [])

    GenServer.start(MembershipServer, opts, name: Malachi.LogMembership)
  end

  @doc """
  Starts an unlinked manager whose coordinator trees are recorded instead of started.

  Every start is reported to `test_pid` as `{:spawn, node(), vnode_id}` and every stop as
  `{:stop, node(), pid}`, so a test on another node can assert on what this node decided to run.
  """
  @spec start_manager(pid(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start_manager(test_pid, opts \\ []) do
    recorded =
      Malachi.Application.vnode_coordinator_manager_opts()
      |> Keyword.drop([:name])
      |> Keyword.merge(
        spawn: fn vnode_id ->
          pid = spawn(fn -> Process.sleep(:infinity) end)
          send(test_pid, {:spawn, node(), vnode_id})
          pid
        end,
        stop: fn pid ->
          send(test_pid, {:stop, node(), pid})
          Process.exit(pid, :kill)
        end,
        interval: Keyword.get(opts, :interval, 200)
      )

    GenServer.start(Manager, recorded)
  end

  @doc "The vnode ids the manager on this node currently runs coordinators for."
  @spec running(pid()) :: [term()]
  def running(manager), do: Manager.reconcile_now(manager)
end
