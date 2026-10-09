defmodule Malachi.Test.StreamCluster do
  @moduledoc """
  What a peer node of the producer stream multinode test needs, as named functions a peer can call
  (`test/support` is on every peer's code path, a closure defined in a test module is not): the brokers it
  counts as live, which the test narrows to fail one, and the processes a `Malachi.BrokerServer` and a
  `Malachi.Cluster.HealCoordinator` expect beside them.
  """

  @key {__MODULE__, :live}

  @doc "Sets the brokers this node counts as live."
  @spec put_live([term()]) :: :ok
  def put_live(brokers), do: :persistent_term.put(@key, brokers)

  @doc "The brokers this node counts as live."
  @spec live_brokers() :: [term()]
  def live_brokers, do: :persistent_term.get(@key, [])

  @doc "Starts, unlinked, the task supervisor a broker's reconcile runs under."
  @spec start_task_supervisor() :: :ok
  def start_task_supervisor do
    {:ok, pid} = Task.Supervisor.start_link(name: Malachi.TaskSupervisor)
    Process.unlink(pid)
    :ok
  end

  @doc "No storage policy: every topic keeps the global defaults."
  @spec no_policy(term()) :: nil
  def no_policy(_name), do: nil

  @doc "Applies a heal command through the broker on this node, as the application wires it."
  @spec apply_heal(term()) :: :ok
  def apply_heal(command), do: Malachi.BrokerServer.apply_heal(:stream_mn_broker, [command])

  @doc "The broker on this node's view of the metadata."
  @spec metadata() :: Malachi.Metadata.t()
  def metadata, do: Malachi.BrokerServer.metadata(:stream_mn_broker)

  @doc "The broker on `node`'s view of the metadata."
  @spec metadata_on(node()) :: Malachi.Metadata.t()
  def metadata_on(node), do: Malachi.BrokerServer.metadata({:stream_mn_broker, node})
end
