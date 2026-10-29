defmodule Malachi.Test.StreamCluster do
  @moduledoc """
  The cluster the producer stream multinode tests run on (`start/0`), and what a peer node of it needs, as
  named functions a peer can call (`test/support` is on every peer's code path, a closure defined in a
  test module is not): the brokers it counts as live, which a test narrows to fail one or to steer where
  a segment is placed, and the processes a `Malachi.BrokerServer` and a `Malachi.Cluster.HealCoordinator`
  expect beside them.
  """

  alias Malachi.BrokerServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Test.RaPeers

  @key {__MODULE__, :live}
  @broker :stream_mn_broker
  @repl :stream_mn_repl

  @doc "The name of the broker on every node of the cluster."
  @spec broker() :: atom()
  def broker, do: @broker

  @doc "The name of the replication server on every node of the cluster."
  @spec repl() :: atom()
  def repl, do: @repl

  @doc """
  Starts three peers, each running a replication server and a broker over one replicated control plane
  (replication factor 2, every peer counting all three as live), with segments that ask to roll once they
  hold three records like `Record.new("r00", key: "k")`, and creates the topic `"events"` with a 4-bit
  keyspace. Must run in the test process: the peers are stopped and their directories removed when the
  test exits. Returns the nodes, the replication servers' references (in the nodes' order) and the
  topic's root range.
  """
  @spec start() :: %{nodes: [node()], refs: [{atom(), node()}], root: Malachi.Metadata.range_id()}
  def start do
    peers = Enum.map(1..3, fn _ -> RaPeers.start(nil) end)
    ExUnit.Callbacks.on_exit(fn -> Enum.each(peers, &RaPeers.stop/1) end)
    nodes = Enum.map(peers, & &1.node)
    refs = Enum.map(nodes, &{@repl, &1})
    cluster = :"stream_mn_#{System.unique_integer([:positive])}"

    for node <- nodes do
      {:ok, _apps} = :erpc.call(node, :application, :ensure_all_started, [:telemetry])
      :ok = :erpc.call(node, Logger, :configure, [[level: :none]])
      :ok = :erpc.call(node, __MODULE__, :put_live, [refs])
      :ok = :erpc.call(node, __MODULE__, :start_task_supervisor, [])
      ExUnit.Callbacks.on_exit(fn -> File.rm_rf!(directory(node)) end)

      {:ok, _repl} =
        :erpc.call(node, GenServer, :start, [
          ReplicationServer,
          [name: @repl, directory: Path.join(directory(node), "repl")],
          [name: @repl]
        ])
    end

    for node <- nodes do
      opts = [
        brokers: refs,
        replication_factor: 2,
        metadata_cluster: cluster,
        metadata_nodes: nodes,
        live_brokers: &__MODULE__.live_brokers/0,
        policy_fun: &__MODULE__.no_policy/1,
        segment_max_bytes: 3 * Record.encoded_size(Record.new("r00", key: "k"))
      ]

      {:ok, _broker} =
        :erpc.call(
          node,
          GenServer,
          :start,
          [BrokerServer, {Path.join(directory(node), "broker"), opts}, [name: @broker]],
          30_000
        )
    end

    {:ok, root} = BrokerServer.create_topic({@broker, hd(nodes)}, "events", 4)
    %{nodes: nodes, refs: refs, root: root}
  end

  defp directory(node), do: Path.join(System.tmp_dir!(), "stream_mn_#{node}")

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
  def apply_heal(command), do: BrokerServer.apply_heal(@broker, [command])

  @doc "The broker on this node's view of the metadata."
  @spec metadata() :: Malachi.Metadata.t()
  def metadata, do: BrokerServer.metadata(@broker)

  @doc "The broker on `node`'s view of the metadata."
  @spec metadata_on(node()) :: Malachi.Metadata.t()
  def metadata_on(node), do: BrokerServer.metadata({@broker, node})
end
