defmodule Malachi.Cluster.ReplicatedMetadata do
  @moduledoc """
  The control plane's metadata made **authoritative via Raft**, with a local read cache.

  It pairs a `Malachi.Cluster.MetadataServer` (one `ra` cluster running `Malachi.Metadata.apply/2`)
  with a local `Malachi.Metadata` materialized view. Mutations go through the Raft log (durable and
  replicated); on commit, the very same command is applied to the local cache. Because
  `Malachi.Metadata.apply/2` is deterministic, the cache always equals the replicated state, so
  reads are served locally from the cache (no Raft round-trip on the hot path) with read-your-writes
  consistency.

  The cache is correct without refreshing as long as this process is the only writer (the
  single-control-node topology). `refresh/1` re-reads the replicated state for the multi-writer case
  or after recovery.

  `ra` must already be running (e.g. `:ra.start_in/1`), as with `Malachi.Cluster.MetadataServer`.
  """

  alias Malachi.Cluster.MachineVersion
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Metadata

  @type t :: %__MODULE__{server_id: MetadataServer.server_id(), cache: Metadata.t()}

  defstruct server_id: nil, cache: nil

  @doc "Starts (or joins) the Raft cluster `cluster_name` and seeds the local cache from it."
  @spec start(MetadataServer.cluster_name()) :: {:ok, t()} | {:error, term()}
  def start(cluster_name) do
    with {:ok, server_id} <- MetadataServer.start(cluster_name),
         {:ok, cache} <- MetadataServer.query(server_id, & &1) do
      {:ok, %__MODULE__{server_id: server_id, cache: cache}}
    end
  end

  @doc """
  Submits `command` through the Raft log and, on commit, applies it to the local cache too.
  Returns `{reply, replicated_metadata}`. `reply` is the machine reply (e.g. `{:ok, root_id}` or
  `{:error, :already_exists}`), or `{:error, reason}` on a transport failure (cache unchanged).
  """
  @spec command(t(), Metadata.command()) :: {term(), t()}
  def command(%__MODULE__{server_id: server_id, cache: cache} = replicated, command) do
    {cache, reply} = apply_command(server_id, cache, command)
    {reply, %{replicated | cache: cache}}
  end

  @doc """
  Stateless form: submit `command` to the Raft cluster `server_id` and apply it to the **caller's**
  `metadata` cache, returning `{metadata, reply}` (the same shape as `Malachi.Metadata.apply/2`, so
  it is a drop-in metadata command function). The caller threads the cache, which lets a single
  operation perform several mutations with read-your-writes between them (e.g. a produce that opens
  and seals a segment). On a transport failure the cache is left unchanged.
  """
  @spec apply_command(MetadataServer.server_id(), Metadata.t(), Metadata.command()) ::
          {Metadata.t(), term()}
  def apply_command(server_id, metadata, command) do
    case submit(server_id, command) do
      {:ok, reply} ->
        if MachineVersion.refusal?(reply),
          do: {metadata, reply},
          else: {elem(Metadata.apply(metadata, command), 0), reply}

      {:error, reason} ->
        {metadata, {:error, reason}}
    end
  end

  # How long a command may wait for its commit. The broker runs these inside its own loop, so every
  # produce and fetch of the shard waits behind the call. The data path's commands keep ra's default,
  # which the produce path's own timeouts are sized around. An operator's binding has nobody waiting on
  # it but the operator, and must not hold the loop when the topic's vnode has lost quorum or is
  # electing; `Malachi.Policies.bind/3` turns the timeout into an answer.
  #
  # For the binding it is a deadline on the whole command, not ra's timeout: ra follows a redirect with a
  # fresh full timeout (`ra_server_proc:statem_call/3`), so a member that queues the call through an
  # election and then redirects would hold the loop for one timeout per hop.
  @admin_command_deadline_ms 2_000

  defp submit(server_id, {:bind_topic_policy, _topic, _name} = command),
    do:
      run_within(
        fn -> MetadataServer.command(server_id, command, @admin_command_deadline_ms) end,
        @admin_command_deadline_ms
      )

  defp submit(server_id, command), do: MetadataServer.command(server_id, command)

  @doc false
  # Runs `fun` in a monitored, unlinked process and answers what it returned, or `{:error, :timeout}` when
  # it has not returned within `deadline_ms`, or `{:error, {:command_crashed, reason}}`. Public only so the
  # deadline can be tested with a function other than a Raft command.
  #
  # A process still running at the deadline is killed, and the answer is given only after its DOWN
  # arrives. The kill is asynchronous, so the process may still send its result after `Process.exit/2`
  # returns; signals between two processes arrive in the order they were sent, so once the DOWN is here
  # any result it sent is already in this mailbox and is dropped, and nothing reaches the caller's
  # message handlers afterwards. A timed out command is ambiguous the way any Raft timeout is: it may
  # still commit.
  @spec run_within((-> result), pos_integer()) :: result | {:error, :timeout | {:command_crashed, term()}}
        when result: term()
  def run_within(fun, deadline_ms) do
    caller = self()
    tag = make_ref()
    {pid, monitor} = spawn_monitor(fn -> send(caller, {tag, fun.()}) end)

    receive do
      {^tag, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^pid, reason} ->
        {:error, {:command_crashed, reason}}
    after
      deadline_ms ->
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^monitor, :process, ^pid, _reason} -> :ok
        end

        receive do
          {^tag, _late} -> :ok
        after
          0 -> :ok
        end

        {:error, :timeout}
    end
  end

  # Why a refusal is not re-applied here. The cache tracks the replicated state because both apply the
  # same deterministic `Malachi.Metadata.apply/2`, so a command the metadata refuses (an existing topic,
  # an unknown range) leaves both unchanged. The machine version gate is the exception: it runs inside
  # the ra machine only (`Malachi.Cluster.MachineVersion.apply/5`), in front of `Metadata.apply/2`. A
  # command introduced above the group's effective version is refused there while `Metadata.apply/2`
  # would accept it, so re-applying it would put into the cache what the log refused, and a caller
  # that journals the cache change would replay it after the next refresh.

  @doc "The local metadata view, for reads (routing, segment lookup)."
  @spec metadata(t()) :: Metadata.t()
  def metadata(%__MODULE__{cache: cache}), do: cache

  @doc "Re-reads the replicated state into the cache (multi-writer or post-recovery)."
  @spec refresh(t()) :: {:ok, t()} | {:error, term()}
  def refresh(%__MODULE__{} = replicated) do
    case MetadataServer.query(replicated.server_id, & &1) do
      {:ok, cache} -> {:ok, %{replicated | cache: cache}}
      {:error, _reason} = error -> error
    end
  end

  @doc "Stops and deletes the underlying Raft cluster (removing its on-disk state)."
  @spec delete(t()) :: :ok | {:error, term()}
  def delete(%__MODULE__{server_id: server_id}), do: MetadataServer.delete(server_id)
end
