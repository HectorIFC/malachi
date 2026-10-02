defmodule Malachi.Storage.DataDirGuard do
  @moduledoc """
  The boot check that a log data directory and the control plane this node is about to start describe
  the same data.

  `ra` keeps a node's control plane under the node's name: its log lives under
  `MALACHI_RA_DATA_DIR/<node>`, and every member is recorded as `{cluster, node()}`. What a node
  acknowledged survives a restart only while the segments on disk and that control plane come back
  together, and when they do not, the orphan sweep used to settle it by deleting the segments (#273).
  Two things are checked before the broker starts:

    * **segments nobody knows.** The log directory already holds segment directories, and the control
      plane that would describe them is about to be formed rather than resumed: the ring store
      (`Malachi.LogRing`, which every clustered node runs, sharded or not) was never started on this
      node, or, for an unsharded control plane, its metadata member was never started here. The node
      name changed (a container recreated without a fixed hostname, `iex --name` with another name), the
      `ra` directory was lost, the cluster was renamed, or the segments were written by a release that
      kept a single node's metadata in memory. On a single node a control plane formed now is empty,
      every one of those directories would read as an orphan, and a topic created again under an old
      name would read the old bytes as its own. A cluster member is refused too, though its peers may
      still hold the control plane: it would come back an amnesiac voter, and adopting is then the
      operator's way back, the sweep removing only what no owner lists. A node that joins a cluster for
      the first time has nothing on disk to refuse.
    * **a single node told it has peers.** The control plane was started here as a cluster of one, and
      the configuration now lists other nodes. Growing a one-member cluster in place is not supported:
      this node would resume its ring store (and its metadata, sharded or not) alone while the others
      formed clusters of their own over the same names. The ring store member, which every clustered
      node runs and which was formed over the same nodes as the metadata, is asked for its recorded
      membership; a member that does not answer within the ring boot timeout refuses the start too,
      since a check that cannot be made must not read as passed.

  Each refuses the start through `Malachi.StartupRefusal` (exit 78) with a line that names the causes
  and the way out. The first can be overridden with `MALACHI_ADOPT_ORPHANED_LOG_DIR=true`, which is the
  operator saying the directories are not wanted: the start goes on, the adopted names are logged, and
  the orphan sweep removes them on its own schedule. The others cannot.

  `decide/1` is the decision over plain facts; `check/4` gathers them and acts.
  """

  require Logger

  alias Malachi.Cluster.RaResume
  alias Malachi.I18n
  alias Malachi.Retention.Orphans
  alias Malachi.StartupRefusal

  # The ra system the control plane clusters live in (`Malachi.Cluster.RaCluster`).
  @system :default
  # The ring store's cluster name (`Malachi.Cluster.RingServer`), started by every clustered node.
  @log_ring Malachi.LogRing
  # One local membership query. A local query is answered by the member itself, not by a leader, so it
  # does not wait on a quorum; it does wait on the member's log replay, which is why it is retried.
  @members_timeout_ms 5_000
  @members_retry_ms 200
  # How many names a refusal lists before it says how many more there are, so one line stays one line.
  @listed 10

  @typedoc """
  What `decide/1` needs to know, all of it read from this node. `ring_known?` and `member_known?` say
  whether the ring store and the unsharded metadata member had ever been started on this node BEFORE
  this boot; `ring_members` is the ring store member's recorded membership, `:unknown` when it did not
  answer, or `nil` when it was not asked.
  """
  @type facts :: %{
          self: node(),
          configured_nodes: [node()],
          sharded?: boolean(),
          ring_known?: boolean(),
          member_known?: boolean(),
          ring_members: [node()] | :unknown | nil,
          segment_dirs: [String.t()],
          adopt?: boolean()
        }

  @type decision ::
          :ok
          | {:adopt, [String.t()]}
          | {:refuse, {:unknown_segments, [String.t()]}}
          | {:refuse, {:grow_unsupported, [node()]}}
          | {:refuse, :membership_unknown}

  @doc """
  The decision over `facts`:

    * `{:refuse, {:unknown_segments, dirs}}`: the log directory holds segment directories and the
      control plane is formed now (`{:adopt, dirs}` instead when `adopt?`);
    * `{:refuse, {:grow_unsupported, others}}`: the ring store was started here as a cluster of one, and
      the configuration lists `others` besides this node;
    * `{:refuse, :membership_unknown}`: that member's membership was needed and it did not answer;
    * `:ok` otherwise.
  """
  @spec decide(facts()) :: decision()
  def decide(%{segment_dirs: [_ | _] = dirs, adopt?: adopt?} = facts) do
    cond do
      not formed_now?(facts) -> decide_membership(facts)
      adopt? -> {:adopt, Enum.sort(dirs)}
      true -> {:refuse, {:unknown_segments, Enum.sort(dirs)}}
    end
  end

  def decide(facts), do: decide_membership(facts)

  defp formed_now?(%{ring_known?: false}), do: true
  defp formed_now?(%{sharded?: false, member_known?: false}), do: true
  defp formed_now?(_facts), do: false

  defp decide_membership(%{ring_known?: true} = facts) do
    case {peers(facts), facts.ring_members} do
      {[], _recorded} -> :ok
      {_others, :unknown} -> {:refuse, :membership_unknown}
      {others, [self]} when self == facts.self -> {:refuse, {:grow_unsupported, others}}
      {_others, _recorded} -> :ok
    end
  end

  defp decide_membership(_facts), do: :ok

  defp peers(facts), do: facts.configured_nodes |> Enum.uniq() |> List.delete(facts.self) |> Enum.sort()

  @doc """
  Gathers the facts for the control plane `cluster` over `nodes` and the log directory `dir`, decides,
  and acts: `:ok` and `{:adopt, dirs}` return (the second after logging the names), a refusal halts
  through `Malachi.StartupRefusal.refuse!/2`.

  The ring store member is asked for its recorded membership only when it was started here before and
  `nodes` lists other nodes, the one case that needs it. The boot starts that member before this check
  runs; asking resumes it if it is not running, which is idempotent (`Malachi.Cluster.RaResume.resume_or/3`).

  ## Options

    * `:ring_known?` - whether the ring store was started on this node before this boot (required: the
      boot starts the ring store before it knows whether the control plane is sharded, so the caller
      reads this first, with `ring_known?/0`);
    * `:sharded?` - whether the control plane is sharded (default `false`);
    * `:adopt?` - accept unknown segment directories (default from `:adopt_orphaned_log_dir`);
    * `:members_deadline_ms` - how long the member gets to report its membership, retried
      (default from `:log_ring_boot_timeout_ms`, 60000);
    * `:members_fun` - how one attempt asks it, `(server_id, timeout) -> ra members reply` (default a
      local `:ra.members/2` query), the seam a test uses;
    * `:halt_fun` - how a refusal halts (default `System.halt/1`), the seam a test uses.
  """
  @spec check(atom(), [node()], Path.t(), keyword()) :: decision() | term()
  def check(cluster, nodes, dir, opts) do
    server_id = {cluster, node()}
    sharded? = Keyword.get(opts, :sharded?, false)
    member_known? = not sharded? and RaResume.registered?(@system, server_id)
    ring_known? = Keyword.fetch!(opts, :ring_known?)

    facts = %{
      self: node(),
      configured_nodes: nodes,
      sharded?: sharded?,
      ring_known?: ring_known?,
      member_known?: member_known?,
      ring_members: if(ring_known? and Enum.uniq(nodes) != [node()], do: recorded_members({@log_ring, node()}, opts)),
      segment_dirs: segment_dirs(dir),
      adopt?: Keyword.get_lazy(opts, :adopt?, fn -> Application.get_env(:malachi, :adopt_orphaned_log_dir, false) end)
    }

    act(decide(facts), cluster, dir, Keyword.get(opts, :halt_fun, &System.halt/1))
  end

  @doc """
  Whether the ring store was ever started on this node: the fact `check/4` needs as `:ring_known?`, read
  before the boot starts the store and so registers it.
  """
  @spec ring_known?() :: boolean()
  def ring_known?, do: RaResume.registered?(@system, {@log_ring, node()})

  @doc "The names at the root of `dir` that are segment directories (`Malachi.Retention.Orphans.segment_directory?/1`)."
  @spec segment_dirs(Path.t()) :: [String.t()]
  def segment_dirs(dir) do
    case File.ls(dir) do
      {:ok, names} -> Enum.filter(names, &(Orphans.segment_directory?(&1) and File.dir?(Path.join(dir, &1))))
      {:error, :enoent} -> []
      {:error, reason} -> raise File.Error, reason: reason, action: "list directory", path: dir
    end
  end

  # The member's recorded membership, asked until the deadline: a member replaying a long log answers
  # only once the replay is done, and a member that cannot come back never does. Either way past the
  # deadline the answer is :unknown, which refuses.
  defp recorded_members(server_id, opts) do
    deadline_ms =
      Keyword.get_lazy(opts, :members_deadline_ms, fn ->
        Application.get_env(:malachi, :log_ring_boot_timeout_ms, 60_000)
      end)

    members_fun = Keyword.get(opts, :members_fun, &local_members/2)
    ask_members(server_id, members_fun, System.monotonic_time(:millisecond) + deadline_ms)
  end

  defp local_members(server_id, timeout), do: :ra.members({:local, server_id}, timeout)

  defp ask_members(server_id, members_fun, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    with true <- remaining > 0,
         :ok <- RaResume.resume_or(@system, server_id, fn -> {:error, :not_registered} end),
         {:ok, members, _leader} <- members_fun.(server_id, min(@members_timeout_ms, remaining)) do
      members |> Enum.map(fn {_name, member_node} -> member_node end) |> Enum.sort()
    else
      false ->
        :unknown

      _not_yet ->
        Process.sleep(@members_retry_ms)
        ask_members(server_id, members_fun, deadline)
    end
  end

  defp act(:ok, _cluster, _dir, _halt_fun), do: :ok

  defp act({:adopt, dirs} = decision, _cluster, dir, _halt_fun) do
    Logger.warning(I18n.t(:data_dir_adopted, count: length(dirs), path: dir, names: listed(dirs)))
    decision
  end

  defp act({:refuse, {:unknown_segments, dirs}}, cluster, dir, halt_fun) do
    I18n.t(:data_dir_unknown_segments,
      count: length(dirs),
      path: dir,
      names: listed(dirs),
      cluster: cluster,
      node: node()
    )
    |> StartupRefusal.refuse!(halt_fun)
  end

  defp act({:refuse, {:grow_unsupported, others}}, cluster, _dir, halt_fun) do
    I18n.t(:data_dir_grow_unsupported,
      cluster: cluster,
      node: node(),
      others: Enum.map_join(others, ", ", &to_string/1)
    )
    |> StartupRefusal.refuse!(halt_fun)
  end

  defp act({:refuse, :membership_unknown}, cluster, _dir, halt_fun) do
    I18n.t(:data_dir_membership_unknown, cluster: cluster, node: node())
    |> StartupRefusal.refuse!(halt_fun)
  end

  defp listed(names) do
    case Enum.split(names, @listed) do
      {shown, []} -> Enum.join(shown, ", ")
      {shown, rest} -> Enum.join(shown, ", ") <> " (+#{length(rest)})"
    end
  end
end
