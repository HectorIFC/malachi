defmodule Malachi.Storage.DataDirGuard do
  @moduledoc """
  The boot check that a log data directory and the control plane this node is about to start describe
  the same data.

  `ra` keeps a node's control plane under the node's name: its log lives under
  `MALACHI_RA_DATA_DIR/<node>`, and every member is recorded as `{cluster, node()}`. What a node
  acknowledged survives a restart only while the segments on disk and that control plane come back
  together, and when they do not, the orphan sweep used to settle it by deleting the segments (#273).
  Two things are checked before the broker starts, after a first one on the directory's own record:

    * **a log directory of another control plane.** A start that passes records the control plane's
      name at the root of the log directory (`malachi.cluster`), just before it writes a seed or once
      every check has passed, and a later start under another name refuses, before the ring store is
      touched. A start refused before its seed is written records nothing; one that fails after the
      seed check passed (the seed write, or a later membership check) keeps the name it seeded under,
      since the seed's vnodes carry that name and whether the write landed may not be known. Every
      other check reads `ra` by node name, and the ring store is one per `ra` directory and node name
      whatever the cluster is called, so a renamed control plane over the same directories would read
      the old one's state as its own; a member that held no segment would pass every other check and
      could seed the ring its peers then trust.

    * **segments nobody knows.** The log directory already holds segment directories, and the control
      plane that would describe them is about to be formed rather than resumed: the ring store
      (`Malachi.LogRing`, which every clustered node runs, sharded or not) was never started on this
      node; or, for an unsharded control plane, its metadata member was never started here; or a sharded
      ring is about to be seeded, forming every vnode now (`check_seed/3`); or, for a
      sharded node configured alone, one of its vnodes was never started here. A cluster is not held
      to that, since a rebalance moves vnodes off a node without republishing the ring, and a vnode
      with other replicas keeps its history on them; a vnode whose only replica is this node
      (`MALACHI_LOG_VNODE_REPLICATION_FACTOR=1` on a cluster) has no such protection. The node
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
  and the way out. Segments nobody knows can be adopted with `MALACHI_ADOPT_ORPHANED_LOG_DIR=true`, which is the
  operator saying the directories are not wanted: the start goes on, the adopted names are logged, and
  the orphan sweep removes them on its own schedule. The others cannot.

  A third refusal guards the switch to sharding: a node whose unsharded metadata member ran here never
  starts sharded. A sharded ring is durable and outranks `MALACHI_LOG_VNODES` once written, while the
  topics stay described by the unsharded metadata nobody would read again, so the sweep would take their
  segments. Converting a control plane in place is not supported. The refusal is made twice, each with
  its own way out: before this node writes a seed (`check_seed/3`), when nothing was written and removing
  `MALACHI_LOG_VNODES` boots it unsharded again; and once the topology is known (`check/4`), for a
  sharded ring already recorded (seeded by another node, or by an earlier release that converted control
  planes in place, or by an earlier release on this node under another cluster name over the same `ra`
  directory), which no setting undoes and which only forming the cluster again gets past.

  The checks run in steps, because the boot starts the ring store before it can tell whether the
  control plane is sharded. `check_ring/4` runs before the boot touches the ring store, so its refusal
  leaves nothing behind: had the ring store been formed and the start then refused, a later attempt
  could find it registered and pass, depending only on whether ra's directory reached the disk before
  the halt. `check_seed/3` runs once the ring store is started but before a seed is written, so its
  refusal leaves an empty ring store and no seed. `check/4` runs once the topology is known, after the
  ring store was resumed or seeded, for everything that needs the topology.

  `decide_ring/1`, `decide_seed/1` and `decide/1` are the decisions over plain facts; `check_ring/4`,
  `check_seed/3` and `check/4` gather them and act.
  """

  require Logger

  alias Malachi.Cluster.RaResume
  alias Malachi.I18n
  alias Malachi.Retention.Orphans
  alias Malachi.StartupRefusal
  alias Malachi.Storage.Directory

  # The ra system the control plane clusters live in (`Malachi.Cluster.RaCluster`).
  @system :default
  # The ring store's cluster name (`Malachi.Cluster.RingServer`), started by every clustered node.
  @log_ring Malachi.LogRing
  # The control plane a log directory belongs to, recorded at its root just before a seed is written or
  # once the last check passed (`record_on_pass/4`).
  @cluster_marker "malachi.cluster"
  @cluster_marker_temp "malachi.cluster.tmp"
  # One local membership query. A local query is answered by the member itself, not by a leader, so it
  # does not wait on a quorum; it does wait on the member's log replay, which is why it is retried.
  @members_timeout_ms 5_000
  @members_retry_ms 200
  # How many names a refusal lists before it says how many more there are, so one line stays one line.
  @listed 10

  @typedoc """
  What `decide/1` needs to know once the topology is known, all of it read from this node.
  `member_known?` says whether the unsharded metadata member had ever been started here, sharded or not;
  `missing_vnodes`
  names the vnodes of a sharded node configured alone that never were; `ring_members` is the ring store
  member's recorded membership, `:unknown` when it did not answer, or `nil` when it was not asked;
  `adopted?` says the operator's adoption was already taken by `check_ring/4` or `check_seed/3`.
  """
  @type facts :: %{
          self: node(),
          configured_nodes: [node()],
          sharded?: boolean(),
          member_known?: boolean(),
          missing_vnodes: [atom()],
          ring_members: [node()] | :unknown | nil,
          segment_dirs: [String.t()],
          adopt?: boolean(),
          adopted?: boolean()
        }

  @typedoc """
  What `decide_ring/1` needs to know, read before the boot touches the ring store. `recorded_cluster` is
  the control plane name the log directory's cluster marker holds: `nil` when there is none yet, and
  `:unreadable` when the file does not parse.
  """
  @type ring_facts :: %{
          required(:ring_known?) => boolean(),
          required(:segment_dirs) => [String.t()],
          required(:adopt?) => boolean(),
          optional(:cluster) => String.t(),
          optional(:recorded_cluster) => String.t() | :unreadable | nil
        }

  @type decision ::
          :ok
          | {:adopt, [String.t()]}
          | {:refuse, {:unknown_segments, [String.t()]}}
          | {:refuse, {:grow_unsupported, [node()]}}
          | {:refuse, :membership_unknown}
          | {:refuse, :reshard_unsupported}
          | {:refuse, :resharded}
          | {:refuse, {:cluster_renamed, String.t() | :unreadable}}

  @doc """
  The decision over `facts`:

    * `{:refuse, {:unknown_segments, dirs}}`: the log directory holds segment directories and the
      control plane is formed now (`{:adopt, dirs}` instead when `adopt?`);
    * `{:refuse, {:grow_unsupported, others}}`: the ring store was started here as a cluster of one, and
      the configuration lists `others` besides this node;
    * `{:refuse, :membership_unknown}`: that member's membership was needed and it did not answer;
    * `{:refuse, :resharded}`: a sharded control plane on a node whose unsharded member ran here,
      whatever is on disk and whatever the operator would adopt;
    * `:ok` otherwise.
  """
  @spec decide(facts()) :: decision()
  def decide(%{sharded?: true, member_known?: true}), do: {:refuse, :resharded}

  def decide(%{segment_dirs: [_ | _] = dirs} = facts) do
    cond do
      not formed_now?(facts) -> decide_membership(facts)
      facts.adopted? -> decide_membership(facts)
      facts.adopt? -> {:adopt, Enum.sort(dirs)}
      true -> {:refuse, {:unknown_segments, Enum.sort(dirs)}}
    end
  end

  def decide(facts), do: decide_membership(facts)

  @doc """
  The decision `check_ring/4` makes before the boot touches the ring store: a log directory whose
  cluster marker names another control plane than `cluster`, or does not parse, refuses, whatever the
  operator adopts (the
  ring store is one per `ra` directory and node name, so it cannot tell the two apart); segment
  directories on a node whose ring store was never started here refuse (`{:adopt, dirs}` instead when
  `adopt?`).
  """
  @spec decide_ring(ring_facts()) ::
          :ok
          | {:adopt, [String.t()]}
          | {:refuse, {:unknown_segments, [String.t()]}}
          | {:refuse, {:cluster_renamed, String.t() | :unreadable}}
  def decide_ring(%{recorded_cluster: recorded, cluster: cluster}) when recorded not in [nil, cluster],
    do: {:refuse, {:cluster_renamed, recorded}}

  def decide_ring(%{ring_known?: false, segment_dirs: [_ | _] = dirs, adopt?: adopt?}) do
    if adopt?, do: {:adopt, Enum.sort(dirs)}, else: {:refuse, {:unknown_segments, Enum.sort(dirs)}}
  end

  def decide_ring(%{}), do: :ok

  defp formed_now?(%{sharded?: false, member_known?: false}), do: true
  defp formed_now?(%{sharded?: true, missing_vnodes: [_ | _]}), do: true
  defp formed_now?(_facts), do: false

  defp decide_membership(facts) do
    case {peers(facts), facts.ring_members} do
      {[], _recorded} -> :ok
      {_others, :unknown} -> {:refuse, :membership_unknown}
      {others, [self]} when self == facts.self -> {:refuse, {:grow_unsupported, others}}
      {_others, _recorded} -> :ok
    end
  end

  defp peers(facts), do: facts.configured_nodes |> Enum.uniq() |> List.delete(facts.self) |> Enum.sort()

  @doc """
  The first step, before the boot touches the ring store: refuses the start when the cluster marker in
  `dir` names another control plane than `cluster` or does not parse, whatever the operator adopts (a
  marker that cannot be read at all raises, `recorded_cluster/1`);
  then when `dir` holds segment directories and the ring store was never started on this node
  (`ring_known?`, read just before), or logs and answers `{:adopt, dirs}` when the operator adopted
  them. When it answers `{:adopt, dirs}`, pass `adopted?: true` to `check_seed/3` and `check/4`, so the
  adoption is not taken twice. It never records the marker.

  ## Options

    * `:adopt?` - accept unknown segment directories (default from `:adopt_orphaned_log_dir`);
    * `:halt_fun` - how a refusal halts (default `System.halt/1`), the seam a test uses.
  """
  @spec check_ring(atom(), Path.t(), boolean(), keyword()) :: :ok | {:adopt, [String.t()]} | term()
  def check_ring(cluster, dir, ring_known?, opts \\ []) do
    %{
      ring_known?: ring_known?,
      segment_dirs: segment_dirs(dir),
      adopt?: adopt?(opts),
      cluster: Atom.to_string(cluster),
      recorded_cluster: recorded_cluster(dir)
    }
    |> decide_ring()
    |> act(cluster, dir, Keyword.get(opts, :halt_fun, &System.halt/1))
  end

  # The cluster marker is written by the steps after `check_ring/4`, never by `check_ring/4` itself,
  # whose pass a later step can still undo: by `check_seed/3` just before a seed is written, and by
  # `check/4` at the end. Written at the ring check, a start refused over its segments would leave a name
  # that never ran, and the real one would then be refused as a rename. The seed is the one write that
  # cannot wait for the end: a seed recorded under no name would let a renamed start take it, so the
  # name is kept even when the seed then fails, since a timed-out write may have landed. Only a passing
  # decision records, and only where no marker is yet.
  defp record_on_pass({:refuse, _reason} = decision, _cluster, _dir, _opts), do: decision

  defp record_on_pass(decision, cluster, dir, opts) do
    if is_nil(recorded_cluster(dir)),
      do: write_cluster_marker!(dir, Atom.to_string(cluster), Keyword.get(opts, :sync_fun, &Directory.sync/1))

    decision
  end

  @doc "The cluster marker's path in the log directory `dir`."
  @spec cluster_marker_path(Path.t()) :: Path.t()
  def cluster_marker_path(dir), do: Path.join(dir, @cluster_marker)

  @doc """
  The control plane name the cluster marker in `dir` records: `nil` when there is none, `:unreadable`
  when the file does not hold exactly one `cluster=<name>` line. Any other read error raises, since a
  marker that cannot be read must not pass as absent.
  """
  @spec recorded_cluster(Path.t()) :: String.t() | :unreadable | nil
  def recorded_cluster(dir) do
    case File.read(cluster_marker_path(dir)) do
      {:ok, content} -> parse_cluster_marker(content)
      {:error, :enoent} -> nil
      {:error, reason} -> raise File.Error, reason: reason, action: "read file", path: cluster_marker_path(dir)
    end
  end

  defp parse_cluster_marker(content) do
    case Regex.run(~r/\Acluster=([^\n]+)\n\z/, content) do
      [_line, name] -> name
      nil -> :unreadable
    end
  end

  # The same way `Malachi.Storage.FormatMarker` writes: a temporary file with `:sync`, a rename over the
  # marker, then an fsync of the directory, so a crash leaves the old state or the new one.
  defp write_cluster_marker!(dir, name, sync_fun) do
    File.mkdir_p!(dir)
    temp = Path.join(dir, @cluster_marker_temp)
    File.write!(temp, "cluster=#{name}\n", [:sync])
    File.rename!(temp, cluster_marker_path(dir))

    case sync_fun.(dir) do
      :ok -> :ok
      {:error, reason} -> raise File.Error, reason: reason, action: "sync directory", path: dir
    end
  end

  @doc """
  The last step, once the topology is known (after `check_seed/3` when a seed was due): gathers the
  facts for the control plane `cluster` over `nodes` and the log directory `dir`, decides, and acts:
  `:ok` and `{:adopt, dirs}` return (the second after logging the names), a refusal halts through
  `Malachi.StartupRefusal.refuse!/2`.

  The ring store member, started by the boot before this runs, is asked for its recorded membership only
  when `nodes` lists other nodes, the one case that needs it; asking resumes it if it is not running,
  which is idempotent (`Malachi.Cluster.RaResume.resume_or/3`).

  ## Options

    * `:sharded?` - whether the control plane is sharded (default `false`);
    * `:vnodes` - the sharded placement, `[{vnode_id, token, nodes}]` (default `[]`): on a node
      configured alone, every one of them must have been started here before;
    * `:adopted?` - `check_ring/4` or `check_seed/3` already took the operator's adoption (default `false`);
    * `:adopt?` - accept unknown segment directories (default from `:adopt_orphaned_log_dir`);
    * `:members_deadline_ms` - how long the member gets to report its membership, retried
      (default from `:log_ring_boot_timeout_ms`, 60000);
    * `:members_fun` - how one attempt asks it, `(server_id, timeout) -> ra members reply` (default a
      local `:ra.members/2` query), the seam a test uses;
    * `:sync_fun` - how the cluster marker's directory is fsynced (default `Malachi.Storage.Directory.sync/1`),
      the seam a test uses;
    * `:halt_fun` - how a refusal halts (default `System.halt/1`), the seam a test uses.
  """
  @spec check(atom(), [node()], Path.t(), keyword()) :: decision() | term()
  def check(cluster, nodes, dir, opts \\ []) do
    sharded? = Keyword.get(opts, :sharded?, false)
    alone? = Enum.uniq(nodes) == [node()]

    facts = %{
      self: node(),
      configured_nodes: nodes,
      sharded?: sharded?,
      member_known?: RaResume.registered?(@system, {cluster, node()}),
      missing_vnodes: if(sharded? and alone?, do: missing_vnodes(Keyword.get(opts, :vnodes, [])), else: []),
      ring_members: if(alone? or node() not in nodes, do: nil, else: recorded_members({@log_ring, node()}, opts)),
      segment_dirs: segment_dirs(dir),
      adopt?: adopt?(opts),
      adopted?: Keyword.get(opts, :adopted?, false)
    }

    facts
    |> decide()
    |> record_on_pass(cluster, dir, opts)
    |> act(cluster, dir, Keyword.get(opts, :halt_fun, &System.halt/1))
  end

  defp adopt?(opts) do
    Keyword.get_lazy(opts, :adopt?, fn -> Application.get_env(:malachi, :adopt_orphaned_log_dir, false) end)
  end

  defp missing_vnodes(vnodes) do
    for {vnode_id, _token, _nodes} <- vnodes, not RaResume.registered?(@system, {vnode_id, node()}), do: vnode_id
  end

  @doc """
  The step before the boot writes a sharded seed into an empty ring store, so a refusal here leaves no
  seed behind. A seed forms every vnode now, empty, and outranks the environment from then on, so it
  refuses the start when the unsharded metadata member of `cluster` was ever started on this node (that
  metadata would never be read again) or when `dir` holds segment directories, which no vnode formed now
  can know (the ring store is one per `ra` directory and node name, so a cluster renamed over the same
  directories gets here). The operator's adoption applies to the segments, not to the unsharded member.

  ## Options

    * `:adopt?` - whether the operator adopted unknown segments (default the `:adopt_orphaned_log_dir`
      setting, `MALACHI_ADOPT_ORPHANED_LOG_DIR`);
    * `:adopted?` - `check_ring/4` already took that adoption (default false), so it is not taken twice;
    * `:sync_fun` - how the cluster marker's directory is fsynced (default `Malachi.Storage.Directory.sync/1`),
      the seam a test uses;
    * `:halt_fun` - how a refusal halts (default `System.halt/1`), the seam a test uses.
  """
  @spec check_seed(atom(), Path.t(), keyword()) :: :ok | {:adopt, [String.t()]} | term()
  def check_seed(cluster, dir, opts \\ []) do
    %{
      member_known?: RaResume.registered?(@system, {cluster, node()}),
      segment_dirs: segment_dirs(dir),
      adopt?: adopt?(opts),
      adopted?: Keyword.get(opts, :adopted?, false)
    }
    |> decide_seed()
    |> record_on_pass(cluster, dir, opts)
    |> act(cluster, dir, Keyword.get(opts, :halt_fun, &System.halt/1))
  end

  @typedoc "What `decide_seed/1` needs to know, read before the boot writes a sharded seed."
  @type seed_facts :: %{
          member_known?: boolean(),
          segment_dirs: [String.t()],
          adopt?: boolean(),
          adopted?: boolean()
        }

  @doc """
  The decision `check_seed/3` makes: a sharded seed over an unsharded member that ran here refuses
  (`{:refuse, :reshard_unsupported}`, whatever the operator adopts); over segment directories it refuses
  them as unknown, or adopts them when `adopt?`, unless `check_ring/4` already did; otherwise `:ok`.
  """
  @spec decide_seed(seed_facts()) ::
          :ok | {:adopt, [String.t()]} | {:refuse, :reshard_unsupported} | {:refuse, {:unknown_segments, [String.t()]}}
  def decide_seed(%{member_known?: true}), do: {:refuse, :reshard_unsupported}
  def decide_seed(%{segment_dirs: []}), do: :ok
  def decide_seed(%{adopted?: true}), do: :ok
  def decide_seed(%{segment_dirs: dirs, adopt?: true}), do: {:adopt, Enum.sort(dirs)}
  def decide_seed(%{segment_dirs: dirs}), do: {:refuse, {:unknown_segments, Enum.sort(dirs)}}

  @doc """
  Whether the ring store was ever started on this node: the fact `check_ring/4` needs, read before the
  boot starts the store and so registers it.
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

  defp act({:refuse, :reshard_unsupported}, cluster, _dir, halt_fun) do
    I18n.t(:data_dir_reshard_unsupported, cluster: cluster, node: node())
    |> StartupRefusal.refuse!(halt_fun)
  end

  defp act({:refuse, {:cluster_renamed, recorded}}, cluster, dir, halt_fun) do
    shown = if recorded == :unreadable, do: I18n.t(:data_dir_cluster_marker_unreadable), else: recorded

    I18n.t(:data_dir_cluster_renamed, path: dir, recorded: shown, cluster: cluster, node: node())
    |> StartupRefusal.refuse!(halt_fun)
  end

  defp act({:refuse, :resharded}, cluster, _dir, halt_fun) do
    I18n.t(:data_dir_resharded, cluster: cluster, node: node())
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
