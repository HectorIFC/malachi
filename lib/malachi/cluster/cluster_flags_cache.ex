defmodule Malachi.Cluster.ClusterFlagsCache do
  @moduledoc """
  This node's local view of the cluster's feature flags: the read every feature on a hot path uses, the
  step that adopts a newly enabled flag, and the gate that stops a node which cannot honour one.

  ## Why a cache at all

  A produce or a replication append cannot ask Raft whether a flag is on. So the flags live in
  `:persistent_term`, which is read without copying and is the primitive this repo already uses for
  cluster state that is read constantly and written almost never (`Malachi.Consumer.CoordinatorRouter`).

  ## Why a lagging cache is safe

  Because a flag only ever goes from off to on, and it only goes on once **every** node advertises the
  capability it names. A node that has not yet noticed a flip keeps to the old behaviour, which every
  node in the cluster still understands, and a node that has noticed it produces the new behaviour,
  which every node can already handle. Neither direction can surprise a peer. That is the whole
  argument for refreshing on a 30 second tick instead of reading Raft per batch, and it is why
  `Malachi.Cluster.ClusterFlags` has no command to switch a flag off.

  ## Why the write is conditional

  `:persistent_term.put/2` triggers a global garbage-collection scan of every process in the VM. Writing
  on every tick would make each node pay that twice a minute, forever, to store a value that changes
  once per release. `refresh/1` writes only when the value actually changed.

  ## Order of adoption

  A flag's local side effect runs **before** the flag is published to the cache. #207 raises the
  data-directory format marker when its flag flips, and the marker has to be up before the first byte of
  the new format is written; publishing first would leave a window where this node believes the feature
  is on and its own directory still says otherwise. If the side effect raises, nothing is published and
  the next tick tries again.

  ## The gate

  A flag this node's build does not advertise cannot be honoured, and the flag will never be switched
  off. Serving anyway means writing data the cluster expects to be in another shape. So the node refuses
  to run, through `Malachi.StartupRefusal`, with the same exit status 78 an unreadable data directory
  uses. In practice this can only be reached by a node restarting on an older build after the flip, since
  the flip itself required every node to advertise the capability.

  A store that cannot be read yet (not formed, no quorum, this node still joining) never refuses and
  never publishes: there is no answer, and treating "I could not ask" as "no flags" is precisely the
  mistake the durable ring boot exists to avoid.
  """

  require Logger

  alias Malachi.Cluster.Capabilities
  alias Malachi.Cluster.ClusterFlags
  alias Malachi.I18n
  alias Malachi.StartupRefusal

  @key {__MODULE__, :enabled}

  # Never published, so it tells "this node has not read the store yet" apart from "the store answered
  # and no flag is on". The first read has to be consistent, and after that the local replica is enough.
  @unread :unread

  @doc """
  Whether `flag` is switched on, as far as this node has seen. The hot-path read: a `:persistent_term`
  lookup and a list membership test over a list that is almost always empty.
  """
  @spec enabled?(ClusterFlags.flag()) :: boolean()
  def enabled?(flag), do: flag in enabled()

  @doc "Every flag this node has seen switched on, sorted. `[]` before the store has been read."
  @spec enabled() :: [ClusterFlags.flag()]
  def enabled do
    case :persistent_term.get(@key, @unread) do
      @unread -> []
      flags -> flags
    end
  end

  @doc """
  One pass over the flag store: read, refuse if this build cannot honour what is on, adopt what is new,
  and publish.

  Called from the flag store's reconciler tick, so it runs on every node in every mode, at boot and for
  as long as the node lives.

  Options, all seams so the pass is testable without a cluster or a live VM to halt:

    * `:read` (required) - `(mode -> {:ok, ClusterFlags.t()} | {:error, reason})`, normally
      `Malachi.Cluster.ClusterFlagsServer.read/2` bound to this node's member;
    * `:advertised` - what this build can honour (default `Capabilities.advertised/0`);
    * `:adopt` - `(flag -> any)`, the local side effect of a newly enabled flag (default none);
    * `:halt_fun` - how a refusal stops the node (default `System.halt/1`).
  """
  @spec refresh(keyword()) :: :ok
  def refresh(opts) do
    read = Keyword.fetch!(opts, :read)

    case read.(mode()) do
      {:ok, %ClusterFlags{} = flags} -> apply_flags(ClusterFlags.enabled(flags), opts)
      # Not formed, no quorum, or this node is still joining: no answer is not an answer.
      {:error, _unreadable} -> :ok
    end
  end

  @doc false
  # Publishes `enabled`, and only when it differs from what is already there, answering which of the two
  # happened. Public (and documented false) so the conditional write can be asserted directly: a
  # persistent_term write is invisible from outside, and writing on every tick is a regression no
  # behavioural test would catch.
  @spec put([ClusterFlags.flag()]) :: :published | :unchanged
  def put(enabled) do
    if :persistent_term.get(@key, @unread) == enabled do
      :unchanged
    else
      :persistent_term.put(@key, enabled)
      :published
    end
  end

  @doc false
  # Drops the published value, so the next refresh reads consistently again. For tests, which share one
  # VM and therefore one persistent_term table across cases.
  @spec forget() :: :ok
  def forget do
    _ = :persistent_term.erase(@key)
    :ok
  end

  defp mode do
    case :persistent_term.get(@key, @unread) do
      @unread -> :consistent
      _published -> :local
    end
  end

  defp apply_flags(enabled, opts) do
    advertised = Keyword.get(opts, :advertised, Capabilities.advertised())

    case Enum.reject(enabled, &(&1 in advertised)) do
      [] -> adopt_and_publish(enabled, opts)
      missing -> refuse(missing, advertised, Keyword.get(opts, :halt_fun, &System.halt/1))
    end
  end

  # The side effect first, the publication second: a flag is only true here once this node has actually
  # done what the flag means locally.
  defp adopt_and_publish(enabled, opts) do
    adopt = Keyword.get(opts, :adopt, fn _flag -> :ok end)

    Enum.each(enabled -- enabled(), fn flag ->
      adopt.(flag)
      Logger.info(I18n.t(:cluster_flag_adopted, flag: flag))
    end)

    _ = put(enabled)
    :ok
  end

  defp refuse(missing, advertised, halt_fun) do
    detail =
      I18n.t(:cluster_flag_missing_capability,
        flags: Enum.map_join(missing, ", ", &to_string/1),
        capabilities: inspect(advertised)
      )

    StartupRefusal.refuse!(detail, halt_fun)
  end
end
