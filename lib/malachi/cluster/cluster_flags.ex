defmodule Malachi.Cluster.ClusterFlags do
  @moduledoc """
  The pure state of the cluster's **feature flags**: the set of features an operator has switched on,
  replicated by `Malachi.Cluster.ClusterFlagsMachine` over a dedicated `ra` cluster, exactly as
  `Malachi.Cluster.Ring` sits behind `RingMachine` and `Malachi.Cluster.Lease` behind `LeaseMachine`.

  A flag is the operator's commitment that a format-changing or protocol-changing feature may start
  being used. It exists because finishing a rolling upgrade and committing to the new behaviour are two
  different moments: while every node runs the new build but no flag is on, rolling the build back is
  still free.

  ## A set that only grows

  There is no command to turn a flag off. Once a feature is on, peers and on-disk data may already be
  in the new shape, and going back is a migration, not a toggle. Being grow-only is what makes three
  other things safe and simple:

    * the local cache each node keeps may lag without any risk, because the worst a node that has not
      noticed yet can do is keep using the old behaviour, which every node still understands;
    * two operators racing the same flag converge, since `enable` is idempotent;
    * applying the log in any order reaches the same state, so a replay after a restart needs no care.

  ## Why the machine does not validate the name

  `apply/2` accepts any atom. Checking the name against a compile-time registry here would look
  tighter and would be a determinism bug: two members can sit at the same effective machine version on
  two different releases whose registries differ, and they would then disagree about whether the same
  command is valid, which is exactly the divergence the machine version exists to prevent. The name is
  validated by the caller, next to the capability check that actually gates the flip
  (`Malachi.Cluster.ClusterFlagsServer.enable/4`).

  ## Why this store and not the metadata

  `Malachi.Metadata` is sharded one Raft group per vnode, so a flag written there would be a per-vnode
  fact with no single source of truth, and the sharded write path routes by topic, which a command that
  names no topic cannot do. The ring store is not an option either: it is only formed on a clustered
  node, and a single-node deployment still has to be able to turn a format-changing feature on.
  """

  # A set held as a map of `flag => true`, the same shape `Malachi.Metadata` holds its migration fence
  # in. A plain map keeps the replicated state a canonical term with nothing opaque in it, which is what
  # a Raft log and its snapshots want.
  defstruct enabled: %{}

  @typedoc "The flags an operator has switched on."
  @type t :: %__MODULE__{enabled: %{optional(flag()) => true}}

  @typedoc "A flag, named after the capability every node must advertise for it (`Malachi.Cluster.Capabilities`)."
  @type flag :: atom()

  @typedoc """
  `{:enable_flag, flag}` switches `flag` on. Idempotent, and there is deliberately no command that
  switches one off.
  """
  @type command :: {:enable_flag, flag()}

  @type reply :: :ok

  @behaviour Malachi.Cluster.MachineVersion

  @doc "A store with no flag switched on (the state every cluster starts in)."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc "Every command shape, mapped to the machine version that introduced it (see `Malachi.Cluster.MachineVersion`)."
  @impl Malachi.Cluster.MachineVersion
  def command_versions, do: %{{:enable_flag, 2} => 2}

  @doc """
  Applies a `command`, returning `{new_state, reply}`. Deterministic: no clock, no randomness, no node
  identity, so every replica reaches the same state from the same log.

  `{:enable_flag, flag}` always answers `:ok`, whether or not the flag was already on. An operator who
  re-runs the command, and a coordinator retrying after a timeout it never saw the answer to, both get
  the same answer as the first caller.
  """
  @spec apply(t(), command()) :: {t(), reply()}
  def apply(%__MODULE__{} = state, {:enable_flag, flag}) when is_atom(flag) do
    {%{state | enabled: Map.put(state.enabled, flag, true)}, :ok}
  end

  @doc "Whether `flag` is switched on."
  @spec enabled?(t(), flag()) :: boolean()
  def enabled?(%__MODULE__{} = state, flag), do: Map.has_key?(state.enabled, flag)

  @doc "Every switched-on flag, sorted, so two reads of the same state compare equal."
  @spec enabled(t()) :: [flag()]
  def enabled(%__MODULE__{} = state), do: state.enabled |> Map.keys() |> Enum.sort()
end
