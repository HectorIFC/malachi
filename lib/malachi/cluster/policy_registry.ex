defmodule Malachi.Cluster.PolicyRegistry do
  @moduledoc """
  The pure state of the cluster's **storage policies**: the named definitions an administrator writes,
  replicated by `Malachi.Cluster.PolicyMachine` over a dedicated `ra` cluster.

  ## Why they do not live in the metadata

  A policy is an administrative object bound to topics, not state a vnode owns. Malachi used to keep the
  definitions inside each vnode's `Malachi.Metadata`, which had a defect built into it: a topic moving
  between vnodes carries the policy NAME it points at, and the definition stayed behind, so after a vnode
  split the topic silently fell back to the global retention and lost its placement spread. One
  definition for the cluster removes that by construction rather than by copying it around.

  What stays in `Malachi.Metadata` is the **binding**, `{:bind_topic_policy, topic, name}`: which policy a
  topic points at is a fact about that topic and travels with it.

  A definition is validated by `Malachi.Cluster.Policy` before it is stored, so every replica agrees on
  what a policy is. Pure: no clock, no config.
  """

  defstruct policies: %{}

  alias Malachi.Cluster.Policy

  @type t :: %__MODULE__{policies: %{Policy.name() => Policy.t()}}

  @type command :: {:define_policy, Policy.name(), Policy.t()} | {:delete_policy, Policy.name()}

  @behaviour Malachi.Cluster.MachineVersion

  @doc "An empty registry."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc "Every command shape, mapped to the machine version that introduced it (see `Malachi.Cluster.MachineVersion`)."
  @impl Malachi.Cluster.MachineVersion
  def command_versions, do: %{{:define_policy, 3} => 3, {:delete_policy, 2} => 3}

  @doc """
  Applies a `command` at the group's effective machine `version`, returning `{new_state, reply}`.

  `:define_policy` is last-write-wins on a name, which is what an administrator editing a policy means.
  The definition is validated against the fields `version` admits (`Malachi.Cluster.Policy.validate/3`),
  never against what this build knows: `version` is replicated, so every member reaches the same answer
  while some still run older code. A field above it is refused as
  `{:unsupported_policy_field, name, since, version}`, anything else that is not a policy as
  `:invalid_policy`.

  `:delete_policy` is idempotent and does **not** look for topics pointing at the name: a topic whose
  policy is undefined holds its data (see `Malachi.Cluster.Retention.effective/4`), and making the delete
  conditional on a scan of every vnode's topics is not something this registry can do. The admin facade
  checks for bound topics before it submits one (`Malachi.Policies.delete/3`).
  """
  @spec apply(t(), command(), non_neg_integer()) :: {t(), term()}
  def apply(%__MODULE__{} = state, {:define_policy, name, policy}, version) do
    case {Policy.valid_name?(name), Policy.validate(policy, version)} do
      {true, :ok} ->
        {%{state | policies: Map.put(state.policies, name, policy)}, :ok}

      {true, {:error, {:unsupported_policy_field, field, since}}} ->
        {state, {:error, {:unsupported_policy_field, field, since, version}}}

      _invalid ->
        {state, {:error, :invalid_policy}}
    end
  end

  def apply(%__MODULE__{} = state, {:delete_policy, name}, _version) do
    {%{state | policies: Map.delete(state.policies, name)}, :ok}
  end

  # Defensive catch-all for callers outside ra (tests, direct use): an unknown command must not raise.
  # Inside ra, `Malachi.Cluster.MachineVersion` refuses it before it gets here.
  def apply(%__MODULE__{} = state, _unknown_command, _version), do: {state, {:error, :unknown_command}}

  @doc "The policy named `name`, or `nil` when no such policy is defined."
  @spec get(t(), Policy.name()) :: Policy.t() | nil
  def get(%__MODULE__{policies: policies}, name) when is_binary(name), do: Map.get(policies, name)
  def get(%__MODULE__{}, _no_name), do: nil

  @doc "Every definition, as a map from name to policy. What a retention sweep resolves against once per pass."
  @spec all(t()) :: %{Policy.name() => Policy.t()}
  def all(%__MODULE__{policies: policies}), do: policies
end
