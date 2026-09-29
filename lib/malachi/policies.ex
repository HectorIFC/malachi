defmodule Malachi.Policies do
  @moduledoc """
  The administrative entry point for storage policies: define and delete a named policy, bind a topic
  to one, and read back what a topic's retention actually is. The wire keys, `mix malachi.policy` and the
  dashboard are thin adapters over this module, so validation, the error vocabulary, the audit trail and
  the log line are written once.

  ## Where each half lives

  A policy's definition is one object for the cluster, in `Malachi.Cluster.PolicyStore`. Which policy a
  topic uses is that topic's own state, in its vnode's `Malachi.Metadata`
  (`{:bind_topic_policy, topic, name}`), so it travels with the topic when a vnode split moves it.

  ## The checks a replicated state machine cannot make

  Neither side can see the other: a metadata vnode cannot read the policy store, and the store cannot
  read the topics. So this module checks what they cannot, before it submits:

    * `bind/3` refuses a name the store does not define (`:no_such_policy`). A topic bound to an
      undefined name holds its data: nothing expires under it (`Malachi.Cluster.Retention.effective/4`).
    * `delete/3` refuses a policy some topic is still bound to (`{:policy_in_use, topics}`) unless it is
      given `force: true`, for the same reason.

  The delete asks the vnodes themselves, read linearizably, and refuses (`{:bindings_unavailable, _}`)
  when one of them does not answer or a split is moving topics between them. The bind reads this node's
  replica of the store. Both are still checks made before a submit, so a bind racing a delete can leave
  a topic bound to a name that is gone. That topic then shows up in `malachi_retention_unresolved_policy_sweeps_total` and in
  `topic_policy/1` as `resolution: :unresolved`, and the operator's backstop bound
  (`MALACHI_RETENTION_UNRESOLVED_POLICY_MAX_AGE_MS`) is the only one that applies to it.

  ## Upgrades

  Binding needs machine version 4. Until every member of a topic's metadata group runs a release that
  implements it and the operator has lifted the version pin, the control plane refuses it identically on
  every member with `{:unsupported_command, key, 4, effective}`, which `reason_string/1` turns into
  "finish the rolling upgrade". Defining and deleting policies need version 3 only.
  """

  require Logger

  alias Malachi.AuditLog
  alias Malachi.BrokerServer
  alias Malachi.Cluster.MachineVersion
  alias Malachi.Cluster.Policy
  alias Malachi.Cluster.PolicyStore
  alias Malachi.Cluster.Retention
  alias Malachi.DataPlaneRouter
  alias Malachi.I18n

  @typedoc "Who asked for a change, as the audit log records it: a username, or `cli@<node>`."
  @type actor :: String.t()

  @typedoc "A topic's policy as read back by `topic_policy/1`."
  @type topic_policy :: %{
          topic: String.t(),
          policy: Policy.name() | nil,
          definition: Policy.t() | nil,
          resolution: Retention.resolution(),
          retention: %{atom() => {non_neg_integer() | nil, Retention.origin()}},
          spread_by: {String.t() | nil, :policy | :global}
        }

  @typedoc "Why a request was refused. `reason_string/1` renders every one of them."
  @type reason ::
          :invalid_policy_name
          | :invalid_topic
          | :invalid_policy
          | :no_such_policy
          | :no_such_topic
          | :migrating
          | {:policy_in_use, [String.t()]}
          | {:bindings_unavailable, term()}
          | {:unknown_policy_field | :invalid_policy_field | :duplicate_policy_field, String.t()}
          | {:unsupported_policy_field, String.t(), non_neg_integer(), non_neg_integer()}
          | {:unsupported_command, term(), non_neg_integer(), non_neg_integer()}
          | term()

  @doc """
  Defines, or replaces, the policy `name`.

  `policy` is either a policy map or the flat `{field, value}` pairs every surface carries
  (`Malachi.Cluster.Policy.from_pairs/2`). The store validates the definition against the cluster's
  effective machine version.
  """
  @spec define(Policy.name(), Policy.t() | [{String.t(), term()}], actor()) :: :ok | {:error, reason()}
  def define(name, policy, actor) do
    result =
      with :ok <- check_name(name),
           {:ok, policy} <- to_policy(policy) do
        PolicyStore.define(name, policy)
      end

    audit(:policy_defined, actor, "define_policy", result, %{policy: name})
    log(result, :policy_defined, policy: name, actor: actor)
    result
  end

  @doc """
  Deletes the policy `name`. Refuses with `{:policy_in_use, topics}` while any topic is bound to it,
  unless `opts` has `force: true`. Deleting a name nothing defines is `:ok`.
  """
  @spec delete(Policy.name(), actor(), keyword()) :: :ok | {:error, reason()}
  def delete(name, actor, opts \\ []) do
    force = Keyword.get(opts, :force, false)

    result =
      with :ok <- check_name(name),
           :ok <- check_unbound(name, force) do
        PolicyStore.delete(name)
      end

    audit(:policy_deleted, actor, "delete_policy", result, %{policy: name, force: force})
    log(result, :policy_deleted, policy: name, actor: actor)
    result
  end

  @doc "Every definition, as a map from name to policy, read from this node's replica of the store."
  @spec list() :: {:ok, %{Policy.name() => Policy.t()}} | {:error, reason()}
  def list, do: PolicyStore.fetch_all()

  @doc """
  Binds `topic` to the policy `name`, or detaches it with `nil`. Refuses a name the store does not
  define, so a typo is reported here rather than holding the topic's data forever.
  """
  @spec bind(String.t(), Policy.name() | nil, actor()) :: :ok | {:error, reason()}
  def bind(topic, name, actor) do
    result =
      with :ok <- check_topic(topic),
           :ok <- check_defined(name) do
        broker_call(fn -> BrokerServer.bind_topic_policy(DataPlaneRouter.shard_for(topic), topic, name) end)
      end

    audit(:topic_policy_bound, actor, "bind_topic_policy", result, %{topic: topic, policy: name})

    log(result, if(name, do: :topic_policy_bound, else: :topic_policy_unbound),
      topic: topic,
      policy: name,
      actor: actor
    )

    result
  end

  @doc """
  What `topic`'s retention and placement spread actually are, and where each value comes from.

  The retention bounds are computed by `Malachi.Cluster.Retention.effective/4`, the function the sweep
  applies, over this node's configured global limits and its replica of the policy store, so what this
  reports is what the data sees. A store that cannot be read is an error here, never an empty store:
  answering "no policies" would report the global limits for a topic whose policy holds its data longer.
  """
  @spec topic_policy(String.t()) :: {:ok, topic_policy()} | {:error, reason()}
  def topic_policy(topic) do
    with :ok <- check_topic(topic),
         {:ok, name} <- broker_call(fn -> BrokerServer.topic_policy_name(DataPlaneRouter.shard_for(topic), topic) end),
         {:ok, policies} <- PolicyStore.fetch_all() do
      definition = if name, do: Map.get(policies, name)
      unresolved_backstop = Application.get_env(:malachi, :retention_unresolved_policy_max_age_ms)
      effective = Retention.effective(name, policies, Malachi.Application.retention_policy(), unresolved_backstop)

      {:ok,
       %{
         topic: topic,
         policy: name,
         definition: definition,
         resolution: effective.resolution,
         retention: effective.retention,
         spread_by: Policy.effective_spread_by(definition, Application.get_env(:malachi, :log_spread_by))
       }}
    end
  end

  @doc """
  The effective value of every field `Malachi.Cluster.Policy.fields/0` lists, as `{name, value, origin}`
  in table order: the flat form a surface prints or encodes. A field with no effective value to report
  is left out.
  """
  @spec effective_pairs(topic_policy()) :: [{String.t(), term(), atom()}]
  def effective_pairs(%{} = topic_policy) do
    effective = Map.take(topic_policy, [:retention, :spread_by])

    for %{name: name, path: path} <- Policy.fields(), {value, origin} <- [get_in(effective, path)] do
      {name, value, origin}
    end
  end

  @doc """
  A stable, human-readable rendering of a refusal, the same on the wire, in the CLI and on the dashboard.
  It starts with the reason's name, so a client can still branch on it.

  ## Examples

      iex> Malachi.Policies.reason_string({:policy_in_use, ["audit", "events"]})
      "policy_in_use: audit, events"

      iex> Malachi.Policies.reason_string(:no_such_policy)
      "no_such_policy"

  """
  @spec reason_string(reason()) :: String.t()
  def reason_string({:policy_in_use, topics}), do: "policy_in_use: " <> Enum.join(topics, ", ")

  def reason_string({:bindings_unavailable, reason}),
    do:
      "bindings_unavailable: the vnodes could not all be asked which topics use this policy (#{inspect(reason)}); retry"

  def reason_string({field_reason, name})
      when field_reason in [:unknown_policy_field, :invalid_policy_field, :duplicate_policy_field],
      do: "#{field_reason}: #{name}"

  def reason_string({:unsupported_policy_field, name, since, effective}),
    do: "unsupported_policy_field: #{name}: " <> MachineVersion.upgrade_pending_message(since, effective)

  def reason_string({:unsupported_command, _key, introduced, effective}),
    do: "unsupported_command: " <> MachineVersion.upgrade_pending_message(introduced, effective)

  def reason_string(:timeout),
    do: "timeout: the control plane did not answer in time; the change may still apply, read it back before retrying"

  def reason_string(reason) when is_atom(reason), do: Atom.to_string(reason)
  def reason_string(reason), do: inspect(reason)

  defp check_name(name), do: if(Policy.valid_name?(name), do: :ok, else: {:error, :invalid_policy_name})

  defp check_topic(topic), do: if(is_binary(topic) and topic != "", do: :ok, else: {:error, :invalid_topic})

  defp to_policy(pairs) when is_list(pairs), do: Policy.from_pairs(pairs)
  defp to_policy(policy) when is_map(policy), do: {:ok, policy}
  defp to_policy(_policy), do: {:error, :invalid_policy}

  # Detaching needs no definition. A name that cannot name a policy is refused before the store is read.
  defp check_defined(nil), do: :ok

  defp check_defined(name) do
    with :ok <- check_name(name),
         {:ok, policies} <- PolicyStore.fetch_all() do
      if Map.has_key?(policies, name), do: :ok, else: {:error, :no_such_policy}
    end
  end

  defp check_unbound(_name, true), do: :ok

  # Asked of the vnodes that hold the bindings, never of this node's cache (`Malachi.Application.bound_topics/1`).
  # A vnode that did not answer, or a split in progress, refuses the delete: the answer that would have
  # cleared it is the one that could not be read.
  defp check_unbound(name, false) do
    case Malachi.Application.bound_topics(name) do
      {:ok, []} -> :ok
      {:ok, topics} -> {:error, {:policy_in_use, topics}}
      {:error, reason} -> {:error, {:bindings_unavailable, reason}}
    end
  end

  # A broker that does not answer is an error to report and audit, never an exit that takes the caller
  # (a client connection, a dashboard request) down with it. A timed out bind may still commit: the
  # control plane answered nobody, and the operator is told to read the binding back.
  defp broker_call(fun) do
    fun.()
  catch
    :exit, {:timeout, _call} -> {:error, :timeout}
    :exit, reason -> {:error, {:broker_unavailable, reason}}
  end

  defp audit(event, actor, action, result, metadata) do
    {status, metadata} =
      case result do
        :ok -> {:success, metadata}
        {:error, reason} -> {:failure, Map.put(metadata, :reason, reason_string(reason))}
      end

    AuditLog.log_event(event, %{username: actor}, action, status, metadata)
  end

  defp log(:ok, key, bindings), do: Logger.info(I18n.t(key, bindings))
  defp log({:error, _reason}, _key, _bindings), do: :ok
end
