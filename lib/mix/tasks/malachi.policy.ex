defmodule Mix.Tasks.Malachi.Policy do
  @shortdoc "Manage storage policies and per-topic retention on a running node"

  @moduledoc """
  Administer **storage policies** on a **running** Malachi node from the box, over Erlang distribution
  (RPC): define and delete named policies, bind a topic to one, and read back what a topic's retention
  actually is. A policy lives in the cluster's replicated policy store and a binding in the topic's own
  metadata, so a change made through any node applies cluster-wide.

      mix malachi.policy list
      mix malachi.policy define <name> [--set <field>=<value>]... [--off <field>]...
      mix malachi.policy delete <name> [--force]
      mix malachi.policy bind <topic> <name>
      mix malachi.policy unbind <topic>
      mix malachi.policy get <topic>

  A field left out of `define` inherits the cluster's global value; `--off` turns that rule off for the
  policy's topics; `0` is a real budget (`--set retention.max_bytes=0` expires every sealed segment).
  The fields are `retention.max_age_ms`, `retention.max_bytes` (non-negative integers),
  `retention.segment_max_age_ms` (how old an active segment may get before it is rolled, at least 60000)
  and `spread_by` (a broker attribute key). `retention.segment_max_age_ms` needs control-plane machine
  version 6.

  `delete` refuses a policy a topic is still bound to, because such a topic stops expiring anything once
  the name no longer resolves; `--force` deletes it anyway. `bind` refuses a name the store does not
  define. `get` prints each bound's effective value and where it comes from (`policy`, `global`, or
  `unresolved_backstop` for a topic bound to a name nothing defines).

  Binding needs control-plane machine version 4: during a rolling upgrade it is refused until every node
  runs a release that implements it and the version pin is lifted.

  Options:

    * `--node`: the target node (default `$MALACHI_NODE` or `malachi@127.0.0.1`)
    * `--cookie`: the Erlang cookie (default `$RELEASE_COOKIE`, else `~/.erlang.cookie`)

  The target node must be **named** (a release, or `iex --name ... -S mix`).
  """
  use Mix.Task

  alias Malachi.CLI.Options
  alias Malachi.CLI.Rpc
  alias Malachi.Cluster.Policy
  alias Malachi.Policies

  @switches [node: :string, cookie: :string, set: :keep, off: :keep, force: :boolean]

  @impl Mix.Task
  def run(argv) do
    case Options.parse(argv, @switches) do
      {:ok, {opts, args}} -> connect_and_run(args, opts)
      {:error, message} -> Mix.raise(message <> "\n\n" <> usage())
    end
  end

  defp connect_and_run(args, opts) do
    node = Rpc.target_node(opts)

    case Rpc.connect(node, opts[:cookie]) do
      :ok -> report(execute(args, opts, Rpc.rpc(node)))
      {:error, message} -> Mix.raise(message)
    end
  end

  defp report({:ok, message}), do: Mix.shell().info(message)

  defp report({:error, message}) do
    Mix.shell().error(message)
    exit({:shutdown, 1})
  end

  @doc """
  The testable core: maps a parsed command to a `Malachi.Policies` call through `call` (a
  `(module, fun, args -> {:ok, value} | {:error, reason})` seam that in production is an RPC to the target
  node). Field values are parsed here, against this build's field table, so a typo is refused before
  anything is sent. Returns `{:ok, message}` / `{:error, message}` for the caller to print.
  """
  @spec execute([String.t()], keyword(), (module(), atom(), list() -> {:ok, term()} | {:error, term()})) ::
          {:ok, String.t()} | {:error, String.t()}
  def execute(["list"], _opts, call) do
    case call.(Policies, :list, []) do
      {:ok, {:ok, policies}} -> {:ok, format_policies(policies)}
      other -> finish(other, nil)
    end
  end

  def execute(["define", name], opts, call) do
    case pairs(opts) do
      {:ok, pairs} -> finish(call.(Policies, :define, [name, pairs, actor()]), "defined policy #{name}")
      {:error, message} -> {:error, message}
    end
  end

  def execute(["delete", name], opts, call) do
    finish(
      call.(Policies, :delete, [name, actor(), [force: Keyword.get(opts, :force, false)]]),
      "deleted policy #{name}"
    )
  end

  def execute(["bind", topic, name], _opts, call) do
    finish(call.(Policies, :bind, [topic, name, actor()]), "bound #{topic} to policy #{name}")
  end

  def execute(["unbind", topic], _opts, call) do
    finish(call.(Policies, :bind, [topic, nil, actor()]), "detached #{topic} from its policy")
  end

  def execute(["get", topic], _opts, call) do
    case call.(Policies, :topic_policy, [topic]) do
      {:ok, {:ok, topic_policy}} -> {:ok, format_topic_policy(topic_policy)}
      other -> finish(other, nil)
    end
  end

  def execute(_args, _opts, _call), do: {:error, usage()}

  # `--set field=value` and `--off field`, in the order given, as the flat pairs `Malachi.Policies`
  # takes. A field set twice is left for `Malachi.Cluster.Policy.from_pairs/2` to refuse by name.
  defp pairs(opts) do
    sets = for {:set, assignment} <- opts, do: {:set, assignment}
    offs = for {:off, name} <- opts, do: {:off, name}

    Enum.reduce_while(sets ++ offs, {:ok, []}, fn entry, {:ok, acc} ->
      case pair(entry) do
        {:ok, pair} -> {:cont, {:ok, acc ++ [pair]}}
        {:error, message} -> {:halt, {:error, message}}
      end
    end)
  end

  defp pair({:off, name}) do
    if Policy.field(name), do: {:ok, {name, nil}}, else: {:error, unknown_field(name)}
  end

  defp pair({:set, assignment}) do
    with [name, raw] <- String.split(assignment, "=", parts: 2),
         %{type: type} <- Policy.field(name) || {:unknown, name},
         {:ok, value} <- parse_value(type, raw) do
      {:ok, {name, value}}
    else
      {:unknown, name} -> {:error, unknown_field(name)}
      {:error, :value} -> {:error, "invalid value in --set #{assignment}"}
      _no_equals -> {:error, "--set takes <field>=<value>, got: #{assignment}"}
    end
  end

  defp parse_value(:bound, raw) do
    case Integer.parse(raw) do
      {value, ""} when value >= 0 -> {:ok, value}
      _not_a_bound -> {:error, :value}
    end
  end

  defp parse_value(:attribute, ""), do: {:error, :value}
  defp parse_value(:attribute, raw), do: {:ok, raw}

  defp unknown_field(name),
    do: "unknown policy field #{name} (known: #{Enum.map_join(Policy.fields(), ", ", & &1.name)})"

  defp actor, do: "cli@#{node()}"

  # `call` returns `{:ok, reply}` (`:ok` or `{:error, reason}`) or `{:error, rpc_reason}`.
  defp finish({:ok, :ok}, message), do: {:ok, message}
  defp finish({:ok, {:error, reason}}, _message), do: {:error, Policies.reason_string(reason)}
  defp finish({:error, reason}, _message), do: {:error, Rpc.rpc_error(reason)}

  defp format_policies(policies) when map_size(policies) == 0, do: "(no policies)"

  defp format_policies(policies) do
    policies
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map_join("\n", fn {name, policy} -> "#{name}\t#{format_fields(Policy.to_pairs(policy))}" end)
  end

  defp format_fields([]), do: "(inherits every global value)"
  defp format_fields(pairs), do: Enum.map_join(pairs, " ", fn {name, value} -> "#{name}=#{format_value(value)}" end)

  defp format_topic_policy(topic_policy) do
    binding =
      case topic_policy do
        %{policy: nil} -> "(none)"
        %{policy: name, resolution: :unresolved} -> "#{name} (undefined: this topic holds its data)"
        %{policy: name} -> name
      end

    lines =
      for {name, value, origin} <- Policies.effective_pairs(topic_policy),
          do: "#{name}\t#{format_value(value)}\t(#{origin})"

    Enum.join(["topic\t#{topic_policy.topic}", "policy\t#{binding}" | lines], "\n")
  end

  defp format_value(nil), do: "off"
  defp format_value(value), do: to_string(value)

  defp usage do
    """
    usage:
      mix malachi.policy list
      mix malachi.policy define <name> [--set <field>=<value>]... [--off <field>]...
      mix malachi.policy delete <name> [--force]
      mix malachi.policy bind <topic> <name>
      mix malachi.policy unbind <topic>
      mix malachi.policy get <topic>

    fields: #{Enum.map_join(Policy.fields(), ", ", & &1.name)}
    connection: --node (or $MALACHI_NODE, default malachi@127.0.0.1), --cookie (or $RELEASE_COOKIE)
    """
  end
end
