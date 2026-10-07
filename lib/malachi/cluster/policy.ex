defmodule Malachi.Cluster.Policy do
  @moduledoc """
  A named storage policy: what an administrator says about a topic's segments, rather than something a
  vnode decides.

  A policy carries a retention override and a placement spread attribute, and a topic points at one by
  name. This module is only the **shape** and the rule for accepting one; where the definitions live is
  `Malachi.Cluster.PolicyRegistry`, and which topic points at which name stays in `Malachi.Metadata`,
  because that part is per-topic state and travels with the topic when it moves between vnodes.

  ## Why validation is here and not at the edge

  A policy that is stored is a policy every node will read, so the control plane is where the answer to
  "is this a policy" has to be the same on every replica. Keeping the predicate pure and in one module
  means the wire, a mix task and the replicated command cannot drift into three different opinions of
  what a policy is.

  What is refused, and why each one is worth refusing:

    * **An unknown key.** `%{retention_ms: 1000}` is a plausible typo for `:retention` and would be
      stored, read back, and silently do nothing forever. There is no valid reason to keep a key the
      code does not read.
    * **A bound that is not a non-negative integer.** `max_bytes: "10GB"` is the shape an operator
      reaches for first. `Malachi.Cluster.Retention` compares it with `>`, which never matches against
      a binary, so the rule would simply never fire.
    * **A negative bound.** An age or a byte budget below zero expires everything the rule can see,
      which is the opposite of what anyone typing a negative number wants.
    * **A bound below its field's floor.** `retention.segment_max_age_ms` rolls a topic's active
      segment, and a roll every few seconds would multiply the topic's segments for no retention a
      sweep running once a minute could deliver.

  `nil` is allowed for a bound and means that rule is off, which is how a policy overrides one of the
  two limits without inheriting the other.

  ## Which fields exist, and since when

  The settable fields are one table (`fields/0`), each with the machine version that introduced it, and
  validation reads that table at the group's effective version (`validate/3`). See the comment above
  the table for why a field is never added any other way.
  """

  alias Malachi.Cluster.MachineVersion

  @typedoc "A policy's name, as an administrator chose it."
  @type name :: String.t()

  @typedoc """
  A named storage policy: per-topic retention overrides and a placement spread attribute. Both keys are
  optional; a policy applies only the ones it sets, falling back to the global defaults otherwise.
  """
  @type t :: %{
          optional(:retention) => %{
            optional(:max_age_ms) => non_neg_integer() | nil,
            optional(:max_bytes) => non_neg_integer() | nil,
            optional(:segment_max_age_ms) => pos_integer() | nil
          },
          optional(:spread_by) => String.t() | nil
        }

  @typedoc """
  How a field's value is checked and carried: a `:bound` is a non-negative integer or `nil` (the rule is
  off); an `:attribute` is a non-empty string or `nil`.
  """
  @type field_type :: :bound | :attribute

  @typedoc """
  One settable field: its flat `name` (what the wire, the CLI and the dashboard say), its `path` in the
  policy map, its `type`, the machine version that introduced it (`since`), and, for a bound, the
  smallest value it accepts other than `nil` (`min`, default 0).
  """
  @type field :: %{
          required(:name) => String.t(),
          required(:path) => [atom(), ...],
          required(:type) => field_type(),
          required(:since) => non_neg_integer(),
          optional(:min) => non_neg_integer()
        }

  # Every field a policy can set, and the machine version that introduced each one.
  #
  # This table is the only thing that grows when a field is added (#199 `max_records`, #200's pin caps
  # and backlog quota, #201's default TTL, #206's cleanup mode), and every surface reads it: validation
  # here, the wire codec, the mix task and the dashboard. A new field goes in at the release's new
  # `Malachi.Cluster.MachineVersion.code_version/0`, raised in the same change. That is what keeps a
  # replay deterministic: `{:define_policy, name, policy}` is validated against the group's EFFECTIVE
  # version, so while any member still runs older code, every member refuses the new field alike,
  # instead of a newer member storing what an older one refuses. Widening `valid?` by hand, without a
  # row here, is exactly the divergence this prevents.
  @fields [
    %{name: "retention.max_age_ms", path: [:retention, :max_age_ms], type: :bound, since: 3},
    %{name: "retention.max_bytes", path: [:retention, :max_bytes], type: :bound, since: 3},
    # How old a topic's ACTIVE segment may get before it is rolled, so that a topic too quiet to fill a
    # segment still seals one and becomes visible to age retention (#197). Its floor is the retention
    # sweep's default cadence: the sweep is what asks for the roll, so a shorter interval would only be
    # rounded up to the next sweep, and every roll is one more segment in the metadata.
    %{
      name: "retention.segment_max_age_ms",
      path: [:retention, :segment_max_age_ms],
      type: :bound,
      since: 6,
      min: 60_000
    },
    %{name: "spread_by", path: [:spread_by], type: :attribute, since: 3}
  ]

  @doc "Every field a policy can set, in table order (see `t:field/0`)."
  @spec fields() :: [field()]
  def fields, do: @fields

  @doc "The field named `name` in `fields`, or `nil`."
  @spec field(String.t(), [field()]) :: field() | nil
  def field(name, fields \\ @fields), do: Enum.find(fields, &(&1.name == name))

  @doc """
  Whether `name` can name a policy: a non-empty binary.

  ## Examples

      iex> Malachi.Cluster.Policy.valid_name?("durable")
      true

      iex> Malachi.Cluster.Policy.valid_name?("")
      false

  """
  @spec valid_name?(term()) :: boolean()
  def valid_name?(name), do: is_binary(name) and name != ""

  @doc """
  Whether `policy` is a policy this build can act on: `validate/3` at `code_version/0`.

  ## Examples

      iex> Malachi.Cluster.Policy.valid?(%{retention: %{max_age_ms: 604_800_000}, spread_by: "rack"})
      true

      iex> Malachi.Cluster.Policy.valid?(%{retention: %{max_bytes: "10GB"}})
      false

      iex> Malachi.Cluster.Policy.valid?(%{retention_ms: 1_000})
      false

      iex> Malachi.Cluster.Policy.valid?(%{retention: %{max_bytes: nil}})
      true

      iex> Malachi.Cluster.Policy.valid?(%{spread_by: :rack})
      false

  """
  @spec valid?(term()) :: boolean()
  def valid?(policy), do: valid?(policy, MachineVersion.code_version())

  @doc "Whether `policy` is valid at machine `version`: `validate/3` answering `:ok`."
  @spec valid?(term(), non_neg_integer(), [field()]) :: boolean()
  def valid?(policy, version, fields \\ @fields), do: validate(policy, version, fields) == :ok

  @doc """
  Checks `policy` against the fields of `fields` introduced at or below `version`.

  Answers `{:error, {:unsupported_policy_field, name, since}}` when the policy sets a field this build
  knows but `version` does not yet admit, so a caller can say "finish the rolling upgrade" rather than
  "invalid", and `{:error, :invalid_policy}` for anything else that is not a policy: an unknown key, a
  bound that is neither `nil` nor a non-negative integer at or above its field's `min`, an attribute
  that is not a non-empty string or `nil`, or a value that is not a map where the table has a nested map.
  """
  @spec validate(term(), non_neg_integer(), [field()]) ::
          :ok | {:error, :invalid_policy | {:unsupported_policy_field, String.t(), non_neg_integer()}}
  def validate(policy, version, fields \\ @fields)

  def validate(policy, version, fields) when is_map(policy) do
    {admitted, later} = Enum.split_with(fields, &(&1.since <= version))

    case Enum.find(later, &set?(policy, &1.path)) do
      %{name: name, since: since} -> {:error, {:unsupported_policy_field, name, since}}
      nil -> if valid_level?(policy, Enum.map(admitted, &{&1.path, &1})), do: :ok, else: {:error, :invalid_policy}
    end
  end

  def validate(_policy, _version, _fields), do: {:error, :invalid_policy}

  # A policy map sets `path` when every step of it is a key of a map.
  defp set?(map, [key]) when is_map(map), do: Map.has_key?(map, key)
  defp set?(map, [key | rest]) when is_map(map), do: set?(Map.get(map, key), rest)
  defp set?(_not_a_map, _path), do: false

  # One level of the policy map against the admitted `{path, field}` entries: every key must head some
  # entry, a leaf must hold a valid value for its field, and a nested key must hold a map valid one level
  # down.
  defp valid_level?(map, entries) when is_map(map) do
    by_head =
      Enum.group_by(entries, fn {[head | _rest], _field} -> head end, fn {[_head | rest], field} -> {rest, field} end)

    Enum.all?(map, fn {key, value} ->
      case Map.fetch(by_head, key) do
        {:ok, [{[], field}]} -> valid_value?(field, value)
        {:ok, nested} -> valid_level?(value, nested)
        :error -> false
      end
    end)
  end

  defp valid_level?(_not_a_map, _entries), do: false

  # A bound is at most 2^64 - 1 because the wire carries it in 64 bits (`Malachi.Wire`), and a larger one
  # would be stored whole and read back truncated, telling an operator a small limit while the data lives
  # under a huge one. Refusing it here is a tightening of what `{:define_policy, name, policy}` accepts,
  # which is admissible only because no release ever emitted that command into either Raft group (the
  # store gained its first caller in the same change, #194; the legacy metadata command has none, #255).
  @max_bound 0xFFFF_FFFF_FFFF_FFFF

  # `nil` turns a rule off, whatever the field's type.
  defp valid_value?(_field, nil), do: true

  defp valid_value?(%{type: :bound} = field, value),
    do: is_integer(value) and value >= Map.get(field, :min, 0) and value <= @max_bound

  # The spread attribute is a KEY into the broker attributes, which arrive from the environment through
  # `Malachi.Application.parse_attributes/1` and are therefore keyed by string. An atom or a number
  # matches no broker, `Malachi.Cluster.Placement` puts every broker in the single nil domain, and the
  # result is a hard placement that answers `:insufficient_domains` or a soft one that quietly stops
  # spreading. Checked here rather than at the placement boundary so the store never holds a definition
  # that cannot do what it says.
  defp valid_value?(%{type: :attribute}, value), do: valid_name?(value)

  @doc """
  A topic's effective placement spread attribute and where it comes from: the policy's `:spread_by`
  when the policy sets the key (`nil` included, which turns spreading off for that topic), otherwise
  `global`. A topic with no policy, or bound to a name nothing defines, passes `nil` and gets `global`,
  which is what placement does (`Malachi.Broker`): placement fails open.

  ## Examples

      iex> Malachi.Cluster.Policy.effective_spread_by(%{spread_by: "rack"}, "dc")
      {"rack", :policy}

      iex> Malachi.Cluster.Policy.effective_spread_by(%{retention: %{}}, "dc")
      {"dc", :global}

  """
  @spec effective_spread_by(t() | nil, String.t() | nil) :: {String.t() | nil, :policy | :global}
  def effective_spread_by(%{spread_by: spread_by}, _global), do: {spread_by, :policy}
  def effective_spread_by(_no_spread_by, global), do: {global, :global}

  @doc """
  Builds a policy from flat `{name, value}` pairs, the form the wire, the CLI and the dashboard carry.

  A field absent from `pairs` is absent from the policy (it inherits the global value), `nil` turns its
  rule off, and `0` is a real budget: the three stay distinct. Refuses an unknown name
  (`{:unknown_policy_field, name}`), a value its type does not allow (`{:invalid_policy_field, name}`) and
  a name given twice (`{:duplicate_policy_field, name}`). Checks the table of this build only: whether the
  cluster's effective version admits each field is the store's decision (`validate/3`).
  """
  @spec from_pairs([{String.t(), term()}], [field()]) ::
          {:ok, t()}
          | {:error, {:unknown_policy_field | :invalid_policy_field | :duplicate_policy_field, String.t()}}
  def from_pairs(pairs, fields \\ @fields) do
    Enum.reduce_while(pairs, {:ok, %{}}, fn {name, value}, {:ok, policy} ->
      case field(name, fields) do
        nil ->
          {:halt, {:error, {:unknown_policy_field, name}}}

        %{path: path} = field ->
          cond do
            set?(policy, path) -> {:halt, {:error, {:duplicate_policy_field, name}}}
            not valid_value?(field, value) -> {:halt, {:error, {:invalid_policy_field, name}}}
            true -> {:cont, {:ok, put_path(policy, path, value)}}
          end
      end
    end)
  end

  defp put_path(map, [key], value), do: Map.put(map, key, value)
  defp put_path(map, [key | rest], value), do: Map.put(map, key, put_path(Map.get(map, key, %{}), rest, value))

  @doc "The fields `policy` sets, as `{name, value}` pairs in table order: the inverse of `from_pairs/2`."
  @spec to_pairs(t(), [field()]) :: [{String.t(), term()}]
  def to_pairs(policy, fields \\ @fields) do
    for %{name: name, path: path} <- fields, set?(policy, path), do: {name, get_in(policy, path)}
  end
end
