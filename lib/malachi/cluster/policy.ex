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

  `nil` is allowed for a bound and means that rule is off, which is how a policy overrides one of the
  two limits without inheriting the other.
  """

  @typedoc "A policy's name, as an administrator chose it."
  @type name :: String.t()

  @typedoc """
  A named storage policy: per-topic retention overrides and a placement spread attribute. Both keys are
  optional; a policy applies only the ones it sets, falling back to the global defaults otherwise.
  """
  @type t :: %{
          optional(:retention) => %{
            optional(:max_age_ms) => non_neg_integer() | nil,
            optional(:max_bytes) => non_neg_integer() | nil
          },
          optional(:spread_by) => String.t() | nil
        }

  @keys [:retention, :spread_by]
  @retention_keys [:max_age_ms, :max_bytes]

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
  Whether `policy` is a policy this code can act on.

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
  def valid?(policy) when is_map(policy) do
    known_keys?(policy, @keys) and valid_retention?(Map.get(policy, :retention, %{})) and
      valid_spread_by?(Map.get(policy, :spread_by))
  end

  def valid?(_policy), do: false

  defp valid_retention?(retention) when is_map(retention) do
    known_keys?(retention, @retention_keys) and
      Enum.all?(@retention_keys, &bound?(Map.get(retention, &1)))
  end

  defp valid_retention?(_retention), do: false

  # The spread attribute is a KEY into the broker attributes, which arrive from the environment through
  # `Malachi.Application.parse_attributes/1` and are therefore keyed by string. An atom or a number
  # matches no broker, `Malachi.Cluster.Placement` puts every broker in the single nil domain, and the
  # result is a hard placement that answers `:insufficient_domains` or a soft one that quietly stops
  # spreading. Checked here rather than at the placement boundary so the store never holds a definition
  # that cannot do what it says, and checked now because nothing emits `{:define_policy, name, policy}`
  # yet: once #194 opens a write path, tightening what an existing command accepts changes the result of
  # a log replay and needs a new command at a new machine version.
  defp valid_spread_by?(nil), do: true
  defp valid_spread_by?(spread_by), do: valid_name?(spread_by)

  defp known_keys?(map, keys), do: map |> Map.keys() |> Enum.all?(&(&1 in keys))

  defp bound?(nil), do: true
  defp bound?(value), do: is_integer(value) and value >= 0
end
