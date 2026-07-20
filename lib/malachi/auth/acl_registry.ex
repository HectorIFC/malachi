defmodule Malachi.Auth.AclRegistry do
  @moduledoc """
  The pure state of **per-topic ACLs** (P5, decision 5-1A/2A/3A): allow-only grants of an operation on a
  topic resource, keyed by username. The deterministic core replicated by `Malachi.Auth.AclMachine` over a
  dedicated `ra` cluster, mirroring `Malachi.Auth.UserRegistry`.

  A grant is `{username, operation, resource}` where `operation` is `:produce` or `:consume` and `resource`
  is `{:literal, topic}` (an exact topic) or `{:prefix, prefix}` (any topic starting with `prefix`, the
  Kafka prefixed-ACL model). Grants only **allow**: there are no deny rules; absence is denial (enforced in
  strict mode by `Malachi.Auth.Authorization`). Pure: no clock, no config.
  """

  defstruct grants: %{}

  @type operation :: :produce | :consume
  @type resource :: {:literal, String.t()} | {:prefix, String.t()}
  @type grant :: {String.t(), operation(), resource()}
  @type t :: %__MODULE__{grants: %{String.t() => MapSet.t({operation(), resource()})}}

  @type command ::
          {:grant, String.t(), operation(), resource()}
          | {:revoke, String.t(), operation(), resource()}
          | {:revoke_user, String.t()}

  @doc "An empty registry."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc "Applies a `command`, returning `{new_state, reply}`. Deterministic; ACLs need no time input."
  @spec apply(t(), command()) :: {t(), term()}
  def apply(%__MODULE__{} = state, {:grant, username, operation, resource}) do
    grants =
      Map.update(state.grants, username, MapSet.new([{operation, resource}]), &MapSet.put(&1, {operation, resource}))

    {%{state | grants: grants}, :ok}
  end

  def apply(%__MODULE__{} = state, {:revoke, username, operation, resource}) do
    grants =
      case Map.get(state.grants, username) do
        nil ->
          state.grants

        set ->
          remaining = MapSet.delete(set, {operation, resource})
          if MapSet.size(remaining) == 0, do: Map.delete(state.grants, username), else: Map.put(state.grants, username, remaining)
      end

    {%{state | grants: grants}, :ok}
  end

  def apply(%__MODULE__{} = state, {:revoke_user, username}) do
    {%{state | grants: Map.delete(state.grants, username)}, :ok}
  end

  # Defensive catch-all: an unknown command must not crash a replica (rolling upgrade safety).
  def apply(%__MODULE__{} = state, _unknown_command), do: {state, {:error, :unknown_command}}

  @doc "Whether `username` has a grant for `operation` on `topic` (a matching literal or prefix resource)."
  @spec authorized?(t(), String.t(), operation(), String.t()) :: boolean()
  def authorized?(%__MODULE__{grants: grants}, username, operation, topic) do
    case Map.get(grants, username) do
      nil -> false
      set -> Enum.any?(set, fn {op, resource} -> op == operation and match_resource?(resource, topic) end)
    end
  end

  @doc "The grants for `username` as `{operation, resource}` (empty if none)."
  @spec list_grants(t(), String.t()) :: [{operation(), resource()}]
  def list_grants(%__MODULE__{grants: grants}, username) do
    case Map.get(grants, username) do
      nil -> []
      set -> MapSet.to_list(set)
    end
  end

  @doc "Every grant across all users as `{username, operation, resource}`."
  @spec list_all(t()) :: [grant()]
  def list_all(%__MODULE__{grants: grants}) do
    for {username, set} <- grants, {operation, resource} <- set, do: {username, operation, resource}
  end

  @doc """
  Parses a resource pattern string into a `resource`: a trailing `*` marks a **prefix** (`\"orders.*\"` →
  `{:prefix, \"orders.\"}`, `\"*\"` → `{:prefix, \"\"}` = all topics), otherwise a **literal** exact topic.
  """
  @spec parse_resource(String.t()) :: resource()
  def parse_resource(pattern) when is_binary(pattern) do
    if String.ends_with?(pattern, "*") do
      {:prefix, binary_part(pattern, 0, byte_size(pattern) - 1)}
    else
      {:literal, pattern}
    end
  end

  @doc "Renders a `resource` back to its pattern string (inverse of `parse_resource/1`)."
  @spec render_resource(resource()) :: String.t()
  def render_resource({:literal, topic}), do: topic
  def render_resource({:prefix, prefix}), do: prefix <> "*"

  # --- internals ---

  defp match_resource?({:literal, topic}, topic), do: true
  defp match_resource?({:literal, _other}, _topic), do: false
  defp match_resource?({:prefix, prefix}, topic), do: String.starts_with?(topic, prefix)
end
