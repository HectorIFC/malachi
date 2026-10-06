defmodule Malachi.Auth.ConsoleRole do
  @moduledoc """
  The three **console roles**, the one place they are listed. A console role says what an operator may do
  through the dashboard and the console, and nothing about the wire protocol: the wire permissions
  (`:admin`, `:produce`, `:consume`) stay what they are, and holding one grants no console role except
  `:admin`, the superuser (`Malachi.Auth.Authorization.superuser?/1`).

  The roles are strictly nested, as in the reference product the specification studied (section 6.10.1 of
  `docs/design/operator-interfaces.md`): `:viewer` reads, `:editor` adds the mutations that are not
  security, and `:admin` adds users, ACLs and diagnostics. They are cluster wide and stored in the
  replicated user registry (`Malachi.Auth.UserRegistry`). What each role may reach is decided by
  `Malachi.Console.Access`.
  """

  @type t :: :viewer | :editor | :admin

  @roles [:viewer, :editor, :admin]

  @doc "Every role, from the least to the most privileged."
  @spec all() :: [t()]
  def all, do: @roles

  @doc "Whether `value` is a role, or `nil` (no role)."
  @spec valid?(term()) :: boolean()
  def valid?(value), do: value in [nil | @roles]

  @doc """
  Parses a role **string** into its atom, or `:error`. `"none"` and `nil` mean no role. Mapping explicitly
  (rather than `String.to_atom/1`) keeps an untrusted client from exhausting the atom table.

  ## Examples

      iex> Malachi.Auth.ConsoleRole.parse("editor")
      {:ok, :editor}

      iex> Malachi.Auth.ConsoleRole.parse("none")
      {:ok, nil}

      iex> Malachi.Auth.ConsoleRole.parse("root")
      :error

  """
  @spec parse(String.t() | nil) :: {:ok, t() | nil} | :error
  def parse("viewer"), do: {:ok, :viewer}
  def parse("editor"), do: {:ok, :editor}
  def parse("admin"), do: {:ok, :admin}
  def parse("none"), do: {:ok, nil}
  def parse(nil), do: {:ok, nil}
  def parse(_other), do: :error

  @doc """
  Whether `role` includes everything `required` grants. No role includes nothing.

  ## Examples

      iex> Malachi.Auth.ConsoleRole.includes?(:editor, :viewer)
      true

      iex> Malachi.Auth.ConsoleRole.includes?(:viewer, :editor)
      false

      iex> Malachi.Auth.ConsoleRole.includes?(nil, :viewer)
      false

  """
  @spec includes?(t() | nil, t()) :: boolean()
  def includes?(nil, _required), do: false
  def includes?(role, required), do: rank(role) >= rank(required)

  @doc """
  The more privileged of two roles, either of which may be `nil`.

  ## Examples

      iex> Malachi.Auth.ConsoleRole.max(:viewer, :admin)
      :admin

      iex> Malachi.Auth.ConsoleRole.max(nil, :viewer)
      :viewer

  """
  @spec max(t() | nil, t() | nil) :: t() | nil
  def max(nil, role), do: role
  def max(role, nil), do: role
  def max(a, b), do: if(rank(a) >= rank(b), do: a, else: b)

  defp rank(role), do: Enum.find_index(@roles, &(&1 == role))
end
