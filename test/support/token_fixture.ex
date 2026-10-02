defmodule Malachi.TokenFixture do
  @moduledoc """
  Reads a token file the way `Malachi.UI.TokenGen.Source` does, keeping declaration order, and edits
  it by path, so a test can start from a valid file and break exactly one thing.
  """

  alias Jason.OrderedObject

  @minimal "test/fixtures/tokens/minimal.json"
  @repo "docs/design/design-tokens.json"

  @doc "The small valid token file the generator tests start from."
  @spec minimal() :: OrderedObject.t()
  def minimal, do: read!(@minimal)

  @doc "The repository's own token file."
  @spec repo() :: OrderedObject.t()
  def repo, do: read!(@repo)

  @doc "Reads a token file with its objects in declaration order."
  @spec read!(Path.t()) :: OrderedObject.t()
  def read!(path), do: path |> File.read!() |> Jason.decode!(objects: :ordered_objects)

  @doc "Encodes a token file back to JSON."
  @spec encode!(OrderedObject.t()) :: String.t()
  def encode!(object), do: Jason.encode!(object, pretty: true)

  @doc "Sets the value at a path of keys, appending the key when the object lacks it."
  @spec put(OrderedObject.t(), [String.t()], term()) :: OrderedObject.t()
  def put(%OrderedObject{values: values} = object, [key], value) do
    if List.keymember?(values, key, 0) do
      %{object | values: List.keyreplace(values, key, 0, {key, value})}
    else
      %{object | values: values ++ [{key, value}]}
    end
  end

  def put(%OrderedObject{} = object, [key | rest], value) do
    put(object, [key], put(fetch!(object, [key]), rest, value))
  end

  @doc "Removes the key at a path."
  @spec delete(OrderedObject.t(), [String.t()]) :: OrderedObject.t()
  def delete(%OrderedObject{values: values} = object, [key]) do
    %{object | values: List.keydelete(values, key, 0)}
  end

  def delete(%OrderedObject{} = object, [key | rest]) do
    put(object, [key], delete(fetch!(object, [key]), rest))
  end

  @doc "Reads the value at a path of keys."
  @spec fetch!(OrderedObject.t(), [String.t()]) :: term()
  def fetch!(value, []), do: value

  def fetch!(%OrderedObject{values: values}, [key | rest]) do
    case List.keyfind(values, key, 0) do
      {^key, value} -> fetch!(value, rest)
      nil -> raise ArgumentError, "no key #{inspect(key)} in the token file"
    end
  end

  @doc "Builds an ordered object from a keyword-like list of string keys."
  @spec object([{String.t(), term()}]) :: OrderedObject.t()
  def object(pairs), do: %OrderedObject{values: pairs}
end
