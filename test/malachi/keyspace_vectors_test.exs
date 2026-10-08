defmodule Malachi.KeyspaceVectorsTest do
  # The keyspace vectors the Node client's hash is checked against (scripts/lib/keyspace.js) were written
  # from the broker's own hash. This keeps them true to it: a change to how the broker places a key would
  # silently send every routing client's records to the wrong range.
  use ExUnit.Case, async: true

  @fixture "test/support/fixtures/wire/keyspace_vectors.json"
  @external_resource @fixture
  @vectors @fixture |> File.read!() |> Jason.decode!()

  test "every vector is where the broker places the key" do
    for %{"key" => key, "hash" => hash, "positions" => positions} <- @vectors["cases"] do
      assert :erlang.phash2(key, 4_294_967_296) == hash
      assert Enum.map(@vectors["sizes"], &Malachi.Keyspace.position_of(key, &1)) == positions
    end
  end

  test "the vectors cover a missing key, the empty key, every length of a hash block, and wide characters" do
    keys = Enum.map(@vectors["cases"], & &1["key"])
    assert nil in keys and "" in keys
    lengths = keys |> Enum.reject(&is_nil/1) |> Enum.map(&byte_size/1) |> MapSet.new()
    assert Enum.all?(1..24, &MapSet.member?(lengths, &1))
    assert Enum.any?(keys, &(is_binary(&1) and byte_size(&1) > String.length(&1)))
    assert 4_294_967_296 in @vectors["sizes"]
  end
end
