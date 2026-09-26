defmodule Malachi.Cluster.PolicyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Cluster.Policy

  doctest Malachi.Cluster.Policy

  describe "valid_name?/1" do
    test "a name is a non-empty binary" do
      assert Policy.valid_name?("durable")
      refute Policy.valid_name?("")
      refute Policy.valid_name?(nil)
      refute Policy.valid_name?(:durable)
    end
  end

  describe "valid?/1" do
    test "a policy may set either key, both, or neither" do
      assert Policy.valid?(%{})
      assert Policy.valid?(%{spread_by: "rack"})
      assert Policy.valid?(%{retention: %{max_age_ms: 1_000}})
      assert Policy.valid?(%{retention: %{max_age_ms: 1_000, max_bytes: 10}, spread_by: nil})
    end

    test "a bound is a non-negative integer, or nil to turn that rule off" do
      assert Policy.valid?(%{retention: %{max_bytes: 0}})
      assert Policy.valid?(%{retention: %{max_bytes: nil, max_age_ms: nil}})
      refute Policy.valid?(%{retention: %{max_bytes: -1}})
      refute Policy.valid?(%{retention: %{max_age_ms: 1.5}})
      refute Policy.valid?(%{retention: %{max_age_ms: "7d"}})
    end

    test "an unknown key is refused rather than kept and never read" do
      refute Policy.valid?(%{retention_ms: 1_000})
      refute Policy.valid?(%{retention: %{max_age_ms: 1_000}, compaction: true})
      refute Policy.valid?(%{retention: %{maxbytes: 10}})
    end

    test "the spread attribute is a key into the broker attributes, so it has to be a non-empty string" do
      # Broker attributes arrive from the environment keyed by string, so an atom matches no broker at
      # all: placement then puts every broker in the single nil domain, and a hard policy answers
      # :insufficient_domains while a soft one quietly stops spreading. An empty key matches nothing for
      # the same reason, which is why it is refused rather than treated as off. Off is nil.
      assert Policy.valid?(%{spread_by: "rack"})
      assert Policy.valid?(%{spread_by: nil})

      refute Policy.valid?(%{spread_by: :rack})
      refute Policy.valid?(%{spread_by: ""})
      refute Policy.valid?(%{spread_by: 1})
      refute Policy.valid?(%{spread_by: ["rack"]})
    end

    test "anything that is not a map is not a policy" do
      refute Policy.valid?(:always)
      refute Policy.valid?(nil)
      refute Policy.valid?(retention: %{})
      refute Policy.valid?(%{retention: [max_bytes: 10]})
    end

    property "a policy built only from the known keys and valid bounds is accepted" do
      check all(
              age <- StreamData.one_of([StreamData.constant(nil), StreamData.non_negative_integer()]),
              bytes <- StreamData.one_of([StreamData.constant(nil), StreamData.non_negative_integer()]),
              # min_length: 1, because an empty key is refused: it is a key into the broker attributes
              # and matches no broker, which is a different thing from nil, which is spreading off.
              spread <-
                StreamData.one_of([StreamData.constant(nil), StreamData.string(:alphanumeric, min_length: 1)])
            ) do
        assert Policy.valid?(%{retention: %{max_age_ms: age, max_bytes: bytes}, spread_by: spread})
      end
    end

    property "any extra key refuses the whole policy" do
      check all(key <- StreamData.atom(:alphanumeric), key not in [:retention, :spread_by]) do
        refute Policy.valid?(%{key => 1})
      end
    end
  end
end
