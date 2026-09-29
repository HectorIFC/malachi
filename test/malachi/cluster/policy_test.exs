defmodule Malachi.Cluster.PolicyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Cluster.MachineVersion
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

  # A table with one field introduced a version above production: the shape #199, #200, #201 and #206
  # each add. Injected, so the rule is proved without inventing a real field.
  @newer MachineVersion.code_version() + 1
  @future_fields Policy.fields() ++
                   [%{name: "retention.max_records", path: [:retention, :max_records], type: :bound, since: @newer}]

  describe "validate/3 (the field table, against the effective machine version)" do
    test "a field introduced above the effective version is refused by name, on every member alike" do
      policy = %{retention: %{max_records: 1_000}}

      assert Policy.validate(policy, @newer - 1, @future_fields) ==
               {:error, {:unsupported_policy_field, "retention.max_records", @newer}}

      assert Policy.validate(policy, @newer, @future_fields) == :ok
    end

    test "the version gate never lets an invalid value through" do
      assert Policy.validate(%{retention: %{max_records: -1}}, @newer, @future_fields) == {:error, :invalid_policy}
      assert Policy.validate(%{retention: %{max_records: "1k"}}, @newer, @future_fields) == {:error, :invalid_policy}
    end

    test "below the version that introduced the store no field exists" do
      assert Policy.validate(%{}, 2) == :ok
      assert Policy.validate(%{spread_by: "rack"}, 2) == {:error, {:unsupported_policy_field, "spread_by", 3}}
    end

    test "valid?/1 is the table at this build's version" do
      assert Policy.valid?(%{retention: %{max_bytes: 1}}) ==
               Policy.valid?(%{retention: %{max_bytes: 1}}, MachineVersion.code_version())

      refute Policy.valid?(%{retention: %{max_records: 1}})
    end

    test "every field in the production table names its own path and a known type" do
      for %{name: name, path: path, type: type, since: since} <- Policy.fields() do
        assert name == Enum.map_join(path, ".", &Atom.to_string/1)
        assert type in [:bound, :attribute]
        assert since <= MachineVersion.code_version()
      end
    end
  end

  describe "from_pairs/2 and to_pairs/1 (the flat form every surface speaks)" do
    test "absent, nil and 0 stay three different things" do
      assert Policy.from_pairs([{"retention.max_bytes", 0}, {"retention.max_age_ms", nil}]) ==
               {:ok, %{retention: %{max_bytes: 0, max_age_ms: nil}}}

      assert Policy.from_pairs([]) == {:ok, %{}}

      assert Policy.to_pairs(%{retention: %{max_bytes: 0, max_age_ms: nil}}) ==
               [{"retention.max_age_ms", nil}, {"retention.max_bytes", 0}]

      assert Policy.to_pairs(%{}) == []
    end

    test "an unknown name, a bad value and a repeated name are each refused by name" do
      assert Policy.from_pairs([{"retention.ms", 1}]) == {:error, {:unknown_policy_field, "retention.ms"}}
      assert Policy.from_pairs([{"spread_by", ""}]) == {:error, {:invalid_policy_field, "spread_by"}}

      assert Policy.from_pairs([{"retention.max_bytes", -1}]) ==
               {:error, {:invalid_policy_field, "retention.max_bytes"}}

      assert Policy.from_pairs([{"retention.max_bytes", "1"}]) ==
               {:error, {:invalid_policy_field, "retention.max_bytes"}}

      assert Policy.from_pairs([{"spread_by", "a"}, {"spread_by", "b"}]) ==
               {:error, {:duplicate_policy_field, "spread_by"}}
    end

    test "a bound fits in the 64 bits the wire carries it in" do
      max = 0xFFFF_FFFF_FFFF_FFFF
      assert {:ok, %{retention: %{max_bytes: ^max}}} = Policy.from_pairs([{"retention.max_bytes", max}])

      assert Policy.from_pairs([{"retention.max_bytes", max + 1}]) ==
               {:error, {:invalid_policy_field, "retention.max_bytes"}}

      refute Policy.valid?(%{retention: %{max_age_ms: max + 1}})
    end

    test "a field the injected table knows is accepted by the flat form too" do
      assert Policy.from_pairs([{"retention.max_records", 5}], @future_fields) ==
               {:ok, %{retention: %{max_records: 5}}}
    end

    test "field/2 finds a field by its flat name" do
      assert %{path: [:spread_by], type: :attribute} = Policy.field("spread_by")
      assert Policy.field("nope") == nil
    end

    property "to_pairs and from_pairs round-trip every valid policy" do
      bound = StreamData.one_of([StreamData.constant(nil), StreamData.non_negative_integer()])

      check all(
              pairs <-
                StreamData.fixed_map(%{
                  "retention.max_age_ms" => bound,
                  "retention.max_bytes" => bound,
                  "spread_by" =>
                    StreamData.one_of([StreamData.constant(nil), StreamData.string(:alphanumeric, min_length: 1)])
                }),
              keep <- StreamData.list_of(StreamData.member_of(Map.keys(pairs)), max_length: 3)
            ) do
        chosen = pairs |> Map.take(keep) |> Enum.to_list()
        assert {:ok, policy} = Policy.from_pairs(chosen)
        assert Policy.valid?(policy)
        assert Enum.sort(Policy.to_pairs(policy)) == Enum.sort(chosen)
      end
    end
  end
end
