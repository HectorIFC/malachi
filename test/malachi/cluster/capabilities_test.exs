defmodule Malachi.Cluster.CapabilitiesTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Cluster.Capabilities
  alias Malachi.Cluster.Membership

  @cap :batch_format

  # A membership view as `supported_by_all/3` wants it, built from `%{node => {status, [capability]}}`.
  # A node absent from the map reads as unknown, which is what a real view answers for a member it has
  # never heard of.
  defp reads(nodes) do
    fn node ->
      case Map.fetch(nodes, node) do
        {:ok, {status, capabilities}} -> {status, Capabilities.attributes(%{}, capabilities)}
        :error -> {nil, %{}}
      end
    end
  end

  describe "attributes/2" do
    test "merges the capability list under the reserved key without touching the operator's own" do
      attributes = Capabilities.attributes(%{"rack" => "a", "dc" => "eu"}, [@cap])

      assert attributes["rack"] == "a"
      assert attributes["dc"] == "eu"
      assert attributes[Capabilities.key()] == [@cap]
    end

    test "the reserved key is an atom, so an operator's string keys can never shadow it" do
      # `Malachi.Application.parse_attributes/1` only ever produces string keys, which is what makes the
      # reserved key collision-proof rather than merely unlikely.
      assert is_atom(Capabilities.key())
      attributes = Capabilities.attributes(%{"capabilities" => "not mine"}, [@cap])

      assert attributes["capabilities"] == "not mine"
      assert attributes[Capabilities.key()] == [@cap]
    end

    test "defaults to what this build advertises" do
      assert Capabilities.attributes(%{})[Capabilities.key()] == Capabilities.advertised()
    end

    test "survives a round trip through the pure membership state, which is how it reaches a peer" do
      view = Membership.new(:self, attributes: Capabilities.attributes(%{"rack" => "a"}, [@cap]))
      {view, _effect} = Membership.apply_update(view, {:peer, :alive, 1, Capabilities.attributes(%{}, [@cap])})

      assert Capabilities.of(Membership.attributes(view, :self)) == [@cap]
      assert Capabilities.of(Membership.attributes(view, :peer)) == [@cap]
      assert Membership.attributes(view, :self)["rack"] == "a"
    end
  end

  describe "of/1" do
    test "reads the advertised list" do
      assert Capabilities.of(Capabilities.attributes(%{}, [@cap])) == [@cap]
    end

    test "a build that predates the key advertises nothing, which is the safe reading" do
      assert Capabilities.of(%{"rack" => "a"}) == []
      assert Capabilities.of(%{}) == []
    end

    test "a malformed value advertises nothing rather than raising inside a gossip merge" do
      assert Capabilities.of(%{Capabilities.key() => :not_a_list}) == []
    end
  end

  describe "supported_by_all/3" do
    test "every member alive and advertising" do
      nodes = %{:a@h => {:alive, [@cap]}, :b@h => {:alive, [@cap]}}

      assert Capabilities.supported_by_all([:a@h, :b@h], @cap, reads(nodes)) == :ok
    end

    test "one member is missing the capability, and the refusal names it" do
      nodes = %{:a@h => {:alive, [@cap]}, :b@h => {:alive, [:something_else]}}

      assert Capabilities.supported_by_all([:a@h, :b@h], @cap, reads(nodes)) ==
               {:error, {:unsupported, [:b@h]}}
    end

    test "a suspect member is refused: a node we are unsure about must not be assumed to be upgraded" do
      nodes = %{:a@h => {:alive, [@cap]}, :b@h => {:suspect, [@cap]}}

      assert Capabilities.supported_by_all([:a@h, :b@h], @cap, reads(nodes)) ==
               {:error, {:unsupported, [:b@h]}}
    end

    test "a dead member is refused: it has to be upgraded or removed before the flip is accepted" do
      nodes = %{:a@h => {:alive, [@cap]}, :b@h => {:dead, [@cap]}}

      assert Capabilities.supported_by_all([:a@h, :b@h], @cap, reads(nodes)) ==
               {:error, {:unsupported, [:b@h]}}
    end

    test "a member with empty attributes is refused, which is what an old build looks like" do
      nodes = %{:a@h => {:alive, [@cap]}, :b@h => {:alive, []}}

      assert Capabilities.supported_by_all([:a@h, :b@h], @cap, reads(nodes)) ==
               {:error, {:unsupported, [:b@h]}}
    end

    test "a configured node the view has never heard of is refused, not skipped" do
      # The whole reason the population is the configured node set: a node that just joined on an old
      # build is absent from the alive set, and counting only who answered would switch the flag on
      # over it.
      nodes = %{:a@h => {:alive, [@cap]}}

      assert Capabilities.supported_by_all([:a@h, :b@h], @cap, reads(nodes)) ==
               {:error, {:unsupported, [:b@h]}}
    end

    test "the single-node cluster" do
      assert Capabilities.supported_by_all([:a@h], @cap, reads(%{:a@h => {:alive, [@cap]}})) == :ok

      assert Capabilities.supported_by_all([:a@h], @cap, reads(%{:a@h => {:alive, []}})) ==
               {:error, {:unsupported, [:a@h]}}
    end

    test "an empty population answers ok, and the caller is what never passes one" do
      assert Capabilities.supported_by_all([], @cap, reads(%{})) == :ok
    end

    test "every missing node is named, sorted, not just the first" do
      nodes = %{:a@h => {:alive, []}, :b@h => {:alive, [@cap]}, :c@h => {:dead, [@cap]}}

      assert Capabilities.supported_by_all([:c@h, :b@h, :a@h], @cap, reads(nodes)) ==
               {:error, {:unsupported, [:a@h, :c@h]}}
    end

    property "adding a node that does not advertise can never turn a refusal into an ok" do
      check all(
              members <- list_of(member(), max_length: 6),
              extra <- member(),
              max_runs: 200
            ) do
        nodes = Map.new(members)
        before = Capabilities.supported_by_all(Map.keys(nodes), @cap, reads(nodes))

        {extra_node, {status, capabilities}} = extra
        grown = Map.put(nodes, extra_node, {status, capabilities})
        after_growth = Capabilities.supported_by_all(Map.keys(grown), @cap, reads(grown))

        advertises = status == :alive and @cap in capabilities

        # Growing the population is monotone: a node that does not advertise can only take the answer
        # from :ok to a refusal. It can never rescue one.
        if before != :ok and not advertises do
          assert after_growth != :ok
        end

        # And whatever the answer is, every refused node really is one of the population.
        case after_growth do
          :ok -> assert Enum.all?(grown, fn {_node, {s, c}} -> s == :alive and @cap in c end)
          {:error, {:unsupported, missing}} -> assert Enum.all?(missing, &Map.has_key?(grown, &1))
        end
      end
    end
  end

  describe "resolve/2" do
    test "resolves a known name" do
      assert Capabilities.resolve("batch_format", [:batch_format, :other]) == {:ok, :batch_format}
    end

    test "refuses an unknown name" do
      assert Capabilities.resolve("nope", [:batch_format]) == {:error, :unknown_flag}
    end

    test "refuses against an empty registry, which is what the bridge release ships" do
      assert Capabilities.resolve("batch_format", []) == {:error, :unknown_flag}
      assert Capabilities.resolve("batch_format") == {:error, :unknown_flag}
    end

    test "creates no atom for an unknown name" do
      # The input is a command line string. String.to_atom/1 here would grow a table that is never
      # collected, from text an operator typed.
      #
      # Asserted on the name itself rather than on the VM's atom count: this module is async, the count
      # is VM-wide, and any test running beside it that creates an atom would fail this one. Asking
      # whether this name became an atom is both deterministic and a tighter question.
      name = "a_name_this_vm_has_never_seen_#{System.unique_integer([:positive])}"

      assert Capabilities.resolve(name, []) == {:error, :unknown_flag}
      assert_raise ArgumentError, fn -> String.to_existing_atom(name) end
    end
  end

  describe "the registry this release ships" do
    test "is empty, and known/0 and advertised/0 agree" do
      # The bridge release ships the gate, not a feature. The first entry arrives with the first thing
      # that needs a cluster-wide commitment (#202). If this ever fails, the release that added a
      # capability has to have added it to both.
      assert Capabilities.known() == []
      assert Capabilities.advertised() == Capabilities.known()
    end
  end

  defp member do
    tuple({
      member_of([:a@h, :b@h, :c@h, :d@h]),
      tuple({member_of([:alive, :suspect, :dead]), member_of([[], [@cap], [:other], [@cap, :other]])})
    })
  end
end
