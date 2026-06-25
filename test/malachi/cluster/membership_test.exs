defmodule Malachi.Cluster.MembershipTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Cluster.Membership

  describe "construction" do
    test "self and peers start alive at incarnation 0" do
      view = Membership.new(:self, peers: [:a, :b])
      assert Membership.alive_members(view) == [:a, :b, :self]
      assert Membership.status(view, :a) == :alive
      assert Membership.incarnation(view, :self) == 0
      assert Membership.status(view, :unknown) == nil
    end
  end

  describe "precedence rules" do
    setup do
      %{view: Membership.new(:self, peers: [:a])}
    end

    test "a higher incarnation always wins", %{view: view} do
      {view, _} = Membership.apply_update(view, {:a, :dead, 5})
      {view, effect} = Membership.apply_update(view, {:a, :alive, 6})
      assert Membership.status(view, :a) == :alive
      assert effect == {:applied, {:a, :alive, 6}}
    end

    test "suspect overrides alive at the same incarnation", %{view: view} do
      {view, effect} = Membership.apply_update(view, {:a, :suspect, 0})
      assert Membership.status(view, :a) == :suspect
      assert effect == {:applied, {:a, :suspect, 0}}
    end

    test "alive does not override suspect at the same incarnation", %{view: view} do
      {view, _} = Membership.apply_update(view, {:a, :suspect, 3})
      {view, effect} = Membership.apply_update(view, {:a, :alive, 3})
      assert Membership.status(view, :a) == :suspect
      assert effect == :ignored
    end

    test "dead overrides suspect at the same incarnation", %{view: view} do
      {view, _} = Membership.apply_update(view, {:a, :suspect, 2})
      {view, _} = Membership.apply_update(view, {:a, :dead, 2})
      assert Membership.status(view, :a) == :dead
    end

    test "an equal {incarnation, status} is ignored (idempotent)", %{view: view} do
      {view, _} = Membership.apply_update(view, {:a, :suspect, 1})
      assert {^view, :ignored} = Membership.apply_update(view, {:a, :suspect, 1})
    end

    test "a learned-about new member is adopted", %{view: view} do
      {view, effect} = Membership.apply_update(view, {:c, :alive, 0})
      assert Membership.status(view, :c) == :alive
      assert effect == {:applied, {:c, :alive, 0}}
    end
  end

  describe "self refutation" do
    test "a suspicion about self is refuted with a higher incarnation and disseminated" do
      view = Membership.new(:self)
      {view, effect} = Membership.apply_update(view, {:self, :suspect, 4})

      assert Membership.status(view, :self) == :alive
      assert Membership.incarnation(view, :self) == 5
      assert effect == {:refute, {:self, :alive, 5}}
    end

    test "a confirmation about self is also refuted" do
      view = Membership.new(:self)
      {view, {:refute, {:self, :alive, inc}}} = Membership.apply_update(view, {:self, :dead, 9})
      assert inc == 10
      assert Membership.status(view, :self) == :alive
    end
  end

  describe "merge" do
    test "applies a batch and returns the changes to disseminate" do
      view = Membership.new(:self, peers: [:a, :b])
      {view, effects} = Membership.merge(view, [{:a, :suspect, 0}, {:b, :alive, 0}, {:c, :alive, 2}])

      # :a suspect changed it, :c is new; :b alive@0 was already known (no change)
      assert Membership.status(view, :a) == :suspect
      assert Membership.status(view, :c) == :alive
      assert effects == [{:a, :suspect, 0}, {:c, :alive, 2}]
    end
  end

  describe "properties" do
    property "merge converges regardless of update order (for other members)" do
      check all(updates <- list_of(update(), max_length: 40)) do
        view = Membership.new(:self, peers: [:a, :b, :c])
        {one, _} = Membership.merge(view, updates)
        {two, _} = Membership.merge(view, Enum.shuffle(updates))
        assert one.members == two.members
      end
    end

    property "a member's incarnation never decreases" do
      check all(updates <- list_of(update_including_self(), max_length: 40)) do
        Enum.reduce(updates, Membership.new(:self, peers: [:a, :b]), fn {member, _, _} = u, view ->
          before = Membership.incarnation(view, member) || -1
          {view, _} = Membership.apply_update(view, u)
          assert (Membership.incarnation(view, member) || -1) >= before
          view
        end)
      end
    end
  end

  # --- generators ---

  defp update do
    {member_of([:a, :b, :c, :d]), member_of([:alive, :suspect, :dead]), integer(0..6)}
    |> tuple()
  end

  defp update_including_self do
    {member_of([:self, :a, :b]), member_of([:alive, :suspect, :dead]), integer(0..6)}
    |> tuple()
  end
end
