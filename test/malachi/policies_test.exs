defmodule Malachi.PoliciesTest do
  # The policy store and the broker are the ones the application started: global state.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Malachi.Cluster.PolicyStore
  alias Malachi.DataPlaneRouter
  alias Malachi.LogApi
  alias Malachi.Policies

  doctest Malachi.Policies

  setup do
    suffix = System.unique_integer([:positive])
    policy = "policies_test_#{suffix}"
    topic = "policies-test-#{suffix}"
    actor = "tester_#{suffix}"
    :ok = LogApi.create_topic(DataPlaneRouter.shard_for(topic), topic)

    on_exit(fn -> PolicyStore.delete(policy) end)
    %{policy: policy, topic: topic, actor: actor}
  end

  defp audited(actor) do
    # The audit log is written by a cast; a call behind it drains the mailbox first.
    _ = :sys.get_state(Malachi.AuditLog)
    Malachi.AuditLog.get_events_by_user(actor)
  end

  describe "define/3" do
    test "stores a policy given as flat pairs or as a map, and audits and logs it", ctx do
      log =
        capture_log(fn ->
          assert Policies.define(ctx.policy, [{"retention.max_bytes", 0}], ctx.actor) == :ok
        end)

      assert PolicyStore.get(ctx.policy) == %{retention: %{max_bytes: 0}}
      assert log =~ ctx.policy

      assert Policies.define(ctx.policy, %{spread_by: "rack"}, ctx.actor) == :ok
      assert PolicyStore.get(ctx.policy) == %{spread_by: "rack"}

      assert Enum.any?(
               audited(ctx.actor),
               &match?(%{event_type: :policy_defined, action: "define_policy", status: :success}, &1)
             )
    end

    test "refuses a bad name, a bad field and a bad map, and audits the refusal by its reason", ctx do
      assert Policies.define("", [], ctx.actor) == {:error, :invalid_policy_name}

      assert Policies.define(ctx.policy, [{"retention.ms", 1}], ctx.actor) ==
               {:error, {:unknown_policy_field, "retention.ms"}}

      assert Policies.define(ctx.policy, %{retention: %{max_bytes: "1"}}, ctx.actor) == {:error, :invalid_policy}
      assert Policies.define(ctx.policy, :nope, ctx.actor) == {:error, :invalid_policy}
      assert PolicyStore.get(ctx.policy) == nil

      # Events of one millisecond tie on the timestamp the log sorts by, so each is looked for, not indexed.
      reasons =
        for %{event_type: :policy_defined, status: :failure, metadata: %{reason: reason}} <- audited(ctx.actor),
            do: reason

      assert Enum.sort(reasons) == [
               "invalid_policy",
               "invalid_policy",
               "invalid_policy_name",
               "unknown_policy_field: retention.ms"
             ]
    end
  end

  describe "bind/3" do
    test "binds a topic to a defined policy and detaches it with nil", ctx do
      :ok = Policies.define(ctx.policy, [{"retention.max_age_ms", 1_000}], ctx.actor)

      assert Policies.bind(ctx.topic, ctx.policy, ctx.actor) == :ok
      assert {:ok, %{policy: policy, resolution: :resolved}} = Policies.topic_policy(ctx.topic)
      assert policy == ctx.policy

      assert Policies.bind(ctx.topic, nil, ctx.actor) == :ok
      assert {:ok, %{policy: nil, resolution: :none}} = Policies.topic_policy(ctx.topic)

      events = audited(ctx.actor)
      assert Enum.count(events, &(&1.event_type == :topic_policy_bound and &1.status == :success)) == 2
    end

    test "refuses a name nothing defines, so a typo cannot hold a topic's data forever", ctx do
      assert Policies.bind(ctx.topic, ctx.policy, ctx.actor) == {:error, :no_such_policy}
      assert {:ok, %{policy: nil}} = Policies.topic_policy(ctx.topic)
    end

    test "refuses an unknown topic, an empty topic and a name that cannot name a policy", ctx do
      :ok = Policies.define(ctx.policy, [], ctx.actor)
      assert Policies.bind("ghost-#{ctx.topic}", ctx.policy, ctx.actor) == {:error, :no_such_topic}
      assert Policies.bind("", ctx.policy, ctx.actor) == {:error, :invalid_topic}
      assert Policies.bind(ctx.topic, "", ctx.actor) == {:error, :invalid_policy_name}
    end
  end

  describe "a broker that does not answer" do
    test "a bind is an audited timeout, not an exit that takes the caller down", ctx do
      :ok = Policies.define(ctx.policy, [], ctx.actor)
      :ok = :sys.suspend(Malachi.LogBroker)

      try do
        assert Policies.bind(ctx.topic, ctx.policy, ctx.actor) == {:error, :timeout}
      after
        :ok = :sys.resume(Malachi.LogBroker)
      end

      assert Enum.any?(
               audited(ctx.actor),
               &match?(%{event_type: :topic_policy_bound, status: :failure, metadata: %{reason: "timeout" <> _}}, &1)
             )
    end
  end

  describe "delete/3" do
    test "refuses a policy a topic is bound to, naming the topics, unless forced", ctx do
      :ok = Policies.define(ctx.policy, [], ctx.actor)
      :ok = Policies.bind(ctx.topic, ctx.policy, ctx.actor)

      assert Policies.delete(ctx.policy, ctx.actor) == {:error, {:policy_in_use, [ctx.topic]}}
      assert PolicyStore.get(ctx.policy) == %{}

      assert Policies.delete(ctx.policy, ctx.actor, force: true) == :ok
      assert PolicyStore.get(ctx.policy) == nil

      # What the force left behind is visible, not silent: the topic holds its data.
      assert {:ok, %{resolution: :unresolved}} = Policies.topic_policy(ctx.topic)

      events = audited(ctx.actor)
      assert Enum.any?(events, &match?(%{event_type: :policy_deleted, status: :success, metadata: %{force: true}}, &1))
      assert Enum.any?(events, &match?(%{event_type: :policy_deleted, status: :failure, metadata: %{force: false}}, &1))
    end

    test "deletes an unbound policy, and deleting a name nothing defines is still ok", ctx do
      :ok = Policies.define(ctx.policy, [], ctx.actor)
      assert Policies.delete(ctx.policy, ctx.actor) == :ok
      assert Policies.delete(ctx.policy, ctx.actor) == :ok
      assert Policies.delete("", ctx.actor) == {:error, :invalid_policy_name}
    end
  end

  describe "list/0" do
    test "lists every definition", ctx do
      :ok = Policies.define(ctx.policy, [{"spread_by", "rack"}], ctx.actor)
      assert {:ok, %{} = all} = Policies.list()
      assert all[ctx.policy] == %{spread_by: "rack"}
    end
  end

  describe "topic_policy/1" do
    setup do
      previous =
        Map.new(
          [:retention_max_age_ms, :retention_max_bytes, :log_spread_by, :retention_unresolved_policy_max_age_ms],
          &{&1, Application.get_env(:malachi, &1)}
        )

      Application.put_env(:malachi, :retention_max_age_ms, 60_000)
      Application.put_env(:malachi, :retention_max_bytes, nil)
      Application.put_env(:malachi, :log_spread_by, "rack")

      on_exit(fn ->
        Enum.each(previous, fn
          {key, nil} -> Application.delete_env(:malachi, key)
          {key, value} -> Application.put_env(:malachi, key, value)
        end)
      end)
    end

    test "reports each bound's effective value and its origin, the way the sweep sees it", ctx do
      assert {:ok, unbound} = Policies.topic_policy(ctx.topic)

      assert unbound == %{
               topic: ctx.topic,
               policy: nil,
               definition: nil,
               resolution: :none,
               retention: %{max_age_ms: {60_000, :global}, max_bytes: {nil, :global}},
               spread_by: {"rack", :global}
             }

      :ok = Policies.define(ctx.policy, [{"retention.max_bytes", 0}, {"spread_by", nil}], ctx.actor)
      :ok = Policies.bind(ctx.topic, ctx.policy, ctx.actor)

      assert {:ok, bound} = Policies.topic_policy(ctx.topic)
      assert bound.definition == %{retention: %{max_bytes: 0}, spread_by: nil}
      assert bound.retention == %{max_age_ms: {60_000, :global}, max_bytes: {0, :policy}}
      assert bound.spread_by == {nil, :policy}

      assert Policies.effective_pairs(bound) == [
               {"retention.max_age_ms", 60_000, :global},
               {"retention.max_bytes", 0, :policy},
               {"spread_by", nil, :policy}
             ]
    end

    test "a topic bound to a name nothing defines reports the operator's backstop, the one bound the sweep applies",
         ctx do
      Application.put_env(:malachi, :retention_unresolved_policy_max_age_ms, 7_000)
      :ok = Policies.define(ctx.policy, [], ctx.actor)
      :ok = Policies.bind(ctx.topic, ctx.policy, ctx.actor)
      :ok = Policies.delete(ctx.policy, ctx.actor, force: true)

      assert {:ok, %{resolution: :unresolved} = orphaned} = Policies.topic_policy(ctx.topic)
      assert orphaned.retention == %{max_age_ms: {7_000, :unresolved_backstop}, max_bytes: {nil, :unresolved_backstop}}
    end

    test "an unknown or empty topic is an error, not a report of the global limits", ctx do
      assert Policies.topic_policy("ghost-#{ctx.topic}") == {:error, :no_such_topic}
      assert Policies.topic_policy("") == {:error, :invalid_topic}
    end
  end

  describe "reason_string/1" do
    test "renders every refusal with its name first" do
      assert Policies.reason_string({:unknown_policy_field, "x"}) == "unknown_policy_field: x"
      assert Policies.reason_string({:duplicate_policy_field, "x"}) == "duplicate_policy_field: x"
      assert Policies.reason_string({:invalid_policy_field, "x"}) == "invalid_policy_field: x"

      assert Policies.reason_string({:unsupported_command, {:bind_topic_policy, 3}, 4, 3}) =~
               ~r/^unsupported_command: .*machine version 3 and this needs 4; finish the rolling upgrade/

      assert Policies.reason_string({:unsupported_policy_field, "retention.max_records", 5, 4}) =~
               ~r/^unsupported_policy_field: retention.max_records: .*needs 5/

      assert Policies.reason_string(:timeout) =~ ~r/^timeout: .*read it back/
      assert Policies.reason_string(:migrating) == "migrating"

      assert Policies.reason_string({:bindings_unavailable, :split_in_progress}) =~
               ~r/^bindings_unavailable: .*split_in_progress/

      assert Policies.reason_string({:noproc, :x}) == "{:noproc, :x}"
    end
  end
end
