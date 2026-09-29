defmodule Mix.Tasks.Malachi.PolicyTest do
  # The local-call cases use the policy store and broker the application started: global state.
  use ExUnit.Case, async: false

  alias Malachi.Cluster.PolicyStore
  alias Malachi.DataPlaneRouter
  alias Malachi.LogApi
  alias Mix.Tasks.Malachi.Policy, as: Task

  defp recording_call(result) do
    parent = self()

    fn module, fun, args ->
      send(parent, {:called, module, fun, args})
      result
    end
  end

  defp local_call, do: fn module, fun, args -> {:ok, apply(module, fun, args)} end

  describe "execute/3: parsing and dispatch" do
    test "define turns --set and --off into flat pairs, parsed against the field table" do
      call = recording_call({:ok, :ok})
      opts = [set: "retention.max_bytes=0", set: "spread_by=rack", off: "retention.max_age_ms"]

      assert {:ok, "defined policy p"} = Task.execute(["define", "p"], opts, call)
      assert_received {:called, Malachi.Policies, :define, ["p", pairs, "cli@" <> _node]}
      assert pairs == [{"retention.max_bytes", 0}, {"spread_by", "rack"}, {"retention.max_age_ms", nil}]
    end

    test "define with no fields sends an empty policy, which inherits every global value" do
      call = recording_call({:ok, :ok})
      assert {:ok, _message} = Task.execute(["define", "p"], [], call)
      assert_received {:called, Malachi.Policies, :define, ["p", [], _actor]}
    end

    test "a bad field or value is refused before anything is sent" do
      call = recording_call({:ok, :ok})

      assert {:error, message} = Task.execute(["define", "p"], [set: "retention.ms=1"], call)
      assert message =~ "unknown policy field retention.ms"
      assert message =~ "retention.max_age_ms"
      assert {:error, "unknown policy field nope" <> _} = Task.execute(["define", "p"], [off: "nope"], call)

      assert {:error, "invalid value in --set retention.max_bytes=-1"} =
               Task.execute(["define", "p"], [set: "retention.max_bytes=-1"], call)

      assert {:error, "invalid value in --set retention.max_bytes=1k"} =
               Task.execute(["define", "p"], [set: "retention.max_bytes=1k"], call)

      assert {:error, "invalid value in --set spread_by="} = Task.execute(["define", "p"], [set: "spread_by="], call)

      assert {:error, "--set takes <field>=<value>, got: spread_by"} =
               Task.execute(["define", "p"], [set: "spread_by"], call)

      refute_received {:called, _, _, _}
    end

    test "delete, bind and unbind dispatch with the actor and the force flag" do
      call = recording_call({:ok, :ok})

      assert {:ok, "deleted policy p"} = Task.execute(["delete", "p"], [force: true], call)
      assert_received {:called, Malachi.Policies, :delete, ["p", _actor, [force: true]]}

      assert {:ok, _message} = Task.execute(["delete", "p"], [], call)
      assert_received {:called, Malachi.Policies, :delete, ["p", _actor, [force: false]]}

      assert {:ok, "bound t to policy p"} = Task.execute(["bind", "t", "p"], [], call)
      assert_received {:called, Malachi.Policies, :bind, ["t", "p", _actor]}

      assert {:ok, "detached t from its policy"} = Task.execute(["unbind", "t"], [], call)
      assert_received {:called, Malachi.Policies, :bind, ["t", nil, _actor]}
    end

    test "a refusal is rendered by the shared reason text, including the upgrade message" do
      in_use = recording_call({:ok, {:error, {:policy_in_use, ["a", "b"]}}})
      assert {:error, "policy_in_use: a, b"} = Task.execute(["delete", "p"], [], in_use)

      upgrading = recording_call({:ok, {:error, {:unsupported_command, {:bind_topic_policy, 3}, 4, 3}}})
      assert {:error, message} = Task.execute(["bind", "t", "p"], [], upgrading)
      assert message =~ "machine version 3"
      assert message =~ "rolling upgrade"
    end

    test "list and get pass a refusal or a transport failure through as an error" do
      assert {:error, "timeout: " <> _} = Task.execute(["list"], [], recording_call({:ok, {:error, :timeout}}))
      assert {:error, "no_such_topic"} = Task.execute(["get", "t"], [], recording_call({:ok, {:error, :no_such_topic}}))
      assert {:error, message} = Task.execute(["list"], [], recording_call({:error, :nodedown}))
      assert message =~ "nodedown"
    end

    test "list formats each policy's fields, off for nil, and an empty store" do
      policies = %{"b" => %{retention: %{max_bytes: 0, max_age_ms: nil}}, "a" => %{}}
      assert {:ok, listing} = Task.execute(["list"], [], recording_call({:ok, {:ok, policies}}))
      assert listing == "a\t(inherits every global value)\nb\tretention.max_age_ms=off retention.max_bytes=0"

      assert {:ok, "(no policies)"} = Task.execute(["list"], [], recording_call({:ok, {:ok, %{}}}))
    end

    test "get prints the binding and every effective value with its origin" do
      topic_policy = %{
        topic: "t",
        policy: "ghost",
        definition: nil,
        resolution: :unresolved,
        retention: %{max_age_ms: {5_000, :unresolved_backstop}, max_bytes: {nil, :unresolved_backstop}},
        spread_by: {"rack", :global}
      }

      assert {:ok, report} = Task.execute(["get", "t"], [], recording_call({:ok, {:ok, topic_policy}}))

      assert report ==
               Enum.join(
                 [
                   "topic\tt",
                   "policy\tghost (undefined: this topic holds its data)",
                   "retention.max_age_ms\t5000\t(unresolved_backstop)",
                   "retention.max_bytes\toff\t(unresolved_backstop)",
                   "spread_by\track\t(global)"
                 ],
                 "\n"
               )

      none = %{topic_policy | policy: nil, resolution: :none}
      assert {:ok, "topic\tt\npolicy\t(none)" <> _} = Task.execute(["get", "t"], [], recording_call({:ok, {:ok, none}}))

      named = %{topic_policy | policy: "p", resolution: :resolved}
      assert {:ok, "topic\tt\npolicy\tp\n" <> _} = Task.execute(["get", "t"], [], recording_call({:ok, {:ok, named}}))
    end

    test "anything else prints the usage" do
      assert {:error, usage} = Task.execute(["frobnicate"], [], recording_call({:ok, :ok}))
      assert usage =~ "mix malachi.policy define"
      assert usage =~ "retention.max_bytes"
    end
  end

  describe "execute/3 against the real node code (no RPC transport)" do
    setup do
      suffix = System.unique_integer([:positive])
      name = "task_policy_#{suffix}"
      topic = "task-topic-#{suffix}"
      :ok = LogApi.create_topic(DataPlaneRouter.shard_for(topic), topic)
      on_exit(fn -> PolicyStore.delete(name) end)
      %{name: name, topic: topic}
    end

    test "define, bind, get, then a refused delete and a forced one", %{name: name, topic: topic} do
      assert {:ok, _} = Task.execute(["define", name], [set: "retention.max_bytes=0"], local_call())
      assert {:ok, _} = Task.execute(["bind", topic, name], [], local_call())

      assert {:ok, report} = Task.execute(["get", topic], [], local_call())
      assert report =~ "policy\t#{name}\n"
      assert report =~ "retention.max_bytes\t0\t(policy)"

      assert {:error, "policy_in_use: " <> ^topic} = Task.execute(["delete", name], [], local_call())
      assert {:ok, _} = Task.execute(["unbind", topic], [], local_call())
      assert {:ok, _} = Task.execute(["delete", name], [], local_call())
      assert {:error, "no_such_policy"} = Task.execute(["bind", topic, name], [], local_call())
    end
  end

  describe "run/1" do
    setup do
      previous = Mix.shell()
      Mix.shell(Mix.Shell.Process)
      on_exit(fn -> Mix.shell(previous) end)
      # The test VM is itself a named node, so the task can target it over real distribution.
      %{target: ["--node", Atom.to_string(node()), "--cookie", Atom.to_string(Node.get_cookie())]}
    end

    test "an unknown option is refused with the usage, before any node is contacted" do
      assert_raise Mix.Error, ~r/unknown option\(s\): --nod/, fn -> Task.run(["list", "--nod", "x@y"]) end
    end

    test "prints the answer of a real node over RPC", %{target: target} do
      Task.run(["list" | target])
      assert_received {:mix_shell, :info, [_listing]}
    end

    test "prints a refusal and exits non-zero", %{target: target} do
      assert catch_exit(Task.run(["get", "ghost-#{System.unique_integer([:positive])}" | target])) == {:shutdown, 1}
      assert_received {:mix_shell, :error, ["no_such_topic"]}
    end

    test "a node that cannot be reached is refused with the reason", %{target: [_flag, _node | cookie]} do
      nobody = "nobody_#{System.unique_integer([:positive])}@127.0.0.1"

      assert_raise Mix.Error, ~r/could not connect to #{nobody}/, fn ->
        Task.run(["list", "--node", nobody | cookie])
      end
    end
  end
end
