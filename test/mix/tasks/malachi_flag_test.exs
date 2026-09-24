defmodule Mix.Tasks.Malachi.FlagTest do
  # Not async: the live-node tests swap `Mix.shell/1`, which is VM-wide, and the other two task suites
  # that do the same (`malachi.docs.results`, `malachi.loadtest.ceiling`) are synchronous for that reason.
  # An async module here could have its shell restored by one of them between the call and the assertion.
  use ExUnit.Case, async: false

  alias Mix.Tasks.Malachi.Flag

  # A `call` seam that records the (module, fun, args) to the test process and returns a canned result.
  defp recording_call(result) do
    parent = self()

    fn module, fun, args ->
      send(parent, {:called, module, fun, args})
      result
    end
  end

  describe "enable" do
    test "asks the remote node to switch the flag on and reports success" do
      call = recording_call({:ok, :ok})

      assert {:ok, msg} = Flag.execute(["enable", "batch_format"], [], call)
      assert msg =~ "batch_format"

      assert_received {:called, Malachi.Application, :enable_cluster_flag, ["batch_format"]}
    end

    test "a refusal names the nodes to upgrade, which is the whole point of refusing" do
      call = recording_call({:ok, {:error, {:unsupported, [:old@a, :old@b]}}})

      assert {:error, msg} = Flag.execute(["enable", "batch_format"], [], call)
      assert msg =~ "old@a"
      assert msg =~ "old@b"
      assert msg =~ "MALACHI_LOG_NODES"
    end

    test "an unknown flag points at the list instead of guessing" do
      assert {:error, msg} = Flag.execute(["enable", "nope"], [], recording_call({:ok, {:error, :unknown_flag}}))
      assert msg =~ "no flag by that name"
      assert msg =~ "--list"
    end

    test "a control plane still below the command's version says so, and what to do" do
      refusal = {:ok, {:error, {:unsupported_command, {:enable_flag, 2}, 2, 1}}}

      assert {:error, msg} = Flag.execute(["enable", "batch_format"], [], recording_call(refusal))
      assert msg =~ "machine version 1"
      assert msg =~ "rolling upgrade"
    end

    test "a store that cannot answer is explained rather than inspected" do
      assert {:error, msg} = Flag.execute(["enable", "batch_format"], [], recording_call({:ok, {:error, :timeout}}))
      assert msg =~ "quorum"
    end

    test "an unmapped error is still surfaced" do
      assert {:error, msg} =
               Flag.execute(["enable", "batch_format"], [], recording_call({:ok, {:error, {:odd, :thing}}}))

      assert msg =~ "odd"
    end

    test "an RPC transport failure is reported, not crashed" do
      assert {:error, msg} = Flag.execute(["enable", "batch_format"], [], recording_call({:error, :nodedown}))
      assert msg =~ "rpc failed"
      assert msg =~ "nodedown"
    end
  end

  describe "--list" do
    test "prints each known flag and whether it is on" do
      call = recording_call({:ok, {:ok, %{known: [:batch_format, :compaction], enabled: [:compaction]}}})

      assert {:ok, msg} = Flag.execute([], [list: true], call)
      assert msg =~ "batch_format\toff"
      assert msg =~ "compaction\ton"

      assert_received {:called, Malachi.Application, :cluster_flags, []}
    end

    test "says so plainly when this build knows no flags, which is what the bridge release ships" do
      call = recording_call({:ok, {:ok, %{known: [], enabled: []}}})

      assert {:ok, msg} = Flag.execute([], [list: true], call)
      assert msg =~ "knows no cluster flags yet"
    end

    test "a store that cannot answer is an error, not an empty list" do
      call = recording_call({:ok, {:error, :timeout}})

      assert {:error, msg} = Flag.execute([], [list: true], call)
      assert msg =~ "quorum"
    end

    test "an RPC transport failure is reported" do
      assert {:error, msg} = Flag.execute([], [list: true], recording_call({:error, :nodedown}))
      assert msg =~ "rpc failed"
    end
  end

  describe "run/1 against a live node" do
    # The suite's own VM is a named, running Malachi node, so pointing the task at it exercises the whole
    # path the seam tests skip: resolve the node, connect, RPC, and print. Nothing else covers it.
    setup do
      shell = Mix.shell()
      Mix.shell(Mix.Shell.Process)
      on_exit(fn -> Mix.shell(shell) end)
      %{target: to_string(node())}
    end

    test "--list reaches the node and prints its answer", %{target: target} do
      Flag.run(["--list", "--node", target])

      assert_received {:mix_shell, :info, [message]}
      assert message =~ "knows no cluster flags yet"
    end

    test "a refusal is printed on the error channel and exits non-zero", %{target: target} do
      assert catch_exit(Flag.run(["enable", "no_such_flag", "--node", target])) == {:shutdown, 1}

      assert_received {:mix_shell, :error, [message]}
      assert message =~ "no flag by that name"
    end
  end

  describe "argument handling" do
    test "no arguments and no --list returns usage without calling the seam" do
      assert {:error, msg} = Flag.execute([], [], recording_call({:ok, :ok}))
      assert msg =~ "usage:"
      refute_received {:called, _module, _fun, _args}
    end

    test "an unknown subcommand returns usage" do
      assert {:error, msg} = Flag.execute(["disable", "batch_format"], [], recording_call({:ok, :ok}))
      assert msg =~ "usage:"
      refute_received {:called, _module, _fun, _args}
    end

    test "--list together with enable is refused rather than resolved in favour of the enable" do
      # A read switch and an irreversible write in one invocation. Falling through to the write would
      # switch a flag on because of a switch the operator meant as a read.
      assert {:error, msg} = Flag.execute(["enable", "batch_format"], [list: true], recording_call({:ok, :ok}))
      assert msg =~ "usage:"
      refute_received {:called, _module, _fun, _args}
    end

    test "enable without a name returns usage" do
      assert {:error, msg} = Flag.execute(["enable"], [], recording_call({:ok, :ok}))
      assert msg =~ "usage:"
      refute_received {:called, _module, _fun, _args}
    end

    test "the usage says a flag is never switched back off" do
      assert {:error, msg} = Flag.execute([], [], recording_call({:ok, :ok}))
      assert msg =~ "never switched back off"
    end

    test "an unknown option aborts with usage and never resolves or connects to a node" do
      # `--nod` lands in OptionParser's invalid list and is absent from opts, so falling through would
      # target $MALACHI_NODE (or the default) as though it had been asked for, and switch a flag on in
      # a different cluster.
      assert_raise Mix.Error, ~r/unknown option\(s\): --nod.*usage:/s, fn ->
        Flag.run(["--nod", "malachi@somewhere", "enable", "batch_format"])
      end
    end
  end
end
