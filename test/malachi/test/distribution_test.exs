defmodule Malachi.Test.DistributionTest do
  # Every multinode test and the suite itself take their node names from this module, so a name that
  # collides with another run on the host (another worktree's suite) is what it exists to prevent.
  use ExUnit.Case, async: false

  alias Malachi.Test.Distribution

  # A fresh VM that loads only this project's compiled code and runs `expression`. Returns its output
  # and exit status. `name_args` names the VM up front (`--name ...`), or leaves it undistributed.
  defp child_vm(expression, name_args) do
    elixir = System.find_executable("elixir") || flunk("elixir is required to start a child VM")
    ebin = Path.dirname(:code.which(Distribution))
    System.cmd(elixir, name_args ++ ["-pa", ebin, "-e", expression], stderr_to_stdout: true)
  end

  describe "ensure_started/0" do
    # The collision it prevents is between two VMs on one host sharing one epmd, which is reproduced
    # (like the rest of Malachi's harnesses) on Linux only.
    @tag :linux
    test "names a second VM on this host after its own pid while this node holds its name" do
      assert Node.alive?()

      {output, status} = child_vm("Malachi.Test.Distribution.ensure_started(); IO.puts(node())", [])

      assert status == 0, output
      child = String.trim(output)
      assert child =~ ~r/\Amalachi_test_\d+@127\.0\.0\.1\z/
      refute child == Atom.to_string(node())
    end

    @tag :linux
    test "keeps the name of a VM that is already distributed" do
      name = "distribution_probe_#{System.unique_integer([:positive])}_#{:os.getpid()}@127.0.0.1"

      {output, status} =
        child_vm("Malachi.Test.Distribution.ensure_started(); IO.puts(node())", ["--name", name])

      assert status == 0, output
      assert String.trim(output) == name
    end

    test "leaves this node's name alone" do
      before = node()
      assert Distribution.ensure_started() == :ok
      assert node() == before
    end
  end

  describe "peer_name/1" do
    test "starts with this node's short name, then the prefix" do
      [short, _host] = node() |> Atom.to_string() |> String.split("@")
      name = Atom.to_string(Distribution.peer_name("probe"))

      assert String.starts_with?(name, "#{short}_probe_")
      refute Distribution.peer_name("probe") == Distribution.peer_name("probe")
    end
  end

  describe "start_peer/1 and stop_peer/1" do
    @describetag :multinode

    defp registered?(name) do
      {:ok, names} = :erl_epmd.names(~c"127.0.0.1")
      List.keymember?(names, Atom.to_charlist(name), 0)
    end

    # epmd drops a name when the node's connection to it closes, which it notices on its own schedule
    # after the peer's VM exits.
    defp unregistered?(name, attempts \\ 50) do
      cond do
        not registered?(name) ->
          true

        attempts == 0 ->
          false

        true ->
          Process.sleep(20)
          unregistered?(name, attempts - 1)
      end
    end

    test "starts a connected peer on this node's code path, and stopping it leaves epmd clean" do
      {peer, node, name} = Distribution.start_peer("dist_test")

      assert node == :"#{name}@127.0.0.1"
      assert :erpc.call(node, :code, :which, [Distribution]) == :code.which(Distribution)
      assert registered?(name)

      assert Distribution.stop_peer(peer) == :ok
      assert unregistered?(name)
    end

    test "stopping a peer that is already down is :ok" do
      {peer, _node, _name} = Distribution.start_peer("dist_test")

      assert Distribution.stop_peer(peer) == :ok
      assert Distribution.stop_peer(peer) == :ok
    end
  end
end
