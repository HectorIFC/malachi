defmodule Malachi.Test.Distribution do
  @moduledoc """
  Erlang distribution for the test suite: the name the test node takes and the names and lifecycle of the
  peer nodes the multinode tests start.

  Node names are unique per host, not per VM, because epmd is host global. Two `mix test` runs on one
  host (two worktrees, say) that both took a fixed name, or that both named peers with
  `System.unique_integer/1` (unique within one VM only, and reset on every run), would collide in epmd.
  So the test node is named after the operating system pid, which no other live process on the host
  holds, and every peer name starts with the test node's short name. `Malachi.CLI.RPC` names the CLI
  node the same way, for the same reason.
  """

  @host ~c"127.0.0.1"

  @doc """
  Makes the test node distributed, starting epmd if it is not running. A node that is already
  distributed keeps its name (`elixir --name ... -S mix test` still picks it); otherwise the node is
  named `malachi_test_<os pid>@127.0.0.1`. Idempotent.
  """
  @spec ensure_started() :: :ok
  def ensure_started do
    _ = System.cmd("epmd", ["-daemon"])

    if Node.alive?() do
      :ok
    else
      case :net_kernel.start([:"malachi_test_#{:os.getpid()}@#{@host}", :longnames]) do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _pid}} -> :ok
      end
    end
  end

  @doc """
  A peer name no other run on this host can hold: the test node's short name, then `prefix`, then an
  integer unique within this VM. The test node must already be distributed.
  """
  @spec peer_name(String.t()) :: atom()
  def peer_name(prefix) do
    [short, _host] = node() |> Atom.to_string() |> String.split("@", parts: 2)
    :"#{short}_#{prefix}_#{System.unique_integer([:positive])}"
  end

  @doc """
  Starts a peer named by `peer_name(prefix)`, linked to the calling test process and loaded with this
  node's code path, and stops it when the test exits. Must be called from a test or its setup (it
  registers an `ExUnit.Callbacks.on_exit/1`). Returns the peer's pid, node and short name.
  """
  @spec start_peer(String.t()) :: {pid(), node(), atom()}
  def start_peer(prefix) do
    name = peer_name(prefix)
    {:ok, peer, node} = :peer.start_link(%{name: name, host: @host, longnames: true})
    ExUnit.Callbacks.on_exit(fn -> stop_peer(peer) end)

    :ok = :erpc.call(node, :code, :add_paths, [:code.get_path()])
    {peer, node, name}
  end

  @doc "Stops `peer`, leaving one that is already down alone. Always `:ok`."
  @spec stop_peer(pid()) :: :ok
  def stop_peer(peer) do
    :peer.stop(peer)
  catch
    _kind, _reason -> :ok
  end
end
