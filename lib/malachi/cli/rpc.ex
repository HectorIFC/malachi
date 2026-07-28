defmodule Malachi.CLI.Rpc do
  @moduledoc """
  Shared Erlang-distribution/RPC plumbing for the operator mix tasks (`mix malachi.user`, `mix malachi.acl`):
  resolve the target node, bring this task's VM up as a distributed node and connect, and build the RPC
  **seam** the task's testable core calls through. Kept free of `Mix.*` (it returns `{:error, message}` rather
  than raising) so the tasks own their own output/exit behavior.
  """

  @default_node "malachi@127.0.0.1"
  @rpc_timeout 5_000
  # Connect budget: a fresh mix VM's distribution plus epmd churn from rapid successive tasks can take a
  # moment to settle, so retry generously (attempts x backoff) before giving up.
  @connect_attempts 30
  @connect_backoff_ms 200

  @doc "The target node: `--node`, else `$MALACHI_NODE`, else `malachi@127.0.0.1`."
  @spec target_node(keyword()) :: node()
  def target_node(opts) do
    (opts[:node] || System.get_env("MALACHI_NODE") || @default_node) |> String.to_atom()
  end

  @doc """
  Brings this task's VM up as a distributed node (if not already) and connects to `node`, applying `cookie`
  (or `$RELEASE_COOKIE`). Returns `:ok`, or `{:error, message}` when the node cannot be reached.
  """
  @spec connect(node(), String.t() | nil) :: :ok | {:error, String.t()}
  def connect(node, cookie) do
    with :ok <- ensure_distribution() do
      cookie_value = cookie || System.get_env("RELEASE_COOKIE")
      if cookie_value, do: Node.set_cookie(node, String.to_atom(cookie_value))

      if connect_with_retry(node, @connect_attempts) do
        :ok
      else
        {:error, "could not connect to #{node} (check --node/--cookie and that the node is running and named)"}
      end
    end
  end

  defp ensure_distribution do
    if Node.alive?() do
      :ok
    else
      # Name the task's node by the OS pid: each `mix` invocation is a fresh VM where
      # `System.unique_integer/1` resets, so two back-to-back tasks would otherwise both pick
      # `malachi_cli_1` and collide in epmd. The OS pid is distinct per invocation, so rapid successive tasks
      # never clash. A start failure (e.g. epmd not reachable) is surfaced, not raised.
      case Node.start(:"malachi_cli_#{:os.getpid()}@127.0.0.1", name_domain: :longnames) do
        {:ok, _pid} -> :ok
        {:error, reason} -> {:error, "could not start distribution (#{inspect(reason)}); is epmd running?"}
      end
    end
  end

  # A fresh node's distribution (and epmd, under rapid task churn) may not be ready the instant it boots, so
  # retry before giving up. `Node.connect/1` returns `true` (connected), `false` (unreachable), or `:ignored`
  # (local distribution not up yet); only `true` is done, the rest are retried while things settle.
  defp connect_with_retry(_node, 0), do: false

  defp connect_with_retry(node, attempts) do
    if Node.connect(node) == true do
      true
    else
      Process.sleep(@connect_backoff_ms)
      connect_with_retry(node, attempts - 1)
    end
  end

  @doc """
  The RPC seam: a `(module, fun, args -> {:ok, value} | {:error, reason})` function that calls `node` over
  Erlang distribution. The task's `execute/3` core calls through this, and tests substitute a fake.
  """
  @spec rpc(node()) :: (module(), atom(), list() -> {:ok, term()} | {:error, term()})
  def rpc(node) do
    fn module, fun, args ->
      case :rpc.call(node, module, fun, args, @rpc_timeout) do
        {:badrpc, reason} -> {:error, reason}
        value -> {:ok, value}
      end
    end
  end

  @doc "Formats an RPC transport failure for display."
  @spec rpc_error(term()) :: String.t()
  def rpc_error(reason), do: "rpc failed: #{inspect(reason)}"
end
