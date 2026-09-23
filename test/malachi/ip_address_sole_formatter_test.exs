defmodule Malachi.IPAddressSoleFormatterTest do
  @moduledoc """
  The convention, enforced instead of remembered: `Malachi.IPAddress` is the only module in `lib/`
  that turns a socket peer into a string.

  Six modules used to do it three different ways, and the divergence was invisible: the client
  address is formatted once on the accept path and travels as a binary, so every other helper's
  tuple clauses were dead code that no test reached. The moment one call site formatted a fresh
  peername with the other implementation, one client split into two connection-limiter buckets, the
  lockout manager stopped recognising repeated attempts from that address, and the audit log carried
  one address in two spellings. None of that raises, and none of it fails a test.

  Unifying the six copies fixes today. This guard is what stops the seventh, which is the shape the
  problem takes next: not an edit to one of the six, but a new call site that reaches for
  `:inet.ntoa/1` inline because it is right there.

  The check reads the parsed file rather than its text, so a mention inside a string or a comment
  (this moduledoc, for one) is not a violation, and a call nested in a `case` clause is found the
  same way as one at the top of a function.
  """
  use ExUnit.Case, async: true

  # Reading a socket's peer and rendering an address are the two halves of the thing being
  # centralized. :inet.parse_address/1 is here too: it is the inverse, and a caller that parses
  # inline is a caller that has its own idea of what the string form looks like.
  @erlang_calls [
    {:inet, :ntoa},
    {:inet, :peername},
    {:inet, :parse_address},
    {:ssl, :peername}
  ]

  @canonical "lib/malachi/ip_address.ex"

  # Who may make which call, and why. Keyed by call rather than by file alone, so an exception grants
  # exactly what it argues for: SocketHelper is the transport dispatch that IPAddress.from_socket/2
  # reads the peer through, and it never turns the tuple into a string.
  @allowed %{
    @canonical => @erlang_calls,
    "lib/malachi/socket_helper.ex" => [{:inet, :peername}, {:ssl, :peername}]
  }

  defp lib_files, do: Path.wildcard("lib/**/*.ex")

  defp allowed?(path, module, fun), do: {module, fun} in Map.get(@allowed, path, [])

  # Every call to one of @erlang_calls in `source`, as `{line, module, fun}`.
  defp address_calls(source) do
    {:ok, ast} = Code.string_to_quoted(source)

    {_ast, calls} =
      Macro.prewalk(ast, [], fn
        {{:., _, [module, fun]}, meta, _args} = node, acc when is_atom(module) and is_atom(fun) ->
          if {module, fun} in @erlang_calls do
            {node, [{meta[:line], module, fun} | acc]}
          else
            {node, acc}
          end

        node, acc ->
          {node, acc}
      end)

    Enum.reverse(calls)
  end

  test "no module in lib/ formats or reads an address except Malachi.IPAddress" do
    violations =
      for path <- lib_files(),
          {line, module, fun} <- address_calls(File.read!(path)),
          not allowed?(path, module, fun) do
        "#{path}:#{line} calls #{inspect(module)}.#{fun}"
      end

    assert violations == [],
           """
           These call sites format or read a client address directly instead of going through
           Malachi.IPAddress, which is how the six divergent formatters happened:

           #{Enum.join(violations, "\n")}

           Use Malachi.IPAddress.from_socket/2 to read a peer, format/1 to render one, and parse/1 to
           read one back. If a new call genuinely belongs outside that module, add it to @allowed
           with the reason, so the exception is argued for in a diff.
           """
  end

  test "the guard can actually see these calls, so a pass is not an empty sweep" do
    # Without this, a rename of Malachi.IPAddress or a change to how calls are matched would leave
    # the test above passing over nothing at all and prove exactly as much.
    assert File.exists?(@canonical)

    found = @canonical |> File.read!() |> address_calls() |> Enum.map(fn {_line, m, f} -> {m, f} end)

    assert {:inet, :ntoa} in found
    assert {:inet, :parse_address} in found
  end

  test "a violation outside the allowed set is actually detected" do
    # The guard's own negative control: the detection is exercised on a sample whose answer is known,
    # so a pass above means the sweep works rather than that the matcher quietly stopped matching.
    source = """
    defmodule Fake do
      def peer(socket), do: socket |> :inet.peername() |> elem(1)
    end
    """

    assert [{2, :inet, :peername}] = address_calls(source)
    refute allowed?("lib/malachi/fake.ex", :inet, :peername)
  end
end
