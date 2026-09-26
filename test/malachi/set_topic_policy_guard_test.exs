defmodule Malachi.SetTopicPolicyGuardTest do
  @moduledoc """
  The invariant that lets `{:set_topic_policy, topic, name}` stay at machine version 0 after it was
  relaxed to accept a name absent from the vnode's own definitions.

  Relaxing what an existing command accepts is normally forbidden: a member on older code refuses the
  name while a newer one stores it, and the replicas diverge with no error anywhere. It is admissible
  only because no release can have written such an entry, and that holds only while nothing in `lib/`
  emits the command. This test is what makes that a build failure rather than an assumption.

  Whoever adds the first caller owes the command a new shape at a new machine version first (see
  `Malachi.Cluster.MachineVersion`).
  """
  use ExUnit.Case, async: true

  @command ":set_topic_policy"
  # The module that DEFINES the command: its type, its version table and the clauses that pattern match
  # on it all name it, and none of them can put one in a log. Every other module in `lib/` that names it
  # in code is a caller, which is the thing this guards against. A caller added inside `Malachi.Metadata`
  # itself would slip past, and it would be written next to the comment that explains why it must not be.
  @definition "lib/malachi/metadata.ex"

  test "nothing in lib/ emits set_topic_policy, which is what keeps it safe at machine version 0" do
    callers =
      "lib/**/*.ex"
      |> Path.wildcard()
      |> Enum.reject(&(&1 == @definition))
      |> Enum.flat_map(fn file ->
        file
        |> File.read!()
        |> String.split("\n")
        |> Enum.with_index(1)
        |> Enum.reject(fn {line, _number} -> comment?(line) end)
        |> Enum.filter(fn {line, _number} -> String.contains?(code_of(line), @command) end)
        |> Enum.map(fn {line, number} -> "#{file}:#{number}: #{String.trim(line)}" end)
      end)

    assert callers == [],
           """
           #{@command} is named in code outside #{@definition}, which breaks the invariant that keeps
           it appliable at machine version 0 after it was relaxed to accept a name the vnode's own
           definitions do not hold. A member on older code refuses that name while a newer one stores
           it, and the replicas then diverge with no error anywhere.

           Give the command a new shape at a new machine version before the caller ships, and see the
           comment beside the clause in Malachi.Metadata.

           #{Enum.join(callers, "\n")}
           """
  end

  # A comment line names the command without being able to emit it.
  defp comment?(line), do: line |> String.trim() |> String.starts_with?("#")

  # What a line says in CODE: everything outside a backtick span, because a doc or a comment that names
  # the command writes it inside one. The spans are removed rather than the whole line being skipped for
  # holding a backtick anywhere, which is what let a real caller through: a call followed by an inline
  # comment mentioning any other module in backticks was read as prose and never reported.
  defp code_of(line), do: String.replace(line, ~r/`[^`]*`/, "")
end
