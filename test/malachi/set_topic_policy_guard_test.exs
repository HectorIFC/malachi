defmodule Malachi.SetTopicPolicyGuardTest do
  @moduledoc """
  Nothing in `lib/` may name `{:set_topic_policy, topic, name}`.

  The command is not in `Malachi.Metadata`'s version table, so a member refuses it with the state
  unchanged, the same on every replica. A caller would therefore not break the build or the suite: it
  would break at runtime, with an unknown-command reply the caller may not expect. This test turns that
  into a build failure. The replacement is `Malachi.Policies.bind/3`, which emits `bind_topic_policy`.
  """
  use ExUnit.Case, async: true

  @command ":set_topic_policy"

  test "nothing in lib/ names set_topic_policy, which the metadata machine no longer accepts" do
    callers =
      "lib/**/*.ex"
      |> Path.wildcard()
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
           #{@command} is named in code under lib/. The metadata machine refuses it as an unknown command,
           so this caller would fail at runtime on every replica. Use Malachi.Policies.bind/3 instead,
           which emits bind_topic_policy.

           #{Enum.join(callers, "\n")}
           """
  end

  # A comment line names the command without being able to emit it.
  defp comment?(line), do: line |> String.trim() |> String.starts_with?("#")

  # What a line says in CODE: everything outside a backtick span, because a doc or a comment that names
  # the command writes it inside one. Removing the spans, rather than skipping any line with a backtick,
  # keeps a real call from slipping through because its line also mentions another module in backticks.
  defp code_of(line), do: String.replace(line, ~r/`[^`]*`/, "")
end
