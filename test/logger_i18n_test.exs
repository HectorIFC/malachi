defmodule Malachi.LoggerI18nTest do
  @moduledoc """
  The convention, enforced instead of remembered: every `Logger` call in `lib/` renders an
  `Malachi.I18n` key, and every key a call site names actually exists in both locales.

  Both halves fail silently without a guard. A plain string simply never reaches a translator, and
  `I18n.t/2` answers a missing key with the key's own name, so a typo ships as a log line reading
  `ring_publish_refused_clearng` and nothing complains. Converting eighteen call sites changed no test
  in this suite, which is the measure of how little log text is otherwise pinned down.
  """
  use ExUnit.Case, async: true

  alias Malachi.I18n

  @logger_call ~r/Logger\.(debug|info|warning|warn|error)\(/

  # The one deliberate exception, with its reason. `raise_or_warn/2` receives an already-translated
  # message because the same text is either raised or logged depending on the environment; translating
  # inside it would translate twice. Listed here rather than tolerated by a loose pattern, so the next
  # exception has to be argued for in a diff.
  @translated_by_caller %{"lib/malachi/tls_validator.ex" => ["Logger.warning(message)"]}

  defp lib_files, do: Path.wildcard("lib/**/*.ex") -- ["lib/malachi/i18n.ex"]

  # A Logger call spans several lines, so the check has to look at the whole call rather than the line
  # the macro starts on: a message on the next line would otherwise read as untranslated.
  defp logger_calls(file) do
    lines = file |> File.read!() |> String.split("\n")

    for {line, index} <- Enum.with_index(lines), Regex.match?(@logger_call, line) do
      {index + 1, call_text(lines, index)}
    end
  end

  # Consumes lines until the parentheses opened by the macro close again. The first line always leaves
  # depth at one or more, so there is no need to special-case it; halting on `depth <= 0` after folding
  # each line is what makes a call whose message sits on the next line read as one unit.
  defp call_text(lines, index) do
    lines
    |> Enum.drop(index)
    |> Enum.reduce_while({[], 0}, fn line, {acc, depth} ->
      depth = depth + count(line, "(") - count(line, ")")
      acc = [line | acc]
      if depth <= 0, do: {:halt, {acc, depth}}, else: {:cont, {acc, depth}}
    end)
    |> elem(0)
    |> Enum.reverse()
    |> Enum.join("\n")
  end

  defp count(string, char), do: string |> String.graphemes() |> Enum.count(&(&1 == char))

  defp allowed?(file, text) do
    @translated_by_caller
    |> Map.get(file, [])
    |> Enum.any?(&String.contains?(text, &1))
  end

  test "every Logger call in lib/ goes through I18n" do
    offenders =
      for file <- lib_files(),
          {line, text} <- logger_calls(file),
          not String.contains?(text, "I18n.t"),
          not allowed?(file, text),
          do: "#{file}:#{line}"

    assert offenders == [],
           "these Logger calls pass a raw string instead of an I18n key:\n  " <> Enum.join(offenders, "\n  ")
  end

  test "every I18n key named in lib/ exists, in every locale" do
    known = MapSet.new(I18n.keys())

    used =
      for file <- lib_files(),
          [_, key] <- Regex.scan(~r/I18n\.t\(:([a-z0-9_]+)/, File.read!(file)),
          do: {file, String.to_atom(key)}

    missing = for {file, key} <- used, not MapSet.member?(known, key), do: "#{key} (#{file})"

    assert missing == [],
           "I18n.t/2 answers an unknown key with the key itself, so these would ship as log text:\n  " <>
             Enum.join(missing, "\n  ")

    # A key defined in only one locale falls back silently, which is the same bug one language further
    # in. Read from the source rather than by switching locales: the locale is global application env,
    # so flipping it here would race every other test in the run.
    source = File.read!("lib/malachi/i18n.ex")

    untranslated =
      for {_file, key} <- Enum.uniq_by(used, &elem(&1, 1)),
          MapSet.member?(known, key),
          block = key_block(source, key),
          locale <- I18n.available_locales(),
          not String.contains?(block, "\"#{locale}\" =>"),
          do: "#{key} (#{locale})"

    assert untranslated == [], "keys with no text in a locale:\n  " <> Enum.join(untranslated, "\n  ")
  end

  # The `key: %{ ... }` entry as written in the translations map, up to the line that closes it.
  defp key_block(source, key) do
    case String.split(source, "\n    #{key}: %{", parts: 2) do
      [_before, rest] -> rest |> String.split("\n    },", parts: 2) |> hd()
      [_none] -> ""
    end
  end

  test "the sweep left exactly one documented exception, so the allowlist cannot quietly grow" do
    assert map_size(@translated_by_caller) == 1
  end
end
