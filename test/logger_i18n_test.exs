defmodule Malachi.LoggerI18nTest do
  @moduledoc """
  The convention, enforced instead of remembered: every `Logger` call in `lib/` renders an
  `Malachi.I18n` key as its **message**, and every key a call site names actually exists in both
  locales.

  Both halves fail silently without a guard. A raw string simply never reaches a translator, and
  `I18n.t/2` answers a missing key with the key's own name, so a typo ships as a log line reading
  `ring_publish_refused_clearng` and nothing complains. Converting eighteen call sites changed no test
  in this suite, which is the measure of how little log text is otherwise pinned down.

  The check reads the parsed file rather than the text of each call. Matching on source was wrong in
  two ways that a substring cannot tell apart: `Logger.warning("I18n.t was not called")` mentions the
  function in a raw message, and `Logger.warning("raw", detail: I18n.t(:key))` translates only its
  metadata. Both passed. Asking for the message argument specifically is the only way to be sure it is
  the message that got translated.
  """
  use ExUnit.Case, async: true

  alias Malachi.I18n

  # Every API that can emit a message, not just the four severities this codebase happens to use
  # today: a raw string through `Logger.notice/2` must not walk past the guard because no call site
  # has needed that level yet. `log/3` and `bare_log/3` take the level first, so their message is the
  # second argument.
  @emitting ~w(debug info notice warning warn error critical alert emergency log bare_log)a
  @level_first [:log, :bare_log]

  # The one deliberate exception, with its reason. `raise_or_warn/2` receives an already-translated
  # message because the same text is either raised or logged depending on the environment, so
  # translating inside it would translate twice. Keyed by the message argument as written, not by a
  # loose pattern, so the next exception has to be argued for in a diff.
  @translated_by_caller %{"lib/malachi/tls_validator.ex" => ["message"]}

  defp lib_files, do: Path.wildcard("lib/**/*.ex") -- ["lib/malachi/i18n.ex"]

  @doc false
  # Every Logger call in `source`, as `{line, message_ast}`. Reading the whole file rather than
  # slicing each call out of it is what makes the line numbers exact and the arguments real: a call
  # nested in a `case` clause or behind `do:` is found the same way as one at the top of a function.
  defp logger_calls(source) do
    {:ok, ast} = Code.string_to_quoted(source)

    {_ast, calls} =
      Macro.prewalk(ast, [], fn
        {{:., _, [{:__aliases__, _, [:Logger]}, fun]}, meta, args} = node, acc when fun in @emitting ->
          {node, [{meta[:line], message_argument(fun, args)} | acc]}

        node, acc ->
          {node, acc}
      end)

    Enum.reverse(calls)
  end

  defp message_argument(fun, args) when fun in @level_first, do: Enum.at(args, 1)
  defp message_argument(_fun, args), do: Enum.at(args, 0)

  # Anywhere inside the message expression, so a translated `if` or `case` branch still counts.
  defp calls_i18n?(nil), do: false

  defp calls_i18n?(ast) do
    {_ast, found?} =
      Macro.prewalk(ast, false, fn
        {{:., _, [{:__aliases__, _, aliased}, :t]}, _, _} = node, _acc
        when aliased in [[:I18n], [:Malachi, :I18n]] ->
          {node, true}

        node, acc ->
          {node, acc}
      end)

    found?
  end

  defp allowed?(file, message) do
    @translated_by_caller
    |> Map.get(file, [])
    |> Enum.member?(Macro.to_string(message))
  end

  # The raw messages in `source`, as written. Fixtures call this directly, so the check can be
  # exercised against calls that are not in the tree yet.
  defp untranslated(source) do
    for {_line, message} <- logger_calls(source), not calls_i18n?(message), do: Macro.to_string(message)
  end

  test "every Logger call in lib/ uses an I18n key as its message" do
    offenders =
      for file <- lib_files(),
          {line, message} <- logger_calls(File.read!(file)),
          not calls_i18n?(message),
          not allowed?(file, message),
          do: "#{file}:#{line}  #{Macro.to_string(message)}"

    assert offenders == [],
           "these Logger calls pass a raw message instead of an I18n key:\n  " <> Enum.join(offenders, "\n  ")
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

    untranslated_keys =
      for {_file, key} <- Enum.uniq_by(used, &elem(&1, 1)),
          MapSet.member?(known, key),
          block = key_block(source, key),
          locale <- I18n.available_locales(),
          not String.contains?(block, "\"#{locale}\" =>"),
          do: "#{key} (#{locale})"

    assert untranslated_keys == [],
           "keys with no text in a locale:\n  " <> Enum.join(untranslated_keys, "\n  ")
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

  describe "the check itself" do
    @severities ~w(debug info notice warning warn error critical alert emergency)

    test "reports a raw message at any severity, not just the four this codebase uses today" do
      for level <- @severities do
        assert untranslated(~s|Logger.#{level}("a raw string")|) == [~s|"a raw string"|],
               "Logger.#{level}/2 can emit a message and must not bypass the check"
      end
    end

    test "reads the level argument of log/3 and bare_log/3, so the message is the one inspected" do
      assert untranslated(~s|Logger.log(:info, "a raw string")|) == [~s|"a raw string"|]
      assert untranslated(~s|Logger.bare_log(:info, "a raw string")|) == [~s|"a raw string"|]
      assert untranslated(~s|Logger.log(:info, I18n.t(:some_key))|) == []
    end

    test "accepts a translated message at any severity" do
      for level <- @severities do
        assert untranslated(~s|Logger.#{level}(I18n.t(:some_key))|) == [],
               "a translated Logger.#{level}/2 must not be reported"
      end
    end

    test "is not fooled by the words I18n.t inside a raw message" do
      assert untranslated(~s|Logger.warning("I18n.t was not called")|) != [],
             "a substring match would have accepted this, which is how the check used to be written"
    end

    test "is not fooled by metadata that is translated while the message is not" do
      assert untranslated(~s|Logger.warning("raw", detail: I18n.t(:some_key))|) != [],
             "only the message argument counts; translated metadata does not make the line readable"
    end

    test "accepts a message whose translation sits inside a branch" do
      assert untranslated(~s|Logger.info(if flag?(), do: I18n.t(:a), else: I18n.t(:b))|) == []
    end

    test "finds a call nested in a case clause or behind do:, not only one standing alone" do
      nested = """
      defp report(x) do
        case x do
          :a -> Logger.warning(I18n.t(:key_a))
          :b -> Logger.warning("raw")
        end
      end
      """

      assert untranslated(nested) == [~s|"raw"|]
      assert untranslated(~s|defp f(x), do: Logger.info(I18n.t(:key, x: x))|) == []
    end

    test "reads a message continued across lines as one expression" do
      raw = """
      Logger.warning(
        "a raw string " <>
          "continued on another line"
      )
      """

      translated = """
      Logger.warning(
        I18n.t(:some_key, binding: value)
      )
      """

      assert untranslated(raw) != []
      assert untranslated(translated) == []
    end
  end
end
