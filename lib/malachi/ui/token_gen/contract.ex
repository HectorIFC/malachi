defmodule Malachi.UI.TokenGen.Contract do
  @moduledoc """
  The cross language contract: each generated file, read as its own language reads it, holds exactly
  the tokens in `docs/design/tokens.snapshot.json`, with the same values.

  Each check compares both ways, so a token missing from a file and a token a file declares that the
  snapshot lacks both fail, and so does a value that differs. The snapshot is language neutral on
  purpose: the expected CSS declaration, Rust expression and Elixir value are derived here from bytes,
  indexes and declared entries, not copied from the other generated files.

  The web console and the terminal interface add their own native contract tests when their projects
  land (#231, #233); these checks are the ones that hold before they exist.
  """

  @type result :: :ok | {:error, [String.t()]}

  @themes ~w(light dark)

  # -- tokens.css -----------------------------------------------------------------------------------

  @doc "Checks `tokens.css` against the snapshot."
  @spec css(String.t(), map()) :: result()
  def css(text, snapshot) do
    {blocks, parse_errors} = text |> strip_comments() |> css_blocks()
    web = for {name, token} <- snapshot["tokens"], "web" in token["platforms"], do: {name, token}

    expected_modes =
      Enum.reduce(web, %{}, fn {name, token}, acc ->
        own = Map.to_list(token["modes"] || %{})
        dependent = for mode <- token["redeclare"]["modes"], do: {mode, token["light"]["web"]}

        Enum.reduce(own ++ dependent, acc, fn {mode, expr}, acc ->
          Map.update(acc, mode, %{name => expr}, &Map.put(&1, name, expr))
        end)
      end)

    actual_modes = for {".density-" <> mode, declarations} <- blocks, into: %{}, do: {mode, declarations}

    mode_errors =
      for mode <- Enum.sort(Enum.uniq(Map.keys(expected_modes) ++ Map.keys(actual_modes))),
          error <-
            compare("tokens.css .density-#{mode}", Map.get(actual_modes, mode, %{}), Map.get(expected_modes, mode, %{})),
          do: error

    errors =
      parse_errors ++
        compare("tokens.css :root", block(blocks, ":root"), Map.new(web, fn {n, t} -> {n, t["light"]["web"]} end)) ++
        compare(
          "tokens.css .dark",
          block(blocks, ~s(.dark, [data-theme="dark"])),
          for({n, t} <- web, t["redeclare"]["dark"], into: %{}, do: {n, t["dark"]["web"]})
        ) ++
        mode_errors ++
        compare(
          "tokens.css reduced motion",
          block(blocks, "@media (prefers-reduced-motion: reduce) :root"),
          for({n, t} <- web, t["reducedMotion"], into: %{}, do: {n, t["reducedMotion"]})
        ) ++
        compare(
          "tokens.css @theme inline",
          block(blocks, "@theme inline"),
          for({n, t} <- web, t["type"] == "color", into: %{}, do: {"color-#{n}", "var(--#{n})"})
        )

    result(errors)
  end

  defp block(blocks, selector), do: blocks |> List.keyfind(selector, 0, {selector, %{}}) |> elem(1)

  defp strip_comments(text), do: Regex.replace(~r{/\*.*?\*/}s, text, "")

  # Reads top level blocks and, inside an at-rule, its nested blocks, keyed by their selector with
  # whitespace collapsed. A nested block's key is the at-rule followed by its own selector.
  defp css_blocks(text, outer \\ nil) do
    case Regex.run(~r/\A\s*([^{}]+?)\s*\{/s, text, return: :index) do
      nil ->
        if String.trim(text) == "",
          do: {[], []},
          else: {[], ["tokens.css: unreadable text #{inspect(String.slice(String.trim(text), 0, 40))}"]}

      [{0, open_end}, {sel_start, sel_len}] ->
        selector = text |> binary_part(sel_start, sel_len) |> String.split() |> Enum.join(" ")
        rest = binary_part(text, open_end, byte_size(text) - open_end)

        case matching_close(rest, 0, 0) do
          nil ->
            {[], ["tokens.css: the block #{selector} is never closed"]}

          close ->
            body = binary_part(rest, 0, close)
            after_block = binary_part(rest, close + 1, byte_size(rest) - close - 1)
            key = if outer, do: "#{outer} #{selector}", else: selector

            {here, here_errors} =
              if String.starts_with?(selector, "@media"),
                do: css_blocks(body, selector),
                else: {[{key, declarations(body)}], []}

            {more, more_errors} = css_blocks(after_block, outer)
            {here ++ more, here_errors ++ more_errors}
        end
    end
  end

  defp matching_close(<<"}", _::binary>>, 0, offset), do: offset
  defp matching_close(<<"}", rest::binary>>, depth, offset), do: matching_close(rest, depth - 1, offset + 1)
  defp matching_close(<<"{", rest::binary>>, depth, offset), do: matching_close(rest, depth + 1, offset + 1)
  defp matching_close(<<_, rest::binary>>, depth, offset), do: matching_close(rest, depth, offset + 1)
  defp matching_close(<<>>, _depth, _offset), do: nil

  defp declarations(body) do
    for [name, value] <- Regex.scan(~r/--([a-z0-9-]+)\s*:\s*([^;]*);/, body, capture: :all_but_first),
        into: %{},
        do: {name, String.trim(value)}
  end

  defp compare(label, actual, expected) do
    missing =
      for name <- Enum.sort(Map.keys(expected)), not Map.has_key?(actual, name), do: "#{label}: missing --#{name}"

    extra =
      for name <- Enum.sort(Map.keys(actual)),
          not Map.has_key?(expected, name),
          do: "#{label}: --#{name} is not in the snapshot"

    differ =
      for name <- Enum.sort(Map.keys(expected)),
          Map.has_key?(actual, name),
          actual[name] != expected[name],
          do: "#{label}: --#{name} is #{actual[name]}, the snapshot says #{expected[name]}"

    missing ++ extra ++ differ
  end

  # -- generated.rs ---------------------------------------------------------------------------------

  @doc "Checks `generated.rs` against the snapshot."
  @spec rust(String.t(), map()) :: result()
  def rust(text, snapshot) do
    tui =
      for {name, token} <- snapshot["tokens"], "tui" in token["platforms"], do: {String.replace(name, "-", "_"), token}

    expected = expected_consts(tui, snapshot)

    palette_fields = struct_fields(text, "Palette")
    surface_fields = struct_fields(text, "Surface")
    actual = consts(text)

    field_errors =
      for(f <- Enum.map(tui, &elem(&1, 0)), f not in palette_fields, do: "generated.rs Palette: missing field #{f}") ++
        for(
          f <- palette_fields,
          not List.keymember?(tui, f, 0),
          do: "generated.rs Palette: #{f} is not a terminal token"
        ) ++
        if(Enum.sort(surface_fields) == ~w(background foreground),
          do: [],
          else: ["generated.rs Surface: has exactly background and foreground"]
        )

    const_errors =
      for(
        name <- Enum.sort(Map.keys(expected)),
        not Map.has_key?(actual, name),
        do: "generated.rs: missing const #{name}"
      ) ++
        for(
          name <- Enum.sort(Map.keys(actual)),
          not Map.has_key?(expected, name),
          do: "generated.rs: const #{name} is not part of the contract"
        )

    value_errors =
      for {name, fields} <- Enum.sort(expected),
          Map.has_key?(actual, name),
          {field, want} <- Enum.sort(fields),
          got = Map.get(actual[name], field, :missing),
          got != want do
        if got == :missing,
          do: "generated.rs #{name}: missing #{field}",
          else: "generated.rs #{name}.#{field} is #{got}, the snapshot says #{want}"
      end

    result(field_errors ++ const_errors ++ value_errors)
  end

  defp expected_consts(tui, snapshot) do
    inherited = snapshot["terminal"]["inherited"]
    tokens = snapshot["tokens"]

    palettes =
      for theme <- @themes,
          {tier, value} <- [
            {"TRUECOLOR", &rgb(&1[theme]["srgb8"])},
            {"ANSI256", &"Color::Indexed(#{&1[theme]["ansi256"]})"}
          ],
          into: %{} do
        {"#{tier}_#{String.upcase(theme)}",
         Map.new(tui, fn {f, t} -> {f, if(t["inherited"], do: "Color::Reset", else: value.(t))} end)}
      end

    surfaces =
      @themes
      |> Enum.flat_map(fn theme ->
        roles = for role <- ~w(background foreground), do: {role, tokens[inherited[role]]}

        [
          {"PAINTED_TRUECOLOR_#{String.upcase(theme)}", Map.new(roles, fn {r, t} -> {r, rgb(t[theme]["srgb8"])} end)},
          {"PAINTED_ANSI256_#{String.upcase(theme)}",
           Map.new(roles, fn {r, t} -> {r, "Color::Indexed(#{t[theme]["ansi256"]})"} end)},
          {"PAINTED_ANSI16_#{String.upcase(theme)}",
           Map.new(roles, fn {r, _t} -> {r, snapshot["terminal"]["painted16"][theme][r]["ratatui"]} end)}
        ]
      end)
      |> Map.new()

    palettes
    |> Map.put("ANSI16", Map.new(tui, fn {f, t} -> {f, t["ansi16"]["ratatui"]} end))
    |> Map.merge(surfaces)
  end

  defp rgb([r, g, b]), do: "Color::Rgb(#{r}, #{g}, #{b})"

  defp struct_fields(text, name) do
    case Regex.run(~r/pub struct #{name} \{(.*?)\}/s, text, capture: :all_but_first) do
      [body] -> for [field] <- Regex.scan(~r/pub ([a-z0-9_]+): Color,/, body, capture: :all_but_first), do: field
      nil -> []
    end
  end

  defp consts(text) do
    for [name, body] <- Regex.scan(~r/pub const ([A-Z0-9_]+): \w+ = (.*?);/s, text, capture: :all_but_first),
        into: %{} do
      fields =
        for [field, value] <-
              Regex.scan(~r/([a-z0-9_]+): (Color::[A-Za-z]+(?:\([0-9, ]+\))?),/, body, capture: :all_but_first),
            into: %{},
            do: {field, value}

      {name, fields}
    end
  end

  # -- tokens.ex ------------------------------------------------------------------------------------

  @doc "Checks the source of `tokens.ex` against the snapshot, reading its `@tokens` literal."
  @spec elixir(String.t(), map()) :: result()
  def elixir(text, snapshot) do
    with {:ok, ast} <- parse(text),
         {:ok, tokens} <- tokens_literal(ast) do
      result(compare_elixir(tokens, snapshot))
    end
  end

  @doc "Checks a compiled tokens module, through its `all/0`, against the snapshot."
  @spec elixir_module(module(), map()) :: result()
  def elixir_module(module, snapshot), do: result(compare_elixir(module.all(), snapshot))

  defp parse(text) do
    case Code.string_to_quoted(text) do
      {:ok, ast} -> {:ok, ast}
      {:error, _reason} -> {:error, ["tokens.ex: does not parse"]}
    end
  end

  defp tokens_literal(ast) do
    {_ast, found} =
      Macro.prewalk(ast, nil, fn
        {:@, _, [{:tokens, _, [literal]}]} = node, nil -> {node, literal}
        node, acc -> {node, acc}
      end)

    if found && Macro.quoted_literal?(found) do
      {value, _binding} = Code.eval_quoted(found)
      {:ok, value}
    else
      {:error, ["tokens.ex: no @tokens literal"]}
    end
  end

  defp compare_elixir(tokens, snapshot) do
    expected =
      for {name, token} <- snapshot["tokens"], "elixir" in token["platforms"], into: %{} do
        {name, %{type: String.to_atom(token["type"]), light: token["light"]["value"], dark: token["dark"]["value"]}}
      end

    missing =
      for name <- Enum.sort(Map.keys(expected)), not Map.has_key?(tokens, name), do: "tokens.ex: missing #{name}"

    extra =
      for name <- Enum.sort(Map.keys(tokens)),
          not Map.has_key?(expected, name),
          do: "tokens.ex: #{name} is not in the snapshot"

    differ =
      for {name, want} <- Enum.sort(expected),
          got = Map.get(tokens, name),
          got != nil,
          key <- [:type, :light, :dark],
          Map.get(got, key) != want[key],
          do: "tokens.ex: #{name} #{key} is #{inspect(Map.get(got, key))}, the snapshot says #{inspect(want[key])}"

    missing ++ extra ++ differ
  end

  defp result([]), do: :ok
  defp result(errors), do: {:error, errors}
end
