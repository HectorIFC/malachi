defmodule Malachi.UI.TokenGen.Lint do
  @moduledoc """
  The raw color literal gate: no interface source outside the generated files may spell a color.

  A component that writes `#0a0e27`, `oklch(`, or `Color::Red` has stepped outside the token file,
  and the shared design stops being provable. Every directory scanned must exist, so a moved project
  or a typo in the list fails instead of passing over nothing. A symbolic link under a scanned
  directory is a hit of its own rather than a silent skip, because the check does not follow one and
  whatever it points at would go unchecked. A Sass or Less stylesheet (`.scss`, `.sass`, `.less`) is a
  hit for the same reason: those languages have interpolation, mixins and indentation this scan does
  not model, and the console is shadcn/ui (operator-interfaces.md section 4.5) on Tailwind v4 with no
  preprocessor in its design, so adopting one is a decision to make explicitly rather than one the gate
  half checks. The same holds for a `<style lang="scss">` (or `sass`, `less`) block in a `.vue`,
  `.svelte` or `.html` file, which is reported alongside the rest of that file's colors, and extensions
  are compared without regard to case. The generated files are exempt by their exact path, not by a
  pattern, so a copy of one is not.

  What counts as a color:

    * `oklch(`, `oklab(`, `lch(`, `lab(`, `hwb(`, `rgb(`, `rgba(`, `hsl(` and `hsla(`, in any letter
      case, and `color(` followed by a predefined color space such as `display-p3`.
    * `Color::Rgb`, `Color::Indexed`, and every named ratatui color including `Color::Reset`.
    * A hex color of 3, 4, 6 or 8 digits:
      * in a stylesheet, anywhere in a declaration's value, so `border: 1px solid #f00` counts and an
        id selector such as `#add` does not, even after `a:hover,` and at any nesting depth;
      * in any other file, anywhere inside a string or template literal, or right after `:`, `,`,
        `(`, `[` or `=`.

  The file is read as a whole, so a value or a template literal that continues on the next line is
  still inside its declaration or its string. A `'` or `"` string ends at the end of its line, and in
  Rust only `"` opens one, so a lifetime such as `'a` does not.

  What does not count: an issue reference, which is a run of digits after `#` inside parentheses that
  hold only references, such as `(#194)` or `(#231, #233)`, and a hex in running prose. Inside a
  comment only the position rule applies, so a comment that says `it's tracked in #227` is not a
  color and one that says `was: #ff0000` is.
  """

  @extensions ~w(.css .ts .tsx .js .jsx .mjs .cjs .vue .svelte .html .json .rs)
  # Sass and Less have syntax of their own (interpolation, mixins, indentation) that this scan does not
  # model, so such a file is reported rather than read halfway (see the moduledoc).
  @dialects ~w(.scss .sass .less)
  @components ~w(.vue .svelte .html)
  @dialect_block ~r/<style\b[^>]*\blang\s*=\s*["']?(?:scss|sass|less)\b/i

  @functions ~r/\b(?:oklch|oklab|lch|lab|hwb|rgba?|hsla?)\(|\bcolor\(\s*(?:srgb-linear|srgb|display-p3|a98-rgb|prophoto-rgb|rec2020|xyz-d50|xyz-d65|xyz)\b/i
  @ratatui ~r/\bColor::(?:Rgb|Indexed|Reset|Black|Red|Green|Yellow|Blue|Magenta|Cyan|Gray|DarkGray|LightRed|LightGreen|LightYellow|LightBlue|LightMagenta|LightCyan|White)\b/
  @hex ~r/(?<![\w&#])#(?:[0-9a-fA-F]{8}|[0-9a-fA-F]{6}|[0-9a-fA-F]{3,4})(?![0-9A-Za-z_-])/
  @issue_list ~r/\A\s*#\d+(?:\s*,\s*#\d+)*\s*\z/
  @context [?:, ?,, ?(, ?[, ?=]
  @quotes [?", ?', ?`]

  @typedoc "A hit: the file relative to the root, its line (0 for a whole path), and what matched."
  @type hit :: {String.t(), non_neg_integer(), String.t()}

  @doc "Scans the directories under `root`, skipping the exempt files, and returns every hit in file and line order."
  @spec scan(Path.t(), [String.t()], [String.t()]) :: [hit()]
  def scan(root, directories, exempt) do
    Enum.flat_map(directories, fn directory ->
      full = Path.join(root, directory)

      if File.dir?(full) do
        full
        |> entries()
        |> Enum.map(fn {kind, path} -> {kind, Path.relative_to(path, root)} end)
        |> Enum.reject(fn {_kind, path} -> path in exempt end)
        |> Enum.sort_by(&elem(&1, 1))
        |> Enum.flat_map(fn
          {:link, path} -> [{path, 0, "symbolic link"}]
          {:dialect, path} -> [{path, 0, "unsupported stylesheet dialect"}]
          {:file, path} -> file_hits(root, path)
        end)
      else
        [{directory, 0, "missing directory"}]
      end
    end)
  end

  @doc "A hit as a message for a person."
  @spec describe(hit()) :: String.t()
  def describe({directory, 0, "missing directory"}),
    do: "#{directory}: the raw color literal check scans this directory, and it does not exist"

  def describe({path, 0, "unsupported stylesheet dialect"}),
    do:
      "#{path}: a Sass or Less stylesheet, which the raw color literal check does not read; " <>
        "the console is shadcn/ui on Tailwind v4 with no preprocessor in its design, so write CSS or decide on one explicitly"

  def describe({path, 0, "symbolic link"}),
    do:
      "#{path}: a symbolic link, which the raw color literal check does not follow; " <>
        "keep interface sources as real files under the scanned directories"

  def describe({file, line, match}),
    do: "#{file}:#{line}: raw color literal #{match}; use a token from docs/design/design-tokens.json"

  defp entries(directory) do
    directory
    |> File.ls!()
    |> Enum.sort()
    |> Enum.flat_map(fn name ->
      path = Path.join(directory, name)

      cond do
        match?({:ok, %File.Stat{type: :symlink}}, File.lstat(path)) -> [{:link, path}]
        File.dir?(path) -> entries(path)
        extension(path) in @dialects -> [{:dialect, path}]
        extension(path) in @extensions -> [{:file, path}]
        true -> []
      end
    end)
  end

  defp extension(path), do: path |> Path.extname() |> String.downcase()

  defp file_hits(root, relative) do
    text = root |> Path.join(relative) |> File.read!()

    # Only a component file holds a <style> block; a script or a stylesheet that merely mentions one
    # is read as itself. The block is reported alongside the file's own colors, never instead of them.
    block =
      if extension(relative) in @components and Regex.match?(@dialect_block, text),
        do: [{relative, 0, "unsupported stylesheet dialect"}],
        else: []

    block ++ color_hits(relative, text)
  end

  defp color_hits(relative, text) do
    kind = kind(relative)
    hexes = scan_matches(@hex, text)
    states = states_at(text, Enum.map(hexes, &elem(&1, 0)), kind)

    colors =
      for {offset, hex} <- hexes, hex_color?(text, offset, hex, Map.fetch!(states, offset), kind), do: {offset, hex}

    (scan_matches(@functions, text) ++ scan_matches(@ratatui, text) ++ colors)
    |> Enum.sort()
    |> Enum.map(fn {offset, match} -> {relative, line_of(text, offset), match} end)
  end

  defp kind(path) do
    cond do
      extension(path) == ".css" -> :stylesheet
      extension(path) == ".rs" -> :rust
      true -> :script
    end
  end

  defp scan_matches(regex, text) do
    for [{offset, length}] <- Regex.scan(regex, text, return: :index) do
      {offset, binary_part(text, offset, length)}
    end
  end

  defp line_of(text, offset), do: 1 + (text |> binary_part(0, offset) |> :binary.matches("\n") |> length())

  defp hex_color?(text, offset, hex, state, kind) do
    cond do
      issue_reference?(text, offset, hex) -> false
      state.mode in [:line_comment, :block_comment] -> state.last in @context
      kind == :stylesheet -> state.colon and not selector?(text, offset)
      match?({:string, _quote}, state.mode) -> true
      true -> state.last in @context
    end
  end

  # In a stylesheet the text since the last `{`, `;` or `}` is a declaration when it ends in `;`, `}` or
  # the end of the file, and a selector when it ends in `{`.
  defp selector?(text, offset) do
    rest = binary_part(text, offset, byte_size(text) - offset)

    case :binary.match(rest, ["{", ";", "}"]) do
      {at, 1} -> binary_part(rest, at, 1) == "{"
      :nomatch -> false
    end
  end

  # Digits after `#`, inside parentheses that hold nothing but such references.
  defp issue_reference?(text, offset, hex) do
    before = binary_part(text, 0, offset)
    rest = binary_part(text, offset + byte_size(hex), byte_size(text) - offset - byte_size(hex))

    with true <- Regex.match?(~r/\A#\d+\z/, hex),
         [_, opened] <- Regex.run(~r/\(([^()]*)\z/, before),
         [closed, _] <- String.split(rest, ")", parts: 2) do
      Regex.match?(@issue_list, opened <> hex <> closed)
    else
      _no -> false
    end
  end

  # One pass over the file, recording the scanner state at each offset in `targets` (ascending): whether
  # it is in code, a string, or a comment, the last character that was not whitespace, and, in a
  # stylesheet, whether it is inside a declaration's value.
  defp states_at(text, targets, kind) do
    initial = %{mode: :code, last: nil, colon: false}
    walk(text, 0, targets, initial, kind, %{})
  end

  defp walk(_text, _pos, [], _state, _kind, acc), do: acc

  # A target the walk has reached or stepped over (an escape consumes two bytes) takes the state here.
  defp walk(text, pos, [target | targets], state, kind, acc) when target <= pos,
    do: walk(text, pos, targets, state, kind, Map.put(acc, target, state))

  defp walk(text, pos, targets, state, kind, acc) do
    {width, state} = step(binary_part(text, pos, min(2, byte_size(text) - pos)), state, kind)
    walk(text, pos + width, targets, state, kind, acc)
  end

  defp step(<<"/*", _::binary>>, %{mode: :code} = state, _kind), do: {2, %{state | mode: :block_comment, last: ?*}}
  defp step(<<"*/", _::binary>>, %{mode: :block_comment} = state, _kind), do: {2, %{state | mode: :code, last: ?/}}

  defp step(<<"//", _::binary>>, %{mode: :code} = state, kind) when kind != :stylesheet,
    do: {2, %{state | mode: :line_comment, last: ?/}}

  defp step(<<?\n, _::binary>>, %{mode: :line_comment} = state, _kind), do: {1, %{state | mode: :code}}

  defp step(<<?\n, _::binary>>, %{mode: {:string, quote}} = state, _kind) when quote != ?`,
    do: {1, %{state | mode: :code}}

  defp step(<<?\\, _escaped, _::binary>>, %{mode: {:string, _}} = state, _kind), do: {2, state}

  defp step(<<quote, _::binary>>, %{mode: {:string, quote}} = state, _kind),
    do: {1, %{state | mode: :code, last: quote}}

  defp step(<<char, _::binary>>, %{mode: :code} = state, kind) do
    state = if quote?(char, kind), do: %{state | mode: {:string, char}}, else: rule(char, state, kind)
    {1, remember(char, state)}
  end

  defp step(<<char, _::binary>>, state, _kind), do: {1, remember(char, state)}

  defp quote?(char, :rust), do: char == ?"
  defp quote?(char, _kind), do: char in @quotes

  defp rule(char, state, :stylesheet) when char in [?{, ?}, ?;], do: %{state | colon: false}
  defp rule(?:, state, :stylesheet), do: %{state | colon: true}
  defp rule(_char, state, _kind), do: state

  defp remember(char, state) when char in [?\s, ?\t, ?\n, ?\r], do: state
  defp remember(char, state), do: %{state | last: char}
end
