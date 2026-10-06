defmodule Malachi.UI.TokenGen.Model do
  @moduledoc """
  Everything the emitters and the gates need, computed once from a parsed token file.

  Each token becomes one `t:entry/0`, in declaration order:

    * `web` is the CSS expression per theme as the stylesheet writes it: a reference stays a
      `var(--name)`, so a theme switch reaches every alias through the token it names.
    * `value` is the same expression fully resolved, which is what the snapshot and the Elixir module
      carry, because neither has a cascade to resolve a `var()` against.
    * `color`, for a color, is its authored OKLCH parsed and the sRGB bytes it shows after the clip.
    * `redeclare` says where else the stylesheet declares the token: under `.dark` when a theme changes
      its value (a themed color, or anything built from one), and in each `.density-*` class whose mode
      changes something it is built from. A `var()` is resolved on the element that declares it, so an
      alias declared only on `:root` would keep its light value inside a dark subtree.
    * `ansi256` and `ansi16`, for a terminal token, are its 256 color index per theme (declared in
      `$ansi256` or else quantised) and its hand declared sixteen color entry.

  Every emitter reads this and nothing else, so no two outputs can format or resolve a value
  differently.
  """

  alias Malachi.UI.TokenGen.{Color, Quantize, Source, Token}

  @enforce_keys [:entries, :source]
  defstruct [:entries, :source]

  @type theme :: :light | :dark
  @type themed(value) :: %{light: value, dark: value}

  @typedoc "A color in one theme: the parsed OKLCH, its alpha, its sRGB bytes and how far the clip moved it."
  @type color_data :: %{color: Color.t(), alpha: float(), srgb8: Color.srgb8(), clip_delta: float()}

  @type entry :: %{
          path: String.t(),
          name: String.t(),
          type: Token.type(),
          platforms: [Token.platform()],
          themed: boolean(),
          ref: String.t() | nil,
          web: themed(String.t()),
          value: themed(String.t()),
          color: themed(color_data()) | nil,
          ansi256: themed(16..255) | nil,
          quantised: themed(16..255) | nil,
          ansi16: Source.ansi16() | nil,
          inherited: boolean(),
          modes: [{String.t(), String.t()}],
          reduced_motion: String.t() | nil,
          redeclare: %{dark: boolean(), modes: [String.t()]}
        }

  @type t :: %__MODULE__{entries: [entry()], source: Source.t()}

  @themes [:light, :dark]

  @doc "Computes every entry from a parsed token file. The file must already have passed `Source.parse/1`."
  @spec build(Source.t()) :: t()
  def build(%Source{} = source) do
    index = Map.new(source.tokens, &{&1.path, &1})
    entries = Enum.map(source.tokens, &entry(&1, index, source))
    entries = Enum.map(entries, &Map.put(&1, :redeclare, redeclare(Map.fetch!(index, &1.path), index)))
    %__MODULE__{entries: entries, source: source}
  end

  @doc "The entry for a token path."
  @spec fetch!(t(), String.t()) :: entry()
  def fetch!(%__MODULE__{entries: entries}, path),
    do: Enum.find(entries, &(&1.path == path)) || raise(KeyError, key: path)

  defp entry(%Token{} = token, index, source) do
    tui? = :tui in token.platforms
    color = if token.type == :color, do: Map.new(@themes, &{&1, color_data(resolve(token, &1, index))})

    quantised = if tui?, do: Map.new(@themes, &{&1, Quantize.nearest(color[&1].srgb8)})
    declared = if tui?, do: Map.new(@themes, &{&1, Map.get(source.ansi256[&1], token.path)})
    ansi256 = if tui?, do: Map.new(@themes, &{&1, declared[&1] || quantised[&1]})

    %{
      path: token.path,
      name: token.name,
      type: token.type,
      platforms: token.platforms,
      themed: token.themed,
      ref: if(token.ref, do: Map.fetch!(index, token.ref).name),
      web: Map.new(@themes, &{&1, css(token, &1, index, :web)}),
      value: Map.new(@themes, &{&1, css(token, &1, index, :value)}),
      color: color,
      ansi256: ansi256,
      quantised: quantised,
      ansi16: Map.get(source.ansi16, token.path),
      inherited: token.path in Map.values(source.terminal.inherited),
      modes: for({mode, path} <- token.modes, do: {mode, var(Map.fetch!(index, path))}),
      reduced_motion: token.reduced_motion
    }
  end

  # A token is redeclared under .dark when it, or anything its value is made of, is a themed color, and
  # in a density class when anything its value is made of declares that mode. Its own modes are written
  # into those classes already, as their alternative values.
  defp redeclare(token, index) do
    %{
      dark: depends?(token, index, & &1.themed),
      modes:
        index
        |> Map.values()
        |> Enum.flat_map(fn t -> Enum.map(t.modes, &elem(&1, 0)) end)
        |> Enum.uniq()
        |> Enum.sort()
        |> Enum.reject(&has_mode?(token, &1))
        |> Enum.filter(fn mode -> built_from?(token, index, &has_mode?(&1, mode)) end)
    }
  end

  defp depends?(token, index, test), do: test.(token) or built_from?(token, index, test)

  defp built_from?(token, index, test) do
    token
    |> Source.value_references()
    |> Enum.any?(&depends?(Map.fetch!(index, &1), index, test))
  end

  defp has_mode?(token, mode), do: List.keymember?(token.modes, mode, 0)

  defp color_data(oklch) do
    {:ok, color} = Color.parse(oklch)
    %{color: color, alpha: color.alpha, srgb8: Color.to_srgb8(color), clip_delta: Color.clip_delta(color)}
  end

  # The authored value of a token in a theme, following whole value references to the end.
  defp resolve(%Token{themed: true, raw: raw}, theme, _index), do: Map.fetch!(raw, theme)
  defp resolve(%Token{ref: ref}, theme, index) when is_binary(ref), do: resolve(Map.fetch!(index, ref), theme, index)

  # A token as CSS. `:web` writes every reference as var(--name); `:value` resolves it.
  defp css(%Token{ref: ref}, _theme, index, :web) when is_binary(ref), do: var(Map.fetch!(index, ref))

  defp css(%Token{ref: ref}, theme, index, :value) when is_binary(ref),
    do: css(Map.fetch!(index, ref), theme, index, :value)

  defp css(%Token{themed: true, raw: raw}, theme, _index, _mode), do: Map.fetch!(raw, theme)

  defp css(%Token{type: type, raw: raw}, theme, index, mode) do
    format(type, raw, fn reference -> member(reference, theme, index, mode) end)
  end

  defp member(reference, theme, index, mode) do
    target = Map.fetch!(index, Source.reference(reference))
    if mode == :web, do: var(target), else: css(target, theme, index, :value)
  end

  defp var(%Token{name: name}), do: "var(--#{name})"

  defp format(type, value, _member) when type in [:dimension, :duration], do: value
  defp format(type, value, _member) when type in [:number, :font_weight], do: number(value)

  defp format(:font_family, names, _member), do: Enum.map_join(names, ", ", &font_name/1)

  defp format(:cubic_bezier, points, _member), do: "cubic-bezier(" <> Enum.map_join(points, ", ", &number/1) <> ")"

  defp format(:shadow, layers, member) do
    Enum.map_join(layers, ", ", fn layer ->
      Enum.join([layer["offsetX"], layer["offsetY"], layer["blur"], layer["spread"], member.(layer["color"])], " ")
    end)
  end

  defp format(:typography, value, member) do
    "#{member.(value["fontWeight"])} #{member.(value["fontSize"])}/#{member.(value["lineHeight"])} " <>
      member.(value["fontFamily"])
  end

  # A family name with a space is quoted; a single word, generic or not, is written bare.
  defp font_name(name), do: if(String.contains?(name, " "), do: ~s("#{name}"), else: name)

  @doc """
  A number as the token file and CSS both write it: an integer bare, a float in its shortest form.
  """
  @spec number(number()) :: String.t()
  def number(value) when is_integer(value), do: Integer.to_string(value)
  def number(value) when is_float(value), do: :erlang.float_to_binary(value, [:short])
end
