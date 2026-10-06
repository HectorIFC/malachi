defmodule Malachi.UI.TokenGen.Source do
  @moduledoc """
  Reads `docs/design/design-tokens.json` and enforces every rule about its shape.

  This module is the only definition of what a valid token file is: there is no separate JSON
  Schema, because most of the rules are about meaning (a reference resolves to a token of the right
  type, a terminal token has a hand declared sixteen color entry, a contrast pair names two colors)
  and a schema cannot express them. Every rule that fails is reported, each message starting with the
  path of the token or section it concerns, so one run lists everything that needs fixing.

  Nothing here computes a color. `Malachi.UI.TokenGen.Model` does that from the `t:t/0` this returns,
  and `Malachi.UI.TokenGen.Gates` judges the result.

  ## The file

    * Every top level key that does not start with `$` is a token group. Groups nest; a group that
      holds tokens declares `$prefix`, and a token's name is that prefix followed by its key unless
      the token declares `$name`. `$type` is inherited down the tree.
    * A token is an object with `$value`, or, for a color only, `light` and `dark`. A `$value` may be
      a reference, `{path.to.token}`, which keeps the referenced token's type.
    * The sections are `$platforms`, `$contrast`, `$cvd`, `$apart`, `$gamut`, `$ansi16`, `$ansi256`
      and `$terminal`, all required, plus the free text `$description` and `$meta`.
    * The sixteen color tier is never inferred. A terminal token without an `$ansi16` entry is an
      error, not a gap this module fills.
  """

  alias Jason.OrderedObject
  alias Malachi.UI.TokenGen.{Color, Token}

  @enforce_keys [:tokens, :tui, :contrast, :cvd, :apart, :gamut, :ansi16, :ansi256, :terminal]
  defstruct @enforce_keys

  @typedoc "A sixteen color entry: an ANSI SGR color number, or `:default` to inherit, and its ratatui name."
  @type ansi16 :: %{code: 0..15 | :default, ratatui: String.t()}

  @type t :: %__MODULE__{
          tokens: [Token.t()],
          tui: [String.t()],
          contrast: [%{foreground: String.t(), background: String.t(), role: :text | :graphic, min: float()}],
          cvd: %{min: float(), sets: [[String.t()]]},
          apart: %{min: float(), sets: [%{these: [String.t()], from: [String.t()]}]},
          gamut: %{max: float()},
          ansi16: %{String.t() => ansi16()},
          ansi256: %{light: %{String.t() => 16..255}, dark: %{String.t() => 16..255}},
          terminal: %{
            inherited: %{background: String.t(), foreground: String.t()},
            distinct: [String.t()],
            painted16: %{
              light: %{background: ansi16(), foreground: ansi16()},
              dark: %{background: ansi16(), foreground: ansi16()}
            }
          }
        }

  @sections ~w($description $meta $platforms $contrast $cvd $apart $gamut $ansi16 $ansi256 $terminal)
  @required ~w($platforms $contrast $cvd $apart $gamut $ansi16 $ansi256 $terminal)

  @types %{
    "color" => :color,
    "dimension" => :dimension,
    "duration" => :duration,
    "fontFamily" => :font_family,
    "fontWeight" => :font_weight,
    "number" => :number,
    "cubicBezier" => :cubic_bezier,
    "shadow" => :shadow,
    "typography" => :typography
  }

  @leaf_keys ~w($value light dark $description $name $modes $reducedMotion)

  @shadow_keys ~w(offsetX offsetY blur spread color)

  # The WCAG 2.2 floors, fixed here rather than authored per pair, so a pair cannot be given a weaker one:
  # text against its background is 1.4.3 (4.5), a mark a reader must see (a state, a chart series, a
  # control border) is 1.4.11 (3).
  @contrast_floors %{"text" => {:text, 4.5}, "graphic" => {:graphic, 3.0}}
  @pair_shape "$contrast: a pair has exactly foreground, background and role"
  @typography_members %{
    "fontWeight" => :font_weight,
    "fontSize" => :dimension,
    "lineHeight" => :number,
    "fontFamily" => :font_family
  }

  @ansi_names {"Color::Black", "Color::Red", "Color::Green", "Color::Yellow", "Color::Blue", "Color::Magenta",
               "Color::Cyan", "Color::Gray", "Color::DarkGray", "Color::LightRed", "Color::LightGreen",
               "Color::LightYellow", "Color::LightBlue", "Color::LightMagenta", "Color::LightCyan", "Color::White"}

  @name ~r/\A[a-z][a-z0-9]*(?:-[a-z0-9]+)*\z/
  @prefix ~r/\A(?:[a-z][a-z0-9]*(?:-[a-z0-9]+)*-)?\z/
  @reference ~r/\A\{([a-z0-9]+(?:[.-][a-z0-9]+)*)\}\z/
  @dimension ~r/\A-?\d+(?:\.\d+)?(?:px|em|rem)\z/
  @duration ~r/\A\d+ms\z/
  @font_name ~r/\A-?[A-Za-z0-9][A-Za-z0-9 -]*\z/

  @doc "The ratatui color a sixteen color code stands for; `:default` is `Color::Reset`."
  @spec ansi_name(0..15 | :default) :: String.t()
  def ansi_name(:default), do: "Color::Reset"
  def ansi_name(code) when code in 0..15, do: elem(@ansi_names, code)

  @doc "Decodes a token file and parses it."
  @spec load(String.t()) :: {:ok, t()} | {:error, [String.t()]}
  def load(text) do
    case Jason.decode(text, objects: :ordered_objects) do
      {:ok, %OrderedObject{} = root} -> parse(root)
      {:ok, _other} -> {:error, ["the token file must be a JSON object"]}
      {:error, error} -> {:error, ["the token file is not valid JSON: " <> Exception.message(error)]}
    end
  end

  @doc "Parses a decoded token file, with its objects in declaration order."
  @spec parse(OrderedObject.t()) :: {:ok, t()} | {:error, [String.t()]}
  def parse(%OrderedObject{values: entries} = root) do
    {sections, groups} = Enum.split_with(entries, fn {key, _value} -> String.starts_with?(key, "$") end)

    {tokens, token_errors} = collect_tokens(groups)
    index = Map.new(tokens, &{&1.path, &1})

    {platforms, platform_errors} = parse_platforms(get(root, "$platforms"), groups, index)
    tokens = Enum.map(tokens, &assign_platforms(&1, platforms))
    index = Map.new(tokens, &{&1.path, &1})

    {terminal, terminal_errors} = parse_terminal(get(root, "$terminal"), platforms.tui)
    {contrast, contrast_errors} = parse_contrast(get(root, "$contrast"), index)
    {cvd, cvd_errors} = parse_cvd(get(root, "$cvd"), index)
    {apart, apart_errors} = parse_apart(get(root, "$apart"), index)
    {gamut, gamut_errors} = parse_gamut(get(root, "$gamut"))
    {ansi16, ansi16_errors} = parse_ansi16(get(root, "$ansi16"), platforms.tui, terminal)
    {ansi256, ansi256_errors} = parse_ansi256(get(root, "$ansi256"), platforms.tui)

    errors =
      section_errors(sections) ++
        token_errors ++
        name_errors(tokens) ++
        reference_errors(tokens, index) ++
        platform_errors ++
        terminal_errors ++
        contrast_errors ++ cvd_errors ++ apart_errors ++ gamut_errors ++ ansi16_errors ++ ansi256_errors

    case errors do
      [] ->
        {:ok,
         %__MODULE__{
           tokens: tokens,
           tui: platforms.tui,
           contrast: contrast,
           cvd: cvd,
           apart: apart,
           gamut: gamut,
           ansi16: ansi16,
           ansi256: ansi256,
           terminal: terminal
         }}

      errors ->
        {:error, errors}
    end
  end

  # -- Sections -----------------------------------------------------------------------------------

  defp section_errors(sections) do
    present = Enum.map(sections, &elem(&1, 0))
    unknown = for key <- present, key not in @sections, do: "#{key}: unknown section"
    missing = for key <- @required, key not in present, do: "#{key}: missing"

    # A required section must be an object: null or any other value would otherwise read as an empty
    # section, and an empty $contrast or $cvd gates nothing.
    shapeless =
      for {key, value} <- sections,
          key in @required,
          not match?(%OrderedObject{}, value),
          do: "#{key}: must be an object"

    unknown ++ missing ++ shapeless
  end

  # -- Tokens -------------------------------------------------------------------------------------

  defp collect_tokens(groups) do
    groups
    |> Enum.map(fn {name, node} -> walk_group(name, node) end)
    |> merge()
  end

  defp walk_group(path, %OrderedObject{} = node), do: walk(node, path, path, nil)
  defp walk_group(path, _node), do: {[], ["#{path}: must be an object"]}

  defp walk(%OrderedObject{values: entries}, path, group, inherited_type) do
    {meta, children} = Enum.split_with(entries, fn {key, _value} -> String.starts_with?(key, "$") end)
    {type, prefix, meta_errors} = group_meta(meta, path, inherited_type)

    results =
      Enum.map(children, fn {key, child} ->
        child_path = "#{path}.#{key}"

        cond do
          not match?(%OrderedObject{}, child) -> {[], ["#{child_path}: must be an object"]}
          leaf?(child) -> leaf(child, child_path, key, group, type, prefix || "")
          true -> walk(child, child_path, group, type)
        end
      end)

    holds_tokens? = Enum.any?(children, fn {_key, child} -> match?(%OrderedObject{}, child) and leaf?(child) end)
    prefix_errors = if holds_tokens? and is_nil(prefix), do: ["#{path}: holds tokens but declares no $prefix"], else: []

    {tokens, errors} = merge(results)
    {tokens, meta_errors ++ prefix_errors ++ errors}
  end

  defp group_meta(meta, path, inherited_type) do
    Enum.reduce(meta, {inherited_type, nil, []}, fn
      {"$type", value}, {type, prefix, errors} ->
        case Map.fetch(@types, value) do
          {:ok, parsed} -> {parsed, prefix, errors}
          :error -> {type, prefix, errors ++ ["#{path}: unknown $type #{inspect(value)}"]}
        end

      {"$prefix", value}, {type, _prefix, errors} ->
        if is_binary(value) and Regex.match?(@prefix, value) do
          {type, value, errors}
        else
          message = "#{path}: $prefix #{inspect(value)} must be empty or a lowercase name ending in a dash"
          {type, "", errors ++ [message]}
        end

      {"$description", _value}, acc ->
        acc

      {key, _value}, {type, prefix, errors} ->
        {type, prefix, errors ++ ["#{path}: unknown key #{key}"]}
    end)
  end

  defp leaf?(%OrderedObject{values: entries}),
    do: Enum.any?(entries, fn {key, _} -> key in ["$value", "light", "dark"] end)

  defp leaf(node, path, key, group, type, prefix) do
    unknown = for {k, _} <- node.values, k not in @leaf_keys, do: "#{path}: unknown key #{k}"

    if is_nil(type) do
      {[], unknown ++ ["#{path}: no $type, on the token or on a group above it"]}
    else
      name = get(node, "$name") || prefix <> key
      {raw, themed, ref, value_errors} = leaf_value(node, path, type)
      {modes, mode_errors} = leaf_modes(get(node, "$modes"), path)
      {reduced, reduced_errors} = leaf_reduced_motion(get(node, "$reducedMotion"), path, type)

      name_errors =
        if is_binary(name) and Regex.match?(@name, name),
          do: [],
          else: ["#{path}: name #{inspect(name)} is not a lowercase CSS identifier"]

      token = %Token{
        path: path,
        name: if(is_binary(name), do: name, else: inspect(name)),
        type: type,
        group: group,
        platforms: [],
        themed: themed,
        raw: raw,
        ref: ref,
        modes: modes,
        reduced_motion: reduced
      }

      {[token], unknown ++ name_errors ++ value_errors ++ mode_errors ++ reduced_errors}
    end
  end

  defp leaf_value(node, path, type) do
    light = get(node, "light")
    dark = get(node, "dark")
    value = node |> get("$value") |> plain()

    if is_nil(light) and is_nil(dark),
      do: plain_value(value, path, type),
      else: themed_value(light, dark, value, path, type)
  end

  defp themed_value(_light, _dark, _value, path, type) when type != :color,
    do: {nil, false, nil, ["#{path}: only a color declares light and dark"]}

  defp themed_value(_light, _dark, value, path, _type) when not is_nil(value),
    do: {nil, true, nil, ["#{path}: declares both $value and light and dark"]}

  defp themed_value(nil, _dark, _value, path, _type), do: {nil, true, nil, ["#{path}: declares dark without light"]}
  defp themed_value(_light, nil, _value, path, _type), do: {nil, true, nil, ["#{path}: declares light without dark"]}

  defp themed_value(light, dark, _value, path, _type) do
    errors = color_errors(path, "light", light) ++ color_errors(path, "dark", dark)
    {%{light: light, dark: dark}, true, nil, errors}
  end

  defp plain_value(value, path, type) do
    case {reference(value), type} do
      {ref, _type} when is_binary(ref) ->
        {value, false, ref, []}

      {nil, :color} ->
        {value, false, nil, ["#{path}: a color declares light and dark, or references another color"]}

      {nil, type} ->
        {value, false, nil, Enum.map(value_errors(type, value), &"#{path}: #{&1}")}
    end
  end

  defp color_errors(path, theme, value) when is_binary(value) do
    case Color.parse(value) do
      {:ok, _color} -> []
      {:error, reason} -> ["#{path} (#{theme}): #{reason}"]
    end
  end

  defp color_errors(path, theme, value), do: ["#{path} (#{theme}): #{inspect(value)} is not a string"]

  defp value_errors(:dimension, value),
    do: format_errors(dimension?(value), "#{inspect(value)} is not a dimension (a number with px, em or rem)")

  defp value_errors(:duration, value),
    do: format_errors(duration?(value), "#{inspect(value)} is not a duration (a whole number of ms)")

  defp value_errors(:number, value),
    do: format_errors(is_number(value) and value >= 0, "#{inspect(value)} is not a number")

  defp value_errors(:font_weight, value) do
    valid? = is_integer(value) and value in 100..900 and rem(value, 100) == 0
    format_errors(valid?, "#{inspect(value)} is not a font weight (100 to 900 in steps of 100)")
  end

  defp value_errors(:font_family, [_ | _] = names) do
    for name <- names, not (is_binary(name) and Regex.match?(@font_name, name)) do
      "font name #{inspect(name)} may only hold letters, digits, spaces and dashes, and may start with one dash"
    end
  end

  defp value_errors(:font_family, _value), do: ["a font family is a non empty list of font names"]

  defp value_errors(:cubic_bezier, [x1, y1, x2, y2] = points) do
    valid? =
      Enum.all?(points, &is_number/1) and x1 >= 0 and x1 <= 1 and x2 >= 0 and x2 <= 1 and is_number(y1) and
        is_number(y2)

    format_errors(valid?, cubic_bezier_message())
  end

  defp value_errors(:cubic_bezier, _value), do: [cubic_bezier_message()]

  defp value_errors(:shadow, [_ | _] = layers), do: Enum.flat_map(layers, &shadow_layer_errors/1)
  defp value_errors(:shadow, _value), do: ["a shadow is a non empty list of layers"]

  defp value_errors(:typography, %{} = value) do
    keys = Map.keys(value) |> Enum.sort()
    expected = @typography_members |> Map.keys() |> Enum.sort()
    valid? = keys == expected and Enum.all?(Map.values(value), &reference/1)

    format_errors(
      valid?,
      "a typography value has exactly fontWeight, fontSize, lineHeight and fontFamily, each a reference"
    )
  end

  defp value_errors(:typography, _value),
    do: ["a typography value has exactly fontWeight, fontSize, lineHeight and fontFamily, each a reference"]

  defp cubic_bezier_message, do: "a cubic bezier is four numbers, the first and third between 0 and 1"

  defp shadow_layer_errors(%{} = layer) do
    if Enum.sort(Map.keys(layer)) == Enum.sort(@shadow_keys) do
      dimensions =
        for key <- ~w(offsetX offsetY blur spread), not dimension?(layer[key]) do
          "shadow #{key} #{inspect(layer[key])} is not a dimension"
        end

      blur =
        if dimension?(layer["blur"]) and String.starts_with?(layer["blur"], "-"),
          do: ["shadow blur may not be negative"],
          else: []

      color = if reference(layer["color"]), do: [], else: ["a shadow color is a reference to a color token"]
      dimensions ++ blur ++ color
    else
      ["a shadow layer has exactly offsetX, offsetY, blur, spread and color"]
    end
  end

  defp shadow_layer_errors(_layer), do: ["a shadow layer has exactly offsetX, offsetY, blur, spread and color"]

  defp format_errors(true, _message), do: []
  defp format_errors(false, message), do: [message]

  defp dimension?(value), do: is_binary(value) and Regex.match?(@dimension, value)
  defp duration?(value), do: is_binary(value) and Regex.match?(@duration, value)

  defp leaf_modes(nil, _path), do: {[], []}

  defp leaf_modes(%OrderedObject{values: entries}, path) do
    results =
      Enum.map(entries, fn {mode, value} ->
        cond do
          not Regex.match?(@name, mode) -> {nil, ["#{path}: mode #{inspect(mode)} is not a lowercase identifier"]}
          ref = reference(value) -> {{mode, ref}, []}
          true -> {nil, ["#{path}: mode #{mode} must reference a token"]}
        end
      end)

    {results |> Enum.map(&elem(&1, 0)) |> Enum.reject(&is_nil/1), Enum.flat_map(results, &elem(&1, 1))}
  end

  defp leaf_modes(_value, path), do: {[], ["#{path}: $modes must be an object of mode names and references"]}

  defp leaf_reduced_motion(nil, _path, _type), do: {nil, []}

  defp leaf_reduced_motion(value, path, :duration) do
    if duration?(value),
      do: {value, []},
      else: {nil, ["#{path}: $reducedMotion #{inspect(value)} is not a duration"]}
  end

  defp leaf_reduced_motion(_value, path, _type), do: {nil, ["#{path}: only a duration declares $reducedMotion"]}

  defp name_errors(tokens) do
    tokens
    |> Enum.group_by(& &1.name, & &1.path)
    |> Enum.filter(fn {_name, paths} -> length(paths) > 1 end)
    |> Enum.sort()
    |> Enum.map(fn {name, paths} -> "#{name}: declared by both #{Enum.join(paths, " and ")}" end)
  end

  # -- References ---------------------------------------------------------------------------------

  defp reference_errors(tokens, index) do
    usage_errors =
      for token <- tokens, {ref, expected} <- references(token) do
        case Map.fetch(index, ref) do
          :error -> "#{token.path}: references #{ref}, which does not exist"
          {:ok, %Token{type: ^expected}} -> nil
          {:ok, target} -> "#{token.path}: references #{ref}, a #{target.type}, where a #{expected} belongs"
        end
      end

    cycle_errors =
      for token <- tokens, token.ref, chain = cycle(token, index), not is_nil(chain) do
        "#{token.path}: reference cycle through #{Enum.join(chain, ", ")}"
      end

    Enum.reject(usage_errors, &is_nil/1) ++ cycle_errors
  end

  defp references(%Token{} = token) do
    whole = if token.ref, do: [{token.ref, token.type}], else: []
    modes = for {_mode, ref} <- token.modes, do: {ref, token.type}
    whole ++ modes ++ composite_references(token)
  end

  @doc """
  The tokens a token's value is made of: the one it references whole, or each one a shadow or a
  typography value references. Not its `$modes`, which replace the value rather than build it.
  """
  @spec value_references(Token.t()) :: [String.t()]
  def value_references(%Token{} = token) do
    whole = if token.ref, do: [token.ref], else: []
    whole ++ Enum.map(composite_references(token), &elem(&1, 0))
  end

  defp composite_references(%Token{type: :shadow, ref: nil, raw: layers}) when is_list(layers) do
    for %{"color" => color} <- layers, ref = reference(color), do: {ref, :color}
  end

  defp composite_references(%Token{type: :typography, ref: nil, raw: %{} = value}) do
    for {member, type} <- @typography_members, ref = reference(value[member]), do: {ref, type}
  end

  defp composite_references(_token), do: []

  defp cycle(token, index), do: follow(token.ref, index, [token.path])

  defp follow(nil, _index, _seen), do: nil

  defp follow(path, index, seen) do
    if path in seen,
      do: Enum.reverse([path | seen]),
      else: index |> Map.get(path) |> next_ref() |> follow(index, [path | seen])
  end

  defp next_ref(%Token{ref: ref}), do: ref
  defp next_ref(nil), do: nil

  @doc false
  @spec reference(term()) :: String.t() | nil
  def reference(value) when is_binary(value) do
    case Regex.run(@reference, value, capture: :all_but_first) do
      [path] -> path
      nil -> nil
    end
  end

  def reference(_value), do: nil

  # -- Platforms ----------------------------------------------------------------------------------

  defp parse_platforms(%OrderedObject{} = platforms, groups, index) do
    known = Enum.map(groups, &elem(&1, 0))

    unknown =
      for {key, _} <- platforms.values,
          key not in ~w($description web elixir tui),
          do: "$platforms: unknown platform #{key}"

    missing = for key <- ~w(web elixir tui), is_nil(get(platforms, key)), do: "$platforms: missing platform #{key}"

    {web, web_errors} = platform_groups(get(platforms, "web"), "web", known)
    {elixir, elixir_errors} = platform_groups(get(platforms, "elixir"), "elixir", known)
    {tui, tui_errors} = platform_tokens(get(platforms, "tui"), index)

    uncovered =
      for group <- known, group not in web and group not in elixir, do: "#{group}: no platform carries this group"

    {%{web: web, elixir: elixir, tui: tui},
     unknown ++ missing ++ web_errors ++ elixir_errors ++ tui_errors ++ uncovered}
  end

  defp parse_platforms(_platforms, _groups, _index), do: {%{web: [], elixir: [], tui: []}, []}

  defp platform_groups(%OrderedObject{} = platform, name, known) do
    case get(platform, "groups") do
      groups when is_list(groups) ->
        errors = for group <- groups, group not in known, do: "$platforms.#{name}: no group #{format_path(group)}"

        {Enum.filter(groups, &(&1 in known)),
         unknown_keys(platform, ~w($description groups), "$platforms.#{name}") ++ errors}

      _other ->
        {[], ["$platforms.#{name}: groups must be a list of token groups"]}
    end
  end

  defp platform_groups(nil, _name, _known), do: {[], []}
  defp platform_groups(_platform, name, _known), do: {[], ["$platforms.#{name}: must be an object"]}

  defp platform_tokens(%OrderedObject{} = platform, index) do
    case get(platform, "tokens") do
      paths when is_list(paths) ->
        errors =
          Enum.flat_map(paths, fn path ->
            case Map.get(index, path) do
              %Token{type: :color} = token -> alpha_errors(token, index)
              _other -> ["$platforms.tui: #{format_path(path)} is not a color token"]
            end
          end)

        {paths, unknown_keys(platform, ~w($description tokens), "$platforms.tui") ++ errors}

      _other ->
        {[], ["$platforms.tui: tokens must be a list of color token paths"]}
    end
  end

  defp platform_tokens(nil, _index), do: {[], []}
  defp platform_tokens(_platform, _index), do: {[], ["$platforms.tui: must be an object"]}

  defp alpha_errors(token, index) do
    case themed_raw(token, index, []) do
      %{light: light, dark: dark} ->
        if Enum.all?([light, dark], &opaque?/1),
          do: [],
          else: ["$platforms.tui: #{token.path} carries alpha, which a terminal cannot show"]

      nil ->
        []
    end
  end

  defp opaque?(value) do
    case Color.parse(value) do
      {:ok, %Color{alpha: alpha}} -> alpha == 1.0
      {:error, _reason} -> true
    end
  end

  defp themed_raw(%Token{themed: true, raw: raw}, _index, _seen), do: raw

  defp themed_raw(%Token{ref: ref, path: path}, index, seen) when is_binary(ref) do
    with false <- ref in seen, %Token{} = target <- Map.get(index, ref) do
      themed_raw(target, index, [path | seen])
    else
      _other -> nil
    end
  end

  defp themed_raw(_token, _index, _seen), do: nil

  defp assign_platforms(%Token{} = token, platforms) do
    on = [
      web: token.group in platforms.web,
      elixir: token.group in platforms.elixir,
      tui: token.path in platforms.tui
    ]

    %{token | platforms: for({platform, true} <- on, do: platform)}
  end

  # -- $contrast, $cvd, $gamut --------------------------------------------------------------------

  defp parse_contrast(%OrderedObject{} = contrast, index) do
    case get(contrast, "pairs") do
      pairs when is_list(pairs) ->
        results = Enum.map(pairs, &contrast_pair(&1, index))
        pairs = for {:ok, pair} <- results, do: pair
        errors = for {:error, errors} <- results, error <- errors, do: error
        {pairs, unknown_keys(contrast, ~w($description pairs), "$contrast") ++ errors}

      _other ->
        {[], ["$contrast: pairs must be a list"]}
    end
  end

  defp parse_contrast(_contrast, _index), do: {[], []}

  defp contrast_pair(%OrderedObject{values: entries} = pair, index) do
    if entries |> Enum.map(&elem(&1, 0)) |> Enum.sort() == ~w(background foreground role) do
      foreground = get(pair, "foreground")
      background = get(pair, "background")
      role = get(pair, "role")

      errors =
        color_path_errors("$contrast", foreground, index) ++
          color_path_errors("$contrast", background, index) ++
          if(Map.has_key?(@contrast_floors, role),
            do: [],
            else: ["$contrast: role #{inspect(role)} is not text or graphic"]
          )

      if errors == [] do
        {name, floor} = Map.fetch!(@contrast_floors, role)
        {:ok, %{foreground: foreground, background: background, role: name, min: floor}}
      else
        {:error, errors}
      end
    else
      {:error, [@pair_shape]}
    end
  end

  defp contrast_pair(_pair, _index), do: {:error, [@pair_shape]}

  defp parse_cvd(%OrderedObject{} = cvd, index) do
    deficiency = get(cvd, "deficiency")
    min = get(cvd, "minDeltaEOK")
    sets = get(cvd, "sets")

    deficiency_errors =
      if deficiency == "deuteranopia",
        do: [],
        else: ["$cvd: deficiency #{inspect(deficiency)} is not simulated; only \"deuteranopia\" is"]

    min_errors = if positive?(min), do: [], else: ["$cvd: minDeltaEOK #{inspect(min)} must be a positive number"]

    {sets, set_errors} =
      if is_list(sets) do
        errors =
          Enum.flat_map(sets, fn
            [_, _ | _] = set -> Enum.flat_map(set, &color_path_errors("$cvd", &1, index))
            _set -> ["$cvd: a set names at least two color tokens"]
          end)

        {sets, errors}
      else
        {[], ["$cvd: sets must be a list of lists of color tokens"]}
      end

    errors =
      unknown_keys(cvd, ~w($description deficiency minDeltaEOK sets), "$cvd") ++
        deficiency_errors ++ min_errors ++ set_errors

    {%{min: if(is_number(min), do: min * 1.0, else: 0.0), sets: sets}, errors}
  end

  defp parse_cvd(_cvd, _index), do: {%{min: 0.0, sets: []}, []}

  defp parse_gamut(%OrderedObject{} = gamut) do
    method = get(gamut, "method")
    max = get(gamut, "maxDeltaEOK")

    errors =
      unknown_keys(gamut, ~w($description method maxDeltaEOK), "$gamut") ++
        if(method == "clip", do: [], else: ["$gamut: method #{inspect(method)} is not supported; only \"clip\" is"]) ++
        if positive?(max), do: [], else: ["$gamut: maxDeltaEOK #{inspect(max)} must be a positive number"]

    {%{max: if(is_number(max), do: max * 1.0, else: 0.0)}, errors}
  end

  defp parse_gamut(_gamut), do: {%{max: 0.0}, []}

  # Colors with different meanings that must never look alike: every member of `these` stays at least
  # minDeltaEOK from every member of `from`, in OKLab, in each theme. It keeps a cluster identity color
  # from reading as a state, which the description of the cluster group promises.
  defp parse_apart(%OrderedObject{} = apart, index) do
    min = get(apart, "minDeltaEOK")
    min_errors = if positive?(min), do: [], else: ["$apart: minDeltaEOK #{inspect(min)} must be a positive number"]

    {sets, set_errors} =
      case get(apart, "sets") do
        sets when is_list(sets) ->
          results = Enum.map(sets, &apart_set(&1, index))
          {for({:ok, set} <- results, do: set), for({:error, errors} <- results, error <- errors, do: error)}

        _other ->
          {[], ["$apart: sets must be a list"]}
      end

    errors = unknown_keys(apart, ~w($description minDeltaEOK sets), "$apart") ++ min_errors ++ set_errors
    {%{min: if(is_number(min), do: min * 1.0, else: 0.0), sets: sets}, errors}
  end

  defp parse_apart(_apart, _index), do: {%{min: 0.0, sets: []}, []}

  defp apart_set(%OrderedObject{values: entries} = set, index) do
    these = get(set, "these")
    from = get(set, "from")

    cond do
      entries |> Enum.map(&elem(&1, 0)) |> Enum.sort() != ~w(from these) ->
        {:error, ["$apart: a set has exactly these and from"]}

      not (match?([_ | _], these) and match?([_ | _], from)) ->
        {:error, ["$apart: these and from each name at least one color"]}

      true ->
        case Enum.flat_map(these ++ from, &color_path_errors("$apart", &1, index)) do
          [] -> {:ok, %{these: these, from: from}}
          errors -> {:error, errors}
        end
    end
  end

  defp apart_set(_set, _index), do: {:error, ["$apart: a set has exactly these and from"]}

  defp positive?(value), do: is_number(value) and value > 0

  defp color_path_errors(section, path, index) do
    case Map.get(index, path) do
      %Token{type: :color} -> []
      _other -> ["#{section}: #{format_path(path)} is not a color token"]
    end
  end

  defp format_path(path) when is_binary(path), do: path
  defp format_path(path), do: inspect(path)

  # -- $ansi16, $ansi256, $terminal ---------------------------------------------------------------

  defp parse_terminal(%OrderedObject{} = terminal, tui) do
    allowed = ~w($description $inheritedNote inherited distinct painted16 modifiers glyphs detection)
    {inherited, inherited_errors} = parse_inherited(get(terminal, "inherited"), tui)
    {distinct, distinct_errors} = tui_list(get(terminal, "distinct"), "$terminal.distinct", tui)
    {painted16, painted_errors} = parse_painted16(get(terminal, "painted16"))

    {%{inherited: inherited, distinct: distinct, painted16: painted16},
     unknown_keys(terminal, allowed, "$terminal") ++ inherited_errors ++ distinct_errors ++ painted_errors}
  end

  defp parse_terminal(_terminal, _tui), do: {%{inherited: %{}, distinct: [], painted16: %{}}, []}

  # Which token the terminal's own background stands in for and which its own foreground, named rather
  # than implied by order, because the painted surface puts each one in a different place.
  defp parse_inherited(%OrderedObject{values: entries} = inherited, tui) do
    if entries |> Enum.map(&elem(&1, 0)) |> Enum.sort() == ~w(background foreground) do
      background = get(inherited, "background")
      foreground = get(inherited, "foreground")
      {_paths, errors} = tui_list([background, foreground], "$terminal.inherited", tui)

      same =
        if background == foreground,
          do: ["$terminal.inherited: background and foreground name two different tokens"],
          else: []

      {%{background: background, foreground: foreground}, Enum.uniq(errors) ++ same}
    else
      {%{}, ["$terminal.inherited: names exactly background and foreground"]}
    end
  end

  defp parse_inherited(_inherited, _tui), do: {%{}, ["$terminal.inherited: names exactly background and foreground"]}

  defp tui_list(paths, context, tui) when is_list(paths) do
    errors = for path <- paths, path not in tui, do: "#{context}: #{format_path(path)} is not in $platforms.tui.tokens"
    {paths, errors}
  end

  defp tui_list(_paths, context, _tui), do: {[], ["#{context}: must be a list of terminal token paths"]}

  defp parse_painted16(%OrderedObject{} = painted) do
    if is_nil(get(painted, "dark")) or is_nil(get(painted, "light")) do
      {%{}, ["$terminal.painted16: declares dark and light"]}
    else
      results =
        for theme <- ~w(light dark), role <- ~w(background foreground) do
          context = "$terminal.painted16.#{theme}.#{role}"
          {theme, role, ansi_entry(context, get(get(painted, theme), role), false)}
        end

      painted =
        Enum.reduce(results, %{light: %{}, dark: %{}}, fn
          {theme, role, {:ok, entry}}, acc ->
            put_in(acc, [String.to_existing_atom(theme), String.to_existing_atom(role)], entry)

          _error, acc ->
            acc
        end)

      {painted, for({_theme, _role, {:error, error}} <- results, do: error)}
    end
  end

  defp parse_painted16(_painted), do: {%{}, ["$terminal.painted16: declares dark and light"]}

  defp parse_ansi16(%OrderedObject{values: entries}, tui, terminal) do
    entries = Enum.reject(entries, fn {key, _value} -> key == "$description" end)

    results =
      for {path, value} <- entries do
        if path in tui,
          do: {path, ansi_entry("$ansi16: #{path}", value, true)},
          else: {path, {:error, "$ansi16: #{path} is not in $platforms.tui.tokens"}}
      end

    ansi16 = for {path, {:ok, entry}} <- results, into: %{}, do: {path, entry}
    entry_errors = for {_path, {:error, error}} <- results, do: error

    missing =
      for path <- tui, not List.keymember?(entries, path, 0) do
        "$ansi16: #{path} is a terminal token and has no entry; the 16 color tier is declared by hand, never computed"
      end

    {ansi16, entry_errors ++ missing ++ inherit_errors(ansi16, terminal) ++ distinct_code_errors(ansi16, terminal)}
  end

  defp parse_ansi16(_ansi16, _tui, _terminal), do: {%{}, []}

  defp inherit_errors(ansi16, terminal) do
    for {path, %{code: code}} <- Enum.sort(ansi16),
        error = inherit_error(path, code, path in Map.values(terminal.inherited)),
        do: error
  end

  defp inherit_error(path, :default, false),
    do: "$ansi16: #{path} uses default, but #{path} is not in $terminal.inherited"

  defp inherit_error(path, code, true) when code != :default, do: "$ansi16: #{path} is inherited and must be default"
  defp inherit_error(_path, _code, _inherited), do: nil

  defp distinct_code_errors(ansi16, terminal) do
    coded = for path <- terminal.distinct, entry = ansi16[path], do: {path, entry.code}

    for {{a, code}, i} <- Enum.with_index(coded), {b, ^code} <- Enum.drop(coded, i + 1) do
      "$ansi16: #{a} and #{b} share code #{code}, and $terminal.distinct requires them apart"
    end
  end

  defp ansi_entry(context, %OrderedObject{values: entries} = entry, allow_default?) do
    code = get(entry, "code")
    ratatui = get(entry, "ratatui")

    cond do
      entries |> Enum.map(&elem(&1, 0)) |> Enum.sort() != ~w(code ratatui) ->
        {:error, "#{context}: an entry has exactly code and ratatui"}

      code == "default" and allow_default? ->
        ratatui_check(context, :default, ratatui)

      is_integer(code) and code in 0..15 ->
        ratatui_check(context, code, ratatui)

      true ->
        {:error, "#{context}: code #{inspect(code)} is not 0 to 15#{if allow_default?, do: " or default", else: ""}"}
    end
  end

  defp ansi_entry(context, _entry, _allow_default?), do: {:error, "#{context}: an entry has exactly code and ratatui"}

  defp ratatui_check(context, code, ratatui) do
    expected = ansi_name(code)

    if ratatui == expected,
      do: {:ok, %{code: code, ratatui: expected}},
      else:
        {:error, "#{context}: code #{if code == :default, do: "default", else: code} is #{expected}, not #{ratatui}"}
  end

  defp parse_ansi256(%OrderedObject{} = ansi256, tui) do
    if is_nil(get(ansi256, "light")) or is_nil(get(ansi256, "dark")) do
      {%{light: %{}, dark: %{}}, ["$ansi256: declares light and dark"]}
    else
      {light, light_errors} = ansi256_theme(get(ansi256, "light"), "light", tui)
      {dark, dark_errors} = ansi256_theme(get(ansi256, "dark"), "dark", tui)

      {%{light: light, dark: dark},
       unknown_keys(ansi256, ~w($description light dark), "$ansi256") ++ light_errors ++ dark_errors}
    end
  end

  defp parse_ansi256(_ansi256, _tui), do: {%{light: %{}, dark: %{}}, []}

  defp ansi256_theme(%OrderedObject{values: entries}, theme, tui) do
    results =
      for {path, index} <- entries do
        cond do
          path not in tui -> {:error, "$ansi256.#{theme}: #{path} is not in $platforms.tui.tokens"}
          is_integer(index) and index in 16..255 -> {:ok, {path, index}}
          true -> {:error, "$ansi256.#{theme}: #{path} index #{inspect(index)} is not between 16 and 255"}
        end
      end

    {Map.new(for({:ok, entry} <- results, do: entry)), for({:error, error} <- results, do: error)}
  end

  defp ansi256_theme(_theme_value, theme, _tui),
    do: {%{}, ["$ansi256.#{theme}: must be an object of token paths and indexes"]}

  # -- Helpers ------------------------------------------------------------------------------------

  defp unknown_keys(%OrderedObject{values: entries}, allowed, context) do
    for {key, _value} <- entries, key not in allowed, do: "#{context}: unknown key #{key}"
  end

  defp get(%OrderedObject{values: entries}, key) do
    case List.keyfind(entries, key, 0) do
      {^key, value} -> value
      nil -> nil
    end
  end

  defp get(_other, _key), do: nil

  defp plain(%OrderedObject{values: entries}), do: Map.new(entries, fn {key, value} -> {key, plain(value)} end)
  defp plain(list) when is_list(list), do: Enum.map(list, &plain/1)
  defp plain(value), do: value

  defp merge(results) do
    {Enum.flat_map(results, &elem(&1, 0)), Enum.flat_map(results, &elem(&1, 1))}
  end
end
