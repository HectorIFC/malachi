defmodule Malachi.UI.TokenGen.SourceTest do
  use ExUnit.Case, async: true

  import Malachi.TokenFixture, only: [minimal: 0, repo: 0, put: 3, delete: 2, fetch!: 2, object: 1, encode!: 1]

  alias Malachi.UI.TokenGen
  alias Malachi.UI.TokenGen.{Source, Token}

  defp errors(file) do
    assert {:error, errors} = Source.parse(file)
    errors
  end

  defp assert_error(file, fragment) do
    errors = errors(file)

    assert Enum.any?(errors, &String.contains?(&1, fragment)),
           "expected an error containing #{inspect(fragment)}, got:\n" <> Enum.join(errors, "\n")
  end

  describe "a valid file" do
    test "the repository's token file parses" do
      assert {:ok, %Source{}} = Source.parse(repo())
    end

    test "yields every token in declaration order with its CSS name, type and platforms" do
      {:ok, source} = Source.parse(minimal())

      assert Enum.map(source.tokens, & &1.name) == [
               "background",
               "foreground",
               "card",
               "text-body",
               "state-on",
               "state-off",
               "chart-1",
               "chart-2",
               "shadow-color-soft",
               "font-sans",
               "text-base",
               "leading-normal",
               "weight-regular",
               "type-body",
               "space-1",
               "space-neg",
               "shadow-xs",
               "row-default",
               "row-compact",
               "row-height",
               "motion-fast",
               "ease-standard"
             ]

      on = Enum.find(source.tokens, &(&1.path == "color.state.on"))
      assert %Token{type: :color, themed: true, ref: nil, platforms: [:web, :elixir, :tui]} = on
      assert on.raw == %{light: "oklch(0.627 0.165 149.2)", dark: "oklch(0.696 0.170 149.2)"}

      alias_token = Enum.find(source.tokens, &(&1.name == "text-body"))
      assert %Token{ref: "color.base.foreground", themed: false, platforms: [:web, :elixir]} = alias_token

      height = Enum.find(source.tokens, &(&1.name == "row-height"))
      assert height.modes == [{"compact", "density.row.compact"}]

      fast = Enum.find(source.tokens, &(&1.name == "motion-fast"))
      assert fast.reduced_motion == "0ms"
    end

    test "a contrast floor comes from the pair's role: 4.5 for text, 3 for a graphic" do
      {:ok, source} = Source.parse(minimal())
      assert Enum.map(source.contrast, &{&1.role, &1.min}) == [{:text, 4.5}, {:graphic, 3.0}, {:graphic, 3.0}]
    end

    test "the repository gates every chart series and the control border at 3 against both surfaces" do
      {:ok, source} = Source.parse(repo())
      gated = MapSet.new(for %{role: :graphic} = p <- source.contrast, do: {p.foreground, p.background})

      for fg <- ~w(color.chart.1 color.chart.2 color.chart.3 color.chart.4 color.chart.5 color.base.input),
          bg <- ~w(color.base.background color.base.card) do
        assert {fg, bg} in gated, "#{fg} on #{bg} is not gated at 3:1"
      end
    end

    test "the repository gates the focus ring at 3 on background, card, muted, popover and the sidebar" do
      {:ok, source} = Source.parse(repo())
      gated = MapSet.new(for %{role: :graphic} = p <- source.contrast, do: {p.foreground, p.background})

      expected =
        for(
          bg <- ~w(color.base.background color.base.card color.base.muted color.base.popover),
          do: {"color.base.ring", bg}
        ) ++
          for bg <- ~w(color.sidebar.background color.sidebar.accent), do: {"color.sidebar.ring", bg}

      for pair <- expected, do: assert(pair in gated, "#{inspect(pair)} is not gated at 3:1")
    end

    test "the repository keeps every cluster color apart from every state color" do
      {:ok, source} = Source.parse(repo())
      clusters = ~w(color.cluster.teal color.cluster.orange color.cluster.violet color.cluster.rose)

      states =
        ~w(active sealed fenced damaged behind ahead blocked unknown) |> Enum.map(&"color.state.#{&1}")

      covered =
        for %{these: these, from: from} <- source.apart.sets, a <- these, b <- from, into: MapSet.new(), do: {a, b}

      for a <- clusters, b <- states, do: assert({a, b} in covered, "#{a} is not kept apart from #{b}")
    end

    test "reads the sections" do
      {:ok, source} = Source.parse(minimal())

      assert [%{foreground: "color.base.foreground", background: "color.base.background", role: :text, min: 4.5} | _] =
               source.contrast

      assert source.cvd == %{min: 0.05, sets: [["color.chart.1", "color.chart.2"]]}
      assert source.gamut == %{max: 0.03}

      assert source.apart == %{
               min: 0.05,
               sets: [%{these: ["color.chart.1", "color.chart.2"], from: ["color.state.on", "color.state.off"]}]
             }

      assert source.ansi16["color.state.on"] == %{code: 10, ratatui: "Color::LightGreen"}
      assert source.ansi16["color.base.background"] == %{code: :default, ratatui: "Color::Reset"}
      assert source.ansi256 == %{light: %{}, dark: %{}}
      assert source.terminal.inherited == %{background: "color.base.background", foreground: "color.base.foreground"}
      assert source.terminal.distinct == ["color.state.on", "color.state.off"]
      assert source.terminal.painted16.dark.foreground == %{code: 15, ratatui: "Color::White"}

      assert source.tui ==
               ~w(color.base.background color.base.foreground color.state.on color.state.off color.chart.1 color.chart.2)
    end
  end

  describe "load/1" do
    test "decodes JSON and parses it" do
      assert {:ok, %Source{}} = Source.load(File.read!("test/fixtures/tokens/minimal.json"))
    end

    test "refuses text that is not JSON, or JSON that is not an object" do
      assert {:error, [message]} = Source.load("{not json")
      assert message =~ "not valid JSON"
      assert {:error, [message]} = Source.load("[1, 2]")
      assert message =~ "must be a JSON object"
    end
  end

  describe "structure" do
    test "a missing section" do
      assert_error(delete(minimal(), ["$gamut"]), "$gamut: missing")
    end

    test "a section present but not an object is an error, never an empty section that gates nothing" do
      for section <- ~w($platforms $contrast $cvd $apart $gamut $ansi16 $ansi256 $terminal), value <- [nil, 1, []] do
        assert_error(put(minimal(), [section], value), "#{section}: must be an object")
      end
    end

    test "every malformed shape inside a section or a token is named, never skipped" do
      alias_to = fn target -> object([{"$value", target}]) end
      tui = fetch!(minimal(), ["$platforms", "tui", "tokens"])

      layer = fn overrides ->
        base = [
          {"offsetX", "0px"},
          {"offsetY", "1px"},
          {"blur", "2px"},
          {"spread", "0px"},
          {"color", "{color.shadow.soft}"}
        ]

        object(Enum.map(base, fn {k, v} -> {k, Keyword.get(overrides, String.to_atom(k), v)} end))
      end

      cases = [
        {["space"], 1, "space: must be an object"},
        {["color", "base", "card", "$value"], "{color.base.foreground}",
         "color.base.card: declares both $value and light and dark"},
        {["color", "base", "card"], object([{"dark", "oklch(0.208 0.005 265)"}]),
         "color.base.card: declares dark without light"},
        {["color", "base", "card", "light"], 5, "color.base.card (light): 5 is not a string"},
        {["typography", "style", "body", "$value"], "body", "typography.style.body: a typography value has exactly"},
        {["shadow", "xs", "$value"], [layer.(offsetX: "1")], "shadow.xs: shadow offsetX \"1\" is not a dimension"},
        {["shadow", "xs", "$value"], [layer.(blur: "-2px")], "shadow.xs: shadow blur may not be negative"},
        {["shadow", "xs", "$value"], [1], "shadow.xs: a shadow layer has exactly"},
        {["density", "row", "height", "$modes"], object([{"compact", "24px"}]),
         "density.row.height: mode compact must reference a token"},
        {["density", "row", "height", "$modes"], "compact", "density.row.height: $modes must be an object"},
        {["$platforms", "desktop"], object([]), "$platforms: unknown platform desktop"},
        {["$platforms", "web", "groups"], "color", "$platforms.web: groups must be a list"},
        {["$platforms", "web"], 1, "$platforms.web: must be an object"},
        {["$platforms", "tui"], 1, "$platforms.tui: must be an object"},
        {["$platforms", "tui", "tokens"], "color.base.background", "$platforms.tui: tokens must be a list"},
        {["$contrast", "pairs"], "all", "$contrast: pairs must be a list"},
        {["$contrast", "pairs"], [1], "$contrast: a pair has exactly foreground, background and role"},
        {["$contrast", "pairs"], [object([{"foreground", 5}, {"background", "color.base.card"}, {"role", "graphic"}])],
         "$contrast: 5 is not a color token"},
        {["$cvd", "sets"], "charts", "$cvd: sets must be a list"},
        {["$terminal", "distinct"], "states", "$terminal.distinct: must be a list"},
        {["$terminal", "painted16"], 1, "$terminal.painted16: declares dark and light"},
        {["$terminal", "painted16", "dark"], 1,
         "$terminal.painted16.dark.background: an entry has exactly code and ratatui"},
        {["$ansi16", "color.state.off"], 5, "$ansi16: color.state.off: an entry has exactly code and ratatui"},
        {["$ansi16", "color.state.off"], object([{"code", 9}, {"ratatui", "Color::LightRed"}, {"bold", true}]),
         "an entry has exactly code and ratatui"},
        {["$ansi256", "dark"], 1, "$ansi256.dark: must be an object"},
        {["$gamut", "colour"], "x", "$gamut: unknown key colour"}
      ]

      for {path, value, fragment} <- cases do
        assert_error(put(minimal(), path, value), fragment)
      end

      assert_error(delete(minimal(), ["$platforms", "web"]), "$platforms: missing platform web")
      assert_error(delete(minimal(), ["$platforms", "tui"]), "$platforms: missing platform tui")

      # A terminal alias whose target is missing, and one that is not a valid color, report their own errors.
      missing = put(minimal(), ["color", "alias", "ghost"], alias_to.("{color.base.nope}"))
      missing = put(missing, ["$platforms", "tui", "tokens"], tui ++ ["color.alias.ghost"])
      missing = put(missing, ["$ansi16", "color.alias.ghost"], object([{"code", 8}, {"ratatui", "Color::DarkGray"}]))
      assert_error(missing, "color.alias.ghost: references color.base.nope, which does not exist")

      broken = put(minimal(), ["color", "chart", "1", "light"], "oklch(2 0 0)")
      assert_error(broken, "color.chart.1 (light): lightness 2.0 is above 1")

      literal = put(minimal(), ["color", "chart", "1"], object([{"$value", "oklch(0.5 0.1 20)"}]))
      assert_error(literal, "color.chart.1: a color declares light and dark")
    end

    test "a null $contrast cannot hide a pair that fails" do
      file = put(minimal(), ["$contrast"], nil)
      file = put(file, ["color", "state", "on", "light"], "oklch(0.900 0.050 149.2)")
      assert {:error, _errors} = TokenGen.generate(encode!(file))
    end

    test "an unknown section" do
      assert_error(put(minimal(), ["$colours"], object([])), "$colours: unknown section")
    end

    test "a token group no platform carries" do
      file = put(minimal(), ["$platforms", "web", "groups"], ["color"])
      file = put(file, ["$platforms", "elixir", "groups"], ["color"])
      assert_error(file, "space: no platform carries this group")
    end

    test "a platform naming a group that does not exist" do
      assert_error(put(minimal(), ["$platforms", "web", "groups"], ["color", "nope"]), "$platforms.web: no group nope")
    end

    test "a group that holds tokens but declares no prefix" do
      assert_error(delete(minimal(), ["space", "$prefix"]), "space: holds tokens but declares no $prefix")
    end

    test "a bad prefix" do
      assert_error(put(minimal(), ["space", "$prefix"], "Space_"), "space: $prefix")
    end

    test "an unknown type" do
      assert_error(put(minimal(), ["space", "$type"], "length"), "space: unknown $type \"length\"")
    end

    test "a token with no type" do
      file = delete(minimal(), ["easing", "$type"])
      assert_error(file, "easing.standard: no $type")
    end

    test "an unknown key on a token" do
      assert_error(put(minimal(), ["space", "1", "$unit"], "px"), "space.1: unknown key $unit")
    end

    test "an unknown key on a group" do
      assert_error(put(minimal(), ["space", "$sort"], "asc"), "space: unknown key $sort")
    end

    test "a group member that is not an object" do
      assert_error(put(minimal(), ["space", "2"], "8px"), "space.2: must be an object")
    end

    test "two tokens with one CSS name" do
      file = put(minimal(), ["space", "dup"], object([{"$name", "space-1"}, {"$value", "8px"}]))
      assert_error(file, "space-1: declared by both space.1 and space.dup")
    end

    test "a name that is not a CSS identifier" do
      assert_error(put(minimal(), ["space", "1", "$name"], "1space"), "space.1: name \"1space\"")
    end
  end

  describe "values" do
    test "a color with only one theme" do
      assert_error(delete(minimal(), ["color", "base", "card", "dark"]), "color.base.card: declares light without dark")
    end

    test "a color that is neither themed nor a reference" do
      file = put(minimal(), ["color", "base", "card"], object([{"$value", "oklch(1 0 0)"}]))
      assert_error(file, "color.base.card: a color declares light and dark, or references another color")
    end

    test "a non color declaring themes" do
      file = put(minimal(), ["space", "1"], object([{"light", "4px"}, {"dark", "4px"}]))
      assert_error(file, "space.1: only a color declares light and dark")
    end

    test "a malformed color" do
      assert_error(put(minimal(), ["color", "base", "card", "dark"], "#ffffff"), "color.base.card (dark)")
    end

    test "a malformed value of every other type" do
      cases = [
        {["space", "1", "$value"], "4", "space.1: \"4\" is not a dimension"},
        {["motion", "fast", "$value"], "0.1s", "motion.fast: \"0.1s\" is not a duration"},
        {["motion", "fast", "$reducedMotion"], "none", "motion.fast: $reducedMotion \"none\" is not a duration"},
        {["typography", "weight", "regular", "$value"], 450, "typography.weight.regular: 450 is not a font weight"},
        {["typography", "leading", "normal", "$value"], "1.5", "typography.leading.normal: \"1.5\" is not a number"},
        {["typography", "family", "sans", "$value"], [], "typography.family.sans: a font family is a non empty list"},
        {["typography", "family", "sans", "$value"], ["Segoe \"UI\""], "typography.family.sans: font name"},
        {["easing", "standard", "$value"], [0.2, 0, 1], "easing.standard: a cubic bezier is four numbers"},
        {["easing", "standard", "$value"], [1.2, 0, 0, 1], "easing.standard: a cubic bezier is four numbers"},
        {["shadow", "xs", "$value"], [], "shadow.xs: a shadow is a non empty list of layers"},
        {["shadow", "xs", "$value"], [object([{"offsetX", "0px"}])], "shadow.xs: a shadow layer has exactly"},
        {["typography", "style", "body", "$value"], object([{"fontSize", "{typography.size.base}"}]),
         "typography.style.body: a typography value has exactly"}
      ]

      for {path, value, fragment} <- cases do
        assert_error(put(minimal(), path, value), fragment)
      end
    end

    test "a shadow layer with a literal color" do
      layer =
        object([
          {"offsetX", "0px"},
          {"offsetY", "1px"},
          {"blur", "2px"},
          {"spread", "0px"},
          {"color", "oklch(0 0 0 / 0.1)"}
        ])

      assert_error(put(minimal(), ["shadow", "xs", "$value"], [layer]), "shadow.xs: a shadow color is a reference")
    end

    test "$reducedMotion on something that is not a duration" do
      assert_error(put(minimal(), ["space", "1", "$reducedMotion"], "0ms"), "space.1: only a duration declares")
    end

    test "a mode name that is not an identifier" do
      file = put(minimal(), ["density", "row", "height", "$modes"], object([{"Tight!", "{density.row.compact}"}]))
      assert_error(file, "density.row.height: mode \"Tight!\"")
    end
  end

  describe "references" do
    test "to a token that does not exist" do
      file = put(minimal(), ["color", "alias", "text-body", "$value"], "{color.base.nope}")
      assert_error(file, "color.alias.text-body: references color.base.nope, which does not exist")
    end

    test "to a token of another type" do
      file = put(minimal(), ["color", "alias", "text-body", "$value"], "{space.1}")
      assert_error(file, "color.alias.text-body: references space.1, a dimension, where a color belongs")
    end

    test "inside a composite, to the wrong type" do
      file = put(minimal(), ["typography", "style", "body", "$value", "fontSize"], "{typography.weight.regular}")
      assert_error(file, "typography.style.body: references typography.weight.regular, a font_weight")
    end

    test "inside a mode, to the wrong type" do
      file = put(minimal(), ["density", "row", "height", "$modes"], object([{"compact", "{motion.fast}"}]))
      assert_error(file, "density.row.height: references motion.fast, a duration")
    end

    test "in a cycle" do
      file = put(minimal(), ["color", "alias", "loop-a"], object([{"$value", "{color.alias.loop-b}"}]))
      file = put(file, ["color", "alias", "loop-b"], object([{"$value", "{color.alias.loop-a}"}]))
      assert_error(file, "color.alias.loop-a: reference cycle")
    end

    test "in a cycle that exists only in one mode, between two alternatives" do
      gap = object([{"$value", "{density.row.default}"}, {"$modes", object([{"compact", "{density.row.gap2}"}])}])
      gap2 = object([{"$value", "{density.row.default}"}, {"$modes", object([{"compact", "{density.row.gap}"}])}])
      file = minimal() |> put(["density", "row", "gap"], gap) |> put(["density", "row", "gap2"], gap2)
      assert_error(file, "density.row.gap: reference cycle in mode compact through density.row.gap, density.row.gap2")
    end

    test "in a cycle that a mode closes through an ordinary value" do
      # gap's own value names height, and height's compact alternative names gap: in .density-compact
      # the two declarations would name each other.
      file = put(minimal(), ["density", "row", "gap"], object([{"$value", "{density.row.height}"}]))
      height = object([{"$value", "{density.row.default}"}, {"$modes", object([{"compact", "{density.row.gap}"}])}])
      file = put(file, ["density", "row", "height"], height)
      assert_error(file, "reference cycle in mode compact through")
    end

    test "a mode alternative that names a token with no mode of its own is not a cycle" do
      gap = object([{"$value", "{density.row.default}"}, {"$modes", object([{"compact", "{density.row.height}"}])}])
      assert {:ok, _source} = Source.parse(put(minimal(), ["density", "row", "gap"], gap))
    end
  end

  describe "$contrast, $cvd and $gamut" do
    test "a pair naming a missing or non color token" do
      pairs = fetch!(minimal(), ["$contrast", "pairs"])
      bad = object([{"foreground", "space.1"}, {"background", "color.base.nope"}, {"role", "graphic"}])
      file = put(minimal(), ["$contrast", "pairs"], pairs ++ [bad])
      assert_error(file, "$contrast: space.1 is not a color token")
      assert_error(file, "$contrast: color.base.nope is not a color token")
    end

    test "a pair with an impossible threshold or extra keys" do
      bad = object([{"foreground", "color.base.foreground"}, {"background", "color.base.card"}, {"role", "decoration"}])

      assert_error(
        put(minimal(), ["$contrast", "pairs"], [bad]),
        ~s|$contrast: role "decoration" is not text or graphic|
      )

      # The floor comes from the role, so a pair cannot declare a weaker one of its own.
      weaker = object([{"foreground", "color.base.foreground"}, {"background", "color.base.card"}, {"min", 2}])

      assert_error(
        put(minimal(), ["$contrast", "pairs"], [weaker]),
        "$contrast: a pair has exactly foreground, background and role"
      )

      extra =
        object([{"foreground", "color.base.foreground"}, {"background", "color.base.card"}, {"role", "text"}, {"x", 1}])

      assert_error(put(minimal(), ["$contrast", "pairs"], [extra]), "$contrast: a pair has exactly")
    end

    test "a deficiency the generator does not simulate" do
      assert_error(put(minimal(), ["$cvd", "deficiency"], "tritanopia"), "$cvd: deficiency \"tritanopia\"")
    end

    test "a cvd set that is too small or names a non color" do
      assert_error(put(minimal(), ["$cvd", "sets"], [["color.chart.1"]]), "$cvd: a set names at least two")
      assert_error(put(minimal(), ["$cvd", "sets"], [["color.chart.1", "space.1"]]), "$cvd: space.1 is not a color")
    end

    test "an $apart set with the wrong shape, a non color member or a bad threshold" do
      assert_error(
        put(minimal(), ["$apart", "sets"], [object([{"these", ["color.chart.1"]}])]),
        "$apart: a set has exactly these and from"
      )

      bad = object([{"these", ["color.chart.1"]}, {"from", ["space.1"]}])
      assert_error(put(minimal(), ["$apart", "sets"], [bad]), "$apart: space.1 is not a color token")

      empty = object([{"these", []}, {"from", ["color.state.on"]}])
      assert_error(put(minimal(), ["$apart", "sets"], [empty]), "$apart: these and from each name at least one color")

      assert_error(put(minimal(), ["$apart", "sets"], "all"), "$apart: sets must be a list")
      assert_error(put(minimal(), ["$apart", "sets"], [1]), "$apart: a set has exactly these and from")
      assert_error(put(minimal(), ["$apart", "minDeltaEOK"], 0), "$apart: minDeltaEOK")
      assert_error(put(minimal(), ["$apart", "x"], 1), "$apart: unknown key x")
    end

    test "a bad threshold or gamut method" do
      assert_error(put(minimal(), ["$cvd", "minDeltaEOK"], 0), "$cvd: minDeltaEOK")
      assert_error(put(minimal(), ["$gamut", "method"], "css4"), "$gamut: method \"css4\"")
      assert_error(put(minimal(), ["$gamut", "maxDeltaEOK"], -1), "$gamut: maxDeltaEOK")
    end
  end

  describe "$ansi16, never inferred" do
    test "a terminal token with no 16 color entry is an error, not a value filled in" do
      file = delete(minimal(), ["$ansi16", "color.state.off"])
      assert_error(file, "$ansi16: color.state.off is a terminal token and has no entry")
    end

    test "an entry for a token that is not a terminal token" do
      entry = object([{"code", 4}, {"ratatui", "Color::Blue"}])
      file = put(minimal(), ["$ansi16", "color.base.card"], entry)
      assert_error(file, "$ansi16: color.base.card is not in $platforms.tui.tokens")
    end

    test "a terminal token that does not exist or is not a color" do
      tui = fetch!(minimal(), ["$platforms", "tui", "tokens"])
      assert_error(put(minimal(), ["$platforms", "tui", "tokens"], tui ++ ["space.1"]), "$platforms.tui: space.1")
    end

    test "a terminal token with alpha" do
      tui = fetch!(minimal(), ["$platforms", "tui", "tokens"])
      file = put(minimal(), ["$platforms", "tui", "tokens"], tui ++ ["color.shadow.soft"])
      entry = object([{"code", 8}, {"ratatui", "Color::DarkGray"}])
      file = put(file, ["$ansi16", "color.shadow.soft"], entry)
      assert_error(file, "$platforms.tui: color.shadow.soft carries alpha")
    end

    test "a terminal token that is an alias of a color with alpha" do
      file = put(minimal(), ["color", "alias", "scrim"], object([{"$value", "{color.shadow.soft}"}]))
      tui = fetch!(minimal(), ["$platforms", "tui", "tokens"])
      file = put(file, ["$platforms", "tui", "tokens"], tui ++ ["color.alias.scrim"])
      file = put(file, ["$ansi16", "color.alias.scrim"], object([{"code", 8}, {"ratatui", "Color::DarkGray"}]))
      assert_error(file, "$platforms.tui: color.alias.scrim carries alpha")
    end

    test "a code outside 0 to 15" do
      entry = object([{"code", 16}, {"ratatui", "Color::Indexed"}])
      assert_error(put(minimal(), ["$ansi16", "color.state.off"], entry), "$ansi16: color.state.off: code 16")
    end

    test "a ratatui name that does not match the code" do
      entry = object([{"code", 9}, {"ratatui", "Color::Red"}])
      assert_error(put(minimal(), ["$ansi16", "color.state.off"], entry), "code 9 is Color::LightRed, not Color::Red")
    end

    test "default on a token the terminal does not inherit, and a code on one it does" do
      entry = object([{"code", "default"}, {"ratatui", "Color::Reset"}])

      assert_error(
        put(minimal(), ["$ansi16", "color.state.off"], entry),
        "color.state.off is not in $terminal.inherited"
      )

      entry = object([{"code", 0}, {"ratatui", "Color::Black"}])
      assert_error(put(minimal(), ["$ansi16", "color.base.background"], entry), "inherited and must be default")
    end

    test "two distinct tokens with one code" do
      entry = object([{"code", 10}, {"ratatui", "Color::LightGreen"}])
      file = put(minimal(), ["$ansi16", "color.state.off"], entry)
      assert_error(file, "$ansi16: color.state.on and color.state.off share code 10")
    end

    test "a painted pair with a bad entry" do
      entry = object([{"code", 15}, {"ratatui", "Color::Gray"}])
      file = put(minimal(), ["$terminal", "painted16", "dark", "foreground"], entry)
      assert_error(file, "$terminal.painted16.dark.foreground: code 15 is Color::White")

      file = delete(minimal(), ["$terminal", "painted16", "light"])
      assert_error(file, "$terminal.painted16: declares dark and light")
    end

    test "inherited and distinct lists naming tokens outside the terminal set" do
      inherited = object([{"background", "color.base.card"}, {"foreground", "color.base.foreground"}])
      assert_error(put(minimal(), ["$terminal", "inherited"], inherited), "$terminal.inherited: color.base.card")

      assert_error(
        put(minimal(), ["$terminal", "inherited"], ["color.base.background"]),
        "$terminal.inherited: names exactly background and foreground"
      )

      same = object([{"background", "color.base.background"}, {"foreground", "color.base.background"}])
      assert_error(put(minimal(), ["$terminal", "inherited"], same), "name two different tokens")

      assert_error(
        put(minimal(), ["$terminal", "distinct"], ["color.base.card"]),
        "$terminal.distinct: color.base.card"
      )
    end
  end

  describe "$ansi256" do
    test "an index outside 16 to 255" do
      file = put(minimal(), ["$ansi256", "dark"], object([{"color.state.off", 7}]))
      assert_error(file, "$ansi256.dark: color.state.off index 7 is not between 16 and 255")
    end

    test "an override for a token that is not a terminal token" do
      file = put(minimal(), ["$ansi256", "light"], object([{"color.base.card", 231}]))
      assert_error(file, "$ansi256.light: color.base.card is not in $platforms.tui.tokens")
    end

    test "a missing theme" do
      assert_error(delete(minimal(), ["$ansi256", "dark"]), "$ansi256: declares light and dark")
    end
  end
end
