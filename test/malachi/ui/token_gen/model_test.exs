defmodule Malachi.UI.TokenGen.ModelTest do
  use ExUnit.Case, async: true

  import Malachi.TokenFixture, only: [minimal: 0, repo: 0, put: 3, object: 1]

  alias Malachi.UI.TokenGen.{Gates, Model, Source}

  defp model(file) do
    {:ok, source} = Source.parse(file)
    Model.build(source)
  end

  defp entry(model, name), do: Enum.find(model.entries, &(&1.name == name))

  defp gate_errors(file), do: file |> model() |> Gates.check()

  defp assert_gate(file, fragment) do
    errors = gate_errors(file)

    assert Enum.any?(errors, &String.contains?(&1, fragment)),
           "expected a gate error containing #{inspect(fragment)}, got:\n" <> Enum.join(errors, "\n")
  end

  describe "Model.build/1" do
    test "a themed color keeps its authored OKLCH and carries its sRGB bytes" do
      on = entry(model(minimal()), "state-on")
      assert on.web == %{light: "oklch(0.627 0.165 149.2)", dark: "oklch(0.696 0.170 149.2)"}
      assert on.value == on.web
      assert on.color.light.alpha == 1.0
      assert {r, g, b} = on.color.light.srgb8
      assert g > r and g > b
    end

    test "a reference is a var() on the web and the referenced value everywhere else" do
      text = entry(model(minimal()), "text-body")
      assert text.ref == "foreground"
      assert text.web == %{light: "var(--foreground)", dark: "var(--foreground)"}
      assert text.value == %{light: "oklch(0.210 0.006 265)", dark: "oklch(0.962 0.002 265)"}
      assert text.color.dark.srgb8 == entry(model(minimal()), "foreground").color.dark.srgb8
    end

    test "every other type is formatted the way CSS writes it" do
      model = model(minimal())
      assert entry(model, "font-sans").value.light == ~s(system-ui, "Segoe UI", sans-serif)
      assert entry(model, "leading-normal").value.light == "1.5"
      assert entry(model, "weight-regular").value.light == "400"
      assert entry(model, "space-neg").value.light == "-0.5em"
      assert entry(model, "ease-standard").value.light == "cubic-bezier(0.2, 0, 0, 1)"

      body = entry(model, "type-body")
      assert body.web.light == "var(--weight-regular) var(--text-base)/var(--leading-normal) var(--font-sans)"
      assert body.value.light == ~s(400 14px/1.5 system-ui, "Segoe UI", sans-serif)

      shadow = entry(model, "shadow-xs")
      assert shadow.web.light == "0px 1px 2px -1px var(--shadow-color-soft)"
      assert shadow.value.dark == "0px 1px 2px -1px oklch(0.000 0.000 0 / 0.1)"
    end

    test "modes and reduced motion are carried as web expressions" do
      model = model(minimal())
      assert entry(model, "row-height").modes == [{"compact", "var(--row-compact)"}]
      assert entry(model, "row-height").value.light == "32px"
      assert entry(model, "motion-fast").reduced_motion == "0ms"
    end

    test "terminal tokens get a 256 index per theme and their 16 color entry" do
      model = model(minimal())
      on = entry(model, "state-on")
      assert on.ansi256.light in 16..255
      assert on.ansi16 == %{code: 10, ratatui: "Color::LightGreen"}
      refute on.inherited

      background = entry(model, "background")
      assert background.inherited
      assert entry(model, "card").ansi256 == nil
    end

    test "a declared 256 index replaces the quantised one, and only for its theme" do
      quantised = entry(model(minimal()), "state-off").ansi256
      file = put(minimal(), ["$ansi256", "dark"], object([{"color.state.off", 160}]))
      declared = entry(model(file), "state-off").ansi256
      assert declared.dark == 160
      assert declared.light == quantised.light
    end

    test "a token is redeclared for a mode it depends on, but not for a mode it declares itself" do
      gap = object([{"$value", "{density.row.height}"}])
      own = object([{"$value", "{density.row.height}"}, {"$modes", object([{"compact", "{density.row.default}"}])}])
      file = put(put(minimal(), ["density", "row", "gap"], gap), ["density", "row", "own"], own)
      model = model(file)

      assert entry(model, "row-gap").redeclare.modes == ["compact"]
      assert entry(model, "row-own").redeclare.modes == []
      assert entry(model, "row-height").redeclare.modes == []
      assert entry(model, "text-body").redeclare.dark
      refute entry(model, "space-1").redeclare.dark
    end

    test "the repository's file builds with the dark behind declared at 221" do
      behind = entry(model(repo()), "state-behind")
      assert behind.ansi256.dark == 221
      assert entry(model(repo()), "sidebar").path == "color.sidebar.background"
    end
  end

  describe "Gates.check/1" do
    test "the minimal file and the repository's file pass every gate" do
      assert gate_errors(minimal()) == []
      assert gate_errors(repo()) == []
    end

    test "a contrast pair below its threshold fails with the pair, the theme and the ratio" do
      file = put(minimal(), ["color", "state", "on", "light"], "oklch(0.900 0.050 149.2)")
      assert_gate(file, "$contrast: color.state.on on color.base.background in light is 1.")
      assert_gate(file, "below 3")
    end

    test "a text pair is held to 4.5 even where a graphic would pass at 3" do
      # state.on in light reaches about 3.4 on the background: enough for a mark, not for text.
      pair = object([{"foreground", "color.state.on"}, {"background", "color.base.background"}, {"role", "text"}])
      file = put(minimal(), ["$contrast", "pairs"], [pair])
      assert_gate(file, "$contrast: color.state.on on color.base.background in light is 3.")
      assert_gate(file, "below 4.5")
    end

    test "a contrast pair with alpha is refused, not composited" do
      pair = object([{"foreground", "color.shadow.soft"}, {"background", "color.base.card"}, {"role", "graphic"}])
      file = put(minimal(), ["$contrast", "pairs"], [pair])
      assert_gate(file, "$contrast: color.shadow.soft on color.base.card carries alpha")
    end

    test "two colors a deuteranope cannot tell apart fail the cvd gate" do
      file =
        put(
          minimal(),
          ["color", "chart", "2"],
          object([{"light", "oklch(0.550 0.120 240.0)"}, {"dark", "oklch(0.769 0.157 70.1)"}])
        )

      assert_gate(file, "$cvd: color.chart.1 and color.chart.2 in light are 0.0")
    end

    test "a color sRGB cannot approximate fails the gamut gate" do
      file = put(minimal(), ["color", "chart", "1", "dark"], "oklch(0.546 0.350 244.3)")
      assert_gate(file, "$gamut: color.chart.1 in dark moves")
    end

    test "two distinct tokens on one 256 index fail, and a declared index resolves it" do
      file = put(minimal(), ["color", "state", "off", "dark"], "oklch(0.700 0.170 149.2)")
      file = put(file, ["$contrast", "pairs"], [])
      assert_gate(file, "$ansi256: color.state.on and color.state.off in dark both quantise to")

      on_dark = entry(model(file), "state-on").ansi256.dark
      other = if on_dark == 34, do: 35, else: 34
      fixed = put(file, ["$ansi256", "dark"], object([{"color.state.off", other}]))
      assert gate_errors(fixed) == []
    end

    test "a declared index equal to the quantised one is refused as redundant" do
      quantised = entry(model(minimal()), "state-off").ansi256.dark
      file = put(minimal(), ["$ansi256", "dark"], object([{"color.state.off", quantised}]))
      assert_gate(file, "$ansi256.dark: color.state.off declares #{quantised}, which quantisation picks anyway")
    end
  end
end
